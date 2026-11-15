"""班次交接：成熟水位、待定尾部、迟到补报与发布差异。

约定
----
- 每个 1 分钟桶 b 覆盖 [b·60, (b+1)·60)，其记录最晚应在 (b+1)·60 + device_deadline 之前到齐。
  交班时刻扣掉「最晚到点界限」得到该设备的成熟水位：水位及之前为已成熟，之后为待定尾部。
- 尾部空白不是缺口：不判定 gap、不插补、不 hold 补值，单独列出并计 pending。
- 已知仍未到位的迟到批次会把水位压在其起点之前；批次到位后水位前推，
  受影响的分钟（从该批次起点到新水位）重新计算。
- 班次摘要与质量分级只统计已成熟时段；待定尾部不参与任何统计。
- 发布前整批核验，通过后由 store 在一个事务里原子替换；核验失败保留上一版。
- 版本以「水位」为粒度：同一水位重跑不新增版本；同一水位内容变化只做修订（revision+1）。

本模块只做纯计算；落库与原子性在 store.py。
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timedelta, timezone

from . import drift, gaps, grid, quality, settings, summary
from .telemetry import Dataset, Series

TZ = timezone(timedelta(hours=8))


class ReleaseError(ValueError):
    """发布参数或水位不合法。"""


class ReleaseVerificationError(ReleaseError):
    """整批核验未通过，调用方必须保留上一版。"""


# ---------------------------------------------------------------------------
# 时间与水位
# ---------------------------------------------------------------------------


def parse_time(raw) -> float:
    """接受 epoch 秒或 ISO-8601；无时区的字符串按项目约定的 +08:00 解释。"""
    if isinstance(raw, bool):
        raise ReleaseError(f"时间不是合法值：{raw!r}")
    if isinstance(raw, (int, float)):
        return float(raw)
    text = str(raw or "").strip()
    if not text:
        raise ReleaseError("时间不能为空")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ReleaseError(f"时间不是合法 ISO-8601：{raw!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    return dt.timestamp()


def iso(epoch: float | None) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=TZ).isoformat(timespec="seconds")


def deadline_for(device_id: str) -> float:
    return float(
        settings.ARRIVAL_DEADLINE_BY_DEVICE.get(device_id, settings.ARRIVAL_DEADLINE_DEFAULT_S)
    )


def deadline_watermark(handover_at: float, deadline_s: float) -> int:
    """按到点界限算成熟水位：最后一个「整桶 + 界限」不晚于交班时刻的桶。"""
    cut = handover_at - deadline_s
    return int(math.floor(cut / settings.BUCKET_SECONDS + 1e-9)) - 1


def normalize_late_batches(raw_batches) -> list[dict]:
    """把外部补报批次规整为 {start_bucket, end_bucket, arrived_at}（end 为开区间）。"""
    if not raw_batches:
        return []
    if not isinstance(raw_batches, list):
        raise ReleaseError("late_batches 必须是数组")
    out: list[dict] = []
    for idx, item in enumerate(raw_batches, start=1):
        if not isinstance(item, dict):
            raise ReleaseError(f"late_batches 第 {idx} 项必须是对象")
        try:
            if "start_bucket" in item:
                start_bucket = int(item["start_bucket"])
            else:
                start_bucket = int(parse_time(item["start"]) // settings.BUCKET_SECONDS)
            if "end_bucket" in item:
                end_bucket = int(item["end_bucket"])
            else:
                end_bucket = int(parse_time(item["end"]) // settings.BUCKET_SECONDS)
            arrived_at = parse_time(item["arrived_at"])
        except KeyError as exc:
            raise ReleaseError(f"late_batches 第 {idx} 项缺少字段 {exc.args[0]}") from None
        if end_bucket <= start_bucket:
            raise ReleaseError(f"late_batches 第 {idx} 项结束必须晚于开始")
        out.append(
            {
                "start_bucket": start_bucket,
                "end_bucket": end_bucket,
                "start_time": grid.bucket_start_iso(start_bucket),
                "end_time": grid.bucket_start_iso(end_bucket),
                "arrived_at": arrived_at,
                "arrived_time": iso(arrived_at),
            }
        )
    out.sort(key=lambda b: b["start_bucket"])
    return out


def _batch_overrides(late_batches: list[dict]) -> dict[int, float]:
    """迟到批次：整段在该时刻才到齐，段内所有点都按这个到位时刻算。"""
    overrides: dict[int, float] = {}
    for batch in late_batches:
        for bucket in range(batch["start_bucket"], batch["end_bucket"]):
            overrides[bucket] = max(overrides.get(bucket, 0.0), batch["arrived_at"])
    return overrides


def normalize_late_records(raw_records) -> list[dict]:
    """迟到记录（晚到但确实存在的原始点）：{ts, value, arrived_at}。

    记录到位（arrived_at ≤ 交班时刻）才参与水位与重算；未到位只登记并压住水位。
    记录只代表它自己那个点，不影响同桶其它原始点。
    """
    if not raw_records:
        return []
    if not isinstance(raw_records, list):
        raise ReleaseError("late_records 必须是数组")
    out: list[dict] = []
    for idx, item in enumerate(raw_records, start=1):
        if not isinstance(item, dict):
            raise ReleaseError(f"late_records 第 {idx} 项必须是对象")
        if "ts" not in item or "value" not in item or "arrived_at" not in item:
            raise ReleaseError(f"late_records 第 {idx} 项需要 ts / value / arrived_at")
        ts = parse_time(item["ts"])
        try:
            value = float(item["value"])
        except (TypeError, ValueError):
            raise ReleaseError(f"late_records 第 {idx} 项 value 不是数字") from None
        arrived_at = parse_time(item["arrived_at"])
        bucket = grid.bucket_of(ts)
        out.append(
            {
                "ts": ts,
                "time": iso(ts),
                "value": value,
                "bucket": bucket,
                "bucket_start": grid.bucket_start_iso(bucket),
                "arrived_at": arrived_at,
                "arrived_time": iso(arrived_at),
            }
        )
    out.sort(key=lambda r: r["ts"])
    return out


def _record_arrival(record: dict, batch_overrides: dict[int, float]) -> float:
    return max(record["arrived_at"], batch_overrides.get(record["bucket"], 0.0))


def _effective_points(
    series: Series,
    late_records: list[dict],
    batch_overrides: dict[int, float],
    handover: float,
) -> tuple[list[tuple[float, float]], list[dict], list[dict]]:
    """把迟到记录并入原始序列，只保留交班前已到位的点。

    返回 (已到位的点, 已到位记录, 未到位记录)。原始点的到达时刻就是它自己的 ts，
    只有被迟到批次覆盖的段才整体后移；迟到记录按自己的 arrived_at 判断。
    """
    arrived: list[tuple[float, float]] = []
    for ts, value in series.points:
        if max(ts, batch_overrides.get(grid.bucket_of(ts), 0.0)) <= handover:
            arrived.append((ts, value))
    merged = {ts: value for ts, value in arrived}
    arrived_records: list[dict] = []
    outstanding_records: list[dict] = []
    for record in late_records:
        if _record_arrival(record, batch_overrides) <= handover:
            merged[record["ts"]] = record["value"]  # 同刻补报覆盖旧值
            arrived_records.append(record)
        else:
            outstanding_records.append(record)
    return sorted(merged.items()), arrived_records, outstanding_records


# ---------------------------------------------------------------------------
# 构建发布内容
# ---------------------------------------------------------------------------


def build_release(
    dataset: Dataset,
    device_id: str,
    metric: str,
    handover_at,
    late_batches=None,
    threshold_s: float = settings.DEFAULT_OFFSET_THRESHOLD_S,
    late_records=None,
    min_watermark: int | None = None,
) -> dict:
    """构建一次交接发布。

    min_watermark 为已发布 live 版的水位：水位只前进不回退——已经成熟并发布过的分钟
    不会因为新登记了一条「未到位」的迟到记录而重新变成待定；该记录到位后只改写那一格
    （同一水位的内容修订），而不是撤回已发布的成熟结论。
    """
    series = dataset.series_for(device_id, metric)
    if series is None:
        raise ReleaseError(f"设备 {device_id} 没有指标 {metric}")

    handover = parse_time(handover_at)
    late = normalize_late_batches(late_batches)
    records = normalize_late_records(late_records)
    batch_overrides = _batch_overrides(late)
    outstanding = [b for b in late if b["arrived_at"] > handover]

    start_bucket = int(dataset.window_start // settings.BUCKET_SECONDS)
    end_bucket = int(dataset.window_end // settings.BUCKET_SECONDS)
    deadline = deadline_for(device_id)
    watermark = deadline_watermark(handover, deadline)
    outstanding_record_buckets = [
        r["bucket"] for r in records if _record_arrival(r, batch_overrides) > handover
    ]
    blocked = [b["start_bucket"] for b in outstanding] + outstanding_record_buckets
    if blocked:
        watermark = min(watermark, min(blocked) - 1)
    watermark = max(start_bucket - 1, min(watermark, end_bucket))
    # 水位只前进：已发布成熟过的分钟不会被新登记的未到位批次「撤回」
    if min_watermark is not None:
        watermark = max(watermark, min(min_watermark, end_bucket))

    effective_points, arrived_records, outstanding_records = _effective_points(
        series, records, batch_overrides, handover
    )
    arrived_points = [
        (ts, value)
        for ts, value in effective_points
        if start_bucket <= grid.bucket_of(ts) <= end_bucket
    ]
    mature_points = [(ts, v) for ts, v in arrived_points if grid.bucket_of(ts) <= watermark]
    provisional_points = [(ts, v) for ts, v in arrived_points if grid.bucket_of(ts) > watermark]

    # 已成熟时段：正常重采样 → 缺口判定 → 有限修复
    mature_cells = (
        grid.resample(mature_points, series.interval_s, start_bucket, watermark)
        if watermark >= start_bucket
        else []
    )
    gap_result = gaps.find_gaps(mature_cells) if mature_cells else _empty_gaps()
    gaps.apply_repair(mature_cells)
    for cell in mature_cells:
        cell["mature"] = True
    _annotate_mature_tail(gap_result, mature_cells, watermark)

    # 待定尾部：只登记已到的原始值，不判定缺口、不插补、不 hold 补值
    pending = _pending_cells(
        provisional_points, series.interval_s, watermark + 1, end_bucket
    ) if watermark < end_bucket else []

    hourly = _hourly(mature_cells, pending, start_bucket, end_bucket)
    coverage = _coverage(mature_cells, gap_result, watermark, start_bucket)
    shift_summary = _shift_summary(mature_cells, gap_result, pending, watermark, start_bucket, end_bucket)

    drift_result = _drift(
        dataset, series, device_id, metric, handover, mature_points, threshold_s
    )

    release = {
        "device_id": device_id,
        "metric": metric,
        "metric_label": settings.METRIC_LABEL.get(metric, metric),
        "metric_unit": settings.METRIC_UNIT.get(metric, ""),
        "interval_s": series.interval_s,
        "threshold_s": threshold_s,
        "handover_at": iso(handover),
        "handover_epoch": handover,
        "arrival_deadline_s": deadline,
        "window": {
            "start_bucket": start_bucket,
            "end_bucket": end_bucket,
            "start": grid.bucket_start_iso(start_bucket),
            "end": grid.bucket_start_iso(end_bucket),
            "bucket_seconds": settings.BUCKET_SECONDS,
            "total_cells": end_bucket - start_bucket + 1,
        },
        "watermark": {
            "bucket": watermark,
            "time": grid.bucket_start_iso(watermark) if watermark >= start_bucket else None,
            "mature_cells": max(0, watermark - start_bucket + 1),
            "pending_cells": max(0, end_bucket - watermark),
            "floor_applied": bool(min_watermark is not None and watermark >= min_watermark and blocked),
            "rule": (
                f"设备最晚到点界限 {deadline:g}s：整桶 + 界限 ≤ 交班时刻的最晚一分钟"
                + ("；另被未到位迟到批次压在水位之下" if outstanding else "")
                + ("；有未到位迟到记录压在水位之下" if outstanding_record_buckets and not outstanding else "")
                + ("；已发布水位为下限，不回退" if min_watermark is not None and watermark >= min_watermark and blocked else "")
            ),
        },
        "late_batches": [
            {**batch, "status": "outstanding" if batch["arrived_at"] > handover else "arrived"}
            for batch in late
        ],
        "late_records": [
            {
                **record,
                "status": "outstanding" if _record_arrival(record, batch_overrides) > handover else "arrived",
            }
            for record in records
        ],
        "recomputed_span": _recomputed_span(
            [b for b in late if b["arrived_at"] <= handover],
            arrived_records,
            watermark,
            start_bucket,
            end_bucket,
        ),
        "mature_grid": mature_cells,
        "pending": pending,
        "pending_tail": _pending_tail(pending, watermark, end_bucket),
        "gaps": gap_result,
        "hourly": hourly,
        "shift_summary": shift_summary,
        "coverage": coverage,
        "drift": drift_result,
        "preview": mature_cells[:60],
    }
    release["digest"] = content_digest(release)
    return release


def _empty_gaps() -> dict:
    return {
        "items": [],
        "count": 0,
        "interpolated_cells": 0,
        "unrepaired_cells": 0,
        "boundary_cells": 0,
    }


def _annotate_mature_tail(gap_result: dict, cells: list[dict], watermark: int) -> None:
    """成熟区结尾的空缺是「到期未到」，不是待定尾部，但同样不外推。"""
    if not cells:
        return
    for item in gap_result["items"]:
        if item["next_time"] is None and item["end_bucket"] == watermark:
            item["at_watermark"] = True
            item["reason"] = (
                f"成熟水位内结尾 {item['cells']} 格无原始点（已过最晚到点界限），"
                "按「首次/水位处不外推」不填值"
            )
    gap_result["at_watermark_cells"] = sum(
        item["cells"] for item in gap_result["items"] if item.get("at_watermark")
    )


def _recomputed_span(
    late: list[dict],
    arrived_records: list[dict],
    watermark: int,
    start_bucket: int,
    end_bucket: int,
) -> dict | None:
    """迟到批次/记录到位后需要重算的分钟：从最早受影响桶到新水位。"""
    starts = [b["start_bucket"] for b in late] + [r["bucket"] for r in arrived_records]
    if not starts:
        return None
    # 迟到点会改变 hold / 插补的取值，影响会向两侧传播；实际重算范围从该校验起点前推
    # HOLD_MAX_BRACKET_BUCKETS 格，一直算到新水位。
    first = max(start_bucket, min(starts) - settings.HOLD_MAX_BRACKET_BUCKETS)
    last = min(watermark, end_bucket)
    if last < first:
        return None
    return {
        "start_bucket": first,
        "end_bucket": last,
        "start": grid.bucket_start_iso(first),
        "end": grid.bucket_start_iso(last),
        "cells": last - first + 1,
        "reason": (
            f"迟到数据到位：从 {grid.bucket_start_iso(first)} 到新水位 "
            f"{grid.bucket_start_iso(last)} 重算（含 hold / 插补向两侧传播的范围）"
        ),
    }


def _pending_cells(    points: list[tuple[float, float]], interval_s: int, start_bucket: int, end_bucket: int
) -> list[dict]:
    if end_bucket < start_bucket:
        return []
    grouped: dict[int, list[tuple[float, float]]] = {}
    for ts, value in points:
        bucket = grid.bucket_of(ts)
        if start_bucket <= bucket <= end_bucket:
            grouped.setdefault(bucket, []).append((ts, value))
    high_rate = interval_s < settings.BUCKET_SECONDS
    out: list[dict] = []
    for bucket in range(start_bucket, end_bucket + 1):
        samples = grouped.get(bucket, [])
        value = None
        if samples:
            value = (
                sum(v for _, v in samples) / len(samples)
                if high_rate
                else max(samples, key=lambda p: p[0])[1]
            )
        out.append(
            {
                "bucket": bucket,
                "bucket_start": grid.bucket_start_iso(bucket),
                "value": value,
                "samples": len(samples),
                "source": "pending",
                "mature": False,
                "provisional": True,
                "has_data": bool(samples),
                "first_ts": min((t for t, _ in samples), default=None),
                "last_ts": max((t for t, _ in samples), default=None),
            }
        )
    return out


def _pending_tail(pending: list[dict], watermark: int, end_bucket: int) -> dict:
    if not pending:
        return {
            "cells": 0,
            "start_bucket": None,
            "end_bucket": None,
            "start": None,
            "end": None,
            "value_cells": 0,
            "reason": "水位已到窗口末尾，没有待定尾部",
        }
    return {
        "cells": len(pending),
        "start_bucket": pending[0]["bucket"],
        "end_bucket": pending[-1]["bucket"],
        "start": pending[0]["bucket_start"],
        "end": pending[-1]["bucket_start"],
        "value_cells": sum(1 for c in pending if c["has_data"]),
        "reason": (
            f"水位（{grid.bucket_start_iso(watermark)}）之后 {len(pending)} 格尚未成熟，"
            "保持待定：不判缺口、不插补、不计入摘要与分级"
        ),
    }


def _hourly(
    mature_cells: list[dict], pending: list[dict], start_bucket: int, end_bucket: int
) -> list[dict]:
    pending_by_hour: dict[int, int] = {}
    for cell in pending:
        hour = cell["bucket"] // 60
        pending_by_hour[hour] = pending_by_hour.get(hour, 0) + 1
    mature_by_hour: dict[int, list[dict]] = {}
    for cell in mature_cells:
        mature_by_hour.setdefault(cell["bucket"] // 60, []).append(cell)

    out: list[dict] = []
    for hour in range(start_bucket // 60, end_bucket // 60 + 1):
        group = sorted(mature_by_hour.get(hour, []), key=lambda c: c["bucket"])
        values = [c["value"] for c in group if c["value"] is not None]
        out.append(
            {
                "hour_index": hour,
                "hour_start": grid.bucket_start_iso(hour * 60),
                "mature_cells": len(group),
                "pending_cells": pending_by_hour.get(hour, 0),
                "partial": bool(pending_by_hour.get(hour, 0)),
                "value_cells": len(values),
                "min": min(values) if values else None,
                "max": max(values) if values else None,
                "mean": sum(values) / len(values) if values else None,
                "p95": summary.percentile(values, 95),
                "gap_cells": sum(1 for c in group if c["source"] == "empty"),
                "unrepaired_cells": sum(1 for c in group if c["source"] == "empty"),
                "carried_cells": sum(1 for c in group if c["carried"]),
            }
        )
    return out


def _coverage(
    mature_cells: list[dict], gap_result: dict, watermark: int, start_bucket: int
) -> dict:
    expected = max(0, watermark - start_bucket + 1)
    if expected == 0:
        return {
            "expected_cells": 0,
            "grid_cells": 0,
            "raw_cells": 0,
            "carried_cells": 0,
            "interpolated_cells": 0,
            "missing_cells": 0,
            "valid_cells": 0,
            "coverage": None,
            "raw_coverage": None,
            "grade": "-",
            "gap_count": 0,
            "gap_cells": 0,
            "unrepaired_cells": 0,
            "mature_only": True,
        }
    report = quality.coverage_report(mature_cells, expected, gap_result)
    report["mature_only"] = True
    return report


def _shift_summary(
    mature_cells: list[dict],
    gap_result: dict,
    pending: list[dict],
    watermark: int,
    start_bucket: int,
    end_bucket: int,
) -> dict:
    values = [c["value"] for c in mature_cells if c["value"] is not None]
    return {
        "mature_start": grid.bucket_start_iso(start_bucket),
        "mature_end": grid.bucket_start_iso(watermark) if watermark >= start_bucket else None,
        "mature_cells": len(mature_cells),
        "value_cells": len(values),
        "min": min(values) if values else None,
        "max": max(values) if values else None,
        "mean": sum(values) / len(values) if values else None,
        "p95": summary.percentile(values, 95),
        "gap_count": gap_result["count"],
        "gap_cells": gap_result["interpolated_cells"] + gap_result["unrepaired_cells"],
        "interpolated_cells": gap_result["interpolated_cells"],
        "unrepaired_cells": gap_result["unrepaired_cells"],
        "carried_cells": sum(1 for c in mature_cells if c["carried"]),
        "pending_cells": len(pending),
        "window_end": grid.bucket_start_iso(end_bucket),
        "matured_only": True,
    }


def _drift(
    dataset: Dataset,
    series: Series,
    device_id: str,
    metric: str,
    handover: float,
    mature_points: list[tuple[float, float]],
    threshold_s: float,
) -> dict:
    ref_series = _reference_series(dataset, metric, device_id)
    self_reference = ref_series is not None and ref_series.device_id == device_id
    if self_reference:
        return {
            "available": False,
            "reason": "该指标最高频序列来自本设备，作为线级时钟基准，不能与自己比对",
            "threshold_s": threshold_s,
            "offset_s": None,
            "rate_ppm": None,
            "rmse_s": None,
            "anchors": 0,
            "drifted": None,
            "verdict": "本设备是该指标的时钟基准，无需漂移校正",
            "suggestion": "基准设备时间视为可信；可对其他设备执行交接以查看偏移。",
            "mature_only": True,
        }
    ref_watermark = deadline_watermark(handover, deadline_for(ref_series.device_id))
    ref_points = [
        (ts, v) for ts, v in ref_series.points if grid.bucket_of(ts) <= ref_watermark
    ]
    if len(mature_points) < 4 or len(ref_points) < 4:
        return {
            "available": False,
            "reason": "成熟时段内的点数不足，暂不出漂移结论",
            "threshold_s": threshold_s,
            "offset_s": None,
            "rate_ppm": None,
            "rmse_s": None,
            "anchors": 0,
            "drifted": None,
            "verdict": "成熟时段数据不足",
            "suggestion": "待水位前推、有更多成熟锚点后再看漂移。",
            "reference_device": ref_series.device_id,
            "mature_only": True,
        }
    try:
        result = drift.estimate_drift(ref_points, mature_points, dataset.window_start, threshold_s)
    except drift.DriftError as exc:
        return {
            "available": False,
            "reason": f"成熟时段内漂移估计不可用：{exc}",
            "threshold_s": threshold_s,
            "offset_s": None,
            "rate_ppm": None,
            "rmse_s": None,
            "anchors": 0,
            "drifted": None,
            "verdict": "成熟时段内无法估计",
            "suggestion": "待水位前推后重试。",
            "reference_device": ref_series.device_id,
            "mature_only": True,
        }
    result["available"] = True
    result["reference_device"] = ref_series.device_id
    result["reference_interval_s"] = ref_series.interval_s
    result["mature_only"] = True
    return result


def _reference_series(dataset: Dataset, metric: str, target_device: str) -> Series | None:
    candidates = [ser for (_, met), ser in dataset.series.items() if met == metric]
    if not candidates:
        return None
    candidates.sort(key=lambda s: (s.interval_s, s.device_id))
    return candidates[0]


# ---------------------------------------------------------------------------
# 内容摘要、核验
# ---------------------------------------------------------------------------


def digest_source(release: dict) -> dict:
    """参与内容摘要的字段：发布内容里除「摘要自身」和「交班/批次登记信息」以外的计算产物。

    只有成熟网格、待定尾部、缺口、小时摘要、班次摘要、覆盖率与漂移结论参与摘要：
    - 同一水位、同一内容重跑（交班时刻或批次登记时间不同）→ 摘要相同 → 复用版本；
    - 内容真的变化（补报改写了已成熟分钟）→ 摘要变化 → 同一版本 revision+1。
    """
    return {
        "device_id": release["device_id"],
        "metric": release["metric"],
        "interval_s": release["interval_s"],
        "threshold_s": release["threshold_s"],
        "arrival_deadline_s": release["arrival_deadline_s"],
        "window": release["window"],
        "watermark_bucket": release["watermark"]["bucket"],
        "mature_grid": release["mature_grid"],
        "pending": release["pending"],
        "gaps": release["gaps"],
        "hourly": release["hourly"],
        "shift_summary": release["shift_summary"],
        "coverage": release["coverage"],
        "drift": release["drift"],
    }


def content_digest(release: dict) -> str:
    blob = json.dumps(digest_source(release), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def verify_release(release: dict, dataset: Dataset) -> None:
    """整批核验：水位、成熟/待定格、样本计数、缺口范围、摘要口径与内容摘要必须自洽。"""
    device_id = release.get("device_id")
    metric = release.get("metric")
    series = dataset.series_for(device_id, metric)
    if series is None:
        raise ReleaseVerificationError(f"核验失败：数据集里没有 {device_id} / {metric}")
    window = release["window"]
    start_bucket = window["start_bucket"]
    end_bucket = window["end_bucket"]
    watermark = release["watermark"]["bucket"]
    if not (start_bucket - 1 <= watermark <= end_bucket):
        raise ReleaseVerificationError(
            f"核验失败：水位 {watermark} 越界（应为 {start_bucket - 1}..{end_bucket}）"
        )
    mature = release["mature_grid"]
    pending = release["pending"]
    expected_mature = max(0, watermark - start_bucket + 1)
    expected_pending = max(0, end_bucket - watermark)
    if len(mature) != expected_mature:
        raise ReleaseVerificationError(
            f"核验失败：成熟格数 {len(mature)} ≠ 水位推算 {expected_mature}"
        )
    if len(pending) != expected_pending:
        raise ReleaseVerificationError(
            f"核验失败：待定格数 {len(pending)} ≠ 水位推算 {expected_pending}"
        )
    for offset, cell in enumerate(mature):
        if cell["bucket"] != start_bucket + offset:
            raise ReleaseVerificationError(f"核验失败：成熟格第 {offset} 项桶号不连续")
        if not cell.get("mature"):
            raise ReleaseVerificationError(f"核验失败：成熟区出现未标记成熟的桶 {cell['bucket']}")
        if cell["source"] == "pending":
            raise ReleaseVerificationError(f"核验失败：成熟区出现待定来源桶 {cell['bucket']}")
    for offset, cell in enumerate(pending):
        if cell["bucket"] != watermark + 1 + offset:
            raise ReleaseVerificationError(f"核验失败：待定格第 {offset} 项桶号不连续")
        if cell.get("mature"):
            raise ReleaseVerificationError(f"核验失败：待定区出现成熟桶 {cell['bucket']}")
        if cell["source"] != "pending":
            raise ReleaseVerificationError(f"核验失败：待定区出现非待定来源 {cell['source']}")

    # 样本计数：成熟格内原始点数必须等于该时段内实际到齐的原始点数（含迟到补报）
    late_records = normalize_late_records(release.get("late_records"))
    batch_overrides = _batch_overrides(normalize_late_batches(release["late_batches"]))
    handover = float(release["handover_epoch"])
    effective_points, _, _ = _effective_points(series, late_records, batch_overrides, handover)
    arrived_mature = [
        (ts, v)
        for ts, v in effective_points
        if grid.bucket_of(ts) <= watermark
        and start_bucket <= grid.bucket_of(ts) <= end_bucket
    ]
    counted = sum(cell["samples"] for cell in mature)
    if counted != len(arrived_mature):
        raise ReleaseVerificationError(
            f"核验失败：成熟格原始点计数 {counted} ≠ 到齐点数 {len(arrived_mature)}"
        )

    coverage = release["coverage"]
    if coverage["expected_cells"] != expected_mature:
        raise ReleaseVerificationError("核验失败：覆盖率应到格与成熟格数不一致")
    if coverage["valid_cells"] + coverage["missing_cells"] != expected_mature:
        raise ReleaseVerificationError("核验失败：有效格 + 缺失格 ≠ 成熟格数")
    if coverage["grade"] != quality.grade_for(coverage["coverage"]):
        raise ReleaseVerificationError("核验失败：分级与覆盖率不一致")
    if release["shift_summary"]["mature_cells"] != expected_mature:
        raise ReleaseVerificationError("核验失败：班次摘要统计范围超出成熟时段")
    if release["shift_summary"]["pending_cells"] != expected_pending:
        raise ReleaseVerificationError("核验失败：班次摘要待定格数与尾部不一致")

    for item in release["gaps"]["items"]:
        if item["start_bucket"] <= watermark and item["end_bucket"] > watermark:
            raise ReleaseVerificationError("核验失败：缺口跨越成熟水位")
        if item["end_bucket"] > watermark:
            raise ReleaseVerificationError("核验失败：缺口落在待定尾部")
        span = mature[item["start_bucket"] - start_bucket : item["end_bucket"] - start_bucket + 1]
        if item["kind"] == "interpolated":
            if len(span) != item["cells"]:
                raise ReleaseVerificationError("核验失败：缺口格数与网格不一致")
            for cell in span:
                if cell["source"] != "interpolated" or cell["value"] is None:
                    raise ReleaseVerificationError("核验失败：插补缺口内存在未插补格")
        else:
            if len(span) != item["cells"]:
                raise ReleaseVerificationError("核验失败：缺口格数与网格不一致")
            for cell in span:
                if cell["value"] is not None:
                    raise ReleaseVerificationError("核验失败：未修复缺口内出现填值")

    hourly_mature = sum(hour["mature_cells"] for hour in release["hourly"])
    hourly_pending = sum(hour["pending_cells"] for hour in release["hourly"])
    if hourly_mature != expected_mature or hourly_pending != expected_pending:
        raise ReleaseVerificationError("核验失败：小时摘要覆盖范围与水位不一致")

    if content_digest(release) != release.get("digest"):
        raise ReleaseVerificationError("核验失败：内容摘要不匹配（批次内容与发布内容不一致）")


# ---------------------------------------------------------------------------
# 两次发布的差异
# ---------------------------------------------------------------------------

_DIFF_DETAIL_LIMIT = 50


def diff_releases(prev: dict | None, cur: dict) -> dict:
    """对比上一版与本次发布：水位、待定尾部、受影响分钟与统计口径。"""
    if prev is None:
        return {
            "first_release": True,
            "watermark_before": None,
            "watermark_after": cur["watermark"],
            "newly_matured_cells": len(cur["mature_grid"]),
            "changed_cells": 0,
            "changed_detail": [],
            "pending_before": None,
            "pending_after": cur["pending_tail"],
            "coverage_before": None,
            "coverage_after": cur["coverage"]["coverage"],
            "grade_before": None,
            "grade_after": cur["coverage"]["grade"],
            "summary": "首次发布，无上一版可比",
        }

    prev_cells = {c["bucket"]: c for c in prev["mature_grid"]}
    cur_cells = {c["bucket"]: c for c in cur["mature_grid"]}
    newly_matured: list[int] = []
    changed: list[dict] = []
    for bucket in sorted(cur_cells):
        now = cur_cells[bucket]
        before = prev_cells.get(bucket)
        if before is None:
            newly_matured.append(bucket)
            continue
        if (
            _value_key(before["value"]) != _value_key(now["value"])
            or before["source"] != now["source"]
            or bool(before["carried"]) != bool(now["carried"])
        ):
            changed.append(
                {
                    "bucket": bucket,
                    "bucket_start": now["bucket_start"],
                    "prev_value": before["value"],
                    "value": now["value"],
                    "prev_source": before["source"],
                    "source": now["source"],
                }
            )
    dropped = sorted(set(prev_cells) - set(cur_cells))

    # 待定尾部也会变化（例如补报点落在尾部），单独列出，避免「差异为空」掩盖内容变化
    prev_pending = {c["bucket"]: c for c in prev["pending"]}
    cur_pending = {c["bucket"]: c for c in cur["pending"]}
    changed_pending: list[dict] = []
    for bucket in sorted(cur_pending):
        now = cur_pending[bucket]
        before = prev_pending.get(bucket)
        if before is None:
            changed_pending.append(
                {
                    "bucket": bucket,
                    "bucket_start": now["bucket_start"],
                    "prev_value": None,
                    "value": now["value"],
                    "prev_samples": 0,
                    "samples": now["samples"],
                    "change": "新进入待定",
                }
            )
        elif _value_key(before["value"]) != _value_key(now["value"]) or before["samples"] != now["samples"]:
            changed_pending.append(
                {
                    "bucket": bucket,
                    "bucket_start": now["bucket_start"],
                    "prev_value": before["value"],
                    "value": now["value"],
                    "prev_samples": before["samples"],
                    "samples": now["samples"],
                    "change": "待定值变化",
                }
            )

    prev_cov = prev["coverage"]
    cur_cov = cur["coverage"]
    prev_pending_tail = prev["pending_tail"]
    cur_pending_tail = cur["pending_tail"]
    return {
        "first_release": False,
        "watermark_before": prev["watermark"],
        "watermark_after": cur["watermark"],
        "watermark_advanced_buckets": cur["watermark"]["bucket"] - prev["watermark"]["bucket"],
        "newly_matured_cells": len(newly_matured),
        "newly_matured_first": grid.bucket_start_iso(newly_matured[0]) if newly_matured else None,
        "newly_matured_last": grid.bucket_start_iso(newly_matured[-1]) if newly_matured else None,
        "changed_cells": len(changed),
        "changed_detail": changed[:_DIFF_DETAIL_LIMIT],
        "changed_detail_truncated": len(changed) > _DIFF_DETAIL_LIMIT,
        "changed_pending_cells": len(changed_pending),
        "changed_pending_detail": changed_pending[:_DIFF_DETAIL_LIMIT],
        "dropped_mature_buckets": dropped,
        "pending_before": prev_pending_tail,
        "pending_after": cur_pending_tail,
        "coverage_before": prev_cov["coverage"],
        "coverage_after": cur_cov["coverage"],
        "coverage_delta": (
            None
            if prev_cov["coverage"] is None or cur_cov["coverage"] is None
            else cur_cov["coverage"] - prev_cov["coverage"]
        ),
        "grade_before": prev_cov["grade"],
        "grade_after": cur_cov["grade"],
        "gap_count_before": prev["gaps"]["count"],
        "gap_count_after": cur["gaps"]["count"],
        "unrepaired_before": prev["gaps"]["unrepaired_cells"],
        "unrepaired_after": cur["gaps"]["unrepaired_cells"],
        "summary": _diff_summary(prev, cur, newly_matured, changed, changed_pending),
    }


def _value_key(value):
    return None if value is None else round(value, 9)


def _diff_summary(
    prev: dict, cur: dict, newly_matured: list[int], changed: list[dict], changed_pending: list[dict] | None = None
) -> str:
    parts = []
    delta = cur["watermark"]["bucket"] - prev["watermark"]["bucket"]
    if delta > 0:
        parts.append(f"水位前推 {delta} 格")
    elif delta == 0:
        parts.append("水位未变")
    else:
        parts.append(f"水位回退 {abs(delta)} 格")
    if newly_matured:
        parts.append(f"新成熟 {len(newly_matured)} 格")
    if changed:
        parts.append(f"重算改动 {len(changed)} 格")
    if changed_pending:
        parts.append(f"待定内容变化 {len(changed_pending)} 格")
    parts.append(f"待定尾部 {prev['pending_tail']['cells']} → {cur['pending_tail']['cells']} 格")
    parts.append(f"覆盖率 {_pct(prev['coverage']['coverage'])} → {_pct(cur['coverage']['coverage'])}")
    parts.append(f"分级 {prev['coverage']['grade']} → {cur['coverage']['grade']}")
    return "；".join(parts)


def _pct(value) -> str:
    return "—" if value is None else f"{value * 100:.2f}%"


# ---------------------------------------------------------------------------
# 导出
# ---------------------------------------------------------------------------

CSV_COLUMNS = [
    "device_id",
    "metric",
    "version",
    "revision",
    "prev_version",
    "prev_revision",
    "handover_at",
    "arrival_deadline_s",
    "watermark_bucket",
    "watermark_time",
    "mature_cells",
    "pending_cells",
    "pending_start",
    "pending_end",
    "raw_cells",
    "carried_cells",
    "interpolated_cells",
    "unrepaired_cells",
    "gap_count",
    "coverage",
    "grade",
    "newly_matured_cells",
    "changed_cells",
    "changed_pending_cells",
    "watermark_advanced_buckets",
    "coverage_before",
    "coverage_after",
    "grade_before",
    "grade_after",
]


def release_csv(wrapper: dict) -> str:
    """导出当前发布：版本、待定尾部与相对上一版的差异同表核对。

    wrapper 既可以是 store 的发布记录，也可以是 {publication, release, diff} 视图。
    """
    import csv
    import io

    publication = wrapper.get("publication") or wrapper
    release = wrapper["release"]
    diff = wrapper.get("diff") or publication.get("diff") or {}
    cov = release["coverage"]
    tail = release["pending_tail"]
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(CSV_COLUMNS)
    writer.writerow(
        [
            release["device_id"],
            release["metric"],
            publication["version"],
            publication["revision"],
            publication.get("prev_version", ""),
            publication.get("prev_revision", ""),
            release["handover_at"],
            f"{release['arrival_deadline_s']:g}",
            release["watermark"]["bucket"],
            release["watermark"]["time"] or "",
            release["watermark"]["mature_cells"],
            tail["cells"],
            tail["start"] or "",
            tail["end"] or "",
            cov["raw_cells"],
            cov["carried_cells"],
            cov["interpolated_cells"],
            cov["missing_cells"],
            cov["gap_count"],
            "" if cov["coverage"] is None else f"{cov['coverage']:.4f}",
            cov["grade"],
            diff.get("newly_matured_cells", ""),
            diff.get("changed_cells", ""),
            diff.get("changed_pending_cells", ""),
            diff.get("watermark_advanced_buckets", ""),
            "" if diff.get("coverage_before") is None else f"{diff['coverage_before']:.4f}",
            "" if diff.get("coverage_after") is None else f"{diff['coverage_after']:.4f}",
            diff.get("grade_before") or "",
            diff.get("grade_after") or "",
        ]
    )
    return buffer.getvalue()
