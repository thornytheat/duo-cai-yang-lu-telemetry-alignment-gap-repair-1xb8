"""FastAPI 路由（薄）：只做参数解析、调用 pipeline、渲染页面。"""
from __future__ import annotations

import json
import pathlib
from functools import partial
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import pipeline, quality, seed, settings, shift, store

BASE = pathlib.Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE / "templates"))
app = FastAPI(title=settings.APP_NAME)
app.mount("/static", StaticFiles(directory=str(BASE / "static")), name="static")

STATE: dict = {"dataset": None, "report_cache": {}}

# 工作台默认演示设备：样例中唯一判定时钟漂移的设备（+37s），便于一眼看到偏移结论；
# 只是模板默认选中项，可随时改选其它设备/指标，不影响任何业务规则。
DEFAULT_WORKBENCH_DEVICE = "LINE-A-03"


def get_dataset():
    if STATE["dataset"] is None:
        STATE["dataset"] = seed.ensure_seed()["dataset"]
    return STATE["dataset"]


def fmt(value, digits: int = 2) -> str:
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, (int, float)):
        return f"{value:.{digits}f}"
    return str(value)


def pct(value, digits: int = 1) -> str:
    if value is None:
        return "—"
    return f"{value * 100:.{digits}f}%"


templates.env.filters["fmt"] = fmt
templates.env.filters["pct"] = pct
templates.env.globals["app_name"] = settings.APP_NAME
templates.env.globals["metric_labels"] = settings.METRIC_LABEL
templates.env.globals["metric_units"] = settings.METRIC_UNIT


def _ctx(request: Request, **kwargs) -> dict:
    base = {"request": request, "app_name": settings.APP_NAME, "active": "", "page": ""}
    base.update(kwargs)
    return base


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "app": settings.SHORT_NAME, "service": "tsgrid-app"}


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    dataset = get_dataset()
    inventory = dataset.inventory()
    runs = store.list_runs(limit=5)
    datasets = store.list_datasets()
    # 首页统计（只做汇总，不改变任何业务判定）：复用质量报告缓存，逐条流水线结果保持一致。
    report = _report(dataset, settings.DEFAULT_OFFSET_THRESHOLD_S)
    summary = report["summary"]
    stats = {
        "device_count": len(inventory),
        "metric_count": len({m["metric"] for dev in inventory for m in dev["metrics"]}),
        "pair_count": summary["pairs"],
        "avg_coverage": summary["avg_coverage"],
        "grade_counts": summary["grade_counts"],
        "total_gaps": summary["total_gaps"],
        "drifted_pairs": summary["drifted_pairs"],
        "run_count": len(store.list_runs(limit=200)),
    }
    last_run = store.get_run(runs[0]["run_id"]) if runs else None
    return templates.TemplateResponse(
        request,
        "index.html",
        _ctx(
            request,
            page="index",
            active="home",
            inventory=inventory,
            dataset=dataset,
            datasets=datasets,
            runs=runs,
            last_run=last_run,
            stats=stats,
            bucket_count=dataset.bucket_count(),
            metrics=settings.METRICS,
            threshold=settings.DEFAULT_OFFSET_THRESHOLD_S,
            seeded=bool(datasets),
        ),
    )


@app.get("/workbench", response_class=HTMLResponse)
def workbench(request: Request):
    dataset = get_dataset()
    inventory = dataset.inventory()
    devices = dataset.devices()
    threshold = settings.DEFAULT_OFFSET_THRESHOLD_S

    # 预演目标：?device=&metric= 显式指定，否则默认演示设备；不写库、不影响接口。
    device_id = request.query_params.get("device") or ""
    if device_id not in devices:
        if DEFAULT_WORKBENCH_DEVICE in devices:
            device_id = DEFAULT_WORKBENCH_DEVICE
        else:
            device_id = devices[0] if devices else ""
    selected_metrics = dataset.metrics_for(device_id) if device_id else []
    metric = request.query_params.get("metric") or ""
    if metric not in selected_metrics:
        metric = selected_metrics[0] if selected_metrics else ""

    # 右侧预演：复用同一条 6 步流水线（pipeline.run_alignment），只展示不落库。
    preview = pipeline.run_alignment(dataset, device_id, metric, threshold) if metric else None

    return templates.TemplateResponse(
        request,
        "workbench.html",
        _ctx(
            request,
            page="workbench",
            active="workbench",
            inventory=inventory,
            dataset=dataset,
            metrics=sorted({m for (_, m) in dataset.series}),
            metric_labels=settings.METRIC_LABEL,
            threshold=threshold,
            bucket_count=dataset.bucket_count(),
            runs=store.list_runs(limit=8),
            preview=preview,
            selected_device=device_id,
            selected_metric=metric,
            selected_metrics=selected_metrics,
        ),
    )


@app.get("/result/{run_id}", response_class=HTMLResponse)
def result(request: Request, run_id: str):
    payload = store.get_run(run_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"运行 {run_id} 不存在，请回到工作台重新执行对齐")
    live_shift = store.get_live_shift(payload["dataset_id"], payload["device_id"], payload["metric"])
    return templates.TemplateResponse(
        request,
        "result.html",
        _ctx(
            request,
            page="result",
            active="workbench",
            run=payload,
            metric_label=payload.get("metric_label"),
            metric_unit=payload.get("metric_unit", ""),
            live_shift=live_shift,
        ),
    )


@app.get("/review", response_class=HTMLResponse)
def review(request: Request):
    dataset = get_dataset()
    report = _report(dataset, settings.DEFAULT_OFFSET_THRESHOLD_S)
    report_metrics = []
    seen = set()
    for row in report["rows"]:
        if row["metric"] not in seen:
            seen.add(row["metric"])
            report_metrics.append({"metric": row["metric"], "label": row["metric_label"]})
    return templates.TemplateResponse(
        request,
        "review.html",
        _ctx(
            request,
            page="review",
            active="review",
            dataset=dataset,
            report=report,
            report_metrics=report_metrics,
        ),
    )


def _report(dataset, threshold: float) -> dict:
    key = (dataset.sha256, threshold)
    cached = STATE["report_cache"].get(key)
    if cached is None:
        cached = pipeline.build_report(dataset, threshold)
        STATE["report_cache"][key] = cached
    return cached


def _parse_threshold(raw) -> float:
    if raw is None or raw == "":
        return settings.DEFAULT_OFFSET_THRESHOLD_S
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail=f"偏移阈值必须是数字，实际收到 {raw!r}") from None
    if value <= 0 or value > 3600:
        raise HTTPException(status_code=400, detail="偏移阈值需在 0–3600 秒之间")
    return value


@app.post("/api/import")
async def api_import(request: Request):
    """重新载入样例遥测（按内容 sha256 幂等，不产生重复数据集）。"""
    try:
        dataset = seed.ensure_seed()["dataset"]
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=422, detail=f"导入失败：{exc}") from exc
    STATE["dataset"] = dataset
    STATE["report_cache"].clear()
    return {
        "ok": True,
        "dataset_id": dataset.dataset_id,
        "sha256": dataset.sha256,
        "rows": dataset.rows,
        "files": dataset.files,
        "devices": dataset.inventory(),
        "window": {"cells": dataset.bucket_count()},
    }


@app.post("/api/align")
async def api_align(request: Request):
    dataset = get_dataset()
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="请求体必须是 JSON") from None
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="请求体必须是 JSON 对象")

    device_id = (body.get("device_id") or "").strip()
    metric = (body.get("metric") or "").strip()
    if not device_id:
        raise HTTPException(status_code=400, detail="缺少 device_id")
    if not metric:
        raise HTTPException(status_code=400, detail="缺少 metric")
    if device_id not in dataset.devices():
        raise HTTPException(
            status_code=400,
            detail=f"设备 {device_id} 不在当前数据集（可用：{'、'.join(dataset.devices())}）",
        )
    if metric not in dataset.metrics_for(device_id):
        raise HTTPException(
            status_code=400,
            detail=f"设备 {device_id} 没有指标 {metric}（可用：{'、'.join(dataset.metrics_for(device_id))}）",
        )
    threshold = _parse_threshold(body.get("offset_threshold"))

    try:
        payload = pipeline.run_alignment(dataset, device_id, metric, threshold)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=422, detail=f"对齐失败：{exc}") from exc

    run_id = store.save_run(dataset.dataset_id, payload)
    payload_out = dict(payload)
    payload_out["run_id"] = run_id
    return payload_out


@app.get("/api/runs/{run_id}")
def api_run(run_id: str):
    payload = store.get_run(run_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"运行 {run_id} 不存在")
    return payload


@app.get("/api/report")
def api_report(threshold: float | None = None):
    dataset = get_dataset()
    report = _report(dataset, _parse_threshold(threshold))
    return {
        "dataset_id": report["dataset_id"],
        "threshold_s": report["threshold_s"],
        "summary": report["summary"],
        "devices": report["devices"],
        "rows": [
            {
                "device_id": row["device_id"],
                "metric": row["metric"],
                "interval_s": row["interval_s"],
                "coverage": row["coverage"],
                "drift": {
                    "available": row["drift"].get("available"),
                    "offset_s": row["drift"].get("offset_s"),
                    "rate_ppm": row["drift"].get("rate_ppm"),
                    "rmse_s": row["drift"].get("rmse_s"),
                    "drifted": row["drift"].get("drifted"),
                },
                "gap_count": row["gaps"]["count"],
            }
            for row in report["rows"]
        ],
    }


@app.get("/api/report.csv", response_class=PlainTextResponse)
def api_report_csv(threshold: float | None = None):
    dataset = get_dataset()
    report = _report(dataset, _parse_threshold(threshold))
    return PlainTextResponse(
        report["csv"],
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="tsgrid-quality-{dataset.dataset_id}.csv"'},
    )


@app.get("/api/datasets")
def api_datasets():
    return {"datasets": store.list_datasets()}


# ---------------------------------------------------------------------------
# 班次交接：成熟水位 / 待定尾部 / 版本化发布
# ---------------------------------------------------------------------------


def _shift_request(body, dataset):
    """解析并校验交接参数，返回 (device, metric, handover, late_batches, late_records, threshold)。"""
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="请求体必须是 JSON 对象")
    device_id = (body.get("device_id") or "").strip()
    metric = (body.get("metric") or "").strip()
    if not device_id:
        raise HTTPException(status_code=400, detail="缺少 device_id")
    if not metric:
        raise HTTPException(status_code=400, detail="缺少 metric")
    if device_id not in dataset.devices():
        raise HTTPException(
            status_code=400,
            detail=f"设备 {device_id} 不在当前数据集（可用：{'、'.join(dataset.devices())}）",
        )
    if metric not in dataset.metrics_for(device_id):
        raise HTTPException(
            status_code=400,
            detail=f"设备 {device_id} 没有指标 {metric}（可用：{'、'.join(dataset.metrics_for(device_id))}）",
        )
    handover_raw = body.get("handover_at")
    if handover_raw is None or handover_raw == "":
        raise HTTPException(status_code=400, detail="缺少 handover_at（交班时刻，ISO-8601 或 epoch 秒）")
    try:
        handover = shift.parse_time(handover_raw)
        late = shift.normalize_late_batches(body.get("late_batches"))
        records = shift.normalize_late_records(body.get("late_records"))
    except shift.ReleaseError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    threshold = _parse_threshold(body.get("offset_threshold"))

    window = (dataset.window_start, dataset.window_end)
    if handover < window[0]:
        raise HTTPException(
            status_code=400,
            detail=f"交班时刻早于数据集窗口起点 {shift.iso(window[0])}",
        )
    return device_id, metric, handover, late, records, threshold


def _shift_publication_view(wrapper: dict) -> dict:
    release = wrapper["release"]
    return {
        "publication_id": wrapper["publication_id"],
        "version": wrapper["version"],
        "revision": wrapper["revision"],
        "version_id": wrapper["version_id"],
        "prev_version": wrapper.get("prev_version"),
        "prev_revision": wrapper.get("prev_revision"),
        "digest": wrapper["digest"],
        "handover_at": wrapper.get("handover_at"),
        "created_at": wrapper.get("created_at"),
        "live": wrapper.get("live", True),
        "watermark": release["watermark"],
        "pending_tail": release["pending_tail"],
        "coverage": release["coverage"],
        "shift_summary": release["shift_summary"],
        "diff": wrapper.get("diff"),
    }


def _shift_payload(dataset, device_id, metric, handover, late, threshold, records=None) -> dict:
    # 水位只前进：已有 live 版时，以它的水位为下限
    live = store.get_live_shift(dataset.dataset_id, device_id, metric)
    floor = live["release"]["watermark"]["bucket"] if live else None
    release = shift.build_release(
        dataset, device_id, metric, handover, late, threshold, records, min_watermark=floor
    )
    plan = store.peek_shift_plan(dataset.dataset_id, device_id, metric, release["watermark"]["bucket"], release["digest"])
    diff = shift.diff_releases(live["release"] if live else None, release)
    return release, plan, live, diff


@app.post("/api/shift/preview")
async def api_shift_preview(request: Request):
    """只算不发布：给出水位、待定尾部、若发布会落到的版本，以及与当前 live 版的差异。"""
    dataset = get_dataset()
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="请求体必须是 JSON") from None
    device_id, metric, handover, late, records, threshold = _shift_request(body, dataset)
    release, plan, live, diff = _shift_payload(dataset, device_id, metric, handover, late, threshold, records)
    try:
        shift.verify_release(release, dataset)
    except shift.ReleaseVerificationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {
        "dataset_id": dataset.dataset_id,
        "published": False,
        "plan": plan,
        "live": _shift_publication_view(live) if live else None,
        "diff": diff,
        "release": release,
    }


@app.post("/api/shift/publish")
async def api_shift_publish(request: Request):
    """整批核验 → 原子替换。核验失败返回 422 且上一版保持 live。"""
    dataset = get_dataset()
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="请求体必须是 JSON") from None
    device_id, metric, handover, late, records, threshold = _shift_request(body, dataset)
    try:
        live = store.get_live_shift(dataset.dataset_id, device_id, metric)
        floor = live["release"]["watermark"]["bucket"] if live else None
        release = shift.build_release(
            dataset, device_id, metric, handover, late, threshold, records, min_watermark=floor
        )
        shift.verify_release(release, dataset)
    except shift.ReleaseVerificationError as exc:
        # 核验失败：不触碰数据库，上一版继续 live
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except shift.ReleaseError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    wrapper = store.publish_shift(
        dataset.dataset_id, release, partial(shift.diff_releases, cur=release)
    )
    return {
        "dataset_id": dataset.dataset_id,
        "published": True,
        "reused": wrapper.get("reused", False),
        "replaced": wrapper.get("replaced", True),
        "publication": _shift_publication_view(wrapper),
        "diff": wrapper.get("diff"),
        "release": wrapper["release"],
    }


@app.get("/api/shift/live")
def api_shift_live(device_id: str, metric: str):
    dataset = get_dataset()
    if device_id not in dataset.devices() or metric not in dataset.metrics_for(device_id):
        raise HTTPException(status_code=400, detail=f"数据集里没有 {device_id} / {metric}")
    live = store.get_live_shift(dataset.dataset_id, device_id, metric)
    if live is None:
        raise HTTPException(status_code=404, detail=f"{device_id} / {metric} 还没有已发布的交接版本")
    return {
        "dataset_id": dataset.dataset_id,
        "publication": _shift_publication_view(live),
        "release": live["release"],
        "diff": live["diff"],
    }


@app.get("/api/shift/versions")
def api_shift_versions(device_id: str, metric: str, limit: int = 20):
    dataset = get_dataset()
    if device_id not in dataset.devices() or metric not in dataset.metrics_for(device_id):
        raise HTTPException(status_code=400, detail=f"数据集里没有 {device_id} / {metric}")
    rows = store.list_shift_publications(dataset.dataset_id, device_id, metric, max(1, min(limit, 100)))
    return {
        "dataset_id": dataset.dataset_id,
        "device_id": device_id,
        "metric": metric,
        "publications": [_shift_publication_view(row) for row in rows],
    }


@app.get("/api/shift/export.csv", response_class=PlainTextResponse)
def api_shift_export_csv(device_id: str, metric: str, version: int | None = None):
    """导出已发布的交接结果；version 省略时导出当前 live 版。"""
    dataset = get_dataset()
    if device_id not in dataset.devices() or metric not in dataset.metrics_for(device_id):
        raise HTTPException(status_code=400, detail=f"数据集里没有 {device_id} / {metric}")
    wrapper = (
        store.get_shift_version(dataset.dataset_id, device_id, metric, version)
        if version is not None
        else store.get_live_shift(dataset.dataset_id, device_id, metric)
    )
    if wrapper is None:
        label = f"v{version}" if version is not None else "已发布版本"
        raise HTTPException(status_code=404, detail=f"{device_id} / {metric} 没有{label}，请先发布交接结果")
    csv_text = shift.release_csv(wrapper)
    filename = f"tsgrid-shift-{device_id}-{metric}-v{wrapper['version']}r{wrapper['revision']}.csv"
    return PlainTextResponse(
        csv_text,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/shift", response_class=HTMLResponse)
def shift_page(
    request: Request,
    device: str = "",
    metric: str = "",
    at: str = "",
    late: str = "",
    threshold: str = "",
):
    dataset = get_dataset()
    devices = dataset.devices()
    device_id = device if device in devices else (DEFAULT_WORKBENCH_DEVICE if DEFAULT_WORKBENCH_DEVICE in devices else (devices[0] if devices else ""))
    selected_metrics = dataset.metrics_for(device_id) if device_id else []
    metric_name = metric if metric in selected_metrics else (selected_metrics[0] if selected_metrics else "")
    handover_text = at or shift.iso(dataset.window_end + 5 * 60)
    late_text = late.strip() or '{"late_batches": [], "late_records": []}'
    threshold_text = threshold.strip() or str(settings.DEFAULT_OFFSET_THRESHOLD_S)
    live = store.get_live_shift(dataset.dataset_id, device_id, metric_name) if metric_name else None
    history = (
        store.list_shift_publications(dataset.dataset_id, device_id, metric_name, 20) if metric_name else []
    )
    preview = None
    error = None
    if metric_name:
        try:
            handover = shift.parse_time(handover_text)
            try:
                threshold_s = float(threshold_text)
            except ValueError:
                raise shift.ReleaseError(f"偏移阈值必须是数字，实际收到 {threshold_text!r}") from None
            if threshold_s <= 0 or threshold_s > 3600:
                raise shift.ReleaseError("偏移阈值需在 0–3600 秒之间")
            try:
                late_body = json.loads(late_text)
            except json.JSONDecodeError as exc:
                raise shift.ReleaseError(f"补报登记不是合法 JSON：{exc}") from None
            if not isinstance(late_body, dict):
                raise shift.ReleaseError("补报登记必须是 JSON 对象（late_batches / late_records）")
            batches = late_body.get("late_batches") or []
            records = late_body.get("late_records") or []
            release, plan, _, diff = _shift_payload(
                dataset, device_id, metric_name, handover, batches, threshold_s, records
            )
            shift.verify_release(release, dataset)
            preview = {"release": release, "plan": plan, "diff": diff}
        except (shift.ReleaseError, shift.ReleaseVerificationError) as exc:
            error = str(exc)
    return templates.TemplateResponse(
        request,
        "shift.html",
        _ctx(
            request,
            page="shift",
            active="shift",
            dataset=dataset,
            inventory=dataset.inventory(),
            devices=devices,
            selected_device=device_id,
            selected_metric=metric_name,
            selected_metrics=selected_metrics,
            metric_labels=settings.METRIC_LABEL,
            handover_at=handover_text,
            late_text=late_text,
            threshold_text=threshold_text,
            deadlines={d: shift.deadline_for(d) for d in devices},
            live=live,
            history=history,
            preview=preview,
            error=error,
        ),
    )
