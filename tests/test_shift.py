"""班次交接：成熟水位、待定尾部、迟到推动水位与原子发布。

样例数据事实（见 sample_data/telemetry/_generate.py）：
- 窗口 2026-05-12 00:00 → 05:59（360 格，+08:00）
- 全线缺采 03:00–03:02（3 格）、04:10–04:14（5 格）
- LINE-A-03 时钟 +37s / +55ppm，60s 采样，温度 05:00–05:50 降级为 120s
"""
from __future__ import annotations

import pytest

from app import shift, store, telemetry

HANDOVER = "2026-05-12T06:00:00+08:00"
DEVICE = "LINE-A-03"
METRIC = "temperature"


@pytest.fixture(scope="module")
def dataset():
    return telemetry.load_sample_dataset()


def release(dataset, handover=HANDOVER, **kwargs):
    return shift.build_release(dataset, DEVICE, METRIC, handover, **kwargs)


# --- 水位计算 -------------------------------------------------------------


def test_deadline_watermark_uses_per_device_deadline():
    at = shift.parse_time("2026-05-12T06:00:00+08:00")
    assert shift.iso(shift.deadline_watermark(at, 300) * 60) == "2026-05-12T05:54:00+08:00"
    assert shift.iso(shift.deadline_watermark(at, 120) * 60) == "2026-05-12T05:57:00+08:00"
    # 界限恰好整除时，不把「桶尾 + 界限 == 交班」的那一格漏掉
    assert shift.iso(shift.deadline_watermark(at, 60) * 60) == "2026-05-12T05:58:00+08:00"


def test_device_deadlines_are_configured(dataset):
    assert shift.deadline_for("LINE-A-01") == 120.0
    assert shift.deadline_for("LINE-A-02") == 180.0
    assert shift.deadline_for("LINE-A-03") == 300.0
    assert shift.deadline_for("UNKNOWN") == shift.settings.ARRIVAL_DEADLINE_DEFAULT_S


# --- 待定尾部 -------------------------------------------------------------


def test_tail_after_watermark_is_pending_and_not_a_gap(dataset):
    result = release(dataset)
    assert result["watermark"]["bucket"] == shift.deadline_watermark(
        shift.parse_time(HANDOVER), 300
    )
    assert result["pending_tail"]["cells"] == 5
    assert result["pending_tail"]["start"] == "2026-05-12T05:55:00+08:00"
    assert result["pending_tail"]["end"] == "2026-05-12T05:59:00+08:00"
    # 尾部不参与缺口判定
    for item in result["gaps"]["items"]:
        assert item["end_bucket"] <= result["watermark"]["bucket"]
    # 待定区不插补、不 hold 补值；只登记已到的原始值
    for cell in result["pending"]:
        assert cell["source"] == "pending"
        assert cell["mature"] is False
        assert cell["samples"] >= 1  # 样例尾部确实已有原始点
        assert cell["value"] is not None
    # 成熟区不含待定格
    assert len(result["mature_grid"]) == result["watermark"]["mature_cells"]


def test_pending_tail_value_never_enters_mature_statistics(dataset):
    result = release(dataset)
    pending_buckets = {c["bucket"] for c in result["pending"]}
    assert pending_buckets
    assert all(c["bucket"] not in pending_buckets for c in result["mature_grid"])
    hourly = result["hourly"]
    last = hourly[-1]
    assert last["pending_cells"] == 5
    assert last["partial"] is True
    assert last["mature_cells"] == 55
    assert result["coverage"]["expected_cells"] == result["watermark"]["mature_cells"]
    assert result["shift_summary"]["pending_cells"] == 5


def test_empty_tail_when_watermark_reaches_window_end(dataset):
    result = release(dataset, "2026-05-12T07:00:00+08:00")
    assert result["watermark"]["bucket"] == result["window"]["end_bucket"]
    assert result["pending"] == []
    assert result["pending_tail"]["cells"] == 0
    assert result["hourly"][-1]["partial"] is False


def test_no_tail_fill_before_watermark_on_late_handover(dataset):
    # 交班很早：水位回到 00:00 之前，全部窗口都是待定
    result = release(dataset, "2026-05-12T00:01:00+08:00")
    assert result["watermark"]["mature_cells"] == 0
    assert result["coverage"]["expected_cells"] == 0
    assert result["coverage"]["grade"] == "-"
    assert result["coverage"]["coverage"] is None
    assert len(result["pending"]) == 360
    assert result["gaps"]["count"] == 0


# --- 迟到批次 / 记录 -------------------------------------------------------


def test_outstanding_batch_holds_watermark_down(dataset):
    base = release(dataset)
    wm = base["watermark"]["bucket"]
    late = [{"start_bucket": wm - 9, "end_bucket": wm - 3, "arrived_at": "2026-05-12T06:20:00+08:00"}]
    held = release(dataset, late_batches=late)
    assert held["watermark"]["bucket"] == wm - 10
    assert held["late_batches"][0]["status"] == "outstanding"
    assert "迟到批次" in held["watermark"]["rule"]
    assert held["pending_tail"]["cells"] == 15


def test_batch_arrival_pushes_watermark_and_marks_recompute_span(dataset):
    base = release(dataset)
    wm = base["watermark"]["bucket"]
    arrived = [{"start_bucket": wm - 9, "end_bucket": wm - 3, "arrived_at": "2026-05-12T05:50:00+08:00"}]
    result = release(dataset, late_batches=arrived)
    assert result["watermark"]["bucket"] == wm
    assert result["late_batches"][0]["status"] == "arrived"
    span = result["recomputed_span"]
    assert span is not None
    assert span["start_bucket"] == wm - 9 - shift.settings.HOLD_MAX_BRACKET_BUCKETS
    assert span["end_bucket"] == wm


def test_late_record_fills_interpolated_gap_and_changes_cells(dataset):
    # 03:01 是全线缺采的 3 格插补窗口；补报 1 分钟原始点后插补消失、相邻格改由 hold 沿用
    early = release(dataset, "2026-05-12T04:30:00+08:00")
    assert early["gaps"]["interpolated_cells"] == 3
    record = {
        "ts": "2026-05-12T03:01:37.006+08:00",
        "value": 21.0,
        "arrived_at": "2026-05-12T04:35:00+08:00",
    }
    # 交班 05:00 时该记录已到位
    fixed = release(dataset, "2026-05-12T05:00:00+08:00", late_records=[record])
    assert fixed["late_records"][0]["status"] == "arrived"
    assert fixed["gaps"]["interpolated_cells"] == 0
    wm = fixed["watermark"]["bucket"]
    by_bucket = {c["bucket"]: c for c in fixed["mature_grid"]}
    assert by_bucket[shift.grid.bucket_of(shift.parse_time(record["ts"]))]["source"] == "sample"


def test_interpolated_gap_disappears_at_same_watermark_as_revision(dataset, db):
    """迟到记录到位但水位不变时，是「同一水位的内容修订」，不是新版本。"""
    floor = release(dataset)["watermark"]["bucket"]
    before = release(dataset, HANDOVER, min_watermark=floor)
    p1 = store.publish_shift(dataset.dataset_id, before, lambda prev: shift.diff_releases(prev, before))
    record = {
        "ts": "2026-05-12T03:01:37.006+08:00",
        "value": 21.0,
        "arrived_at": "2026-05-12T05:58:00+08:00",  # 交班前已到
    }
    after = release(dataset, HANDOVER, late_records=[record], min_watermark=floor)
    shift.verify_release(after, dataset)
    assert after["watermark"]["bucket"] == before["watermark"]["bucket"]
    assert after["gaps"]["interpolated_cells"] == 0 < before["gaps"]["interpolated_cells"]
    p2 = store.publish_shift(dataset.dataset_id, after, lambda prev: shift.diff_releases(prev, after))
    assert (p2["version"], p2["revision"]) == (p1["version"], 2)
    assert p2["diff"]["changed_cells"] >= 3
    assert p2["diff"]["watermark_advanced_buckets"] == 0


def test_late_batch_arrival_at_new_watermark_updates_recompute_span(dataset):
    """批次到位后，受影响分钟从批次起点（含 hold 传播）算到新水位。"""
    wm = release(dataset)["watermark"]["bucket"]
    arrived = [
        {"start_bucket": wm - 9, "end_bucket": wm - 3, "arrived_at": "2026-05-12T05:50:00+08:00"}
    ]
    result = release(dataset, HANDOVER, late_batches=arrived)
    span = result["recomputed_span"]
    assert span["start_bucket"] == wm - 9 - shift.settings.HOLD_MAX_BRACKET_BUCKETS
    assert span["end_bucket"] == wm
    assert result["late_batches"][0]["status"] == "arrived"
    shift.verify_release(result, dataset)


def test_outstanding_record_holds_watermark_below_its_bucket(dataset):
    record = {
        "ts": "2026-05-12T03:01:37.006+08:00",
        "value": 21.0,
        "arrived_at": "2026-05-12T04:35:00+08:00",
    }
    held = release(dataset, "2026-05-12T04:30:00+08:00", late_records=[record])
    bucket = shift.grid.bucket_of(shift.parse_time(record["ts"]))
    assert held["watermark"]["bucket"] == bucket - 1
    assert held["late_records"][0]["status"] == "outstanding"
    assert all(c["bucket"] <= bucket - 1 for c in held["mature_grid"])


def test_late_batch_validation_errors(dataset):
    with pytest.raises(shift.ReleaseError):
        shift.normalize_late_batches([{"start_bucket": 10}])
    with pytest.raises(shift.ReleaseError):
        shift.normalize_late_batches([{"start_bucket": 10, "end_bucket": 10, "arrived_at": 0}])
    with pytest.raises(shift.ReleaseError):
        shift.normalize_late_records([{"ts": "2026-05-12T00:00:00+08:00", "value": "x", "arrived_at": 0}])
    with pytest.raises(shift.ReleaseError):
        shift.normalize_late_records([{"ts": "2026-05-12T00:00:00+08:00"}])
    with pytest.raises(shift.ReleaseError):
        release(dataset, "not-a-time")


def test_watermark_never_moves_backwards_once_published(dataset):
    """已发布的成熟分钟不会因为后来登记一条「未到位」记录而被撤回成待定。"""
    first = release(dataset)
    record = {
        "ts": "2026-05-12T05:50:37.0+08:00",
        "value": 22.5,
        "arrived_at": "2026-05-12T06:05:00+08:00",  # 晚于 06:00 交班
    }
    naive = release(dataset, HANDOVER, late_records=[record])
    assert naive["watermark"]["bucket"] < first["watermark"]["bucket"]  # 没有下限时会回退

    floored = release(
        dataset, HANDOVER, late_records=[record], min_watermark=first["watermark"]["bucket"]
    )
    assert floored["watermark"]["bucket"] == first["watermark"]["bucket"]
    assert floored["watermark"]["floor_applied"] is True
    assert "已发布水位为下限" in floored["watermark"]["rule"]
    assert floored["late_records"][0]["status"] == "outstanding"
    shift.verify_release(floored, dataset)


def test_late_record_only_affects_its_own_minute(dataset):
    """迟到记录只代表自己那个原始点，不该把同桶/其它桶的原始点一起判为未到位。"""
    ts = "2026-05-12T02:00:07+08:00"  # 原始序列在该刻没有点，补报是一个新增原始点
    bucket = shift.grid.bucket_of(shift.parse_time(ts))
    published = release(dataset)
    floor = published["watermark"]["bucket"]
    arrived = release(dataset, HANDOVER, late_records=[
        {"ts": ts, "value": 99.0, "arrived_at": "2026-05-12T05:58:00+08:00"}
    ], min_watermark=floor)
    held = release(dataset, HANDOVER, late_records=[
        {"ts": ts, "value": 99.0, "arrived_at": "2026-05-12T06:05:00+08:00"}
    ], min_watermark=floor)
    arrived_by = {c["bucket"]: c for c in arrived["mature_grid"]}
    held_by = {c["bucket"]: c for c in held["mature_grid"]}
    assert arrived_by[bucket]["samples"] == held_by[bucket]["samples"] + 1
    assert arrived_by[bucket]["value"] != 99.0  # 60s 设备取桶内最后一个原始点，01:00:37 仍更大
    for other, cell in held_by.items():
        if other != bucket:
            assert cell["samples"] == arrived_by[other]["samples"]
            assert cell["value"] == arrived_by[other]["value"]
    shift.verify_release(arrived, dataset)
    shift.verify_release(held, dataset)


def test_arrived_record_rewrites_value_when_it_is_the_last_sample(dataset):
    # 与设备原始点同刻补报改值后，该桶（60s 设备取桶内最后一个点）应跟着变
    ts = "2026-05-12T02:01:37.399+08:00"
    bucket = shift.grid.bucket_of(shift.parse_time(ts))
    floor = release(dataset)["watermark"]["bucket"]
    base = release(dataset, min_watermark=floor)
    arrived = release(dataset, HANDOVER, late_records=[
        {"ts": ts, "value": 99.0, "arrived_at": "2026-05-12T05:58:00+08:00"}
    ], min_watermark=floor)
    base_cell = {c["bucket"]: c for c in base["mature_grid"]}[bucket]
    arrived_cell = {c["bucket"]: c for c in arrived["mature_grid"]}[bucket]
    assert base_cell["value"] == 23.0
    assert arrived_cell["value"] == 99.0
    assert arrived_cell["samples"] == base_cell["samples"]


# --- 核验与原子发布 -------------------------------------------------------


_TEST_DB = "shift-test"


@pytest.fixture()
def db(monkeypatch, tmp_path):
    monkeypatch.setattr(shift.settings, "DB_PATH", tmp_path / "shift.sqlite3")
    monkeypatch.setattr(shift.settings, "DATA_DIR", tmp_path)
    store.init_db()
    return tmp_path


def test_verify_rejects_watermark_tampering(dataset, db):
    result = release(dataset)
    shift.verify_release(result, dataset)  # 正常内容通过
    tampered = {
        **result,
        "watermark": {**result["watermark"], "bucket": result["watermark"]["bucket"] + 1},
    }
    with pytest.raises(shift.ReleaseVerificationError):
        shift.verify_release(tampered, dataset)


def test_verify_rejects_pending_cell_inside_mature_grid(dataset, db):
    result = release(dataset)
    tampered = {
        **result,
        "mature_grid": [
            {**result["mature_grid"][0], "source": "pending"},
            *result["mature_grid"][1:],
        ],
    }
    with pytest.raises(shift.ReleaseVerificationError):
        shift.verify_release(tampered, dataset)


def test_verify_rejects_gap_beyond_watermark(dataset, db):
    result = release(dataset)
    wm = result["watermark"]["bucket"]
    bogus_gap = {
        **result["gaps"],
        "items": [
            {
                "start_bucket": wm + 1,
                "end_bucket": wm + 2,
                "kind": "interpolated",
                "cells": 2,
                "boundary": False,
            }
        ],
    }
    with pytest.raises(shift.ReleaseVerificationError):
        shift.verify_release({**result, "gaps": bogus_gap}, dataset)


def test_publish_is_rejected_before_any_write_when_verification_fails(dataset, db, monkeypatch):
    from fastapi.testclient import TestClient

    from app.main import app

    # 让核验必然失败：篡改 build_release 的返回
    original = shift.build_release

    def broken(*args, **kwargs):
        result = original(*args, **kwargs)
        result["coverage"]["valid_cells"] += 1
        return result

    with TestClient(app) as client:
        client.post("/api/import")
        good = client.post(
            "/api/shift/publish",
            json={"device_id": DEVICE, "metric": METRIC, "handover_at": HANDOVER},
        )
        assert good.status_code == 200
        live_before = store.get_live_shift(dataset.dataset_id, DEVICE, METRIC)
        monkeypatch.setattr(shift, "build_release", broken)
        bad = client.post(
            "/api/shift/publish",
            json={"device_id": DEVICE, "metric": METRIC, "handover_at": "2026-05-12T06:30:00+08:00"},
        )
        assert bad.status_code == 422
        live_after = store.get_live_shift(dataset.dataset_id, DEVICE, METRIC)
        assert live_after["publication_id"] == live_before["publication_id"]
        assert live_after["release"]["digest"] == live_before["release"]["digest"]


def test_rerun_same_watermark_does_not_add_version(dataset, db):
    first = release(dataset)
    shift.verify_release(first, dataset)
    p1 = store.publish_shift(dataset.dataset_id, first, lambda prev: shift.diff_releases(prev, first))
    assert p1["version"] == 1 and p1["revision"] == 1 and p1["reused"] is False

    again = release(dataset)  # 同一交接时刻、同一内容
    p2 = store.publish_shift(dataset.dataset_id, again, lambda prev: shift.diff_releases(prev, again))
    assert p2["reused"] is True
    assert (p2["version"], p2["revision"]) == (1, 1)
    assert len(store.list_shift_publications(dataset.dataset_id, DEVICE, METRIC, 50)) == 1


def test_reuse_of_older_version_makes_it_live_without_new_version(dataset, db):
    first = release(dataset)
    store.publish_shift(dataset.dataset_id, first, lambda prev: shift.diff_releases(prev, first))
    later = release(dataset, "2026-05-12T06:10:00+08:00")
    p2 = store.publish_shift(dataset.dataset_id, later, lambda prev: shift.diff_releases(prev, later))
    # 回到旧水位、旧内容：不新增版本，但把 v1 切回 live
    p3 = store.publish_shift(dataset.dataset_id, first, lambda prev: shift.diff_releases(prev, first))
    assert p3["reused"] is True
    assert (p3["version"], p3["revision"]) == (1, 1)
    assert p3["replaced"] is True
    live = store.get_live_shift(dataset.dataset_id, DEVICE, METRIC)
    assert live["version"] == 1
    assert p2["version"] == 2
    assert len(store.list_shift_publications(dataset.dataset_id, DEVICE, METRIC, 50)) == 2


def test_same_watermark_with_new_content_bumps_revision_only(dataset, db):
    first = release(dataset)
    p1 = store.publish_shift(dataset.dataset_id, first, lambda prev: shift.diff_releases(prev, first))
    revised = release(dataset, threshold_s=0.001)
    shift.verify_release(revised, dataset)
    p2 = store.publish_shift(dataset.dataset_id, revised, lambda prev: shift.diff_releases(prev, revised))
    assert (p2["version"], p2["revision"]) == (p1["version"], 2)
    assert p2["prev_version"] == p1["version"] and p2["prev_revision"] == 1


def test_new_watermark_creates_next_version_and_atomic_swap(dataset, db):
    first = release(dataset, "2026-05-12T06:00:00+08:00")
    p1 = store.publish_shift(dataset.dataset_id, first, lambda prev: shift.diff_releases(prev, first))
    later = release(dataset, "2026-05-12T06:10:00+08:00")
    p2 = store.publish_shift(dataset.dataset_id, later, lambda prev: shift.diff_releases(prev, later))
    assert p2["version"] == p1["version"] + 1
    live = store.get_live_shift(dataset.dataset_id, DEVICE, METRIC)
    assert live["version"] == p2["version"]
    history = store.list_shift_publications(dataset.dataset_id, DEVICE, METRIC, 10)
    assert [p["live"] for p in history].count(True) == 1
    # 差异基于真正被替换的那一版
    assert p2["diff"]["watermark_before"]["bucket"] == p1["release"]["watermark"]["bucket"]
    assert p2["diff"]["pending_after"]["cells"] == later["pending_tail"]["cells"]


def test_publish_rolls_back_if_diff_fails(dataset, db):
    first = release(dataset)
    store.publish_shift(dataset.dataset_id, first, lambda prev: shift.diff_releases(prev, first))

    def boom(prev):
        raise RuntimeError("diff exploded")

    later = release(dataset, "2026-05-12T06:10:00+08:00")
    with pytest.raises(RuntimeError):
        store.publish_shift(dataset.dataset_id, later, boom)
    live = store.get_live_shift(dataset.dataset_id, DEVICE, METRIC)
    assert live["release"]["digest"] == first["digest"]
    assert len(store.list_shift_publications(dataset.dataset_id, DEVICE, METRIC, 10)) == 1


# --- 差异与导出 -----------------------------------------------------------


def test_diff_reports_watermark_tail_and_recomputed_cells(dataset, db):
    wm = shift.deadline_watermark(shift.parse_time(HANDOVER), 300)
    record = {
        "ts": "2026-05-12T05:53:37.0+08:00",
        "value": 22.5,
        "arrived_at": "2026-05-12T06:05:00+08:00",
    }
    held = release(dataset, HANDOVER, late_records=[record])
    # 记录未到位 → 水位被压到记录所在桶之前
    record_bucket = shift.grid.bucket_of(shift.parse_time(record["ts"]))
    assert held["watermark"]["bucket"] == min(wm, record_bucket - 1)
    later = release(dataset, "2026-05-12T06:20:00+08:00", late_records=[record])
    shift.verify_release(later, dataset)
    assert later["watermark"]["bucket"] == later["window"]["end_bucket"]
    diff = shift.diff_releases(held, later)
    assert diff["watermark_advanced_buckets"] == later["watermark"]["bucket"] - held["watermark"]["bucket"]
    assert diff["watermark_advanced_buckets"] > 0
    assert diff["pending_after"]["cells"] < diff["pending_before"]["cells"]
    assert diff["newly_matured_cells"] > 0
    assert diff["summary"]


def test_csv_export_contains_version_pending_tail_and_diff(dataset, db):
    first = release(dataset)
    p1 = store.publish_shift(dataset.dataset_id, first, lambda prev: shift.diff_releases(prev, first))
    later = release(dataset, "2026-05-12T06:10:00+08:00")
    p2 = store.publish_shift(dataset.dataset_id, later, lambda prev: shift.diff_releases(prev, later))
    csv_text = shift.release_csv(p2)
    lines = [line for line in csv_text.strip().splitlines() if line]
    header = lines[0].split(",")
    assert "version" in header and "pending_cells" in header
    assert "watermark_time" in header and "newly_matured_cells" in header
    row = dict(zip(header, lines[1].split(",")))
    assert row["version"] == str(p2["version"])
    assert row["prev_version"] == str(p1["version"])
    assert row["pending_cells"] == str(later["pending_tail"]["cells"])
    assert row["watermark_time"] == later["watermark"]["time"]
    assert row["grade_before"] == p1["release"]["coverage"]["grade"]
    assert row["grade_after"] == later["coverage"]["grade"]
