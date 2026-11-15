"""SQLite 访问层：数据集元信息、对齐运行结果。命名卷 tsgrid-data:/app/data。"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone

from . import settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id TEXT PRIMARY KEY,
    sha256     TEXT UNIQUE,
    source     TEXT,
    rows       INTEGER,
    files_json TEXT,
    inventory_json TEXT,
    created_at TEXT
);
CREATE TABLE IF NOT EXISTS runs (
    run_id      TEXT PRIMARY KEY,
    dataset_id  TEXT,
    device_id   TEXT,
    metric      TEXT,
    threshold_s REAL,
    created_at  TEXT,
    payload_json TEXT
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
CREATE TABLE IF NOT EXISTS shift_versions (
    version_id      TEXT PRIMARY KEY,
    dataset_id      TEXT,
    device_id       TEXT,
    metric          TEXT,
    version         INTEGER,
    watermark_bucket INTEGER,
    revision        INTEGER,
    digest          TEXT,
    created_at      TEXT,
    UNIQUE(dataset_id, device_id, metric, watermark_bucket)
);
CREATE TABLE IF NOT EXISTS shift_publications (
    publication_id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id     TEXT,
    device_id      TEXT,
    metric         TEXT,
    version_id     TEXT,
    version        INTEGER,
    revision       INTEGER,
    prev_version   INTEGER,
    prev_revision  INTEGER,
    watermark_bucket INTEGER,
    digest         TEXT,
    handover_at    TEXT,
    created_at     TEXT,
    live           INTEGER NOT NULL DEFAULT 0,
    release_json   TEXT,
    diff_json      TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS shift_publications_live
    ON shift_publications(dataset_id, device_id, metric) WHERE live = 1;
CREATE INDEX IF NOT EXISTS shift_publications_lookup
    ON shift_publications(dataset_id, device_id, metric, publication_id);
"""


def _connect() -> sqlite3.Connection:
    settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(settings.DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _connect() as conn:
        conn.executescript(SCHEMA)


def _now() -> str:
    return datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")


def upsert_dataset(dataset) -> str:
    """按内容 sha256 幂等：同一份数据重复导入返回既有 dataset_id。"""
    init_db()
    with _connect() as conn:
        row = conn.execute("SELECT dataset_id FROM datasets WHERE sha256 = ?", (dataset.sha256,)).fetchone()
        if row:
            return row["dataset_id"]
        conn.execute(
            "INSERT INTO datasets (dataset_id, sha256, source, rows, files_json, inventory_json, created_at)"
            " VALUES (?,?,?,?,?,?,?)",
            (
                dataset.dataset_id,
                dataset.sha256,
                dataset.source,
                dataset.rows,
                json.dumps(dataset.files, ensure_ascii=False),
                json.dumps(dataset.inventory(), ensure_ascii=False),
                _now(),
            ),
        )
        return dataset.dataset_id


def list_datasets() -> list[dict]:
    init_db()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT dataset_id, sha256, source, rows, files_json, inventory_json, created_at"
            " FROM datasets ORDER BY created_at"
        ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["files"] = json.loads(item.pop("files_json") or "[]")
        item["inventory"] = json.loads(item.pop("inventory_json") or "[]")
        out.append(item)
    return out


def save_run(dataset_id: str, payload: dict) -> str:
    init_db()
    run_id = "run-" + uuid.uuid4().hex[:10]
    with _connect() as conn:
        conn.execute(
            "INSERT INTO runs (run_id, dataset_id, device_id, metric, threshold_s, created_at, payload_json)"
            " VALUES (?,?,?,?,?,?,?)",
            (
                run_id,
                dataset_id,
                payload.get("device_id"),
                payload.get("metric"),
                payload.get("threshold_s"),
                _now(),
                json.dumps(payload, ensure_ascii=False),
            ),
        )
    return run_id


def get_run(run_id: str) -> dict | None:
    init_db()
    with _connect() as conn:
        row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    if not row:
        return None
    payload = json.loads(row["payload_json"])
    payload["run_id"] = row["run_id"]
    payload["created_at"] = row["created_at"]
    return payload


def list_runs(limit: int = 20) -> list[dict]:
    init_db()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT run_id, device_id, metric, threshold_s, created_at FROM runs"
            " ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_meta(key: str) -> str | None:
    init_db()
    with _connect() as conn:
        row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def set_meta(key: str, value: str) -> None:
    init_db()
    with _connect() as conn:
        conn.execute("INSERT INTO meta (key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, value))


# ---------------------------------------------------------------------------
# 班次交接发布：版本（按水位去重）、原子替换、历史
# ---------------------------------------------------------------------------


def _version_id(dataset_id: str, device_id: str, metric: str, watermark_bucket: int) -> str:
    raw = f"{dataset_id}|{device_id}|{metric}|{watermark_bucket}"
    return "ver-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def get_live_shift(dataset_id: str, device_id: str, metric: str) -> dict | None:
    init_db()
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM shift_publications WHERE dataset_id=? AND device_id=? AND metric=? AND live=1",
            (dataset_id, device_id, metric),
        ).fetchone()
    return _publication_row(row) if row else None


def get_shift_version(dataset_id: str, device_id: str, metric: str, version: int) -> dict | None:
    """按版本号取该版本的最新修订（发布历史回看用）。"""
    init_db()
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM shift_publications WHERE dataset_id=? AND device_id=? AND metric=? AND version=?"
            " ORDER BY publication_id DESC LIMIT 1",
            (dataset_id, device_id, metric, version),
        ).fetchone()
    return _publication_row(row) if row else None


def peek_shift_plan(dataset_id: str, device_id: str, metric: str, watermark_bucket: int, digest: str) -> dict:
    """只读推演「若现在发布会得到哪个版本」：用于预览，不写库。"""
    init_db()
    version_id = _version_id(dataset_id, device_id, metric, watermark_bucket)
    with _connect() as conn:
        row = conn.execute("SELECT * FROM shift_versions WHERE version_id=?", (version_id,)).fetchone()
        live_row = conn.execute(
            "SELECT * FROM shift_publications WHERE dataset_id=? AND device_id=? AND metric=? AND live=1",
            (dataset_id, device_id, metric),
        ).fetchone()
    live_version = live_row["version"] if live_row else None
    live_revision = live_row["revision"] if live_row else None
    if row and row["digest"] == digest:
        return {
            "action": "reuse",
            "version": row["version"],
            "revision": row["revision"],
            "version_id": version_id,
            "live_version": live_version,
            "live_revision": live_revision,
            "note": f"同一水位且内容一致：复用版本 v{row['version']}，不新增版本",
        }
    if row:
        return {
            "action": "revise",
            "version": row["version"],
            "revision": int(row["revision"]) + 1,
            "version_id": version_id,
            "live_version": live_version,
            "live_revision": live_revision,
            "note": f"同一水位但内容有变：版本 v{row['version']} 修订为 r{int(row['revision']) + 1}",
        }
    with _connect() as conn:
        max_row = conn.execute(
            "SELECT MAX(version) AS v FROM shift_versions WHERE dataset_id=? AND device_id=? AND metric=?",
            (dataset_id, device_id, metric),
        ).fetchone()
    version = int(max_row["v"] or 0) + 1
    return {
        "action": "new",
        "version": version,
        "revision": 1,
        "version_id": version_id,
        "live_version": live_version,
        "live_revision": live_revision,
        "note": f"水位推进：将发布新版本 v{version}",
    }


def list_shift_publications(dataset_id: str, device_id: str, metric: str, limit: int = 30) -> list[dict]:
    init_db()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM shift_publications WHERE dataset_id=? AND device_id=? AND metric=?"
            " ORDER BY publication_id DESC LIMIT ?",
            (dataset_id, device_id, metric, limit),
        ).fetchall()
    return [_publication_row(row) for row in rows]


def publish_shift(dataset_id: str, release: dict, diff_fn) -> dict:
    """在一个事务里整批发布：核验-比较-替换要么全成，要么保留上一版。

    - 同一水位、内容一致（digest 相同）→ 不新增版本，返回既有发布（reused=True）；
    - 同一水位、内容变化 → 同一版本号 revision+1（仍不新增版本）；
    - 新水位 → 版本号 +1。
    diff_fn(prev_release) 在事务内调用，保证差异基于真正被替换的那一版。
    """
    init_db()
    device_id = release["device_id"]
    metric = release["metric"]
    watermark = release["watermark"]["bucket"]
    version_id = _version_id(dataset_id, device_id, metric, watermark)
    conn = _connect()
    cursor = None
    try:
        conn.execute("BEGIN IMMEDIATE")
        live_row = conn.execute(
            "SELECT * FROM shift_publications WHERE dataset_id=? AND device_id=? AND metric=? AND live=1",
            (dataset_id, device_id, metric),
        ).fetchone()
        live = _publication_row(live_row) if live_row else None
        prev_release = live["release"] if live else None

        version_row = conn.execute(
            "SELECT * FROM shift_versions WHERE version_id=?", (version_id,)
        ).fetchone()
        now = _now()

        if version_row and version_row["digest"] == release["digest"]:
            # 同一水位、同一内容：不新增版本。若它不是当前 live（例如人工指定了旧版内容），
            # 仍在同一事务里原子切换过去，保证接口返回的就是 live 结果。
            existing = conn.execute(
                "SELECT * FROM shift_publications WHERE version_id=? ORDER BY publication_id DESC LIMIT 1",
                (version_id,),
            ).fetchone()
            if existing is None:  # 理论上不会发生；避免半截数据被当成发布
                conn.rollback()
                raise RuntimeError(f"版本 {version_id} 存在但没有任何发布记录")
            already_live = bool(existing["live"])
            if not already_live:
                conn.execute(
                    "UPDATE shift_publications SET live=0"
                    " WHERE dataset_id=? AND device_id=? AND metric=? AND live=1",
                    (dataset_id, device_id, metric),
                )
                conn.execute(
                    "UPDATE shift_publications SET live=1 WHERE publication_id=?",
                    (existing["publication_id"],),
                )
            conn.commit()
            wrapper = _publication_row(existing)
            wrapper["live"] = True
            wrapper["reused"] = True
            wrapper["replaced"] = not already_live
            wrapper["live_version"] = live["version"] if live else None
            wrapper["live_revision"] = live["revision"] if live else None
            return wrapper

        if version_row:
            version = int(version_row["version"])
            revision = int(version_row["revision"]) + 1
            conn.execute(
                "UPDATE shift_versions SET digest=?, revision=?, created_at=? WHERE version_id=?",
                (release["digest"], revision, now, version_id),
            )
        else:
            max_row = conn.execute(
                "SELECT MAX(version) AS v FROM shift_versions WHERE dataset_id=? AND device_id=? AND metric=?",
                (dataset_id, device_id, metric),
            ).fetchone()
            version = int(max_row["v"] or 0) + 1
            revision = 1
            conn.execute(
                "INSERT INTO shift_versions"
                " (version_id, dataset_id, device_id, metric, version, watermark_bucket, revision, digest, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                (version_id, dataset_id, device_id, metric, version, watermark, revision, release["digest"], now),
            )

        # 原子替换：旧 live 置 0 与新 live=1 在同一事务内
        conn.execute(
            "UPDATE shift_publications SET live=0 WHERE dataset_id=? AND device_id=? AND metric=? AND live=1",
            (dataset_id, device_id, metric),
        )
        diff = diff_fn(prev_release)
        cursor = conn.execute(
            "INSERT INTO shift_publications"
            " (dataset_id, device_id, metric, version_id, version, revision, prev_version, prev_revision,"
            "  watermark_bucket, digest, handover_at, created_at, live, release_json, diff_json)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,?,?)",
            (
                dataset_id,
                device_id,
                metric,
                version_id,
                version,
                revision,
                live["version"] if live else None,
                live["revision"] if live else None,
                watermark,
                release["digest"],
                release.get("handover_at"),
                now,
                json.dumps(release, ensure_ascii=False),
                json.dumps(diff, ensure_ascii=False),
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "publication_id": cursor.lastrowid,
        "version": version,
        "revision": revision,
        "version_id": version_id,
        "digest": release["digest"],
        "created_at": now,
        "prev_version": live["version"] if live else None,
        "prev_revision": live["revision"] if live else None,
        "release": release,
        "diff": diff,
        "reused": False,
        "replaced": True,
    }


def _publication_row(row) -> dict:
    return {
        "publication_id": row["publication_id"],
        "dataset_id": row["dataset_id"],
        "device_id": row["device_id"],
        "metric": row["metric"],
        "version_id": row["version_id"],
        "version": row["version"],
        "revision": row["revision"],
        "prev_version": row["prev_version"],
        "prev_revision": row["prev_revision"],
        "watermark_bucket": row["watermark_bucket"],
        "digest": row["digest"],
        "handover_at": row["handover_at"],
        "created_at": row["created_at"],
        "live": bool(row["live"]),
        "release": json.loads(row["release_json"]),
        "diff": json.loads(row["diff_json"]),
    }
