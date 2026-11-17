"""分区合并持久层：归属账本、封账快照、合并单与逐条转账流水。

并发模型：SQLite 写串行；m7_meta.version 为乐观版本，
合并与封账携带 expected_version，过期版本在任何写入前被拒绝（STALE_VERSION）。
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS m7_meta(lock_key INTEGER PRIMARY KEY CHECK(lock_key=1), version INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS m7_attributions(
    key TEXT PRIMARY KEY,
    etag TEXT NOT NULL,
    partition TEXT NOT NULL,
    sealed INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS m7_seals(
    partition TEXT PRIMARY KEY,
    sealed_at TEXT NOT NULL,
    base_version INTEGER NOT NULL,
    sealed_count INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS m7_merges(
    merge_id TEXT PRIMARY KEY,
    sources TEXT NOT NULL,
    target TEXT NOT NULL,
    base_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT 'OK',
    detail TEXT NOT NULL DEFAULT '',
    baseline_total INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS m7_merge_items(
    merge_id TEXT NOT NULL,
    key TEXT NOT NULL,
    etag TEXT NOT NULL,
    source_partition TEXT NOT NULL,
    target_partition TEXT NOT NULL,
    item_kind TEXT NOT NULL,
    PRIMARY KEY(merge_id, key)
);
CREATE TABLE IF NOT EXISTS m7_transfers(
    transfer_id TEXT PRIMARY KEY,
    merge_id TEXT NOT NULL,
    key TEXT NOT NULL,
    etag TEXT NOT NULL,
    from_partition TEXT NOT NULL,
    to_partition TEXT NOT NULL,
    direction TEXT NOT NULL,
    kept_sealed INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_m7_transfers_merge ON m7_transfers(merge_id);
CREATE INDEX IF NOT EXISTS idx_m7_attr_partition ON m7_attributions(partition);
"""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class MergeStore:
    def __init__(self, path: str):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript(SCHEMA)
            db.execute(
                "INSERT OR IGNORE INTO m7_meta(lock_key, version) VALUES(1, 0)"
            )

    def connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA foreign_keys=ON")
        return db

    # ---- 版本与通用读 -------------------------------------------------
    def version(self, db: sqlite3.Connection | None = None) -> int:
        own = db is None
        db = db or self.connect()
        try:
            return db.execute("SELECT version FROM m7_meta WHERE lock_key=1").fetchone()[0]
        finally:
            if own:
                db.close()

    def assert_version(self, db: sqlite3.Connection, expected: int) -> None:
        current = self.version(db)
        if expected != current:
            from app.errors import DomainError

            raise DomainError(
                "STALE_VERSION",
                f"提交版本已过期：expected={expected} current={current}",
            )

    def bump_version(self, db: sqlite3.Connection) -> int:
        db.execute("UPDATE m7_meta SET version=version+1 WHERE lock_key=1")
        return self.version(db)

    # ---- 归属账本 ------------------------------------------------------
    def get_attribution(self, db: sqlite3.Connection, key: str) -> sqlite3.Row | None:
        return db.execute(
            "SELECT key, etag, partition, sealed FROM m7_attributions WHERE key=?",
            (key,),
        ).fetchone()

    def insert_attribution(
        self, db: sqlite3.Connection, key: str, etag: str, partition: str
    ) -> None:
        db.execute(
            "INSERT INTO m7_attributions(key, etag, partition, sealed, created_at)"
            " VALUES(?, ?, ?, 0, ?)",
            (key, etag, partition, now_iso()),
        )

    def list_attributions(self) -> list[dict]:
        with self.connect() as db:
            rows = db.execute(
                "SELECT key, etag, partition, sealed FROM m7_attributions ORDER BY key"
            ).fetchall()
        return [dict(r) for r in rows]

    def active_total(self, db: sqlite3.Connection | None = None) -> int:
        own = db is None
        db = db or self.connect()
        try:
            return db.execute("SELECT COUNT(*) FROM m7_attributions").fetchone()[0]
        finally:
            if own:
                db.close()

    def partition_counts(self) -> dict[str, dict]:
        with self.connect() as db:
            rows = db.execute(
                "SELECT partition, COUNT(*) AS total,"
                " SUM(CASE WHEN sealed=1 THEN 1 ELSE 0 END) AS sealed_total"
                " FROM m7_attributions GROUP BY partition ORDER BY partition"
            ).fetchall()
        return {
            r["partition"]: {
                "current": r["total"],
                "sealed": r["sealed_total"] or 0,
                "unsealed": r["total"] - (r["sealed_total"] or 0),
            }
            for r in rows
        }

    # ---- 封账 ----------------------------------------------------------
    def is_sealed(self, db: sqlite3.Connection, partition: str) -> bool:
        row = db.execute(
            "SELECT 1 FROM m7_seals WHERE partition=?", (partition,)
        ).fetchone()
        return row is not None

    def sealed_partitions(self, db: sqlite3.Connection) -> set[str]:
        return {
            r[0]
            for r in db.execute("SELECT partition FROM m7_seals").fetchall()
        }

    def mark_sealed(self, db: sqlite3.Connection, partition: str) -> int:
        db.execute(
            "UPDATE m7_attributions SET sealed=1 WHERE partition=?", (partition,)
        )
        count = db.execute(
            "SELECT COUNT(*) FROM m7_attributions WHERE partition=?", (partition,)
        ).fetchone()[0]
        db.execute(
            "INSERT INTO m7_seals(partition, sealed_at, base_version, sealed_count)"
            " VALUES(?, ?, ?, ?)",
            (partition, now_iso(), self.version(db), count),
        )
        return count

    def list_seals(self) -> list[dict]:
        with self.connect() as db:
            rows = db.execute(
                "SELECT partition, sealed_at, base_version, sealed_count"
                " FROM m7_seals ORDER BY partition"
            ).fetchall()
        return [dict(r) for r in rows]

    # ---- 合并单与计划项 ------------------------------------------------
    def get_merge(self, merge_id: str) -> dict | None:
        with self.connect() as db:
            row = db.execute(
                "SELECT * FROM m7_merges WHERE merge_id=?", (merge_id,)
            ).fetchone()
        return dict(row) if row else None

    def create_merge(
        self,
        db: sqlite3.Connection,
        merge_id: str,
        sources: list[str],
        target: str,
        baseline_total: int,
    ) -> None:
        import json

        db.execute(
            "INSERT INTO m7_merges(merge_id, sources, target, base_version, status,"
            " reason, detail, baseline_total, created_at, updated_at)"
            " VALUES(?, ?, ?, ?, 'RUNNING', 'OK', '', ?, ?, ?)",
            (
                merge_id,
                json.dumps(sorted(sources), ensure_ascii=False),
                target,
                self.version(db),
                baseline_total,
                now_iso(),
                now_iso(),
            ),
        )

    def add_item(
        self,
        db: sqlite3.Connection,
        merge_id: str,
        key: str,
        etag: str,
        source_partition: str,
        target_partition: str,
        item_kind: str,
    ) -> None:
        db.execute(
            "INSERT INTO m7_merge_items(merge_id, key, etag, source_partition,"
            " target_partition, item_kind) VALUES(?, ?, ?, ?, ?, ?)",
            (merge_id, key, etag, source_partition, target_partition, item_kind),
        )

    def list_items(self, db: sqlite3.Connection, merge_id: str) -> list[sqlite3.Row]:
        return db.execute(
            "SELECT * FROM m7_merge_items WHERE merge_id=? ORDER BY key", (merge_id,)
        ).fetchall()

    def update_merge(
        self,
        db: sqlite3.Connection,
        merge_id: str,
        status: str,
        reason: str,
        detail: str = "",
    ) -> None:
        db.execute(
            "UPDATE m7_merges SET status=?, reason=?, detail=?, updated_at=?"
            " WHERE merge_id=?",
            (status, reason, detail, now_iso(), merge_id),
        )

    def list_merges(self) -> list[dict]:
        with self.connect() as db:
            rows = db.execute(
                "SELECT merge_id, sources, target, base_version, status, reason,"
                " detail, baseline_total, created_at, updated_at"
                " FROM m7_merges ORDER BY created_at"
            ).fetchall()
        import json

        out = []
        for r in rows:
            item = dict(r)
            item["sources"] = json.loads(item["sources"])
            out.append(item)
        return out

    # ---- 逐条转账流水（幂等键 transfer_id） ----------------------------
    def transfer_exists(self, db: sqlite3.Connection, transfer_id: str) -> bool:
        return db.execute(
            "SELECT 1 FROM m7_transfers WHERE transfer_id=?", (transfer_id,)
        ).fetchone() is not None

    def record_transfer(
        self,
        db: sqlite3.Connection,
        transfer_id: str,
        merge_id: str,
        key: str,
        etag: str,
        from_partition: str,
        to_partition: str,
        direction: str,
        kept_sealed: bool,
    ) -> None:
        db.execute(
            "INSERT INTO m7_transfers(transfer_id, merge_id, key, etag, from_partition,"
            " to_partition, direction, kept_sealed, created_at)"
            " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                transfer_id,
                merge_id,
                key,
                etag,
                from_partition,
                to_partition,
                direction,
                1 if kept_sealed else 0,
                now_iso(),
            ),
        )

    def move_attribution(
        self, db: sqlite3.Connection, key: str, target_partition: str
    ) -> None:
        # key、etag 原样保留，仅改归属分区；sealed 不随迁移改变（封账项不会进入此路径）
        db.execute(
            "UPDATE m7_attributions SET partition=? WHERE key=?",
            (target_partition, key),
        )

    def list_transfers(self, merge_id: str) -> list[dict]:
        with self.connect() as db:
            rows = db.execute(
                "SELECT transfer_id, merge_id, key, etag, from_partition,"
                " to_partition, direction, kept_sealed, created_at"
                " FROM m7_transfers WHERE merge_id=? ORDER BY created_at, key",
                (merge_id,),
            ).fetchall()
        return [dict(r) for r in rows]
