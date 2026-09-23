"""SQLite 持久化层。

单一数据库文件同时保存事件、影响快照、裁定日志与更正审核材料，因此一次
提交可以原子写入一个裁定版本下的全部数据：失败时回滚，不会留下部分墓碑
或错误汇总。数据库位于挂载卷时，WAL 模式可在容器重启后继续保留数据。

所有被采纳的写入都携带一个全库单调的裁定序号（``projection_journal.seq``，
即“裁定版本”）。事件、影响快照、拒绝记录和更正决定都挂在某个序号下，
查询可按指定版本重放，重放结果对事件查询、机场汇总和受影响航班列表一致。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

SCHEMA_EVENTS = """
CREATE TABLE IF NOT EXISTS events (
    event_id             TEXT PRIMARY KEY,
    event_version        INTEGER NOT NULL,
    event_type           TEXT NOT NULL,
    airport_code         TEXT NOT NULL,
    effective_from       TEXT NOT NULL,
    effective_until      TEXT,
    reported_at          TEXT NOT NULL,
    supersedes_event_id  TEXT,
    reason               TEXT,
    payload_json         TEXT NOT NULL,
    root_event_id        TEXT NOT NULL,
    adoption_seq         INTEGER NOT NULL UNIQUE,
    replay_count         INTEGER NOT NULL DEFAULT 0,
    created_at           TEXT NOT NULL
);
"""

SCHEMA_IMPACTS = """
CREATE TABLE IF NOT EXISTS impacts (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id           TEXT NOT NULL REFERENCES events(event_id),
    root_event_id      TEXT NOT NULL,
    airport_code       TEXT NOT NULL,
    flight_id          TEXT NOT NULL,
    flight_number      TEXT NOT NULL,
    affected_endpoint  TEXT NOT NULL,
    impact_status      TEXT NOT NULL,
    overlap_minutes    INTEGER,
    delay_minutes      INTEGER,
    proposed_departure TEXT,
    proposed_arrival   TEXT,
    passenger_count    INTEGER NOT NULL,
    crosses_midnight   INTEGER NOT NULL,
    projection_seq     INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_impacts_root    ON impacts(root_event_id);
CREATE INDEX IF NOT EXISTS idx_impacts_airport ON impacts(airport_code, impact_status, projection_seq);
CREATE INDEX IF NOT EXISTS idx_impacts_flight  ON impacts(flight_id);
CREATE INDEX IF NOT EXISTS idx_impacts_event  ON impacts(event_id, projection_seq);
CREATE INDEX IF NOT EXISTS idx_events_airport  ON events(airport_code, event_version);
CREATE UNIQUE INDEX IF NOT EXISTS uq_impacts_snapshot
    ON impacts(event_id, flight_id, airport_code, projection_seq);
"""

SCHEMA_INTAKE = """
CREATE TABLE IF NOT EXISTS intake_records (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    intake_seq    INTEGER NOT NULL UNIQUE,
    event_id      TEXT NOT NULL,
    airport_code  TEXT,
    payload_json  TEXT NOT NULL,
    reasons_json  TEXT NOT NULL,
    created_at    TEXT NOT NULL
);
"""

SCHEMA_CORRECTIONS = """
CREATE TABLE IF NOT EXISTS corrections (
    request_id            TEXT PRIMARY KEY,
    target_event_id       TEXT NOT NULL,
    base_version          INTEGER NOT NULL,
    patch_json            TEXT NOT NULL,
    payload_json          TEXT NOT NULL,
    submitted_by          TEXT NOT NULL,
    reason                TEXT,
    state                 TEXT NOT NULL,
    feasible              INTEGER NOT NULL,
    scope_json            TEXT NOT NULL,
    created_seq           INTEGER NOT NULL UNIQUE,
    decided_seq           INTEGER,
    decided_by            TEXT,
    decided_reason        TEXT,
    created_at            TEXT NOT NULL,
    decided_at            TEXT
);
CREATE INDEX IF NOT EXISTS idx_corrections_target ON corrections(target_event_id);
CREATE INDEX IF NOT EXISTS idx_corrections_state  ON corrections(state);
"""

SCHEMA_JOURNAL = """
CREATE TABLE IF NOT EXISTS projection_journal (
    seq           INTEGER PRIMARY KEY,
    kind          TEXT NOT NULL,
    airport_code  TEXT,
    ref_id        TEXT,
    detail_json   TEXT NOT NULL,
    created_at    TEXT NOT NULL
);
"""

SCHEMA_AMENDMENTS = """
CREATE TABLE IF NOT EXISTS event_amendments (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id            TEXT NOT NULL,
    event_id              TEXT NOT NULL,
    airport_code          TEXT,
    effective_from        TEXT,
    effective_until       TEXT,
    effective_until_set   INTEGER NOT NULL,
    projection_seq        INTEGER NOT NULL,
    created_at            TEXT NOT NULL,
    UNIQUE(request_id, event_id)
);
CREATE INDEX IF NOT EXISTS idx_amendments_event ON event_amendments(event_id, projection_seq);
"""

ALL_SCHEMA = (
    SCHEMA_EVENTS,
    SCHEMA_IMPACTS,
    SCHEMA_INTAKE,
    SCHEMA_CORRECTIONS,
    SCHEMA_JOURNAL,
    SCHEMA_AMENDMENTS,
)


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Repository:
    """对单一 SQLite 连接提供线程安全封装。"""

    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._migrate()
            for stmt in ALL_SCHEMA:
                self._conn.executescript(stmt)

    def _migrate(self) -> None:
        """补齐早期版本库中缺失的列与裁定数据。

        旧库中的事件没有链根与裁定序号：按接收顺序（rowid）为每个事件分配
        唯一 adoption_seq，并沿 supersedes 链回填 root_event_id；同时在裁定
        日志中补登记，使旧数据也能按版本一致重放。
        """
        event_cols = {
            r["name"] for r in self._conn.execute("PRAGMA table_info(events)").fetchall()
        }
        if event_cols and "root_event_id" not in event_cols:
            self._conn.execute(
                "ALTER TABLE events ADD COLUMN root_event_id TEXT NOT NULL DEFAULT ''"
            )
        if event_cols and "adoption_seq" not in event_cols:
            self._conn.execute(
                "ALTER TABLE events ADD COLUMN adoption_seq INTEGER NOT NULL DEFAULT 0"
            )

        if event_cols:
            needs_backfill = self._conn.execute(
                "SELECT COUNT(*) AS n FROM events WHERE adoption_seq = 0 "
                "OR root_event_id = ''"
            ).fetchone()["n"]
            if needs_backfill:
                self._backfill_adjournal()

        impact_cols = {
            r["name"] for r in self._conn.execute("PRAGMA table_info(impacts)").fetchall()
        }
        if impact_cols and "projection_seq" not in impact_cols:
            # The old UNIQUE(event_id, flight_id, airport_code) constraint would
            # forbid versioned replay rows, so the table must be rebuilt.
            self._conn.executescript(
                """
                ALTER TABLE impacts RENAME TO impacts_legacy;
                CREATE TABLE impacts (
                    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id           TEXT NOT NULL REFERENCES events(event_id),
                    root_event_id      TEXT NOT NULL,
                    airport_code       TEXT NOT NULL,
                    flight_id          TEXT NOT NULL,
                    flight_number      TEXT NOT NULL,
                    affected_endpoint  TEXT NOT NULL,
                    impact_status      TEXT NOT NULL,
                    overlap_minutes    INTEGER,
                    delay_minutes      INTEGER,
                    proposed_arrival   TEXT,
                    proposed_departure TEXT,
                    passenger_count    INTEGER NOT NULL,
                    crosses_midnight   INTEGER NOT NULL,
                    projection_seq     INTEGER NOT NULL DEFAULT 1
                );
                INSERT INTO impacts (id, event_id, root_event_id, airport_code,
                                     flight_id, flight_number, affected_endpoint,
                                     impact_status, overlap_minutes, delay_minutes,
                                     proposed_departure, proposed_arrival,
                                     passenger_count, crosses_midnight, projection_seq)
                SELECT l.id, l.event_id,
                       COALESCE(NULLIF(e.root_event_id, ''), l.root_event_id),
                       l.airport_code, l.flight_id, l.flight_number,
                       l.affected_endpoint, l.impact_status, l.overlap_minutes,
                       l.delay_minutes, l.proposed_departure, l.proposed_arrival,
                       l.passenger_count, l.crosses_midnight,
                       COALESCE(NULLIF(e.adoption_seq, 0), 1)
                FROM impacts_legacy l
                LEFT JOIN events e ON e.event_id = l.event_id;
                DROP TABLE impacts_legacy;
                """
            )

    def _backfill_adjournal(self) -> None:
        # Migration runs before the normal schema bootstrap; the journal table
        # must exist for the backfilled entries.
        self._conn.executescript(SCHEMA_JOURNAL)
        rows = self._conn.execute(
            "SELECT event_id, event_type, event_version, airport_code, "
            "supersedes_event_id FROM events ORDER BY rowid"
        ).fetchall()
        roots: dict[str, str] = {}

        def root_of(event_id: str, seen: set[str] | None = None) -> str:
            seen = seen or set()
            if event_id in roots:
                return roots[event_id]
            if event_id in seen:
                return event_id
            seen.add(event_id)
            ref = refs.get(event_id)
            if ref:
                resolved = root_of(ref, seen)
            else:
                resolved = event_id
            roots[event_id] = resolved
            return resolved

        refs = {r["event_id"]: r["supersedes_event_id"] for r in rows}
        seq = 0
        for r in rows:
            root_of(r["event_id"])
        for r in rows:
            seq += 1
            self._conn.execute(
                "UPDATE events SET adoption_seq = ?, root_event_id = ? "
                "WHERE event_id = ?",
                (seq, roots.get(r["event_id"], r["event_id"]), r["event_id"]),
            )
            self._conn.execute(
                "INSERT INTO projection_journal (seq, kind, airport_code, ref_id, "
                "detail_json, created_at) VALUES (?, 'event_adopted', ?, ?, ?, ?)",
                (
                    seq,
                    r["airport_code"],
                    r["event_id"],
                    json.dumps(
                        {"event_version": r["event_version"],
                         "event_type": r["event_type"], "backfilled": True},
                        ensure_ascii=False,
                    ),
                    utcnow_iso(),
                ),
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def ping(self) -> bool:
        with self._lock:
            row = self._conn.execute("SELECT 1").fetchone()
            return row is not None and row[0] == 1

    # ------------------------------------------------------------------ #
    # Reads
    # ------------------------------------------------------------------ #

    def get_event_row(self, event_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()

    def get_impacts(
        self, event_id: str, *, as_of_seq: int | None = None
    ) -> list[sqlite3.Row]:
        """返回事件在指定裁定版本下的最新快照行（不含更晚裁定）。"""
        where = "event_id = ?"
        params: list[Any] = [event_id]
        if as_of_seq is not None:
            where += " AND projection_seq <= ?"
            params.append(as_of_seq)
        sql = f"""
            WITH ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY flight_id, airport_code
                           ORDER BY projection_seq DESC, id DESC
                       ) AS rn
                FROM impacts
                WHERE {where}
            )
            SELECT * FROM ranked WHERE rn = 1 ORDER BY flight_id
        """
        with self._lock:
            return list(self._conn.execute(sql, params))

    def events_for_airport(
        self, airport_code: str, *, as_of_seq: int | None = None
    ) -> list[sqlite3.Row]:
        sql = (
            "SELECT * FROM events WHERE airport_code = ? "
            "ORDER BY adoption_seq, event_version"
        )
        params: list[Any] = [airport_code]
        if as_of_seq is not None:
            sql = (
                "SELECT * FROM events WHERE airport_code = ? AND adoption_seq <= ? "
                "ORDER BY adoption_seq, event_version"
            )
            params.append(as_of_seq)
        with self._lock:
            return list(self._conn.execute(sql, params))

    def chain_events(
        self, conn, root_event_id: str, *, as_of_seq: int | None = None
    ) -> list[sqlite3.Row]:
        """同一事件链（按 root_event_id）按接收顺序排列的已采纳事件。"""
        sql = (
            "SELECT * FROM events WHERE root_event_id = ? ORDER BY adoption_seq"
        )
        params: list[Any] = [root_event_id]
        if as_of_seq is not None:
            sql += " AND adoption_seq <= ?"
            params.append(as_of_seq)
        return list(conn.execute(sql, params))

    def count_replays(self, event_id: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT replay_count FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
            return int(row["replay_count"]) if row else 0

    def current_seq(self, conn=None) -> int:
        sql = "SELECT COALESCE(MAX(seq), 0) AS s FROM projection_journal"
        if conn is None:
            with self._lock:
                return int(self._conn.execute(sql).fetchone()["s"])
        return int(conn.execute(sql).fetchone()["s"])

    def journal_entries(self, *, since: int = 0, limit: int = 200) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM projection_journal WHERE seq > ? "
                "ORDER BY seq LIMIT ?",
                (since, limit),
            ).fetchall()
            return [self._journal_dict(r) for r in rows]

    @staticmethod
    def _journal_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "seq": row["seq"],
            "kind": row["kind"],
            "airport_code": row["airport_code"],
            "ref_id": row["ref_id"],
            "detail": json.loads(row["detail_json"]),
            "created_at": row["created_at"],
        }

    def rejected_intake(
        self, *, airport: str | None = None, as_of_seq: int | None = None
    ) -> list[dict[str, Any]]:
        where = []
        params: list[Any] = []
        if airport:
            where.append("airport_code = ?")
            params.append(airport)
        if as_of_seq is not None:
            where.append("intake_seq <= ?")
            params.append(as_of_seq)
        sql = "SELECT * FROM intake_records"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY intake_seq"
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [
            {
                "intake_seq": r["intake_seq"],
                "event_id": r["event_id"],
                "airport_code": r["airport_code"],
                "payload": json.loads(r["payload_json"]),
                "reasons": json.loads(r["reasons_json"]),
                "created_at": r["created_at"],
            }
            for r in rows
        ]

    def get_correction(self, request_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM corrections WHERE request_id = ?", (request_id,)
            ).fetchone()

    def list_corrections(self, *, state: str | None = None) -> list[sqlite3.Row]:
        with self._lock:
            if state:
                return list(
                    self._conn.execute(
                        "SELECT * FROM corrections WHERE state = ? ORDER BY created_seq",
                        (state,),
                    )
                )
            return list(
                self._conn.execute(
                    "SELECT * FROM corrections ORDER BY created_seq"
                )
            )

    def amendments_for_chain(
        self, conn, event_ids: list[str], *, as_of_seq: int
    ) -> dict[str, sqlite3.Row]:
        """截至指定版本，每个事件最新生效的修正案（同一事件可被多次更正）。"""
        if not event_ids:
            return {}
        placeholders = ",".join("?" for _ in event_ids)
        rows = conn.execute(
            f"SELECT * FROM event_amendments WHERE event_id IN ({placeholders}) "
            "AND projection_seq <= ? ORDER BY projection_seq",
            [*event_ids, as_of_seq],
        ).fetchall()
        latest: dict[str, sqlite3.Row] = {}
        for row in rows:
            latest[row["event_id"]] = row
        return latest

    def amendments_for_event(self, event_id: str, *, as_of_seq: int) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM event_amendments WHERE event_id = ? "
                    "AND projection_seq <= ? ORDER BY projection_seq",
                    (event_id, as_of_seq),
                )
            )

    def active_impacts_for_event(self, conn, event_id: str) -> list[sqlite3.Row]:
        """事件在当前最新版本快照中仍处于活动状态的影响行。"""
        rows = conn.execute(
            """
            SELECT i.* FROM impacts i
            JOIN (
                SELECT flight_id, airport_code, MAX(id) AS max_id
                FROM impacts WHERE event_id = ?
                GROUP BY flight_id, airport_code
            ) latest ON latest.max_id = i.id
            WHERE i.impact_status != 'resolved'
            ORDER BY i.id
            """,
            (event_id,),
        ).fetchall()
        return list(rows)

    def latest_impacts(
        self,
        *,
        airport: str | None = None,
        status: str | None = None,
        as_of_seq: int | None = None,
    ) -> list[dict[str, Any]]:
        """返回每个航班与机场组合在指定裁定版本下的最新影响。

        同一机场内，每条事件链采用该事件最新版本的快照（更正批准会写入
        更高序号的重算快照）；航班同时出现在多条链时，采用最后裁定的快照。
        `resolved` 墓碑参与排序，使恢复开放或更正后释放的航班不再出现。
        最终结果按 flight_id 稳定排序。
        """
        where = ["i.airport_code = ?"] if airport else []
        params: list[Any] = [airport] if airport else []
        if as_of_seq is not None:
            where.append("i.projection_seq <= ?")
            params.append(as_of_seq)
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        outer = ["rn = 1", "impact_status != 'resolved'"]
        outer_params: list[Any] = []
        if status:
            outer.append("impact_status = ?")
            outer_params.append(status)

        sql = f"""
        WITH ranked AS (
            SELECT i.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY i.flight_id, i.airport_code
                       ORDER BY i.projection_seq DESC,
                                i.id DESC
                   ) AS rn
            FROM impacts i
            {where_sql}
        )
        SELECT * FROM ranked
        WHERE {' AND '.join(outer)}
        ORDER BY flight_id, airport_code
        """
        with self._lock:
            return [dict(r) for r in self._conn.execute(sql, params + outer_params)]

    def prior_chain_impact_ids(self, conn, root_event_id: str) -> set[str]:
        rows = conn.execute(
            "SELECT DISTINCT flight_id, airport_code FROM impacts "
            "WHERE root_event_id = ? AND impact_status != 'resolved'",
            (root_event_id,),
        ).fetchall()
        return {(r["flight_id"], r["airport_code"]) for r in rows}

    # ------------------------------------------------------------------ #
    # Writes (all callers run inside ``transaction``)
    # ------------------------------------------------------------------ #

    def transaction(self):
        return _Transaction(self._conn, self._lock)

    def read(self):
        """持锁的只读上下文（不开启事务），用于一致版本读取。"""
        return _ReadScope(self._conn, self._lock)

    def append_journal(
        self,
        conn: sqlite3.Connection,
        kind: str,
        *,
        airport_code: str | None,
        ref_id: str | None,
        detail: dict[str, Any],
    ) -> int:
        cur = conn.execute(
            "INSERT INTO projection_journal (kind, airport_code, ref_id, "
            "detail_json, created_at) VALUES (?, ?, ?, ?, ?)",
            (kind, airport_code, ref_id, json.dumps(detail, ensure_ascii=False),
             utcnow_iso()),
        )
        return int(cur.lastrowid)

    def insert_event(
        self, conn: sqlite3.Connection, event_dict: dict[str, Any], *,
        root_event_id: str, adoption_seq: int,
    ) -> None:
        conn.execute(
            """
            INSERT INTO events (event_id, event_version, event_type, airport_code,
                                effective_from, effective_until, reported_at,
                                supersedes_event_id, reason, payload_json,
                                root_event_id, adoption_seq, replay_count, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
            """,
            (
                event_dict["event_id"],
                event_dict["event_version"],
                event_dict["event_type"],
                event_dict["airport_code"],
                event_dict["effective_from"],
                event_dict["effective_until"],
                event_dict["reported_at"],
                event_dict["supersedes_event_id"],
                event_dict["reason"],
                json.dumps(event_dict, sort_keys=True, ensure_ascii=False),
                root_event_id,
                adoption_seq,
                utcnow_iso(),
            ),
        )

    def insert_impacts(
        self, conn: sqlite3.Connection,
        impacts: Iterable[dict[str, Any]],
        projection_seq: int,
    ) -> None:
        rows = [dict(r, projection_seq=projection_seq) for r in impacts]
        conn.executemany(
            """
            INSERT INTO impacts (event_id, root_event_id, airport_code, flight_id,
                                 flight_number, affected_endpoint, impact_status,
                                 overlap_minutes, delay_minutes, proposed_departure,
                                 proposed_arrival, passenger_count, crosses_midnight,
                                 projection_seq)
            VALUES (:event_id, :root_event_id, :airport_code, :flight_id,
                    :flight_number, :affected_endpoint, :impact_status,
                    :overlap_minutes, :delay_minutes, :proposed_departure,
                    :proposed_arrival, :passenger_count, :crosses_midnight,
                    :projection_seq)
            """,
            rows,
        )

    def insert_rejection(
        self, conn: sqlite3.Connection, *, intake_seq: int, event_id: str,
        airport_code: str | None, payload: dict[str, Any], reasons: list[dict],
    ) -> None:
        conn.execute(
            "INSERT INTO intake_records (intake_seq, event_id, airport_code, "
            "payload_json, reasons_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (
                intake_seq, event_id, airport_code,
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                json.dumps(reasons, ensure_ascii=False),
                utcnow_iso(),
            ),
        )

    def insert_correction(
        self, conn: sqlite3.Connection, *, request_id: str, target_event_id: str,
        base_version: int, patch: dict[str, Any], payload: dict[str, Any],
        submitted_by: str, reason: str | None, feasible: bool,
        scope: dict[str, Any], created_seq: int,
    ) -> None:
        conn.execute(
            """
            INSERT INTO corrections (request_id, target_event_id, base_version,
                                     patch_json, payload_json, submitted_by, reason,
                                     state, feasible, scope_json, created_seq,
                                     created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending_review', ?, ?, ?, ?)
            """,
            (
                request_id, target_event_id, base_version,
                json.dumps(patch, ensure_ascii=False, sort_keys=True),
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                submitted_by, reason, 1 if feasible else 0,
                json.dumps(scope, ensure_ascii=False, sort_keys=True),
                created_seq, utcnow_iso(),
            ),
        )

    def decide_correction(
        self, conn: sqlite3.Connection, *, request_id: str, state: str,
        decided_seq: int, decided_by: str, decided_reason: str | None,
    ) -> None:
        conn.execute(
            "UPDATE corrections SET state = ?, decided_seq = ?, decided_by = ?, "
            "decided_reason = ?, decided_at = ? WHERE request_id = ?",
            (state, decided_seq, decided_by, decided_reason, utcnow_iso(),
             request_id),
        )

    def insert_amendment(
        self, conn: sqlite3.Connection, *, event_id: str, request_id: str,
        projection_seq: int, patched_event: Any,
    ) -> None:
        """登记一条已批准更正后的完整生效值（原事件与快照保持不变）。"""
        conn.execute(
            "INSERT INTO event_amendments (request_id, event_id, airport_code, "
            "effective_from, effective_until, effective_until_set, projection_seq, "
            "created_at) VALUES (?, ?, ?, ?, ?, 1, ?, ?)",
            (
                request_id,
                event_id,
                patched_event.airport_code,
                iso_or_none(patched_event.effective_from),
                iso_or_none(patched_event.effective_until),
                projection_seq,
                utcnow_iso(),
            ),
        )

    def increment_replay(self, conn: sqlite3.Connection, event_id: str) -> None:
        conn.execute(
            "UPDATE events SET replay_count = replay_count + 1 WHERE event_id = ?",
            (event_id,),
        )


class _Transaction:
    """管理 BEGIN IMMEDIATE、COMMIT 与 ROLLBACK 的事务上下文。"""

    def __init__(self, conn: sqlite3.Connection, lock: threading.RLock):
        self._conn = conn
        self._lock = lock

    def __enter__(self) -> sqlite3.Connection:
        self._lock.acquire()
        self._conn.execute("BEGIN IMMEDIATE")
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is None:
                self._conn.execute("COMMIT")
            else:
                self._conn.execute("ROLLBACK")
        finally:
            self._lock.release()


class _ReadScope:
    """持锁但不开启事务的只读上下文，保证多次读取落在同一版本上。"""

    def __init__(self, conn: sqlite3.Connection, lock: threading.RLock):
        self._conn = conn
        self._lock = lock

    def __enter__(self) -> sqlite3.Connection:
        self._lock.acquire()
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> None:
        self._lock.release()
