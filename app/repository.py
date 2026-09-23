"""SQLite 持久化层。

单一数据库文件同时保存事件、影响快照、机场状态投影与更正提案，所有写入都在
``BEGIN IMMEDIATE`` 事务中完成，失败时整体回滚，不会留下部分墓碑或错误汇总。

关键不变量：

* ``airport_state`` 是每个机场唯一的裁定投影：一个机场至多有一条活动事件链，
  ``head_event_id`` 指向当前链头；所有读取（事件状态、机场汇总、受影响航班）
  都从同一投影重放。
* ``projection_log`` 只追加，给出全局单调的裁定版本号；任何接受、拒绝或更正
  裁定都留痕，服务重启后仍可解释状态是如何形成的。
* ``impacts`` 快照按 ``generation`` 不可变保存；更正被批准后写入新一代快照，
  原事件与原影响快照仍然保留。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

SCHEMA = """
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
    replay_count         INTEGER NOT NULL DEFAULT 0,
    processing_state     TEXT NOT NULL DEFAULT 'processed',
    decision_json        TEXT,
    projection_version   INTEGER,
    created_at           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS impacts (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id             TEXT NOT NULL REFERENCES events(event_id),
    root_event_id        TEXT NOT NULL,
    airport_code         TEXT NOT NULL,
    flight_id            TEXT NOT NULL,
    flight_number        TEXT NOT NULL,
    affected_endpoint    TEXT NOT NULL,
    impact_status        TEXT NOT NULL,
    overlap_minutes      INTEGER,
    delay_minutes        INTEGER,
    proposed_departure   TEXT,
    proposed_arrival     TEXT,
    passenger_count      INTEGER NOT NULL,
    crosses_midnight     INTEGER NOT NULL,
    generation           INTEGER NOT NULL DEFAULT 1,
    proposal_id          TEXT,
    projection_version   INTEGER NOT NULL DEFAULT 0,
    UNIQUE(event_id, flight_id, airport_code, generation)
);

CREATE TABLE IF NOT EXISTS airport_state (
    airport_code        TEXT PRIMARY KEY,
    root_event_id       TEXT,
    head_event_id       TEXT,
    chain_state         TEXT NOT NULL,
    last_event_version  INTEGER NOT NULL DEFAULT 0,
    generation          INTEGER NOT NULL DEFAULT 1,
    projection_version  INTEGER NOT NULL DEFAULT 0,
    updated_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projection_log (
    version       INTEGER PRIMARY KEY AUTOINCREMENT,
    airport_code  TEXT NOT NULL,
    event_id      TEXT,
    kind          TEXT NOT NULL,
    detail_json   TEXT NOT NULL,
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS correction_proposals (
    request_id                  TEXT PRIMARY KEY,
    target_event_id             TEXT NOT NULL,
    airport_code                TEXT NOT NULL,
    base_projection_version     INTEGER NOT NULL,
    patch_json                  TEXT NOT NULL,
    submitted_by                TEXT NOT NULL,
    reason                      TEXT,
    status                      TEXT NOT NULL,
    changes_published_result    INTEGER NOT NULL,
    impact_delta_json           TEXT NOT NULL,
    replay_scope_json           TEXT NOT NULL,
    payload_json                TEXT NOT NULL,
    generation                  INTEGER,
    projection_version          INTEGER NOT NULL DEFAULT 0,
    reviewer_id                 TEXT,
    review_comment              TEXT,
    decided_at                  TEXT,
    replay_count                INTEGER NOT NULL DEFAULT 0,
    created_at                  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_impacts_root    ON impacts(root_event_id);
CREATE INDEX IF NOT EXISTS idx_impacts_airport ON impacts(airport_code, impact_status);
CREATE INDEX IF NOT EXISTS idx_impacts_flight  ON impacts(flight_id);
CREATE INDEX IF NOT EXISTS idx_events_airport  ON events(airport_code, event_version);
"""

# Indexes added after the first schema cut; created post-migration so they can
# reference columns that older databases only gain through ALTER TABLE. The
# impacts indexes are re-created here too because the legacy impacts table is
# rebuilt (DROP + rename), which drops the indexes that SCHEMA just made.
POST_MIGRATION_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_impacts_root    ON impacts(root_event_id);
CREATE INDEX IF NOT EXISTS idx_impacts_airport ON impacts(airport_code, impact_status);
CREATE INDEX IF NOT EXISTS idx_impacts_flight  ON impacts(flight_id);
CREATE INDEX IF NOT EXISTS idx_events_state    ON events(airport_code, processing_state);
CREATE INDEX IF NOT EXISTS idx_proposals_state ON correction_proposals(airport_code, status);
CREATE INDEX IF NOT EXISTS idx_projlog_airport ON projection_log(airport_code, version);
"""


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
            self._conn.executescript(SCHEMA)
            self._migrate_legacy_schema()
            self._conn.executescript(POST_MIGRATION_INDEXES)
            self.bootstrap_report = self._recover_projection()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def ping(self) -> bool:
        with self._lock:
            row = self._conn.execute("SELECT 1").fetchone()
            return row is not None and row[0] == 1

    # ------------------------------------------------------------------ #
    # Legacy migration / bootstrap
    # ------------------------------------------------------------------ #

    def _migrate_legacy_schema(self) -> None:
        """Add columns introduced after the first schema cut.

        The ``impacts`` unique key gained ``generation``: legacy rows keyed on
        ``(event_id, flight_id, airport_code)`` only, which would collide when an
        approved correction writes a new generation. A legacy table is rebuilt
        with the current shape, preserving every existing snapshot row.
        """
        events_cols = {
            r["name"] for r in self._conn.execute("PRAGMA table_info(events)")
        }
        for column, decl in (
            ("processing_state", "TEXT NOT NULL DEFAULT 'processed'"),
            ("decision_json", "TEXT"),
            ("projection_version", "INTEGER"),
        ):
            if column not in events_cols:
                self._conn.execute(f"ALTER TABLE events ADD COLUMN {column} {decl}")

        impact_cols = {
            r["name"] for r in self._conn.execute("PRAGMA table_info(impacts)")
        }
        if "generation" not in impact_cols:
            self._rebuild_legacy_impacts()

    def _rebuild_legacy_impacts(self) -> None:
        self._conn.execute("PRAGMA foreign_keys=OFF")
        try:
            self._conn.executescript(
                """
                CREATE TABLE impacts_new (
                    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id             TEXT NOT NULL REFERENCES events(event_id),
                    root_event_id        TEXT NOT NULL,
                    airport_code         TEXT NOT NULL,
                    flight_id            TEXT NOT NULL,
                    flight_number        TEXT NOT NULL,
                    affected_endpoint    TEXT NOT NULL,
                    impact_status        TEXT NOT NULL,
                    overlap_minutes      INTEGER,
                    delay_minutes        INTEGER,
                    proposed_departure   TEXT,
                    proposed_arrival     TEXT,
                    passenger_count      INTEGER NOT NULL,
                    crosses_midnight     INTEGER NOT NULL,
                    generation           INTEGER NOT NULL DEFAULT 1,
                    proposal_id          TEXT,
                    projection_version   INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(event_id, flight_id, airport_code, generation)
                );

                INSERT INTO impacts_new (
                    event_id, root_event_id, airport_code, flight_id, flight_number,
                    affected_endpoint, impact_status, overlap_minutes, delay_minutes,
                    proposed_departure, proposed_arrival, passenger_count,
                    crosses_midnight, generation, proposal_id, projection_version)
                SELECT event_id, root_event_id, airport_code, flight_id, flight_number,
                       affected_endpoint, impact_status, overlap_minutes, delay_minutes,
                       proposed_departure, proposed_arrival, passenger_count,
                       crosses_midnight, 1, NULL, 0
                FROM impacts;

                DROP TABLE impacts;
                ALTER TABLE impacts_new RENAME TO impacts;
                """
            )
        finally:
            self._conn.execute("PRAGMA foreign_keys=ON")

    def _recover_projection(self) -> dict[str, Any]:
        """Rebuild airport_state rows missing after a restart and verify heads.

        Accepted events are the only chain participants. The projection is a
        pure function of their ordering, so rebuilding is deterministic and
        never rewrites projection history.
        """
        report: dict[str, Any] = {"rebuilt": [], "verified": 0}
        airports = [
            r["airport_code"]
            for r in self._conn.execute(
                "SELECT DISTINCT airport_code FROM events "
                "WHERE processing_state = 'processed' ORDER BY airport_code"
            )
        ]
        for airport in airports:
            row = self._conn.execute(
                "SELECT * FROM airport_state WHERE airport_code = ?", (airport,)
            ).fetchone()
            if row is None:
                self._rebuild_airport_state(airport)
                report["rebuilt"].append(airport)
            else:
                report["verified"] += 1
        return report

    def _rebuild_airport_state(self, airport: str) -> None:
        rows = self._conn.execute(
            "SELECT * FROM events WHERE airport_code = ? AND processing_state = 'processed' "
            "ORDER BY event_version",
            (airport,),
        ).fetchall()
        if not rows:
            return
        # Accepted submissions form at most one linear chain per airport; the
        # last (highest version) event is the current head.
        head = rows[-1]
        root = head
        seen: set[str] = set()
        while root["supersedes_event_id"] is not None:
            ref = self._conn.execute(
                "SELECT * FROM events WHERE event_id = ?",
                (root["supersedes_event_id"],),
            ).fetchone()
            if ref is None or ref["event_id"] in seen:
                break
            seen.add(ref["event_id"])
            root = ref
        generation = self._conn.execute(
            "SELECT COALESCE(MAX(generation), 1) AS g FROM impacts WHERE airport_code = ?",
            (airport,),
        ).fetchone()["g"]
        proj_version = self._conn.execute(
            "SELECT COALESCE(MAX(version), 0) AS v FROM projection_log WHERE airport_code = ?",
            (airport,),
        ).fetchone()["v"]
        self._conn.execute(
            """
            INSERT INTO airport_state
                (airport_code, root_event_id, head_event_id, chain_state,
                 last_event_version, generation, projection_version, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                airport,
                root["event_id"],
                head["event_id"],
                "resolved" if head["event_type"] == "airport.reopened" else "active",
                head["event_version"],
                int(generation),
                int(proj_version),
                utcnow_iso(),
            ),
        )

    # ------------------------------------------------------------------ #
    # Reads
    # ------------------------------------------------------------------ #

    def get_event_row(self, event_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()

    def get_impacts(
        self, event_id: str, *, generation: int | None = None
    ) -> list[sqlite3.Row]:
        sql = "SELECT * FROM impacts WHERE event_id = ?"
        params: list[Any] = [event_id]
        if generation is not None:
            sql += " AND generation = ?"
            params.append(generation)
        else:
            sql += " ORDER BY generation, flight_id"
        with self._lock:
            return list(self._conn.execute(sql + " ORDER BY flight_id", params))

    def latest_generation_impacts(self, event_id: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM impacts WHERE event_id = ? AND generation = ("
                    "SELECT MAX(generation) FROM impacts WHERE event_id = ?"
                    ") ORDER BY flight_id",
                    (event_id, event_id),
                )
            )

    def accepted_events_for_airport(self, conn, airport_code: str) -> list[sqlite3.Row]:
        return list(
            conn.execute(
                "SELECT * FROM events WHERE airport_code = ? "
                "AND processing_state = 'processed' "
                "ORDER BY event_version",
                (airport_code,),
            )
        )

    def events_for_airport(self, airport_code: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM events WHERE airport_code = ? "
                    "AND processing_state = 'processed' "
                    "ORDER BY event_version, effective_from",
                    (airport_code,),
                )
            )

    def list_events(
        self, *, state: str | None = None, airport: str | None = None
    ) -> list[sqlite3.Row]:
        where: list[str] = []
        params: list[Any] = []
        if state:
            where.append("processing_state = ?")
            params.append(state)
        if airport:
            where.append("airport_code = ?")
            params.append(airport)
        sql = "SELECT * FROM events"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_at, event_id"
        with self._lock:
            return list(self._conn.execute(sql, params))

    def count_replays(self, event_id: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT replay_count FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
            return int(row["replay_count"]) if row else 0

    def get_airport_state(self, conn, airport_code: str) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM airport_state WHERE airport_code = ?", (airport_code,)
        ).fetchone()

    def current_projection_version(self, conn) -> int:
        row = conn.execute("SELECT MAX(version) AS v FROM projection_log").fetchone()
        return int(row["v"] or 0)

    def projection_log(
        self, *, airport: str | None = None, limit: int = 100
    ) -> list[sqlite3.Row]:
        sql = "SELECT * FROM projection_log"
        params: list[Any] = []
        if airport:
            sql += " WHERE airport_code = ?"
            params.append(airport)
        sql += " ORDER BY version DESC LIMIT ?"
        params.append(limit)
        with self._lock:
            return list(self._conn.execute(sql, params))

    def latest_impacts(
        self,
        *,
        airport: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        """返回当前裁定投影上每个航班/机场组合的链头影响。

        已恢复开放的链，其最终窗口内的影响仍然成立；只有墓碑航班被排除。结果按
        flight_id/airport_code 稳定排序。
        """
        head_where = ["s.head_event_id IS NOT NULL"]
        head_params: list[Any] = []
        outer_where = ["i.impact_status != 'resolved'"]
        outer_params: list[Any] = []
        if airport:
            head_where.append("s.airport_code = ?")
            head_params.append(airport)
            outer_where.append("i.airport_code = ?")
            outer_params.append(airport)
        if status:
            outer_where.append("i.impact_status = ?")
            outer_params.append(status)

        sql = f"""
        SELECT i.*
        FROM impacts i
        JOIN airport_state s
          ON i.event_id = s.head_event_id
         AND i.airport_code = s.airport_code
         AND i.generation = s.generation
        WHERE {' AND '.join(head_where + outer_where)}
        ORDER BY i.flight_id, i.airport_code
        """
        with self._lock:
            return [
                dict(r)
                for r in self._conn.execute(sql, head_params + outer_params)
            ]

    def prior_chain_impact_ids(
        self, conn, root_event_id: str, generation: int
    ) -> set[str]:
        rows = conn.execute(
            "SELECT DISTINCT flight_id FROM impacts WHERE root_event_id = ? "
            "AND generation = ? AND impact_status != 'resolved'",
            (root_event_id, generation),
        ).fetchall()
        return {r["flight_id"] for r in rows}

    # ------------------------------------------------------------------ #
    # Correction proposals
    # ------------------------------------------------------------------ #

    def get_proposal(self, request_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM correction_proposals WHERE request_id = ?",
                (request_id,),
            ).fetchone()

    def list_proposals(
        self, *, status: str | None = None, airport: str | None = None
    ) -> list[sqlite3.Row]:
        where: list[str] = []
        params: list[Any] = []
        if status:
            where.append("status = ?")
            params.append(status)
        if airport:
            where.append("airport_code = ?")
            params.append(airport)
        sql = "SELECT * FROM correction_proposals"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_at, request_id"
        with self._lock:
            return list(self._conn.execute(sql, params))

    def insert_proposal(self, conn, proposal: dict[str, Any]) -> None:
        conn.execute(
            """
            INSERT INTO correction_proposals
                (request_id, target_event_id, airport_code, base_projection_version,
                 patch_json, submitted_by, reason, status, changes_published_result,
                 impact_delta_json, replay_scope_json, payload_json, generation,
                 projection_version, reviewer_id, review_comment, decided_at,
                 replay_count, created_at)
            VALUES (:request_id, :target_event_id, :airport_code, :base_projection_version,
                    :patch_json, :submitted_by, :reason, :status, :changes_published_result,
                    :impact_delta_json, :replay_scope_json, :payload_json, NULL,
                    :projection_version, NULL, NULL, NULL, 0, :created_at)
            """,
            {
                "request_id": proposal["request_id"],
                "target_event_id": proposal["target_event_id"],
                "airport_code": proposal["airport_code"],
                "base_projection_version": proposal["base_projection_version"],
                "patch_json": json.dumps(proposal["patch"], sort_keys=True),
                "submitted_by": proposal["submitted_by"],
                "reason": proposal.get("reason"),
                "status": proposal["status"],
                "changes_published_result": 1
                if proposal["changes_published_result"]
                else 0,
                "impact_delta_json": json.dumps(
                    proposal["impact_delta"], sort_keys=True
                ),
                "replay_scope_json": json.dumps(proposal["replay_scope"]),
                "payload_json": json.dumps(
                    proposal["payload"], sort_keys=True, ensure_ascii=False
                ),
                "projection_version": proposal["projection_version"],
                "created_at": utcnow_iso(),
            },
        )

    def ensure_airport_state(self, conn, airport_code: str) -> None:
        """Ensure an idle state row exists for a decision-only adjudication.

        Rejections and pending proposals append to the global projection log but
        do not move the chain, so they must not advance the airport's chain
        projection version (that version drives correction optimistic locking).
        """
        conn.execute(
            """
            INSERT INTO airport_state
                (airport_code, root_event_id, head_event_id, chain_state,
                 last_event_version, generation, projection_version, updated_at)
            VALUES (?, NULL, NULL, 'idle', 0, 1, 0, ?)
            ON CONFLICT(airport_code) DO NOTHING
            """,
            (airport_code, utcnow_iso()),
        )

    def decide_proposal(
        self,
        conn,
        *,
        request_id: str,
        status: str,
        reviewer_id: str,
        comment: str | None,
        generation: int | None,
        projection_version: int,
    ) -> None:
        conn.execute(
            """
            UPDATE correction_proposals
            SET status = ?, reviewer_id = ?, review_comment = ?,
                decided_at = ?, generation = COALESCE(?, generation),
                projection_version = ?
            WHERE request_id = ?
            """,
            (status, reviewer_id, comment, utcnow_iso(), generation,
             projection_version, request_id),
        )

    # ------------------------------------------------------------------ #
    # Writes (all callers run inside ``transaction``)
    # ------------------------------------------------------------------ #

    def transaction(self):
        return _Transaction(self._conn, self._lock)

    def snapshot(self):
        """Read-only consistent snapshot across several SELECTs."""
        return _Snapshot(self._conn, self._lock)

    def append_projection(
        self,
        conn,
        *,
        airport_code: str,
        event_id: str | None,
        kind: str,
        detail: dict[str, Any],
    ) -> int:
        cur = conn.execute(
            """
            INSERT INTO projection_log (airport_code, event_id, kind, detail_json, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (airport_code, event_id, kind, json.dumps(detail, sort_keys=True), utcnow_iso()),
        )
        return int(cur.lastrowid)

    def upsert_airport_state(
        self,
        conn,
        *,
        airport_code: str,
        root_event_id: str | None,
        head_event_id: str | None,
        chain_state: str,
        last_event_version: int,
        generation: int,
        projection_version: int,
    ) -> None:
        conn.execute(
            """
            INSERT INTO airport_state
                (airport_code, root_event_id, head_event_id, chain_state,
                 last_event_version, generation, projection_version, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(airport_code) DO UPDATE SET
                root_event_id = excluded.root_event_id,
                head_event_id = excluded.head_event_id,
                chain_state = excluded.chain_state,
                last_event_version = excluded.last_event_version,
                generation = excluded.generation,
                projection_version = excluded.projection_version,
                updated_at = excluded.updated_at
            """,
            (
                airport_code,
                root_event_id,
                head_event_id,
                chain_state,
                last_event_version,
                generation,
                projection_version,
                utcnow_iso(),
            ),
        )

    def insert_event(
        self,
        conn,
        event_dict: dict[str, Any],
        *,
        processing_state: str = "processed",
        decision: dict[str, Any] | None = None,
        projection_version: int | None = None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO events (event_id, event_version, event_type, airport_code,
                                effective_from, effective_until, reported_at,
                                supersedes_event_id, reason, payload_json,
                                replay_count, processing_state, decision_json,
                                projection_version, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
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
                processing_state,
                json.dumps(decision, sort_keys=True) if decision else None,
                projection_version,
                utcnow_iso(),
            ),
        )

    def insert_impacts(
        self,
        conn,
        impacts: Iterable[dict[str, Any]],
        *,
        generation: int = 1,
        proposal_id: str | None = None,
        projection_version: int = 0,
    ) -> None:
        rows = [
            (
                i["event_id"],
                i["root_event_id"],
                i["airport_code"],
                i["flight_id"],
                i["flight_number"],
                i["affected_endpoint"],
                i["impact_status"],
                i["overlap_minutes"],
                i["delay_minutes"],
                i["proposed_departure"],
                i["proposed_arrival"],
                i["passenger_count"],
                i["crosses_midnight"],
                generation,
                proposal_id,
                projection_version,
            )
            for i in impacts
        ]
        conn.executemany(
            """
            INSERT INTO impacts (event_id, root_event_id, airport_code, flight_id,
                                 flight_number, affected_endpoint, impact_status,
                                 overlap_minutes, delay_minutes, proposed_departure,
                                 proposed_arrival, passenger_count, crosses_midnight,
                                 generation, proposal_id, projection_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    def increment_replay(self, conn, event_id: str) -> None:
        conn.execute(
            "UPDATE events SET replay_count = replay_count + 1 WHERE event_id = ?",
            (event_id,),
        )

    def increment_proposal_replay(self, conn, request_id: str) -> None:
        conn.execute(
            "UPDATE correction_proposals SET replay_count = replay_count + 1 "
            "WHERE request_id = ?",
            (request_id,),
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


class _Snapshot:
    """只读快照：持锁期间的多次 SELECT 共享同一数据库快照。"""

    def __init__(self, conn: sqlite3.Connection, lock: threading.RLock):
        self._conn = conn
        self._lock = lock

    def __enter__(self) -> sqlite3.Connection:
        self._lock.acquire()
        self._conn.execute("BEGIN")
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            self._conn.execute("ROLLBACK")
        finally:
            self._lock.release()
