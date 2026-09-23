"""处理事件接入语义、持久化协调、更正复核与业务查询的应用服务。

裁定原则：

* 机场维度的 ``event_version`` 必须严格递增；链头（同一 root 事件链中最后
  被采纳的事件）结合开放时刻、来源报告时刻与接收顺序（裁定序号）共同裁定，
  任何事件都不得产生“恢复早于关闭开始”的窗口。
* 每次采纳或拒绝都占用一个全库单调的裁定版本（``projection_journal.seq``）。
  事件、影响快照、拒绝材料、更正提案与决定都挂在某个版本下，可按版本重放。
* 历史材料更正不覆盖原事件与已发布快照；会改变已发布结果的更正进入
  ``pending_review``，由具备权限的审核员裁定后再以新版本重算。
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from typing import Any

from app.errors import (
    BadRequestError,
    ConflictStateError,
    EventConflictError,
    ForbiddenReviewerError,
    NotFoundError,
    RejectedEventError,
    ValidationError,
)
from app.engine import compute_impacts
from app.models import (
    AUDITOR_ROLE,
    CORRECTION_APPROVED,
    CORRECTION_PENDING,
    CORRECTION_REJECTED,
    DECISION_APPROVE,
    DECISION_REJECT,
    EVENT_CLOSED,
    EVENT_EXTENDED,
    EVENT_REOPENED,
    JOURNAL_CORRECTION_APPROVED,
    JOURNAL_CORRECTION_PROPOSED,
    JOURNAL_CORRECTION_REJECTED,
    JOURNAL_EVENT_ADOPTED,
    JOURNAL_EVENT_REJECTED,
    REVIEWER_ROLES,
    Airport,
    DisruptionEvent,
    Flight,
    iso_utc,
)
from app.repository import Repository
from app.timeutil import parse_event_datetime, parse_utc
from app.validation import validate_event

_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{7,63}$")


class DisruptionService:
    def __init__(
        self,
        repo: Repository,
        airports: dict[str, Airport],
        flights: dict[str, Flight],
        reviewers: dict[str, list[str]] | None = None,
    ):
        self._repo = repo
        self._airports = airports
        self._flights = flights
        self._reviewers = reviewers or {}

    def healthy(self) -> bool:
        return self._repo.ping()

    def current_projection(self) -> int:
        return self._repo.current_seq()

    # ------------------------------------------------------------------ #
    # Event intake
    # ------------------------------------------------------------------ #

    def submit_event(self, payload: Any) -> dict[str, Any]:
        # Structural + basic semantic validation happens before any DB write.
        event = validate_event(payload, self._airports)

        rejection_to_raise = None
        result: dict[str, Any] | None = None

        with self._repo.transaction() as conn:
            existing = conn.execute(
                "SELECT event_version, payload_json, adoption_seq FROM events "
                "WHERE event_id = ?",
                (event.event_id,),
            ).fetchone()
            if existing is not None:
                return self._handle_duplicate(conn, event, existing)

            # An identical earlier rejection is replayed as the same decision;
            # retries must not pile up duplicate intake records.
            prior_rejection = conn.execute(
                "SELECT intake_seq, reasons_json FROM intake_records "
                "WHERE event_id = ? ORDER BY intake_seq DESC LIMIT 1",
                (event.event_id,),
            ).fetchone()
            if prior_rejection is not None:
                stored_payload = self._stored_rejection_payload(conn, event.event_id)
                if stored_payload == event.to_dict():
                    reasons = json.loads(prior_rejection["reasons_json"])
                    rejection_to_raise = RejectedEventError(
                        "Event was previously rejected by chain adjudication",
                        {
                            "errors": reasons,
                            "intake_seq": prior_rejection["intake_seq"],
                            "replayed": True,
                        },
                    )
                else:
                    rejection_to_raise = EventConflictError(  # type: ignore[assignment]
                        f"Event '{event.event_id}' was already rejected under a "
                        "different payload",
                        {"event_id": event.event_id, "issue": "rejected_id_reuse"},
                    )
            else:
                violations = self._arbitrate(conn, event)
                if violations:
                    seq = self._repo.append_journal(
                        conn,
                        JOURNAL_EVENT_REJECTED,
                        airport_code=event.airport_code,
                        ref_id=event.event_id,
                        detail={"errors": violations},
                    )
                    self._repo.insert_rejection(
                        conn,
                        intake_seq=seq,
                        event_id=event.event_id,
                        airport_code=event.airport_code,
                        payload=event.to_dict(),
                        reasons=violations,
                    )
                    rejection_to_raise = RejectedEventError(
                        "Event failed chain adjudication",
                        {"errors": violations, "intake_seq": seq},
                    )
                else:
                    result = self._adopt_event(conn, event)

        if rejection_to_raise is not None:
            raise rejection_to_raise
        assert result is not None
        return result

    def _stored_rejection_payload(self, conn, event_id: str) -> dict[str, Any] | None:
        row = conn.execute(
            "SELECT payload_json FROM intake_records WHERE event_id = ? "
            "ORDER BY intake_seq DESC LIMIT 1",
            (event_id,),
        ).fetchone()
        return json.loads(row["payload_json"]) if row else None

    def _adopt_event(self, conn, event: DisruptionEvent) -> dict[str, Any]:
        if event.event_type == EVENT_CLOSED:
            root = event
        else:
            # Use the effective root: an approved historical correction may
            # already have moved this chain's start/end.
            root_id = self._resolve_root_id(conn, event)
            chain_rows = self._repo.chain_events(conn, root_id)
            effective_chain = self._effective_chain(
                conn, chain_rows, as_of_seq=self._repo.current_seq(conn)
            )
            root = effective_chain[0]
        airport = self._airports[event.airport_code]
        impacts = compute_impacts(event, root, airport, self._flights)
        impacts.extend(self._resolved_tombstones(conn, event, root, impacts))
        seq = self._repo.append_journal(
            conn,
            JOURNAL_EVENT_ADOPTED,
            airport_code=event.airport_code,
            ref_id=event.event_id,
            detail={
                "event_version": event.event_version,
                "event_type": event.event_type,
                "root_event_id": root.event_id,
                "impact_count": sum(
                    1 for i in impacts if i["impact_status"] != "resolved"
                ),
                "resolved_count": sum(
                    1 for i in impacts if i["impact_status"] == "resolved"
                ),
            },
        )
        self._repo.insert_event(
            conn, event.to_dict(), root_event_id=root.event_id, adoption_seq=seq
        )
        if impacts:
            self._repo.insert_impacts(conn, impacts, seq)
        return self._result(event, impacts, replayed=False, version=seq)

    def _resolved_tombstones(
        self, conn, event: DisruptionEvent, root: DisruptionEvent, impacts: list[dict]
    ) -> list[dict[str, Any]]:
        """返回本事件生效后不再受影响、但曾出现在同链中的航班。"""
        if event.event_type == EVENT_CLOSED:
            return []
        still_affected = {(r["flight_id"], r["airport_code"]) for r in impacts}
        tombstones: list[dict[str, Any]] = []
        prior_keys = self._repo.prior_chain_impact_ids(conn, root.event_id)
        for flight_id, airport_code in sorted(prior_keys - still_affected):
            flight = self._flights.get(flight_id)
            if flight is None:
                continue
            tombstones.append(
                {
                    "event_id": event.event_id,
                    "root_event_id": root.event_id,
                    "airport_code": airport_code,
                    "flight_id": flight.flight_id,
                    "flight_number": flight.flight_number,
                    "affected_endpoint": "none",
                    "impact_status": "resolved",
                    "overlap_minutes": 0,
                    "delay_minutes": None,
                    "proposed_departure": None,
                    "proposed_arrival": None,
                    "passenger_count": flight.passenger_count,
                    "crosses_midnight": 0,
                }
            )
        return tombstones

    def _handle_duplicate(
        self, conn, event: DisruptionEvent, existing
    ) -> dict[str, Any]:
        stored_version = existing["event_version"]
        stored_payload = json.loads(existing["payload_json"])

        same_body = stored_payload == event.to_dict()
        if same_body:
            # Idempotent retry: return the original result, bump the counter.
            self._repo.increment_replay(conn, event.event_id)
            impacts = self._repo.get_impacts(
                event.event_id, as_of_seq=existing["adoption_seq"]
            )
            return self._result(
                event,
                [dict(r) for r in impacts],
                replayed=True,
                version=existing["adoption_seq"],
            )

        # Same identity, different content.
        if event.event_version == stored_version:
            raise EventConflictError(
                f"Event '{event.event_id}' version {stored_version} already exists "
                "with a different payload",
                {
                    "event_id": event.event_id,
                    "stored_version": stored_version,
                    "received_version": event.event_version,
                    "issue": "payload_mismatch",
                },
            )
        raise EventConflictError(
            f"Event '{event.event_id}' already exists at version {stored_version}; "
            "new versions must use a new event_id and reference the previous one "
            "via supersedes_event_id",
            {
                "event_id": event.event_id,
                "stored_version": stored_version,
                "received_version": event.event_version,
                "issue": "event_id_reuse",
            },
        )

    # ------------------------------------------------------------------ #
    # Chain arbitration
    # ------------------------------------------------------------------ #

    def _arbitrate(self, conn, event: DisruptionEvent) -> list[dict[str, str]]:
        """结合数据库现状裁定事件；返回违规列表（空列表表示采纳）。

        裁定三要素：开放/恢复时刻不得制造倒序窗口；``reported_at`` 沿链单调；
        接收顺序（裁定事务）配合机场全局单调版本号，保证并发提交只有一个赢家。
        """
        violations: list[dict[str, str]] = []

        max_version_row = conn.execute(
            "SELECT MAX(event_version) AS v FROM events WHERE airport_code = ?",
            (event.airport_code,),
        ).fetchone()
        max_version = max_version_row["v"]

        if event.event_type == EVENT_CLOSED:
            if max_version is not None and event.event_version <= max_version:
                violations.append(
                    {
                        "field": "event_version",
                        "issue": "must_extend_airport_history",
                        "stored_version": str(max_version),
                        "received_version": str(event.event_version),
                    }
                )
            active = self._active_chains(conn, event.airport_code)
            if active:
                violations.append(
                    {
                        "field": "event_type",
                        "issue": "airport_chain_still_active",
                        "active_root_event_id": sorted(active)[0],
                    }
                )
            return violations

        # extended / reopened must reference an existing event at the same airport
        ref_id = event.supersedes_event_id
        ref = (
            conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (ref_id,)
            ).fetchone()
            if ref_id is not None
            else None
        )
        if ref_id is None:
            violations.append(
                {
                    "field": "supersedes_event_id",
                    "issue": "required_for_chain_event",
                }
            )
            return violations
        if ref is None:
            # A dangling reference is bad input, not an adjudicated rejection:
            # do not consume an intake/audit record for it.
            raise ValidationError(
                "Event failed chain validation",
                {
                    "errors": [
                        {
                            "field": "supersedes_event_id",
                            "issue": "unknown_event",
                            "event_id": ref_id,
                        }
                    ]
                },
            )
        if ref["airport_code"] != event.airport_code:
            violations.append(
                {
                    "field": "supersedes_event_id",
                    "issue": "airport_mismatch",
                    "referenced_airport": ref["airport_code"],
                    "received_airport": event.airport_code,
                }
            )
            return violations

        chain_rows = self._repo.chain_events(conn, ref["root_event_id"])
        # Arbitrate against the *effective* chain so an approved historical
        # correction actually moves the head window used for later events.
        effective_chain = self._effective_chain(
            conn, chain_rows, as_of_seq=self._repo.current_seq(conn)
        )
        head_row = chain_rows[-1]
        head_event = effective_chain[-1]
        root = effective_chain[0]

        if head_event.airport_code != event.airport_code:
            # A prior correction reassigned this chain to another airport.
            violations.append(
                {
                    "field": "airport_code",
                    "issue": "chain_reassigned_to_other_airport",
                    "effective_airport": head_event.airport_code,
                    "received_airport": event.airport_code,
                }
            )
            return violations

        # One airport-wide monotonic version; the head check is the same bound,
        # so report it only once.
        bound = max_version if max_version is not None else head_row["event_version"]
        if event.event_version <= bound:
            violations.append(
                {
                    "field": "event_version",
                    "issue": "version_must_increase",
                    "stored_version": str(bound),
                    "received_version": str(event.event_version),
                }
            )
        # A chain event must continue the current head; referencing a buried
        # member would fork the chain and make the airport state ambiguous.
        if ref_id != head_row["event_id"]:
            violations.append(
                {
                    "field": "supersedes_event_id",
                    "issue": "must_supersede_chain_head",
                    "head_event_id": head_row["event_id"],
                    "received_event_id": ref_id,
                }
            )

        # Report time must follow the order in which the source learned facts.
        head_reported = head_event.reported_at
        if event.reported_at < head_reported:
            violations.append(
                {
                    "field": "reported_at",
                    "issue": "reported_at_precedes_chain_head",
                    "head_reported_at": iso_utc(head_reported),
                    "received_reported_at": iso_utc(event.reported_at),
                }
            )

        head_end = head_event.effective_until
        head_type = head_event.event_type

        if event.event_type == EVENT_EXTENDED:
            if head_type == EVENT_REOPENED:
                violations.append(
                    {
                        "field": "supersedes_event_id",
                        "issue": "chain_already_closed",
                        "event_id": head_row["event_id"],
                    }
                )
            else:
                self._check_extension_window(event, root, head_end, violations)
        else:  # EVENT_REOPENED
            if head_type == EVENT_REOPENED:
                violations.append(
                    {
                        "field": "event_type",
                        "issue": "chain_already_closed",
                        "event_id": head_row["event_id"],
                    }
                )
            else:
                self._check_reopen_window(event, root, head_end, violations)

        return violations

    def _check_extension_window(
        self,
        event: DisruptionEvent,
        root: DisruptionEvent,
        head_end: datetime | None,
        violations: list[dict[str, str]],
    ) -> None:
        if event.effective_from < root.effective_from:
            violations.append(
                {"field": "effective_from", "issue": "must_not_precede_chain_start"}
            )
        if head_end is None:
            # Supplying the first known end of an open-ended closure is always
            # forward progress; it still must not end before the chain started.
            if event.effective_until <= root.effective_from:
                violations.append(
                    {
                        "field": "effective_until",
                        "issue": "must_be_after_chain_start",
                    }
                )
            return
        if event.effective_from > head_end:
            violations.append(
                {"field": "effective_from", "issue": "extension_leaves_uncovered_gap"}
            )
        if event.effective_until <= head_end:
            violations.append(
                {"field": "effective_until", "issue": "must_extend_previous_window"}
            )

    def _check_reopen_window(
        self,
        event: DisruptionEvent,
        root: DisruptionEvent,
        head_end: datetime | None,
        violations: list[dict[str, str]],
    ) -> None:
        airport = self._airports[event.airport_code]
        resume_at = event.effective_from + timedelta(
            minutes=airport.reopen_buffer_minutes
        )
        if event.effective_from < root.effective_from:
            violations.append(
                {
                    "field": "effective_from",
                    "issue": "recovery_precedes_closure_start",
                    "closure_started_at": iso_utc(root.effective_from),
                    "reopened_at": iso_utc(event.effective_from),
                }
            )
        # The hard invariant: no end-before-start window, even with a zero buffer.
        if resume_at <= root.effective_from:
            violations.append(
                {
                    "field": "effective_from",
                    "issue": "recovery_window_not_after_closure_start",
                    "closure_started_at": iso_utc(root.effective_from),
                    "resume_at": iso_utc(resume_at),
                    "reopen_buffer_minutes": str(airport.reopen_buffer_minutes),
                }
            )
        # A reopening declared after the closure had already (per the latest
        # announced window) ended is contradictory late paperwork; it must arrive
        # as a historical correction instead. The operational buffer running a
        # few minutes past the forecast end is normal and allowed.
        if head_end is not None and event.effective_from > head_end:
            violations.append(
                {
                    "field": "effective_from",
                    "issue": "reopen_after_known_window_end",
                    "window_ended_at": iso_utc(head_end),
                    "reopened_at": iso_utc(event.effective_from),
                }
            )

    def _chains_snapshot(
        self, conn, airport_code: str | None, *, as_of_seq: int
    ) -> dict[str, DisruptionEvent]:
        """返回截至指定版本每个事件链的生效链头（叠加已批准更正）。

        键为 root_event_id，值为生效后的链头事件（机场可能已被更正改派）。
        """
        rows = conn.execute(
            "SELECT * FROM events ORDER BY adoption_seq"
        ).fetchall()
        grouped: dict[str, list] = {}
        for r in rows:
            if r["adoption_seq"] > as_of_seq:
                continue
            grouped.setdefault(r["root_event_id"], []).append(r)
        heads: dict[str, DisruptionEvent] = {}
        for root_id, chain_rows in grouped.items():
            effective = self._effective_chain(conn, chain_rows, as_of_seq=as_of_seq)
            head = effective[-1]
            if airport_code is None or head.airport_code == airport_code:
                heads[root_id] = head
        return heads

    def _active_chains(self, conn, airport_code: str) -> set[str]:
        as_of = self._repo.current_seq(conn)
        heads = self._chains_snapshot(conn, airport_code, as_of_seq=as_of)
        return {
            root_id
            for root_id, head in heads.items()
            if head.event_type != EVENT_REOPENED
        }

    def _resolve_root_id(self, conn, event: DisruptionEvent) -> str:
        """沿 supersedes 链找到 closed 根事件的 id。"""
        if event.event_type == EVENT_CLOSED:
            return event.event_id
        seen: set[str] = set()
        current = event
        while current.supersedes_event_id is not None:
            ref_id = current.supersedes_event_id
            if ref_id in seen:
                raise ValidationError("supersedes chain contains a cycle")
            seen.add(ref_id)
            row = conn.execute(
                "SELECT event_type, supersedes_event_id FROM events "
                "WHERE event_id = ?",
                (ref_id,),
            ).fetchone()
            if row is None:
                raise ValidationError(f"unknown superseded event '{ref_id}'")
            if row["event_type"] == EVENT_CLOSED:
                return ref_id
            current = DisruptionEvent(
                event_id=ref_id,
                event_version=1,
                event_type=row["event_type"],
                airport_code=event.airport_code,
                effective_from=event.effective_from,
                effective_until=None,
                reported_at=event.reported_at,
                supersedes_event_id=row["supersedes_event_id"],
                reason=None,
            )
        raise ValidationError("extended/reopened event chain has no closed root")

    def _resolve_root(self, conn, event: DisruptionEvent) -> DisruptionEvent:
        if event.event_type == EVENT_CLOSED:
            return event
        root_id = self._resolve_root_id(conn, event)
        row = conn.execute(
            "SELECT * FROM events WHERE event_id = ?", (root_id,)
        ).fetchone()
        return _row_to_event(row)

    # ------------------------------------------------------------------ #
    # Corrections (historical material review workflow)
    # ------------------------------------------------------------------ #

    def submit_correction(self, payload: Any) -> dict[str, Any]:
        proposal = self._validate_correction_payload(payload)

        with self._repo.transaction() as conn:
            existing = self._repo.get_correction(proposal["request_id"])
            if existing is not None:
                return self._handle_correction_duplicate(proposal, existing)

            current = self._repo.current_seq(conn)
            if proposal["base_projection_version"] != current:
                raise ConflictStateError(
                    "base_projection_version does not match the current "
                    f"adjudication version ({current})",
                    {
                        "base_projection_version": proposal["base_projection_version"],
                        "current_projection_version": current,
                    },
                )

            target_row = conn.execute(
                "SELECT * FROM events WHERE event_id = ?",
                (proposal["target_event_id"],),
            ).fetchone()
            if target_row is None:
                raise NotFoundError(
                    f"Target event '{proposal['target_event_id']}' was not found",
                    {"target_event_id": proposal["target_event_id"]},
                )

            chain_rows = self._repo.chain_events(conn, target_row["root_event_id"])
            effective = self._effective_chain(conn, chain_rows, as_of_seq=current)
            target_index = next(
                i for i, e in enumerate(effective)
                if e.event_id == proposal["target_event_id"]
            )
            patched = self._apply_patch(effective[target_index], proposal["patch"])

            released_keys: set[tuple[str, str]] = set()
            if patched.airport_code != target_row["airport_code"]:
                if len(effective) > 1:
                    raise ValidationError(
                        "Reassigning an event to another airport is only allowed for "
                        "isolated events without chain successors",
                        {"errors": [{"field": "patch.airport_code",
                                     "issue": "chain_has_successor_events"}]},
                    )
                # Flights the event currently affects at its old airport must be
                # released as resolved tombstones under that airport.
                released_keys = {
                    (r["flight_id"], r["airport_code"])
                    for r in self._repo.active_impacts_for_event(
                        conn, proposal["target_event_id"]
                    )
                }

            replay = self._replay_chain(
                effective, target_index, patched, released_keys=released_keys
            )
            scope = self._recompute_scope(
                conn, effective, target_index, replay, as_of_seq=current,
                released_keys=released_keys,
            )

            seq = self._repo.append_journal(
                conn,
                JOURNAL_CORRECTION_PROPOSED,
                airport_code=patched.airport_code,
                ref_id=proposal["request_id"],
                detail={
                    "target_event_id": patched.event_id,
                    "base_projection_version": current,
                    "submitted_by": proposal["submitted_by"],
                    "scope": scope,
                },
            )
            self._repo.insert_correction(
                conn,
                request_id=proposal["request_id"],
                target_event_id=patched.event_id,
                base_version=current,
                patch=self._patch_to_jsonable(proposal["patch"]),
                payload=proposal["raw"],
                submitted_by=proposal["submitted_by"],
                reason=proposal.get("reason"),
                feasible=True,
                scope=scope,
                created_seq=seq,
            )
            return self._correction_result(
                proposal, CORRECTION_PENDING, seq, None, seq, scope
            )

    def decide_correction(
        self, request_id: str, decision: str, reviewer_id: str,
        reason: str | None,
    ) -> dict[str, Any]:
        if decision not in (DECISION_APPROVE, DECISION_REJECT):
            raise ValidationError(
                "decision must be 'approved' or 'rejected'",
                {"field": "decision", "received": decision},
            )
        roles = self._reviewers.get(reviewer_id)
        if roles is None:
            raise NotFoundError(
                f"Reviewer '{reviewer_id}' is not registered",
                {"reviewer_id": reviewer_id},
            )
        if AUDITOR_ROLE in roles or not any(r in REVIEWER_ROLES for r in roles):
            raise ForbiddenReviewerError(
                f"Reviewer '{reviewer_id}' lacks an adjudication role",
                {"reviewer_id": reviewer_id, "roles": roles},
            )

        with self._repo.transaction() as conn:
            row = self._repo.get_correction(request_id)
            if row is None:
                raise NotFoundError(
                    f"Correction request '{request_id}' was not found",
                    {"request_id": request_id},
                )
            if row["state"] != CORRECTION_PENDING:
                raise EventConflictError(
                    f"Correction '{request_id}' is already {row['state']}",
                    {
                        "request_id": request_id,
                        "state": row["state"],
                        "decided_by": row["decided_by"],
                    },
                )

            current = self._repo.current_seq(conn)
            if decision == DECISION_REJECT:
                seq = self._repo.append_journal(
                    conn,
                    JOURNAL_CORRECTION_REJECTED,
                    airport_code=None,
                    ref_id=request_id,
                    detail={
                        "target_event_id": row["target_event_id"],
                        "reviewer_id": reviewer_id,
                        "reason": reason,
                    },
                )
                self._repo.decide_correction(
                    conn,
                    request_id=request_id,
                    state=CORRECTION_REJECTED,
                    decided_seq=seq,
                    decided_by=reviewer_id,
                    decided_reason=reason,
                )
                return self._correction_result_from_row(row, CORRECTION_REJECTED,
                                                        seq, reviewer_id, reason,
                                                        seq)

            # Approval: re-validate against the *current* projection, then write
            # the amendment and a full recomputed tail as one new version.
            patch = self._patch_from_json(json.loads(row["patch_json"]))
            target_row = conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (row["target_event_id"],)
            ).fetchone()
            chain_rows = self._repo.chain_events(conn, target_row["root_event_id"])
            effective = self._effective_chain(conn, chain_rows, as_of_seq=current)
            target_index = next(
                i for i, e in enumerate(effective)
                if e.event_id == row["target_event_id"]
            )
            released_keys: set[tuple[str, str]] = set()
            try:
                patched = self._apply_patch(effective[target_index], patch)
                if patched.airport_code != target_row["airport_code"]:
                    if len(effective) > 1:
                        raise ValidationError(
                            "Reassigning an event to another airport is only "
                            "allowed for isolated chains",
                            {"errors": [{"field": "patch.airport_code",
                                         "issue": "chain_has_successor_events"}]},
                        )
                    released_keys = {
                        (r["flight_id"], r["airport_code"])
                        for r in self._repo.active_impacts_for_event(
                            conn, patched.event_id
                        )
                    }
                replay = self._replay_chain(
                    effective, target_index, patched,
                    released_keys=released_keys,
                )
            except ValidationError as exc:
                raise ConflictStateError(
                    "Correction is no longer feasible against the current "
                    "projection; a new proposal is required",
                    {"request_id": request_id, "errors": exc.details.get("errors", [])},
                ) from None
            scope = self._recompute_scope(
                conn, effective, target_index, replay, as_of_seq=current,
                released_keys=released_keys,
            )

            seq = self._repo.append_journal(
                conn,
                JOURNAL_CORRECTION_APPROVED,
                airport_code=patched.airport_code,
                ref_id=request_id,
                detail={
                    "target_event_id": patched.event_id,
                    "reviewer_id": reviewer_id,
                    "reason": reason,
                    "scope": scope,
                },
            )
            self._repo.insert_amendment(
                conn,
                event_id=patched.event_id,
                request_id=request_id,
                projection_seq=seq,
                patched_event=patched,
            )
            self._repo.decide_correction(
                conn,
                request_id=request_id,
                state=CORRECTION_APPROVED,
                decided_seq=seq,
                decided_by=reviewer_id,
                decided_reason=reason,
            )
            # Write the whole recomputed chain tail. A shorter window can free
            # flights that have no successor event to carry a tombstone, so each
            # snapshot is augmented with resolved rows for flights the currently
            # published snapshot still showed as affected; without these rows the
            # latest-snapshot view would resurrect the pre-correction impacts.
            for idx, snapshot in replay.snapshots[target_index:]:
                augmented = self._with_release_tombstones(
                    conn, replay.root.event_id, replay.events[idx], snapshot,
                    as_of_seq=current,
                )
                self._repo.insert_impacts(conn, augmented, seq)
            return self._correction_result_from_row(row, CORRECTION_APPROVED,
                                                    seq, reviewer_id, reason,
                                                    seq, scope)

    def _with_release_tombstones(
        self, conn, root_event_id: str, event: DisruptionEvent,
        snapshot: list[dict[str, Any]], *, as_of_seq: int,
    ) -> list[dict[str, Any]]:
        """补齐发布快照中仍活动、但重算快照里已消失航班的 resolved 墓碑。"""
        before_rows = self._repo.get_impacts(event.event_id, as_of_seq=as_of_seq)
        before_active = {
            (r["flight_id"], r["airport_code"])
            for r in before_rows
            if r["impact_status"] != "resolved"
        }
        after_keys = {(r["flight_id"], r["airport_code"]) for r in snapshot}
        out = list(snapshot)
        for flight_id, airport_code in sorted(before_active - after_keys):
            flight = self._flights.get(flight_id)
            if flight is None:
                continue
            out.append(
                {
                    "event_id": event.event_id,
                    "root_event_id": root_event_id,
                    "airport_code": airport_code,
                    "flight_id": flight.flight_id,
                    "flight_number": flight.flight_number,
                    "affected_endpoint": "none",
                    "impact_status": "resolved",
                    "overlap_minutes": 0,
                    "delay_minutes": None,
                    "proposed_departure": None,
                    "proposed_arrival": None,
                    "passenger_count": flight.passenger_count,
                    "crosses_midnight": 0,
                }
            )
        return out

    def list_corrections(self, state: str | None = None) -> dict[str, Any]:
        if state is not None and state not in (
            CORRECTION_PENDING, CORRECTION_APPROVED, CORRECTION_REJECTED
        ):
            raise ValidationError(
                "Unsupported correction state filter",
                {"field": "state", "received": state},
            )
        rows = self._repo.list_corrections(state=state)
        current = self._repo.current_seq()
        return {
            "projection_version": current,
            "corrections": [self._correction_row_dict(r) for r in rows],
        }

    def correction_status(self, request_id: str) -> dict[str, Any]:
        row = self._repo.get_correction(request_id)
        if row is None:
            raise NotFoundError(
                f"Correction request '{request_id}' was not found",
                {"request_id": request_id},
            )
        return self._correction_row_dict(row)

    def _validate_correction_payload(self, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValidationError("Correction payload must be a JSON object")
        errors: list[dict[str, str]] = []
        required = (
            "request_id", "target_event_id", "base_projection_version",
            "patch", "submitted_by",
        )
        missing = [f for f in required if f not in payload]
        if missing:
            raise ValidationError(
                "Correction payload is missing required field(s)",
                {"errors": [{"field": ".", "issue": "missing_fields",
                             "fields": ", ".join(missing)}]},
            )
        # ``reason`` is the only optional field.
        extra = sorted(k for k in payload if k not in required and k != "reason")
        if extra:
            errors.append({"field": ".", "issue": "unknown_fields",
                           "fields": ", ".join(extra)})

        request_id = payload["request_id"]
        target_id = payload["target_event_id"]
        base = payload["base_projection_version"]
        patch = payload["patch"]
        submitted_by = payload["submitted_by"]
        reason = payload.get("reason")

        import re

        id_re = re.compile(r"^[a-z0-9][a-z0-9-]{7,63}$")
        if not isinstance(request_id, str) or not id_re.match(request_id):
            errors.append({"field": "request_id", "issue": "pattern_mismatch"})
        if not isinstance(target_id, str) or not id_re.match(target_id):
            errors.append({"field": "target_event_id", "issue": "pattern_mismatch"})
        if not isinstance(base, int) or isinstance(base, bool) or base < 1:
            errors.append(
                {"field": "base_projection_version", "issue": "must_be_positive_integer"}
            )
        if not isinstance(submitted_by, str) or not 3 <= len(submitted_by) <= 64:
            errors.append({"field": "submitted_by", "issue": "length_out_of_range"})
        if reason is not None and (
            not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 240
        ):
            errors.append({"field": "reason", "issue": "length_out_of_range"})

        clean_patch: dict[str, Any] = {}
        if not isinstance(patch, dict) or not patch:
            errors.append({"field": "patch", "issue": "must_be_non_empty_object"})
        else:
            bad_patch_keys = set(patch) - {
                "airport_code", "effective_from", "effective_until"
            }
            if bad_patch_keys:
                errors.append(
                    {"field": "patch", "issue": "unknown_fields",
                     "fields": ", ".join(sorted(bad_patch_keys))}
                )
            if "airport_code" in patch:
                code = patch["airport_code"]
                if not (isinstance(code, str) and len(code) == 3 and code.isupper()):
                    errors.append(
                        {"field": "patch.airport_code", "issue": "pattern_mismatch"}
                    )
                elif code not in self._airports:
                    errors.append(
                        {"field": "patch.airport_code", "issue": "unknown_airport",
                         "received": code}
                    )
                else:
                    clean_patch["airport_code"] = code
            for field in ("effective_from", "effective_until"):
                if field not in patch:
                    continue
                value = patch[field]
                if value is None:
                    clean_patch[field] = None
                    continue
                try:
                    clean_patch[field] = parse_event_datetime(value, f"patch.{field}")
                except ValidationError as exc:
                    errors.append({"field": f"patch.{field}", "issue": exc.message})
            if "effective_from" in clean_patch and "effective_until" in clean_patch:
                start = clean_patch["effective_from"]
                end = clean_patch["effective_until"]
                if end is not None and end <= start:
                    errors.append(
                        {"field": "patch.effective_until",
                         "issue": "must_be_after_effective_from"}
                    )

        if errors:
            raise ValidationError("Correction payload failed validation",
                                  {"errors": errors})
        return {
            "request_id": request_id,
            "target_event_id": target_id,
            "base_projection_version": base,
            "patch": clean_patch,
            "submitted_by": submitted_by,
            "reason": reason.strip() if isinstance(reason, str) else None,
            "raw": payload,
        }

    def _handle_correction_duplicate(self, proposal: dict, existing) -> dict[str, Any]:
        stored_payload = json.loads(existing["payload_json"])
        if stored_payload != proposal["raw"]:
            raise EventConflictError(
                f"Correction request '{proposal['request_id']}' already exists with "
                "a different payload",
                {"request_id": proposal["request_id"],
                 "issue": "payload_mismatch"},
            )
        return self._correction_row_dict(existing)

    @staticmethod
    def _patch_to_jsonable(patch: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if "airport_code" in patch:
            out["airport_code"] = patch["airport_code"]
        for field in ("effective_from", "effective_until"):
            if field in patch:
                value = patch[field]
                out[field] = iso_utc(value) if value is not None else None
        return out

    @staticmethod
    def _patch_from_json(patch: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = dict(patch)
        for field in ("effective_from", "effective_until"):
            if field in out and out[field] is not None:
                out[field] = parse_utc(out[field])
        return out

    # ------------------------------------------------------------------ #
    # Correction replay helpers
    # ------------------------------------------------------------------ #

    def _effective_chain(self, conn, chain_rows, *, as_of_seq: int) -> list[DisruptionEvent]:
        """链上事件叠加截至指定版本已批准的更正后的生效形态。"""
        amendments = self._repo.amendments_for_chain(
            conn,
            [r["event_id"] for r in chain_rows],
            as_of_seq=as_of_seq,
        )
        events: list[DisruptionEvent] = []
        for row in chain_rows:
            event = _row_to_event(row)
            amendment = amendments.get(row["event_id"])
            if amendment is not None:
                event = self._amend_event(event, amendment)
            events.append(event)
        return events

    @staticmethod
    def _amend_event(event: DisruptionEvent, amendment: sqlite3_row_like) -> DisruptionEvent:
        effective_until = event.effective_until
        if amendment["effective_until_set"]:
            # An explicit NULL means an open-ended closure, distinct from an
            # unmodified field.
            raw_until = amendment["effective_until"]
            effective_until = parse_utc(raw_until) if raw_until is not None else None
        return DisruptionEvent(
            event_id=event.event_id,
            event_version=event.event_version,
            event_type=event.event_type,
            airport_code=amendment["airport_code"] or event.airport_code,
            effective_from=(
                parse_utc(amendment["effective_from"])
                if amendment["effective_from"] is not None
                else event.effective_from
            ),
            effective_until=effective_until,
            reported_at=event.reported_at,
            supersedes_event_id=event.supersedes_event_id,
            reason=event.reason,
        )

    def _apply_patch(
        self, event: DisruptionEvent, patch: dict[str, Any]
    ) -> DisruptionEvent:
        airport_code = patch.get("airport_code", event.airport_code)
        effective_from = patch.get("effective_from", event.effective_from)
        effective_until = (
            patch["effective_until"] if "effective_until" in patch
            else event.effective_until
        )
        if effective_from is None:
            raise ValidationError(
                "patch.effective_from cannot be null",
                {"errors": [{"field": "patch.effective_from",
                             "issue": "must_be_timestamp"}]},
            )
        # Validate the patched event in isolation against airport/type rules.
        if event.event_type == EVENT_EXTENDED and effective_until is None:
            raise ValidationError(
                "patched extended event must define effective_until",
                {"errors": [{"field": "patch.effective_until",
                             "issue": "required_for_extended_event"}]},
            )
        if event.event_type == EVENT_REOPENED and effective_until is not None:
            raise ValidationError(
                "patched reopened event must not define effective_until",
                {"errors": [{"field": "patch.effective_until",
                             "issue": "not_allowed_for_reopened_event"}]},
            )
        return DisruptionEvent(
            event_id=event.event_id,
            event_version=event.event_version,
            event_type=event.event_type,
            airport_code=airport_code,
            effective_from=effective_from,
            effective_until=effective_until,
            reported_at=event.reported_at,
            supersedes_event_id=event.supersedes_event_id,
            reason=event.reason,
        )

    def _replay_chain(
        self,
        effective: list[DisruptionEvent],
        target_index: int,
        patched: DisruptionEvent,
        *,
        released_keys: set[tuple[str, str]] | None = None,
    ) -> "_ReplayResult":
        """以更正后的事件重放整条链，返回每个事件的完整快照。

        纯函数式重放，不写库；任何一步产生倒序窗口即判定更正不可行。
        ``released_keys`` 用于机场改派：这些（航班, 旧机场）在改派事件的
        快照里必须写成旧机场的 resolved 墓碑。
        """
        candidate = list(effective)
        candidate[target_index] = patched
        released_keys = set(released_keys or ())

        errors: list[dict[str, str]] = []
        root = candidate[0]
        if root.event_type != EVENT_CLOSED:
            errors.append({"field": "event_type", "issue": "chain_has_no_closed_root"})
        if root.effective_until is not None and root.effective_until <= root.effective_from:
            errors.append(
                {"field": "effective_until", "issue": "must_be_after_effective_from"}
            )

        snapshots: list[tuple[int, list[dict[str, Any]]]] = []
        prev_affected: set[tuple[str, str]] = set()
        head_end = root.effective_until
        head_reported = root.reported_at

        for idx, event in enumerate(candidate):
            airport = self._airports.get(event.airport_code)
            if airport is None:
                raise ValidationError(
                    "Correction references an unknown airport",
                    {"errors": [{"field": "airport_code",
                                 "received": event.airport_code}]},
                )
            if idx > 0:
                if event.reported_at < head_reported:
                    errors.append(
                        {"field": "reported_at",
                         "issue": "reported_at_precedes_chain_head",
                         "event_id": event.event_id}
                    )
                if event.event_type == EVENT_EXTENDED:
                    self._collect_extension_errors(event, root, head_end, errors)
                elif event.event_type == EVENT_REOPENED:
                    self._collect_reopen_errors(event, root, head_end, errors)
                else:
                    errors.append(
                        {"field": "event_type",
                         "issue": "nested_close_in_chain",
                         "event_id": event.event_id}
                    )

            impacts: list[dict[str, Any]] = []
            try:
                impacts = compute_impacts(event, root, airport, self._flights)
            except ValueError:
                # The engine refuses inverted windows; _collect_*_errors has
                # already recorded the precise violation for the proposal.
                errors.append(
                    {
                        "field": "effective_from",
                        "issue": "recovery_window_not_after_closure_start",
                        "event_id": event.event_id,
                    }
                )
            affected = {(r["flight_id"], r["airport_code"]) for r in impacts}

            def add_tombstone(flight_id: str, airport_code: str) -> None:
                flight = self._flights[flight_id]
                impacts.append(
                    {
                        "event_id": event.event_id,
                        "root_event_id": root.event_id,
                        "airport_code": airport_code,
                        "flight_id": flight.flight_id,
                        "flight_number": flight.flight_number,
                        "affected_endpoint": "none",
                        "impact_status": "resolved",
                        "overlap_minutes": 0,
                        "delay_minutes": None,
                        "proposed_departure": None,
                        "proposed_arrival": None,
                        "passenger_count": flight.passenger_count,
                        "crosses_midnight": 0,
                    }
                )

            for flight_id, airport_code in sorted(prev_affected - affected):
                add_tombstone(flight_id, airport_code)
            if idx == target_index:
                for flight_id, airport_code in sorted(released_keys - affected):
                    add_tombstone(flight_id, airport_code)
            snapshots.append((idx, impacts))
            prev_affected = affected
            if event.event_type == EVENT_EXTENDED:
                head_end = event.effective_until
            elif event.event_type == EVENT_REOPENED:
                head_end = event.effective_from + timedelta(
                    minutes=airport.reopen_buffer_minutes
                )
            head_reported = event.reported_at

        if errors:
            raise ValidationError(
                "Correction would produce an invalid chain", {"errors": errors}
            )
        return _ReplayResult(root=root, events=candidate, snapshots=snapshots)

    def _collect_extension_errors(
        self, event, root, head_end, errors: list[dict[str, str]]
    ) -> None:
        local: list[dict[str, str]] = []
        self._check_extension_window(event, root, head_end, local)
        errors.extend(local)

    def _collect_reopen_errors(
        self, event, root, head_end, errors: list[dict[str, str]]
    ) -> None:
        local: list[dict[str, str]] = []
        self._check_reopen_window(event, root, head_end, local)
        errors.extend(local)

    def _recompute_scope(
        self,
        conn,
        effective: list[DisruptionEvent],
        target_index: int,
        replay: "_ReplayResult",
        *,
        as_of_seq: int,
        released_keys: set[tuple[str, str]] | None = None,
    ) -> dict[str, Any]:
        """对比更正前后的已发布结果，得出重算范围与变更航班。"""
        changed: list[dict[str, Any]] = []
        recomputed_events: list[str] = []
        for idx, new_rows in replay.snapshots[target_index:]:
            event = effective[idx]
            recomputed_events.append(event.event_id)
            before_rows = self._repo.get_impacts(event.event_id, as_of_seq=as_of_seq)
            before = {
                (r["flight_id"], r["airport_code"]): r["impact_status"]
                for r in before_rows
            }
            after = {
                (r["flight_id"], r["airport_code"]): r["impact_status"]
                for r in new_rows
            }
            for key in sorted(set(before) | set(after)):
                if before.get(key) != after.get(key):
                    changed.append(
                        {
                            "event_id": event.event_id,
                            "flight_id": key[0],
                            "airport_code": key[1],
                            "before": before.get(key),
                            "after": after.get(key),
                        }
                    )
        return {
            "events_recomputed": recomputed_events,
            "flights_changed": changed,
            "change_count": len(changed),
            "published_result_changed": bool(changed),
        }

    # ------------------------------------------------------------------ #
    # Queries
    # ------------------------------------------------------------------ #

    def _resolve_as_of(
        self, projection_version: int | None, conn=None
    ) -> int:
        current = self._repo.current_seq(conn)
        if projection_version is None:
            return current
        if not isinstance(projection_version, int) or isinstance(
            projection_version, bool
        ) or projection_version < 1:
            raise BadRequestError("projection_version must be a positive integer")
        if projection_version > current:
            raise NotFoundError(
                f"projection_version {projection_version} does not exist "
                f"(current: {current})",
                {"requested": projection_version, "current": current},
            )
        return projection_version

    def event_status(
        self, event_id: str, *, projection_version: int | None = None
    ) -> dict[str, Any]:
        with self._repo.read() as conn:
            as_of = self._resolve_as_of(projection_version, conn)
            current = self._repo.current_seq(conn)
            row = conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError(
                    f"Event '{event_id}' was not found", {"event_id": event_id}
                )
            impact_rows = self._repo.get_impacts(event_id, as_of_seq=as_of)
            impacts = [self._impact_dict(r) for r in impact_rows]
            active = [i for i in impacts if i["impact_status"] != "resolved"]
            statuses: dict[str, int] = {}
            passengers = 0
            for imp in active:
                statuses[imp["impact_status"]] = (
                    statuses.get(imp["impact_status"], 0) + 1
                )
                passengers += imp["passenger_count"]
            amendments = self._repo.amendments_for_event(event_id, as_of_seq=as_of)
            chain_rows = self._repo.chain_events(conn, row["root_event_id"])
            effective = self._effective_chain(conn, chain_rows, as_of_seq=as_of)
            effective_event = next(
                e for e in effective if e.event_id == event_id
            )
            return {
                "event": json.loads(row["payload_json"]),
                "effective_event": effective_event.to_dict(),
                "processing": {
                    "state": "processed",
                    "replay_count": row["replay_count"],
                    "created_at": row["created_at"],
                    "adopted_at_version": row["adoption_seq"],
                    "impact_count": len(active),
                    "resolved_count": len(impacts) - len(active),
                    "affected_passengers": passengers,
                    "status_breakdown": statuses,
                    "amended_by": [a["request_id"] for a in amendments],
                },
                "projection_version": as_of,
                "current_projection_version": current,
                "impacts": active,
            }

    def _airport_view(self, conn, airport_code: str, as_of: int) -> dict[str, int]:
        """按生效（叠加更正后）机场统计事件数、链数与活动链数。"""
        rows = conn.execute(
            "SELECT * FROM events WHERE adoption_seq <= ? ORDER BY adoption_seq",
            (as_of,),
        ).fetchall()
        grouped: dict[str, list] = {}
        for r in rows:
            grouped.setdefault(r["root_event_id"], []).append(r)
        event_count = 0
        heads: dict[str, str] = {}
        for root_id, chain_rows in grouped.items():
            effective = self._effective_chain(conn, chain_rows, as_of_seq=as_of)
            here = [e for e in effective if e.airport_code == airport_code]
            event_count += len(here)
            head = effective[-1]
            if head.airport_code == airport_code:
                heads[root_id] = head.event_type
        return {
            "event_count": event_count,
            "chain_count": len(heads),
            "active_chains": sum(
                1 for kind in heads.values() if kind != EVENT_REOPENED
            ),
        }

    def airport_summary(
        self, airport_code: str, *, projection_version: int | None = None
    ) -> dict[str, Any]:
        if airport_code not in self._airports:
            raise NotFoundError(
                f"Unknown airport code '{airport_code}'",
                {"field": "airport_code", "received": airport_code},
            )
        with self._repo.read() as conn:
            as_of = self._resolve_as_of(projection_version, conn)
            view = self._airport_view(conn, airport_code, as_of)
            latest = self._repo.latest_impacts(
                airport=airport_code, as_of_seq=as_of
            )
            by_status: dict[str, dict[str, Any]] = {}
            total_passengers = 0
            for r in latest:
                bucket = by_status.setdefault(
                    r["impact_status"],
                    {"flight_count": 0, "passenger_count": 0, "flights": []},
                )
                bucket["flight_count"] += 1
                bucket["passenger_count"] += r["passenger_count"]
                total_passengers += r["passenger_count"]
                bucket["flights"].append(r["flight_id"])
            return {
                "airport_code": airport_code,
                "airport_name": self._airports[airport_code].name,
                "event_count": view["event_count"],
                "chain_count": view["chain_count"],
                "active_chains": view["active_chains"],
                "affected_flights": len(latest),
                "affected_passengers": total_passengers,
                "by_status": by_status,
                "projection_version": as_of,
            }

    def affected_flights(
        self,
        *,
        airport: str | None,
        status: str | None,
        limit: int,
        offset: int,
        projection_version: int | None = None,
    ) -> dict[str, Any]:
        if airport is not None and airport not in self._airports:
            raise NotFoundError(
                f"Unknown airport code '{airport}'",
                {"field": "airport", "received": airport},
            )
        allowed = {"cancelled", "delayed", "pending_confirmation"}
        if status is not None and status not in allowed:
            raise ValidationError(
                "Unsupported impact status filter",
                {"field": "status", "allowed": sorted(allowed)},
            )
        with self._repo.read():
            as_of = self._resolve_as_of(projection_version)
            rows = self._repo.latest_impacts(
                airport=airport, status=status, as_of_seq=as_of
            )
        total = len(rows)
        page = rows[offset : offset + limit]
        return {
            "pagination": {
                "limit": limit,
                "offset": offset,
                "total": total,
            },
            "projection_version": as_of,
            "flights": [self._impact_dict(r) for r in page],
        }

    def rejected_material(
        self, *, airport: str | None = None, projection_version: int | None = None
    ) -> dict[str, Any]:
        if airport is not None and airport not in self._airports:
            raise NotFoundError(
                f"Unknown airport code '{airport}'",
                {"field": "airport", "received": airport},
            )
        with self._repo.read():
            as_of = self._resolve_as_of(projection_version)
            records = self._repo.rejected_intake(airport=airport, as_of_seq=as_of)
        return {
            "projection_version": as_of,
            "count": len(records),
            "rejections": records,
        }

    def journal(self, *, since: int = 0, limit: int = 200) -> dict[str, Any]:
        if not isinstance(since, int) or since < 0:
            raise BadRequestError("since must be a non-negative integer")
        if not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise BadRequestError("limit must be between 1 and 1000")
        entries = self._repo.journal_entries(since=since, limit=limit)
        return {
            "projection_version": self._repo.current_seq(),
            "entries": entries,
        }

    # ------------------------------------------------------------------ #
    # Serialization helpers
    # ------------------------------------------------------------------ #

    def _correction_row_dict(self, row: Any) -> dict[str, Any]:
        if not isinstance(row, dict):
            row = dict(row)
        return {
            "request_id": row["request_id"],
            "target_event_id": row["target_event_id"],
            "base_projection_version": row["base_version"],
            "patch": json.loads(row["patch_json"]),
            "submitted_by": row["submitted_by"],
            "reason": row["reason"],
            "state": row["state"],
            "feasible": bool(row["feasible"]),
            "scope": json.loads(row["scope_json"]),
            "created_at_version": row["created_seq"],
            "decided_at_version": row["decided_seq"],
            "decided_by": row["decided_by"],
            "decided_reason": row["decided_reason"],
            "created_at": row["created_at"],
            "decided_at": row["decided_at"],
        }

    def _correction_result(
        self, proposal: dict, state: str, seq: int, reviewer: str | None,
        current: int, scope: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "request_id": proposal["request_id"],
            "target_event_id": proposal["target_event_id"],
            "state": state,
            "base_projection_version": proposal["base_projection_version"],
            "projection_version": seq,
            "current_projection_version": current,
            "submitted_by": proposal["submitted_by"],
            "reason": proposal.get("reason"),
            "scope": scope,
            "decided_by": reviewer,
        }

    def _correction_result_from_row(
        self, row, state: str, seq: int, reviewer: str | None,
        reason: str | None, current: int, scope: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "request_id": row["request_id"],
            "target_event_id": row["target_event_id"],
            "state": state,
            "base_projection_version": row["base_version"],
            "projection_version": seq,
            "current_projection_version": current,
            "submitted_by": row["submitted_by"],
            "reason": row["reason"],
            "scope": scope if scope is not None else json.loads(row["scope_json"]),
            "decided_by": reviewer,
            "decided_reason": reason,
        }

    def _result(
        self, event: DisruptionEvent, impacts: list[dict[str, Any]], *,
        replayed: bool, version: int,
    ) -> dict[str, Any]:
        active_impacts = [i for i in impacts if i["impact_status"] != "resolved"]
        serialized = [self._impact_dict(i) for i in active_impacts]
        statuses: dict[str, int] = {}
        passengers = 0
        for imp in serialized:
            statuses[imp["impact_status"]] = statuses.get(imp["impact_status"], 0) + 1
            passengers += imp["passenger_count"]
        return {
            "event_id": event.event_id,
            "event_version": event.event_version,
            "processing_state": "replayed" if replayed else "processed",
            "projection_version": version,
            "impact_count": len(serialized),
            "resolved_count": len(impacts) - len(active_impacts),
            "affected_passengers": passengers,
            "status_breakdown": statuses,
            "impacts": serialized,
        }

    @staticmethod
    def _impact_dict(row: Any) -> dict[str, Any]:
        if not isinstance(row, dict):
            row = dict(row)
        return {
            "event_id": row["event_id"],
            "root_event_id": row["root_event_id"],
            "airport_code": row["airport_code"],
            "flight_id": row["flight_id"],
            "flight_number": row["flight_number"],
            "affected_endpoint": row["affected_endpoint"],
            "impact_status": row["impact_status"],
            "overlap_minutes": row["overlap_minutes"],
            "delay_minutes": row["delay_minutes"],
            "proposed_departure": row["proposed_departure"],
            "proposed_arrival": row["proposed_arrival"],
            "passenger_count": row["passenger_count"],
            "crosses_midnight": bool(row["crosses_midnight"]),
        }


class _ReplayResult:
    def __init__(self, *, root: DisruptionEvent, events: list, snapshots: list):
        self.root = root
        self.events = events
        self.snapshots = snapshots


# Typing alias kept as a lightweight annotation for amendment rows.
sqlite3_row_like = Any


def parse_ts(value: str) -> datetime:
    return parse_utc(value)


def _row_to_event(row) -> DisruptionEvent:
    return DisruptionEvent(
        event_id=row["event_id"],
        event_version=row["event_version"],
        event_type=row["event_type"],
        airport_code=row["airport_code"],
        effective_from=parse_ts(row["effective_from"]),
        effective_until=parse_ts(row["effective_until"]) if row["effective_until"] else None,
        reported_at=parse_ts(row["reported_at"]),
        supersedes_event_id=row["supersedes_event_id"],
        reason=row["reason"],
    )
