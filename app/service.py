"""处理事件接入语义、持久化协调与业务查询的应用服务。

裁定模型
========

* 每个机场在 ``airport_state`` 投影中至多拥有一条事件链，链头 ``head_event_id``
  是唯一的当前裁定。延长与恢复必须引用当前链头；链已结束后只能提交新的关闭。
* 三种时间都不能制造“结束早于开始”的窗口：

  - 开放时刻：恢复事件的 ``effective_from``（含恢复缓冲后的窗口末端）不得早于
    关闭链的开始时刻；
  - 来源报告时刻：沿同一事件链，``reported_at`` 必须因果递增；
  - 接收顺序：所有提交在 ``BEGIN IMMEDIATE`` 事务下串行裁定，版本号沿链头严格
    递增，迟到且与当前裁定矛盾的材料被记录为拒绝，而不是覆盖当前状态。

* 所有读取（事件状态、机场汇总、受影响航班）都在同一只读快照内、基于同一投影
  版本重放链头当前代的影响快照。

历史材料的更正在 :class:`~app.corrections` 流程中处理：原始事件与影响代际快照
永不覆盖；会改变已发布结果的更正进入待复核，批准后写入新一代重算快照。
"""

from __future__ import annotations

import json
from dataclasses import replace as replace_event
from datetime import datetime, timezone
from typing import Any

from app.corrections import validate_proposal
from app.errors import (
    EventConflictError,
    ForbiddenError,
    NotFoundError,
    ValidationError,
)
from app.engine import compute_impacts
from app.models import iso_utc
from app.models import (
    EVENT_CLOSED,
    EVENT_EXTENDED,
    EVENT_REOPENED,
    REVIEW_APPROVED,
    REVIEW_PENDING,
    REVIEW_REJECTED,
    STATE_PROCESSED,
    STATE_REJECTED,
    Airport,
    DisruptionEvent,
    Flight,
)
from app.repository import Repository
from app.validation import validate_event

# Reviewers carrying one of these roles may adjudicate correction proposals.
DECISION_ROLES = frozenset({"operations_reviewer", "safety_reviewer"})

CHAIN_ACTIVE = "active"
CHAIN_RESOLVED = "resolved"
CHAIN_CORRECTED = "corrected"
CHAIN_IDLE = "idle"


class _DeferredRejection:
    """事务成功提交后再向调用方抛出的拒绝结果。

    拒绝裁定必须持久化，因此不能在 ``with transaction`` 内抛出（那会触发
    ROLLBACK）；先提交，再在事务块外抛出对应的 ValidationError。
    """

    __slots__ = ("error",)

    def __init__(self, error: ValidationError):
        self.error = error


class DisruptionService:
    def __init__(
        self,
        repo: Repository,
        airports: dict[str, Airport],
        flights: dict[str, Flight],
        reviewers: dict[str, frozenset[str]] | None = None,
    ):
        self._repo = repo
        self._airports = airports
        self._flights = flights
        self._reviewers = reviewers or {}

    def healthy(self) -> bool:
        return self._repo.ping()

    # ------------------------------------------------------------------ #
    # Event intake
    # ------------------------------------------------------------------ #

    def submit_event(self, payload: Any) -> dict[str, Any]:
        # Structural + basic semantic validation happens before any DB write;
        # malformed intake is rejected and leaves no record.
        event = validate_event(payload, self._airports)

        deferred_error: ValidationError | None = None
        with self._repo.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM events WHERE event_id = ?",
                (event.event_id,),
            ).fetchone()
            if existing is not None:
                outcome = self._handle_duplicate(conn, event, existing)
                if isinstance(outcome, _DeferredRejection):
                    deferred_error = outcome.error
                else:
                    return outcome
            else:
                # Structural chain problems (unknown reference, wrong airport)
                # raise here and roll the transaction back: nothing is recorded.
                violations = self._adjudicate(conn, event)
                if violations:
                    version = self._persist_rejection(conn, event, violations)
                    deferred_error = ValidationError(
                        "Event contradicted the adjudicated airport state and was "
                        "rejected; the decision has been recorded for review",
                        {
                            "errors": violations,
                            "processing_state": STATE_REJECTED,
                            "event_id": event.event_id,
                            "projection_version": version,
                        },
                    )
                else:
                    return self._accept(conn, event)

        # The rejection decision committed successfully; surface it as 422.
        raise deferred_error

    # ------------------------------------------------------------------ #
    # Adjudication
    # ------------------------------------------------------------------ #

    def _adjudicate(self, conn, event: DisruptionEvent) -> list[dict[str, str]]:
        """对照当前机场投影裁定新提交。

        返回违反项列表；为空表示接受。引用不存在/机场不符等“材料本身不完整”的
        问题属于结构性错误，由调用方直接以 422 拒绝且不落库；这里只处理材料完整、
        但与当前裁定矛盾的情况，拒绝会被持久化为可审计决定。
        """
        airport = event.airport_code
        state = conn.execute(
            "SELECT * FROM airport_state WHERE airport_code = ?", (airport,)
        ).fetchone()

        if event.event_type == EVENT_CLOSED:
            if state is not None and state["chain_state"] == CHAIN_ACTIVE:
                return [
                    {
                        "field": "event_type",
                        "issue": "close_while_chain_active",
                        "head_event_id": state["head_event_id"],
                    }
                ]
            last_version = state["last_event_version"] if state is not None else 0
            if event.event_version <= last_version:
                return [
                    {
                        "field": "event_version",
                        "issue": "must_extend_airport_history",
                        "stored_version": str(last_version),
                        "received_version": str(event.event_version),
                    }
                ]
            return []

        # extended / reopened: the referenced material must exist and belong here
        ref_id = event.supersedes_event_id
        ref = conn.execute(
            "SELECT * FROM events WHERE event_id = ?", (ref_id,)
        ).fetchone()
        if ref is None:
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
            raise ValidationError(
                "Event failed chain validation",
                {
                    "errors": [
                        {
                            "field": "supersedes_event_id",
                            "issue": "airport_mismatch",
                            "referenced_airport": ref["airport_code"],
                            "received_airport": event.airport_code,
                        }
                    ]
                },
            )
        if ref["processing_state"] != STATE_PROCESSED:
            return [
                {
                    "field": "supersedes_event_id",
                    "issue": "cannot_build_on_rejected_material",
                    "event_id": ref_id,
                }
            ]

        violations: list[dict[str, str]] = []

        if state is None:
            # Only possible when no accepted history exists at this airport.
            return [
                {
                    "field": "supersedes_event_id",
                    "issue": "no_active_chain",
                    "event_id": ref_id,
                }
            ]
        if state["chain_state"] != CHAIN_ACTIVE:
            violations.append(
                {
                    "field": "supersedes_event_id",
                    "issue": "chain_already_closed",
                    "head_event_id": state["head_event_id"],
                }
            )
        if state["head_event_id"] != ref_id:
            violations.append(
                {
                    "field": "supersedes_event_id",
                    "issue": "must_supersede_chain_head",
                    "head_event_id": state["head_event_id"],
                    "received_event_id": ref_id,
                }
            )
        if event.event_version <= state["last_event_version"]:
            violations.append(
                {
                    "field": "event_version",
                    "issue": "version_must_increase",
                    "stored_version": str(state["last_event_version"]),
                    "received_version": str(event.event_version),
                }
            )

        # Source-report clock: reports along a chain must be causally ordered.
        if event.reported_at < parse_ts(ref["reported_at"]):
            violations.append(
                {
                    "field": "reported_at",
                    "issue": "report_precedes_previous_report",
                    "previous_reported_at": ref["reported_at"],
                    "received_reported_at": event.reported_at.strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                }
            )

        root = self._load_root(conn, state, ref)
        # The head itself may carry an approved correction patch; extension
        # windows must be judged against its corrected effective end.
        head_event = self._effective_event(conn, _row_to_event(ref))
        if root is not None:
            if event.event_type == EVENT_REOPENED:
                # Open-time clock: the airport must not reopen before the chain
                # ever closed. Including the operational buffer, the effective
                # window end must not precede the window start.
                if event.effective_from < root.effective_from:
                    violations.append(
                        {
                            "field": "effective_from",
                            "issue": "reopen_before_chain_start",
                            "chain_start": root.effective_from.strftime(
                                "%Y-%m-%dT%H:%M:%SZ"
                            ),
                            "received_reopen_at": event.effective_from.strftime(
                                "%Y-%m-%dT%H:%M:%SZ"
                            ),
                        }
                    )
            if event.event_type == EVENT_EXTENDED:
                self._adjudicate_extension(event, head_event, root, violations)

        return violations

    def _adjudicate_extension(
        self,
        event: DisruptionEvent,
        head: DisruptionEvent,
        root: DisruptionEvent,
        violations: list[dict[str, str]],
    ) -> None:
        if event.effective_from < root.effective_from:
            violations.append(
                {
                    "field": "effective_from",
                    "issue": "must_not_precede_chain_start",
                    "chain_start": root.effective_from.strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                }
            )
        prev_until = head.effective_until
        if prev_until is None:
            return
        prev_until_raw = iso_utc(prev_until)
        if event.effective_from > prev_until:
            violations.append(
                {
                    "field": "effective_from",
                    "issue": "extension_leaves_uncovered_gap",
                    "previous_until": prev_until_raw,
                }
            )
        if event.effective_until <= prev_until:
            violations.append(
                {
                    "field": "effective_until",
                    "issue": "must_extend_previous_window",
                    "previous_until": prev_until_raw,
                }
            )

    def _load_root(self, conn, state, ref) -> DisruptionEvent | None:
        """Load the chain's root event with approved corrections applied."""
        root_id = state["root_event_id"] if state is not None else None
        if root_id is None:
            return None
        row = conn.execute(
            "SELECT * FROM events WHERE event_id = ?", (root_id,)
        ).fetchone()
        if row is None:  # defensive
            return None
        return self._effective_event(conn, _row_to_event(row))

    def _effective_event(self, conn, event: DisruptionEvent) -> DisruptionEvent:
        """Apply all approved corrections to an event, oldest patch first.

        The events table keeps the original submission immutable; each approved
        correction stores only the fields it changed. To reconstruct the current
        effective event, patches are layered in decision order so a later patch
        that omits a field does not silently revert an earlier correction.
        """
        rows = conn.execute(
            "SELECT patch_json FROM correction_proposals "
            "WHERE target_event_id = ? AND status = 'approved' "
            "ORDER BY decided_at ASC, request_id ASC",
            (event.event_id,),
        ).fetchall()
        if not rows:
            return event
        changes: dict[str, Any] = {}
        for row in rows:
            patch = json.loads(row["patch_json"])
            if "airport_code" in patch:
                changes["airport_code"] = patch["airport_code"]
            if "effective_from" in patch and patch["effective_from"] is not None:
                changes["effective_from"] = parse_ts(patch["effective_from"])
            if "effective_until" in patch:
                until = patch["effective_until"]
                changes["effective_until"] = (
                    None if until is None else parse_ts(until)
                )
        return replace_event(event, **changes) if changes else event

    # ------------------------------------------------------------------ #
    # Accept / reject side effects
    # ------------------------------------------------------------------ #

    def _accept(self, conn, event: DisruptionEvent) -> dict[str, Any]:
        state = conn.execute(
            "SELECT * FROM airport_state WHERE airport_code = ?",
            (event.airport_code,),
        ).fetchone()

        if event.event_type == EVENT_CLOSED:
            root = event
            prev_head = None
            generation = 1 if state is None else int(state["generation"]) + 1
        else:
            root = self._effective_event(
                conn,
                _row_to_event(
                    conn.execute(
                        "SELECT * FROM events WHERE event_id = ?",
                        (state["root_event_id"],),
                    ).fetchone()
                ),
            )
            prev_head = conn.execute(
                "SELECT * FROM events WHERE event_id = ?",
                (state["head_event_id"],),
            ).fetchone()
            generation = int(state["generation"]) + 1

        airport = self._airports[event.airport_code]
        impacts = compute_impacts(event, root, airport, self._flights)
        impacts.extend(
            self._resolved_tombstones(conn, event, root, prev_head, generation, impacts)
        )

        chain_state = (
            CHAIN_RESOLVED if event.event_type == EVENT_REOPENED else CHAIN_ACTIVE
        )
        scope = self._chain_event_ids(conn, root.event_id, event.event_id)
        version = self._repo.append_projection(
            conn,
            airport_code=event.airport_code,
            event_id=event.event_id,
            kind="event_accepted",
            detail={
                "event_type": event.event_type,
                "chain_state": chain_state,
                "generation": generation,
                "root_event_id": root.event_id,
                "replay_scope": scope,
            },
        )
        decision = {"outcome": "accepted", "reasons": []}
        self._repo.insert_event(
            conn,
            event.to_dict(),
            processing_state=STATE_PROCESSED,
            decision=decision,
            projection_version=version,
        )
        if impacts:
            self._repo.insert_impacts(
                conn,
                impacts,
                generation=generation,
                projection_version=version,
            )
        self._repo.upsert_airport_state(
            conn,
            airport_code=event.airport_code,
            root_event_id=root.event_id,
            head_event_id=event.event_id,
            chain_state=chain_state,
            last_event_version=event.event_version,
            generation=generation,
            projection_version=version,
        )
        return self._result(event, impacts, replayed=False, version=version)

    def _persist_rejection(
        self, conn, event: DisruptionEvent, reasons: list[dict[str, str]]
    ) -> int:
        """写入拒绝裁定与投影日志；不产生任何事件链副作用。"""
        version = self._repo.append_projection(
            conn,
            airport_code=event.airport_code,
            event_id=event.event_id,
            kind="event_rejected",
            detail={"reasons": reasons},
        )
        decision = {"outcome": "rejected", "reasons": reasons}
        self._repo.insert_event(
            conn,
            event.to_dict(),
            processing_state=STATE_REJECTED,
            decision=decision,
            projection_version=version,
        )
        self._repo.ensure_airport_state(conn, event.airport_code)
        return version

    def _resolved_tombstones(
        self,
        conn,
        event: DisruptionEvent,
        root: DisruptionEvent,
        prev_head,
        generation: int,
        impacts: list[dict],
    ) -> list[dict[str, Any]]:
        """同链中链头快照曾受影响、本事件后不再受影响的航班。"""
        if event.event_type == EVENT_CLOSED or prev_head is None:
            return []
        # Only tombstone within the same chain; a new closure never resolves the
        # previous (already terminal) chain's rows.
        rows = conn.execute(
            "SELECT * FROM impacts WHERE event_id = ? AND generation = ? "
            "AND root_event_id = ? AND impact_status != 'resolved'",
            (prev_head["event_id"], generation - 1, root.event_id),
        ).fetchall()
        still_affected = {r["flight_id"] for r in impacts}
        tombstones: list[dict[str, Any]] = []
        for r in rows:
            if r["flight_id"] in still_affected:
                continue
            flight = self._flights.get(r["flight_id"])
            if flight is None:
                continue
            tombstones.append(
                {
                    "event_id": event.event_id,
                    "root_event_id": root.event_id,
                    "airport_code": event.airport_code,
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

    def _chain_event_ids(
        self, conn, root_event_id: str, head_event_id: str
    ) -> list[str]:
        ids: list[str] = []
        current = head_event_id
        seen: set[str] = set()
        while current is not None:
            if current in seen:
                break
            seen.add(current)
            ids.append(current)
            if current == root_event_id:
                break
            row = conn.execute(
                "SELECT supersedes_event_id FROM events WHERE event_id = ?",
                (current,),
            ).fetchone()
            current = row["supersedes_event_id"] if row is not None else None
        ids.reverse()
        return ids

    def _handle_duplicate(
        self, conn, event: DisruptionEvent, existing
    ) -> Any:
        stored_payload = json.loads(existing["payload_json"])
        same_body = stored_payload == event.to_dict()
        if same_body:
            if existing["processing_state"] == STATE_REJECTED:
                # Idempotent retry of a rejected submission: return the original
                # rejection decision rather than recomputing it.
                self._repo.increment_replay(conn, event.event_id)
                decision = json.loads(existing["decision_json"] or "{}")
                error = ValidationError(
                    "Duplicate of a previously rejected submission; "
                    "the original rejection decision is returned",
                    {
                        "errors": decision.get("reasons", []),
                        "processing_state": STATE_REJECTED,
                        "event_id": event.event_id,
                        "projection_version": existing["projection_version"],
                        "replayed": True,
                    },
                )
                return _DeferredRejection(error)
            # Idempotent retry: return the original result, bump the counter.
            self._repo.increment_replay(conn, event.event_id)
            impacts = self._repo.latest_generation_impacts(event.event_id)
            return self._result(
                event,
                [dict(r) for r in impacts],
                replayed=True,
                version=existing["projection_version"],
            )

        # Same identity, different content.
        if event.event_version == existing["event_version"]:
            raise EventConflictError(
                f"Event '{event.event_id}' version {existing['event_version']} "
                "already exists with a different payload",
                {
                    "event_id": event.event_id,
                    "stored_version": existing["event_version"],
                    "received_version": event.event_version,
                    "issue": "payload_mismatch",
                },
            )
        raise EventConflictError(
            f"Event '{event.event_id}' already exists at version "
            f"{existing['event_version']}; new versions must use a new event_id "
            "and reference the previous one via supersedes_event_id",
            {
                "event_id": event.event_id,
                "stored_version": existing["event_version"],
                "received_version": event.event_version,
                "issue": "event_id_reuse",
            },
        )

    # ------------------------------------------------------------------ #
    # Correction proposals
    # ------------------------------------------------------------------ #

    def submit_correction(self, payload: Any) -> dict[str, Any]:
        proposal_in = validate_proposal(payload, self._airports)

        with self._repo.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM correction_proposals WHERE request_id = ?",
                (proposal_in["request_id"],),
            ).fetchone()
            if existing is not None:
                return self._handle_duplicate_proposal(conn, proposal_in, existing)

            target = conn.execute(
                "SELECT * FROM events WHERE event_id = ?",
                (proposal_in["target_event_id"],),
            ).fetchone()
            if target is None:
                raise NotFoundError(
                    f"Target event '{proposal_in['target_event_id']}' was not found",
                    {"field": "target_event_id"},
                )
            if target["processing_state"] != STATE_PROCESSED:
                raise ValidationError(
                    "Only accepted events can be corrected",
                    {
                        "errors": [
                            {
                                "field": "target_event_id",
                                "issue": "target_not_adjudicated",
                                "processing_state": target["processing_state"],
                            }
                        ]
                    },
                )

            state = conn.execute(
                "SELECT * FROM airport_state WHERE airport_code = ?",
                (target["airport_code"],),
            ).fetchone()
            base = proposal_in["base_projection_version"]
            if state is None or int(state["projection_version"]) != int(base):
                current = state["projection_version"] if state is not None else 0
                raise EventConflictError(
                    "Correction is based on a stale projection version",
                    {
                        "field": "base_projection_version",
                        "submitted_version": base,
                        "current_version": current,
                        "issue": "stale_projection",
                    },
                )

            chain = self._load_chain(conn, state["root_event_id"], state["head_event_id"])
            target_ids = [r["event_id"] for r in chain]
            if target["event_id"] not in target_ids:
                # Defensive: state and event rows disagree.
                raise ValidationError(
                    "Target event is not part of the airport's adjudicated chain",
                    {
                        "errors": [
                            {
                                "field": "target_event_id",
                                "issue": "not_in_active_history",
                            }
                        ]
                    },
                )

            patch = proposal_in["patch"]
            airport_change = "airport_code" in patch and (
                patch["airport_code"] != target["airport_code"]
            )
            target_index = target_ids.index(target["event_id"])
            if airport_change and (
                target["event_type"] != EVENT_CLOSED or len(chain) > 1
            ):
                raise ValidationError(
                    "airport_code can only be corrected on a sole root closure",
                    {
                        "errors": [
                            {
                                "field": "patch.airport_code",
                                "issue": "airport_change_restricted_to_root",
                            }
                        ]
                    },
                )
            if airport_change:
                dest_state = conn.execute(
                    "SELECT * FROM airport_state WHERE airport_code = ?",
                    (patch["airport_code"],),
                ).fetchone()
                if dest_state is not None and dest_state["chain_state"] == CHAIN_ACTIVE:
                    raise ValidationError(
                        "Destination airport already has an active closure chain",
                        {
                            "errors": [
                                {
                                    "field": "patch.airport_code",
                                    "issue": "destination_chain_active",
                                    "airport_code": patch["airport_code"],
                                }
                            ]
                        },
                    )

            hypothetical = self._hypothetical_chain(conn, chain, target, patch)
            if hypothetical is None:
                raise ValidationError(
                    "Corrected event fails event validation",
                    {
                        "errors": [
                            {
                                "field": "patch",
                                "issue": "corrected_event_invalid",
                            }
                        ]
                    },
                )
            chain_events, snapshots = hypothetical
            scope_ids = target_ids[target_index:]

            delta = self._impact_delta(
                conn, state, chain_events, snapshots, target_index, airport_change
            )
            changes_published = bool(delta["added"] or delta["removed"] or delta["changed"])

            version = self._repo.append_projection(
                conn,
                airport_code=target["airport_code"],
                event_id=target["event_id"],
                kind="correction_proposed",
                detail={
                    "request_id": proposal_in["request_id"],
                    "target_event_id": target["event_id"],
                    "base_projection_version": base,
                    "changes_published_result": changes_published,
                    "replay_scope": scope_ids,
                },
            )
            record = {
                "request_id": proposal_in["request_id"],
                "target_event_id": target["event_id"],
                "airport_code": target["airport_code"],
                "base_projection_version": base,
                "patch": self._patch_json(patch),
                "submitted_by": proposal_in["submitted_by"],
                "reason": proposal_in.get("reason"),
                "status": REVIEW_PENDING,
                "changes_published_result": changes_published,
                "impact_delta": delta,
                "replay_scope": scope_ids,
                "payload": proposal_in["raw"],
                "projection_version": version,
            }
            self._repo.insert_proposal(conn, record)
            self._repo.ensure_airport_state(conn, target["airport_code"])
            return self._proposal_dict(record, version=version, replay_count=0)

    def decide_correction(
        self,
        request_id: str,
        payload: Any,
    ) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValidationError("Decision payload must be a JSON object")
        decision = payload.get("decision")
        reviewer_id = payload.get("reviewer_id")
        comment = payload.get("comment")
        errors: list[dict[str, str]] = []
        if decision not in ("approved", "rejected"):
            errors.append(
                {"field": "decision", "issue": "invalid_enum_value",
                 "allowed": "approved, rejected"}
            )
        if not isinstance(reviewer_id, str) or not reviewer_id.strip():
            errors.append({"field": "reviewer_id", "issue": "required"})
        if comment is not None and not isinstance(comment, str):
            errors.append({"field": "comment", "issue": "must_be_string"})
        if errors:
            raise ValidationError("Correction decision failed validation", {"errors": errors})

        roles = self._reviewers.get(reviewer_id)
        if roles is None:
            raise ForbiddenError(
                f"Unknown reviewer '{reviewer_id}'",
                {"field": "reviewer_id", "received": reviewer_id},
            )
        if not DECISION_ROLES.intersection(roles):
            raise ForbiddenError(
                f"Reviewer '{reviewer_id}' may not adjudicate corrections",
                {
                    "field": "reviewer_id",
                    "received": reviewer_id,
                    "required_role": sorted(DECISION_ROLES),
                },
            )

        with self._repo.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM correction_proposals WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(
                    f"Correction proposal '{request_id}' was not found",
                    {"request_id": request_id},
                )
            if row["status"] != REVIEW_PENDING:
                raise EventConflictError(
                    f"Proposal '{request_id}' is already {row['status']}",
                    {
                        "request_id": request_id,
                        "status": row["status"],
                        "issue": "already_decided",
                    },
                )

            if decision == "rejected":
                version = self._repo.append_projection(
                    conn,
                    airport_code=row["airport_code"],
                    event_id=row["target_event_id"],
                    kind="correction_rejected",
                    detail={"request_id": request_id, "reviewer_id": reviewer_id},
                )
                self._repo.decide_proposal(
                    conn,
                    request_id=request_id,
                    status=REVIEW_REJECTED,
                    reviewer_id=reviewer_id,
                    comment=comment,
                    generation=None,
                    projection_version=version,
                )
                self._repo.ensure_airport_state(conn, row["airport_code"])
                fresh = conn.execute(
                    "SELECT * FROM correction_proposals WHERE request_id = ?",
                    (request_id,),
                ).fetchone()
                return self._proposal_row_dict(fresh)

            return self._approve_correction(conn, row, reviewer_id, comment)

    def _approve_correction(self, conn, row, reviewer_id: str, comment: str | None):
        patch = json.loads(row["patch_json"])
        state = conn.execute(
            "SELECT * FROM airport_state WHERE airport_code = ?",
            (row["airport_code"],),
        ).fetchone()
        if state is None or int(state["projection_version"]) != int(
            row["base_projection_version"]
        ):
            current = state["projection_version"] if state is not None else 0
            raise EventConflictError(
                "Airport state moved since the proposal was submitted; "
                "the correction must be resubmitted on the current projection",
                {
                    "request_id": row["request_id"],
                    "submitted_version": row["base_projection_version"],
                    "current_version": current,
                    "issue": "stale_projection",
                },
            )

        target = conn.execute(
            "SELECT * FROM events WHERE event_id = ?",
            (row["target_event_id"],),
        ).fetchone()
        chain = self._load_chain(conn, state["root_event_id"], state["head_event_id"])
        hypothetical = self._hypothetical_chain(conn, chain, target, patch)
        if hypothetical is None:
            raise ValidationError(
                "Corrected event fails event validation",
                {"errors": [{"field": "patch", "issue": "corrected_event_invalid"}]},
            )
        chain_events, snapshots = hypothetical
        target_ids = [r["event_id"] for r in chain]
        target_index = target_ids.index(target["event_id"])
        scope_ids = target_ids[target_index:]
        airport_change = (
            patch.get("airport_code") is not None
            and patch["airport_code"] != target["airport_code"]
        )

        new_generation = int(state["generation"]) + 1
        version = self._repo.append_projection(
            conn,
            airport_code=target["airport_code"],
            event_id=target["event_id"],
            kind="correction_approved",
            detail={
                "request_id": row["request_id"],
                "target_event_id": target["event_id"],
                "generation": new_generation,
                "replay_scope": scope_ids,
                "airport_change": patch.get("airport_code")
                if airport_change
                else None,
                "reviewer_id": reviewer_id,
            },
        )

        impact_airport = (
            patch["airport_code"] if airport_change else target["airport_code"]
        )
        # Write new, immutable generation snapshots for every event in scope.
        # A snapshot must be complete relative to that event's previous
        # generation, so flights the correction removes are recorded as
        # resolved tombstones (even when every impact disappears), guaranteeing
        # the new generation is never empty and cannot fall back to stale rows.
        for idx, event_obj in enumerate(chain_events):
            if idx < target_index:
                continue
            prev_generation_row = conn.execute(
                "SELECT MAX(generation) AS g FROM impacts WHERE event_id = ?",
                (event_obj.event_id,),
            ).fetchone()
            event_prev_gen = int(prev_generation_row["g"] or state["generation"])
            complete = self._with_generation_tombstones(
                conn,
                event_obj=event_obj,
                root_obj=chain_events[0],
                new_rows=snapshots[idx],
                prev_generation=event_prev_gen,
                airport_change=airport_change,
                old_airport=target["airport_code"],
            )
            self._repo.insert_impacts(
                conn,
                complete,
                generation=new_generation,
                proposal_id=row["request_id"],
                projection_version=version,
            )

        if airport_change:
            # Old airport: the closure is corrected away; history rows stay.
            self._repo.upsert_airport_state(
                conn,
                airport_code=target["airport_code"],
                root_event_id=None,
                head_event_id=None,
                chain_state=CHAIN_CORRECTED,
                last_event_version=int(state["last_event_version"]),
                generation=new_generation,
                projection_version=version,
            )
            dest = conn.execute(
                "SELECT * FROM airport_state WHERE airport_code = ?",
                (impact_airport,),
            ).fetchone()
            dest_generation = (
                int(dest["generation"]) + 1 if dest is not None else new_generation
            )
            self._repo.upsert_airport_state(
                conn,
                airport_code=impact_airport,
                root_event_id=target["event_id"],
                head_event_id=target["event_id"],
                chain_state=CHAIN_ACTIVE,
                last_event_version=target["event_version"],
                generation=dest_generation,
                projection_version=version,
            )
        else:
            self._repo.upsert_airport_state(
                conn,
                airport_code=target["airport_code"],
                root_event_id=state["root_event_id"],
                head_event_id=state["head_event_id"],
                chain_state=state["chain_state"],
                last_event_version=int(state["last_event_version"]),
                generation=new_generation,
                projection_version=version,
            )

        self._repo.decide_proposal(
            conn,
            request_id=row["request_id"],
            status=REVIEW_APPROVED,
            reviewer_id=reviewer_id,
            comment=comment,
            generation=new_generation,
            projection_version=version,
        )
        fresh = conn.execute(
            "SELECT * FROM correction_proposals WHERE request_id = ?",
            (row["request_id"],),
        ).fetchone()
        return self._proposal_row_dict(fresh)

    def _hypothetical_chain(
        self, conn, chain_rows: list, target_row, patch: dict[str, Any]
    ) -> tuple[list[DisruptionEvent], list[list[dict[str, Any]]]] | None:
        """Replay the chain with ``patch`` applied to the target event.

        Already-approved corrections on other chain events stay in effect, so a
        second correction layers on top of the first rather than reverting it.
        Returns the ordered chain events and a full impact snapshot per event,
        or ``None`` if the patched material is not a valid event.
        """
        events: list[DisruptionEvent] = [
            self._effective_event(conn, _row_to_event(r)) for r in chain_rows
        ]
        target_index = next(
            i for i, e in enumerate(events) if e.event_id == target_row["event_id"]
        )
        # Layer the new patch on top of the current effective (already possibly
        # corrected) values; absent patch fields keep their current value.
        try:
            patched_dict = events[target_index].to_dict()
            if "airport_code" in patch:
                patched_dict["airport_code"] = patch["airport_code"]
            if "effective_from" in patch:
                patched_dict["effective_from"] = _as_event_iso(
                    patch["effective_from"]
                )
            if "effective_until" in patch:
                until = patch["effective_until"]
                patched_dict["effective_until"] = (
                    None if until is None else _as_event_iso(until)
                )
            corrected = validate_event(patched_dict, self._airports)
        except ValidationError:
            return None
        events[target_index] = corrected

        root = events[0]
        # When an airport change moves a sole root, all computations happen at
        # the destination airport.
        airport = self._airports[root.airport_code]
        snapshots: list[list[dict[str, Any]]] = []
        previous: list[dict[str, Any]] = []
        for idx, event_obj in enumerate(events):
            impacts = compute_impacts(event_obj, root, airport, self._flights)
            if idx > 0:
                still = {r["flight_id"] for r in impacts}
                for prior in previous:
                    if (
                        prior["flight_id"] not in still
                        and prior["impact_status"] != "resolved"
                    ):
                        impacts.append(
                            self._tombstone_dict(event_obj, root, prior)
                        )
            impacts.sort(key=lambda r: r["flight_id"])
            snapshots.append(impacts)
            previous = impacts
        return events, snapshots

    def _impact_delta(
        self,
        conn,
        state,
        chain_events: list[DisruptionEvent],
        snapshots: list[list[dict[str, Any]]],
        target_index: int,
        airport_change: bool,
    ) -> dict[str, Any]:
        head_id = state["head_event_id"]
        old_rows = conn.execute(
            "SELECT * FROM impacts WHERE event_id = ? AND generation = ? "
            "AND impact_status != 'resolved' ORDER BY flight_id",
            (head_id, int(state["generation"])),
        ).fetchall()
        old = {r["flight_id"]: dict(r) for r in old_rows}
        new_head = snapshots[-1]
        new_airport = chain_events[0].airport_code
        new = {
            r["flight_id"]: r
            for r in new_head
            if r["impact_status"] != "resolved"
            and (airport_change or r["airport_code"] == state["airport_code"])
        }

        def brief(row: dict[str, Any]) -> dict[str, Any]:
            return {
                "flight_id": row["flight_id"],
                "airport_code": row["airport_code"],
                "impact_status": row["impact_status"],
                "delay_minutes": row.get("delay_minutes"),
            }

        added = [brief(new[k]) for k in sorted(set(new) - set(old))]
        removed = [brief(old[k]) for k in sorted(set(old) - set(new))]
        changed: list[dict[str, Any]] = []
        for key in sorted(set(old) & set(new)):
            before, after = old[key], new[key]
            if (
                before["impact_status"] != after["impact_status"]
                or before["delay_minutes"] != after["delay_minutes"]
            ):
                changed.append(
                    {
                        "flight_id": key,
                        "airport_code": after["airport_code"],
                        "from_status": before["impact_status"],
                        "to_status": after["impact_status"],
                        "from_delay_minutes": before["delay_minutes"],
                        "to_delay_minutes": after["delay_minutes"],
                    }
                )
        return {"added": added, "removed": removed, "changed": changed}

    def _with_generation_tombstones(
        self,
        conn,
        *,
        event_obj: DisruptionEvent,
        root_obj: DisruptionEvent,
        new_rows: list[dict[str, Any]],
        prev_generation: int,
        airport_change: bool,
        old_airport: str,
    ) -> list[dict[str, Any]]:
        """Complete a new-generation snapshot with resolved tombstones.

        Flights the event impacted in its previous generation but no longer
        impacts after the correction must be carried into the new generation as
        ``resolved`` rows. Otherwise a correction that removes every impact
        would leave the generation empty and make readers fall back to stale
        rows. Chain-internal tombstones are already present in ``new_rows``.
        """
        if airport_change:
            # The event leaves its old airport; that airport's state loses its
            # head so its previous rows are never read again.
            return new_rows
        prev_rows = conn.execute(
            "SELECT * FROM impacts WHERE event_id = ? AND generation = ? "
            "AND impact_status != 'resolved'",
            (event_obj.event_id, prev_generation),
        ).fetchall()
        current = {r["flight_id"] for r in new_rows}
        out = list(new_rows)
        for prev in prev_rows:
            if prev["flight_id"] in current:
                continue
            flight = self._flights.get(prev["flight_id"])
            if flight is None:
                continue
            out.append(
                {
                    "event_id": event_obj.event_id,
                    "root_event_id": root_obj.event_id,
                    "airport_code": old_airport,
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
        out.sort(key=lambda r: r["flight_id"])
        return out

    def _tombstone_dict(
        self, event: DisruptionEvent, root: DisruptionEvent, prior: dict[str, Any]
    ) -> dict[str, Any]:
        return {
            "event_id": event.event_id,
            "root_event_id": root.event_id,
            "airport_code": event.airport_code,
            "flight_id": prior["flight_id"],
            "flight_number": prior["flight_number"],
            "affected_endpoint": "none",
            "impact_status": "resolved",
            "overlap_minutes": 0,
            "delay_minutes": None,
            "proposed_departure": None,
            "proposed_arrival": None,
            "passenger_count": prior["passenger_count"],
            "crosses_midnight": 0,
        }

    # ------------------------------------------------------------------ #
    # Correction queries
    # ------------------------------------------------------------------ #

    def get_correction(self, request_id: str) -> dict[str, Any]:
        with self._repo.snapshot() as conn:
            row = conn.execute(
                "SELECT * FROM correction_proposals WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(
                    f"Correction proposal '{request_id}' was not found",
                    {"request_id": request_id},
                )
            return self._proposal_row_dict(row)

    def list_corrections(
        self, *, status: str | None = None, airport: str | None = None
    ) -> dict[str, Any]:
        if status is not None and status not in ("pending", "approved", "rejected"):
            raise ValidationError(
                "Unsupported proposal status filter",
                {"field": "status", "allowed": ["pending", "approved", "rejected"]},
            )
        if airport is not None and airport not in self._airports:
            raise NotFoundError(
                f"Unknown airport code '{airport}'",
                {"field": "airport", "received": airport},
            )
        with self._repo.snapshot() as conn:
            rows = conn.execute(
                self._proposal_list_sql(status, airport),
                self._proposal_list_params(status, airport),
            ).fetchall()
            version = self._current_version(conn)
            return {
                "projection_version": version,
                "count": len(rows),
                "proposals": [self._proposal_row_dict(r) for r in rows],
            }
    def review_queue(self, airport: str | None = None) -> dict[str, Any]:
        if airport is not None and airport not in self._airports:
            raise NotFoundError(
                f"Unknown airport code '{airport}'",
                {"field": "airport", "received": airport},
            )
        with self._repo.snapshot() as conn:
            rejected = conn.execute(
                "SELECT * FROM events WHERE processing_state = 'rejected'"
                + (" AND airport_code = ?" if airport else "")
                + " ORDER BY created_at, event_id",
                (airport,) if airport else (),
            ).fetchall()
            pending = conn.execute(
                "SELECT * FROM correction_proposals WHERE status = 'pending'"
                + (" AND airport_code = ?" if airport else "")
                + " ORDER BY created_at, request_id",
                (airport,) if airport else (),
            ).fetchall()
            return {
                "projection_version": self._current_version(conn),
                "rejected_events": [self._rejected_event_dict(r) for r in rejected],
                "pending_corrections": [self._proposal_row_dict(r) for r in pending],
            }

    def projection_log(
        self, *, airport: str | None = None, limit: int = 100
    ) -> dict[str, Any]:
        if airport is not None and airport not in self._airports:
            raise NotFoundError(
                f"Unknown airport code '{airport}'",
                {"field": "airport", "received": airport},
            )
        limit = max(1, min(int(limit), 500))
        with self._repo.snapshot() as conn:
            sql = "SELECT * FROM projection_log"
            params: list[Any] = []
            if airport:
                sql += " WHERE airport_code = ?"
                params.append(airport)
            sql += " ORDER BY version DESC LIMIT ?"
            params.append(limit)
            rows = conn.execute(sql, params).fetchall()
            return {
                "projection_version": self._current_version(conn),
                "entries": [
                    {
                        "version": r["version"],
                        "airport_code": r["airport_code"],
                        "event_id": r["event_id"],
                        "kind": r["kind"],
                        "detail": json.loads(r["detail_json"]),
                        "created_at": r["created_at"],
                    }
                    for r in rows
                ],
            }

    @staticmethod
    def _proposal_list_sql(status: str | None, airport: str | None) -> str:
        where = []
        if status:
            where.append("status = ?")
        if airport:
            where.append("airport_code = ?")
        sql = "SELECT * FROM correction_proposals"
        if where:
            sql += " WHERE " + " AND ".join(where)
        return sql + " ORDER BY created_at, request_id"

    @staticmethod
    def _proposal_list_params(status: str | None, airport: str | None):
        params = []
        if status:
            params.append(status)
        if airport:
            params.append(airport)
        return params

    def _handle_duplicate_proposal(self, conn, proposal_in, existing) -> dict[str, Any]:
        stored_payload = json.loads(existing["payload_json"])
        if stored_payload != proposal_in["raw"]:
            raise EventConflictError(
                f"Request '{proposal_in['request_id']}' already exists with a "
                "different payload",
                {
                    "request_id": proposal_in["request_id"],
                    "issue": "payload_mismatch",
                },
            )
        self._repo.increment_proposal_replay(conn, proposal_in["request_id"])
        fresh = conn.execute(
            "SELECT * FROM correction_proposals WHERE request_id = ?",
            (proposal_in["request_id"],),
        ).fetchone()
        return self._proposal_row_dict(fresh)

    # ------------------------------------------------------------------ #
    # Queries (all replay the same projection version in one snapshot)
    # ------------------------------------------------------------------ #

    def event_status(self, event_id: str) -> dict[str, Any]:
        with self._repo.snapshot() as conn:
            row = conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError(
                    f"Event '{event_id}' was not found", {"event_id": event_id}
                )
            version = self._current_version(conn)

            if row["processing_state"] == STATE_REJECTED:
                decision = json.loads(row["decision_json"] or "{}")
                return {
                    "event": json.loads(row["payload_json"]),
                    "processing": {
                        "state": STATE_REJECTED,
                        "replay_count": row["replay_count"],
                        "created_at": row["created_at"],
                        "projection_version": row["projection_version"],
                        "decision": decision,
                    },
                    "impacts": [],
                    "projection_version": version,
                }

            latest_gen_row = conn.execute(
                "SELECT MAX(generation) AS g, MIN(generation) AS min_g "
                "FROM impacts WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            gen = latest_gen_row["g"] or 1
            original_gen = latest_gen_row["min_g"] or gen
            impact_rows = conn.execute(
                "SELECT * FROM impacts WHERE event_id = ? AND generation = ? "
                "ORDER BY flight_id",
                (event_id, gen),
            ).fetchall()
            impacts = [self._impact_dict(r) for r in impact_rows]
            active = [i for i in impacts if i["impact_status"] != "resolved"]
            statuses: dict[str, int] = {}
            passengers = 0
            for imp in active:
                statuses[imp["impact_status"]] = (
                    statuses.get(imp["impact_status"], 0) + 1
                )
                passengers += imp["passenger_count"]
            response = {
                "event": json.loads(row["payload_json"]),
                "processing": {
                    "state": "processed",
                    "replay_count": row["replay_count"],
                    "created_at": row["created_at"],
                    "impact_count": len(active),
                    "resolved_count": len(impacts) - len(active),
                    "affected_passengers": passengers,
                    "status_breakdown": statuses,
                    "generation": gen,
                    "original_generation": original_gen,
                    "projection_version": row["projection_version"],
                },
                "impacts": active,
                "projection_version": version,
            }
            if gen > original_gen:
                proposal_id = conn.execute(
                    "SELECT proposal_id FROM impacts WHERE event_id = ? AND generation = ?",
                    (event_id, gen),
                ).fetchone()["proposal_id"]
                original_rows = conn.execute(
                    "SELECT COUNT(*) AS n FROM impacts WHERE event_id = ? "
                    "AND generation = ? AND impact_status != 'resolved'",
                    (event_id, original_gen),
                ).fetchone()["n"]
                response["correction"] = {
                    "applied_proposal_id": proposal_id,
                    "generation": gen,
                    "original_impact_count": int(original_rows),
                }
            return response

    def airport_summary(self, airport_code: str) -> dict[str, Any]:
        if airport_code not in self._airports:
            raise NotFoundError(
                f"Unknown airport code '{airport_code}'",
                {"field": "airport_code", "received": airport_code},
            )
        with self._repo.snapshot() as conn:
            rows = self._repo.accepted_events_for_airport(conn, airport_code)
            state = conn.execute(
                "SELECT * FROM airport_state WHERE airport_code = ?",
                (airport_code,),
            ).fetchone()
            latest = self._head_impacts(conn, airport=airport_code)
            version = self._current_version(conn)

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

            rejected_count = conn.execute(
                "SELECT COUNT(*) AS n FROM events WHERE airport_code = ? "
                "AND processing_state = 'rejected'",
                (airport_code,),
            ).fetchone()["n"]
            pending_count = conn.execute(
                "SELECT COUNT(*) AS n FROM correction_proposals WHERE airport_code = ? "
                "AND status = 'pending'",
                (airport_code,),
            ).fetchone()["n"]

            chain_state = state["chain_state"] if state is not None else CHAIN_IDLE
            return {
                "airport_code": airport_code,
                "airport_name": self._airports[airport_code].name,
                "event_count": len(rows),
                "chain_state": chain_state,
                "active_chains": 1 if chain_state == CHAIN_ACTIVE else 0,
                "head_event_id": state["head_event_id"] if state is not None else None,
                "generation": state["generation"] if state is not None else 0,
                "affected_flights": len(latest),
                "affected_passengers": total_passengers,
                "by_status": by_status,
                "rejected_submissions": int(rejected_count),
                "pending_corrections": int(pending_count),
                "projection_version": version,
                "airport_projection_version": state["projection_version"]
                if state is not None
                else 0,
            }

    def affected_flights(
        self,
        *,
        airport: str | None,
        status: str | None,
        limit: int,
        offset: int,
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
        with self._repo.snapshot() as conn:
            rows = self._head_impacts(conn, airport=airport, status=status)
            version = self._current_version(conn)
            total = len(rows)
            page = rows[offset : offset + limit]
            return {
                "pagination": {
                    "limit": limit,
                    "offset": offset,
                    "total": total,
                },
                "projection_version": version,
                "flights": [self._impact_dict(r) for r in page],
            }

    def _head_impacts(
        self,
        conn,
        *,
        airport: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        """读取链头当前代影响（同一投影快照）。

        已恢复开放（resolved）的链仍可能有受影响航班：其最终窗口
        ``[链开始, 恢复时刻+缓冲)`` 内的航班影响已经发生，只有墓碑航班被释放。
        因此按"存在链头"而非"链活动"过滤；``resolved`` 行排除在结果外。
        """
        where = ["s.head_event_id IS NOT NULL", "i.impact_status != 'resolved'"]
        params: list[Any] = []
        if airport:
            where.append("s.airport_code = ?")
            params.append(airport)
        if status:
            where.append("i.impact_status = ?")
            params.append(status)
        sql = f"""
            SELECT i.*
            FROM impacts i
            JOIN airport_state s
              ON i.event_id = s.head_event_id
             AND i.airport_code = s.airport_code
             AND i.generation = s.generation
            WHERE {' AND '.join(where)}
            ORDER BY i.flight_id, i.airport_code
        """
        return [dict(r) for r in conn.execute(sql, params)]

    # ------------------------------------------------------------------ #
    # Serialization helpers
    # ------------------------------------------------------------------ #

    def _result(
        self,
        event: DisruptionEvent,
        impacts: list[dict[str, Any]],
        *,
        replayed: bool,
        version: int | None,
    ) -> dict[str, Any]:
        active_impacts = [i for i in impacts if i["impact_status"] != "resolved"]
        serialized = [self._impact_dict(i) for i in active_impacts]
        statuses: dict[str, int] = {}
        passengers = 0
        for imp in serialized:
            statuses[imp["impact_status"]] = (
                statuses.get(imp["impact_status"], 0) + 1
            )
            passengers += imp["passenger_count"]
        return {
            "event_id": event.event_id,
            "event_version": event.event_version,
            "processing_state": "replayed" if replayed else "processed",
            "impact_count": len(serialized),
            "resolved_count": len(impacts) - len(active_impacts),
            "affected_passengers": passengers,
            "status_breakdown": statuses,
            "projection_version": version,
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

    def _rejected_event_dict(self, row: Any) -> dict[str, Any]:
        if not isinstance(row, dict):
            row = dict(row)
        decision = json.loads(row["decision_json"] or "{}")
        return {
            "event_id": row["event_id"],
            "event_version": row["event_version"],
            "event_type": row["event_type"],
            "airport_code": row["airport_code"],
            "processing_state": STATE_REJECTED,
            "decision": decision,
            "projection_version": row["projection_version"],
            "created_at": row["created_at"],
            "replay_count": row["replay_count"],
        }

    def _proposal_dict(
        self, record: dict[str, Any], *, version: int, replay_count: int
    ) -> dict[str, Any]:
        return {
            "request_id": record["request_id"],
            "target_event_id": record["target_event_id"],
            "airport_code": record["airport_code"],
            "base_projection_version": record["base_projection_version"],
            "patch": record["patch"],
            "status": record["status"],
            "submitted_by": record["submitted_by"],
            "reason": record.get("reason"),
            "changes_published_result": record["changes_published_result"],
            "impact_delta": record["impact_delta"],
            "replay_scope": record.get("replay_scope", []),
            "generation": None,
            "reviewer_id": None,
            "review_comment": None,
            "decided_at": None,
            "replay_count": replay_count,
            "projection_version": version,
        }

    def _proposal_row_dict(self, row: Any) -> dict[str, Any]:
        if not isinstance(row, dict):
            row = dict(row)
        return {
            "request_id": row["request_id"],
            "target_event_id": row["target_event_id"],
            "airport_code": row["airport_code"],
            "base_projection_version": row["base_projection_version"],
            "patch": json.loads(row["patch_json"]),
            "status": row["status"],
            "submitted_by": row["submitted_by"],
            "reason": row["reason"],
            "changes_published_result": bool(row["changes_published_result"]),
            "impact_delta": json.loads(row["impact_delta_json"]),
            "replay_scope": json.loads(row["replay_scope_json"]),
            "generation": row["generation"],
            "reviewer_id": row["reviewer_id"],
            "review_comment": row["review_comment"],
            "decided_at": row["decided_at"],
            "replay_count": int(row["replay_count"]),
            "created_at": row["created_at"],
            "projection_version": row["projection_version"],
        }

    @staticmethod
    def _patch_json(patch: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for key, value in patch.items():
            if key in ("effective_from", "effective_until"):
                out[key] = None if value is None else _iso(value)
            else:
                out[key] = value
        return out

    def _load_chain(self, conn, root_event_id: str, head_event_id: str) -> list:
        ids = self._chain_event_ids(conn, root_event_id, head_event_id)
        return [
            conn.execute("SELECT * FROM events WHERE event_id = ?", (i,)).fetchone()
            for i in ids
        ]

    def _current_version(self, conn) -> int:
        row = conn.execute("SELECT MAX(version) AS v FROM projection_log").fetchone()
        return int(row["v"] or 0)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _as_event_iso(value: Any) -> str:
    """接受内存中的 datetime 或从 JSON 读回的 ISO 字符串。"""
    if isinstance(value, datetime):
        return _iso(value)
    return _iso(parse_ts(value))


def parse_ts(value: str) -> datetime:
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    return datetime.fromisoformat(text).astimezone(timezone.utc)


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
