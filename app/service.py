"""处理事件接入语义、持久化协调与业务查询的应用服务。"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.errors import EventConflictError, NotFoundError, ValidationError
from app.engine import compute_impacts
from app.models import (
    EVENT_CLOSED,
    EVENT_EXTENDED,
    EVENT_REOPENED,
    Airport,
    DisruptionEvent,
    Flight,
)
from app.repository import Repository
from app.validation import validate_event


class DisruptionService:
    def __init__(
        self,
        repo: Repository,
        airports: dict[str, Airport],
        flights: dict[str, Flight],
    ):
        self._repo = repo
        self._airports = airports
        self._flights = flights

    def healthy(self) -> bool:
        return self._repo.ping()

    # ------------------------------------------------------------------ #
    # Event intake
    # ------------------------------------------------------------------ #

    def submit_event(self, payload: Any) -> dict[str, Any]:
        # Structural + basic semantic validation happens before any DB write.
        event = validate_event(payload, self._airports)

        with self._repo.transaction() as conn:
            existing = conn.execute(
                "SELECT event_version, payload_json FROM events WHERE event_id = ?",
                (event.event_id,),
            ).fetchone()

            if existing is not None:
                return self._handle_duplicate(conn, event, existing)

            self._validate_chain(conn, event)
            root = self._resolve_root(conn, event)
            impacts = compute_impacts(
                event,
                root,
                self._airports[event.airport_code],
                self._flights,
            )
            impacts.extend(self._resolved_tombstones(conn, event, root, impacts))
            self._repo.insert_event(conn, event.to_dict())
            if impacts:
                self._repo.insert_impacts(conn, impacts)
            return self._result(event, impacts, replayed=False)

    def _resolved_tombstones(
        self, conn, event: DisruptionEvent, root: DisruptionEvent, impacts: list[dict]
    ) -> list[dict[str, Any]]:
        """返回本事件生效后不再受影响、但曾出现在同链中的航班。"""
        if event.event_type == EVENT_CLOSED:
            return []
        still_affected = {r["flight_id"] for r in impacts}
        tombstones: list[dict[str, Any]] = []
        prior_ids = self._repo.prior_chain_impact_ids(conn, root.event_id)
        for flight_id in sorted(prior_ids - still_affected):
            flight = self._flights.get(flight_id)
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

    def _handle_duplicate(
        self, conn, event: DisruptionEvent, existing
    ) -> dict[str, Any]:
        stored_version = existing["event_version"]
        stored_payload = json.loads(existing["payload_json"])

        same_body = stored_payload == event.to_dict()
        if same_body:
            # Idempotent retry: return the original result, bump the counter.
            self._repo.increment_replay(conn, event.event_id)
            impacts = self._repo.get_impacts(event.event_id)
            return self._result(event, [dict(r) for r in impacts], replayed=True)

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

    def _validate_chain(self, conn, event: DisruptionEvent) -> None:
        """校验必须结合数据库现状判断的事件链规则。"""
        errors: list[dict[str, str]] = []

        def prior_versions(airport: str) -> list[int]:
            rows = conn.execute(
                "SELECT event_version FROM events WHERE airport_code = ?", (airport,)
            ).fetchall()
            return [r["event_version"] for r in rows]

        if event.event_type == EVENT_CLOSED:
            versions = prior_versions(event.airport_code)
            if versions and event.event_version <= max(versions):
                errors.append(
                    {
                        "field": "event_version",
                        "issue": "must_extend_airport_history",
                        "stored_version": str(max(versions)),
                        "received_version": str(event.event_version),
                    }
                )
            return

        # extended / reopened must reference a prior event
        ref_id = event.supersedes_event_id
        ref = None
        if ref_id is not None:
            ref = conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (ref_id,)
            ).fetchone()
            if ref is None:
                errors.append(
                    {
                        "field": "supersedes_event_id",
                        "issue": "unknown_event",
                        "event_id": ref_id,
                    }
                )
            elif ref["airport_code"] != event.airport_code:
                errors.append(
                    {
                        "field": "supersedes_event_id",
                        "issue": "airport_mismatch",
                        "referenced_airport": ref["airport_code"],
                        "received_airport": event.airport_code,
                    }
                )
            elif ref["event_type"] == EVENT_REOPENED:
                errors.append(
                    {
                        "field": "supersedes_event_id",
                        "issue": "chain_already_closed",
                        "event_id": ref_id,
                    }
                )

        if event.event_type == EVENT_EXTENDED and ref is not None and not errors:
            # An extension continues a still-active closure. When the previous
            # window had a known end, the extension must start no later than it
            # (no unmodelled open gap) and push the end further out. Extending
            # an open-ended closure simply supplies the newly known end.
            prev_until_raw = ref["effective_until"]
            if prev_until_raw is None:
                if event.effective_from < parse_ts(ref["effective_from"]):
                    errors.append(
                        {
                            "field": "effective_from",
                            "issue": "must_not_precede_chain_start",
                        }
                    )
            else:
                prev_until = parse_ts(prev_until_raw)
                if event.effective_from > prev_until:
                    errors.append(
                        {
                            "field": "effective_from",
                            "issue": "extension_leaves_uncovered_gap",
                        }
                    )
                if event.effective_until <= prev_until:
                    errors.append(
                        {
                            "field": "effective_until",
                            "issue": "must_extend_previous_window",
                        }
                    )
            if event.event_version <= ref["event_version"]:
                errors.append(
                    {
                        "field": "event_version",
                        "issue": "version_must_increase",
                        "stored_version": str(ref["event_version"]),
                        "received_version": str(event.event_version),
                    }
                )

        if event.event_type == EVENT_REOPENED and ref is not None and not errors:
            if event.event_version <= ref["event_version"]:
                errors.append(
                    {
                        "field": "event_version",
                        "issue": "version_must_increase",
                        "stored_version": str(ref["event_version"]),
                        "received_version": str(event.event_version),
                    }
                )

        if errors:
            raise ValidationError("Event failed chain validation", {"errors": errors})

    def _resolve_root(self, conn, event: DisruptionEvent) -> DisruptionEvent:
        if event.event_type == EVENT_CLOSED:
            return event
        # Walk the supersedes chain to the originating closed event.
        seen: set[str] = set()
        current = event
        while current.supersedes_event_id is not None:
            ref_id = current.supersedes_event_id
            if ref_id in seen:  # defensive; cycles are structurally prevented
                raise ValidationError("supersedes chain contains a cycle")
            seen.add(ref_id)
            row = conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (ref_id,)
            ).fetchone()
            if row is None:  # validated earlier; defensive
                raise ValidationError(f"unknown superseded event '{ref_id}'")
            current = _row_to_event(row)
            if current.event_type == EVENT_CLOSED:
                return current
        raise ValidationError("extended/reopened event chain has no closed root")

    # ------------------------------------------------------------------ #
    # Queries
    # ------------------------------------------------------------------ #

    def event_status(self, event_id: str) -> dict[str, Any]:
        row = self._repo.get_event_row(event_id)
        if row is None:
            raise NotFoundError(
                f"Event '{event_id}' was not found", {"event_id": event_id}
            )
        impact_rows = self._repo.get_impacts(event_id)
        impacts = [self._impact_dict(r) for r in impact_rows]
        active = [i for i in impacts if i["impact_status"] != "resolved"]
        statuses: dict[str, int] = {}
        passengers = 0
        for imp in active:
            statuses[imp["impact_status"]] = statuses.get(imp["impact_status"], 0) + 1
            passengers += imp["passenger_count"]
        return {
            "event": json.loads(row["payload_json"]),
            "processing": {
                "state": "processed",
                "replay_count": row["replay_count"],
                "created_at": row["created_at"],
                "impact_count": len(active),
                "resolved_count": len(impacts) - len(active),
                "affected_passengers": passengers,
                "status_breakdown": statuses,
            },
            "impacts": active,
        }

    def airport_summary(self, airport_code: str) -> dict[str, Any]:
        if airport_code not in self._airports:
            raise NotFoundError(
                f"Unknown airport code '{airport_code}'",
                {"field": "airport_code", "received": airport_code},
            )
        rows = self._repo.events_for_airport(airport_code)
        latest = self._repo.latest_impacts(airport=airport_code)
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
        chain_roots = [r["event_id"] for r in rows if r["event_type"] == EVENT_CLOSED]
        return {
            "airport_code": airport_code,
            "airport_name": self._airports[airport_code].name,
            "event_count": len(rows),
            "active_chains": len(chain_roots)
            - sum(1 for r in rows if r["event_type"] == EVENT_REOPENED),
            "affected_flights": len(latest),
            "affected_passengers": total_passengers,
            "by_status": by_status,
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
        rows = self._repo.latest_impacts(airport=airport, status=status)
        total = len(rows)
        page = rows[offset : offset + limit]
        return {
            "pagination": {
                "limit": limit,
                "offset": offset,
                "total": total,
            },
            "flights": [self._impact_dict(r) for r in page],
        }

    # ------------------------------------------------------------------ #
    # Serialization helpers
    # ------------------------------------------------------------------ #

    def _result(
        self, event: DisruptionEvent, impacts: list[dict[str, Any]], *, replayed: bool
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
