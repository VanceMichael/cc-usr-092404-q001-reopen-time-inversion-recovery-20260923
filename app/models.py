"""领域模型类型。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

# Event type constants (mirrors contracts/disruption-event.schema.json)
EVENT_CLOSED = "airport.closed"
EVENT_EXTENDED = "airport.extended"
EVENT_REOPENED = "airport.reopened"

EVENT_TYPES = (EVENT_CLOSED, EVENT_EXTENDED, EVENT_REOPENED)

# Impact statuses
IMPACT_CANCELLED = "cancelled"
IMPACT_DELAYED = "delayed"
IMPACT_PENDING = "pending_confirmation"
# Internal lifecycle marker: a flight that an earlier event of the same chain
# impacted but the current chain state no longer affects. Kept out of client
# facing summaries; used so "latest snapshot" queries cannot resurrect stale
# cancelled/delayed rows after a reopening.
IMPACT_RESOLVED = "resolved"

ACTIVE_IMPACT_STATUSES = (IMPACT_CANCELLED, IMPACT_DELAYED, IMPACT_PENDING)

# Event processing states
STATE_PROCESSED = "processed"
STATE_REPLAYED = "replayed"

# Intake / correction adjudication states
INTAKE_ADOPTED = "adopted"
INTAKE_REJECTED = "rejected"

CORRECTION_PENDING = "pending_review"
CORRECTION_APPROVED = "approved"
CORRECTION_REJECTED = "rejected"
CORRECTION_STATES = (CORRECTION_PENDING, CORRECTION_APPROVED, CORRECTION_REJECTED)

DECISION_APPROVE = "approved"
DECISION_REJECT = "rejected"

# Projection journal entry kinds
JOURNAL_EVENT_ADOPTED = "event_adopted"
JOURNAL_EVENT_REJECTED = "event_rejected"
JOURNAL_CORRECTION_PROPOSED = "correction_proposed"
JOURNAL_CORRECTION_APPROVED = "correction_adopted"
JOURNAL_CORRECTION_REJECTED = "correction_rejected"

CLOSED_TYPES = (EVENT_CLOSED, EVENT_EXTENDED)

# Reviewer roles allowed to adjudicate corrections; auditors are read-only.
REVIEWER_ROLES = ("operations_reviewer", "safety_reviewer")
AUDITOR_ROLE = "audit_reader"


@dataclass(frozen=True)
class Airport:
    code: str
    name: str
    timezone: str
    reopen_buffer_minutes: int


@dataclass(frozen=True)
class Flight:
    flight_id: str
    flight_number: str
    origin: str
    destination: str
    scheduled_departure: datetime  # always timezone-aware UTC
    scheduled_arrival: datetime  # always timezone-aware UTC
    passenger_count: int
    can_retime: bool
    max_delay_minutes: int

    @property
    def duration_minutes(self) -> int:
        return int((self.scheduled_arrival - self.scheduled_departure).total_seconds() // 60)


@dataclass(frozen=True)
class DisruptionEvent:
    """已经校验的中断事件，所有时间均为带时区的 UTC。"""

    event_id: str
    event_version: int
    event_type: str
    airport_code: str
    effective_from: datetime
    effective_until: datetime | None
    reported_at: datetime
    supersedes_event_id: str | None
    reason: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_version": self.event_version,
            "event_type": self.event_type,
            "airport_code": self.airport_code,
            "effective_from": iso_utc(self.effective_from),
            "effective_until": iso_utc(self.effective_until) if self.effective_until else None,
            "reported_at": iso_utc(self.reported_at),
            "supersedes_event_id": self.supersedes_event_id,
            "reason": self.reason,
        }


def iso_utc(dt: datetime) -> str:
    """将带时区时间输出为规范的 UTC ISO 8601 字符串。"""
    from app.timeutil import to_utc

    dt = to_utc(dt)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
