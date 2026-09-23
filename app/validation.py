"""中断事件载荷的严格校验。

校验分为两个阶段：

1. 按 contracts/disruption-event.schema.json 检查类型、枚举、格式、必填字段
   以及额外属性。
2. 检查机场代码、时间窗口、事件类型专属规则和版本顺序。

失败时统一抛出带稳定错误码和字段明细的 ValidationError；全部校验通过前
不会写入任何数据。
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from app.errors import UnknownAirportError, ValidationError
from app.models import (
    EVENT_CLOSED,
    EVENT_EXTENDED,
    EVENT_REOPENED,
    EVENT_TYPES,
    Airport,
    DisruptionEvent,
)
from app.timeutil import parse_event_datetime

EVENT_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{7,63}$")
AIRPORT_RE = re.compile(r"^[A-Z]{3}$")

REQUIRED_FIELDS = (
    "event_id",
    "event_version",
    "event_type",
    "airport_code",
    "effective_from",
    "reported_at",
)
ALLOWED_FIELDS = frozenset(
    REQUIRED_FIELDS
    + ("effective_until", "supersedes_event_id", "reason")
)

_MIN_WINDOW_MIN = 15
_MAX_WINDOW_MIN = 7 * 24 * 60  # a closure longer than a week is almost certainly bad input


def validate_event(
    payload: Any,
    airports: dict[str, Airport],
) -> DisruptionEvent:
    """校验已经解码的 JSON，并转换为 DisruptionEvent。

    替代关系和版本顺序等事件链语义需要访问数据库，由服务层负责检查。
    """
    errors: list[dict[str, str]] = []

    if not isinstance(payload, dict):
        raise ValidationError("Event payload must be a JSON object")

    unknown = sorted(set(payload) - ALLOWED_FIELDS)
    if unknown:
        errors.append({"field": ".", "issue": "unknown_fields", "fields": ", ".join(unknown)})

    missing = [f for f in REQUIRED_FIELDS if f not in payload]
    if missing:
        errors.append(
            {"field": ".", "issue": "missing_fields", "fields": ", ".join(missing)}
        )
        raise ValidationError("Event payload is missing required field(s)", {"errors": errors})

    event_id = _check_string_pattern(payload, "event_id", EVENT_ID_RE, errors)
    event_version = _check_positive_int(payload, "event_version", errors)
    event_type = _check_enum(payload, "event_type", EVENT_TYPES, errors)
    airport_code = _check_string_pattern(payload, "airport_code", AIRPORT_RE, errors)
    effective_from = _check_datetime(payload, "effective_from", errors)
    reported_at = _check_datetime(payload, "reported_at", errors)
    effective_until = _check_nullable_datetime(payload, "effective_until", errors)
    supersedes = _check_nullable_id(payload, "supersedes_event_id", errors)
    reason = _check_reason(payload, errors)

    if errors:
        raise ValidationError("Event payload failed structural validation", {"errors": errors})

    # --- Semantic checks (all structurally valid values are now typed) ---
    semantic: list[dict[str, str]] = []

    if airport_code not in airports:
        raise UnknownAirportError(
            f"Unknown airport code '{airport_code}'",
            {"field": "airport_code", "received": airport_code},
        )

    assert event_type is not None  # narrowed by the errors check above

    if effective_until is not None:
        window_min = int((effective_until - effective_from).total_seconds() // 60)
        if effective_until <= effective_from:
            semantic.append(
                {
                    "field": "effective_until",
                    "issue": "must_be_after_effective_from",
                }
            )
        elif window_min < _MIN_WINDOW_MIN:
            semantic.append(
                {
                    "field": "effective_until",
                    "issue": "window_too_short",
                    "window_minutes": str(window_min),
                    "minimum_minutes": str(_MIN_WINDOW_MIN),
                }
            )
        elif window_min > _MAX_WINDOW_MIN:
            semantic.append(
                {
                    "field": "effective_until",
                    "issue": "window_too_long",
                    "window_minutes": str(window_min),
                    "maximum_minutes": str(_MAX_WINDOW_MIN),
                }
            )

    if event_type == EVENT_CLOSED:
        # effective_until may be null: an open-ended closure ("closed until
        # further notice") yields pending_confirmation impacts.
        if supersedes is not None:
            semantic.append(
                {"field": "supersedes_event_id", "issue": "not_allowed_for_close_event"}
            )
    elif event_type == EVENT_EXTENDED:
        if effective_until is None:
            semantic.append(
                {"field": "effective_until", "issue": "required_for_extended_event"}
            )
        if supersedes is None:
            semantic.append(
                {"field": "supersedes_event_id", "issue": "required_for_extended_event"}
            )
    elif event_type == EVENT_REOPENED:
        # reopening is an instantaneous point-in-time event
        if effective_until is not None:
            semantic.append(
                {"field": "effective_until", "issue": "not_allowed_for_reopened_event"}
            )

    if semantic:
        raise ValidationError("Event payload failed semantic validation", {"errors": semantic})

    return DisruptionEvent(
        event_id=event_id,
        event_version=event_version,
        event_type=event_type,
        airport_code=airport_code,
        effective_from=effective_from,
        effective_until=effective_until,
        reported_at=reported_at,
        supersedes_event_id=supersedes,
        reason=reason,
    )


# --------------------------------------------------------------------------- #
# Structural field checks
# --------------------------------------------------------------------------- #

def _check_string_pattern(
    payload: dict, field: str, pattern: re.Pattern[str], errors: list
) -> str | None:
    value = payload.get(field)
    if not isinstance(value, str):
        errors.append({"field": field, "issue": "must_be_string"})
        return None
    if not pattern.match(value):
        errors.append({"field": field, "issue": "pattern_mismatch", "received": value})
        return None
    return value


def _check_positive_int(payload: dict, field: str, errors: list) -> int | None:
    value = payload.get(field)
    # bool is a subclass of int; reject it explicitly
    if not isinstance(value, int) or isinstance(value, bool):
        errors.append({"field": field, "issue": "must_be_integer"})
        return None
    if value < 1:
        errors.append({"field": field, "issue": "must_be_at_least_1", "received": str(value)})
        return None
    return value


def _check_enum(payload: dict, field: str, allowed: tuple, errors: list) -> str | None:
    value = payload.get(field)
    if not isinstance(value, str) or value not in allowed:
        errors.append(
            {
                "field": field,
                "issue": "invalid_enum_value",
                "allowed": ", ".join(allowed),
            }
        )
        return None
    return value


def _check_datetime(payload: dict, field: str, errors: list) -> datetime | None:
    value = payload.get(field)
    if not isinstance(value, str):
        errors.append({"field": field, "issue": "must_be_string"})
        return None
    try:
        return parse_event_datetime(value, field)
    except ValidationError as exc:
        errors.append({"field": field, "issue": exc.message, "received": value})
        return None


def _check_nullable_datetime(payload: dict, field: str, errors: list) -> datetime | None:
    if field not in payload or payload[field] is None:
        return None
    return _check_datetime(payload, field, errors)


def _check_nullable_id(payload: dict, field: str, errors: list) -> str | None:
    if field not in payload or payload[field] is None:
        return None
    return _check_string_pattern(payload, field, EVENT_ID_RE, errors)


def _check_reason(payload: dict, errors: list) -> str | None:
    if "reason" not in payload or payload["reason"] is None:
        return None
    value = payload["reason"]
    if not isinstance(value, str):
        errors.append({"field": "reason", "issue": "must_be_string"})
        return None
    stripped = value.strip()
    if not 1 <= len(stripped) <= 240:
        errors.append({"field": "reason", "issue": "length_out_of_range"})
        return None
    return stripped
