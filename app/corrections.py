"""历史材料更正提案（见 contracts/correction-proposal.schema.json）的结构校验。

提案本身不改变当前机场状态：它只声明针对某条已裁定事件的补丁以及提交时所基于的
投影版本。补丁应用后是否仍构成合法事件链、是否会改变已发布影响，由服务层在事务
内做假设重放判定。
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from app.errors import ValidationError
from app.models import Airport
from app.timeutil import parse_event_datetime

_REQUEST_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{7,63}$")
_EVENT_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{7,63}$")
_AIRPORT_RE = re.compile(r"^[A-Z]{3}$")

_PATCH_TIME_FIELDS = ("effective_from", "effective_until")
_PATCH_ALLOWED = frozenset(("airport_code",) + _PATCH_TIME_FIELDS)
REQUIRED_FIELDS = (
    "request_id",
    "target_event_id",
    "base_projection_version",
    "patch",
    "submitted_by",
)


def validate_proposal(
    payload: Any,
    airports: dict[str, Airport],
) -> dict[str, Any]:
    errors: list[dict[str, str]] = []

    if not isinstance(payload, dict):
        raise ValidationError("Correction proposal must be a JSON object")

    missing = [f for f in REQUIRED_FIELDS if f not in payload]
    if missing:
        raise ValidationError(
            "Correction proposal is missing required field(s)",
            {"errors": [{"field": ".", "issue": "missing_fields",
                         "fields": ", ".join(missing)}]},
        )

    request_id = payload.get("request_id")
    if not isinstance(request_id, str) or not _REQUEST_ID_RE.match(request_id):
        errors.append({"field": "request_id", "issue": "pattern_mismatch"})

    target_event_id = payload.get("target_event_id")
    if not isinstance(target_event_id, str) or not _EVENT_ID_RE.match(target_event_id):
        errors.append({"field": "target_event_id", "issue": "pattern_mismatch"})

    base_version = payload.get("base_projection_version")
    if not isinstance(base_version, int) or isinstance(base_version, bool) or base_version < 1:
        errors.append({"field": "base_projection_version", "issue": "must_be_positive_integer"})

    submitted_by = payload.get("submitted_by")
    if not isinstance(submitted_by, str) or not 3 <= len(submitted_by.strip()) <= 64:
        errors.append({"field": "submitted_by", "issue": "length_out_of_range"})

    reason = payload.get("reason")
    if reason is not None and (not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 240):
        errors.append({"field": "reason", "issue": "length_out_of_range"})

    raw_patch = payload.get("patch")
    patch: dict[str, Any] | None = None
    if not isinstance(raw_patch, dict) or not raw_patch:
        errors.append({"field": "patch", "issue": "must_be_nonempty_object"})
    else:
        unknown = sorted(set(raw_patch) - _PATCH_ALLOWED)
        if unknown:
            errors.append(
                {"field": "patch", "issue": "unknown_fields",
                 "fields": ", ".join(unknown)}
            )
        else:
            patch = _parse_patch(raw_patch, airports, errors)

    if errors:
        raise ValidationError(
            "Correction proposal failed structural validation", {"errors": errors}
        )

    return {
        "request_id": request_id,
        "target_event_id": target_event_id,
        "base_projection_version": base_version,
        "patch": patch,
        "submitted_by": submitted_by.strip(),
        "reason": reason.strip() if isinstance(reason, str) else None,
        "raw": payload,
    }


def _parse_patch(
    raw_patch: dict[str, Any],
    airports: dict[str, Airport],
    errors: list[dict[str, str]],
) -> dict[str, Any]:
    patch: dict[str, Any] = {}
    if "airport_code" in raw_patch:
        code = raw_patch["airport_code"]
        if not isinstance(code, str) or not _AIRPORT_RE.match(code):
            errors.append({"field": "patch.airport_code", "issue": "pattern_mismatch"})
        elif code not in airports:
            errors.append(
                {"field": "patch.airport_code", "issue": "unknown_airport",
                 "received": code}
            )
        else:
            patch["airport_code"] = code

    for field in _PATCH_TIME_FIELDS:
        if field not in raw_patch:
            continue
        value = raw_patch[field]
        if value is None:
            patch[field] = None
            continue
        if not isinstance(value, str):
            errors.append({"field": f"patch.{field}", "issue": "must_be_string"})
            continue
        try:
            patch[field] = parse_event_datetime(value, f"patch.{field}")
        except ValidationError as exc:
            errors.append(
                {"field": f"patch.{field}", "issue": exc.message, "received": value}
            )

    from_dt: datetime | None = patch.get("effective_from")
    until_dt: datetime | None = patch.get("effective_until")
    if from_dt is not None and until_dt is not None and until_dt <= from_dt:
        errors.append(
            {"field": "patch.effective_until",
             "issue": "must_be_after_effective_from"}
        )
    return patch
