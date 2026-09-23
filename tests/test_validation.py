"""事件载荷严格校验测试。"""

from __future__ import annotations

import unittest

from app.errors import ValidationError
from app.validation import validate_event
from tests.support import base_event


class ValidationTest(unittest.TestCase):
    def setUp(self) -> None:
        from tests.support import FIXTURES_DIR
        from app.config import load_airports

        self.airports = load_airports(FIXTURES_DIR)

    def _validate(self, payload):
        return validate_event(payload, self.airports)

    def test_valid_event_normalizes_offset_to_utc(self) -> None:
        event = self._validate(
            base_event(effective_from="2026-09-07T23:00:00+08:00")
        )
        self.assertEqual(
            event.effective_from.isoformat(), "2026-09-07T15:00:00+00:00"
        )

    def test_missing_required_fields_rejected(self) -> None:
        with self.assertRaises(ValidationError) as ctx:
            self._validate({})
        self.assertEqual(ctx.exception.code, "validation_error")
        issues = ctx.exception.details["errors"][0]
        self.assertEqual(issues["issue"], "missing_fields")
        for field in (
            "event_id",
            "event_version",
            "event_type",
            "airport_code",
            "effective_from",
            "reported_at",
        ):
            self.assertIn(field, issues["fields"])

    def test_unknown_fields_rejected(self) -> None:
        with self.assertRaises(ValidationError) as ctx:
            self._validate(base_event(unexpected="x"))
        self.assertIn("unknown_fields", ctx.exception.details["errors"][0]["issue"])

    def test_non_object_payload_rejected(self) -> None:
        for bad in ([], "x", 42, None):
            with self.assertRaises(ValidationError):
                self._validate(bad)

    def test_airport_code_format(self) -> None:
        for bad in ("ap s", "APSX", "aps", "A1S", ""):
            with self.assertRaises(ValidationError):
                self._validate(base_event(airport_code=bad))

    def test_unknown_airport_code(self) -> None:
        with self.assertRaises(ValidationError) as ctx:
            self._validate(base_event(airport_code="XKZ"))
        self.assertEqual(ctx.exception.code, "unknown_airport")

    def test_event_id_pattern(self) -> None:
        for bad in ("AB-CDEF1234", "-abcdef12", "ab", "a" + "x" * 70):
            with self.assertRaises(ValidationError):
                self._validate(base_event(event_id=bad))

    def test_version_must_be_positive_integer(self) -> None:
        for bad in (0, -1, "1", 1.0, True):
            with self.assertRaises(ValidationError):
                self._validate(base_event(event_version=bad))

    def test_event_type_enum(self) -> None:
        with self.assertRaises(ValidationError):
            self._validate(base_event(event_type="airport.delayed"))

    def test_naive_timestamp_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self._validate(base_event(effective_from="2026-09-07T15:00:00"))

    def test_malformed_timestamp_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self._validate(base_event(effective_from="not-a-dateZ"))

    def test_open_ended_close_allowed(self) -> None:
        event = self._validate(base_event(effective_until=None))
        self.assertIsNone(event.effective_until)

    def test_close_must_not_supersede(self) -> None:
        with self.assertRaises(ValidationError):
            self._validate(base_event(supersedes_event_id="evt-other0000001"))

    def test_extended_requires_until_and_supersedes(self) -> None:
        with self.assertRaises(ValidationError) as ctx:
            self._validate(
                base_event(
                    event_id="evt-extend000001",
                    event_type="airport.extended",
                    effective_from="2026-09-07T19:00:00Z",
                    effective_until="2026-09-07T22:00:00Z",
                )
            )
        issues = [e["issue"] for e in ctx.exception.details["errors"]]
        self.assertIn("required_for_extended_event", issues)

    def test_reopen_rejects_until(self) -> None:
        with self.assertRaises(ValidationError):
            self._validate(
                base_event(
                    event_id="evt-reopen00001",
                    event_type="airport.reopened",
                    effective_from="2026-09-07T19:00:00Z",
                    effective_until="2026-09-07T20:00:00Z",
                    supersedes_event_id="evt-close0000001",
                )
            )

    def test_window_ordering(self) -> None:
        with self.assertRaises(ValidationError):
            self._validate(
                base_event(
                    effective_from="2026-09-07T19:00:00Z",
                    effective_until="2026-09-07T15:00:00Z",
                )
            )

    def test_window_too_short(self) -> None:
        with self.assertRaises(ValidationError) as ctx:
            self._validate(
                base_event(
                    effective_from="2026-09-07T15:00:00Z",
                    effective_until="2026-09-07T15:05:00Z",
                )
            )
        self.assertIn("window_too_short", str(ctx.exception.details))

    def test_reason_length(self) -> None:
        with self.assertRaises(ValidationError):
            self._validate(base_event(reason="x" * 241))
        with self.assertRaises(ValidationError):
            self._validate(base_event(reason=123))


if __name__ == "__main__":
    unittest.main()
