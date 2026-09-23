"""重叠区间与影响计算引擎测试。"""

from __future__ import annotations

import unittest
from datetime import timedelta

from app.engine import chain_window, classify_flight
from app.models import (
    EVENT_CLOSED,
    EVENT_EXTENDED,
    EVENT_REOPENED,
    IMPACT_CANCELLED,
    IMPACT_DELAYED,
    IMPACT_PENDING,
    DisruptionEvent,
)
from tests.support import FIXTURES_DIR, ServiceTestCase


def make_event(event_type=EVENT_CLOSED, **kw) -> DisruptionEvent:
    from app.timeutil import parse_event_datetime

    defaults = {
        "event_id": "evt-engine00001",
        "event_version": 1,
        "airport_code": "APS",
        "effective_from": parse_event_datetime("2026-09-07T15:00:00Z", "f"),
        "effective_until": parse_event_datetime("2026-09-07T19:00:00Z", "u"),
        "reported_at": parse_event_datetime("2026-09-07T14:00:00Z", "r"),
        "supersedes_event_id": None,
        "reason": None,
    }
    defaults["event_type"] = event_type
    for key, value in kw.items():
        if key in ("effective_from", "effective_until", "reported_at") and isinstance(value, str):
            value = parse_event_datetime(value, key)
        defaults[key] = value
    return DisruptionEvent(**defaults)


class EngineTest(ServiceTestCase):
    def flight(self, flight_id: str):
        return self.flights[flight_id]

    def test_cross_midnight_window_marks_flag(self) -> None:
        # 15:00-19:00 UTC == 23:00-03:00 local at APS (UTC+8)
        result = self.service.submit_event(
            {
                "event_id": "evt-crossmid001",
                "event_version": 1,
                "event_type": "airport.closed",
                "airport_code": "APS",
                "effective_from": "2026-09-07T15:00:00Z",
                "effective_until": "2026-09-07T19:00:00Z",
                "reported_at": "2026-09-07T14:00:00Z",
            }
        )
        self.assertEqual(result["impact_count"], 3)
        self.assertTrue(all(i["crosses_midnight"] for i in result["impacts"]))

    def test_window_not_spanning_local_midnight_not_flagged(self) -> None:
        # 16:00-19:00 UTC == 00:00-03:00 local: a single local date
        result = self.service.submit_event(
            {
                "event_id": "evt-nomidnight01",
                "event_version": 1,
                "event_type": "airport.closed",
                "airport_code": "APS",
                "effective_from": "2026-09-07T16:00:00Z",
                "effective_until": "2026-09-07T19:00:00Z",
                "reported_at": "2026-09-07T14:00:00Z",
            }
        )
        self.assertTrue(result["impacts"])
        self.assertFalse(any(i["crosses_midnight"] for i in result["impacts"]))

    def test_delay_within_max_is_delayed(self) -> None:
        # BSR 15:00-16:00 local UTC window: BY205 departs 15:05 -> 55 min hold
        result = self.service.submit_event(
            {
                "event_id": "evt-delay0000001",
                "event_version": 1,
                "event_type": "airport.closed",
                "airport_code": "BSR",
                "effective_from": "2026-09-07T15:00:00Z",
                "effective_until": "2026-09-07T16:00:00Z",
                "reported_at": "2026-09-07T14:00:00Z",
            }
        )
        self.assertEqual(result["status_breakdown"], {"delayed": 1})
        impact = result["impacts"][0]
        self.assertEqual(impact["flight_id"], "BY-205-20260908")
        self.assertEqual(impact["delay_minutes"], 55)
        self.assertEqual(impact["proposed_departure"], "2026-09-07T16:00:00Z")
        self.assertEqual(impact["proposed_arrival"], "2026-09-07T17:40:00Z")

    def test_required_delay_over_max_cancels(self) -> None:
        aps = self.airports["APS"]
        event = make_event()
        window = chain_window(event, event, aps)
        record = classify_flight(self.flight("AX-410-20260907"), window)
        self.assertEqual(record["impact_status"], IMPACT_CANCELLED)
        self.assertEqual(record["overlap_minutes"], 210)
        self.assertIsNone(record["delay_minutes"])

    def test_non_retimable_flight_cancels(self) -> None:
        aps = self.airports["APS"]
        event = make_event()
        window = chain_window(event, event, aps)
        record = classify_flight(self.flight("AX-412-20260908"), window)
        self.assertEqual(record["impact_status"], IMPACT_CANCELLED)
        self.assertEqual(record["affected_endpoint"], "origin")

    def test_open_ended_closure_is_pending(self) -> None:
        aps = self.airports["APS"]
        event = make_event(effective_until=None)
        window = chain_window(event, event, aps)
        record = classify_flight(self.flight("AX-410-20260907"), window)
        self.assertEqual(record["impact_status"], IMPACT_PENDING)
        self.assertIsNone(record["overlap_minutes"])

    def test_flight_outside_window_not_affected(self) -> None:
        aps = self.airports["APS"]
        event = make_event(
            effective_from="2026-09-07T15:00:00Z",
            effective_until="2026-09-07T15:30:00Z",
        )
        window = chain_window(event, event, aps)
        # AX410 departs at exactly 15:30: touching endpoint -> not overlapping
        self.assertIsNone(classify_flight(self.flight("AX-410-20260907"), window))

    def test_offset_timestamp_normalized_before_comparison(self) -> None:
        aps = self.airports["APS"]
        # 23:00+08:00 == 15:00Z
        event = make_event(effective_from="2026-09-07T23:00:00+08:00")
        window = chain_window(event, event, aps)
        self.assertEqual(window.start, self.flight("AX-410-20260907").scheduled_departure - timedelta(minutes=30))
        record = classify_flight(self.flight("AX-410-20260907"), window)
        self.assertEqual(record["impact_status"], IMPACT_CANCELLED)

    def test_reopen_buffer_extends_window(self) -> None:
        bsr = self.airports["BSR"]  # 15 minute buffer
        root = make_event(
            airport_code="BSR",
            effective_from="2026-09-07T15:00:00Z",
            effective_until="2026-09-07T17:00:00Z",
        )
        reopen = make_event(
            EVENT_REOPENED,
            event_id="evt-engine00002",
            event_version=2,
            airport_code="BSR",
            effective_from="2026-09-07T15:40:00Z",
            effective_until=None,
            supersedes_event_id="evt-engine00001",
        )
        window = chain_window(reopen, root, bsr)
        self.assertTrue(window.terminal)
        # resume at 15:55 -> BY205 (15:05 departure) needs 50 minutes
        record = classify_flight(self.flight("BY-205-20260908"), window)
        self.assertEqual(record["impact_status"], IMPACT_DELAYED)
        self.assertEqual(record["overlap_minutes"], 50)

    def test_extension_union_window(self) -> None:
        aps = self.airports["APS"]
        root = make_event(
            effective_from="2026-09-07T15:00:00Z",
            effective_until="2026-09-07T16:00:00Z",
        )
        extended = make_event(
            EVENT_EXTENDED,
            event_id="evt-engine00002",
            event_version=2,
            effective_from="2026-09-07T15:50:00Z",
            effective_until="2026-09-07T19:30:00Z",
            supersedes_event_id="evt-engine00001",
        )
        window = chain_window(extended, root, aps)
        self.assertEqual(window.start.isoformat(), "2026-09-07T15:00:00+00:00")
        # KX099 arrives APS 19:15, can retime 45 min: needs 15 -> delayed
        record = classify_flight(self.flight("KX-099-20260908"), window)
        self.assertEqual(record["impact_status"], IMPACT_DELAYED)
        self.assertEqual(record["delay_minutes"], 15)
        self.assertEqual(record["affected_endpoint"], "destination")


if __name__ == "__main__":
    unittest.main()
