"""事件链时间裁定、机场全局版本与并发唯一性测试。"""

from __future__ import annotations

import threading

from app.errors import EventConflictError, RejectedEventError, ValidationError
from tests.support import ServiceTestCase, base_event


def close(event_id="evt-close0000001", airport="APS", **kw) -> dict:
    payload = base_event(event_id=event_id, airport_code=airport)
    payload.update(kw)
    return payload


def extend(event_id, supersedes, version=2, airport="APS", **kw) -> dict:
    payload = {
        "event_id": event_id,
        "event_version": version,
        "event_type": "airport.extended",
        "airport_code": airport,
        "effective_from": kw.pop("effective_from", "2026-09-07T15:50:00Z"),
        "effective_until": kw.pop("effective_until", "2026-09-07T20:00:00Z"),
        "reported_at": kw.pop("reported_at", "2026-09-07T14:30:00Z"),
        "supersedes_event_id": supersedes,
    }
    payload.update(kw)
    return payload


def reopen(event_id, supersedes, version=3, airport="APS", **kw) -> dict:
    payload = {
        "event_id": event_id,
        "event_version": version,
        "event_type": "airport.reopened",
        "airport_code": airport,
        "effective_from": kw.pop("effective_from", "2026-09-07T16:00:00Z"),
        "reported_at": kw.pop("reported_at", "2026-09-07T15:00:00Z"),
        "supersedes_event_id": supersedes,
    }
    payload.update(kw)
    return payload


def issues(exc: ValidationError) -> set[str]:
    return {e["issue"] for e in exc.details["errors"]}


class ReopenTimeArbitrationTest(ServiceTestCase):
    def setUp(self) -> None:
        super().setUp()
        # Closure 15:00-19:00Z at APS (buffer 20 min).
        self.service.submit_event(close())

    def test_reopen_before_closure_start_rejected(self) -> None:
        bad = reopen(
            "evt-reopen-bad01",
            "evt-close0000001",
            version=2,
            effective_from="2026-09-07T14:00:00Z",
        )
        with self.assertRaises(RejectedEventError) as ctx:
            self.service.submit_event(bad)
        self.assertIn("recovery_precedes_closure_start", issues(ctx.exception))
        self.assertIn("recovery_window_not_after_closure_start", issues(ctx.exception))

    def test_rejected_reopen_does_not_end_chain(self) -> None:
        bad = reopen(
            "evt-reopen-bad01",
            "evt-close0000001",
            version=2,
            effective_from="2026-09-07T14:00:00Z",
        )
        with self.assertRaises(RejectedEventError):
            self.service.submit_event(bad)
        # The chain is still active and its flights remain in summaries.
        summary = self.service.airport_summary("APS")
        self.assertEqual(summary["active_chains"], 1)
        self.assertEqual(summary["affected_flights"], 3)
        flights = self.service.affected_flights(
            airport="APS", status=None, limit=50, offset=0
        )
        self.assertEqual(len(flights["flights"]), 3)

    def test_rejected_event_not_in_events_but_in_rejections(self) -> None:
        bad = reopen(
            "evt-reopen-bad01",
            "evt-close0000001",
            version=2,
            effective_from="2026-09-07T14:00:00Z",
        )
        with self.assertRaises(RejectedEventError):
            self.service.submit_event(bad)
        with self.assertRaises(Exception):
            self.service.event_status("evt-reopen-bad01")
        rejected = self.service.rejected_material(airport="APS")
        self.assertEqual(rejected["count"], 1)
        self.assertEqual(rejected["rejections"][0]["event_id"], "evt-reopen-bad01")

    def test_rejected_retry_is_idempotent_decision(self) -> None:
        bad = reopen(
            "evt-reopen-bad01",
            "evt-close0000001",
            version=2,
            effective_from="2026-09-07T14:00:00Z",
        )
        with self.assertRaises(RejectedEventError):
            self.service.submit_event(dict(bad))
        with self.assertRaises(RejectedEventError) as ctx:
            self.service.submit_event(dict(bad))
        self.assertTrue(ctx.exception.details.get("replayed"))
        # Retries must not pile up duplicate intake records.
        self.assertEqual(self.service.rejected_material()["count"], 1)

    def test_reopen_after_known_window_end_rejected(self) -> None:
        # setUp closed APS 15:00-19:00; a reopening that only resumes after the
        # already announced end (19:00) contradicts the chain.
        bad = reopen(
            "evt-reopen-bad02",
            "evt-close0000001",
            version=2,
            effective_from="2026-09-07T19:20:00Z",  # resume 19:40 > 19:00
        )
        with self.assertRaises(RejectedEventError) as ctx:
            self.service.submit_event(bad)
        self.assertIn("reopen_after_known_window_end", issues(ctx.exception))

    def test_valid_reopen_still_accepted(self) -> None:
        good = reopen(
            "evt-reopen-good1",
            "evt-close0000001",
            version=2,
            effective_from="2026-09-07T15:10:00Z",  # resume 15:30
        )
        result = self.service.submit_event(good)
        self.assertEqual(result["impact_count"], 0)
        self.assertEqual(result["resolved_count"], 3)


class ReportedAtArbitrationTest(ServiceTestCase):
    def test_reported_at_must_follow_chain_head(self) -> None:
        self.service.submit_event(close(reported_at="2026-09-07T14:00:00Z"))
        # Reopen reported earlier than the closure report.
        bad = reopen(
            "evt-reopen-bad03",
            "evt-close0000001",
            version=2,
            effective_from="2026-09-07T15:10:00Z",
            reported_at="2026-09-07T13:00:00Z",
        )
        with self.assertRaises(RejectedEventError) as ctx:
            self.service.submit_event(bad)
        self.assertIn("reported_at_precedes_chain_head", issues(ctx.exception))
        # Chain remains active.
        self.assertEqual(self.service.airport_summary("APS")["active_chains"], 1)


class VersionAndHeadArbitrationTest(ServiceTestCase):
    def test_version_must_increase_airport_wide(self) -> None:
        self.service.submit_event(close())
        self.service.submit_event(
            reopen(
                "evt-reopen-good1",
                "evt-close0000001",
                version=2,
                effective_from="2026-09-07T15:10:00Z",
            )
        )
        # A new closure must outrank every airport event, not just the first.
        bad = close(
            event_id="evt-close0000002",
            event_version=2,
            effective_from="2026-09-08T00:00:00Z",
            effective_until="2026-09-08T02:00:00Z",
            reported_at="2026-09-07T20:00:00Z",
        )
        with self.assertRaises(RejectedEventError) as ctx:
            self.service.submit_event(bad)
        self.assertIn("must_extend_airport_history", issues(ctx.exception))

    def test_new_close_while_chain_active_rejected(self) -> None:
        self.service.submit_event(close())
        bad = close(
            event_id="evt-close0000002",
            event_version=5,
            effective_from="2026-09-08T00:00:00Z",
            effective_until="2026-09-08T02:00:00Z",
            reported_at="2026-09-07T20:00:00Z",
        )
        with self.assertRaises(RejectedEventError) as ctx:
            self.service.submit_event(bad)
        self.assertIn("airport_chain_still_active", issues(ctx.exception))

    def test_must_supersede_current_head(self) -> None:
        self.service.submit_event(close())
        self.service.submit_event(
            extend(
                "evt-extend000001",
                "evt-close0000001",
                effective_until="2026-09-07T20:00:00Z",
            )
        )
        # A reopen referencing the buried closure instead of the extension head.
        bad = reopen(
            "evt-reopen-bad04",
            "evt-close0000001",
            version=3,
            effective_from="2026-09-07T15:10:00Z",
        )
        with self.assertRaises(RejectedEventError) as ctx:
            self.service.submit_event(bad)
        self.assertIn("must_supersede_chain_head", issues(ctx.exception))

    def test_reopen_after_reopen_rejected(self) -> None:
        self.service.submit_event(close())
        self.service.submit_event(
            reopen(
                "evt-reopen-good1",
                "evt-close0000001",
                version=2,
                effective_from="2026-09-07T15:10:00Z",
            )
        )
        bad = reopen(
            "evt-reopen-bad05",
            "evt-reopen-good1",
            version=3,
            effective_from="2026-09-07T15:20:00Z",
        )
        with self.assertRaises(RejectedEventError) as ctx:
            self.service.submit_event(bad)
        self.assertIn("chain_already_closed", issues(ctx.exception))


class ConcurrentArbitrationTest(ServiceTestCase):
    def test_concurrent_extend_and_reopen_yield_unique_state(self) -> None:
        self.service.submit_event(
            close(
                airport="BSR",
                effective_from="2026-09-07T15:00:00Z",
                effective_until="2026-09-07T16:00:00Z",
            )
        )
        ext = extend(
            "evt-extend-concur",
            "evt-close0000001",
            airport="BSR",
            effective_from="2026-09-07T15:55:00Z",
            effective_until="2026-09-07T18:00:00Z",
            reported_at="2026-09-07T14:30:00Z",
        )
        rop = reopen(
            "evt-reopen-concur",
            "evt-close0000001",
            airport="BSR",
            effective_from="2026-09-07T15:40:00Z",
            reported_at="2026-09-07T14:31:00Z",
        )
        results: dict[str, str] = {}
        barrier = threading.Barrier(2)

        def submit(name: str, payload: dict) -> None:
            barrier.wait()
            try:
                self.service.submit_event(payload)
                results[name] = "adopted"
            except RejectedEventError:
                results[name] = "rejected"

        t1 = threading.Thread(target=submit, args=("extend", ext))
        t2 = threading.Thread(target=submit, args=("reopen", rop))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertEqual(sorted(results.values()), ["adopted", "rejected"])
        # Exactly one head event exists; the loser left no partial impacts.
        summary = self.service.airport_summary("BSR")
        self.assertEqual(summary["event_count"], 2)
        self.assertEqual(self.service.rejected_material(airport="BSR")["count"], 1)
        # The state is explainable: either open extension or closed chain.
        adopted = [n for n, v in results.items() if v == "adopted"][0]
        if adopted == "reopen":
            self.assertEqual(summary["active_chains"], 0)
        else:
            self.assertEqual(summary["active_chains"], 1)

    def test_failed_transaction_leaves_no_tombstones(self) -> None:
        self.service.submit_event(close())
        bad = reopen(
            "evt-reopen-bad06",
            "evt-close0000001",
            version=2,
            effective_from="2026-09-07T14:00:00Z",
        )
        with self.assertRaises(RejectedEventError):
            self.service.submit_event(bad)
        rejected_rows = [
            i
            for i in self.service.rejected_material()["rejections"]
            if i["event_id"] == "evt-reopen-bad06"
        ]
        self.assertEqual(len(rejected_rows), 1)
        # Original closure impacts are intact and still active.
        status = self.service.event_status("evt-close0000001")
        self.assertEqual(status["processing"]["impact_count"], 3)


if __name__ == "__main__":
    import unittest

    unittest.main()
