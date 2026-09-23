"""覆盖幂等、冲突、事件链、查询与重启语义的服务测试。"""

from __future__ import annotations

from app.errors import EventConflictError, NotFoundError, ValidationError
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
        "reported_at": "2026-09-07T14:30:00Z",
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
        "effective_until": None,
        "reported_at": "2026-09-07T15:00:00Z",
        "supersedes_event_id": supersedes,
    }
    payload.update(kw)
    return payload


class IdempotencyTest(ServiceTestCase):
    def test_duplicate_submission_replays_original_result(self) -> None:
        payload = close()
        first = self.service.submit_event(payload)
        second = self.service.submit_event(dict(payload))
        self.assertEqual(first["event_id"], second["event_id"])
        self.assertEqual(first["impacts"], second["impacts"])
        self.assertEqual(first["processing_state"], "processed")
        self.assertEqual(second["processing_state"], "replayed")

        status = self.service.event_status(payload["event_id"])
        self.assertEqual(status["processing"]["replay_count"], 1)
        self.assertEqual(status["impacts"], first["impacts"])

    def test_duplicate_with_changed_payload_conflicts(self) -> None:
        payload = close()
        self.service.submit_event(payload)
        changed = dict(payload)
        changed["reason"] = "different reason text"
        with self.assertRaises(EventConflictError) as ctx:
            self.service.submit_event(changed)
        self.assertEqual(ctx.exception.code, "event_conflict")
        # Original result must be untouched.
        status = self.service.event_status(payload["event_id"])
        self.assertEqual(status["event"]["reason"], "volcanic ash")
        self.assertEqual(status["processing"]["replay_count"], 0)

    def test_reused_id_with_other_version_conflicts(self) -> None:
        payload = close()
        self.service.submit_event(payload)
        changed = dict(payload)
        changed["event_version"] = 2
        with self.assertRaises(EventConflictError):
            self.service.submit_event(changed)


class ChainTest(ServiceTestCase):
    def test_extend_requires_known_supersedes(self) -> None:
        self.service.submit_event(close())
        bad = extend("evt-extend000001", "evt-doesnotexist")
        with self.assertRaises(ValidationError) as ctx:
            self.service.submit_event(bad)
        issues = [e["issue"] for e in ctx.exception.details["errors"]]
        self.assertIn("unknown_event", issues)

    def test_extend_airport_mismatch_rejected(self) -> None:
        self.service.submit_event(close(airport="APS"))
        bad = extend("evt-extend000001", "evt-close0000001", airport="BSR")
        with self.assertRaises(ValidationError) as ctx:
            self.service.submit_event(bad)
        self.assertIn("airport_mismatch", str(ctx.exception.details))

    def test_extend_must_push_window_end(self) -> None:
        self.service.submit_event(close())
        bad = extend(
            "evt-extend000001",
            "evt-close0000001",
            effective_from="2026-09-07T15:50:00Z",
            effective_until="2026-09-07T18:00:00Z",
        )
        with self.assertRaises(ValidationError) as ctx:
            self.service.submit_event(bad)
        self.assertIn("must_extend_previous_window", str(ctx.exception.details))

    def test_extension_leaving_gap_rejected(self) -> None:
        self.service.submit_event(
            close(effective_until="2026-09-07T16:00:00Z")
        )
        bad = extend(
            "evt-extend000001",
            "evt-close0000001",
            effective_from="2026-09-07T16:30:00Z",
            effective_until="2026-09-07T19:00:00Z",
        )
        with self.assertRaises(ValidationError) as ctx:
            self.service.submit_event(bad)
        self.assertIn("extension_leaves_uncovered_gap", str(ctx.exception.details))

    def test_version_must_increase_along_chain(self) -> None:
        self.service.submit_event(close())
        bad = extend("evt-extend000001", "evt-close0000001", version=1)
        with self.assertRaises(ValidationError) as ctx:
            self.service.submit_event(bad)
        self.assertIn("version_must_increase", str(ctx.exception.details))

    def test_cannot_extend_reopened_chain(self) -> None:
        self.service.submit_event(close())
        self.service.submit_event(reopen("evt-reopen000001", "evt-close0000001"))
        bad = extend(
            "evt-extend000001", "evt-reopen000001", version=4
        )
        with self.assertRaises(ValidationError) as ctx:
            self.service.submit_event(bad)
        self.assertIn("chain_already_closed", str(ctx.exception.details))

    def test_full_chain_recompute_and_resolve(self) -> None:
        # Closure 16:20-17:00Z: AX412 (16:30 dep, cannot retime) is cancelled,
        # BY205 (16:45 arrival) needs 15 min and can retime -> delayed.
        first = self.service.submit_event(
            close(
                effective_from="2026-09-07T16:20:00Z",
                effective_until="2026-09-07T17:00:00Z",
            )
        )
        affected = {i["flight_id"] for i in first["impacts"]}
        self.assertEqual(affected, {"AX-412-20260908", "BY-205-20260908"})
        by_status = {i["flight_id"]: i["impact_status"] for i in first["impacts"]}
        self.assertEqual(by_status["AX-412-20260908"], "cancelled")
        self.assertEqual(by_status["BY-205-20260908"], "delayed")

        # Extend to 19:30Z: the union window [16:20, 19:30) is recomputed, so
        # BY205's required hold grows past its 90 min retime limit -> it flips
        # delayed -> cancelled, and KX099 (arrives 19:15) becomes delayed.
        extended = self.service.submit_event(
            extend(
                "evt-extend000001",
                "evt-close0000001",
                effective_from="2026-09-07T16:55:00Z",
                effective_until="2026-09-07T19:30:00Z",
            )
        )
        ext_status = {i["flight_id"]: i for i in extended["impacts"]}
        self.assertEqual(
            set(ext_status), {"AX-412-20260908", "BY-205-20260908", "KX-099-20260908"}
        )
        self.assertEqual(ext_status["AX-412-20260908"]["impact_status"], "cancelled")
        self.assertEqual(ext_status["BY-205-20260908"]["impact_status"], "cancelled")
        self.assertEqual(ext_status["KX-099-20260908"]["impact_status"], "delayed")
        self.assertEqual(extended["resolved_count"], 0)

        # Reopen at 16:25Z; APS resumes after its 20 min buffer -> the effective
        # window ends 16:45Z. BY205 arrives at exactly 16:45 (touching endpoint,
        # half-open interval) and KX099 at 19:15 are freed; AX412 (16:30 dep)
        # stays cancelled. Freed flights are recorded as resolved tombstones.
        reopened = self.service.submit_event(
            reopen(
                "evt-reopen000001",
                "evt-extend000001",
                effective_from="2026-09-07T16:25:00Z",
            )
        )
        self.assertEqual(
            [i["flight_id"] for i in reopened["impacts"]], ["AX-412-20260908"]
        )
        self.assertEqual(reopened["resolved_count"], 2)

        # Latest-snapshot queries must not resurrect the freed flights.
        rows = self.service.affected_flights(
            airport="APS", status=None, limit=50, offset=0
        )
        self.assertEqual(
            [f["flight_id"] for f in rows["flights"]], ["AX-412-20260908"]
        )
        summary = self.service.airport_summary("APS")
        self.assertEqual(summary["active_chains"], 0)
        self.assertEqual(summary["affected_flights"], 1)


class QueryTest(ServiceTestCase):
    def _submit_aps_close(self):
        return self.service.submit_event(close())

    def test_event_status_unknown_404(self) -> None:
        with self.assertRaises(NotFoundError):
            self.service.event_status("evt-missing0001")

    def test_airport_summary_unknown_404(self) -> None:
        with self.assertRaises(NotFoundError):
            self.service.airport_summary("ZZZ")

    def test_airport_summary_counts(self) -> None:
        self._submit_aps_close()
        summary = self.service.airport_summary("APS")
        self.assertEqual(summary["event_count"], 1)
        self.assertEqual(summary["active_chains"], 1)
        self.assertEqual(summary["affected_flights"], 3)
        self.assertEqual(summary["affected_passengers"], 441)
        self.assertEqual(summary["by_status"]["cancelled"]["flight_count"], 3)

    def test_summary_deactivates_chain_after_reopen(self) -> None:
        self._submit_aps_close()
        # Reopen 15:10Z -> resume 15:30Z after APS's 20 min buffer; AX410's
        # 15:30 departure touches (but does not cross) the half-open window end.
        self.service.submit_event(
            reopen(
                "evt-reopen000001",
                "evt-close0000001",
                effective_from="2026-09-07T15:10:00Z",
            )
        )
        summary = self.service.airport_summary("APS")
        self.assertEqual(summary["active_chains"], 0)
        self.assertEqual(summary["affected_flights"], 0)

    def test_pagination(self) -> None:
        self._submit_aps_close()
        page1 = self.service.affected_flights(
            airport=None, status=None, limit=2, offset=0
        )
        page2 = self.service.affected_flights(
            airport=None, status=None, limit=2, offset=2
        )
        self.assertEqual(page1["pagination"]["total"], 3)
        self.assertEqual(len(page1["flights"]), 2)
        self.assertEqual(len(page2["flights"]), 1)
        ids = [f["flight_id"] for f in page1["flights"]] + [
            f["flight_id"] for f in page2["flights"]
        ]
        self.assertEqual(len(ids), len(set(ids)))

    def test_status_filter(self) -> None:
        self.service.submit_event(
            close(
                event_id="evt-bsr000000001",
                airport="BSR",
                effective_from="2026-09-07T15:00:00Z",
                effective_until="2026-09-07T16:00:00Z",
            )
        )
        delayed = self.service.affected_flights(
            airport="BSR", status="delayed", limit=50, offset=0
        )
        self.assertEqual([f["flight_id"] for f in delayed["flights"]], ["BY-205-20260908"])
        cancelled = self.service.affected_flights(
            airport="BSR", status="cancelled", limit=50, offset=0
        )
        self.assertEqual(cancelled["flights"], [])
        with self.assertRaises(ValidationError):
            self.service.affected_flights(
                airport=None, status="bogus", limit=50, offset=0
            )


class PersistenceTest(ServiceTestCase):
    def test_data_survives_restart(self) -> None:
        payload = close()
        first = self.service.submit_event(payload)
        self.restart_service()
        status = self.service.event_status(payload["event_id"])
        self.assertEqual(status["event"]["event_id"], payload["event_id"])
        self.assertEqual(len(status["impacts"]), first["impact_count"])
        # Idempotency still works after restart.
        replay = self.service.submit_event(dict(payload))
        self.assertEqual(replay["processing_state"], "replayed")
        self.assertEqual(replay["impacts"], first["impacts"])

    def test_invalid_event_writes_nothing(self) -> None:
        with self.assertRaises(ValidationError):
            self.service.submit_event(close(airport_code="ZZZ"))
        rows = self.service.affected_flights(
            airport=None, status=None, limit=50, offset=0
        )
        self.assertEqual(rows["pagination"]["total"], 0)
        with self.assertRaises(NotFoundError):
            self.service.event_status("evt-close0000001")

    def test_failed_chain_event_leaves_no_partial_writes(self) -> None:
        self.service.submit_event(close())
        before = self.service.event_status("evt-close0000001")
        with self.assertRaises(ValidationError):
            self.service.submit_event(
                extend("evt-extend000001", "evt-unknown00001")
            )
        with self.assertRaises(NotFoundError):
            self.service.event_status("evt-extend000001")
        after = self.service.event_status("evt-close0000001")
        self.assertEqual(after["impacts"], before["impacts"])


if __name__ == "__main__":
    unittest.main()
