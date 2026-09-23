"""时间与链路裁定、并发唯一性、更正流程与重启可审计性测试。"""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor

from app.errors import (
    EventConflictError,
    ForbiddenError,
    NotFoundError,
    ValidationError,
)
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
        "effective_from": kw.pop("effective_from", "2026-09-07T18:55:00Z"),
        "effective_until": kw.pop("effective_until", "2026-09-07T21:00:00Z"),
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
        "effective_from": kw.pop("effective_from", "2026-09-07T18:00:00Z"),
        "effective_until": None,
        "reported_at": kw.pop("reported_at", "2026-09-07T15:00:00Z"),
        "supersedes_event_id": supersedes,
    }
    payload.update(kw)
    return payload


class TimeAdjudicationTest(ServiceTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.service.submit_event(close())

    def test_reopen_before_chain_start_rejected(self) -> None:
        # Version alone used to admit this out-of-order reopen, ending the chain
        # before it began and erasing every impacted flight.
        bad = reopen(
            "evt-reopen-bad01",
            "evt-close0000001",
            version=9,
            effective_from="2026-09-07T10:00:00Z",
        )
        with self.assertRaises(ValidationError) as ctx:
            self.service.submit_event(bad)
        issues = [e["issue"] for e in ctx.exception.details["errors"]]
        self.assertIn("reopen_before_chain_start", issues)

        # Chain is still active; nothing was erased.
        summary = self.service.airport_summary("APS")
        self.assertEqual(summary["active_chains"], 1)
        self.assertEqual(summary["affected_flights"], 3)
        rows = self.service.affected_flights(
            airport="APS", status=None, limit=50, offset=0
        )
        self.assertEqual(rows["pagination"]["total"], 3)

    def test_reopen_buffer_cannot_end_window_before_start(self) -> None:
        # 15:00 start; a 14:59 reopen at BSR would resume 15:14 (>= start),
        # but 14:30 would resume 14:45 (< start). Use the raw open time rule.
        self.service.submit_event(
            close(
                event_id="evt-bsr-close001",
                airport="BSR",
                effective_from="2026-09-07T15:00:00Z",
                effective_until="2026-09-07T19:00:00Z",
            )
        )
        bad = reopen(
            "evt-bsr-reopenbad",
            "evt-bsr-close001",
            version=2,
            airport="BSR",
            effective_from="2026-09-07T14:30:00Z",
        )
        with self.assertRaises(ValidationError) as ctx:
            self.service.submit_event(bad)
        self.assertIn(
            "reopen_before_chain_start", str(ctx.exception.details)
        )

    def test_backdated_report_rejected(self) -> None:
        bad = reopen(
            "evt-reopen-bad02",
            "evt-close0000001",
            effective_from="2026-09-07T16:00:00Z",
            reported_at="2026-09-07T13:00:00Z",  # before the closure's report
        )
        with self.assertRaises(ValidationError) as ctx:
            self.service.submit_event(bad)
        self.assertIn(
            "report_precedes_previous_report", str(ctx.exception.details)
        )

    def test_rejected_submission_is_persisted_and_visible(self) -> None:
        bad = reopen(
            "evt-reopen-bad03",
            "evt-close0000001",
            version=9,
            effective_from="2026-09-07T10:00:00Z",
        )
        with self.assertRaises(ValidationError):
            self.service.submit_event(bad)
        status = self.service.event_status("evt-reopen-bad03")
        self.assertEqual(status["processing"]["state"], "rejected")
        self.assertTrue(status["processing"]["decision"]["reasons"])
        queue = self.service.review_queue()
        ids = [e["event_id"] for e in queue["rejected_events"]]
        self.assertIn("evt-reopen-bad03", ids)

    def test_duplicate_of_rejected_returns_original_decision(self) -> None:
        bad = reopen(
            "evt-reopen-bad04",
            "evt-close0000001",
            version=9,
            effective_from="2026-09-07T10:00:00Z",
        )
        with self.assertRaises(ValidationError):
            self.service.submit_event(bad)
        with self.assertRaises(ValidationError) as ctx:
            self.service.submit_event(dict(bad))
        self.assertTrue(ctx.exception.details.get("replayed"))
        status = self.service.event_status("evt-reopen-bad04")
        self.assertEqual(status["processing"]["replay_count"], 1)

    def test_extension_must_reference_chain_head(self) -> None:
        self.service.submit_event(
            extend("evt-extend000001", "evt-close0000001")
        )
        # A second event referencing the root instead of the new head forks the
        # chain and must be refused.
        fork = extend(
            "evt-fork00000001",
            "evt-close0000001",
            version=3,
            effective_from="2026-09-07T20:55:00Z",
            effective_until="2026-09-07T22:00:00Z",
            reported_at="2026-09-07T15:00:00Z",
        )
        with self.assertRaises(ValidationError) as ctx:
            self.service.submit_event(fork)
        self.assertIn("must_supersede_chain_head", str(ctx.exception.details))
        # Exactly one explainable head.
        summary = self.service.airport_summary("APS")
        self.assertEqual(summary["head_event_id"], "evt-extend000001")

    def test_new_close_while_active_rejected(self) -> None:
        with self.assertRaises(ValidationError) as ctx:
            self.service.submit_event(
                close(
                    "evt-close0000002",
                    event_version=2,
                    reported_at="2026-09-07T15:30:00Z",
                )
            )
        self.assertIn("close_while_chain_active", str(ctx.exception.details))

    def test_new_close_allowed_only_after_reopen(self) -> None:
        self.service.submit_event(
            reopen(
                "evt-reopen000001",
                "evt-close0000001",
                version=2,
                effective_from="2026-09-07T15:10:00Z",
            )
        )
        accepted = self.service.submit_event(
            close(
                "evt-close0000002",
                event_version=3,
                reported_at="2026-09-07T16:00:00Z",
                effective_from="2026-09-07T22:00:00Z",
                effective_until="2026-09-07T23:00:00Z",
            )
        )
        self.assertEqual(accepted["processing_state"], "processed")
        summary = self.service.airport_summary("APS")
        self.assertEqual(summary["active_chains"], 1)
        self.assertEqual(summary["head_event_id"], "evt-close0000002")


class ConcurrencyTest(ServiceTestCase):
    def test_concurrent_chain_events_leave_one_head(self) -> None:
        self.service.submit_event(close())
        # Three distinct continuation events all reference the same head and
        # race each other: an extension, a reopen and a would-be second closure.
        ext = extend("evt-extend-conc01", "evt-close0000001", version=2)
        reo = reopen(
            "evt-reopen-conc01",
            "evt-close0000001",
            version=2,
            effective_from="2026-09-07T18:50:00Z",
        )
        new_close = close(
            "evt-close-conc001",
            event_version=2,
            reported_at="2026-09-07T15:30:00Z",
        )

        outcomes: list[tuple[str, object]] = []

        def submit(label: str, payload: dict) -> None:
            try:
                self.service.submit_event(payload)
                outcomes.append((label, "accepted"))
            except ValidationError as exc:
                outcomes.append((label, ("rejected", exc.details["errors"][0]["issue"])))
            except EventConflictError as exc:
                outcomes.append((label, ("conflict", exc.code)))

        with ThreadPoolExecutor(max_workers=3) as pool:
            futs = [
                pool.submit(submit, "extend", ext),
                pool.submit(submit, "reopen", reo),
                pool.submit(submit, "close", new_close),
            ]
            for f in futs:
                f.result()

        accepted = [label for label, result in outcomes if result == "accepted"]
        self.assertEqual(len(accepted), 1, outcomes)
        self.assertIn(accepted[0], {"extend", "reopen"})

        summary = self.service.airport_summary("APS")
        # Exactly one explainable head, and active/resolved is decided by which
        # continuation event won the race.
        head = summary["head_event_id"]
        self.assertIn(
            head,
            {"evt-extend-conc01", "evt-reopen-conc01"},
        )
        if head == "evt-reopen-conc01":
            self.assertEqual(summary["active_chains"], 0)
        else:
            self.assertEqual(summary["active_chains"], 1)
        # Head impacts and the affected list agree on the same projection.
        rows = self.service.affected_flights(
            airport="APS", status=None, limit=50, offset=0
        )
        head_status = self.service.event_status(head)
        self.assertEqual(
            rows["projection_version"],
            summary["projection_version"],
        )
        self.assertEqual(
            {f["flight_id"] for f in rows["flights"]},
            {i["flight_id"] for i in head_status["impacts"]},
        )

    def test_concurrent_identical_submissions_both_return_original(self) -> None:
        payload = close()
        results: list[dict] = []

        def submit() -> None:
            results.append(self.service.submit_event(json.loads(json.dumps(payload))))

        t1 = threading.Thread(target=submit)
        t2 = threading.Thread(target=submit)
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["impacts"], results[1]["impacts"])
        states = {r["processing_state"] for r in results}
        self.assertEqual(states, {"processed", "replayed"})

    def test_failed_transaction_leaves_no_tombstones(self) -> None:
        self.service.submit_event(close())
        with self.assertRaises(ValidationError):
            # Structural failure (unknown referenced event): whole tx rolls back.
            self.service.submit_event(
                extend("evt-extend-broken1", "evt-missing000001")
            )
        with self.assertRaises(NotFoundError):
            self.service.event_status("evt-extend-broken1")
        count = self.repo._conn.execute(
            "SELECT COUNT(*) AS n FROM impacts WHERE event_id = 'evt-extend-broken1'"
        ).fetchone()["n"]
        self.assertEqual(count, 0)


class CorrectionFlowTest(ServiceTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.service.submit_event(close())
        self.base_version = self.service.airport_summary("APS")[
            "airport_projection_version"
        ]

    def _proposal(self, request_id="req-correct001", **patch) -> dict:
        patch = patch or {"effective_from": "2026-09-07T16:00:00Z"}
        return {
            "request_id": request_id,
            "target_event_id": "evt-close0000001",
            "base_projection_version": self.base_version,
            "patch": patch,
            "submitted_by": "ops-rerun-batch",
            "reason": "historical log correction",
        }

    def test_changing_correction_goes_to_review_without_touching_state(self) -> None:
        result = self.service.submit_correction(self._proposal())
        self.assertEqual(result["status"], "pending")
        self.assertTrue(result["changes_published_result"])
        self.assertTrue(result["impact_delta"]["removed"])
        # Published state is untouched while pending.
        summary = self.service.airport_summary("APS")
        self.assertEqual(summary["affected_flights"], 3)
        self.assertEqual(summary["pending_corrections"], 1)

    def test_non_changing_correction_marked_benign(self) -> None:
        # Re-stating the exact same effective_from changes nothing published.
        result = self.service.submit_correction(
            self._proposal(
                "req-correct002", effective_from="2026-09-07T15:00:00Z"
            )
        )
        self.assertFalse(result["changes_published_result"])
        self.assertEqual(
            result["impact_delta"],
            {"added": [], "removed": [], "changed": []},
        )

    def test_approval_writes_new_generation_and_keeps_original(self) -> None:
        self.service.submit_correction(self._proposal())
        decision = self.service.decide_correction(
            "req-correct001",
            {"decision": "approved", "reviewer_id": "ops-duty-01"},
        )
        self.assertEqual(decision["status"], "approved")
        self.assertEqual(decision["generation"], 2)
        # New published result.
        summary = self.service.airport_summary("APS")
        self.assertEqual(summary["affected_flights"], 2)
        self.assertEqual(summary["generation"], 2)
        # Original snapshot generation is retained.
        gens = {
            r["generation"]
            for r in self.repo._conn.execute(
                "SELECT DISTINCT generation FROM impacts "
                "WHERE event_id = 'evt-close0000001'"
            )
        }
        self.assertEqual(gens, {1, 2})
        status = self.service.event_status("evt-close0000001")
        self.assertEqual(
            status["correction"]["applied_proposal_id"], "req-correct001"
        )
        self.assertEqual(status["correction"]["original_impact_count"], 3)

    def test_rejection_leaves_state_untouched(self) -> None:
        self.service.submit_correction(self._proposal())
        decision = self.service.decide_correction(
            "req-correct001",
            {"decision": "rejected", "reviewer_id": "safety-duty-01",
             "comment": "source log unverified"},
        )
        self.assertEqual(decision["status"], "rejected")
        self.assertEqual(
            self.service.airport_summary("APS")["affected_flights"], 3
        )

    def test_reviewer_roles_enforced(self) -> None:
        self.service.submit_correction(self._proposal())
        with self.assertRaises(ForbiddenError):
            self.service.decide_correction(
                "req-correct001",
                {"decision": "approved", "reviewer_id": "audit-observer-01"},
            )
        with self.assertRaises(ForbiddenError):
            self.service.decide_correction(
                "req-correct001",
                {"decision": "approved", "reviewer_id": "unknown-person"},
            )

    def test_decision_is_idempotent_state_once(self) -> None:
        self.service.submit_correction(self._proposal())
        self.service.decide_correction(
            "req-correct001",
            {"decision": "approved", "reviewer_id": "ops-duty-01"},
        )
        with self.assertRaises(EventConflictError):
            self.service.decide_correction(
                "req-correct001",
                {"decision": "rejected", "reviewer_id": "ops-duty-01"},
            )

    def test_proposal_idempotency_returns_same_record(self) -> None:
        payload = self._proposal()
        first = self.service.submit_correction(payload)
        second = self.service.submit_correction(dict(payload))
        self.assertEqual(first["request_id"], second["request_id"])
        self.assertEqual(second["replay_count"], 1)
        self.assertEqual(first["impact_delta"], second["impact_delta"])

    def test_stale_projection_base_conflicts(self) -> None:
        stale = self._proposal()
        stale["base_projection_version"] = 999
        with self.assertRaises(EventConflictError) as ctx:
            self.service.submit_correction(stale)
        self.assertEqual(ctx.exception.details["issue"], "stale_projection")

    def test_chain_correction_replays_in_scope(self) -> None:
        # close -> extend; correcting the close's start must recompute the
        # extension snapshot too (replay scope covers the whole suffix).
        self.service.submit_event(
            extend(
                "evt-extend000001",
                "evt-close0000001",
                effective_from="2026-09-07T18:55:00Z",
                effective_until="2026-09-07T21:00:00Z",
            )
        )
        base = self.service.airport_summary("APS")["airport_projection_version"]
        proposal = {
            "request_id": "req-correct003",
            "target_event_id": "evt-close0000001",
            "base_projection_version": base,
            "patch": {"effective_from": "2026-09-07T16:00:00Z"},
            "submitted_by": "ops-rerun-batch",
        }
        result = self.service.submit_correction(proposal)
        self.assertEqual(
            result["replay_scope"],
            ["evt-close0000001", "evt-extend000001"],
        )
        self.service.decide_correction(
            "req-correct003",
            {"decision": "approved", "reviewer_id": "ops-duty-01"},
        )
        # Extension head snapshot is recomputed at the airport's new generation.
        summary = self.service.airport_summary("APS")
        ext_status = self.service.event_status("evt-extend000001")
        self.assertEqual(ext_status["processing"]["generation"], summary["generation"])
        self.assertEqual(
            ext_status["processing"]["generation"], 3
        )
        # The extension's original generation-2 snapshot remains preserved.
        gens = {
            r["generation"]
            for r in self.repo._conn.execute(
                "SELECT DISTINCT generation FROM impacts "
                "WHERE event_id = 'evt-extend000001'"
            )
        }
        self.assertEqual(gens, {2, 3})

    def test_unknown_target_404(self) -> None:
        with self.assertRaises(NotFoundError):
            self.service.submit_correction(self._proposal(
                "req-correct004",
                **{"effective_from": "2026-09-07T16:00:00Z"},
            ) | {"target_event_id": "evt-missing000001"})

    def _approve(self, request_id: str, target: str, base: int, patch: dict) -> None:
        self.service.submit_correction(
            {
                "request_id": request_id,
                "target_event_id": target,
                "base_projection_version": base,
                "patch": patch,
                "submitted_by": "ops-rerun-batch",
            }
        )
        self.service.decide_correction(
            request_id,
            {"decision": "approved", "reviewer_id": "ops-duty-01"},
        )

    def test_successive_corrections_layer_instead_of_reverting(self) -> None:
        self._approve(
            "req-layer00001",
            "evt-close0000001",
            self.base_version,
            {"effective_from": "2026-09-07T16:00:00Z"},
        )
        base2 = self.service.airport_summary("APS")["airport_projection_version"]
        # Second patch only touches effective_until; the 16:00 start must stay.
        self._approve(
            "req-layer00002",
            "evt-close0000001",
            base2,
            {"effective_until": "2026-09-07T18:00:00Z"},
        )
        status = self.service.event_status("evt-close0000001")
        # Effective window is 16:00-18:00; AX410 departs 15:30 -> no longer hit.
        affected = {i["flight_id"] for i in status["impacts"]}
        self.assertNotIn("AX-410-20260907", affected)

    def test_extension_after_correction_uses_corrected_window(self) -> None:
        # BSR closure corrected to end 17:00, then extended.
        self.service.submit_event(
            close(
                event_id="evt-bsr-close001",
                airport="BSR",
                effective_from="2026-09-07T15:00:00Z",
                effective_until="2026-09-07T19:00:00Z",
            )
        )
        base = self.service.airport_summary("BSR")["airport_projection_version"]
        self._approve(
            "req-bsrcorr0001",
            "evt-bsr-close001",
            base,
            {"effective_until": "2026-09-07T17:00:00Z"},
        )
        # An extension must be judged against the corrected 17:00 end: a valid
        # 20-minute window ending exactly at 17:00 does not push the end out.
        bad = extend(
            "evt-bsrext-bad1",
            "evt-bsr-close001",
            version=2,
            airport="BSR",
            effective_from="2026-09-07T16:40:00Z",
            effective_until="2026-09-07T17:00:00Z",
        )
        with self.assertRaises(ValidationError) as ctx:
            self.service.submit_event(bad)
        self.assertIn("must_extend_previous_window", str(ctx.exception.details))

        good = extend(
            "evt-bsrext-ok01",
            "evt-bsr-close001",
            version=2,
            airport="BSR",
            effective_from="2026-09-07T16:55:00Z",
            effective_until="2026-09-07T20:00:00Z",
        )
        accepted = self.service.submit_event(good)
        # Window [15:00, 20:00) at BSR; BY205 departs 15:05 and is still hit.
        self.assertIn(
            "BY-205-20260908",
            {i["flight_id"] for i in accepted["impacts"]},
        )

    def test_correction_removing_all_impacts_keeps_generation_complete(self) -> None:
        # Correct the BSR closure to 15:30-17:00, which removes both BSR flights
        # (BY205 dep 15:05, AX410 arr 17:10). The new generation must still exist
        # with resolved tombstones rather than silently falling back to gen 1.
        self.service.submit_event(
            close(
                event_id="evt-bsr-close002",
                airport="BSR",
                effective_from="2026-09-07T15:00:00Z",
                effective_until="2026-09-07T19:00:00Z",
            )
        )
        base = self.service.airport_summary("BSR")["airport_projection_version"]
        self._approve(
            "req-bsrempty001",
            "evt-bsr-close002",
            base,
            {"effective_from": "2026-09-07T15:30:00Z",
             "effective_until": "2026-09-07T17:00:00Z"},
        )
        self.assertEqual(self.service.airport_summary("BSR")["affected_flights"], 0)
        status = self.service.event_status("evt-bsr-close002")
        self.assertGreaterEqual(status["processing"]["generation"], 2)
        self.assertEqual(status["processing"]["impact_count"], 0)
        # The current generation carries resolved rows for the removed flights.
        gen = status["processing"]["generation"]
        resolved = {
            r["flight_id"]
            for r in self.repo._conn.execute(
                "SELECT flight_id FROM impacts WHERE event_id = ? "
                "AND generation = ? AND impact_status = 'resolved'",
                ("evt-bsr-close002", gen),
            )
        }
        self.assertEqual(resolved, {"BY-205-20260908", "AX-410-20260907"})


class RestartAuditTest(ServiceTestCase):
    def test_rejected_and_decided_material_survives_restart(self) -> None:
        self.service.submit_event(close())
        # A rejected out-of-order reopen.
        with self.assertRaises(ValidationError):
            self.service.submit_event(
                reopen(
                    "evt-reopen-bad10",
                    "evt-close0000001",
                    version=9,
                    effective_from="2026-09-07T10:00:00Z",
                )
            )
        # An approved correction.
        base = self.service.airport_summary("APS")["airport_projection_version"]
        proposal = {
            "request_id": "req-correct010",
            "target_event_id": "evt-close0000001",
            "base_projection_version": base,
            "patch": {"effective_from": "2026-09-07T16:00:00Z"},
            "submitted_by": "ops-rerun-batch",
        }
        self.service.submit_correction(proposal)
        self.service.decide_correction(
            "req-correct010",
            {"decision": "approved", "reviewer_id": "ops-duty-01"},
        )

        self.restart_service()

        # Rejected material and its decision are still visible.
        rejected = self.service.event_status("evt-reopen-bad10")
        self.assertEqual(rejected["processing"]["state"], "rejected")
        self.assertTrue(rejected["processing"]["decision"]["reasons"])

        # Approved correction and recomputed result survive.
        record = self.service.get_correction("req-correct010")
        self.assertEqual(record["status"], "approved")
        self.assertEqual(record["reviewer_id"], "ops-duty-01")
        self.assertEqual(record["generation"], 2)
        summary = self.service.airport_summary("APS")
        self.assertEqual(summary["affected_flights"], 2)
        self.assertEqual(summary["generation"], 2)

        # Projection history explains how the state was reached, including the
        # rejection, the proposal, its approval and the replay scope.
        log = self.service.projection_log(airport="APS")
        kinds = [e["kind"] for e in log["entries"]]
        self.assertIn("event_rejected", kinds)
        self.assertIn("correction_proposed", kinds)
        self.assertIn("correction_approved", kinds)
        approval = next(e for e in log["entries"] if e["kind"] == "correction_approved")
        self.assertEqual(
            approval["detail"]["replay_scope"], ["evt-close0000001"]
        )

    def test_queries_share_projection_version_after_restart(self) -> None:
        self.service.submit_event(close())
        self.restart_service()
        summary = self.service.airport_summary("APS")
        flights = self.service.affected_flights(
            airport="APS", status=None, limit=50, offset=0
        )
        status = self.service.event_status("evt-close0000001")
        self.assertEqual(
            summary["projection_version"], flights["projection_version"]
        )
        self.assertEqual(
            summary["projection_version"], status["projection_version"]
        )


if __name__ == "__main__":
    import unittest

    unittest.main()
