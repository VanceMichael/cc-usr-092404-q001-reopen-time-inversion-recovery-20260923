"""历史更正提案、待复核裁定与版本重放测试。"""

from __future__ import annotations

from app.errors import (
    ConflictStateError,
    EventConflictError,
    ForbiddenReviewerError,
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
        "effective_from": kw.pop("effective_from", "2026-09-07T16:55:00Z"),
        "effective_until": kw.pop("effective_until", "2026-09-07T19:30:00Z"),
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
        "effective_from": kw.pop("effective_from", "2026-09-07T16:25:00Z"),
        "reported_at": kw.pop("reported_at", "2026-09-07T15:00:00Z"),
        "supersedes_event_id": supersedes,
    }
    payload.update(kw)
    return payload


class CorrectionProposalTest(ServiceTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.service.submit_event(close())
        self.v1 = self.service.current_projection()

    def _shorten(self, request_id="cor-shorten00001", *, base=None,
                 reason="ash cleared earlier than first reported", **patch):
        return {
            "request_id": request_id,
            "target_event_id": "evt-close0000001",
            "base_projection_version": self.v1 if base is None else base,
            "patch": patch or {"effective_until": "2026-09-07T15:30:00Z"},
            "submitted_by": "ops-duty-01",
            "reason": reason,
        }

    def test_proposal_enters_review_and_keeps_published_result(self) -> None:
        result = self.service.submit_correction(self._shorten())
        self.assertEqual(result["state"], "pending_review")
        self.assertGreaterEqual(result["scope"]["change_count"], 1)
        # Published results are untouched while pending.
        self.assertEqual(self.service.airport_summary("APS")["affected_flights"], 3)
        listing = self.service.list_corrections(state="pending_review")
        self.assertEqual(
            [c["request_id"] for c in listing["corrections"]],
            ["cor-shorten00001"],
        )

    def test_proposal_duplicate_returns_original(self) -> None:
        first = self.service.submit_correction(self._shorten())
        second = self.service.submit_correction(self._shorten())
        self.assertEqual(second["state"], first["state"])
        self.assertEqual(self.service.list_corrections()["corrections"].__len__(), 1)

    def test_proposal_duplicate_changed_payload_conflicts(self) -> None:
        self.service.submit_correction(self._shorten())
        changed = self._shorten(reason="different justification")
        with self.assertRaises(EventConflictError):
            self.service.submit_correction(changed)

    def test_stale_base_version_conflicts(self) -> None:
        with self.assertRaises(ConflictStateError):
            self.service.submit_correction(self._shorten(base=999))

    def test_unknown_target_event_404(self) -> None:
        proposal = self._shorten()
        proposal["target_event_id"] = "evt-missing00001"
        with self.assertRaises(NotFoundError):
            self.service.submit_correction(proposal)

    def test_inverted_correction_rejected(self) -> None:
        proposal = self._shorten(effective_until="2026-09-07T14:00:00Z")
        with self.assertRaises(ValidationError):
            self.service.submit_correction(proposal)

    def test_correction_that_reopens_before_start_rejected(self) -> None:
        proposal = self._shorten(effective_from="2026-09-07T20:00:00Z")
        with self.assertRaises(ValidationError) as ctx:
            self.service.submit_correction(proposal)
        issues_ = {e["issue"] for e in ctx.exception.details["errors"]}
        self.assertIn("must_be_after_effective_from", issues_)


class CorrectionDecisionTest(ServiceTestCase):
    def setUp(self) -> None:
        super().setUp()
        # Chain 16:20 closure -> extend to 19:30 -> reopen 16:25 (resume 16:45).
        self.service.submit_event(
            close(
                effective_from="2026-09-07T16:20:00Z",
                effective_until="2026-09-07T17:00:00Z",
            )
        )
        self.service.submit_event(
            extend("evt-extend000001", "evt-close0000001")
        )
        self.service.submit_event(
            reopen("evt-reopen000001", "evt-extend000001")
        )
        self.base = self.service.current_projection()

    def test_auditor_cannot_adjudicate(self) -> None:
        self.service.submit_correction(
            {
                "request_id": "cor-decision001",
                "target_event_id": "evt-extend000001",
                "base_projection_version": self.base,
                "patch": {"effective_until": "2026-09-07T18:00:00Z"},
                "submitted_by": "ops-duty-01",
            }
        )
        with self.assertRaises(ForbiddenReviewerError):
            self.service.decide_correction(
                "cor-decision001", "approved", "audit-observer-01", None
            )

    def test_unknown_reviewer_404(self) -> None:
        self.service.submit_correction(
            {
                "request_id": "cor-decision002",
                "target_event_id": "evt-extend000001",
                "base_projection_version": self.base,
                "patch": {"effective_until": "2026-09-07T18:00:00Z"},
                "submitted_by": "ops-duty-01",
            }
        )
        with self.assertRaises(NotFoundError):
            self.service.decide_correction(
                "cor-decision002", "approved", "nobody-duty-99", None
            )

    def test_approve_recomputes_and_versions_result(self) -> None:
        before = self.service.airport_summary("APS")
        self.assertEqual(before["affected_flights"], 1)  # AX412 still cancelled

        self.service.submit_correction(
            {
                "request_id": "cor-decision003",
                "target_event_id": "evt-extend000001",
                "base_projection_version": self.base,
                "patch": {"effective_until": "2026-09-07T18:00:00Z"},
                "submitted_by": "safety-duty-01",
            }
        )
        decision = self.service.decide_correction(
            "cor-decision003", "approved", "safety-duty-01", "tower confirmed"
        )
        self.assertEqual(decision["state"], "approved")
        self.assertIn("evt-extend000001", decision["scope"]["events_recomputed"])

        status = self.service.event_status("evt-extend000001")
        self.assertEqual(
            status["processing"]["amended_by"], ["cor-decision003"]
        )
        self.assertEqual(
            status["effective_event"]["effective_until"], "2026-09-07T18:00:00Z"
        )
        # Original event payload is preserved, not overwritten.
        self.assertEqual(
            status["event"]["effective_until"], "2026-09-07T19:30:00Z"
        )

    def test_double_decision_conflicts(self) -> None:
        self.service.submit_correction(
            {
                "request_id": "cor-decision004",
                "target_event_id": "evt-extend000001",
                "base_projection_version": self.base,
                "patch": {"effective_until": "2026-09-07T18:00:00Z"},
                "submitted_by": "ops-duty-01",
            }
        )
        self.service.decide_correction(
            "cor-decision004", "rejected", "ops-duty-01", "not credible"
        )
        with self.assertRaises(EventConflictError):
            self.service.decide_correction(
                "cor-decision004", "approved", "ops-duty-01", None
            )


class ProjectionReplayTest(ServiceTestCase):
    def test_replay_consistent_across_queries(self) -> None:
        self.service.submit_event(
            close(
                effective_from="2026-09-07T15:00:00Z",
                effective_until="2026-09-07T19:00:00Z",
            )
        )
        v1 = self.service.current_projection()
        proposal = {
            "request_id": "cor-replay00001",
            "target_event_id": "evt-close0000001",
            "base_projection_version": v1,
            "patch": {"effective_until": "2026-09-07T15:30:00Z"},
            "submitted_by": "ops-duty-01",
        }
        self.service.submit_correction(proposal)
        v_after_proposal = self.service.current_projection()
        self.service.decide_correction(
            "cor-replay00001", "approved", "ops-duty-01", None
        )

        # Current view: window freed every flight (AX410 15:30 touches the end).
        self.assertEqual(self.service.airport_summary("APS")["affected_flights"], 0)
        self.assertEqual(
            self.service.affected_flights(
                airport="APS", status=None, limit=50, offset=0
            )["pagination"]["total"],
            0,
        )

        # Replay at v1: event, summary and flights all show the old 3-flight result.
        old_summary = self.service.airport_summary(
            "APS", projection_version=v1
        )
        self.assertEqual(old_summary["affected_flights"], 3)
        self.assertEqual(old_summary["projection_version"], v1)
        old_flights = self.service.affected_flights(
            airport="APS", status=None, limit=50, offset=0,
            projection_version=v1,
        )
        self.assertEqual(old_flights["pagination"]["total"], 3)
        old_event = self.service.event_status(
            "evt-close0000001", projection_version=v1
        )
        self.assertEqual(old_event["processing"]["amended_by"], [])
        self.assertEqual(
            old_event["effective_event"]["effective_until"], "2026-09-07T19:00:00Z"
        )

        # Pending-proposal version still shows the old published result too.
        self.assertEqual(
            self.service.airport_summary(
                "APS", projection_version=v_after_proposal
            )["affected_flights"],
            3,
        )

    def test_future_version_404(self) -> None:
        self.service.submit_event(close())
        with self.assertRaises(NotFoundError):
            self.service.airport_summary("APS", projection_version=999)

    def test_rejections_and_decisions_survive_restart(self) -> None:
        self.service.submit_event(
            close(
                effective_from="2026-09-07T15:00:00Z",
                effective_until="2026-09-07T19:00:00Z",
            )
        )
        # A rejected out-of-order reopen.
        try:
            self.service.submit_event(
                {
                    "event_id": "evt-reopen-bad01",
                    "event_version": 2,
                    "event_type": "airport.reopened",
                    "airport_code": "APS",
                    "effective_from": "2026-09-07T14:00:00Z",
                    "reported_at": "2026-09-07T15:00:00Z",
                    "supersedes_event_id": "evt-close0000001",
                }
            )
        except ValidationError:
            pass

        v1 = self.service.current_projection() - 1
        proposal = {
            "request_id": "cor-restart0001",
            "target_event_id": "evt-close0000001",
            "base_projection_version": self.service.current_projection(),
            "patch": {"effective_until": "2026-09-07T15:30:00Z"},
            "submitted_by": "ops-duty-01",
        }
        self.service.submit_correction(proposal)
        self.service.decide_correction(
            "cor-restart0001", "approved", "ops-duty-01", "after-hours review"
        )

        self.restart_service()

        # Rejected material, decision and adopted amendment are all visible.
        self.assertEqual(self.service.rejected_material()["count"], 1)
        correction = self.service.correction_status("cor-restart0001")
        self.assertEqual(correction["state"], "approved")
        self.assertEqual(correction["decided_by"], "ops-duty-01")
        self.assertEqual(
            self.service.event_status("evt-close0000001")[
                "processing"
            ]["amended_by"],
            ["cor-restart0001"],
        )
        journal = self.service.journal()
        kinds = [e["kind"] for e in journal["entries"]]
        self.assertIn("event_adopted", kinds)
        self.assertIn("event_rejected", kinds)
        self.assertIn("correction_adopted", kinds)
        # Historical replay still works.
        self.assertEqual(
            self.service.airport_summary("APS", projection_version=v1)[
                "affected_flights"
            ],
            3,
        )


if __name__ == "__main__":
    import unittest

    unittest.main()
