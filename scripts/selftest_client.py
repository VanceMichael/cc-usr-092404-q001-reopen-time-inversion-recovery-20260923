#!/usr/bin/env python3
"""机场中断 API 的容器内自检客户端。

两个阶段都只通过 HTTP 调用本地服务：

* ``seed``：在新服务上检查合法事件、非法事件、幂等重放和跨午夜处理。
* ``verify``：容器重启后确认事件、影响和重放计数仍保存在 SQLite 卷中。

脚本只使用 Python 标准库，并且只访问命令行传入的服务地址，不连接外部航班
或地图服务。
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from typing import Any

BASE = sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:8080"

APS_CLOSE = "volc-aps-close001"
BSR_CLOSE = "volc-bsr-close001"
KTA_CLOSE = "volc-kta-close001"
APS_REOPEN = "volc-aps-reopen01"
APS_CLOSE_2 = "volc-aps-close002"

FAILURES: list[str] = []


def check(condition: bool, message: str) -> None:
    marker = "ok  " if condition else "FAIL"
    print(f"  [{marker}] {message}")
    if not condition:
        FAILURES.append(message)


def request(method: str, path: str, body: Any = None, *, raw: bytes | None = None,
            content_type: str = "application/json"):
    if raw is not None:
        data = raw
        headers = {"Content-Type": content_type}
    elif body is not None:
        data = json.dumps(body).encode("utf-8")
        headers = {"Content-Type": "application/json"}
    else:
        data = None
        headers = {}
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def post_event(payload: Any):
    return request("POST", "/api/v1/events", payload)


def impacts_by_flight(result: dict) -> dict[str, dict]:
    return {i["flight_id"]: i for i in result.get("impacts", [])}


# --------------------------------------------------------------------------- #
# Seed phase
# --------------------------------------------------------------------------- #

def seed() -> int:
    print("== seed: malformed / non-JSON requests ==")
    status, body = request(
        "POST", "/api/v1/events", raw=b"{not valid json",
        content_type="application/json",
    )
    check(status == 400, f"malformed JSON -> 400 (got {status})")
    check(body["error"]["code"] == "bad_request", "malformed JSON error code is stable")

    status, body = request(
        "POST", "/api/v1/events", raw=b"{}", content_type="text/plain"
    )
    check(status == 400, f"wrong content type -> 400 (got {status})")
    check(body["error"]["code"] == "unsupported_media_type", "media type error code stable")

    print("== seed: invalid events must be rejected and write nothing ==")
    invalid_submissions = [
        ("unknown airport", {
            "event_id": "evt-invalid-aps01",
            "event_version": 1,
            "event_type": "airport.closed",
            "airport_code": "ZZZ",
            "effective_from": "2026-09-07T15:00:00Z",
            "effective_until": "2026-09-07T19:00:00Z",
            "reported_at": "2026-09-07T14:00:00Z",
        }, 422, "unknown_airport"),
        ("missing required fields", {}, 422, "validation_error"),
        ("unknown field", {
            "event_id": "evt-invalid-aps02",
            "event_version": 1,
            "event_type": "airport.closed",
            "airport_code": "APS",
            "effective_from": "2026-09-07T15:00:00Z",
            "effective_until": "2026-09-07T19:00:00Z",
            "reported_at": "2026-09-07T14:00:00Z",
            "extra": 1,
        }, 422, "validation_error"),
        ("naive timestamp", {
            "event_id": "evt-invalid-aps03",
            "event_version": 1,
            "event_type": "airport.closed",
            "airport_code": "APS",
            "effective_from": "2026-09-07T15:00:00",
            "effective_until": "2026-09-07T19:00:00Z",
            "reported_at": "2026-09-07T14:00:00Z",
        }, 422, "validation_error"),
        ("inverted window", {
            "event_id": "evt-invalid-aps04",
            "event_version": 1,
            "event_type": "airport.closed",
            "airport_code": "APS",
            "effective_from": "2026-09-07T19:00:00Z",
            "effective_until": "2026-09-07T15:00:00Z",
            "reported_at": "2026-09-07T14:00:00Z",
        }, 422, "validation_error"),
        ("version must be >= 1", {
            "event_id": "evt-invalid-aps05",
            "event_version": 0,
            "event_type": "airport.closed",
            "airport_code": "APS",
            "effective_from": "2026-09-07T15:00:00Z",
            "effective_until": "2026-09-07T19:00:00Z",
            "reported_at": "2026-09-07T14:00:00Z",
        }, 422, "validation_error"),
        ("bad airport pattern", {
            "event_id": "evt-invalid-aps06",
            "event_version": 1,
            "event_type": "airport.closed",
            "airport_code": "aps",
            "effective_from": "2026-09-07T15:00:00Z",
            "effective_until": "2026-09-07T19:00:00Z",
            "reported_at": "2026-09-07T14:00:00Z",
        }, 422, "validation_error"),
    ]
    invalid_ids = []
    for label, payload, want_status, want_code in invalid_submissions:
        status, body = post_event(payload)
        check(status == want_status, f"{label}: status {want_status} (got {status})")
        check(
            body.get("error", {}).get("code") == want_code,
            f"{label}: error code '{want_code}' (got {body.get('error', {}).get('code')})",
        )
        if isinstance(payload, dict) and "event_id" in payload:
            invalid_ids.append(payload["event_id"])

    print("== seed: valid closure events ==")
    aps_close = {
        "event_id": APS_CLOSE,
        "event_version": 1,
        "event_type": "airport.closed",
        "airport_code": "APS",
        "effective_from": "2026-09-07T15:00:00Z",
        "effective_until": "2026-09-07T19:00:00Z",
        "reported_at": "2026-09-07T14:00:00Z",
        "reason": "volcanic ash from Mount Api",
    }
    status, aps_result = post_event(aps_close)
    check(status == 201, f"APS closure accepted (got {status})")
    check(aps_result["processing_state"] == "processed", "APS result state=processed")
    check(aps_result["impact_count"] == 3, "APS closure impacts 3 flights "
          f"(got {aps_result['impact_count']})")
    check(aps_result["affected_passengers"] == 441,
          f"APS closure affects 441 passengers (got {aps_result['affected_passengers']})")
    aps_impacts = impacts_by_flight(aps_result)
    expected_aps = {
        "AX-410-20260907": ("cancelled", 210),
        "AX-412-20260908": ("cancelled", 150),
        "BY-205-20260908": ("cancelled", 135),
    }
    for fid, (want_status_, want_overlap) in expected_aps.items():
        record = aps_impacts.get(fid)
        check(record is not None, f"APS impact includes {fid}")
        if record:
            check(record["impact_status"] == want_status_,
                  f"{fid} status {want_status_} (got {record['impact_status']})")
            check(record["overlap_minutes"] == want_overlap,
                  f"{fid} overlap {want_overlap} min (got {record['overlap_minutes']})")
            check(record["crosses_midnight"] is True,
                  f"{fid} flagged as crossing local midnight (23:00-03:00 WITA)")

    # Open-ended closure ("until further notice") -> pending_confirmation.
    bsr_close = {
        "event_id": BSR_CLOSE,
        "event_version": 1,
        "event_type": "airport.closed",
        "airport_code": "BSR",
        "effective_from": "2026-09-07T15:00:00Z",
        "effective_until": None,
        "reported_at": "2026-09-07T14:05:00Z",
        "reason": "ash cloud, reopening time unknown",
    }
    status, bsr_result = post_event(bsr_close)
    check(status == 201, f"BSR open-ended closure accepted (got {status})")
    bsr_impacts = impacts_by_flight(bsr_result)
    check(
        bsr_impacts.get("BY-205-20260908", {}).get("impact_status") == "pending_confirmation",
        "BY205 (departs BSR 15:05) at open-ended closure is pending_confirmation",
    )
    check(
        bsr_impacts.get("AX-410-20260907", {}).get("impact_status") == "pending_confirmation",
        "AX410 (arrives BSR 17:10) at open-ended closure is pending_confirmation",
    )
    check(
        bsr_impacts.get("BY-205-20260908", {}).get("overlap_minutes") is None,
        "pending impact has no numeric overlap",
    )

    # KTA 16:30-18:00Z == 23:30-01:00 WIB: crosses local midnight; KX099's
    # 17:40Z departure needs a 20 minute hold, within its 45 minute retime limit.
    kta_close = {
        "event_id": KTA_CLOSE,
        "event_version": 1,
        "event_type": "airport.closed",
        "airport_code": "KTA",
        "effective_from": "2026-09-07T16:30:00Z",
        "effective_until": "2026-09-07T18:00:00Z",
        "reported_at": "2026-09-07T14:10:00Z",
    }
    status, kta_result = post_event(kta_close)
    check(status == 201, f"KTA closure accepted (got {status})")
    kx = impacts_by_flight(kta_result).get("KX-099-20260908")
    check(kx is not None and kx["impact_status"] == "delayed", "KX099 delayed at KTA")
    check(kx is not None and kx["delay_minutes"] == 20, "KX099 delay is 20 minutes")
    check(
        kx is not None and kx["proposed_departure"] == "2026-09-07T18:00:00Z",
        "KX099 proposed departure rescheduled to window end",
    )
    check(kx is not None and kx["crosses_midnight"] is True,
          "KX099 closure window crosses Jakarta local midnight")

    print("== seed: idempotent replay returns the original results ==")
    for label, payload, original in (
        ("APS", aps_close, aps_result),
        ("BSR", bsr_close, bsr_result),
        ("KTA", kta_close, kta_result),
    ):
        status, replayed = post_event(dict(payload))
        check(status == 201, f"{label} replay accepted (got {status})")
        check(replayed["processing_state"] == "replayed",
              f"{label} replay marked 'replayed'")
        check(replayed["impacts"] == original["impacts"],
              f"{label} replay returns the original impact set")

    status, aps_status = request("GET", f"/api/v1/events/{APS_CLOSE}")
    check(status == 200 and aps_status["processing"]["replay_count"] == 1,
          f"APS replay_count persisted on event status (got {aps_status.get('processing', {}).get('replay_count')})")

    print("== seed: same event_id with altered body conflicts ==")
    changed = dict(aps_close)
    changed["reason"] = "contradictory update"
    status, body = post_event(changed)
    check(status == 409 and body["error"]["code"] == "event_conflict",
          f"same id/different payload -> 409 event_conflict (got {status})")
    changed_v2 = dict(aps_close)
    changed_v2["event_version"] = 2
    status, body = post_event(changed_v2)
    check(status == 409, "same id with bumped version still conflicts")

    print("== seed: chain validation (extend / reopen) ==")
    status, body = post_event({
        "event_id": "evt-dangling-ext01",
        "event_version": 2,
        "event_type": "airport.extended",
        "airport_code": "APS",
        "effective_from": "2026-09-07T18:55:00Z",
        "effective_until": "2026-09-07T21:00:00Z",
        "reported_at": "2026-09-07T18:00:00Z",
        "supersedes_event_id": "evt-does-not-exist1",
    })
    check(status == 422 and body["error"]["code"] == "validation_error",
          f"extension of unknown event rejected (got {status})")
    status, _ = request("GET", "/api/v1/events/evt-dangling-ext01")
    check(status == 404, "failed extension produced no event row")

    # Reopen APS at 15:10Z; with APS's 20 minute buffer operations resume at
    # 15:30Z. AX410 departs at exactly 15:30: touching a half-open interval
    # end means it is no longer impacted - all three flights are resolved.
    reopen = {
        "event_id": APS_REOPEN,
        "event_version": 2,
        "event_type": "airport.reopened",
        "airport_code": "APS",
        "effective_from": "2026-09-07T15:10:00Z",
        "reported_at": "2026-09-07T15:12:00Z",
        "supersedes_event_id": APS_CLOSE,
    }
    status, reopen_result = post_event(reopen)
    check(status == 201, f"APS reopen accepted (got {status})")
    check(reopen_result["impact_count"] == 0,
          f"reopen frees all APS flights (active impacts {reopen_result['impact_count']})")
    check(reopen_result["resolved_count"] == 3,
          f"three prior impacts recorded resolved (got {reopen_result['resolved_count']})")

    print("== seed: offset timestamps normalize and cross midnight ==")
    # Fresh incident submitted with local +08:00 offsets; exactly the same
    # UTC window as the original APS closure, so impacts must be identical.
    aps_close_2 = {
        "event_id": APS_CLOSE_2,
        "event_version": 3,
        "event_type": "airport.closed",
        "airport_code": "APS",
        "effective_from": "2026-09-07T23:00:00+08:00",
        "effective_until": "2026-09-08T03:00:00+08:00",
        "reported_at": "2026-09-07T22:30:00+08:00",
        "reason": "second ash wave",
    }
    status, aps2_result = post_event(aps_close_2)
    check(status == 201, f"offset-timestamp closure accepted (got {status})")
    new_impacts = impacts_by_flight(aps2_result)
    same = all(
        new_impacts.get(fid, {}).get("impact_status") == want_status
        and new_impacts.get(fid, {}).get("overlap_minutes") == want_overlap
        for fid, (want_status, want_overlap) in expected_aps.items()
    )
    check(same, "offset window computes the same cancellations/overlaps as UTC window")
    check(all(i["crosses_midnight"] for i in aps2_result["impacts"]),
          "all offset-window impacts flagged cross-midnight")

    print("== seed: summaries, filters, pagination ==")
    status, summary = request("GET", "/api/v1/airports/APS/summary")
    check(status == 200, "APS summary 200")
    check(summary["active_chains"] == 1,
          f"one active APS chain after reopen + new closure (got {summary['active_chains']})")
    check(summary["affected_flights"] == 3,
          f"APS summary lists 3 affected flights (got {summary['affected_flights']})")
    check(summary["by_status"]["cancelled"]["flight_count"] == 3,
          "APS summary groups 3 cancelled")

    status, bsr_summary = request("GET", "/api/v1/airports/BSR/summary")
    check(
        bsr_summary["by_status"].get("pending_confirmation", {}).get("flight_count") == 2,
        "BSR summary groups 2 pending flights (BY205 departure, AX410 arrival)",
    )

    status, page1 = request("GET", "/api/v1/flights/affected?limit=2&offset=0")
    status2, page2 = request("GET", "/api/v1/flights/affected?limit=2&offset=2")
    status3, page3 = request("GET", "/api/v1/flights/affected?limit=2&offset=4")
    check(status == 200 and page1["pagination"]["total"] == 6,
          f"6 latest affected flight/airport pairs (got {page1['pagination']['total']})")
    check([len(p["flights"]) for p in (page1, page2, page3)] == [2, 2, 2],
          "pagination returns pages of 2, 2, 2")
    page_ids = {
        (f["flight_id"], f["airport_code"])
        for p in (page1, page2, page3) for f in p["flights"]
    }
    check(len(page_ids) == 6, "paginated flight/airport pairs are distinct")

    _, cancelled = request("GET", "/api/v1/flights/affected?status=cancelled")
    check(cancelled["pagination"]["total"] == 3, "cancelled filter -> 3")
    _, pending = request("GET", "/api/v1/flights/affected?status=pending_confirmation&airport=BSR")
    pending_ids = {f["flight_id"] for f in pending["flights"]}
    check(pending["pagination"]["total"] == 2
          and pending_ids == {"BY-205-20260908", "AX-410-20260907"},
          "pending+BSR filter -> BY205 and AX410")
    status, bad_filter = request("GET", "/api/v1/flights/affected?status=bogus")
    check(status == 422, f"unknown status filter rejected (got {status})")
    status, bad_limit = request("GET", "/api/v1/flights/affected?limit=0")
    check(status == 400, f"limit=0 rejected (got {status})")

    status, _ = request("GET", "/api/v1/events/evt-missing00001")
    check(status == 404, "unknown event -> 404")
    status, _ = request("GET", "/api/v1/airports/ZZZ/summary")
    check(status == 404, "unknown airport summary -> 404")
    for invalid_id in invalid_ids:
        status, _ = request("GET", f"/api/v1/events/{invalid_id}")
        check(status == 404, f"rejected event '{invalid_id}' was never persisted")

    print("== seed: health ==")
    status, health = request("GET", "/healthz")
    check(status == 200 and health["status"] == "ok", "health endpoint reports ok")

    return finish("seed")


# --------------------------------------------------------------------------- #
# Verify phase (after container restart)
# --------------------------------------------------------------------------- #

def verify() -> int:
    print("== verify: service is healthy after restart ==")
    status, health = request("GET", "/healthz")
    check(status == 200 and health["status"] == "ok", "health endpoint ok after restart")

    print("== verify: events and impacts survived the restart ==")
    status, aps1 = request("GET", f"/api/v1/events/{APS_CLOSE}")
    check(status == 200 and len(aps1["impacts"]) == 3,
          "original APS closure with 3 impacts survived")
    check(aps1["processing"]["replay_count"] == 1,
          f"replay_count=1 survived (got {aps1['processing']['replay_count']})")

    status, aps2 = request("GET", f"/api/v1/events/{APS_CLOSE_2}")
    check(status == 200 and len(aps2["impacts"]) == 3,
          "second (offset) APS closure with 3 impacts survived")
    check(all(i["crosses_midnight"] for i in aps2["impacts"]),
          "cross-midnight flags survived")

    status, reopened = request("GET", f"/api/v1/events/{APS_REOPEN}")
    check(status == 200 and reopened["processing"]["resolved_count"] == 3,
          "reopen event with 3 resolutions survived")

    status, bsr = request("GET", f"/api/v1/events/{BSR_CLOSE}")
    by = impacts_by_flight(bsr)
    check(
        by.get("BY-205-20260908", {}).get("impact_status") == "pending_confirmation",
        "BSR open-ended pending impact (BY205) survived",
    )
    check(
        by.get("AX-410-20260907", {}).get("impact_status") == "pending_confirmation",
        "BSR open-ended pending impact (AX410) survived",
    )
    status, kta = request("GET", f"/api/v1/events/{KTA_CLOSE}")
    kx = impacts_by_flight(kta).get("KX-099-20260908")
    check(kx is not None and kx["impact_status"] == "delayed" and kx["delay_minutes"] == 20,
          "KTA delayed impact survived")

    _, page = request("GET", "/api/v1/flights/affected?limit=100")
    check(page["pagination"]["total"] == 6,
          f"latest affected-flight view still totals 6 (got {page['pagination']['total']})")

    print("== verify: idempotency still works against persisted state ==")
    aps_close_2 = {
        "event_id": APS_CLOSE_2,
        "event_version": 3,
        "event_type": "airport.closed",
        "airport_code": "APS",
        "effective_from": "2026-09-07T23:00:00+08:00",
        "effective_until": "2026-09-08T03:00:00+08:00",
        "reported_at": "2026-09-07T22:30:00+08:00",
        "reason": "second ash wave",
    }
    count_before = aps2["processing"]["replay_count"]
    status, replay_after = post_event(aps_close_2)
    check(status == 201 and replay_after["processing_state"] == "replayed",
          "duplicate submission after restart replays")
    check(replay_after["impacts"] == aps2["impacts"],
          "post-restart replay returns identical original result")
    status, aps2_again = request("GET", f"/api/v1/events/{APS_CLOSE_2}")
    check(aps2_again["processing"]["replay_count"] == count_before + 1,
          f"replay_count advanced by one after restart "
          f"({count_before} -> {aps2_again['processing']['replay_count']})")

    print("== verify: validation still enforced after restart ==")
    status, body = post_event({
        "event_id": "evt-post-restart01",
        "event_version": 9,
        "event_type": "airport.closed",
        "airport_code": "ZZZ",
        "effective_from": "2026-09-07T15:00:00Z",
        "effective_until": "2026-09-07T19:00:00Z",
        "reported_at": "2026-09-07T14:00:00Z",
    })
    check(status == 422 and body["error"]["code"] == "unknown_airport",
          "unknown airport still rejected after restart")

    return finish("verify")


def finish(phase: str) -> int:
    if FAILURES:
        print(f"\n{phase.upper()} FAILED: {len(FAILURES)} assertion(s) failed")
        for message in FAILURES:
            print(f"  - {message}")
        return 1
    print(f"\n{phase.upper()} PASSED")
    return 0


def main() -> int:
    phase = sys.argv[1] if len(sys.argv) > 1 else "seed"
    if phase == "seed":
        return seed()
    if phase == "verify":
        return verify()
    print(f"unknown phase: {phase}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
