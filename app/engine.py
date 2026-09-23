"""航班影响计算引擎。

规则：

* 比较前将所有时间统一转换为带时区的 UTC。时间窗口采用左闭右开语义，
  端点相接不算重叠。
* 航班从受影响机场起飞或抵达受影响机场的计划时刻落入关闭窗口时受影响。
* ``airport.closed`` 建立事件链，``effective_until = null`` 表示结束时间未知。
* ``airport.extended`` 延续事件链，并把窗口延长到当前事件的结束时刻。
* ``airport.reopened`` 结束事件链，机场在恢复缓冲时间结束后重新运行。
* 结束时间未知时结果为 ``pending_confirmation``；可在最大延误内改时的结果
  为 ``delayed``；其余受影响航班为 ``cancelled``。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from app.models import (
    EVENT_CLOSED,
    EVENT_EXTENDED,
    EVENT_REOPENED,
    Airport,
    DisruptionEvent,
    Flight,
    IMPACT_CANCELLED,
    IMPACT_DELAYED,
    IMPACT_PENDING,
)
from app.timeutil import crosses_local_midnight
from app.models import iso_utc

# Severity ordering used when one flight is affected at both endpoints.
_SEVERITY = {IMPACT_DELAYED: 0, IMPACT_PENDING: 1, IMPACT_CANCELLED: 2}


@dataclass(frozen=True)
class ClosureWindow:
    """事件生效后整条链对应的停运窗口。"""

    airport_code: str
    root_event_id: str
    start: datetime
    end: datetime | None  # None == open-ended ("until further notice")
    terminal: bool  # True for a reopened chain

    def contains(self, point: datetime) -> bool:
        if point < self.start:
            return False
        return self.end is None or point < self.end

    def minutes_from(self, point: datetime) -> int | None:
        """计算指定时刻到窗口末端的分钟数；开放窗口返回 None。"""
        if self.end is None:
            return None
        return int((self.end - point).total_seconds() // 60)


def chain_window(
    event: DisruptionEvent,
    root: DisruptionEvent,
    airport: Airport,
) -> ClosureWindow:
    """计算应用当前事件后的有效关闭窗口。"""
    if event.event_type == EVENT_CLOSED:
        return ClosureWindow(
            airport_code=event.airport_code,
            root_event_id=root.event_id,
            start=event.effective_from,
            end=event.effective_until,
            terminal=False,
        )
    if event.event_type == EVENT_EXTENDED:
        if event.effective_until is None:  # validated earlier, defensive
            raise ValueError("extended event must define effective_until")
        return ClosureWindow(
            airport_code=event.airport_code,
            root_event_id=root.event_id,
            start=root.effective_from,
            end=event.effective_until,
            terminal=False,
        )
    # reopened: operations resume after the airport's operational buffer
    resume_at = event.effective_from + timedelta(minutes=airport.reopen_buffer_minutes)
    return ClosureWindow(
        airport_code=event.airport_code,
        root_event_id=root.event_id,
        start=root.effective_from,
        end=resume_at,
        terminal=True,
    )


@dataclass(frozen=True)
class EndpointImpact:
    endpoint: str  # "origin" | "destination"
    needed_delay: int | None  # None == open-ended closure


def _endpoint_impact(
    flight: Flight, endpoint: str, window: ClosureWindow
) -> EndpointImpact | None:
    point = (
        flight.scheduled_departure if endpoint == "origin" else flight.scheduled_arrival
    )
    if not window.contains(point):
        return None
    return EndpointImpact(endpoint=endpoint, needed_delay=window.minutes_from(point))


def classify_flight(
    flight: Flight, window: ClosureWindow
) -> dict[str, Any] | None:
    """返回航班在指定机场的一条影响记录；不受影响时返回 None。"""
    endpoints: list[EndpointImpact] = []
    if flight.origin == window.airport_code:
        impact = _endpoint_impact(flight, "origin", window)
        if impact:
            endpoints.append(impact)
    if flight.destination == window.airport_code:
        impact = _endpoint_impact(flight, "destination", window)
        if impact:
            endpoints.append(impact)
    if not endpoints:
        return None

    if len(endpoints) == 2:
        affected_endpoint = "both"
    else:
        affected_endpoint = endpoints[0].endpoint

    # Required delay is driven by the endpoint that can resume latest.
    finite_needed = [e.needed_delay for e in endpoints if e.needed_delay is not None]
    if len(finite_needed) < len(endpoints):
        status = IMPACT_PENDING
        needed_delay: int | None = None
    else:
        needed_delay = max(finite_needed)  # type: ignore[arg-type]
        if flight.can_retime and needed_delay <= flight.max_delay_minutes:
            status = IMPACT_DELAYED
        else:
            status = IMPACT_CANCELLED

    proposed_departure = proposed_arrival = None
    overlap = needed_delay
    if status == IMPACT_DELAYED:
        proposed_departure = iso_utc(
            flight.scheduled_departure + timedelta(minutes=needed_delay)  # type: ignore[arg-type]
        )
        proposed_arrival = iso_utc(
            flight.scheduled_arrival + timedelta(minutes=needed_delay)  # type: ignore[arg-type]
        )

    return {
        "flight_id": flight.flight_id,
        "flight_number": flight.flight_number,
        "airport_code": window.airport_code,
        "affected_endpoint": affected_endpoint,
        "impact_status": status,
        "overlap_minutes": overlap,
        "delay_minutes": needed_delay if status == IMPACT_DELAYED else None,
        "proposed_departure": proposed_departure,
        "proposed_arrival": proposed_arrival,
        "passenger_count": flight.passenger_count,
    }


def compute_impacts(
    event: DisruptionEvent,
    root: DisruptionEvent,
    airport: Airport,
    flights: dict[str, Flight],
) -> list[dict[str, Any]]:
    """计算一个事件产生的完整且顺序稳定的影响快照。"""
    window = chain_window(event, root, airport)
    midnight = (
        window.end is not None
        and crosses_local_midnight(window.start, window.end, airport_tz(airport))
    )
    rows: list[dict[str, Any]] = []
    for flight in sorted(flights.values(), key=lambda f: f.flight_id):
        record = classify_flight(flight, window)
        if record is None:
            continue
        record["event_id"] = event.event_id
        record["root_event_id"] = window.root_event_id
        record["crosses_midnight"] = 1 if midnight else 0
        rows.append(record)
    return rows


def airport_tz(airport: Airport):
    from zoneinfo import ZoneInfo

    return ZoneInfo(airport.timezone)
