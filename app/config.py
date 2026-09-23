"""配置与领域夹具加载。"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from app.errors import AppError
from app.models import Airport, Flight
from app.timeutil import load_timezone, parse_event_datetime

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_FIXTURES_DIR = ROOT / "fixtures"
DEFAULT_DB_PATH = ROOT / "data" / "disruptions.db"


@dataclass(frozen=True)
class Config:
    fixtures_dir: Path
    db_path: Path
    host: str
    port: int

    @classmethod
    def from_env(cls) -> "Config":
        fixtures_dir = Path(os.environ.get("FIXTURES_DIR", DEFAULT_FIXTURES_DIR))
        db_path = Path(os.environ.get("DB_PATH", DEFAULT_DB_PATH))
        host = os.environ.get("HOST", "0.0.0.0")
        port = int(os.environ.get("PORT", "8080"))
        return cls(fixtures_dir=fixtures_dir, db_path=db_path, host=host, port=port)


def _load_json(path: Path) -> object:
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        raise AppError(f"Required fixture file is missing: {path}") from None
    except json.JSONDecodeError as exc:
        raise AppError(f"Fixture file {path} is not valid JSON: {exc.msg}") from None


def _require_fields(obj: dict, fields: tuple, source: str) -> None:
    missing = [f for f in fields if f not in obj]
    if missing:
        raise AppError(f"{source} is missing required field(s): {', '.join(missing)}")


def load_airports(fixtures_dir: Path) -> dict[str, Airport]:
    raw = _load_json(fixtures_dir / "airports.json")
    if not isinstance(raw, list):
        raise AppError("fixtures/airports.json must be a JSON array")
    airports: dict[str, Airport] = {}
    for idx, item in enumerate(raw):
        source = f"airports[{idx}]"
        if not isinstance(item, dict):
            raise AppError(f"{source} must be an object")
        _require_fields(
            item, ("code", "name", "timezone", "reopen_buffer_minutes"), source
        )
        code = item["code"]
        if not isinstance(code, str) or len(code) != 3 or not code.isupper():
            raise AppError(f"{source}.code must be a 3-letter uppercase string")
        buffer = item["reopen_buffer_minutes"]
        if not isinstance(buffer, int) or isinstance(buffer, bool) or buffer < 0:
            raise AppError(f"{source}.reopen_buffer_minutes must be a non-negative integer")
        tz = load_timezone(str(item["timezone"]))  # validated up front
        airports[code] = Airport(
            code=code,
            name=str(item["name"]),
            timezone=str(tz),
            reopen_buffer_minutes=buffer,
        )
    if not airports:
        raise AppError("fixtures/airports.json contains no airports")
    return airports


def load_flights(fixtures_dir: Path, airports: dict[str, Airport]) -> dict[str, Flight]:
    raw = _load_json(fixtures_dir / "flights.json")
    if not isinstance(raw, list):
        raise AppError("fixtures/flights.json must be a JSON array")
    flights: dict[str, Flight] = {}
    for idx, item in enumerate(raw):
        source = f"flights[{idx}]"
        if not isinstance(item, dict):
            raise AppError(f"{source} must be an object")
        _require_fields(
            item,
            (
                "flight_id",
                "flight_number",
                "origin",
                "destination",
                "scheduled_departure",
                "scheduled_arrival",
                "passenger_count",
                "can_retime",
                "max_delay_minutes",
            ),
            source,
        )
        flight_id = item["flight_id"]
        if not isinstance(flight_id, str) or not flight_id:
            raise AppError(f"{source}.flight_id must be a non-empty string")
        if flight_id in flights:
            raise AppError(f"Duplicate flight_id in fixtures: {flight_id}")
        for endpoint in ("origin", "destination"):
            code = item[endpoint]
            if code not in airports:
                raise AppError(
                    f"{source}.{endpoint} references unknown airport code '{code}'"
                )
        departure = parse_fixture_datetime(item["scheduled_departure"], f"{source}.scheduled_departure")
        arrival = parse_fixture_datetime(item["scheduled_arrival"], f"{source}.scheduled_arrival")
        if arrival <= departure:
            raise AppError(f"{source}: scheduled_arrival must be after scheduled_departure")
        passengers = item["passenger_count"]
        if not isinstance(passengers, int) or isinstance(passengers, bool) or passengers < 0:
            raise AppError(f"{source}.passenger_count must be a non-negative integer")
        can_retime = item["can_retime"]
        if not isinstance(can_retime, bool):
            raise AppError(f"{source}.can_retime must be a boolean")
        max_delay = item["max_delay_minutes"]
        if not isinstance(max_delay, int) or isinstance(max_delay, bool) or max_delay < 0:
            raise AppError(f"{source}.max_delay_minutes must be a non-negative integer")
        if not can_retime and max_delay != 0:
            raise AppError(f"{source}: max_delay_minutes must be 0 when can_retime is false")
        flights[flight_id] = Flight(
            flight_id=flight_id,
            flight_number=str(item["flight_number"]),
            origin=item["origin"],
            destination=item["destination"],
            scheduled_departure=departure,
            scheduled_arrival=arrival,
            passenger_count=passengers,
            can_retime=can_retime,
            max_delay_minutes=max_delay,
        )
    if not flights:
        raise AppError("fixtures/flights.json contains no flights")
    return flights


def parse_fixture_datetime(value: object, field: str) -> datetime:
    from datetime import timezone

    if not isinstance(value, str):
        raise AppError(f"{field} must be a string")
    text = value.strip()
    if not text.endswith(("Z", "+00:00")):
        # Fixture timestamps are documented as ISO 8601 UTC values.
        raise AppError(f"{field} must be a UTC timestamp (end with Z)")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        raise AppError(f"{field} is not a valid ISO 8601 date-time") from None
    return dt.astimezone(timezone.utc)
