"""测试共享辅助函数。"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.config import ROOT, load_airports, load_flights
from app.repository import Repository
from app.service import DisruptionService

FIXTURES_DIR = ROOT / "fixtures"


class ServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmp.name) / "test.db"
        self.airports = load_airports(FIXTURES_DIR)
        self.flights = load_flights(FIXTURES_DIR, self.airports)
        self.repo = Repository(self.db_path)
        self.service = DisruptionService(self.repo, self.airports, self.flights)

    def tearDown(self) -> None:
        self.repo.close()
        self._tmp.cleanup()

    def restart_service(self) -> DisruptionService:
        """模拟容器重启后重新打开同一数据库文件。"""
        self.repo.close()
        self.repo = Repository(self.db_path)
        self.service = DisruptionService(self.repo, self.airports, self.flights)
        return self.service


def base_event(**overrides) -> dict:
    payload = {
        "event_id": "evt-close0000001",
        "event_version": 1,
        "event_type": "airport.closed",
        "airport_code": "APS",
        "effective_from": "2026-09-07T15:00:00Z",
        "effective_until": "2026-09-07T19:00:00Z",
        "reported_at": "2026-09-07T14:00:00Z",
        "reason": "volcanic ash",
    }
    payload.update(overrides)
    return payload
