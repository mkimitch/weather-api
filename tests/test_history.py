import json
import sqlite3
import tempfile
import unittest
import zlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import history
from timeutils import day_bounds, local_noon


class HistoryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config = {"cache_dir": self.temp.name}
        self.location = {"name": "home", "lat": 44.62, "lon": -93.14, "timezone": "America/Chicago"}
        self.now = datetime(2026, 10, 30, 12, tzinfo=timezone.utc)
        start, end = day_bounds("2026-11-01", self.location["timezone"])
        self.normalized = {"updatedAt": self.now.isoformat(), "timezone": self.location["timezone"],
                           "daily": [{"date": "2026-11-01", "tempC": {"min": 1, "max": 8}, "pop": 0,
                                      "_period": {"start": start.isoformat(), "end": end.isoformat(), "complete": True}}],
                           "hourly": [{"time": local_noon("2026-11-01", self.location["timezone"]).isoformat(), "windKph": 0}]}

    def record(self):
        history.record_forecast(self.config, self.location, "openweather", {"daily": [{"dt": 1, "temp": {"max": 8}}], "apikey": "must-not-store"}, self.normalized)

    def test_snapshots_append_and_duplicate_ingestion_is_idempotent(self):
        self.record()
        self.record()
        self.normalized["updatedAt"] = (self.now + timedelta(minutes=5)).isoformat()
        self.normalized["daily"][0]["tempC"]["max"] = 12
        self.record()
        with history.database(self.config) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM responses").fetchone()[0], 2)
            rows = db.execute("SELECT f.*,r.payload FROM forecasts f JOIN responses r ON r.id=f.response_id ORDER BY r.retrieved_at").fetchall()
        self.assertEqual(json.loads(rows[0]["data"])["tempC"]["max"], 8)
        self.assertEqual(json.loads(rows[1]["data"])["tempC"]["max"], 12)
        self.assertEqual(rows[0]["lead_hours"], 41)
        self.assertEqual((datetime.fromisoformat(rows[0]["valid_end"]) - datetime.fromisoformat(rows[0]["valid_start"])).total_seconds(), 25 * 3600)
        payload = zlib.decompress(rows[0]["payload"]).decode()
        self.assertNotIn("must-not-store", payload)
        self.assertEqual(json.loads(payload)["originalUnits"], "metric")
        self.assertEqual(json.loads(rows[0]["data"])["noonWind"]["windKph"], 0)

    def test_retention_does_not_touch_cache_or_leave_orphans(self):
        from pathlib import Path
        sentinel = Path(self.temp.name) / "home.json"
        sentinel.write_text('{"old":"cache"}')
        self.record()
        history.prune(dict(self.config, history_retention_days=2), self.now + timedelta(days=3))
        with history.database(self.config) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM forecasts").fetchone()[0], 0)
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], history.SCHEMA_VERSION)
        self.assertEqual(sentinel.read_text(), '{"old":"cache"}')

    def test_shared_budget_is_atomic(self):
        with history.database(self.config):
            pass
        config = dict(self.config, max_calls_per_24h_openweather=3)
        with ThreadPoolExecutor(max_workers=6) as workers:
            attempts = list(workers.map(lambda _: history.reserve_attempt(config, "openweather", "home", self.now), range(12)))
        self.assertEqual(sum(x is not None for x in attempts), 3)

    def test_job_claim_prevents_repeated_requests_and_persists_failure(self):
        self.assertTrue(history.claim_job(self.config, "nws", 60, self.now))
        history.finish_job(self.config, "nws", "failed", "Timeout", self.now)
        self.assertFalse(history.claim_job(self.config, "nws", 60, self.now + timedelta(minutes=10)))
        self.assertTrue(history.claim_job(self.config, "nws", 60, self.now + timedelta(hours=1)))

    def test_newer_schema_is_not_overwritten(self):
        with history.database(self.config) as db:
            db.execute("PRAGMA user_version=99")
        with self.assertRaises(RuntimeError):
            with history.database(self.config):
                pass

    def test_legacy_attempts_survive_migration_and_still_count_toward_quota(self):
        db = sqlite3.connect(history.db_path(self.config))
        try:
            legacy_schema = history.SCHEMA.replace(", request_type TEXT, identity TEXT", "")
            db.executescript(legacy_schema)
            db.execute("PRAGMA user_version=1")
            db.execute("INSERT INTO attempts(provider,location,at,status) VALUES(?,?,?,'ok')",
                       ("openweather", "home", self.now.isoformat()))
            db.commit()
        finally:
            db.close()

        limited = dict(self.config, max_calls_per_24h_openweather=1)
        self.assertIsNone(history.reserve_attempt(limited, "openweather", "home", self.now))
        with history.database(self.config) as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], history.SCHEMA_VERSION)
            row = db.execute("SELECT status, request_type, identity FROM attempts").fetchone()
            self.assertEqual(tuple(row), ("ok", None, None))

    def test_known_timezone_is_scoped_to_coordinates(self):
        self.record()
        self.assertEqual(history.known_timezone(self.config, self.location), "America/Chicago")
        self.assertIsNone(history.known_timezone(self.config, dict(self.location, lon=-92)))
