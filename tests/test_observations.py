import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import history
import observations
from timeutils import day_bounds


def feature(at, temp=10, wind=12, precip=None):
    def q(value, unit):
        return {"value": value, "unitCode": "wmoUnit:" + unit, "qualityControl": "V"}
    return {"geometry": {"type": "Point", "coordinates": [-93.23, 44.63]}, "properties": {
        "station": "https://api.weather.gov/stations/KLVN", "timestamp": at.isoformat(),
        "temperature": q(temp, "degC"), "windSpeed": q(wind, "km_h-1"), "precipitationLastHour": q(precip, "mm")}}


class ObservationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config = {"cache_dir": self.temp.name, "locations": [{"name": "home", "observation_station": "KLVN"}], "nws_user_agent": "weather-tests (test@example.invalid)"}
        self.now = datetime(2026, 11, 3, tzinfo=timezone.utc)

    def test_units_quality_and_zero(self):
        self.assertEqual(observations.quantity({"value": 32, "unitCode": "wmoUnit:degF", "qualityControl": "V"}, "temperature"), 0)
        self.assertEqual(observations.quantity({"value": .001, "unitCode": "wmoUnit:m", "qualityControl": "C"}, "precip"), 1)
        self.assertEqual(observations.quantity({"value": 5, "unitCode": "wmoUnit:m_s-1", "qualityControl": "S"}, "wind"), 18)
        for value in (None, float("nan"), -1, True):
            self.assertIsNone(observations.quantity({"value": value, "unitCode": "wmoUnit:mm", "qualityControl": "V"}, "precip"))
        self.assertIsNone(observations.quantity({"value": 5, "unitCode": "wmoUnit:unknown", "qualityControl": "V"}, "wind"))
        self.assertIsNone(observations.quantity({"value": 5, "unitCode": "wmoUnit:degC", "qualityControl": "Z"}, "temperature"))

    def test_ingestion_idempotent_corrections_and_wrong_station(self):
        at = self.now - timedelta(hours=1)
        observations.ingest_observations(self.config, "KLVN", [feature(at)], self.now)
        observations.ingest_observations(self.config, "KLVN", [feature(at, temp=11)], self.now + timedelta(minutes=1))
        observations.ingest_observations(self.config, "KLVN", [feature(at, temp=0)], self.now)
        self.assertEqual(observations.ingest_observations(self.config, "KMSP", [feature(at)], self.now), (0, 1))
        with history.database(self.config) as db:
            rows = db.execute("SELECT data FROM observations").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(json.loads(rows[0][0])["tempC"], 11)
        self.assertIsNone(json.loads(rows[0][0])["precipLastHourMm"])

    def test_dst_coverage_missing_readings_and_precipitation_tiling(self):
        for day, hours in (("2026-03-08", 23), ("2026-11-01", 25)):
            start, end = day_bounds(day, "America/Chicago")
            now = end + timedelta(hours=8)
            rows = [observations.normalize_observation(feature(start + timedelta(hours=i), temp=i, precip=.5), "KLVN", now)
                    for i in range(hours + 1)]
            daily = observations.daily_observation(rows, day, "America/Chicago", now)
            self.assertEqual(daily["coverage"]["expectedHours"], hours)
            self.assertEqual(daily["tempMinC"], 0)
            self.assertEqual(daily["tempMaxC"], hours - 1)
            self.assertEqual(daily["precipMm"], hours * .5)
            rows[5]["precipLastHourMm"] = None
            self.assertIsNone(observations.daily_observation(rows, day, "America/Chicago", now)["precipMm"])
            self.assertIsNone(observations.daily_observation(rows[:5] + rows[10:], day, "America/Chicago", now)["tempMaxC"])
            self.assertIsNone(observations.daily_observation(rows, day, "America/Chicago", end)["tempMaxC"])

    def test_overlapping_precipitation_not_summed_or_missing_treated_as_dry(self):
        start, end = day_bounds("2026-11-01", "America/Chicago")
        rows = [observations.normalize_observation(feature(start + timedelta(minutes=20*i), precip=1), "KLVN", self.now) for i in range(1, 76)]
        self.assertEqual(observations.daily_observation(rows, "2026-11-01", "America/Chicago", self.now)["precipMm"], 25)
        for row in rows:
            row["precipLastHourMm"] = None
        self.assertIsNone(observations.daily_observation(rows, "2026-11-01", "America/Chicago", self.now)["precipMm"])

    def test_collector_pagination_dedup_and_schedule(self):
        responses = [Mock(), Mock()]
        responses[0].json.return_value = {"features": [feature(self.now - timedelta(hours=1))], "pagination": {"next": "https://api.weather.gov/stations/KLVN/observations?cursor=2"}}
        responses[1].json.return_value = {"features": [feature(self.now - timedelta(hours=2))]}
        with patch.object(observations.requests, "get", side_effect=responses) as get:
            observations.collect_observations(self.config, self.now)
            observations.collect_observations(self.config, self.now + timedelta(minutes=5))
        self.assertEqual(get.call_count, 2)
        self.assertIn("User-Agent", get.call_args.kwargs["headers"])
        with history.database(self.config) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM observations").fetchone()[0], 2)
            self.assertEqual(db.execute("SELECT status FROM jobs").fetchone()[0], "ok")

    def test_outage_and_unsafe_pagination_are_recorded_without_retry_storm(self):
        for response in (RuntimeError("secret must not leak"), Mock()):
            with tempfile.TemporaryDirectory() as directory:
                config = dict(self.config, cache_dir=directory)
                if isinstance(response, Mock):
                    response.json.return_value = {"features": [], "pagination": {"next": "https://example.com/private"}}
                with patch.object(observations.requests, "get", side_effect=response if isinstance(response, Exception) else None, return_value=response) as get:
                    observations.collect_observations(config, self.now)
                    observations.collect_observations(config, self.now)
                    self.assertEqual(get.call_count, 1)
                with history.database(config) as db:
                    row = db.execute("SELECT * FROM jobs").fetchone()
                self.assertEqual(row["status"], "failed")
                self.assertNotIn("secret", row["detail"])
