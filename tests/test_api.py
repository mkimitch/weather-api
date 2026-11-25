"""Offline endpoint/cache integration using captured multi-day provider responses."""
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import cache
import weather

with patch.dict(os.environ, {"WEATHER_API_CONFIG": str(Path(__file__).parents[1] / "config.yaml.example")}):
    import main


class WeatherEndpointTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.old_cache = cache.CACHE_DIR, cache.CACHE_EXPIRY_MINUTES
        self.addCleanup(self.restore_cache)
        cache.init_cache({"cache_dir": self.directory.name})
        self.fixture = json.loads((Path(__file__).parent / "fixtures/forecast-2026-09-19.json").read_text())
        self.config = {"locations": [{"name": "home", "type": "latlon", "lat": 44.62, "lon": -93.14, "timezone": "America/Chicago"}],
                       "openweather_api_key": "test", "tomorrow_api_key": "test", "cache_dir": self.directory.name,
                       "daily_provider_priority": ["tomorrow", "openweather"]}
        config_patch = patch.object(main, "config", self.config)
        config_patch.start()
        self.addCleanup(config_patch.stop)
        clock_patch = patch.object(weather, "utcnow", return_value=datetime(2026, 9, 19, 12, tzinfo=timezone.utc))
        clock_patch.start()
        self.addCleanup(clock_patch.stop)

    def restore_cache(self):
        cache.CACHE_DIR, cache.CACHE_EXPIRY_MINUTES = self.old_cache

    def refresh(self):
        with patch.object(weather, "fetch_openweather", return_value=self.fixture["openweather"]), \
             patch.object(weather, "fetch_tomorrow", return_value=self.fixture["tomorrow"]) as tmr:
            response = json.loads(main.get_weather("home", refresh=1).body)
        self.assertEqual(tmr.call_args.kwargs["timezone_name"], "America/Chicago")
        return response

    def test_captured_forecast_through_normalization_merge_cache_and_endpoint(self):
        response = self.refresh()
        cache_data = cache.read_cached_payload("home")["data"]
        self.assertEqual(response["daily"], cache_data["daily"])
        self.assertEqual([d["date"] for d in response["daily"]], [f"2026-09-{day}" for day in range(19, 27)])
        self.assertEqual(sum("tomorrow" in d["_providers"] for d in response["daily"]), 6)
        self.assertEqual(len(response["daily"][1:8]), 7)
        for day in response["daily"]:
            source = "tomorrow" if day["date"] < "2026-09-24" else "openweather"
            upstream = day["_providers"][source]
            self.assertEqual(day["tempC"]["min"], upstream["tempC"]["min"])
            self.assertEqual(day["tempC"]["max"], upstream["tempC"]["max"])
            self.assertLessEqual(day["tempC"]["min"], day["tempC"]["max"])
            self.assertEqual(day["pop"], upstream["pop"])
            self.assertEqual(day["_sources"]["tempC"]["max"], source)
        tomorrow = response["daily"][0]["_providers"]["tomorrow"]
        self.assertEqual(tomorrow["pop"], 0.095)
        self.assertEqual(tomorrow["extra"]["precipitationProbability"]["precipitationProbabilityMax"], 0.74)
        # Production WeatherCard uses Math.round(C * 9/5 + 32), round(POP * 100).
        day = response["daily"][1]
        self.assertEqual(round(day["tempC"]["max"] * 9 / 5 + 32), 66)
        self.assertEqual(round(day["tempC"]["min"] * 9 / 5 + 32), 54)
        self.assertEqual(round(day["pop"] * 100), 0)
        self.assertTrue(response["extra"]["providerFetchedAt"]["openweather"])
        self.assertFalse(response["_cache"]["stale"])
        with patch.object(main, "get_weather_for_location") as fetch:
            cached = json.loads(main.get_weather("home", refresh=0).body)
        fetch.assert_not_called()
        self.assertEqual(response["updatedAt"], cached["updatedAt"])

    def test_failed_refresh_returns_old_data_with_honest_staleness(self):
        before = self.refresh()
        path = Path(cache.get_cache_path("home"))
        old = json.loads(path.read_text())
        old["timestamp"] = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        path.write_text(json.dumps(old))
        with patch.object(main, "get_weather_for_location", side_effect=RuntimeError("upstream failed")):
            response = json.loads(main.get_weather("home", refresh=1).body)
        self.assertEqual(response["updatedAt"], before["updatedAt"])
        self.assertEqual(response["generatedAt"], before["generatedAt"])
        self.assertEqual(response["extra"]["providerFetchedAt"], before["extra"]["providerFetchedAt"])
        self.assertTrue(response["_cache"]["stale"])
        self.assertTrue(response["_cache"]["refreshFailed"])
        self.assertEqual(json.loads(path.read_text()), old)

    def test_tomorrow_only_failure_fallback_and_empty_payload(self):
        with patch.object(weather, "fetch_openweather", side_effect=RuntimeError("unavailable")), \
             patch.object(weather, "fetch_tomorrow", return_value=self.fixture["tomorrow"]):
            response = json.loads(main.get_weather("home", refresh=1).body)
        self.assertEqual(len(response["daily"]), 5)
        self.assertEqual(response["daily"][0]["date"], "2026-09-19")
        self.assertEqual(response["daily"][0]["_sources"]["pop"], "tomorrow")
        self.assertIsNone(response["extra"]["providerFetchedAt"]["openweather"])
        self.assertEqual(response["extra"]["providerStatus"]["openweather"], "failed")
        with patch.object(weather, "fetch_openweather", return_value={}), \
             patch.object(weather, "fetch_tomorrow", return_value={}):
            stale = json.loads(main.get_weather("home", refresh=1).body)
        self.assertTrue(stale["_cache"]["refreshFailed"])
        self.assertEqual(stale["updatedAt"], response["updatedAt"])

    def test_accuracy_endpoint_and_validation_do_not_fetch_providers(self):
        from fastapi import HTTPException
        with patch.object(weather, "fetch_openweather") as owm, patch.object(weather, "fetch_tomorrow") as tmr:
            for days in (30, 90, 365):
                response = json.loads(main.get_accuracy("home", days=days).body)
                self.assertEqual(response["status"], "insufficient_data")
                self.assertFalse(response["automaticSwitching"])
            for location, days, status in (("missing", 30, 404), ("home", 7, 422)):
                with self.assertRaises(HTTPException) as error:
                    main.get_accuracy(location, days=days)
                self.assertEqual(error.exception.status_code, status)
        owm.assert_not_called()
        tmr.assert_not_called()
        self.assertIn("/accuracy/{location_name}", main.app.openapi()["paths"])

    def test_archive_keeps_both_providers_including_unselected_endpoint_day(self):
        import history
        self.refresh()
        with history.database(self.config) as db:
            counts = dict(db.execute("SELECT provider,COUNT(*) FROM responses GROUP BY provider").fetchall())
            days = db.execute("SELECT COUNT(*) FROM forecasts").fetchone()[0]
        self.assertEqual(counts, {"openweather": 1, "tomorrow": 1})
        self.assertEqual(days, 14)

    def test_archive_failure_is_visible_and_does_not_discard_valid_weather(self):
        with patch.object(weather, "record_forecast", side_effect=OSError("test disk error")):
            result = self.refresh()
        self.assertEqual(result["extra"]["historyStatus"], {"tomorrow": "failed", "openweather": "failed"})
        self.assertEqual(result["extra"]["providerStatus"], {"tomorrow": "ok", "openweather": "ok"})

    def test_attempt_status_write_failure_preserves_weather_and_cache(self):
        with patch.object(weather, "finish_attempt", side_effect=sqlite3.OperationalError) as finish, \
             patch.object(weather, "record_forecast") as archive:
            result = self.refresh()
        self.assertEqual(result["extra"]["providerStatus"], {"tomorrow": "ok", "openweather": "ok"})
        self.assertEqual(cache.read_cached_payload("home")["data"]["daily"], result["daily"])
        self.assertEqual(finish.call_count, 2)
        self.assertEqual(archive.call_count, 2)

    def test_quota_reservation_failure_prevents_provider_requests(self):
        with patch.object(weather, "reserve_attempt", side_effect=sqlite3.OperationalError), \
             patch.object(weather, "fetch_openweather") as owm, patch.object(weather, "fetch_tomorrow") as tmr:
            with self.assertRaises(main.HTTPException) as error:
                main.get_weather("home", refresh=1)
        self.assertEqual(error.exception.status_code, 502)
        owm.assert_not_called()
        tmr.assert_not_called()

    def test_database_outage_after_first_fetch_keeps_weather_and_blocks_next_request(self):
        from history import reserve_attempt

        with patch.object(weather, "reserve_attempt", side_effect=[
            reserve_attempt(self.config, "openweather", "home"), sqlite3.OperationalError()
        ]), patch.object(weather, "finish_attempt", side_effect=sqlite3.OperationalError), \
             patch.object(weather, "fetch_openweather", return_value=self.fixture["openweather"]), \
             patch.object(weather, "fetch_tomorrow") as tmr:
            result = json.loads(main.get_weather("home", refresh=1).body)
        tmr.assert_not_called()
        self.assertEqual(result["extra"]["providerStatus"], {"openweather": "ok", "tomorrow": "quota_unavailable"})
        self.assertEqual(cache.read_cached_payload("home")["data"]["daily"], result["daily"])
