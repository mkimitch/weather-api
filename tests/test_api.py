"""Offline endpoint/cache integration using captured multi-day provider responses."""
import json
import os
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
        self.config = {"locations": [{"name": "home", "type": "latlon", "lat": 44.62, "lon": -93.14}],
                       "openweather_api_key": "test", "tomorrow_api_key": "test"}
        config_patch = patch.object(main, "config", self.config)
        config_patch.start()
        self.addCleanup(config_patch.stop)

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
        for day, upstream in zip(response["daily"], self.fixture["openweather"]["daily"]):
            self.assertEqual(day["tempC"]["min"], upstream["temp"]["min"])
            self.assertEqual(day["tempC"]["max"], upstream["temp"]["max"])
            self.assertLessEqual(day["tempC"]["min"], day["tempC"]["max"])
            self.assertEqual(day["pop"], upstream["pop"])
            self.assertTrue(0 <= day["pop"] <= 1)
            self.assertEqual(day["_sources"]["tempC"]["max"], "openweather")
        tomorrow = response["daily"][0]["_providers"]["tomorrow"]
        self.assertEqual(tomorrow["pop"], 0.095)
        self.assertEqual(tomorrow["extra"]["precipitationProbability"]["precipitationProbabilityMax"], 0.74)
        # Production WeatherCard uses Math.round(C * 9/5 + 32), round(POP * 100).
        day = response["daily"][1]
        self.assertEqual(round(day["tempC"]["max"] * 9 / 5 + 32), 66)
        self.assertEqual(round(day["tempC"]["min"] * 9 / 5 + 32), 53)
        self.assertEqual(round(day["pop"] * 100), 23)
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
        self.assertEqual(len(response["daily"]), 6)
        self.assertEqual(response["daily"][0]["date"], "2026-09-19")
        self.assertEqual(response["daily"][0]["_sources"]["pop"], "tomorrow")
        self.assertIsNone(response["extra"]["providerFetchedAt"]["openweather"])
        self.assertEqual(response["extra"]["providerStatus"]["openweather"], "failed")
        with patch.object(weather, "fetch_openweather", return_value={}), \
             patch.object(weather, "fetch_tomorrow", return_value={}):
            stale = json.loads(main.get_weather("home", refresh=1).body)
        self.assertTrue(stale["_cache"]["refreshFailed"])
        self.assertEqual(stale["updatedAt"], response["updatedAt"])
