import tempfile
import unittest
from unittest.mock import patch

import cache
import scheduler


class SchedulerFallbackTest(unittest.TestCase):
    def test_exhausted_provider_is_disabled_in_request_and_other_provider_continues(self):
        for exhausted in ("tomorrow", "openweather"):
            with self.subTest(exhausted=exhausted), tempfile.TemporaryDirectory() as directory, \
                 patch.object(cache, "CACHE_DIR", directory), \
                 patch.object(scheduler, "get_weather_for_location", return_value={}) as fetch:
                config = {"cache_dir": directory, "locations": [], "openweather_api_key": "test", "tomorrow_api_key": "test",
                          "max_calls_per_24h_" + exhausted: 0}
                runner = scheduler.WeatherScheduler(config)
                runner.refresh_location({"name": "home"})
                sent = fetch.call_args.args[1]
                other = "openweather" if exhausted == "tomorrow" else "tomorrow"
                self.assertFalse(sent["enable_" + exhausted])
                self.assertTrue(sent["enable_" + other])
                self.assertEqual(len(runner.api_call_timestamps[other]), 1)
                self.assertNotIn("enable_" + exhausted, config)

    def test_failed_attempts_count_and_tomorrow_only_works(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(cache, "CACHE_DIR", directory), \
             patch.object(scheduler, "get_weather_for_location", side_effect=RuntimeError("unavailable")):
            runner = scheduler.WeatherScheduler({"cache_dir": directory, "locations": [], "tomorrow_api_key": "test"})
            with self.assertRaises(RuntimeError):
                runner.refresh_location({"name": "home"})
            self.assertEqual(len(runner.api_call_timestamps["tomorrow"]), 1)
            self.assertEqual(len(runner.api_call_timestamps["openweather"]), 0)
