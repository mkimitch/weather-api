import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import history
import scheduler
import weather


class SchedulerBudgetTest(unittest.TestCase):
    def test_scheduler_delegates_to_shared_fetch_budget(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(scheduler, "get_weather_for_location") as fetch:
            config = {"cache_dir": directory, "locations": []}
            runner = scheduler.WeatherScheduler(config)
            runner.refresh_location({"name": "home"})
            self.assertIs(fetch.call_args.args[1], config)

    def test_durable_budgets_count_failures_and_survive_new_connections(self):
        with tempfile.TemporaryDirectory() as directory:
            config = {"cache_dir": directory, "max_calls_per_24h_tomorrow": 1}
            now = datetime(2026, 1, 1, tzinfo=timezone.utc)
            attempt = history.reserve_attempt(config, "tomorrow", "home", now)
            history.finish_attempt(config, attempt, "failed")
            self.assertIsNone(history.reserve_attempt(dict(config), "tomorrow", "home", now + timedelta(hours=1)))
            self.assertIsNotNone(history.reserve_attempt(config, "openweather", "home", now))
            self.assertIsNotNone(history.reserve_attempt(config, "tomorrow", "home", now + timedelta(days=1)))

    def test_exhausted_provider_falls_back_in_direct_fetch(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(weather, "fetch_tomorrow") as tmr, \
             patch.object(weather, "fetch_openweather", return_value={"current": {"dt": 1, "temp": 10}}), \
             patch.object(weather, "save_weather", side_effect=lambda name, data: data):
            config = {"cache_dir": directory, "tomorrow_api_key": "test", "openweather_api_key": "test", "max_calls_per_24h_tomorrow": 0}
            result = weather.get_weather_for_location({"name": "home", "type": "latlon", "lat": 44, "lon": -93}, config)
            tmr.assert_not_called()
            self.assertEqual(result["extra"]["providerStatus"]["tomorrow"], "rate_limited")

    def test_hour_and_second_limits(self):
        with tempfile.TemporaryDirectory() as directory:
            now = datetime(2026, 1, 1, tzinfo=timezone.utc)
            config = {"cache_dir": directory, "max_calls_per_hour_tomorrow": 2, "max_calls_per_second_tomorrow": 1}
            self.assertIsNotNone(history.reserve_attempt(config, "tomorrow", "home", now))
            self.assertIsNone(history.reserve_attempt(config, "tomorrow", "other", now))
            self.assertIsNotNone(history.reserve_attempt(config, "tomorrow", "home", now + timedelta(seconds=2)))
            self.assertIsNone(history.reserve_attempt(config, "tomorrow", "home", now + timedelta(minutes=20)))
