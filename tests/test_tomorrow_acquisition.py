"""Offline acquisition through the fetch, scheduler, quota and archive paths."""

import fcntl
import json
import os
import tempfile
import unittest
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import requests

from cache import init_cache
from history import database, health, record_forecast
from scheduler import WeatherScheduler
from timeutils import day_bounds
from tomorrow_acquisition import inspect_acquisition, read_timelines
from weather import fetch_tomorrow, get_weather_for_location, normalize_tomorrow

ZONE = "America/Chicago"
NOW = datetime(2026, 10, 31, 17, 15, tzinfo=timezone.utc)
RICH = {"weatherCodeFullDay": 2101, "weatherCodeDay": 62060, "weatherCodeNight": 71031}
BASE_FIELDS = [
    "temperature", "temperatureApparent", "humidity", "dewPoint", "pressureSurfaceLevel",
    "windSpeed", "windGust", "windDirection", "visibility", "uvIndex", "cloudCover",
    "precipitationProbability", "precipitationProbabilityAvg", "precipitationProbabilityMax",
    "rainIntensity", "snowIntensity", "sleetIntensity", "weatherCode", "temperatureMin",
    "temperatureMax",
]


def forecast(expanded=False, now=NOW):
    hours = []
    first = now.replace(minute=0)
    for i in range(72):
        hours.append({"startTime": (first + timedelta(hours=i)).isoformat(),
                      "values": {"weatherCode": 1001, "temperature": 9,
                                 "precipitationProbability": 0, "humidity": 70}})
    days = []
    local_date = now.astimezone(ZoneInfo(ZONE)).date()
    for i in range(4):
        start, _ = day_bounds((local_date + timedelta(days=i)).isoformat(), ZONE)
        values = {"temperatureMin": 4, "temperatureMax": 10, "weatherCode": 1001,
                  "precipitationProbabilityAvg": 0, "precipitationProbabilityMax": 40}
        if expanded and i in (1, 2):
            values.update(RICH)
        days.append({"startTime": start.isoformat(), "values": values})
    return {"data": {"timelines": [
        {"timestep": "1h", "intervals": hours, "endTime": hours[-1]["startTime"]},
        {"timestep": "1d", "intervals": days, "endTime": days[-1]["startTime"]},
    ]}}


def response(status, raw):
    result = requests.Response()
    result.status_code = status
    result._content = json.dumps(raw).encode()
    return result


class TomorrowAcquisitionTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "rich-conditions.control"
        self.location = {"name": "forecast-test", "type": "latlon", "lat": 44, "lon": -93, "timezone": ZONE,
                         "tomorrow_rich_conditions_control_file": str(self.path)}
        self.config = {"cache_dir": directory.name, "locations": [self.location], "tomorrow_api_key": "fake",
                       "tomorrow_forecast_days": 3, "enable_openweather": False, "refresh_interval_minutes": 5}
        init_cache(self.config)
        clock = patch("weather.utcnow", return_value=NOW)
        clock.start()
        self.addCleanup(clock.stop)

    def arm(self, mode, token="a" * 32):
        staged = self.path.with_suffix(".pending")
        staged.write_text(f"{mode} {token}\n")
        staged.chmod(0o600)
        staged.replace(self.path)

    def seed_baseline(self):
        raw = forecast()
        normalized = normalize_tomorrow(raw, timezone_name=ZONE, fetched_at=(NOW - timedelta(minutes=5)).isoformat())
        record_forecast(self.config, self.location, "tomorrow", raw, normalized)

    def attempt_statuses(self):
        with database(self.config) as db:
            return [tuple(row) for row in db.execute(
                "SELECT request_type,status FROM attempts WHERE provider='tomorrow' ORDER BY id")]

    def test_only_the_three_fields_are_appended_and_redirects_are_disabled(self):
        with patch("weather.requests.post", side_effect=[response(200, forecast()), response(200, forecast(True))]) as post:
            fetch_tomorrow(44, -93, "fake", timezone_name=ZONE, forecast_days=3)
            fetch_tomorrow(44, -93, "fake", timezone_name=ZONE, forecast_days=3, rich_conditions=True)
        baseline, expanded = [c.kwargs for c in post.call_args_list]
        self.assertEqual(baseline["json"], {"location": [44, -93], "fields": BASE_FIELDS, "units": "metric",
                                           "timesteps": ["1h", "1d"], "startTime": "now", "endTime": "nowPlus3d",
                                           "timezone": ZONE, "dailyStartHour": 0})
        self.assertEqual(expanded["json"], {**baseline["json"], "fields": [*BASE_FIELDS, *RICH]})
        self.assertEqual([c.args[0] for c in post.call_args_list], ["https://api.tomorrow.io/v4/timelines"] * 2)
        self.assertIs(baseline["allow_redirects"], False)
        self.assertIs(expanded["allow_redirects"], False)
        self.assertEqual(baseline["headers"], {"accept": "application/json", "content-type": "application/json",
                                               "apikey": "fake"})
        self.assertEqual(baseline["timeout"], 12)
        self.assertEqual(baseline["headers"], expanded["headers"])
        self.assertEqual(baseline["timeout"], expanded["timeout"])

    def test_enabled_scheduler_preserves_baseline_and_solar_provenance_across_dst(self):
        self.seed_baseline()
        self.arm("publish")
        raw = forecast(True)
        raw["data"]["timelines"][0]["intervals"][0]["values"].update(RICH)
        raw["warnings"] = [{"code": 246099, "type": "Timestep Bounded Field",
                            "meta": {"field": "weatherCodeDay", "timestep": "1h"},
                            "message": "do not log this provider string"}]
        def post_after_reservation(*args, **kwargs):
            self.assertEqual(self.attempt_statuses(), [("forecast", "pending")])
            return response(200, raw)

        with patch("weather.requests.post", side_effect=post_after_reservation) as post, \
             self.assertLogs("tomorrow_acquisition", level="INFO") as diagnostic:
            result = WeatherScheduler(self.config).refresh_location(self.location)
        self.assertEqual(len(post.call_args_list), 1)
        self.assertEqual(self.attempt_statuses(), [("forecast", "ok")])
        self.assertEqual(result["conditionSchemaVersion"], 1)
        self.assertEqual(len(result["hourly"]), 72)
        self.assertEqual(result["current"]["condition"]["code"], 1001)
        self.assertEqual(result["current"]["pop"], 0)
        self.assertEqual(result["hourly"][0]["conditions"]["hourly"]["source"]["native"],
                         [{"field": "weatherCode", "code": 1001}])
        day = next(row for row in result["daily"] if row["date"] == "2026-11-01")
        self.assertEqual(day["pop"], 0)
        self.assertEqual(day["popBasis"], "precipitationProbabilityAvg")
        self.assertEqual(day["popPeriod"]["start"], "2026-11-01T05:00:00+00:00")
        self.assertEqual(day["popPeriod"]["end"], "2026-11-02T06:00:00+00:00")
        night = day["conditions"]["nighttime"]
        self.assertEqual(night["period"], {"basis": "sunset_to_sunrise", "start": None, "end": None,
                                           "localDate": "2026-11-01"})
        self.assertEqual(night["source"]["native"], [{"field": "weatherCodeNight", "code": 71031}])
        self.assertIn("246099", str(diagnostic.output))
        self.assertNotIn("do not log this provider string", str(diagnostic.output))

    def test_optional_data_failure_keeps_generic_weather_and_disarms_this_activation(self):
        self.seed_baseline()
        self.arm("publish")
        raw = forecast(True)
        raw["data"]["timelines"][1]["intervals"][1]["values"]["weatherCodeNight"] = "71031"
        raw["warnings"] = [{"code": 246001, "type": "Time Bounded Field",
                            "meta": {"field": "weatherCodeNight", "from": "0h", "to": "48h", "secret": "hidden"}}]
        with patch("weather.requests.post", side_effect=[response(200, raw), response(200, forecast())]) as post:
            first = WeatherScheduler(self.config).refresh_location(self.location)
            second = WeatherScheduler(self.config).refresh_location(self.location)
        self.assertIn("weatherCodeNight", post.call_args_list[0].kwargs["json"]["fields"])
        self.assertEqual(post.call_args_list[1].kwargs["json"]["fields"], BASE_FIELDS)
        self.assertEqual(self.attempt_statuses(), [("forecast", "ok"), ("forecast", "ok")])
        for result in (first, second):
            self.assertEqual(result["extra"]["providerStatus"]["tomorrow"], "ok")
            self.assertTrue(all(row["conditions"]["daytime"] is None for row in result["daily"]))
            self.assertTrue(all(row["condition"]["code"] == 1001 for row in result["daily"]))

    def test_rejection_or_missing_baseline_coverage_uses_fallback_without_retry(self):
        self.seed_baseline()
        self.config.update(enable_openweather=True, openweather_api_key="fake")
        damaged = forecast(True)
        damaged["data"]["timelines"][0]["intervals"].pop(10)
        self.arm("publish")
        with patch("weather.requests.post", side_effect=[response(200, damaged), response(200, forecast())]) as post, \
             patch("weather.fetch_openweather", return_value={"timezone": ZONE,
                     "current": {"dt": int(NOW.timestamp()), "temp": 12}}):
            first = WeatherScheduler(self.config).refresh_location(self.location)
            second = WeatherScheduler(self.config).refresh_location(self.location)
        self.assertEqual(len(post.call_args_list), 2)
        self.assertEqual(post.call_args_list[1].kwargs["json"]["fields"], BASE_FIELDS)
        self.assertEqual(self.attempt_statuses(), [("forecast", "failed"), ("forecast", "ok")])
        self.assertEqual(first["extra"]["providerStatus"], {"openweather": "ok", "tomorrow": "failed"})
        self.assertEqual(second["extra"]["providerStatus"]["tomorrow"], "ok")

    def test_http_rejection_consumes_one_attempt_and_only_next_slot_uses_generic_body(self):
        self.seed_baseline()
        self.arm("publish")
        with patch("weather.requests.post", side_effect=[response(403, {"message": "denied"}), response(200, forecast())]) as post:
            with self.assertRaisesRegex(RuntimeError, "All providers failed"):
                WeatherScheduler(self.config).refresh_location(self.location)
            self.assertEqual(post.call_count, 1)
            WeatherScheduler(self.config).refresh_location(self.location)
        self.assertEqual(self.attempt_statuses(), [("forecast", "failed"), ("forecast", "ok")])
        self.assertEqual(post.call_args_list[1].kwargs["json"]["fields"], BASE_FIELDS)

    def test_observe_is_one_shot_without_publication_or_enriched_archival(self):
        self.seed_baseline()
        self.arm("observe")
        with patch("weather.requests.post", side_effect=[response(200, forecast(True)), response(200, forecast())]) as post:
            first = WeatherScheduler(self.config).refresh_location(self.location)
            WeatherScheduler(self.config).refresh_location(self.location)
        self.assertEqual(self.attempt_statuses(), [("forecast", "ok"), ("forecast", "ok")])
        self.assertIn("weatherCodeDay", post.call_args_list[0].kwargs["json"]["fields"])
        self.assertEqual(post.call_args_list[1].kwargs["json"]["fields"], BASE_FIELDS)
        self.assertTrue(all(row["conditions"]["daytime"] is None for row in first["daily"]))
        with database(self.config) as db:
            payload = db.execute("SELECT payload FROM responses ORDER BY retrieved_at DESC LIMIT 1").fetchone()[0]
        self.assertNotIn("weatherCodeDay", zlib.decompress(payload).decode())

    def test_missing_control_and_http_refresh_use_generic_and_deletion_takes_effect(self):
        self.seed_baseline()
        self.arm("publish")
        with patch("weather.requests.post", side_effect=[response(200, forecast()), response(200, forecast())]) as post:
            get_weather_for_location(self.location, self.config)
            self.path.unlink()
            WeatherScheduler(self.config).refresh_location(self.location)
        self.assertEqual([c.kwargs["json"]["fields"] for c in post.call_args_list], [BASE_FIELDS, BASE_FIELDS])
        self.assertEqual(self.attempt_statuses(), [("forecast", "ok"), ("forecast", "ok")])

    def test_no_fresh_baseline_requests_generic(self):
        self.arm("publish")
        with patch("weather.requests.post", return_value=response(200, forecast())) as post:
            WeatherScheduler(self.config).refresh_location(self.location)
        self.assertEqual(post.call_args.kwargs["json"]["fields"], BASE_FIELDS)
        self.assertEqual(self.attempt_statuses(), [("forecast", "ok")])

    def test_exhausted_ledger_does_not_consume_unstarted_observation(self):
        self.seed_baseline()
        self.arm("observe")
        self.config["max_calls_per_24h_tomorrow"] = 0
        runner = WeatherScheduler(self.config)
        with patch("weather.requests.post", return_value=response(200, forecast(True))) as post:
            with self.assertRaisesRegex(RuntimeError, "All providers failed"):
                runner.refresh_location(self.location)
            post.assert_not_called()
            self.config["max_calls_per_24h_tomorrow"] = 500
            runner.refresh_location(self.location)
        self.assertEqual(post.call_args.kwargs["json"]["fields"], [*BASE_FIELDS, *RICH])
        self.assertEqual(self.attempt_statuses(), [("forecast", "ok")])

    def test_publish_stays_armed_until_control_is_removed(self):
        self.seed_baseline()
        self.arm("publish")
        with patch("weather.requests.post", side_effect=[response(200, forecast(True)), response(200, forecast(True)),
                                                        response(200, forecast())]) as post:
            for _ in range(2):
                result = WeatherScheduler(self.config).refresh_location(self.location)
                day = next(row for row in result["daily"] if row["date"] == "2026-11-01")
                self.assertEqual(day["_sources"]["conditions"]["daytime"], "tomorrow")
            self.path.unlink()
            result = WeatherScheduler(self.config).refresh_location(self.location)
        self.assertEqual([c.kwargs["json"]["fields"] for c in post.call_args_list],
                         [[*BASE_FIELDS, *RICH], [*BASE_FIELDS, *RICH], BASE_FIELDS])
        self.assertTrue(all(row["conditions"]["daytime"] is None for row in result["daily"]))

    def test_errors_and_redirect_are_recorded_once_without_a_retry(self):
        self.seed_baseline()
        for i, failure in enumerate((response(302, {}), response(429, {}), requests.Timeout())):
            with self.subTest(case=i):
                self.arm("observe", f"{i:032x}")
                with patch("weather.requests.post", side_effect=[failure]) as post:
                    with self.assertRaisesRegex(RuntimeError, "All providers failed"):
                        WeatherScheduler(self.config).refresh_location(self.location)
                self.assertEqual(post.call_count, 1)
                self.assertFalse(post.call_args.kwargs["allow_redirects"])
                self.assertEqual(self.attempt_statuses(), [("forecast", "failed")] * (i + 1))

    def test_inspection_bounds_warning_samples_and_counts_fields_independently(self):
        baseline, _ = read_timelines(forecast()["data"])
        raw = forecast(True)
        raw["warnings"] = [{"code": 123456, "type": "something secret", "meta": {"field": "secret-url"}}] * 40
        days = raw["data"]["timelines"][1]["intervals"]
        days[0]["values"]["weatherCodeNight"] = None
        days[1]["values"]["weatherCodeNight"] = 0
        days[2]["values"]["weatherCodeFullDay"] = 10000  # A documented code in the wrong field.
        result = inspect_acquisition(raw, baseline, NOW, NOW + timedelta(days=3), ZONE)
        self.assertEqual(result["baselineIssues"], {})
        self.assertFalse(result["publishable"])
        self.assertEqual(result["completeInteriorDays"], 2)
        self.assertEqual(result["available"], {"weatherCodeFullDay": False, "weatherCodeDay": True,
                                                "weatherCodeNight": False})
        fields = result["fields"]
        self.assertEqual(fields["weatherCodeNight"]["1d"]["null"], 1)
        self.assertEqual(fields["weatherCodeNight"]["completeInteriorDays"]["sentinel"], 1)
        self.assertEqual(fields["weatherCodeFullDay"]["completeInteriorDays"]["unknown"], 1)
        self.assertEqual(fields["weatherCodeDay"]["completeInteriorDays"]["codes"], {62060: 2})
        warnings = result["warnings"]
        self.assertEqual(warnings["count"], 40)
        self.assertEqual(len(warnings["items"]), 12)
        self.assertNotIn("secret", str(warnings))

    def test_gate_detects_regression_from_known_generic_codes_or_valid_pop(self):
        baseline, _ = read_timelines(forecast()["data"])
        raw = forecast(True)
        days = raw["data"]["timelines"][1]["intervals"]
        days[1]["values"]["weatherCode"] = 0
        days[1]["values"]["precipitationProbabilityAvg"] = -1
        result = inspect_acquisition(raw, baseline, NOW, NOW + timedelta(days=3), ZONE)
        self.assertEqual(result["baselineIssues"], {"1d:invalid:weatherCode": 1,
                                                   "1d:missing:precipitationProbabilityAvg": 1})
        self.assertFalse(result["publishable"])

    def test_new_hour_requires_usable_generic_data(self):
        baseline, _ = read_timelines(forecast()["data"])
        for values, expected in (
            ({}, {"1h:unusable_core_data": 1}),
            ({"temperature": "9", "weatherCode": 0, "precipitationProbability": True},
             {"1h:unusable_core_data": 1}),
            ({"precipitationProbability": 0}, {}),
            ({"weatherCode": 1001}, {}),
        ):
            with self.subTest(values=values):
                raw = forecast(True)
                hourly = raw["data"]["timelines"][0]
                last = datetime.fromisoformat(hourly["intervals"][-1]["startTime"])
                hourly["endTime"] = (last + timedelta(hours=1)).isoformat()
                hourly["intervals"].append({"startTime": hourly["endTime"], "values": values})
                result = inspect_acquisition(raw, baseline, NOW, NOW + timedelta(days=3), ZONE)
                self.assertEqual(result["baselineIssues"], expected)
                self.assertEqual(result["publishable"], not expected)

    def test_new_complete_day_requires_core_data_but_boundary_rows_do_not(self):
        generic = forecast()
        generic["data"]["timelines"][1]["intervals"].pop(2)
        baseline, _ = read_timelines(generic["data"])
        for values, expected in (
            ({}, {"1d:unusable_core_data": 1}),
            ({"temperatureMin": 10, "temperatureMax": 5, "weatherCode": "1001",
              "precipitationProbabilityAvg": 101}, {"1d:unusable_core_data": 1}),
            ({"temperatureMin": 4, "temperatureMax": 10}, {}),
        ):
            with self.subTest(values=values):
                raw = forecast(True)
                days = raw["data"]["timelines"][1]["intervals"]
                days[2]["values"] = {**RICH, **values}
                result = inspect_acquisition(raw, baseline, NOW, NOW + timedelta(days=3), ZONE)
                self.assertEqual(result["baselineIssues"], expected)
                self.assertEqual(result["publishable"], not expected)

        # A newly returned incomplete trailing boundary can be present without core data.
        generic["data"]["timelines"][1]["intervals"].pop()
        baseline, _ = read_timelines(generic["data"])
        raw = forecast(True)
        raw["data"]["timelines"][1]["intervals"][-1]["values"] = {}
        result = inspect_acquisition(raw, baseline, NOW, NOW + timedelta(days=3), ZONE)
        self.assertEqual(result["baselineIssues"], {})
        self.assertTrue(result["publishable"])

    def test_spring_dst_day_and_boundary_days_keep_their_distinct_coverage(self):
        spring = datetime(2026, 3, 7, 17, 15, tzinfo=timezone.utc)
        baseline, _ = read_timelines(forecast(now=spring)["data"])
        expanded = forecast(expanded=True, now=spring)
        result = inspect_acquisition(expanded, baseline, spring, spring + timedelta(days=3), ZONE)
        self.assertTrue(result["publishable"])
        self.assertEqual(result["completeInteriorDays"], 2)
        for field in RICH:
            self.assertEqual(result["fields"][field]["1d"]["missing"], 2)
            self.assertEqual(result["fields"][field]["completeInteriorDays"]["known"], 2)

    def test_replacement_during_post_prevents_rich_writes(self):
        self.seed_baseline()
        self.arm("publish")

        def post_then_replace(*args, **kwargs):
            self.assertEqual(kwargs["json"]["fields"], [*BASE_FIELDS, *RICH])
            self.arm("observe", "b" * 32)
            return response(200, forecast(True))

        with patch("weather.requests.post", side_effect=post_then_replace) as post:
            result = WeatherScheduler(self.config).refresh_location(self.location)
        self.assertEqual(post.call_count, 1)
        self.assertTrue(all(row["conditions"]["daytime"] is None for row in result["daily"]))
        with database(self.config) as db:
            latest = db.execute("SELECT payload FROM responses ORDER BY retrieved_at DESC LIMIT 1").fetchone()[0]
        self.assertNotIn("weatherCodeDay", zlib.decompress(latest).decode())

    def test_removal_during_post_prevents_rich_cache_and_rearming(self):
        self.seed_baseline()
        self.arm("publish")

        def post_then_remove(*args, **kwargs):
            self.path.unlink()
            return response(200, forecast(True))

        with patch("weather.requests.post", side_effect=post_then_remove) as post:
            result = WeatherScheduler(self.config).refresh_location(self.location)
            self.arm("publish")
            post.side_effect = None
            post.return_value = response(200, forecast())
            WeatherScheduler(self.config).refresh_location(self.location)
        self.assertTrue(all(row["conditions"]["daytime"] is None for row in result["daily"]))
        self.assertEqual([call.kwargs["json"]["fields"] for call in post.call_args_list],
                         [[*BASE_FIELDS, *RICH], BASE_FIELDS])

    def test_consumed_observe_token_stays_blocked_after_touch_or_replacement(self):
        self.seed_baseline()
        self.arm("observe")
        with patch("weather.requests.post", side_effect=[response(200, forecast(True)), response(200, forecast())]) as post:
            WeatherScheduler(self.config).refresh_location(self.location)
            self.arm("observe")  # Replacing the file with the old token is not a new activation.
            WeatherScheduler(self.config).refresh_location(self.location)
        self.assertEqual([call.kwargs["json"]["fields"] for call in post.call_args_list],
                         [[*BASE_FIELDS, *RICH], BASE_FIELDS])
        self.assertFalse(any(job["name"].startswith("tomorrow_rich:") for job in health(self.config)["jobs"]))

    def test_claim_survives_interruption_before_publication(self):
        self.seed_baseline()
        self.arm("publish")
        with patch("weather.requests.post", side_effect=[KeyboardInterrupt(), response(200, forecast())]) as post:
            with self.assertRaises(KeyboardInterrupt):
                WeatherScheduler(self.config).refresh_location(self.location)
            WeatherScheduler(dict(self.config)).refresh_location(self.location)
        self.assertEqual([call.kwargs["json"]["fields"] for call in post.call_args_list],
                         [[*BASE_FIELDS, *RICH], BASE_FIELDS])
        self.assertEqual(self.attempt_statuses(), [("forecast", "pending"), ("forecast", "ok")])

    def test_archive_failure_keeps_generic_cache_and_blocks_rearming(self):
        self.seed_baseline()
        self.arm("publish")
        with patch("weather.requests.post", side_effect=[response(200, forecast(True)), response(200, forecast())]) as post:
            with patch("weather.record_forecast", side_effect=OSError("disk unavailable")):
                result = WeatherScheduler(self.config).refresh_location(self.location)
            WeatherScheduler(self.config).refresh_location(self.location)
        self.assertTrue(all(row["conditions"]["daytime"] is None for row in result["daily"]))
        self.assertEqual(result["extra"]["providerStatus"]["tomorrow"], "ok")
        self.assertEqual(result["extra"]["historyStatus"]["tomorrow"], "failed")
        self.assertEqual([call.kwargs["json"]["fields"] for call in post.call_args_list],
                         [[*BASE_FIELDS, *RICH], BASE_FIELDS])

    def test_archive_commits_before_cache_and_cache_failure_blocks_rearming(self):
        self.seed_baseline()
        self.arm("publish")

        def fail_cache(*_):
            with database(self.config) as db:
                latest = db.execute("SELECT payload FROM responses ORDER BY retrieved_at DESC LIMIT 1").fetchone()[0]
            self.assertIn("weatherCodeDay", zlib.decompress(latest).decode())
            raise OSError("cache unavailable")

        with patch("weather.requests.post", side_effect=[response(200, forecast(True)), response(200, forecast())]) as post:
            with patch("weather.save_weather", side_effect=fail_cache):
                with self.assertRaises(OSError):
                    WeatherScheduler(self.config).refresh_location(self.location)
            WeatherScheduler(self.config).refresh_location(self.location)
        self.assertEqual([call.kwargs["json"]["fields"] for call in post.call_args_list],
                         [[*BASE_FIELDS, *RICH], BASE_FIELDS])

    def test_unsafe_markers_and_busy_operator_lock_keep_generic_acquisition(self):
        self.seed_baseline()
        self.config["max_calls_per_second_tomorrow"] = 100
        runner = WeatherScheduler(self.config)
        with patch("weather.requests.post", side_effect=[response(200, forecast()) for _ in range(5)]) as post:
            self.arm("publish")
            self.path.chmod(0o666)
            runner.refresh_location(self.location)
            actual = self.path.with_suffix(".outside")
            self.path.replace(actual)
            actual.chmod(0o600)
            self.path.symlink_to(actual)
            runner.refresh_location(self.location)
            self.location["tomorrow_rich_conditions_control_file"] = str(self.path) + "\0"
            runner.refresh_location(self.location)
            self.location["tomorrow_rich_conditions_control_file"] = str(self.path)
            self.path.unlink()
            self.arm("publish")
            directory = os.open(self.path.parent, os.O_RDONLY)
            try:
                fcntl.flock(directory, fcntl.LOCK_EX)
                runner.refresh_location(self.location)
            finally:
                os.close(directory)
            self.path.unlink()
            os.mkfifo(self.path)
            runner.refresh_location(self.location)
        self.assertEqual([call.kwargs["json"]["fields"] for call in post.call_args_list], [BASE_FIELDS] * 5)

    def test_publish_holds_operator_lock_during_archive_and_cache(self):
        from weather import save_weather

        self.seed_baseline()
        self.arm("publish")
        directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
        self.addCleanup(os.close, directory)

        def assert_locked(write):
            def checked(*args, **kwargs):
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(directory, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return write(*args, **kwargs)
            return checked

        with patch("weather.requests.post", return_value=response(200, forecast(True))) as post, \
             patch("weather.record_forecast", side_effect=assert_locked(record_forecast)), \
             patch("weather.save_weather", side_effect=assert_locked(save_weather)):
            published = WeatherScheduler(self.config).refresh_location(self.location)
        self.assertTrue(any(row["conditions"]["daytime"] for row in published["daily"]))
        self.assertEqual(post.call_count, 1)
        fcntl.flock(directory, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_http_cache_miss_joins_the_controlled_scheduled_fetch(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Event
        from cache import refresh_lock
        from test_api import main

        self.seed_baseline()
        self.arm("publish")
        entered_post, complete_post = Event(), Event()

        def wait_in_post(*args, **kwargs):
            entered_post.set()
            if not complete_post.wait(3):
                raise TimeoutError("test did not release provider response")
            return response(200, forecast(True))

        runner = WeatherScheduler(self.config)
        with patch.object(main, "config", self.config), \
             patch("weather.requests.post", side_effect=wait_in_post) as post, ThreadPoolExecutor() as pool:
            scheduled = pool.submit(runner.refresh_location, self.location)
            try:
                self.assertTrue(entered_post.wait(3))
                lock = refresh_lock(self.location["name"])
                acquired = lock.acquire(blocking=False)
                if acquired:
                    lock.release()
                self.assertFalse(acquired)
                http = pool.submit(main.get_weather, self.location["name"], refresh=0)
            finally:
                complete_post.set()
            scheduled_data = scheduled.result(timeout=3)
            cached = json.loads(http.result(timeout=3).body)
        self.assertEqual(post.call_count, 1)
        self.assertEqual(self.attempt_statuses(), [("forecast", "ok")])
        self.assertEqual(cached["daily"], scheduled_data["daily"])
        self.assertTrue(any(row["conditions"]["daytime"] for row in cached["daily"]))

    def test_missing_or_malformed_timelines_use_existing_fallback(self):
        self.seed_baseline()
        self.config.update(enable_openweather=True, openweather_api_key="fake")
        damaged = forecast(True)
        damaged["data"]["timelines"].pop()
        for i, raw in enumerate((damaged, {"data": {"timelines": [None]}})):
            self.arm("publish", f"{i:032x}")
            with patch("weather.requests.post", return_value=response(200, raw)) as post, \
                 patch("weather.fetch_openweather", return_value={"timezone": ZONE,
                         "current": {"dt": int(NOW.timestamp()), "temp": 12}}) as openweather:
                result = WeatherScheduler(self.config).refresh_location(self.location)
            self.assertEqual(post.call_count, 1)
            self.assertEqual(openweather.call_count, 1)
            self.assertEqual(result["extra"]["providerStatus"], {"openweather": "ok", "tomorrow": "failed"})
        self.assertEqual(self.attempt_statuses(), [("forecast", "failed"), ("forecast", "failed")])
