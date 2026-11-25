import unittest
import tempfile
import os
import sys
import types
import copy
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

try:
    import requests
except ImportError:
    sys.modules.setdefault("requests", types.ModuleType("requests"))

import weather
from weather import fetch_tomorrow, normalize_openweather, normalize_tomorrow


class ProbabilityNormalizationTest(unittest.TestCase):
    def test_tomorrow_request_includes_daily_probability_aggregates(self):
        captured = {}

        class FakeResponse:
            ok = True

            def raise_for_status(self):
                pass

            def json(self):
                return {"data": {"timelines": []}}

        def fake_post(url, json, headers, timeout):
            captured.update(json)
            return FakeResponse()

        old_post = getattr(weather.requests, "post", None)
        weather.requests.post = fake_post
        try:
            fetch_tomorrow(44.98, -93.26, "test-key")
        finally:
            if old_post is None:
                del weather.requests.post
            else:
                weather.requests.post = old_post

        self.assertIn("precipitationProbabilityAvg", captured["fields"])
        self.assertIn("precipitationProbabilityMax", captured["fields"])
        self.assertEqual(captured["endTime"], "nowPlus5d")
        self.assertEqual(captured["dailyStartHour"], 0)
        self.assertEqual(captured["timezone"], "auto")
        self.assertEqual(captured["units"], "metric")

    def test_openweather_pop_stays_unit_interval(self):
        raw = {
            "hourly": [{"dt": 1700000000, "pop": 0.75}],
            "daily": [{"dt": 1700000000, "temp": {}, "pop": 0.4}],
        }

        normalized = normalize_openweather(raw)

        self.assertEqual(normalized["hourly"][0]["pop"], 0.75)
        self.assertEqual(normalized["daily"][0]["pop"], 0.4)

    def test_tomorrow_daily_prefers_average_probability(self):
        raw = {
            "location": {"lat": 44.98, "lon": -93.26},
            "data": {
                "timelines": [
                    {
                        "timestep": "1d",
                        "intervals": [
                            {
                                "startTime": "2026-05-26T00:00:00Z",
                                "values": {
                                    "temperatureMin": 12,
                                    "temperatureMax": 22,
                                    "precipitationProbabilityAvg": 28.3,
                                    "precipitationProbabilityMax": 100,
                                },
                            }
                        ],
                    }
                ]
            },
        }

        normalized = normalize_tomorrow(raw)

        self.assertAlmostEqual(normalized["daily"][0]["pop"], 0.283)

    def test_tomorrow_daily_falls_back_to_max_probability(self):
        raw = {
            "data": {
                "timelines": [
                    {
                        "timestep": "1d",
                        "intervals": [
                            {
                                "startTime": "2026-05-26T00:00:00Z",
                                "values": {
                                    "temperature": 18,
                                    "precipitationProbabilityMax": 65,
                                },
                            }
                        ],
                    }
                ]
            },
        }

        normalized = normalize_tomorrow(raw)

        self.assertEqual(normalized["daily"][0]["pop"], 0.65)



def forecast(provider, count=8):
    return {"daily": [
        {"date": f"2026-09-{day:02}", "tempC": {"min": 20, "max": 39 if provider == "openweather" else 30},
         "pop": 1.0 if provider == "openweather" else 0.25,
         "popBasis": "daily" if provider == "openweather" else "precipitationProbabilityAvg",
         "condition": {"code": 500 if provider == "openweather" else 4001},
         "sunrise": "2026-09-02T11:30:00+00:00" if provider == "openweather" else None,
         "_src": provider}
        for day in range(2, 2 + count)
    ]}


class ForecastContractTest(unittest.TestCase):
    def test_daily_policy_is_consistent_across_full_and_short_tomorrow_horizons(self):
        for count in (2, 8):
            with self.subTest(tomorrow_days=count):
                merged = weather.merge_weather(forecast("openweather"), forecast("tomorrow", count))
                self.assertEqual(len(merged["daily"]), 8)
                self.assertTrue(all(day["_sources"]["tempC"]["max"] == "openweather" for day in merged["daily"]))
                merged = weather.merge_weather(forecast("openweather"), forecast("tomorrow", count),
                                               {"daily_provider_priority": ["tomorrow", "openweather"]})
                self.assertEqual(len(merged["daily"]), 8)
                for i, day in enumerate(merged["daily"]):
                    self.assertEqual(day["tempC"]["max"], 30 if i < count else 39)
                    self.assertEqual(day["pop"], 1)
                    self.assertEqual(day["_sources"]["tempC"]["max"], "tomorrow" if i < count else "openweather")
                self.assertEqual(len(merged["extra"]["dailyDisagreements"]), count * 2)

    def test_pop_policy_is_preference_not_maximum(self):
        a, b = forecast("openweather", 1), forecast("tomorrow", 1)
        a["daily"][0]["pop"], b["daily"][0]["pop"] = 0.25, 1
        day = weather.merge_weather(a, b)["daily"][0]
        self.assertEqual(day["pop"], 0.25)
        self.assertEqual(day["_sources"]["pop"], "openweather")

    def test_field_fallback_and_zero_values_and_provenance(self):
        a, b = forecast("openweather", 1), forecast("tomorrow", 1)
        b["daily"][0].update(tempC={"max": None, "min": 0, "day": 0}, pop=None,
                              windKph=0, condition={"code": None})
        a["daily"][0].update(windKph=5, uv=0)
        day = weather.merge_weather(a, b, {"daily_provider_priority": ["tomorrow", "openweather"]})["daily"][0]
        self.assertEqual(day["tempC"], {"max": 39, "min": 0, "day": 0})
        self.assertEqual(day["windKph"], 0)
        self.assertEqual(day["pop"], 1)
        self.assertEqual(day["popBasis"], "daily")
        self.assertEqual(day["condition"]["code"], 500)
        self.assertEqual(day["_sources"]["tempC"], {"max": "openweather", "min": "tomorrow", "day": "tomorrow"})
        self.assertEqual(day["_sources"]["sunrise"], "openweather")
        self.assertEqual(day["_sources"]["condition"], "openweather")
        self.assertEqual(day["_src"], "mixed")

    def test_single_provider_fallback(self):
        for a, b, source in (({}, forecast("tomorrow"), "tomorrow"),
                              (forecast("openweather"), {}, "openweather")):
            merged = weather.merge_weather(a, b)
            self.assertEqual(len(merged["daily"]), 8)
            self.assertEqual(merged["daily"][0]["_sources"]["tempC"]["max"], source)

    def test_hourly_and_current_null_fallback_preserves_zero(self):
        a = {"time": "2026-09-02T12:00:00+00:00", "tempC": 10, "pop": 0.5, "windKph": 5, "_src": "openweather"}
        b = {"time": a["time"], "tempC": None, "pop": 0, "windKph": 0, "_src": "tomorrow"}
        m = weather.merge_weather({"current": a, "hourly": [a]}, {"current": b, "hourly": [b]})
        for record in (m["current"], m["hourly"][0]):
            self.assertEqual(record["tempC"], 10)
            self.assertEqual(record["pop"], 0)
            self.assertEqual(record["windKph"], 0)
            self.assertEqual(record["_sources"]["tempC"], "openweather")

    def test_disagreement_thresholds_and_provider_values_are_preserved(self):
        a, b = forecast("openweather", 1), forecast("tomorrow", 1)
        a["daily"][0]["tempC"].update(min=14.5, max=35.5)
        b["daily"][0]["tempC"].update(min=20, max=30)
        a["daily"][0]["pop"] = 0.75
        original = copy.deepcopy((a, b))
        m = weather.merge_weather(a, b)
        self.assertEqual({x["field"] for x in m["extra"]["dailyDisagreements"]}, {"tempC.min", "tempC.max", "pop"})
        self.assertEqual(m["daily"][0]["_providers"]["tomorrow"]["tempC"]["max"], 30)
        self.assertEqual((a, b), original)

    def test_missing_or_nonfinite_preferred_values_fall_back(self):
        a, b = forecast("openweather", 1), forecast("tomorrow", 1)
        a["daily"][0]["tempC"]["max"] = float("nan")
        self.assertEqual(weather.merge_weather(a, b)["daily"][0]["tempC"]["max"], 30)
        a["daily"][0]["tempC"]["min"] = None
        b["daily"][0]["tempC"]["min"] = None
        day = weather.merge_weather(a, b)["daily"][0]
        self.assertIsNone(day["tempC"]["min"])
        self.assertIsNone(day["_sources"]["tempC"]["min"])

    def test_historical_103f_conversion_is_reproducible_not_historical_evidence(self):
        a = forecast("openweather")
        a["daily"][2]["tempC"]["max"] = (103 - 32) * 5 / 9
        day = weather.merge_weather(a, forecast("tomorrow", 2))["daily"][2]
        self.assertAlmostEqual(day["tempC"]["max"], 39.4444444444)
        self.assertEqual(round(day["tempC"]["max"] * 9 / 5 + 32), 103)


class ProviderNormalizationTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        env = patch.dict(os.environ, {"WEATHER_HISTORY_DB": temp.name + "/history.sqlite3"})
        env.start()
        self.addCleanup(env.stop)

    def test_multi_day_tomorrow_and_percentage_fields(self):
        intervals = [{"startTime": f"2026-09-{d:02}T00:00:00-05:00", "values": {
            "temperatureMin": 10, "temperatureMax": 20, "precipitationProbabilityAvg": 75,
            "precipitationProbabilityMax": 100, "rainIntensity": 2, "windSpeed": 5}}
            for d in range(2, 10)]
        raw = {"data": {"timelines": [{"timestep": "1d", "intervals": intervals},
             {"timestep": "1h", "intervals": [{"startTime": intervals[0]["startTime"], "values": {"precipitationProbability": 75}}]}]}}
        result = normalize_tomorrow(raw, timezone_name="America/Chicago")
        self.assertEqual(len(result["daily"]), 8)
        for day in result["daily"]:
            self.assertEqual(day["pop"], 0.75)
            self.assertEqual(day["popBasis"], "precipitationProbabilityAvg")
            self.assertEqual(day["windKph"], 18)
            self.assertIsNone(day["precipMm"])  # intensity is not daily accumulation
        self.assertEqual(result["hourly"][0]["pop"], 0.75)
        self.assertEqual(result["current"]["pop"], 0.75)

    def test_tomorrow_missing_extrema_are_not_replaced_with_generic_temperature(self):
        result = normalize_tomorrow({"data": {"timelines": [{"timestep": "1d", "intervals": [
            {"startTime": "2026-09-02T00:00:00-05:00", "values": {"temperature": 30, "temperatureMin": None}}]}]}})
        self.assertEqual(result["daily"][0]["tempC"], {"min": None, "max": None})

    def test_local_dates_and_utc_instant_semantics(self):
        for stamp, expected in (("2026-09-03T02:00:00+00:00", "2026-09-02"),
                                 ("2026-01-03T05:30:00+00:00", "2026-01-02"),
                                 ("2026-03-08T05:30:00+00:00", "2026-03-07")):
            dt = int(datetime.fromisoformat(stamp).timestamp())
            a = normalize_openweather({"timezone": "America/Chicago", "daily": [{"dt": dt, "sunrise": dt}]})
            b = normalize_tomorrow({"data": {"timelines": [{"timestep": "1d", "intervals": [{"startTime": stamp, "values": {}}]}]}}, timezone_name="America/Chicago")
            self.assertEqual(a["daily"][0]["date"], expected)
            self.assertEqual(b["daily"][0]["date"], expected)
            self.assertEqual(a["daily"][0]["sunrise"], stamp)
        # Tomorrow's auto timezone returns offsets: retain its date even east of UTC.
        b = normalize_tomorrow({"data": {"timelines": [{"timestep": "1d", "intervals": [{"startTime": "2026-09-03T00:00:00+09:00", "values": {}}]}]}})
        self.assertEqual(b["daily"][0]["date"], "2026-09-03")

    def test_openweather_timezone_offset_fallback_and_missing_dates(self):
        raw = {"timezone_offset": -18000, "daily": [{"dt": 1788400800}, {"temp": {"max": 22}}]}
        result = normalize_openweather(raw)
        expected = datetime.fromtimestamp(raw["daily"][0]["dt"], timezone(timedelta(hours=-5))).date().isoformat()
        self.assertEqual([d["date"] for d in result["daily"]], [expected])

    def test_canonical_metric_fields(self):
        raw = {"current": {"dt": 1, "temp": 20, "feels_like": 19, "dew_point": 10, "wind_speed": 5, "visibility": 10000},
               "daily": [{"dt": 1, "temp": {"min": 10, "max": 20}, "pop": 0.75}],
               "hourly": [{"dt": 1, "temp": 20, "wind_speed": 5}]}
        result = normalize_openweather(raw)
        self.assertEqual(result["current"]["tempC"], 20)
        self.assertEqual(result["current"]["windKph"], 18)
        self.assertEqual(result["current"]["visibilityKm"], 10)
        self.assertEqual(result["daily"][0]["pop"], 0.75)
        self.assertEqual(result["hourly"][0]["windKph"], 18)

    def test_invalid_probability_is_missing_not_clamped(self):
        self.assertIsNone(weather.pop_from_unit_interval(75))
        self.assertIsNone(weather.pop_from_percent(float("nan")))
        self.assertIsNone(weather.pop_from_percent(101))
        self.assertEqual(weather.pop_from_percent(0), 0)

    def test_provider_requests_reject_nonmetric_units(self):
        for fetch in (weather.fetch_openweather, weather.fetch_tomorrow):
            with self.assertRaises(ValueError):
                fetch(44, -93, "test-key", units="imperial")

    def test_legacy_imperial_config_cannot_change_normalized_units(self):
        loc = {"name": "test", "type": "latlon", "lat": 0, "lon": 0, "timezone": "America/Chicago"}
        raw = {"current": {"dt": 1, "temp": 20, "wind_speed": 5}}
        with patch.object(weather, "fetch_openweather", return_value=raw) as owm, \
             patch.object(weather, "fetch_tomorrow", return_value={}) as tmr, \
             patch.object(weather, "save_weather", side_effect=lambda name, data: data):
            result = weather.get_weather_for_location(loc, {"openweather_api_key": "test", "tomorrow_api_key": "test", "units": "imperial"})
        self.assertEqual(owm.call_args.kwargs["units"], "metric")
        self.assertEqual(tmr.call_args.kwargs["units"], "metric")
        self.assertEqual(result["current"]["tempC"], 20)
        self.assertEqual(result["lat"], 0)
        self.assertEqual(result["extra"]["providerStatus"]["tomorrow"], "failed")

    def test_all_empty_providers_do_not_write_fresh_cache(self):
        with patch.object(weather, "fetch_openweather", return_value={}), \
             patch.object(weather, "save_weather") as save:
            with self.assertRaises(RuntimeError):
                weather.get_weather_for_location({"name": "test", "type": "latlon", "lat": 44, "lon": -93}, {"openweather_api_key": "test"})
        save.assert_not_called()


class SelectionPolicyTest(unittest.TestCase):
    def test_config_and_environment_priority(self):
        a, b = forecast("openweather", 1), forecast("tomorrow", 1)
        self.assertEqual(weather.merge_weather(a, b)["daily"][0]["pop"], 1)
        self.assertEqual(weather.merge_weather(a, b, {"daily_provider_priority": ["tomorrow", "openweather"]})["daily"][0]["pop"], 1)
        with patch.dict(os.environ, {"WEATHER_DAILY_PROVIDER_PRIORITY": "tomorrow,openweather"}):
            self.assertEqual(weather.merge_weather(a, b)["daily"][0]["pop"], 1)
        for invalid in ("tomorrow", ["tomorrow", "tomorrow"], ["unknown", "openweather"]):
            with self.assertRaises(ValueError):
                weather.merge_weather(a, b, {"daily_provider_priority": invalid})

    def test_stale_and_partial_provider_days_fall_back(self):
        a, b = forecast("openweather", 1), forecast("tomorrow", 1)
        b["updatedAt"] = "2026-09-01T00:00:00+00:00"
        config = {"daily_provider_priority": ["tomorrow", "openweather"]}
        with patch.object(weather, "utcnow", return_value=datetime(2026, 9, 2, tzinfo=timezone.utc)):
            self.assertEqual(weather.merge_weather(a, b, config)["daily"][0]["_sources"]["pop"], "openweather")
        b.pop("updatedAt")
        b["daily"][0]["_period"] = {"complete": False}
        self.assertEqual(weather.merge_weather(a, b, config)["daily"][0]["_sources"]["pop"], "openweather")
        self.assertEqual(weather.merge_weather({}, b)["daily"], [])

    def test_invalid_extrema_condition_classification_and_missing_precip(self):
        raw = {"timezone": "America/Chicago", "daily": [{"dt": 1793552400, "temp": {"min": 20, "max": 10}, "weather": [{"id": 500}]}]}
        day = normalize_openweather(raw)["daily"][0]
        self.assertIsNone(day["tempC"]["min"])
        self.assertIsNone(day["tempC"]["max"])
        self.assertIsNone(day["precipMm"])
        self.assertEqual(day["condition"]["category"], "rain")
        self.assertEqual(weather.condition_category("tomorrow", 4001), "rain")
        self.assertEqual(weather.daily_owm_precip({"rain": 0}), 0)
        self.assertIsNone(weather.daily_owm_precip({"rain": -1}))
        self.assertIsNone(weather.kph_from_ms("bad"))
        self.assertIsNone(weather.pop_from_percent("bad"))

    def test_field_fallback_cannot_create_inverted_high_low_pair(self):
        a, b = forecast("openweather", 1), forecast("tomorrow", 1)
        a["daily"][0]["tempC"] = {"min": 5, "max": 10}
        b["daily"][0]["tempC"] = {"min": 20, "max": None}
        day = weather.merge_weather(a, b, {"daily_provider_priority": ["tomorrow", "openweather"]})["daily"][0]
        self.assertEqual(day["tempC"], {"min": 5, "max": 10})
        self.assertEqual(day["_sources"]["tempC"], {"min": "openweather", "max": "openweather"})

    def test_wrong_daily_boundary_is_ineligible(self):
        result = normalize_tomorrow({"data": {"timelines": [{"timestep": "1d", "intervals": [
            {"startTime": "2026-09-02T06:00:00-05:00", "values": {"temperatureMin": 10, "temperatureMax": 20}}]}]}}, timezone_name="America/Chicago")
        self.assertFalse(result["daily"][0]["_period"]["complete"])

    def test_tomorrow_location_array_is_latitude_longitude(self):
        result = normalize_tomorrow({"location": [44.62, -93.14]})
        self.assertEqual((result["lat"], result["lon"]), (44.62, -93.14))
