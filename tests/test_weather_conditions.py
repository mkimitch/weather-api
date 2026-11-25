import unittest

from weather import merge_fields, normalize_openweather, normalize_tomorrow, pop_from_percent


class ImmediateWeatherFixesTest(unittest.TestCase):
    def test_openweather_hourly_condition_is_preserved(self):
        raw = {"hourly": [{"dt": 1700000000, "weather": [{"id": 511}]}]}
        self.assertEqual(normalize_openweather(raw)["hourly"][0]["condition"]["code"], 511)

    def test_tomorrow_hourly_condition_is_preserved(self):
        raw = {"data": {"timelines": [{"timestep": "1h", "intervals": [
            {"startTime": "2026-11-25T00:00:00Z", "values": {"weatherCode": 6000}},
        ]}]}}
        self.assertEqual(normalize_tomorrow(raw)["hourly"][0]["condition"]["code"], 6000)

    def test_invalid_probability_cannot_mask_valid_zero(self):
        for invalid in (True, "0", float("nan"), float("inf"), -0.01, 1.01):
            with self.subTest(invalid=invalid):
                merged = merge_fields({"pop": invalid, "popBasis": "daily"},
                                      {"pop": 0, "popBasis": "precipitationProbabilityMax"},
                                      "openweather", "tomorrow")
                self.assertEqual(merged["pop"], 0)
                self.assertEqual(merged["popBasis"], "precipitationProbabilityMax")
                self.assertEqual(merged["_sources"]["pop"], "tomorrow")

    def test_tomorrow_probability_requires_numeric_percent(self):
        for invalid in (True, "0", float("nan"), float("inf"), -1, 101):
            self.assertIsNone(pop_from_percent(invalid))

    def test_daily_native_zero_beats_preferred_interval_aggregate(self):
        from weather import merge_weather
        a = {"daily": [{"date": "2099-01-01", "pop": 0, "popBasis": "daily"}]}
        b = {"daily": [{"date": "2099-01-01", "pop": 1, "popBasis": "precipitationProbabilityAvg"}]}
        day = merge_weather(a, b, {"daily_provider_priority": ["tomorrow", "openweather"]})["daily"][0]
        self.assertEqual((day["pop"], day["popBasis"], day["_sources"]["pop"]), (0, "daily", "openweather"))
        self.assertEqual(day["_providers"]["tomorrow"]["pop"], 1)
