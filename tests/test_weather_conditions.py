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


class CanonicalWeatherTest(unittest.TestCase):
    def test_secondary_conditions_remain_distinct(self):
        raw = {"hourly": [{"dt": 1700000000, "weather": [
            {"id": 511, "description": "freezing rain"}, {"id": 615, "description": "light rain and snow"}
        ]}]}
        condition = normalize_openweather(raw)["hourly"][0].get("conditions", {}).get("hourly")
        self.assertIsNotNone(condition)
        self.assertEqual([item["code"] for item in condition["source"]["native"]], [511, 615])
        self.assertEqual([item["kind"] for item in condition["phenomena"]],
                         ["freezing_rain", "rain", "snow"])

    def test_solar_fields_keep_separate_basis_and_calendar_label(self):
        raw = {"data": {"timelines": [{"timestep": "1d", "intervals": [
            {"startTime": "2026-11-01T00:00:00-05:00", "values": {
                "weatherCode": 1001, "weatherCodeFullDay": 1103, "weatherCodeDay": 11030,
                "weatherCodeNight": 11031,
            }},
        ]}]}}
        day = normalize_tomorrow(raw, timezone_name="America/Chicago")["daily"][0]
        conditions = day.get("conditions", {})
        self.assertEqual({key: value["period"]["basis"] for key, value in conditions.items()},
                         {"fullDay": "sunrise_to_sunrise", "daytime": "sunrise_to_sunset",
                          "nighttime": "sunset_to_sunrise"})
        self.assertEqual(conditions["nighttime"]["period"]["localDate"], "2026-11-01")
        self.assertIsNone(conditions["nighttime"]["period"]["start"])
        self.assertTrue(conditions["daytime"]["cloudCover"]["mixed"])
        self.assertEqual(day["condition"]["code"], 1001)

    def test_daily_native_zero_beats_preferred_interval_aggregate(self):
        from weather import merge_weather
        a = {"daily": [{"date": "2099-01-01", "pop": 0, "popBasis": "daily"}]}
        b = {"daily": [{"date": "2099-01-01", "pop": 1, "popBasis": "precipitationProbabilityAvg"}]}
        day = merge_weather(a, b, {"daily_provider_priority": ["tomorrow", "openweather"]})["daily"][0]
        self.assertEqual((day["pop"], day["popBasis"], day["_sources"]["pop"]), (0, "daily", "openweather"))
        self.assertEqual(day["_providers"]["tomorrow"]["pop"], 1)

    def test_unknown_hourly_code_falls_back_with_raw_evidence_preserved(self):
        from weather import merge_weather
        a = normalize_openweather({"hourly": [{"dt": 1700000000, "weather": [{"id": 500}]}]})
        b = normalize_tomorrow({"data": {"timelines": [{"timestep": "1h", "intervals": [
            {"startTime": "2023-11-14T22:13:20Z", "values": {"weatherCode": 9999}},
        ]}]}})
        row = merge_weather(a, b)["hourly"][0]
        self.assertEqual(row["condition"]["code"], 500)
        self.assertEqual(row["conditions"]["hourly"]["source"]["provider"], "openweather")
        self.assertEqual(row["_providers"]["tomorrow"]["conditions"]["hourly"]["status"], "unknown")

    def test_current_fallback_probability_cannot_leak_past_hour_end(self):
        from weather import merge_weather
        a = normalize_openweather({"current": {"dt": 1700003600, "temp": 5}})
        b = normalize_tomorrow({"data": {"timelines": [{"timestep": "1h", "intervals": [
            {"startTime": "2023-11-14T22:13:20Z", "values": {"precipitationProbability": 0}},
        ]}]}})
        current = merge_weather(a, b)["current"]
        self.assertIsNone(current["pop"])
        self.assertIsNone(current["_sources"]["pop"])
        self.assertEqual(current["_providers"]["tomorrow"]["pop"], 0)

        a["current"]["time"] = "2023-11-14T22:43:20+00:00"
        current = merge_weather(a, b)["current"]
        self.assertEqual(current["pop"], 0)
        self.assertEqual(current["_sources"]["popPeriod"], "tomorrow")
        self.assertEqual(current["popPeriod"]["start"], "2023-11-14T22:13:20+00:00")

    def test_calendar_day_boundaries_follow_dst_in_both_directions(self):
        from timeutils import instant
        for start, date, hours in [
            ("2026-03-08T00:00:00-06:00", "2026-03-08", 23),
            ("2026-11-01T00:00:00-05:00", "2026-11-01", 25),
        ]:
            raw = {"data": {"timelines": [{"timestep": "1d", "intervals": [
                {"startTime": start, "values": {"weatherCode": 1000, "precipitationProbabilityAvg": 0}},
            ]}]}}
            day = normalize_tomorrow(raw, timezone_name="America/Chicago")["daily"][0]
            self.assertEqual(day["date"], date)
            for period in (day["popPeriod"], day["conditions"]["fullDay"]["period"]):
                self.assertEqual(period["basis"], "calendar_day")
                self.assertEqual((instant(period["end"]) - instant(period["start"])).total_seconds(), hours * 3600)

    def test_multi_class_cloud_cover_retains_all_classes_without_dominance(self):
        condition = normalize_openweather({"current": {"dt": 1700000000, "weather": [
            {"id": 801}, {"id": 802},
        ]}})["current"]["conditions"]["current"]
        self.assertEqual(condition["cloudCover"], {
            "classes": ["mostly_clear", "partly_cloudy"], "mixed": True, "percent": None,
        })

    def test_condition_only_payload_is_usable(self):
        from weather import has_weather
        data = normalize_openweather({"current": {"dt": 1700000000, "weather": [{"id": 800}]}})
        self.assertTrue(has_weather(data))

    def test_probability_comparison_distinguishes_evidence_from_unknown(self):
        from precipitation import compare_probability
        period = {
            "basis": "calendar_day", "start": "2026-11-01T05:00:00Z", "end": "2026-11-02T06:00:00Z",
            "thresholdMm": 0.254, "eventDefinition": "liquid_equivalent_above_threshold",
            "issuedAt": "2026-10-30T12:00:00Z",
        }
        left = {"popBasis": "daily", "popPeriod": period}
        for changes, expected, reason in [
            ({}, "confirmed_comparable", None),
            ({"thresholdMm": None}, "unverified", "thresholdMm"),
            ({"thresholdMm": 1}, "confirmed_incompatible", "thresholdMm"),
            ({"end": "2026-11-02T05:00:00Z"}, "confirmed_incompatible", "end"),
            ({"issuedAt": "2026-10-31T12:00:00Z"}, "confirmed_incompatible", "issuedAt"),
        ]:
            with self.subTest(changes=changes):
                result = compare_probability(left, {"popBasis": "daily", "popPeriod": {**period, **changes}})
                self.assertEqual(result["status"], expected)
                if reason:
                    self.assertIn(reason, result["reasons"])

    def test_terminal_partial_day_stays_in_diagnostics_only(self):
        from weather import merge_weather
        raw = {"data": {"timelines": [{"timestep": "1d", "intervals": [
            {"startTime": "2099-01-01T00:00:00Z", "values": {
                "weatherCode": 7000, "weatherCodeDay": 11030, "precipitationProbabilityAvg": 10}},
        ]}]}}
        tomorrow = normalize_tomorrow(raw, timezone_name="UTC")
        tomorrow["daily"][0]["_period"]["complete"] = False
        merged = merge_weather({}, tomorrow)
        self.assertEqual(merged["daily"], [])
        alternative = merged.get("extra", {}).get("ineligibleDaily", [{}])[0]
        self.assertEqual(alternative.get("date"), "2099-01-01")
        self.assertEqual(alternative["_providers"]["tomorrow"]["pop"], 0.1)
        self.assertFalse(alternative["_providers"]["tomorrow"]["eligible"])

    def test_archive_retains_all_native_tomorrow_condition_fields(self):
        from history import raw_weather
        values = {"weatherCode": 7000, "weatherCodeFullDay": 1103, "weatherCodeDay": 11030,
                  "weatherCodeNight": 11031, "ignoredField": "not weather evidence"}
        raw = {"data": {"timelines": [{"timestep": "1d", "intervals": [
            {"startTime": "2026-11-01T05:00:00Z", "values": values},
        ]}]}}
        archived = raw_weather("tomorrow", raw)["timelines"][0]["intervals"][0]["values"]
        self.assertEqual(archived, {key: value for key, value in values.items() if key != "ignoredField"})

    def test_rich_daily_selection_preserves_compatible_legacy_projection(self):
        from weather import merge_weather
        owm = normalize_openweather({"timezone": "UTC", "daily": [
            {"dt": 4070952000, "weather": [{"id": 800, "icon": "01d"}]},
        ]})
        date = owm["daily"][0]["date"]
        tmr = normalize_tomorrow({"data": {"timelines": [{"timestep": "1d", "intervals": [
            {"startTime": date + "T00:00:00Z", "values": {
                "weatherCodeFullDay": 1103, "weatherCodeDay": 11030, "weatherCodeNight": 11031,
            }},
        ]}]}}, timezone_name="UTC")

        day = merge_weather(owm, tmr, {"daily_provider_priority": ["tomorrow", "openweather"]})["daily"][0]
        self.assertEqual(day["conditions"]["fullDay"]["source"]["provider"], "tomorrow")
        self.assertEqual(day["condition"]["code"], 800)
        self.assertEqual(day["_sources"]["condition"], "openweather")

        day = merge_weather(owm, tmr)["daily"][0]
        self.assertEqual(day["conditions"]["fullDay"]["source"]["timestep"], "daily")
        self.assertEqual(day["_sources"]["conditions"],
                         {"fullDay": "openweather", "daytime": "tomorrow", "nighttime": "tomorrow"})
        self.assertEqual(day["conditions"]["nighttime"]["source"]["native"], [{"field": "weatherCodeNight", "code": 11031}])
