"""Independent expectations for provider-shaped examples, separate from gallery exports."""

import json
import unittest
from unittest.mock import patch
from pathlib import Path

from timeutils import instant
from weather import merge_weather, normalize_openweather, normalize_tomorrow

FIXTURE = json.loads((Path(__file__).parent / "fixtures/provider-condition-semantics.json").read_text())
FETCHED_AT = "2026-10-30T15:00:00+00:00"
HOURLY_TIMESTAMP = "2023-11-14T22:13:20+00:00"


def tomorrow_timeline(timestep, values, start="2026-11-01T00:00:00-05:00"):
    return {"data": {"timelines": [{"timestep": timestep, "intervals": [
        {"startTime": start, "values": values}
    ]}]}}


class ProviderConditionSemanticsTest(unittest.TestCase):
    def setUp(self):
        clock = patch("weather.utcnow", return_value=instant(FETCHED_AT))
        clock.start()
        self.addCleanup(clock.stop)
    def test_tomorrow_mixed_cloud_codes_in_documented_fields(self):
        forecast = normalize_tomorrow(FIXTURE["tomorrowMixedCloud"], timezone_name="America/Chicago",
                                      fetched_at=FETCHED_AT)
        record = forecast["daily"][0]
        for field, code, context, basis in (
            ("weatherCodeFullDay", 1103, "fullDay", "sunrise_to_sunrise"),
            ("weatherCodeDay", 11030, "daytime", "sunrise_to_sunset"),
            ("weatherCodeNight", 11031, "nighttime", "sunset_to_sunrise"),
        ):
            with self.subTest(field=field):
                condition = record["conditions"][context]
                self.assertEqual(set(condition["cloudCover"]["classes"]), {"partly_cloudy", "mostly_clear"})
                self.assertEqual(len(condition["cloudCover"]["classes"]), 2)
                self.assertTrue(condition["cloudCover"]["mixed"])
                self.assertIsNone(condition["cloudCover"]["percent"])
                self.assertEqual(condition["phenomena"], [])
                self.assertEqual(condition["status"], "known")
                self.assertEqual(condition["context"], context)
                self.assertEqual(condition["period"], {
                    "basis": basis, "start": None, "end": None, "localDate": "2026-11-01"
                })
                self.assertEqual(condition["source"], {
                    "provider": "tomorrow", "retrievedAt": FETCHED_AT,
                    "sourceTime": "2026-11-01T05:00:00+00:00", "observedAt": None, "issuedAt": None,
                    "timestep": "1d", "dataKind": "forecast", "native": [{"field": field, "code": code}],
                })
        merged = merge_weather({}, forecast)["daily"][0]
        self.assertEqual(merged["_sources"]["conditions"], {
            "fullDay": "tomorrow", "daytime": "tomorrow", "nighttime": "tomorrow"
        })
        self.assertEqual(merged["condition"]["code"], 1001)

    def test_tomorrow_rejects_documented_codes_in_wrong_fields_but_preserves_raw_evidence(self):
        codes = {"weatherCodeFullDay": 1103, "weatherCodeDay": 11030, "weatherCodeNight": 11031}
        for code_field, code in codes.items():
            for field in (*codes, "weatherCode"):
                if field == code_field:
                    continue
                with self.subTest(code=code, field=field):
                    data = normalize_tomorrow(tomorrow_timeline("1d", {field: code}),
                                              timezone_name="America/Chicago")
                    record = data["daily"][0]
                    merged = merge_weather({}, data)["daily"][0]
                    diagnostic = merged["_providers"]["tomorrow"]
                    candidate = next(
                        condition for condition in (
                            *diagnostic["conditions"].values(),
                            *(diagnostic.get("conditionCandidates") or {}).values()
                        ) if condition and condition["source"]["native"] == [{"field": field, "code": code}]
                    )
                    self.assertEqual(candidate["source"]["native"], [{"field": field, "code": code}])
                    self.assertEqual(candidate["cloudCover"]["classes"], [])
                    self.assertEqual(candidate["status"], "unknown")
                    self.assertTrue(all(condition is None for condition in merged["conditions"].values()))

    def test_tomorrow_documented_mixes_keep_distinct_components(self):
        raw = tomorrow_timeline("1d", {"weatherCodeDay": 51080, "cloudCover": 60})
        condition = normalize_tomorrow(raw)["daily"][0]["conditions"]["daytime"]
        self.assertEqual(condition["status"], "known")
        self.assertEqual(condition["phenomena"], [
            {"kind": "rain", "intensity": None, "qualifiers": [], "evidenceIndex": 0},
            {"kind": "snow", "intensity": None, "qualifiers": [], "evidenceIndex": 0},
        ])
        self.assertEqual(condition["cloudCover"], {"classes": [], "mixed": False, "percent": None})
        self.assertEqual(condition["period"]["basis"], "sunrise_to_sunset")
        self.assertEqual(condition["source"]["native"], [{"field": "weatherCodeDay", "code": 51080}])

    def test_tomorrow_full_day_and_night_catalog_cases(self):
        for field, context, code, classes, expected in (
            ("weatherCodeFullDay", "fullDay", 2101, ["mostly_clear"], [("fog", "light")]),
            ("weatherCodeFullDay", "fullDay", 5122, [], [("drizzle", None), ("snow", "light")]),
            ("weatherCodeFullDay", "fullDay", 6214, ["partly_cloudy"], [("freezing_rain", None)]),
            ("weatherCodeFullDay", "fullDay", 8001, ["mostly_clear"], [("thunderstorm", None)]),
            ("weatherCodeDay", "daytime", 51100, [], [("drizzle", None), ("snow", None)]),
            ("weatherCodeDay", "daytime", 51120, [], [("snow", None), ("ice_pellets", None)]),
            ("weatherCodeDay", "daytime", 51140, [], [("snow", None), ("freezing_rain", None)]),
            ("weatherCodeDay", "daytime", 51220, [], [("drizzle", None), ("snow", "light")]),
            ("weatherCodeDay", "daytime", 62040, [], [("drizzle", None), ("freezing_drizzle", None)]),
            ("weatherCodeDay", "daytime", 62060, [], [("rain", "light"), ("freezing_drizzle", None)]),
            ("weatherCodeDay", "daytime", 62120, [], [("drizzle", None), ("freezing_rain", None)]),
            ("weatherCodeDay", "daytime", 62200, [], [("rain", "light"), ("freezing_rain", None)]),
            ("weatherCodeDay", "daytime", 62220, [], [("rain", None), ("freezing_rain", None)]),
            ("weatherCodeDay", "daytime", 60000, [], [("freezing_drizzle", None)]),
            ("weatherCodeDay", "daytime", 60020, ["partly_cloudy"], [("freezing_drizzle", None)]),
            ("weatherCodeDay", "daytime", 60030, ["mostly_clear"], [("freezing_drizzle", None)]),
            ("weatherCodeDay", "daytime", 60040, ["mostly_cloudy"], [("freezing_drizzle", None)]),
            ("weatherCodeDay", "daytime", 71030, [], [("freezing_rain", None), ("ice_pellets", "heavy")]),
            ("weatherCodeDay", "daytime", 71050, [], [("drizzle", None), ("ice_pellets", None)]),
            ("weatherCodeDay", "daytime", 71060, [], [("freezing_rain", None), ("ice_pellets", None)]),
            ("weatherCodeDay", "daytime", 71150, [], [("rain", "light"), ("ice_pellets", None)]),
            ("weatherCodeDay", "daytime", 71170, [], [("rain", None), ("ice_pellets", None)]),
            ("weatherCodeNight", "nighttime", 10011, ["cloudy"], []),
            ("weatherCodeNight", "nighttime", 60021, ["partly_cloudy"], [("freezing_drizzle", None)]),
            ("weatherCodeNight", "nighttime", 60031, ["mostly_clear"], [("freezing_drizzle", None)]),
            ("weatherCodeNight", "nighttime", 60041, ["mostly_cloudy"], [("freezing_drizzle", None)]),
            ("weatherCodeNight", "nighttime", 71031, [], [("freezing_rain", None), ("ice_pellets", "heavy")]),
            ("weatherCodeNight", "nighttime", 51121, [], [("snow", None), ("ice_pellets", None)]),
        ):
            with self.subTest(field=field, code=code):
                row = normalize_tomorrow(tomorrow_timeline("1d", {field: code}))["daily"][0]["conditions"][context]
                self.assertEqual(row["status"], "known")
                self.assertEqual(row["cloudCover"]["classes"], classes)
                self.assertEqual([(event["kind"], event["intensity"]) for event in row["phenomena"]], expected)
                self.assertEqual(row["source"]["native"], [{"field": field, "code": code}])

    def test_tomorrow_checks_exact_field_and_keeps_disputed_or_unverified_evidence(self):
        cases = {
            "weatherCode": (10000, 2101, 3000, 3001, 3002, 80001, 0),
            "weatherCodeFullDay": (6200, 10000, 0),
            "weatherCodeDay": (62000, 1000, 10011, 0),
            "weatherCodeNight": (62001, 1000, 10010, 0),
        }
        for field, codes in cases.items():
            for code in codes:
                with self.subTest(field=field, code=code):
                    row = normalize_tomorrow(tomorrow_timeline("1d", {field: code}))["daily"][0]
                    condition = row["_conditionCandidates"][field]
                    self.assertEqual(condition["status"], "unknown")
                    self.assertEqual(condition["source"]["native"], [{"field": field, "code": code}])
                    self.assertEqual(condition["phenomena"], [])
        row = normalize_tomorrow(tomorrow_timeline("1d", {"weatherCode": 6200, "weatherCodeFullDay": 6200}))["daily"][0]
        self.assertEqual(row["conditions"]["fullDay"]["phenomena"][0]["kind"], "freezing_rain")
        self.assertEqual(row["conditions"]["fullDay"]["source"]["native"], [{"field": "weatherCode", "code": 6200}])

    def test_tomorrow_current_and_minute_keep_valid_same_record_cloud_cover(self):
        for timestep, kind in (("current", "unknown"), ("1m", "forecast")):
            for percent, expected in ((0, 0), (35.5, 35.5), (100, 100), (-1, None), (101, None),
                                      (True, None), ("45", None), (float("nan"), None), (None, None)):
                with self.subTest(timestep=timestep, percent=percent):
                    raw = tomorrow_timeline(timestep, {"weatherCode": 4201, "cloudCover": percent})
                    record = normalize_tomorrow(raw)["current"]
                    self.assertEqual(record["cloudCoverPct"], expected)
                    condition = record["conditions"]["current"]
                    self.assertEqual(condition["status"], "known")
                    self.assertEqual(condition["cloudCover"]["percent"], expected)
                    self.assertEqual(condition["phenomena"], [
                        {"kind": "rain", "intensity": "heavy", "qualifiers": [], "evidenceIndex": 0}
                    ])
                    self.assertEqual(condition["period"]["basis"], "unknown")
                    self.assertEqual(condition["source"]["timestep"], timestep)
                    self.assertEqual(condition["source"]["dataKind"], kind)

    def test_openweather_hourly_provenance_retains_timestamp_with_unconfirmed_bounds(self):
        record = normalize_openweather(FIXTURE["openWeatherHourly"], fetched_at=FETCHED_AT)["hourly"][0]
        condition = record["conditions"]["hourly"]
        self.assertEqual(record["time"], HOURLY_TIMESTAMP)
        self.assertEqual(condition["source"]["sourceTime"], HOURLY_TIMESTAMP)
        self.assertEqual(condition["source"]["provider"], "openweather")
        self.assertEqual(condition["source"]["dataKind"], "forecast")
        self.assertEqual(condition["source"]["retrievedAt"], FETCHED_AT)
        self.assertEqual(condition["source"]["native"], [
            {"field": "weather", "code": 502, "main": "Rain", "description": "heavy intensity rain", "icon": "10n"},
            {"field": "weather", "code": 741, "main": "Fog", "description": "fog", "icon": "50n"},
        ])
        self.assertEqual(condition["phenomena"], [
            {"kind": "rain", "intensity": "heavy", "qualifiers": [], "evidenceIndex": 0},
            {"kind": "fog", "intensity": None, "qualifiers": [], "evidenceIndex": 1},
        ])
        self.assertEqual(condition["status"], "known")
        self.assertEqual(condition["lightHint"], "night")
        self.assertEqual(condition["cloudCover"]["percent"], 35)
        for period in (condition["period"], record["popPeriod"]):
            self.assertEqual(period["basis"], "hourly")
            self.assertIsNone(period["start"])
            self.assertIsNone(period["end"])
        self.assertEqual(record["popPeriod"]["sourceTime"], HOURLY_TIMESTAMP)
        self.assertEqual(record["pop"], 0)

    def test_hourly_timestamp_alignment_preserves_fallback_without_asserting_comparability(self):
        owm = normalize_openweather(FIXTURE["openWeatherHourly"], fetched_at=FETCHED_AT)
        tmr_raw = tomorrow_timeline("1h", {"weatherCode": 4001, "precipitationProbability": 50},
                                    HOURLY_TIMESTAMP)
        tmr = normalize_tomorrow(tmr_raw, fetched_at=FETCHED_AT)
        row = merge_weather(owm, tmr)["hourly"][0]
        self.assertEqual(row["_popComparability"]["status"], "unverified")
        self.assertTrue(row["_popComparability"]["timestampAligned"])
        self.assertTrue({"start", "end"}.issubset(row["_popComparability"]["reasons"]))
        self.assertEqual(row["_sources"]["pop"], "tomorrow")

        del tmr_raw["data"]["timelines"][0]["intervals"][0]["values"]["precipitationProbability"]
        row = merge_weather(owm, normalize_tomorrow(tmr_raw, fetched_at=FETCHED_AT))["hourly"][0]
        self.assertEqual(row["pop"], 0)
        self.assertEqual(row["_sources"]["pop"], "openweather")
        self.assertEqual(row["popPeriod"]["sourceTime"], HOURLY_TIMESTAMP)
        self.assertIsNone(row["popPeriod"]["start"])
