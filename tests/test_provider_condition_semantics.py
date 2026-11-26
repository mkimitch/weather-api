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

    def test_tomorrow_artwork_only_wintry_mix_keeps_components_unknown(self):
        raw = tomorrow_timeline("1d", {"weatherCodeDay": 51080, "cloudCover": 60})
        condition = normalize_tomorrow(raw)["daily"][0]["conditions"]["daytime"]
        self.assertEqual(condition["status"], "partial")
        self.assertEqual(condition["phenomena"], [
            {"kind": "mixed_precipitation", "intensity": None, "qualifiers": [], "evidenceIndex": 0}
        ])
        self.assertEqual(condition["cloudCover"], {"classes": [], "mixed": False, "percent": None})
        self.assertEqual(condition["period"]["basis"], "sunrise_to_sunset")
        self.assertEqual(condition["source"]["native"], [{"field": "weatherCodeDay", "code": 51080}])

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
