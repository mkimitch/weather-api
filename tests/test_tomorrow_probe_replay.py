"""Regression based on observed bindings and coverage, with synthetic public data."""

import json
import unittest
from collections import Counter
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from conditions import TOMORROW_SOLAR_FIELDS, tomorrow_code_mapping
from timeutils import instant
from tomorrow_acquisition import filter_rich_values, inspect_acquisition, read_timelines
from weather import merge_weather, normalize_openweather, normalize_tomorrow_response

EVIDENCE = json.loads((Path(__file__).parent / "fixtures" / "tomorrow-probe-regression.json").read_text())
CODE_FIELDS = ("weatherCode", *TOMORROW_SOLAR_FIELDS)


def timeline(step, intervals):
    return {"timestep": step, "startTime": intervals[0]["startTime"],
            "endTime": intervals[-1]["startTime"], "intervals": intervals}


def synthetic_response():
    zone = ZoneInfo("America/Chicago")
    midnight = datetime(2026, 2, 9, tzinfo=zone)
    first_hour = midnight + timedelta(hours=20)
    start = first_hour + timedelta(minutes=48)
    hours = []
    for i in range(EVIDENCE["hourlyCount"]):
        values = {field: codes[i % len(codes)] for field, codes in EVIDENCE["hourlyCodeSets"].items()}
        values.update(temperature=14, precipitationProbability=(i % 3) * 20)
        if i < EVIDENCE["hourlyUvCount"]:
            values["uvIndex"] = 1
        hours.append({"startTime": (first_hour + timedelta(hours=i)).isoformat(), "values": values})

    boundary = EVIDENCE["boundaryNightCode"]
    night_codes = [boundary]
    for code, count in EVIDENCE["interiorNightCodeCounts"].items():
        night_codes.extend([int(code)] * count)
    night_codes.append(boundary)
    days = []
    for i, night_code in enumerate(night_codes):
        values = {field: codes[i % len(codes)] for field, codes in EVIDENCE["dailyCodeSets"].items()}
        values.update(weatherCodeNight=night_code, temperatureMin=4, temperatureMax=16,
                      precipitationProbabilityAvg=i * 10, precipitationProbabilityMax=80)
        days.append({"startTime": (midnight + timedelta(days=i)).isoformat(), "values": values})
    return {"data": {"timelines": [timeline("1h", hours), timeline("1d", days)]}}, start, zone.key


class TomorrowProbeReplayTest(unittest.TestCase):
    def test_observed_code_inventory_and_production_shaped_coverage(self):
        raw, start, zone = synthetic_response()
        end = start + timedelta(days=5)
        received_at = (start + timedelta(milliseconds=200)).isoformat()
        self.check_replay(raw, start, end, zone, received_at)

    def check_replay(self, raw, start, end, zone, received_at, archived_coverage=None):
        # A generic projection isolates normalization effects. The capture did
        # not include the original archived generic response.
        generic = deepcopy(raw)
        filter_rich_values(generic, publish=False)
        baseline, _ = read_timelines(generic["data"])
        if archived_coverage is not None:
            metadata = {row["timestep"]: row for row in archived_coverage}
            for step, rows in baseline.items():
                saved = metadata[step]
                self.assertEqual((len(rows), next(iter(rows)), next(reversed(rows))),
                                 (saved["count"], instant(saved["first"]), instant(saved["last"])))
                self.assertEqual(Counter(field for values in rows.values() for field, value in values.items()
                                         if value is not None), saved["fieldCounts"])

        result = inspect_acquisition(raw, baseline, start, end, zone)
        self.assertTrue(result["publishable"], result)
        self.assertEqual(result["baselineIssues"], {})
        self.assertEqual(result["warnings"]["count"], 0)
        self.assertEqual(result["completeInteriorDays"], 4)
        intervals = {t["timestep"]: t["intervals"] for t in raw["data"]["timelines"]}
        for step, name, count in (("1h", "hourly", 121), ("1d", "daily", 6)):
            self.assertEqual(result["timelines"][step]["rows"], count)
            observed = {field: set() for field in CODE_FIELDS}
            for interval in intervals[step]:
                for field in CODE_FIELDS:
                    code = interval["values"][field]
                    entry = tomorrow_code_mapping(field, code)
                    self.assertTrue(entry and entry.get("status", "known") == "known", (field, code))
                    observed[field].add(code)
            self.assertEqual({field: sorted(codes) for field, codes in observed.items()},
                             EVIDENCE[f"{name}CodeSets"])
        for field in TOMORROW_SOLAR_FIELDS:
            self.assertEqual([result["fields"][field][step]["known"] for step in
                              ("1h", "1d", "completeInteriorDays")], [121, 6, 4])

        enriched = normalize_tomorrow_response(raw, zone, received_at, start, end)
        plain = normalize_tomorrow_response(generic, zone, received_at, start, end)
        for key in ("current", "hourly"):
            self.assertEqual(enriched[key], plain[key])
        for interval, row in zip(intervals["1h"], enriched["hourly"]):
            self.assertEqual(row["pop"], interval["values"]["precipitationProbability"] / 100)
        self.assertEqual(len(enriched["daily"]), 6)
        periods = [row["_period"] for row in enriched["daily"]]
        self.assertEqual([p["complete"] for p in periods], [True] * 5 + [False])
        self.assertEqual([p["startedBeforeRetrieval"] for p in periods], [True] + [False] * 5)
        for enriched_day, plain_day, interval in zip(enriched["daily"], plain["daily"], intervals["1d"]):
            for key in ("date", "_period", "condition", "tempC", "pop", "popBasis", "popPeriod"):
                self.assertEqual(enriched_day[key], plain_day[key])
            self.assertEqual(enriched_day["popBasis"], "precipitationProbabilityAvg")
            self.assertEqual(enriched_day["pop"], interval["values"]["precipitationProbabilityAvg"] / 100)
            self.assertIsNone(plain_day["conditions"]["nighttime"])
            for field, (context, basis) in TOMORROW_SOLAR_FIELDS.items():
                condition = enriched_day["conditions"][context]
                self.assertEqual(condition["status"], "known")
                self.assertEqual(condition["source"]["native"], [{"field": field, "code": interval["values"][field]}])
                self.assertEqual(condition["period"], {"basis": basis, "start": None, "end": None,
                                                       "localDate": enriched_day["date"]})

        by_night_code = Counter()
        for day in enriched["daily"][1:-1]:
            night = day["conditions"]["nighttime"]
            code = night["source"]["native"][0]["code"]
            by_night_code[str(code)] += 1
            classes = {10011: ["cloudy"], 11031: ["partly_cloudy", "mostly_clear"]}
            self.assertEqual(night["cloudCover"], {"classes": classes[code], "mixed": code == 11031,
                                                  "percent": None})
        self.assertEqual(by_night_code, EVIDENCE["interiorNightCodeCounts"])

        # Add a synthetic competing provider for leading/interior dates only.
        # One day has no OpenWeather POP so Tomorrow's averaged POP must survive.
        owm = normalize_openweather({"daily": [
            {"dt": int(instant(period["start"]).timestamp()), "temp": {"min": 3, "max": 15},
             "pop": None if i == 1 else 0.35, "weather": [{"id": 801, "icon": "02d"}]}
            for i, period in enumerate(periods[:-1])
        ]}, timezone_name=zone, fetched_at=received_at)
        merged_payloads = {}
        with patch("weather.utcnow", return_value=instant(received_at)):
            for order in (("openweather", "tomorrow"), ("tomorrow", "openweather")):
                with self.subTest(priority=order), patch.dict("os.environ", {"WEATHER_DAILY_PROVIDER_PRIORITY": ",".join(order)}):
                    merged = merge_weather(owm, enriched)
                    control = merge_weather(owm, plain)
                    for key in ("current", "hourly"):
                        self.assertEqual(merged[key], control[key])
                    self.assertEqual(merged["conditionSchemaVersion"], 1)
                    self.assertEqual([d["date"] for d in merged["daily"]],
                                     [d["date"] for d in enriched["daily"][:-1]])
                    for i, (row, original) in enumerate(zip(merged["daily"], control["daily"])):
                        self.assertEqual(row["_sources"]["conditions"],
                                         {"fullDay": order[0], "daytime": "tomorrow", "nighttime": "tomorrow"})
                        for key in ("pop", "popBasis", "popPeriod"):
                            self.assertEqual(row[key], original[key])
                            self.assertEqual(row["_sources"][key], original["_sources"][key])
                        self.assertEqual(row["_sources"]["pop"], "tomorrow" if i == 1 else "openweather")
                        self.assertEqual(row["pop"], enriched["daily"][i]["pop"] if i == 1 else 0.35)
                    merged_payloads[order] = merged
        return merged_payloads
