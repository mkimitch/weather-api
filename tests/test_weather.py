import unittest
import sys
import types

sys.modules.setdefault("requests", types.ModuleType("requests"))

import weather
from weather import fetch_tomorrow, normalize_openweather, normalize_tomorrow


class ProbabilityNormalizationTest(unittest.TestCase):
    def test_tomorrow_request_includes_daily_probability_aggregates(self):
        captured = {}

        class FakeResponse:
            ok = True

            def json(self):
                return {"data": {"timelines": []}}

        def fake_post(url, json, headers, timeout):
            captured["fields"] = json["fields"]
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


if __name__ == "__main__":
    unittest.main()
