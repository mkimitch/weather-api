import copy
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import history
import observations
import verification
from test_observations import feature
from timeutils import day_bounds, local_noon


class VerificationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config = {"cache_dir": self.temp.name}
        self.location = {"name": "home", "lat": 44.62, "lon": -93.14, "timezone": "America/Chicago", "observation_station": "KLVN"}
        self.day = "2026-11-01"
        self.start, self.end = day_bounds(self.day, "America/Chicago")
        self.now = self.end + timedelta(hours=12)
        features = [feature(self.start + timedelta(hours=i), temp=10 if i < 12 else 20, wind=12) for i in range(26)]
        observations.ingest_observations(self.config, "KLVN", features, self.now)

    def snapshot(self, provider, retrieved=None, high=None, low=None):
        retrieved = retrieved or self.start - timedelta(hours=12)
        offset = 0 if provider == "tomorrow" else 2
        data = {"updatedAt": retrieved.isoformat(), "timezone": "America/Chicago",
                "daily": [{"date": self.day, "tempC": {"max": high if high is not None else 20 + offset, "min": low if low is not None else 10 + offset},
                           "pop": .8, "_period": {"start": self.start.isoformat(), "end": self.end.isoformat(), "complete": True}}],
                "hourly": [{"time": local_noon(self.day, "America/Chicago").isoformat(), "windKph": 12 + offset}]}
        history.record_forecast(self.config, self.location, provider, {}, data)
        return data

    def test_scoring_kernels(self):
        score = verification.scores([(4, 2), (2, 3)], "tempMaxC")
        self.assertEqual(score["mae"], 1.5)
        self.assertEqual(score["bias"], .5)
        self.assertAlmostEqual(verification.scores([(.8, 1), (.3, 0)], "pop")["brierScore"], .065)
        self.assertAlmostEqual(verification.scores([(.8, 1), (.3, 0)], "pop")["probabilityBias"], .05)
        amount = verification.scores([(2, 1), (0, 1), (1, 0), (0, 0)], "precipMm")
        self.assertEqual(amount["mae"], .75)
        self.assertEqual(amount["events"]["criticalSuccessIndex"], 1/3)
        self.assertEqual(verification.scores([("rain", "rain"), ("rain", "snow")], "condition")["accuracy"], .5)
        with self.assertRaises(ValueError):
            verification.scores([(75, 1)], "pop")

    def test_report_paired_accuracy_bias_lead_coverage_and_unverifiable_metrics(self):
        self.snapshot("tomorrow")
        self.snapshot("openweather", self.start - timedelta(hours=12) + timedelta(seconds=10))
        report = verification.report(self.config, self.location, 30, self.now)
        high = next(r for r in report["results"] if r["variable"] == "tempMaxC")
        self.assertEqual(high["providers"]["tomorrow"]["paired"]["mae"], 0)
        self.assertEqual(high["providers"]["openweather"]["paired"]["bias"], 2)
        self.assertEqual(high["pairedEvidence"]["sampleCount"], 1)
        self.assertEqual(high["pairedEvidence"]["status"], "insufficient_data")
        self.assertEqual(high["leadHours"], {"minInclusive": 0, "maxExclusive": 24})
        wind = next(r for r in report["results"] if r["variable"] == "noonWindKph")
        self.assertEqual(wind["providers"]["openweather"]["paired"]["mae"], 2)
        self.assertEqual(report["observationCoverage"]["temperatureVerifiableDays"], 1)
        self.assertEqual(report["observationCoverage"]["precipitationVerifiableDays"], 0)
        self.assertEqual(set(report["unverifiable"]), {"pop", "precipMm", "condition"})
        self.assertFalse(report["automaticSwitching"])
        self.assertEqual(report, verification.report(self.config, self.location, 30, self.now))
        json.dumps(report, allow_nan=False)

    def test_refreshes_not_independent_samples_and_post_start_forecasts_excluded(self):
        for i in range(12):
            self.snapshot("tomorrow", self.start - timedelta(hours=2) + timedelta(minutes=i*5))
            self.snapshot("openweather", self.start - timedelta(hours=2) + timedelta(minutes=i*5, seconds=10))
        self.snapshot("tomorrow", self.start + timedelta(hours=1), high=100)
        results = verification.report(self.config, self.location, 30, self.now)["results"]
        self.assertEqual(results[0]["pairedEvidence"]["sampleCount"], 1)
        self.assertEqual(results[0]["providers"]["tomorrow"]["paired"]["mae"], 0)

    def test_mismatched_retrieval_times_not_paired_and_different_leads_separate(self):
        self.snapshot("tomorrow", self.start - timedelta(hours=2))
        self.snapshot("openweather", self.start - timedelta(hours=20))
        self.snapshot("tomorrow", self.start - timedelta(hours=50))
        results = verification.report(self.config, self.location, 90, self.now)["results"]
        self.assertEqual({r["leadHours"]["minInclusive"] for r in results}, {0, 48})
        self.assertTrue(all(r["pairedEvidence"]["sampleCount"] == 0 for r in results))
        self.assertEqual(results[0]["unpairedDueToRetrievalTime"], 1)

    def test_location_period_and_observation_gates(self):
        self.snapshot("tomorrow")
        self.snapshot("openweather")
        report = verification.report(self.config, dict(self.location, lat=45), 365, self.now)
        self.assertEqual(report["results"], [])
        early = verification.report(self.config, self.location, 30, self.end + timedelta(hours=1))
        self.assertTrue(all(r["pairedEvidence"]["sampleCount"] == 0 for r in early["results"]))
        missing = verification.report(self.config, dict(self.location, observation_station="KMSP"), 30, self.now)
        self.assertEqual(missing["observationCoverage"]["temperatureVerifiableDays"], 0)
        with self.assertRaises(ValueError):
            verification.report(self.config, self.location, 7, self.now)

    def test_clustered_evidence_is_conservative_and_reproducible(self):
        self.assertIsNone(verification.paired_evidence([("2026-01-01", 1, 3)] * 10, "tempMaxC")["recommendation"])
        events = [((datetime(2026, 1, 1) + timedelta(days=i)).date().isoformat(), 1, 3) for i in range(90)]
        evidence = verification.paired_evidence(events, "tempMaxC")
        self.assertEqual(evidence["recommendation"], "tomorrow")
        self.assertEqual(evidence["confidenceInterval95"], [-2, -2])
        self.assertEqual(evidence, verification.paired_evidence(events, "tempMaxC"))
        no_difference = [(d, 1, 1) for d, _, _ in events]
        self.assertIsNone(verification.paired_evidence(no_difference, "tempMaxC")["recommendation"])

    def test_partial_periods_are_archived_but_excluded_from_scores(self):
        data = self.snapshot("tomorrow")
        data["updatedAt"] = (self.start - timedelta(hours=1)).isoformat()
        data["daily"][0]["_period"]["complete"] = False
        history.record_forecast(self.config, self.location, "tomorrow", {}, data)
        report = verification.report(self.config, self.location, 30, self.now)
        self.assertEqual(report["excluded"]["incomplete_or_unknown_forecast_period"], 1)
        self.assertTrue(all(r["providers"]["tomorrow"]["allAvailable"]["sampleCount"] == 0 for r in report["results"]))

    def test_forecast_reliability_excludes_geocoding_and_other_locations(self):
        identity = history.location_identity(self.location)
        other_identity = history.location_identity(dict(self.location, lat=45))
        for request_type, point, status in (
            ("geocode", None, "geocoded"), ("geocode", None, "failed"),
            ("forecast", other_identity, "failed"),
            ("forecast", identity, "ok"), ("forecast", identity, "ok"),
        ):
            attempt = history.reserve_attempt(self.config, "openweather", "home", self.now,
                                              request_type=request_type, identity=point)
            history.finish_attempt(self.config, attempt, status)
        report = verification.report(self.config, self.location, 30, self.now)
        reliability = report["reliability"]["openweather"]
        self.assertEqual(reliability["attempts"], 2)
        self.assertEqual(reliability["failureRate"], 0)
        limited = dict(self.config, max_calls_per_24h_openweather=5)
        self.assertIsNone(history.reserve_attempt(limited, "openweather", "home", self.now))
