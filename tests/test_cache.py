import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import cache


class CacheContractTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.old_settings = cache.CACHE_DIR, cache.CACHE_EXPIRY_MINUTES
        self.addCleanup(self.restore_settings)
        cache.init_cache({"cache_dir": self.directory.name, "cache_expiry_minutes": 30})
        self.data = {"updatedAt": "2026-09-02T12:00:00+00:00", "generatedAt": "2026-09-02T12:00:00+00:00",
                     "extra": {"providerFetchedAt": {"openweather": "2026-09-02T11:59:59+00:00"}}, "daily": []}

    def restore_settings(self):
        cache.CACHE_DIR, cache.CACHE_EXPIRY_MINUTES = self.old_settings

    def test_write_time_and_fetch_merge_times_are_distinct(self):
        response = cache.save_weather("home", self.data)
        self.assertEqual(response["updatedAt"], self.data["updatedAt"])
        self.assertEqual(response["generatedAt"], self.data["generatedAt"])
        self.assertNotEqual(response["_cache"]["writtenAt"], self.data["updatedAt"])
        self.assertFalse(response["_cache"]["stale"])
        self.assertNotIn("_cache", cache.read_cached_payload("home")["data"])

    def test_stale_legacy_cache_preserves_original_timestamps(self):
        old = (datetime.now(timezone.utc) - timedelta(hours=2)).replace(tzinfo=None).isoformat()
        Path(cache.get_cache_path("home")).write_text(json.dumps({"timestamp": old, "data": self.data}))
        self.assertIsNone(cache.get_cached_weather("home"))
        payload = cache.read_cached_payload("home")
        response = cache.cached_response(payload, refresh_failed=True)
        self.assertTrue(response["_cache"]["stale"])
        self.assertTrue(response["_cache"]["refreshFailed"])
        self.assertGreaterEqual(response["_cache"]["ageSeconds"], 7200)
        self.assertEqual(response["updatedAt"], self.data["updatedAt"])
        self.assertEqual(response["extra"], self.data["extra"])

    def test_failed_write_preserves_old_cache(self):
        cache.save_weather("home", self.data)
        before = Path(cache.get_cache_path("home")).read_bytes()
        with patch.object(cache.os, "replace", side_effect=OSError("disk error")):
            with self.assertRaises(OSError):
                cache.save_weather("home", {"updatedAt": "new"})
        self.assertEqual(Path(cache.get_cache_path("home")).read_bytes(), before)
        self.assertEqual(os.listdir(self.directory.name), ["home.json"])

    def test_reader_sees_complete_old_payload_until_atomic_replace(self):
        cache.save_weather("home", self.data)
        replace = os.replace
        def inspect_then_replace(src, dest):
            self.assertEqual(cache.get_cached_weather("home")["updatedAt"], self.data["updatedAt"])
            replace(src, dest)
        with patch.object(cache.os, "replace", side_effect=inspect_then_replace):
            cache.save_weather("home", {"updatedAt": "new"})
        self.assertEqual(cache.get_cached_weather("home")["updatedAt"], "new")

    def test_corrupt_cache_is_treated_as_missing(self):
        Path(cache.get_cache_path("home")).write_text('{"timestamp":')
        self.assertIsNone(cache.get_cached_weather("home"))
        self.assertFalse(cache.get_cache_meta("home")["exists"])
