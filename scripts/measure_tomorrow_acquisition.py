"""Measure the scheduled acquisition and archive with no provider network I/O."""

import copy
import gzip
import json
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import requests
from fastapi.responses import JSONResponse

from history import database, encode, record_forecast
from scheduler import WeatherScheduler
from timeutils import instant
from weather import normalize_tomorrow

CAPTURE = json.loads((ROOT / "tests/fixtures/forecast-2026-09-19.json").read_text())
ZONE = CAPTURE["evidence"]["timezone"]
BASE = CAPTURE["tomorrow"]
DAILY, HOURLY = BASE["data"]["timelines"]
FIRST = instant(HOURLY["intervals"][0]["startTime"])
NOW = FIRST + timedelta(minutes=15)
HOUR_VALUES = [r["values"] for r in HOURLY["intervals"]]
HOURLY["intervals"] = [
    {"startTime": (FIRST + timedelta(hours=i)).isoformat(), "values": HOUR_VALUES[i % len(HOUR_VALUES)]}
    for i in range(120)
]

FIELDS = ("weatherCodeFullDay", "weatherCodeDay", "weatherCodeNight")
CODES = (
    (1000, 10000, 10001),
    (1103, 11030, 11031),
    (2101, 62060, 71031),
    (5122, 62120, 51121),
    (8001, 71150, 80001),
    (1102, 11020, 11021),
)


def measure(mode):
    provider = copy.deepcopy(BASE)
    if mode != "generic":
        days, hours = provider["data"]["timelines"]
        for interval, codes in zip(days["intervals"], CODES):
            interval["values"].update(zip(FIELDS, codes))
        if mode != "daily-only":
            for i, interval in enumerate(hours["intervals"]):
                # Rows repeat in the capture, so copy before appending fields.
                codes = (None,) * 3 if mode == "hourly-null" else CODES[i % len(CODES)]
                interval["values"] = {**interval["values"], **dict(zip(FIELDS, codes))}

    raw = encode(provider).encode()
    response = requests.Response()
    response.status_code = 200
    response._content = raw

    with tempfile.TemporaryDirectory() as directory:
        control = Path(directory) / "tomorrow-rich.control"
        if mode != "generic":
            control.write_text(f"publish {'a' * 32}\n")
            control.chmod(0o600)
        location = {"name": "measurement", "type": "latlon", "lat": 0, "lon": 0, "timezone": ZONE,
                    "tomorrow_rich_conditions_control_file": str(control)}
        config = {"locations": [location], "cache_dir": directory, "refresh_interval_minutes": 5,
                  "tomorrow_forecast_days": 5, "enable_openweather": True}
        previous = (NOW - timedelta(minutes=5)).isoformat()
        record_forecast(config, location, "tomorrow", BASE,
                        normalize_tomorrow(BASE, timezone_name=ZONE, fetched_at=previous))
        with patch.dict("os.environ", {"TOMORROW_API_KEY": "offline", "OPENWEATHER_API_KEY": "offline",
                                      "WEATHER_DAILY_PROVIDER_PRIORITY": "openweather,tomorrow"}), \
             patch("weather.utcnow", return_value=NOW), \
             patch("cache.datetime", wraps=datetime) as cache_clock, \
             patch("weather.fetch_openweather", return_value=CAPTURE["openweather"]), \
             patch("weather.requests.post", return_value=response) as post, \
             patch.object(requests.sessions.Session, "request", side_effect=AssertionError("Offline replay")):
            cache_clock.now.return_value = NOW
            payload = WeatherScheduler(config).refresh_location(location)
        assert post.call_count == 1 and payload["extra"]["providerStatus"]["tomorrow"] == "ok"
        chosen = sum(row["conditions"]["daytime"] is not None for row in payload["daily"])
        assert chosen == (0 if mode == "generic" else 5), (mode, chosen)

        with database(config) as db:
            blob, = db.execute(
                "SELECT length(payload) FROM responses WHERE provider='tomorrow' AND retrieved_at=?",
                (NOW.isoformat(),)).fetchone()
            days, = db.execute(
                "SELECT sum(length(CAST(f.data AS blob))) FROM forecasts f JOIN responses r ON f.response_id=r.id "
                "WHERE r.provider='tomorrow' AND r.retrieved_at=?", (NOW.isoformat(),)).fetchone()
            attempts, = db.execute("SELECT count(*) FROM attempts WHERE provider='tomorrow'").fetchone()
        assert attempts == 1
        public = JSONResponse(payload).body
        size = {
            "upstreamJson": len(raw), "upstreamGzipEstimate": len(gzip.compress(raw, mtime=0)),
            "apiJson": len(public), "apiGzipEstimate": len(gzip.compress(public, mtime=0)),
            "cacheJson": (Path(directory) / "measurement.json").stat().st_size,
            "archiveBlob": blob, "archiveDailyRows": days, "archiveLogicalTotal": blob + days,
        }
        return size, payload


if __name__ == "__main__":
    start, baseline = measure("generic")
    results = {"generic": start}
    for scenario in ("daily-only", "hourly-null", "hourly-populated"):
        size, expanded = measure(scenario)
        for key in ("current", "hourly"):
            assert expanded[key] == baseline[key], key
        pop_fields = ("date", "pop", "popBasis", "popPeriod")
        assert [([d.get(k) for k in pop_fields], d["_sources"].get("pop")) for d in baseline["daily"]] == [
            ([d.get(k) for k in pop_fields], d["_sources"].get("pop")) for d in expanded["daily"]
        ]
        results[scenario] = {"total": size, "increase": {k: v - start[k] for k, v in size.items()}}
    print(json.dumps(results, indent=2))
