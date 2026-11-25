"""SQLite forecast archive and shared budgets; no request URLs, headers or keys stored."""
import hashlib
import json
import math
import os
import sqlite3
import zlib
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path

from timeutils import day_bounds, instant, local_noon, utcnow

SCHEMA_VERSION = 2
PROVIDERS = ("openweather", "tomorrow")
SCHEMA = """
CREATE TABLE IF NOT EXISTS responses (
 id TEXT PRIMARY KEY, location TEXT NOT NULL, identity TEXT NOT NULL,
 provider TEXT NOT NULL, retrieved_at TEXT NOT NULL, issued_at TEXT,
 source TEXT NOT NULL, timezone TEXT, lat REAL, lon REAL, payload BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS responses_location_time ON responses(location, retrieved_at);
CREATE INDEX IF NOT EXISTS responses_identity_time ON responses(identity, retrieved_at);
CREATE TABLE IF NOT EXISTS forecasts (
 response_id TEXT NOT NULL REFERENCES responses(id) ON DELETE CASCADE,
 target_date TEXT NOT NULL, valid_start TEXT, valid_end TEXT, lead_hours REAL,
 data TEXT NOT NULL, PRIMARY KEY(response_id, target_date)
);
CREATE INDEX IF NOT EXISTS forecasts_date ON forecasts(target_date);
CREATE TABLE IF NOT EXISTS attempts (
 id INTEGER PRIMARY KEY, provider TEXT NOT NULL, location TEXT NOT NULL,
 at TEXT NOT NULL, status TEXT NOT NULL, daily_count INTEGER NOT NULL DEFAULT 0,
 incomplete_days INTEGER NOT NULL DEFAULT 0, request_type TEXT, identity TEXT
);
CREATE INDEX IF NOT EXISTS attempts_time ON attempts(provider, at);
CREATE TABLE IF NOT EXISTS observations (
 station TEXT NOT NULL, time TEXT NOT NULL, retrieved_at TEXT NOT NULL,
 data TEXT NOT NULL, PRIMARY KEY(station, time)
);
CREATE TABLE IF NOT EXISTS jobs (
 name TEXT PRIMARY KEY, attempted_at TEXT NOT NULL, succeeded_at TEXT,
 status TEXT NOT NULL, detail TEXT
);
"""


def db_path(config):
    return os.getenv("WEATHER_HISTORY_DB", config.get("history_db", str(Path(config.get("cache_dir", "./weather_cache")) / "history.sqlite3")))


@contextmanager
def database(config):
    path = db_path(config)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=10)
    db.row_factory = sqlite3.Row
    try:
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA journal_mode=WAL")
        version = db.execute("PRAGMA user_version").fetchone()[0]
        if version < SCHEMA_VERSION:
            with db:
                db.execute("BEGIN IMMEDIATE")
                version = db.execute("PRAGMA user_version").fetchone()[0]
                if version == 0:
                    for statement in SCHEMA.strip().rstrip(";").split(";"):
                        db.execute(statement)
                if version == 1:
                    # Older attempts still count toward quota. Their request type
                    # and point cannot be recovered for forecast reliability.
                    db.execute("ALTER TABLE attempts ADD COLUMN request_type TEXT")
                    db.execute("ALTER TABLE attempts ADD COLUMN identity TEXT")
                if version < SCHEMA_VERSION:
                    db.execute("CREATE INDEX IF NOT EXISTS attempts_forecasts ON attempts(request_type, identity, provider, at)")
                    db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        if version > SCHEMA_VERSION:
            raise RuntimeError("History schema is newer than this application")
        with db:
            yield db
    finally:
        db.close()


def clean(value):
    if isinstance(value, dict):
        return {k: clean(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [clean(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def encode(value):
    return json.dumps(clean(value), sort_keys=True, separators=(",", ":"), allow_nan=False)


def location_identity(location, zone=None):
    return encode({"name": location["name"], "lat": float(location["lat"]) if location.get("lat") is not None else None,
                   "lon": float(location["lon"]) if location.get("lon") is not None else None,
                   "query": location.get("query"), "timezone": zone or location.get("timezone")})


def reserve_attempt(config, provider, location, now=None, *, request_type="forecast", identity=None):
    """Count attempts atomically across scheduler, HTTP, processes, and restarts."""
    now = now or utcnow()
    limits = [(86400, config.get(f"max_calls_per_24h_{provider}", config.get("max_calls_per_24h", 900) if provider == "openweather" else 500))]
    if provider == "tomorrow":
        limits += [(3600, config.get("max_calls_per_hour_tomorrow", 25)),
                   (1, config.get("max_calls_per_second_tomorrow", 3))]
    with database(config) as db:
        db.execute("BEGIN IMMEDIATE")
        for seconds, limit in limits:
            count = db.execute("SELECT COUNT(*) FROM attempts WHERE provider=? AND at>?",
                               (provider, (now - timedelta(seconds=seconds)).isoformat())).fetchone()[0]
            if count >= int(limit):
                return None
        return db.execute("INSERT INTO attempts(provider,location,at,status,request_type,identity) "
                          "VALUES(?,?,?,'pending',?,?)",
                          (provider, location, now.isoformat(), request_type, identity)).lastrowid


def finish_attempt(config, attempt, status, normalized=None):
    daily = (normalized or {}).get("daily", [])
    incomplete = sum(not all((d.get("tempC") or {}).get(k) is not None for k in ("min", "max"))
                     or d.get("pop") is None or d.get("_period", {}).get("complete") is False for d in daily)
    with database(config) as db:
        db.execute("UPDATE attempts SET status=?,daily_count=?,incomplete_days=? WHERE id=?",
                   (status, len(daily), incomplete, attempt))


# Archive only weather payload branches; omit provider error messages and arbitrary metadata.
OWM_FIELDS = {"dt", "temp", "feels_like", "humidity", "dew_point", "pressure", "wind_speed", "wind_gust",
              "wind_deg", "visibility", "uvi", "clouds", "pop", "rain", "snow", "weather", "sunrise",
              "sunset", "moonrise", "moonset", "moon_phase"}
TOMORROW_FIELDS = {"temperature", "temperatureMin", "temperatureMax", "temperatureApparent", "humidity",
                   "dewPoint", "pressureSurfaceLevel", "windSpeed", "windGust", "windDirection", "visibility",
                   "uvIndex", "cloudCover", "precipitationProbability", "precipitationProbabilityAvg",
                   "precipitationProbabilityMax", "rainIntensity", "snowIntensity", "sleetIntensity", "weatherCode"}


def raw_weather(provider, raw):
    if provider == "openweather":
        return {kind: [{k: v for k, v in record.items() if k in OWM_FIELDS} for record in raw.get(kind, [])]
                for kind in ("daily", "hourly")}
    return {"timelines": [{"timestep": t.get("timestep"), "intervals": [
        {"startTime": i.get("startTime"), "values": {k: v for k, v in i.get("values", {}).items() if k in TOMORROW_FIELDS}}
        for i in t.get("intervals", [])]} for t in raw.get("data", {}).get("timelines", [])]}


def known_timezone(config, location):
    """Reuse previously learned IANA metadata only for the same configured point."""
    with database(config) as db:
        row = db.execute("SELECT timezone FROM responses WHERE location=? AND lat=? AND lon=? AND timezone IS NOT NULL "
                         "ORDER BY retrieved_at DESC LIMIT 1", (location["name"], location.get("lat"), location.get("lon"))).fetchone()
        return row[0] if row else None


def record_forecast(config, location, provider, raw, normalized):
    if not config.get("history_enabled", True):
        return
    retrieved = normalized["updatedAt"]
    zone = normalized.get("timezone") or location.get("timezone")
    identity = location_identity(location, zone)
    response_id = hashlib.sha256(encode([identity, provider, retrieved]).encode()).hexdigest()
    payload = {"originalUnits": "metric", "normalizedUnits": {"temperature": "C", "wind": "km/h", "precipitation": "mm"},
               "raw": raw_weather(provider, raw), "normalized": normalized,
               "issuance": "unknown; retrieved_at is the time this service obtained the prediction"}
    with database(config) as db:
        db.execute("INSERT OR IGNORE INTO responses VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                   (response_id, location["name"], identity, provider, retrieved, None,
                    "OWM One Call 3.0" if provider == "openweather" else "Tomorrow Timelines v4",
                    zone, location.get("lat"), location.get("lon"), zlib.compress(encode(payload).encode())))
        hours = {h.get("time"): h for h in normalized.get("hourly", [])}
        for day in normalized.get("daily", []):
            start, end = day_bounds(day["date"], zone) if zone else (None, None)
            data = dict(day)
            # A defined instant supports wind comparison without assuming daily wind is a mean.
            noon = local_noon(day["date"], zone) if zone else None
            data["noonWind"] = {"time": noon.isoformat(), "windKph": hours.get(noon.isoformat(), {}).get("windKph")} if noon else None
            db.execute("INSERT OR IGNORE INTO forecasts VALUES(?,?,?,?,?,?)",
                       (response_id, day["date"], start.isoformat() if start else None,
                        end.isoformat() if end else None,
                        (start - instant(retrieved)).total_seconds() / 3600 if start else None, encode(data)))


def claim_job(config, name, interval_minutes, now=None):
    now = now or utcnow()
    with database(config) as db:
        db.execute("BEGIN IMMEDIATE")
        previous = db.execute("SELECT attempted_at FROM jobs WHERE name=?", (name,)).fetchone()
        if previous and (now - instant(previous[0])).total_seconds() < interval_minutes * 60:
            return False
        db.execute("INSERT INTO jobs(name,attempted_at,status) VALUES(?,?,'running') "
                   "ON CONFLICT(name) DO UPDATE SET attempted_at=excluded.attempted_at,status='running',detail=NULL",
                   (name, now.isoformat()))
        return True


def finish_job(config, name, status, detail=None, now=None):
    with database(config) as db:
        db.execute("UPDATE jobs SET status=?,detail=?,succeeded_at=CASE WHEN ?='ok' THEN ? ELSE succeeded_at END WHERE name=?",
                   (status, detail, status, (now or utcnow()).isoformat(), name))


def prune(config, now=None):
    """Remove only this feature's expired rows; SQLite reuses freed pages."""
    now = now or utcnow()
    days = int(config.get("history_retention_days", 400))
    if days < 2:
        raise ValueError("history_retention_days must be >= 2")
    cutoff = (now - timedelta(days=days)).isoformat()
    with database(config) as db:
        db.execute("DELETE FROM responses WHERE retrieved_at<?", (cutoff,))
        db.execute("DELETE FROM observations WHERE time<?", (cutoff,))
        db.execute("DELETE FROM attempts WHERE at<?", (cutoff,))


def health(config):
    with database(config) as db:
        return {"schemaVersion": SCHEMA_VERSION, "enabled": config.get("history_enabled", True),
                "automaticSwitching": False,
                "jobs": [dict(r) for r in db.execute("SELECT * FROM jobs")],
                "latestForecast": db.execute("SELECT MAX(retrieved_at) FROM responses").fetchone()[0],
                "latestObservation": db.execute("SELECT MAX(time) FROM observations").fetchone()[0]}
