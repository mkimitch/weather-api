"""Independent NWS station measurements; null precipitation is never dry weather."""
import logging
import math
import os
import re
from datetime import timedelta
from urllib.parse import urlparse

import requests

from history import claim_job, database, encode, finish_job
from timeutils import day_bounds, instant, local_noon, utcnow

logger = logging.getLogger("observations")
NWS_ROOT = "https://api.weather.gov"


def quantity(value, kind):
    value = value or {}
    number, unit = value.get("value"), value.get("unitCode", "").split(":")[-1]
    if value.get("qualityControl") not in ("V", "C", "S"):
        return None
    if not isinstance(number, (int, float)) or isinstance(number, bool) or not math.isfinite(number):
        return None
    conversions = {
        "temperature": {"degC": lambda x: x, "degF": lambda x: (x - 32) * 5 / 9, "K": lambda x: x - 273.15},
        "wind": {"km_h-1": lambda x: x, "m_s-1": lambda x: x * 3.6, "kn": lambda x: x * 1.852},
        "precip": {"mm": lambda x: x, "m": lambda x: x * 1000, "in": lambda x: x * 25.4},
    }
    conversion = conversions[kind].get(unit)
    result = conversion(number) if conversion else None
    if result is not None and ((kind == "temperature" and not -100 <= result <= 70) or (kind != "temperature" and result < 0)):
        return None
    return result


def normalize_observation(feature, station, retrieved):
    p = feature.get("properties", {})
    if p.get("station", "").rstrip("/").split("/")[-1] != station:
        raise ValueError("Observation station mismatch")
    at = instant(p["timestamp"])
    if at > retrieved + timedelta(minutes=5):
        raise ValueError("Future observation")
    return {"time": at.isoformat(), "station": station, "source": "NWS station observations",
            "kind": "measured", "location": feature.get("geometry"),
            "tempC": quantity(p.get("temperature"), "temperature"),
            "windKph": quantity(p.get("windSpeed"), "wind"),
            "precipLastHourMm": quantity(p.get("precipitationLastHour"), "precip"),
            "precipPeriod": {"start": (at - timedelta(hours=1)).isoformat(), "end": at.isoformat()},
            "raw": {k: p.get(k) for k in ("temperature", "windSpeed", "precipitationLastHour", "presentWeather", "rawMessage")}}


def ingest_observations(config, station, features, retrieved=None):
    retrieved = retrieved or utcnow()
    accepted = rejected = 0
    with database(config) as db:
        for feature in features:
            try:
                row = normalize_observation(feature, station, retrieved)
            except (KeyError, TypeError, ValueError):
                rejected += 1
                continue
            # NWS may correct a reading; use its latest version, keeping original units/QC.
            db.execute("INSERT INTO observations VALUES(?,?,?,?) ON CONFLICT(station,time) DO UPDATE SET "
                       "retrieved_at=excluded.retrieved_at,data=excluded.data WHERE excluded.retrieved_at>=observations.retrieved_at",
                       (station, row["time"], retrieved.isoformat(), encode(row)))
            accepted += 1
    return accepted, rejected


def collect_observations(config, now=None):
    if not config.get("history_enabled", True) or not config.get("observations_enabled", True):
        return
    now = now or utcnow()
    stations = {loc["observation_station"] for loc in config.get("locations", []) if loc.get("observation_station")}
    for station in sorted(stations):
        if not re.fullmatch(r"[A-Z0-9]{3,8}", station):
            raise ValueError("Invalid NWS station identifier")
        job = "observations:" + station
        if not claim_job(config, job, config.get("observation_interval_minutes", 60), now):
            continue
        try:
            user_agent = os.getenv("NWS_USER_AGENT", config.get("nws_user_agent", ""))
            if not user_agent:
                raise ValueError("NWS_USER_AGENT contact information is required")
            lookback = min(7, max(1, int(config.get("observation_lookback_days", 3))))
            params = {"start": (now - timedelta(days=lookback)).isoformat(), "end": now.isoformat(), "limit": 500}
            url = f"{NWS_ROOT}/stations/{station}/observations"
            accepted = rejected = 0
            pages = 0
            while url and pages < 12:
                parsed = urlparse(url)
                if parsed.scheme != "https" or parsed.netloc != "api.weather.gov" or parsed.path != f"/stations/{station}/observations":
                    raise ValueError("Unexpected NWS pagination URL")
                response = requests.get(url, params=params, headers={"User-Agent": user_agent, "Accept": "application/geo+json"},
                                        timeout=20, allow_redirects=False)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload.get("features"), list):
                    raise ValueError("Invalid NWS feature collection")
                good, bad = ingest_observations(config, station, payload["features"], now)
                accepted += good
                rejected += bad
                url = (payload.get("pagination") or {}).get("next")
                params = None
                pages += 1
            status = "partial" if url or rejected else "ok" if accepted else "empty"
            finish_job(config, job, status, f"accepted={accepted}, rejected={rejected}, pages={pages}", now)
        except Exception as error:
            finish_job(config, job, "failed", type(error).__name__, now)
            logger.warning("NWS collection failed for %s (%s)", station, type(error).__name__)


def daily_observation(rows, day, zone, now=None):
    """Derived sampled extrema; conservative exact tiling of precipitation periods."""
    start, end = day_bounds(day, zone)
    now = now or utcnow()
    points = sorted((instant(row["time"]), row) for row in rows if start <= instant(row["time"]) < end)
    temperatures = [(at, row["tempC"]) for at, row in points if row.get("tempC") is not None]
    hours = (end - start).total_seconds() / 3600
    covered = len({int((at - start).total_seconds() // 3600) for at, _ in temperatures})
    times = [start] + [at for at, _ in temperatures] + [end]
    gap = max((b - a).total_seconds() / 60 for a, b in zip(times, times[1:]))
    complete = now >= end + timedelta(hours=6) and covered / hours >= .9 and gap <= 90
    # Accept only nonoverlapping complete hourly measurement intervals on the day boundary.
    precipitation = {instant(r["time"]): r["precipLastHourMm"] for r in rows
                     if r.get("precipLastHourMm") is not None and start < instant(r["time"]) <= end}
    totals = [precipitation.get(start + timedelta(hours=i)) for i in range(1, int(hours) + 1)]
    precip_complete = now >= end + timedelta(hours=6) and all(v is not None for v in totals)
    noon = local_noon(day, zone)
    winds = [(abs((at - noon).total_seconds()), at, row["windKph"])
             for at, row in points if row.get("windKph") is not None and abs((at - noon).total_seconds()) <= 600]
    wind = min(winds) if winds and now >= end + timedelta(hours=6) else None
    return {"kind": "derived_from_measurements", "date": day, "validStart": start.isoformat(), "validEnd": end.isoformat(),
            "tempMinC": min(v for _, v in temperatures) if complete else None,
            "tempMaxC": max(v for _, v in temperatures) if complete else None,
            "noonWindKph": wind[2] if wind else None, "windObservedAt": wind[1].isoformat() if wind else None,
            "precipMm": sum(totals) if precip_complete else None,
            "coverage": {"expectedHours": hours, "temperatureHours": covered, "temperatureFraction": covered / hours,
                         "maxTemperatureGapMinutes": gap, "temperatureComplete": complete,
                         "precipitationHours": sum(v is not None for v in totals), "precipitationComplete": precip_complete},
            "limitations": ["Sampled extrema can miss the true daily extremes.",
                            "Station measurements are a spatial proxy for the home.",
                            "Daily condition codes have no comparable measured daily event definition."]}
