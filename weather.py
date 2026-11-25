"""
Weather fetching, normalization, merging, and caching for the Weather API Caching Proxy.

This module talks to BOTH OpenWeather (One Call 3.0) and Tomorrow.io (Timelines v4),
normalizes their responses to a unified shape, merges them deterministically,
and writes the merged result to cache.

Free-tier friendly:
- OpenWeather: alerts + sun/moon (astronomy) included
- Tomorrow.io: richer precip split (rain/snow/sleet), flexible fields
"""

import logging
import os
import math
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests

from cache import save_weather
from timeutils import day_bounds, instant, utcnow
from conditions import (
    SCHEMA_VERSION, TOMORROW_SOLAR_FIELDS, cloud_cover_percent, coverage, interval_coverage, simple_value,
    openweather_condition, selectable, text_value, tomorrow_condition,
)
from precipitation import DAILY_POP_RANK, compare_probability, covers_time, probability_period
from history import finish_attempt, known_timezone, location_identity, record_forecast, reserve_attempt

# --- Endpoints ---
ONECALL_URL = "https://api.openweathermap.org/data/3.0/onecall"
GEOCODE_URL = "https://api.openweathermap.org/geo/1.0/direct"
TOMORROW_TIMELINES_URL = "https://api.tomorrow.io/v4/timelines"

logger = logging.getLogger("weather")


class RateLimitExceeded(Exception):
    """Raised by scheduler when the rate limit is exceeded."""


# -----------------------
# Geocoding (OpenWeather)
# -----------------------
def get_latlon_from_city(city: str, owm_api_key: str) -> Tuple[float, float]:
    """Use OpenWeather geocoding API to get latitude and longitude for a city name."""
    resp = requests.get(
        GEOCODE_URL,
        params={"q": city, "appid": owm_api_key, "limit": 1},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data:
        raise ValueError(f"City not found: {city}")
    return float(data[0]["lat"]), float(data[0]["lon"])


# -------------------------
# Provider: OpenWeatherMap
# -------------------------
def fetch_openweather(
    lat: float, lon: float, api_key: str, *, units: str = "metric"
) -> Dict[str, Any]:
    """
    Fetch OpenWeather One Call 3.0.
    We keep minutely for completeness (you can exclude if you don't need it).
    """
    require_metric(units)
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": units,  # metric => C, m/s, mm
        # "exclude": "minutely",  # uncomment to trim payload
    }
    resp = requests.get(ONECALL_URL, params=params, timeout=12)
    resp.raise_for_status()
    return resp.json()


# ----------------------
# Provider: Tomorrow.io
# ----------------------
def fetch_tomorrow(
    lat: float, lon: float, api_key: str, *, units: str = "metric",
    timezone_name: str = "auto", forecast_days: int = 5,
) -> Dict[str, Any]:
    """
    Fetch Tomorrow.io v4 hourly+daily Timelines in a single POST.
    The first hourly interval supplies the current fallback.
    Tomorrow uses header 'apikey'.
    """
    require_metric(units)
    if not isinstance(forecast_days, int) or not 1 <= forecast_days <= 14:
        raise ValueError("tomorrow_forecast_days must be an integer from 1 to 14")
    fields = [
        "temperature",
        "temperatureApparent",
        "humidity",
        "dewPoint",
        "pressureSurfaceLevel",
        "windSpeed",
        "windGust",
        "windDirection",
        "visibility",
        "uvIndex",
        "cloudCover",
        "precipitationProbability",
        "precipitationProbabilityAvg",
        "precipitationProbabilityMax",
        "rainIntensity",
        "snowIntensity",
        "sleetIntensity",
        "weatherCode",
        "temperatureMin",
        "temperatureMax",
    ]
    body = {
        "location": [lat, lon],
        "fields": fields,
        "units": units,
        "timesteps": ["1h", "1d"],
        "startTime": "now",
        # Timelines relative syntax; free plans support at most +5d.
        "endTime": f"nowPlus{forecast_days}d",
        "timezone": timezone_name,
        # Match calendar-day forecasts, not Tomorrow's default 06:00–06:00.
        "dailyStartHour": 0,
    }
    resp = requests.post(
        TOMORROW_TIMELINES_URL,
        json=body,
        headers={
            "accept": "application/json",
            "content-type": "application/json",
            "apikey": api_key,
        },
        timeout=12,
    )
    # Do not log error bodies or request URLs: they can contain credentials.
    resp.raise_for_status()
    return resp.json()


# ------------------
# Normalize helpers
# ------------------
def kph_from_ms(ms: Optional[float]) -> Optional[float]:
    return round(ms * 3.6, 2) if isinstance(ms, (int, float)) and not isinstance(ms, bool) and math.isfinite(ms) and ms >= 0 else None


def to_iso(ts: Optional[int or str], *, zero_as_none: bool = False) -> Optional[str]:
    if ts is None or (zero_as_none and ts == 0):
        return None
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    return (
        datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        .astimezone(timezone.utc)
        .isoformat()
    )


def mm_from_owm_precip(obj: Optional[Dict[str, Any]]) -> float:
    # OWM rain/snow can be {"1h": mm} or {"3h": mm}; default 0
    if not obj:
        return 0.0
    return float(obj.get("1h") or obj.get("3h") or 0.0)


def daily_owm_precip(day):
    values = [day.get("rain"), day.get("snow")]
    if all(v is None for v in values):
        return None
    if any(v is not None and (not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0) for v in values):
        return None
    return sum(v for v in values if v is not None)


def condition_category(provider, code):
    if not isinstance(code, int):
        return None
    if provider == "openweather":
        if 200 <= code < 300: return "thunderstorm"
        if 300 <= code < 600: return "rain"
        if 600 <= code < 700: return "snow"
        if 700 <= code < 800: return "obscuration"
        if code == 800: return "clear"
        if 801 <= code <= 804: return "cloudy"
    else:
        if code == 1000: return "clear"
        if code in (1001, 1100, 1101, 1102): return "cloudy"
        if code in (2000, 2100): return "obscuration"
        if code in (4000, 4001, 4200, 4201): return "rain"
        if code in (5000, 5001, 5100, 5101): return "snow"
        if code in (6000, 6001, 6200, 6201): return "freezing_rain"
        if code in (7000, 7101, 7102): return "ice_pellets"
        if code == 8000: return "thunderstorm"
    return None


def annotate_daily(records, provider, zone, request_start=None, request_end=None):
    for record in records:
        temps = record.get("tempC") or {}
        for key, value in temps.items():
            if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value) or not -100 <= value <= 70:
                temps[key] = None
        if temps.get("min") is not None and temps.get("max") is not None and temps["min"] > temps["max"]:
            temps["min"] = temps["max"] = None
        condition = record.get("condition") or {}
        if condition:
            condition["category"] = condition_category(provider, condition.get("code"))
        if zone:
            start, end = day_bounds(record["date"], zone)
            # Today's remaining forecast is useful for display, but never scored as a full day.
            complete = not request_end or end <= request_end
            if record.get("_sourceStart") and instant(record["_sourceStart"]) != start:
                complete = False
            record["_period"] = {"start": start.isoformat(), "end": end.isoformat(),
                                 "complete": complete,
                                 "basis": "local_calendar_day",
                                 "sourceStart": record.get("_sourceStart"),
                                 "startedBeforeRetrieval": bool(request_start and start < request_start)}


def require_metric(units: str) -> None:
    if units != "metric":
        raise ValueError("Provider input must use metric units for the canonical API")


def usable(value: Any) -> bool:
    """Missing/nonfinite values fall back; zero is a valid measurement."""
    if value is None or isinstance(value, bool):
        return False
    if isinstance(value, (float, int)):
        return math.isfinite(value)
    if isinstance(value, dict):
        return any(usable(v) for v in value.values())
    return bool(value)


def local_date(ts: Any, timezone_name: Optional[str] = None, offset: int = 0) -> Optional[str]:
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        dt = datetime.fromtimestamp(ts, timezone.utc)
    else:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if timezone_name:
        dt = dt.astimezone(ZoneInfo(timezone_name))
    elif isinstance(ts, (int, float)):
        dt = dt.astimezone(timezone(timedelta(seconds=offset)))
    # For Tomorrow timezone=auto, preserve the provider's returned local offset.
    return dt.date().isoformat()


def pop_from_unit_interval(value: Optional[float]) -> Optional[float]:
    if type(value) in (int, float) and 0 <= value <= 1:
        return float(value)
    return None


def pop_from_percent(value: Optional[float]) -> Optional[float]:
    if type(value) in (int, float) and 0 <= value <= 100:
        return value / 100
    return None


def openweather_legacy_condition(record):
    entries = record.get("weather")
    if not isinstance(entries, list) or not entries or not isinstance(entries[0], dict):
        return None
    entry = entries[0]
    return {"code": simple_value(entry.get("id")), "main": text_value(entry.get("main")),
            "desc": text_value(entry.get("description")), "icon": text_value(entry.get("icon"))}


TOMORROW_POP_FIELDS = (
    "precipitationProbabilityAvg",
    "precipitationProbabilityMax",
    "precipitationProbability",
)


def tomorrow_pop_basis(values: Dict[str, Any]) -> Optional[str]:
    return next((field for field in TOMORROW_POP_FIELDS
                 if pop_from_percent(values.get(field)) is not None), None)


def pop_from_tomorrow_daily(values: Dict[str, Any]) -> Optional[float]:
    # Prefer the documented average, then maximum, to the unknown unsuffixed field.
    # An average of interval probabilities is not probability of any rain that day.
    field = tomorrow_pop_basis(values)
    return pop_from_percent(values[field]) if field else None


# ----------------------------
# Normalize: OpenWeather → uni
# ----------------------------
def interval_probability(value, provider, source_time, fetched_at):
    pop = pop_from_percent(value) if provider == "tomorrow" else pop_from_unit_interval(value)
    period = None
    if pop is not None:
        bounds = interval_coverage(source_time) if provider == "tomorrow" else coverage("hourly")
        period = {**probability_period(bounds, provider, retrieved_at=fetched_at), "sourceTime": source_time}
    return {"pop": pop, "popBasis": "interval" if pop is not None else None, "popPeriod": period}


def normalize_openweather(
    raw: Dict[str, Any], *, timezone_name: Optional[str] = None,
    fetched_at: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Unified shape:
    {
      "lat","lon","timezone","updatedAt",
      "current": {..., "_src":"openweather"},
      "hourly":[...],
      "daily":[...],
      "alerts":[...],
      "astronomy": {...}
    }
    """
    fetched_at = fetched_at or utcnow().isoformat()
    timezone_name = timezone_name or raw.get("timezone")
    current = None
    rc = raw.get("current") or {}
    if rc:
        source_time = to_iso(rc.get("dt"))
        current = {
            "time": source_time,
            "tempC": rc.get("temp"),  # already C (units=metric)
            "feelsLikeC": rc.get("feels_like"),
            "humidity": rc.get("humidity"),
            "dewPointC": rc.get("dew_point"),
            "pressureHpa": rc.get("pressure"),
            "windKph": kph_from_ms(rc.get("wind_speed")),
            "windGustKph": kph_from_ms(rc.get("wind_gust")),
            "windDeg": rc.get("wind_deg"),
            "visibilityKm": (
                (rc.get("visibility") or 0) / 1000
                if rc.get("visibility") is not None
                else None
            ),
            "uv": rc.get("uvi"),
            "cloudCoverPct": rc.get("clouds"),
            "precipMmHr": mm_from_owm_precip(rc.get("rain"))
            + mm_from_owm_precip(rc.get("snow")),
            "pop": None,
            "condition": openweather_legacy_condition(rc),
            "conditions": {"current": openweather_condition(rc, "current", source_time, fetched_at)},
            "_src": "openweather",
        }

    hourly = []
    for h in raw.get("hourly") or []:
        source_time = to_iso(h.get("dt"))
        hourly.append(
            {
                "time": source_time,
                "tempC": h.get("temp"),
                "feelsLikeC": h.get("feels_like"),
                "condition": openweather_legacy_condition(h),
                "conditions": {"hourly": openweather_condition(h, "hourly", source_time, fetched_at)},
                **interval_probability(h.get("pop"), "openweather", source_time, fetched_at),
                "precipMm": mm_from_owm_precip(h.get("rain"))
                + mm_from_owm_precip(h.get("snow")),
                "windKph": kph_from_ms(h.get("wind_speed")),
                "windGustKph": kph_from_ms(h.get("wind_gust")),
                "windDeg": h.get("wind_deg"),
                "cloudCoverPct": h.get("clouds"),
                "uv": h.get("uvi"),
                "_src": "openweather",
            }
        )

    daily = []
    for d in raw.get("daily") or []:
        date = local_date(d.get("dt"), timezone_name, raw.get("timezone_offset", 0))
        if date is None:
            continue
        entry = {
            "date": date,
            "sunrise": to_iso(d.get("sunrise"), zero_as_none=True),
            "sunset": to_iso(d.get("sunset"), zero_as_none=True),
            "moonrise": to_iso(d.get("moonrise"), zero_as_none=True),
            "moonset": to_iso(d.get("moonset"), zero_as_none=True),
            "moonPhase": d.get("moon_phase"),
            "tempC": {
                "min": (d.get("temp") or {}).get("min"),
                "max": (d.get("temp") or {}).get("max"),
                "day": (d.get("temp") or {}).get("day"),
                "night": (d.get("temp") or {}).get("night"),
                "morn": (d.get("temp") or {}).get("morn"),
                "eve": (d.get("temp") or {}).get("eve"),
            },
            "pop": pop_from_unit_interval(d.get("pop")),
            "popBasis": "daily" if pop_from_unit_interval(d.get("pop")) is not None else None,
            "precipMm": daily_owm_precip(d),
            "precipBasis": "owm_daily_rain_plus_snow_mm",
            "precipComponents": {"rainMm": d.get("rain"), "snowMm": d.get("snow")},
            "windKph": kph_from_ms(d.get("wind_speed")),
            "windGustKph": kph_from_ms(d.get("wind_gust")),
            "uv": d.get("uvi"),
            "condition": openweather_legacy_condition(d),
            "conditions": {"fullDay": openweather_condition(d, "fullDay", to_iso(d.get("dt")),
                                                             fetched_at, local_date=date)},
            "popPeriod": probability_period(coverage("unknown", local_date=date), "openweather",
                                            retrieved_at=fetched_at)
            if pop_from_unit_interval(d.get("pop")) is not None else None,
            "_src": "openweather",
        }
        daily.append(entry)

    alerts = []
    for a in raw.get("alerts") or []:
        alerts.append(
            {
                "start": to_iso(a.get("start")),
                "end": to_iso(a.get("end")),
                "title": a.get("event"),
                "description": a.get("description"),
                "issuer": a.get("sender_name"),
                "categories": a.get("tags"),
                "_src": "openweather",
            }
        )

    astronomy = None
    if daily:
        first = daily[0]
        astronomy = {
            "sunrise": first.get("sunrise"),
            "sunset": first.get("sunset"),
            "moonrise": first.get("moonrise"),
            "moonset": first.get("moonset"),
            "moonPhase": first.get("moonPhase"),
            "_src": "openweather",
        }

    annotate_daily(daily, "openweather", timezone_name)
    return {
        "lat": raw.get("lat"),
        "lon": raw.get("lon"),
        "timezone": timezone_name,
        "updatedAt": fetched_at or utcnow().isoformat(),
        "current": current,
        "hourly": hourly,
        "daily": daily,
        "alerts": alerts,
        "astronomy": astronomy,
    }


# ---------------------------
# Normalize: Tomorrow → uni
# ---------------------------
def _get_timeline(raw: Dict[str, Any], step: str) -> List[Dict[str, Any]]:
    timelines = (raw.get("data") or {}).get("timelines") or []
    for tl in timelines:
        if step == tl.get("timestep"):
            return tl.get("intervals") or []
    return []


def normalize_tomorrow(
    raw: Dict[str, Any], *, timezone_name: Optional[str] = None,
    fetched_at: Optional[str] = None,
) -> Dict[str, Any]:
    fetched_at = fetched_at or utcnow().isoformat()
    loc = raw.get("location")
    lat = lon = None
    if isinstance(loc, dict):
        lat = loc.get("lat")
        lon = loc.get("lon")
    elif isinstance(loc, (list, tuple)) and len(loc) == 2:
        # Timelines uses latitude/longitude arrays.
        lat, lon = loc[0], loc[1]

    current = None
    for timestep in ("current", "1m", "1h"):
        intervals = _get_timeline(raw, timestep)
        if intervals:
            break
    if intervals:
        cv = intervals[0].get("values") or {}
        source_time = to_iso(intervals[0].get("startTime"))
        condition_period = interval_coverage(source_time) if timestep == "1h" else coverage("unknown")
        current = {
            "time": source_time,
            "tempC": cv.get("temperature"),
            "feelsLikeC": cv.get("temperatureApparent") if cv.get("temperatureApparent") is not None else cv.get("temperature"),
            "humidity": cv.get("humidity"),
            "dewPointC": cv.get("dewPoint"),
            "pressureHpa": cv.get("pressureSurfaceLevel"),
            "windKph": kph_from_ms(cv.get("windSpeed")),
            "windGustKph": kph_from_ms(cv.get("windGust")),
            "windDeg": cv.get("windDirection"),
            "visibilityKm": cv.get("visibility"),  # metric Tomorrow visibility is km
            "uv": cv.get("uvIndex"),
            "cloudCoverPct": cloud_cover_percent(cv.get("cloudCover")),
            "precipMmHr": float(cv.get("rainIntensity") or 0)
            + float(cv.get("snowIntensity") or 0)
            + float(cv.get("sleetIntensity") or 0),
            **(
                interval_probability(cv.get("precipitationProbability"), "tomorrow", source_time, fetched_at)
                if timestep == "1h" else {"pop": None, "popBasis": None, "popPeriod": None}
            ),
            "condition": {"code": simple_value(cv.get("weatherCode"))},
            "conditions": {"current": tomorrow_condition(cv, "weatherCode", "current", condition_period,
                                                          source_time, fetched_at, timestep)},
            "extra": {
                "rainIntensity": cv.get("rainIntensity"),
                "snowIntensity": cv.get("snowIntensity"),
                "sleetIntensity": cv.get("sleetIntensity"),
            },
            "_src": "tomorrow",
        }

    hourly = []
    for h in _get_timeline(raw, "1h"):
        v = h.get("values") or {}
        source_time = to_iso(h.get("startTime"))
        hourly.append(
            {
                "time": source_time,
                "tempC": v.get("temperature"),
                "feelsLikeC": v.get("temperatureApparent"),
                **interval_probability(v.get("precipitationProbability"), "tomorrow", source_time, fetched_at),
                "conditions": {"hourly": tomorrow_condition(v, "weatherCode", "hourly", interval_coverage(source_time),
                                                             source_time, fetched_at, "1h")},
                "condition": {"code": simple_value(v.get("weatherCode"))},
                "precipMm": float(v.get("rainIntensity") or 0)
                + float(v.get("snowIntensity") or 0)
                + float(v.get("sleetIntensity") or 0),
                "windKph": kph_from_ms(v.get("windSpeed")),
                "windGustKph": kph_from_ms(v.get("windGust")),
                "windDeg": v.get("windDirection"),
                "cloudCoverPct": cloud_cover_percent(v.get("cloudCover")),
                "uv": v.get("uvIndex"),
                "_src": "tomorrow",
            }
        )

    daily = []
    for d in _get_timeline(raw, "1d"):
        date = local_date(d.get("startTime"), timezone_name)
        if date is None:
            continue
        v = d.get("values") or {}
        source_time = to_iso(d.get("startTime"))
        calendar_start, calendar_end = day_bounds(date, timezone_name) if timezone_name else (None, None)
        if calendar_start and source_time == calendar_start.isoformat():
            daily_period = coverage("calendar_day", calendar_start.isoformat(), calendar_end.isoformat(), date)
        else:
            daily_period = coverage("unknown", local_date=date)
        native_daily = tomorrow_condition(v, "weatherCode", "fullDay", daily_period, source_time, fetched_at, "1d")
        condition_candidates = {
            field: tomorrow_condition(v, field, context, coverage(basis, local_date=date),
                                      source_time, fetched_at, "1d")
            for field, (context, basis) in TOMORROW_SOLAR_FIELDS.items()
        }
        conditions = {
            context: condition_candidates[field]
            for field, (context, _) in TOMORROW_SOLAR_FIELDS.items()
        }
        if not selectable(conditions["fullDay"]):
            conditions["fullDay"] = native_daily or conditions["fullDay"]
        condition_candidates["weatherCode"] = native_daily
        daily.append(
            {
                "date": date,
                "_sourceStart": source_time,
                "conditions": conditions,
                "_conditionCandidates": condition_candidates,
                "popPeriod": probability_period(daily_period, "tomorrow", retrieved_at=fetched_at)
                if tomorrow_pop_basis(v) else None,
                "tempC": {
                    "min": v.get("temperatureMin"),
                    "max": v.get("temperatureMax"),
                },
                "pop": pop_from_tomorrow_daily(v),
                "popBasis": tomorrow_pop_basis(v),
                # Daily intensity aggregates are mm/hour, not daily mm totals.
                "precipMm": None,
                "extra": {
                    "precipitationProbability": {
                        field: pop_from_percent(v.get(field)) for field in TOMORROW_POP_FIELDS
                    },
                    "precipIntensityMmHr": {
                        field: v.get(field) for field in ("rainIntensity", "snowIntensity", "sleetIntensity")
                    },
                },
                "windKph": kph_from_ms(v.get("windSpeed")),
                "windGustKph": kph_from_ms(v.get("windGust")),
                "uv": v.get("uvIndex"),
                "condition": {"code": simple_value(v.get("weatherCode"))},
                "_src": "tomorrow",
            }
        )

    annotate_daily(daily, "tomorrow", timezone_name)
    return {
        "lat": lat,
        "lon": lon,
        "timezone": timezone_name,
        "updatedAt": fetched_at or utcnow().isoformat(),
        "current": current,
        "hourly": hourly,
        "daily": daily,
        # Tomorrow free: no native alerts/astronomy
    }


# ----------------
# Merge strategies
# ----------------
def _fresher(a: Optional[str], b: Optional[str]) -> Optional[str]:
    if a and b:
        return "a" if datetime.fromisoformat(a) > datetime.fromisoformat(b) else "b"
    return "a" if a else ("b" if b else None)


def select_probability(candidates, context, displayed_time):
    eligible = []
    for provider, record in candidates:
        value = pop_from_unit_interval(record.get("pop"))
        if value is None:
            continue
        period = record.get("popPeriod")
        if context == "current":
            aligned = covers_time(period, displayed_time) if "popPeriod" in record else (
                displayed_time is not None and record.get("time") == displayed_time
            )
            if not aligned:
                continue
        eligible.append((provider, record, value))
    if not eligible:
        return None, {}, None
    if context == "daily":
        return min(eligible, key=lambda item: DAILY_POP_RANK.get(item[1].get("popBasis"), 3))
    return eligible[0]


def select_conditions(candidates, contexts):
    selected, sources = {}, {}
    for context in contexts:
        selected[context] = sources[context] = None
        for provider, record in candidates:
            condition = (record.get("conditions") or {}).get(context)
            if selectable(condition):
                selected[context], sources[context] = condition, provider
                break
    return selected, sources


def provider_comparisons(candidates, eligible=None):
    return {
        provider: {
            "tempC": {key: value if usable(value) else None for key, value in record["tempC"].items()}
            if isinstance(record.get("tempC"), dict) else (
                record.get("tempC") if usable(record.get("tempC")) else None),
            "condition": record.get("condition"),
            "conditions": record.get("conditions"),
            "conditionCandidates": record.get("_conditionCandidates"),
            "pop": pop_from_unit_interval(record.get("pop")),
            "popBasis": record.get("popBasis"),
            "popPeriod": record.get("popPeriod"),
            "extra": record.get("extra"),
            "period": record.get("_period"),
            "eligible": bool(eligible[provider]) if eligible is not None else True,
        }
        for provider, record in candidates if record
    }


def merge_fields(preferred: Dict[str, Any], fallback: Dict[str, Any],
                 preferred_source: str, fallback_source: str, *, context=None) -> Dict[str, Any]:
    """Select each usable field; never let a null record field mask fallback data.

    Conditions stay atomic: provider weather-code namespaces cannot be combined.
    Temperature extrema have independent fallback and independent provenance.
    """
    result, sources = {}, {}
    candidates = ((preferred_source, preferred), (fallback_source, fallback))
    for field in sorted(set(preferred) | set(fallback)):
        if field.startswith("_") or field in ("pop", "popBasis", "popPeriod", "conditions", "precipBasis", "precipComponents"):
            continue
        p, f = preferred.get(field), fallback.get(field)
        if field in {"windKph", "windGustKph", "windDeg", "uv", "humidity", "pressureHpa", "precipMm", "precipMmHr", "min", "max", "day", "night", "morn", "eve"}:
            p = p if isinstance(p, (int, float)) and not isinstance(p, bool) and math.isfinite(p) else None
            f = f if isinstance(f, (int, float)) and not isinstance(f, bool) and math.isfinite(f) else None
        if field == "tempC" and (isinstance(p, dict) or isinstance(f, dict)):
            temps = merge_fields(p or {}, f or {}, preferred_source, fallback_source)
            result[field] = {k: v for k, v in temps.items() if not k.startswith("_")}
            sources[field] = temps["_sources"]
            continue
        source = preferred_source if usable(p) else fallback_source if usable(f) else None
        result[field] = p if source == preferred_source else f if source else None
        if field not in ("date", "time"):
            sources[field] = source
    if "pop" in preferred or "pop" in fallback:
        provider, record, value = select_probability(candidates, context, result.get("time"))
        for field, field_value in (("pop", value), ("popBasis", record.get("popBasis")),
                                   ("popPeriod", record.get("popPeriod"))):
            result[field], sources[field] = field_value, provider

    contexts = ("fullDay", "daytime", "nighttime") if context == "daily" else (context,)
    if context and ("conditions" in preferred or "conditions" in fallback):
        conditions, condition_sources = select_conditions(candidates, contexts)
        result["conditions"], sources["conditions"] = conditions, condition_sources
        primary = contexts[0]
        provider = condition_sources[primary]
        if provider:
            # Prefer a legacy projection from the same provider when available.
            # Rich-only codes leave the independently selected legacy fallback intact.
            legacy_record = preferred if provider == preferred_source else fallback
            legacy = legacy_record.get("condition")
            if isinstance(legacy, dict) and legacy.get("code") is not None:
                result["condition"], sources["condition"] = legacy, provider

    if "precipMm" in result:
        record = preferred if sources.get("precipMm") == preferred_source else fallback
        for field in ("precipBasis", "precipComponents"):
            if field in preferred or field in fallback:
                result[field] = record.get(field) if sources.get("precipMm") else None
                sources[field] = sources.get("precipMm")
    result["_sources"] = sources
    selected = set()
    for source in sources.values():
        selected.update(source.values() if isinstance(source, dict) else [source])
    selected.discard(None)
    result["_src"] = next(iter(selected)) if len(selected) == 1 else "mixed" if selected else None
    return result


def pop_comparability(a, b):
    if any(pop_from_unit_interval(record.get("pop")) is None for record in (a, b)):
        return None
    return compare_probability(a, b)


def daily_disagreements(date: str, a: Dict[str, Any], b: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = []
    for field, threshold in (("tempC.max", 5.5), ("tempC.min", 5.5), ("pop", 0.5)):
        av = (a.get("tempC") or {}).get(field[6:]) if field.startswith("tempC.") else a.get(field)
        bv = (b.get("tempC") or {}).get(field[6:]) if field.startswith("tempC.") else b.get(field)
        if usable(av) and usable(bv) and abs(av - bv) >= threshold:
            comparison = pop_comparability(a, b) if field == "pop" else None
            result.append({"date": date, "field": field, "openweather": av, "tomorrow": bv,
                           "difference": round(abs(av - bv), 4),
                           "comparable": comparison is None or comparison["status"] == "confirmed_comparable",
                           "comparability": comparison,
                           "bases": [a.get("popBasis"), b.get("popBasis")] if field == "pop" else None})
    return result


def daily_priority(config=None):
    config = config or {}
    order = os.getenv("WEATHER_DAILY_PROVIDER_PRIORITY", config.get("daily_provider_priority", "openweather,tomorrow"))
    order = [x.strip() for x in order.split(",")] if isinstance(order, str) else order
    if not isinstance(order, list) or len(order) != 2 or set(order) != {"tomorrow", "openweather"}:
        raise ValueError("daily_provider_priority must contain tomorrow and openweather exactly once")
    return order


def eligible_daily(record, fetched_at, max_age, now):
    period = record.get("_period", {})
    if period.get("complete") is False or (period.get("end") and instant(period["end"]) <= now):
        return {}
    if fetched_at and (now - instant(fetched_at)).total_seconds() > max_age * 60:
        return {}
    return record


def merge_weather(owm: Dict[str, Any], tmr: Dict[str, Any], config=None) -> Dict[str, Any]:
    config = config or {}
    order = daily_priority(config)
    now = utcnow()

    # Current: freshest wins, Tomorrow on ties, with field-level fallback.
    ca, cb = owm.get("current") or {}, tmr.get("current") or {}
    current = None
    if ca or cb:
        if _fresher(ca.get("time"), cb.get("time")) == "a":
            current = merge_fields(ca, cb, "openweather", "tomorrow", context="current")
        else:
            current = merge_fields(cb, ca, "tomorrow", "openweather", context="current")
        current["_providers"] = provider_comparisons((("openweather", ca), ("tomorrow", cb)))

    # Hourly: Tomorrow preference, OWM fallback. Zero POP/precip remain zero.
    ha = {x["time"]: x for x in owm.get("hourly", []) if x.get("time")}
    hb = {x["time"]: x for x in tmr.get("hourly", []) if x.get("time")}
    hourly = []
    for time in sorted(ha.keys() | hb.keys()):
        a, b = ha.get(time, {}), hb.get(time, {})
        row = merge_fields(b, a, "tomorrow", "openweather", context="hourly")
        row["_providers"] = provider_comparisons((("openweather", a), ("tomorrow", b)))
        hourly.append(row)

    # Daily: configured priority, restricted to fresh, complete returned periods.
    # Never ensemble daily POP with Tomorrow's average/max interval probabilities.
    da = {x["date"]: x for x in owm.get("daily", []) if x.get("date")}
    db = {x["date"]: x for x in tmr.get("daily", []) if x.get("date")}
    daily, disagreements, ineligible_days = [], [], []
    for date in sorted(da.keys() | db.keys()):
        a, b = da.get(date, {}), db.get(date, {})
        candidates = {"openweather": a, "tomorrow": b}
        fetched = {"openweather": owm.get("updatedAt"), "tomorrow": tmr.get("updatedAt")}
        eligible = {p: eligible_daily(candidates[p], fetched[p],
                    config.get("daily_max_age_minutes", 90), now) for p in order}
        if not any(eligible.values()):
            ineligible_days.append({"date": date, "_providers": provider_comparisons(candidates.items(), eligible)})
            continue
        day = merge_fields(eligible[order[0]], eligible[order[1]], *order, context="daily")
        temps = day.get("tempC") or {}
        if temps.get("min") is not None and temps.get("max") is not None and temps["min"] > temps["max"]:
            # Field fallback must not manufacture an impossible high/low pair.
            for provider in order:
                pair = eligible[provider].get("tempC") or {}
                if all(usable(pair.get(k)) for k in ("min", "max")) and pair["min"] <= pair["max"]:
                    for k in ("min", "max"):
                        temps[k] = pair[k]
                        day["_sources"]["tempC"][k] = provider
                    break
            else:
                temps["min"] = temps["max"] = None
                day["_sources"]["tempC"].update(min=None, max=None)
            sources = {v for source in day["_sources"].values() for v in (source.values() if isinstance(source, dict) else [source]) if v}
            day["_src"] = next(iter(sources)) if len(sources) == 1 else "mixed" if sources else None
        # Keep provider alternatives visible even when below warning thresholds.
        day["_providers"] = provider_comparisons(candidates.items(), eligible)
        disagreements.extend(daily_disagreements(date, a, b))
        daily.append(day)

    alerts = []
    for alert in owm.get("alerts") or []:
        key = (alert.get("title"), alert.get("start"), alert.get("end"))
        if key not in {(x.get("title"), x.get("start"), x.get("end")) for x in alerts}:
            alerts.append(alert)

    generated_at = utcnow().isoformat()
    return {
        "lat": owm.get("lat") if owm.get("lat") is not None else tmr.get("lat"),
        "lon": owm.get("lon") if owm.get("lon") is not None else tmr.get("lon"),
        "timezone": owm.get("timezone") or tmr.get("timezone"),
        "updatedAt": generated_at,  # compatibility: time this merge was generated
        "generatedAt": generated_at,
        "conditionSchemaVersion": SCHEMA_VERSION,
        "current": current,
        "hourly": hourly,
        "daily": daily,
        "alerts": alerts,
        "astronomy": owm.get("astronomy"),
        "extra": {
            "owmUpdatedAt": owm.get("updatedAt"),
            "tmrUpdatedAt": tmr.get("updatedAt"),
            "providerFetchedAt": {"openweather": owm.get("updatedAt"), "tomorrow": tmr.get("updatedAt")},
            "dailyDisagreements": disagreements,
            "ineligibleDaily": ineligible_days,
            "selectionPolicy": {"dailyPriority": order, "automaticSwitching": False},
        },
    }


# ---------------------------
# Public API for this module
# ---------------------------
def has_weather(data: Dict[str, Any]) -> bool:
    records = [data.get("current") or {}] + data.get("daily", []) + data.get("hourly", [])
    return any(
        usable(record.get("tempC")) or pop_from_unit_interval(record.get("pop")) is not None
        or any(selectable(condition) for condition in (record.get("conditions") or {}).values())
        for record in records
    )


def record_attempt_outcome(config, attempt, status, normalized=None):
    try:
        finish_attempt(config, attempt, status, normalized)
    except Exception as error:
        # The quota reservation is already durable, even if this row stays pending.
        logger.error("Request outcome write failed (attempt %s, %s)", attempt, type(error).__name__)


def get_weather_for_location(
    location: Dict[str, Any], config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Fetch and cache MERGED weather for a location (city or lat/lon) using both providers.
    - Uses OpenWeather geocoding for city -> lat/lon
    - Calls both providers
    - Normalizes, merges, and saves the merged payload
    """
    daily_priority(config)  # Validate policy before any billable request.
    owm_key = os.getenv("OPENWEATHER_API_KEY", config.get("openweather_api_key"))
    tmr_key = os.getenv("TOMORROW_API_KEY", config.get("tomorrow_api_key"))
    # Legacy config.units cannot change the meaning of tempC/windKph/etc.
    units = "metric"
    timezone_name = location.get("timezone")

    if location["type"] == "city":
        if not owm_key:
            raise ValueError("City geocoding requires OPENWEATHER_API_KEY; prefer configured coordinates")
        attempt = reserve_attempt(config, "openweather", location["name"], request_type="geocode",
                                  identity=location_identity(location))
        if attempt is None:
            raise RateLimitExceeded()
        try:
            lat, lon = get_latlon_from_city(location["query"], owm_key)
        except Exception:
            record_attempt_outcome(config, attempt, "failed")
            raise
        record_attempt_outcome(config, attempt, "geocoded")
    elif location["type"] == "latlon":
        lat, lon = float(location["lat"]), float(location["lon"])
    else:
        raise ValueError(f"Unknown location type: {location['type']}")

    archive_location = dict(location, lat=lat, lon=lon)
    identity = location_identity(archive_location)
    timezone_name = timezone_name or known_timezone(config, archive_location)
    archive_status = {}

    # Determine which providers are enabled this call (based on config and key presence)
    enabled_owm = bool(config.get("enable_openweather", True)) and bool(owm_key)
    enabled_tmr = bool(config.get("enable_tomorrow", bool(tmr_key))) and bool(tmr_key)

    # Fetch providers with graceful fallback: proceed even if one fails
    status = {"openweather": "disabled", "tomorrow": "disabled"}
    owm_uni: Dict[str, Any] = {}
    attempt_owm = reserve_attempt(config, "openweather", location["name"], identity=identity) if enabled_owm else None
    if enabled_owm and attempt_owm is None:
        status["openweather"] = "rate_limited"
    if attempt_owm is not None:
        try:
            request_start = utcnow()
            owm_raw = fetch_openweather(lat, lon, owm_key, units=units)
            fetched_at = utcnow().isoformat()
            candidate = normalize_openweather(owm_raw, timezone_name=timezone_name, fetched_at=fetched_at)
            annotate_daily(candidate["daily"], "openweather", candidate.get("timezone"), request_start)
            if not has_weather(candidate):
                raise ValueError("Empty OpenWeather forecast")
        except Exception as e:
            status["openweather"] = "failed"
            record_attempt_outcome(config, attempt_owm, "failed")
            logger.warning("OpenWeather fetch failed for %s (%s, HTTP %s)",
                           location["name"], type(e).__name__, getattr(getattr(e, "response", None), "status_code", None))
        else:
            owm_uni = candidate
            timezone_name = timezone_name or owm_uni.get("timezone")
            status["openweather"] = "ok"
            record_attempt_outcome(config, attempt_owm, "ok", candidate)
            try:
                record_forecast(config, archive_location, "openweather", owm_raw, candidate)
                archive_status["openweather"] = "stored" if config.get("history_enabled", True) else "disabled"
            except Exception as archive_error:
                archive_status["openweather"] = "failed"
                logger.error("Forecast archive failed (%s)", type(archive_error).__name__)

    tmr_uni: Dict[str, Any] = {}
    try:
        attempt_tmr = reserve_attempt(config, "tomorrow", location["name"], identity=identity) if enabled_tmr else None
    except (OSError, sqlite3.Error) as error:
        if not owm_uni:
            raise
        attempt_tmr = None
        status["tomorrow"] = "quota_unavailable"
        logger.error("Tomorrow.io quota reservation failed (%s)", type(error).__name__)
    else:
        if enabled_tmr and attempt_tmr is None:
            status["tomorrow"] = "rate_limited"
    if attempt_tmr is not None:
        try:
            request_start = utcnow()
            tmr_raw = fetch_tomorrow(lat, lon, tmr_key, units=units,
                                     timezone_name=timezone_name or "auto",
                                     forecast_days=config.get("tomorrow_forecast_days", 5))
            fetched_at = utcnow().isoformat()
            candidate = normalize_tomorrow(tmr_raw, timezone_name=timezone_name, fetched_at=fetched_at)
            horizon_end = request_start + timedelta(days=config.get("tomorrow_forecast_days", 5))
            timeline_ends = [instant(t["endTime"]) for t in (tmr_raw.get("data") or {}).get("timelines", [])
                             if t.get("timestep") == "1d" and t.get("endTime")]
            if timeline_ends:
                horizon_end = min(horizon_end, min(timeline_ends))
            annotate_daily(candidate["daily"], "tomorrow", timezone_name, request_start, horizon_end)
            if not timezone_name:
                for day in candidate["daily"]:
                    day["_period"] = {"complete": False, "basis": "unknown_timezone"}
            if not has_weather(candidate):
                raise ValueError("Empty Tomorrow forecast")
        except Exception as e:
            status["tomorrow"] = "failed"
            record_attempt_outcome(config, attempt_tmr, "failed")
            logger.warning("Tomorrow.io fetch failed for %s (%s, HTTP %s)",
                           location["name"], type(e).__name__, getattr(getattr(e, "response", None), "status_code", None))
        else:
            tmr_uni = candidate
            status["tomorrow"] = "ok"
            record_attempt_outcome(config, attempt_tmr, "ok", candidate)
            try:
                record_forecast(config, archive_location, "tomorrow", tmr_raw, candidate)
                archive_status["tomorrow"] = "stored" if config.get("history_enabled", True) else "disabled"
            except Exception as archive_error:
                archive_status["tomorrow"] = "failed"
                logger.error("Forecast archive failed (%s)", type(archive_error).__name__)

    if not owm_uni and not tmr_uni:
        # Both providers failed; let caller decide how to handle (usually serve stale cache)
        raise RuntimeError(
            f"All providers failed for {location['name']} (lat={lat}, lon={lon})."
        )

    merged = merge_weather(owm_uni or {}, tmr_uni or {}, config)
    if not has_weather(merged):
        raise RuntimeError("No eligible weather to cache.")
    merged["lat"], merged["lon"] = lat, lon
    merged["timezone"] = timezone_name or merged.get("timezone")
    merged["extra"]["providerStatus"] = status
    merged["extra"]["historyStatus"] = archive_status
    return save_weather(location["name"], merged)
