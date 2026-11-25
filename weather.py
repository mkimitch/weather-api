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
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests

from cache import save_weather
from timeutils import day_bounds, instant, utcnow

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
    if not usable(value):
        return None
    try:
        value = float(value)
    except (ValueError, TypeError):
        return None
    return value if 0 <= value <= 1 else None


def pop_from_percent(value: Optional[float]) -> Optional[float]:
    try:
        return None if not usable(value) else pop_from_unit_interval(float(value) / 100)
    except (ValueError, TypeError):
        return None


TOMORROW_POP_FIELDS = (
    "precipitationProbabilityAvg",
    "precipitationProbability",
    "precipitationProbabilityMax",
)


def tomorrow_pop_basis(values: Dict[str, Any]) -> Optional[str]:
    return next((field for field in TOMORROW_POP_FIELDS
                 if pop_from_percent(values.get(field)) is not None), None)


def pop_from_tomorrow_daily(values: Dict[str, Any]) -> Optional[float]:
    # Preserve the existing average-first fallback, but label its semantics.
    # An average of interval probabilities is not probability of any rain that day.
    field = tomorrow_pop_basis(values)
    return pop_from_percent(values[field]) if field else None


# ----------------------------
# Normalize: OpenWeather → uni
# ----------------------------
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
    timezone_name = timezone_name or raw.get("timezone")
    current = None
    rc = raw.get("current") or {}
    if rc:
        current = {
            "time": to_iso(rc.get("dt")),
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
            "condition": (rc.get("weather") or [{}])[0]
            and {
                "code": (rc.get("weather") or [{}])[0].get("id"),
                "main": (rc.get("weather") or [{}])[0].get("main"),
                "desc": (rc.get("weather") or [{}])[0].get("description"),
                "icon": (rc.get("weather") or [{}])[0].get("icon"),
            },
            "_src": "openweather",
        }

    hourly = []
    for h in raw.get("hourly") or []:
        hourly.append(
            {
                "time": to_iso(h.get("dt")),
                "tempC": h.get("temp"),
                "feelsLikeC": h.get("feels_like"),
                "pop": pop_from_unit_interval(h.get("pop")),
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
            "condition": (d.get("weather") or [{}])[0]
            and {
                "code": (d.get("weather") or [{}])[0].get("id"),
                "main": (d.get("weather") or [{}])[0].get("main"),
                "desc": (d.get("weather") or [{}])[0].get("description"),
                "icon": (d.get("weather") or [{}])[0].get("icon"),
            },
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
        if step.lower() in str(tl.get("timestep", "")).lower():
            return tl.get("intervals") or []
    return []


def normalize_tomorrow(
    raw: Dict[str, Any], *, timezone_name: Optional[str] = None,
    fetched_at: Optional[str] = None,
) -> Dict[str, Any]:
    loc = raw.get("location")
    lat = lon = None
    if isinstance(loc, dict):
        lat = loc.get("lat")
        lon = loc.get("lon")
    elif isinstance(loc, (list, tuple)) and len(loc) == 2:
        # Timelines uses latitude/longitude arrays.
        lat, lon = loc[0], loc[1]

    current = None
    cur = (
        _get_timeline(raw, "current")
        or _get_timeline(raw, "1m")
        or _get_timeline(raw, "1h")
    )[:1]
    if cur:
        cv = cur[0]["values"]
        current = {
            "time": to_iso(cur[0]["startTime"]),
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
            "cloudCoverPct": cv.get("cloudCover"),
            "precipMmHr": float(cv.get("rainIntensity") or 0)
            + float(cv.get("snowIntensity") or 0)
            + float(cv.get("sleetIntensity") or 0),
            "pop": pop_from_percent(cv.get("precipitationProbability")),
            "condition": {"code": cv.get("weatherCode")},
            "extra": {
                "rainIntensity": cv.get("rainIntensity"),
                "snowIntensity": cv.get("snowIntensity"),
                "sleetIntensity": cv.get("sleetIntensity"),
            },
            "_src": "tomorrow",
        }

    hourly = []
    for h in _get_timeline(raw, "1h"):
        v = h["values"]
        hourly.append(
            {
                "time": to_iso(h.get("startTime")),
                "tempC": v.get("temperature"),
                "feelsLikeC": v.get("temperatureApparent"),
                "pop": pop_from_percent(v.get("precipitationProbability")),
                "precipMm": float(v.get("rainIntensity") or 0)
                + float(v.get("snowIntensity") or 0)
                + float(v.get("sleetIntensity") or 0),
                "windKph": kph_from_ms(v.get("windSpeed")),
                "windGustKph": kph_from_ms(v.get("windGust")),
                "windDeg": v.get("windDirection"),
                "cloudCoverPct": v.get("cloudCover"),
                "uv": v.get("uvIndex"),
                "_src": "tomorrow",
            }
        )

    daily = []
    for d in _get_timeline(raw, "1d"):
        date = local_date(d.get("startTime"), timezone_name)
        if date is None:
            continue
        v = d["values"]
        daily.append(
            {
                "date": date,
                "_sourceStart": to_iso(d.get("startTime")),
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
                "condition": {"code": v.get("weatherCode")},
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


def merge_fields(preferred: Dict[str, Any], fallback: Dict[str, Any],
                 preferred_source: str, fallback_source: str) -> Dict[str, Any]:
    """Select each usable field; never let a null record field mask fallback data.

    Conditions stay atomic: provider weather-code namespaces cannot be combined.
    Temperature extrema have independent fallback and independent provenance.
    """
    result, sources = {}, {}
    for field in sorted(set(preferred) | set(fallback)):
        if field.startswith("_") or field in ("popBasis", "precipBasis", "precipComponents"):
            continue
        p, f = preferred.get(field), fallback.get(field)
        if field in {"pop", "windKph", "windGustKph", "windDeg", "uv", "humidity", "pressureHpa", "precipMm", "precipMmHr", "min", "max", "day", "night", "morn", "eve"}:
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
    if "pop" in result and ("popBasis" in preferred or "popBasis" in fallback):
        record = preferred if sources["pop"] == preferred_source else fallback
        result["popBasis"] = record.get("popBasis") if sources["pop"] else None
        sources["popBasis"] = sources["pop"]
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


def daily_disagreements(date: str, a: Dict[str, Any], b: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = []
    for field, threshold in (("tempC.max", 5.5), ("tempC.min", 5.5), ("pop", 0.5)):
        av = (a.get("tempC") or {}).get(field[6:]) if field.startswith("tempC.") else a.get(field)
        bv = (b.get("tempC") or {}).get(field[6:]) if field.startswith("tempC.") else b.get(field)
        if usable(av) and usable(bv) and abs(av - bv) >= threshold:
            result.append({"date": date, "field": field, "openweather": av, "tomorrow": bv,
                           "difference": round(abs(av - bv), 4),
                           "comparable": field != "pop" or a.get("popBasis") == b.get("popBasis"),
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
            current = merge_fields(ca, cb, "openweather", "tomorrow")
        else:
            current = merge_fields(cb, ca, "tomorrow", "openweather")

    # Hourly: Tomorrow preference, OWM fallback. Zero POP/precip remain zero.
    ha = {x["time"]: x for x in owm.get("hourly", []) if x.get("time")}
    hb = {x["time"]: x for x in tmr.get("hourly", []) if x.get("time")}
    hourly = [merge_fields(hb.get(t, {}), ha.get(t, {}), "tomorrow", "openweather")
              for t in sorted(ha.keys() | hb.keys())]

    # Daily: configured priority, restricted to fresh, complete returned periods.
    # Never ensemble daily POP with Tomorrow's average/max interval probabilities.
    da = {x["date"]: x for x in owm.get("daily", []) if x.get("date")}
    db = {x["date"]: x for x in tmr.get("daily", []) if x.get("date")}
    daily, disagreements = [], []
    for date in sorted(da.keys() | db.keys()):
        a, b = da.get(date, {}), db.get(date, {})
        candidates = {"openweather": a, "tomorrow": b}
        fetched = {"openweather": owm.get("updatedAt"), "tomorrow": tmr.get("updatedAt")}
        eligible = {p: eligible_daily(candidates[p], fetched[p],
                    config.get("daily_max_age_minutes", 90), now) for p in order}
        if not any(eligible.values()):
            continue
        day = merge_fields(eligible[order[0]], eligible[order[1]], *order)
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
        day["_providers"] = {
            provider: {
                "tempC": {k: v if usable(v) else None for k, v in (record.get("tempC") or {}).items()},
                "pop": record.get("pop") if usable(record.get("pop")) else None,
                "popBasis": record.get("popBasis"),
                "extra": record.get("extra"),
                "period": record.get("_period"),
                "eligible": bool(eligible[provider]),
            }
            for provider, record in (("openweather", a), ("tomorrow", b)) if record
        }
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
            "selectionPolicy": {"dailyPriority": order, "automaticSwitching": False},
        },
    }


# ---------------------------
# Public API for this module
# ---------------------------
def has_weather(data: Dict[str, Any]) -> bool:
    records = [data.get("current") or {}] + data.get("daily", []) + data.get("hourly", [])
    return any(usable(record.get(field)) for record in records for field in ("tempC", "pop"))


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
        lat, lon = get_latlon_from_city(location["query"], owm_key)
    elif location["type"] == "latlon":
        lat, lon = float(location["lat"]), float(location["lon"])
    else:
        raise ValueError(f"Unknown location type: {location['type']}")

    # Determine which providers are enabled this call (based on config and key presence)
    enabled_owm = bool(config.get("enable_openweather", True)) and bool(owm_key)
    enabled_tmr = bool(config.get("enable_tomorrow", bool(tmr_key))) and bool(tmr_key)

    # Fetch providers with graceful fallback: proceed even if one fails
    status = {"openweather": "disabled", "tomorrow": "disabled"}
    owm_uni: Dict[str, Any] = {}
    if enabled_owm:
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
            logger.warning("OpenWeather fetch failed for %s (%s, HTTP %s)",
                           location["name"], type(e).__name__, getattr(getattr(e, "response", None), "status_code", None))
        else:
            owm_uni = candidate
            timezone_name = timezone_name or owm_uni.get("timezone")
            status["openweather"] = "ok"

    tmr_uni: Dict[str, Any] = {}
    if enabled_tmr:
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
            logger.warning("Tomorrow.io fetch failed for %s (%s, HTTP %s)",
                           location["name"], type(e).__name__, getattr(getattr(e, "response", None), "status_code", None))
        else:
            tmr_uni = candidate
            status["tomorrow"] = "ok"

    if not owm_uni and not tmr_uni:
        # Both providers failed; let caller decide how to handle (usually serve stale cache)
        raise RuntimeError(
            f"All providers failed for {location['name']} (lat={lat}, lon={lon})."
        )

    merged = merge_weather(owm_uni or {}, tmr_uni or {}, config)
    merged["lat"], merged["lon"] = lat, lon
    merged["timezone"] = timezone_name or merged.get("timezone")
    merged["extra"]["providerStatus"] = status
    return save_weather(location["name"], merged)
