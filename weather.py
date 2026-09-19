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
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests

from cache import save_weather

# --- Endpoints ---
ONECALL_URL = "https://api.openweathermap.org/data/3.0/onecall"
GEOCODE_URL = "http://api.openweathermap.org/geo/1.0/direct"
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
    return None if ms is None else round(ms * 3.6, 2)


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


def require_metric(units: str) -> None:
    if units != "metric":
        raise ValueError("Provider input must use metric units for the canonical API")


def usable(value: Any) -> bool:
    """Missing/nonfinite values fall back; zero is a valid measurement."""
    if value is None:
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
    value = float(value)
    return value if 0 <= value <= 1 else None


def pop_from_percent(value: Optional[float]) -> Optional[float]:
    return None if not usable(value) else pop_from_unit_interval(float(value) / 100)


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
            "precipMm": float(d.get("rain") or 0) + float(d.get("snow") or 0),
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

    return {
        "lat": raw.get("lat"),
        "lon": raw.get("lon"),
        "timezone": timezone_name,
        "updatedAt": fetched_at or datetime.now(timezone.utc).isoformat(),
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
        # Tomorrow echoes [lon, lat] when arrays are used
        lon, lat = loc[0], loc[1]

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

    return {
        "lat": lat,
        "lon": lon,
        "timezone": timezone_name,
        "updatedAt": fetched_at or datetime.now(timezone.utc).isoformat(),
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
        if field.startswith("_") or field == "popBasis":
            continue
        p, f = preferred.get(field), fallback.get(field)
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
                           "difference": round(abs(av - bv), 4)})
    return result


def merge_weather(owm: Dict[str, Any], tmr: Dict[str, Any]) -> Dict[str, Any]:
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

    # Daily: OWM has eight days and a native daily POP. Prefer it consistently
    # across the horizon, using Tomorrow per field only when OWM is missing.
    # Never ensemble daily POP with Tomorrow's average/max interval probabilities.
    da = {x["date"]: x for x in owm.get("daily", []) if x.get("date")}
    db = {x["date"]: x for x in tmr.get("daily", []) if x.get("date")}
    daily, disagreements = [], []
    for date in sorted(da.keys() | db.keys()):
        a, b = da.get(date, {}), db.get(date, {})
        day = merge_fields(a, b, "openweather", "tomorrow")
        # Keep provider alternatives visible even when below warning thresholds.
        day["_providers"] = {
            provider: {
                "tempC": {k: v if usable(v) else None for k, v in (record.get("tempC") or {}).items()},
                "pop": record.get("pop") if usable(record.get("pop")) else None,
                "popBasis": record.get("popBasis"),
                "extra": record.get("extra"),
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

    generated_at = datetime.now(timezone.utc).isoformat()
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
    owm_key = config.get("openweather_api_key")
    tmr_key = config.get("tomorrow_api_key")
    # Legacy config.units cannot change the meaning of tempC/windKph/etc.
    units = "metric"
    timezone_name = location.get("timezone")

    if location["type"] == "city":
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
            owm_raw = fetch_openweather(lat, lon, owm_key, units=units)
            fetched_at = datetime.now(timezone.utc).isoformat()
            candidate = normalize_openweather(owm_raw, timezone_name=timezone_name, fetched_at=fetched_at)
            if not has_weather(candidate):
                raise ValueError("Empty OpenWeather forecast")
            owm_uni = candidate
            timezone_name = timezone_name or owm_uni.get("timezone")
            status["openweather"] = "ok"
        except Exception as e:
            status["openweather"] = "failed"
            logger.warning("OpenWeather fetch failed for %s (%s, HTTP %s)",
                           location["name"], type(e).__name__, getattr(getattr(e, "response", None), "status_code", None))

    tmr_uni: Dict[str, Any] = {}
    if enabled_tmr:
        try:
            tmr_raw = fetch_tomorrow(lat, lon, tmr_key, units=units,
                                     timezone_name=timezone_name or "auto",
                                     forecast_days=config.get("tomorrow_forecast_days", 5))
            fetched_at = datetime.now(timezone.utc).isoformat()
            candidate = normalize_tomorrow(tmr_raw, timezone_name=timezone_name, fetched_at=fetched_at)
            if not has_weather(candidate):
                raise ValueError("Empty Tomorrow forecast")
            tmr_uni = candidate
            status["tomorrow"] = "ok"
        except Exception as e:
            status["tomorrow"] = "failed"
            logger.warning("Tomorrow.io fetch failed for %s (%s, HTTP %s)",
                           location["name"], type(e).__name__, getattr(getattr(e, "response", None), "status_code", None))

    if not owm_uni and not tmr_uni:
        # Both providers failed; let caller decide how to handle (usually serve stale cache)
        raise RuntimeError(
            f"All providers failed for {location['name']} (lat={lat}, lon={lon})."
        )

    merged = merge_weather(owm_uni or {}, tmr_uni or {})
    merged["lat"], merged["lon"] = lat, lon
    merged["timezone"] = timezone_name or merged.get("timezone")
    merged["extra"]["providerStatus"] = status
    return save_weather(location["name"], merged)
