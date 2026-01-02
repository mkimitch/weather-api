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
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

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
    lat: float, lon: float, api_key: str, *, units: str = "metric"
) -> Dict[str, Any]:
    """
    Fetch Tomorrow.io v4 Timelines for current+hourly+daily in a single POST.
    Tomorrow uses header 'apikey'.
    """
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
        "endTime": "nowPlus1d",
        "timezone": "UTC",
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
    if not resp.ok:
        try:
            detail = resp.json()
        except Exception:
            detail = resp.text
        logger.error("Tomorrow.io Timelines error %s: %s", resp.status_code, detail)
        resp.raise_for_status()
    return resp.json()


# ------------------
# Normalize helpers
# ------------------
def kph_from_ms(ms: Optional[float]) -> Optional[float]:
    return None if ms is None else round(ms * 3.6, 2)


def to_iso(ts: Optional[int or str]) -> Optional[str]:
    if ts is None:
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


# ----------------------------
# Normalize: OpenWeather → uni
# ----------------------------
def normalize_openweather(raw: Dict[str, Any]) -> Dict[str, Any]:
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
                "pop": h.get("pop"),
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
        entry = {
            "date": (
                datetime.fromtimestamp(d.get("dt", 0), tz=timezone.utc).isoformat()
            )[:10],
            "sunrise": to_iso(d.get("sunrise")),
            "sunset": to_iso(d.get("sunset")),
            "moonrise": to_iso(d.get("moonrise")),
            "moonset": to_iso(d.get("moonset")),
            "moonPhase": d.get("moon_phase"),
            "tempC": {
                "min": (d.get("temp") or {}).get("min"),
                "max": (d.get("temp") or {}).get("max"),
                "day": (d.get("temp") or {}).get("day"),
                "night": (d.get("temp") or {}).get("night"),
                "morn": (d.get("temp") or {}).get("morn"),
                "eve": (d.get("temp") or {}).get("eve"),
            },
            "pop": d.get("pop"),
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
        "timezone": raw.get("timezone"),
        "updatedAt": datetime.now(timezone.utc).isoformat(),
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


def normalize_tomorrow(raw: Dict[str, Any]) -> Dict[str, Any]:
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
            "feelsLikeC": cv.get("temperatureApparent", cv.get("temperature")),
            "humidity": cv.get("humidity"),
            "dewPointC": cv.get("dewPoint"),
            "pressureHpa": cv.get("pressureSurfaceLevel"),
            "windKph": kph_from_ms(cv.get("windSpeed")),
            "windGustKph": kph_from_ms(cv.get("windGust")),
            "windDeg": cv.get("windDirection"),
            # Tomorrow visibility often already in km; if meters, adjust here if needed
            "visibilityKm": (
                cv.get(
                    "visibility",
                    None if cv.get("visibility") is None else float(cv["visibility"]),
                )
                if cv.get("visibility") is not None
                else None
            ),
            "uv": cv.get("uvIndex"),
            "cloudCoverPct": cv.get("cloudCover"),
            "precipMmHr": float(cv.get("rainIntensity") or 0)
            + float(cv.get("snowIntensity") or 0)
            + float(cv.get("sleetIntensity") or 0),
            "pop": (
                (cv.get("precipitationProbability") / 100.0)
                if cv.get("precipitationProbability") is not None
                else None
            ),
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
                "pop": (
                    (v.get("precipitationProbability") / 100.0)
                    if v.get("precipitationProbability") is not None
                    else None
                ),
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
        v = d["values"]
        daily.append(
            {
                "date": to_iso(d.get("startTime"))[:10],
                "tempC": {
                    "min": v.get("temperatureMin", v.get("temperature")),
                    "max": v.get("temperatureMax", v.get("temperature")),
                },
                "pop": (
                    (v.get("precipitationProbability") / 100.0)
                    if v.get("precipitationProbability") is not None
                    else None
                ),
                "precipMm": float(v.get("rainIntensity") or 0)
                + float(v.get("snowIntensity") or 0)
                + float(v.get("sleetIntensity") or 0),
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
        "updatedAt": datetime.now(timezone.utc).isoformat(),
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


def merge_weather(owm: Dict[str, Any], tmr: Dict[str, Any]) -> Dict[str, Any]:
    # Current: freshest wins; if tie, prefer Tomorrow for precip richness
    def pick_current(
        a: Optional[Dict[str, Any]], b: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        if not a and not b:
            return None
        if a and not b:
            return a
        if b and not a:
            return b
        which = _fresher(a.get("time"), b.get("time"))
        if which == "a":
            return a
        if which == "b":
            return b
        return a if (a or {}).get("_src") == "tomorrow" else b

    # Hourly: by timestamp union; conservative POP/precip = max
    def index_by_time(arr: List[Dict[str, Any]]) -> Dict[str, int]:
        return {x["time"]: i for i, x in enumerate(arr) if x.get("time")}

    hourly_out: List[Dict[str, Any]] = []
    a = owm.get("hourly") or []
    b = tmr.get("hourly") or []
    map_a = index_by_time(a)
    map_b = index_by_time(b)
    times = sorted(
        set(
            [x["time"] for x in a if x.get("time")]
            + [x["time"] for x in b if x.get("time")]
        )
    )
    for ts in times:
        ha = a[map_a[ts]] if ts in map_a else None
        hb = b[map_b[ts]] if ts in map_b else None
        if ha and not hb:
            hourly_out.append(ha)
            continue
        if hb and not ha:
            hourly_out.append(hb)
            continue
        # merge conservatively
        pop = max((ha.get("pop") or 0), (hb.get("pop") or 0)) or None
        precip = max((ha.get("precipMm") or 0), (hb.get("precipMm") or 0)) or None
        merged = {
            **ha,
            **hb,
            "pop": pop,
            "precipMm": precip,
            "_src": hb.get("_src") or ha.get("_src"),
        }
        hourly_out.append(merged)

    # Daily: union on date; keep OWM astronomy, join temps; conservative precip/pop
    daily_out: List[Dict[str, Any]] = []
    da = owm.get("daily") or []
    db = tmr.get("daily") or []
    map_da = {x["date"]: i for i, x in enumerate(da) if x.get("date")}
    map_db = {x["date"]: i for i, x in enumerate(db) if x.get("date")}
    days = sorted(
        set(
            [x["date"] for x in da if x.get("date")]
            + [x["date"] for x in db if x.get("date")]
        )
    )
    for d in days:
        oa = da[map_da[d]] if d in map_da else None
        tb = db[map_db[d]] if d in map_db else None
        if oa and not tb:
            daily_out.append(oa)
            continue
        if tb and not oa:
            daily_out.append(tb)
            continue
        merged = {
            "date": d,
            "sunrise": oa.get("sunrise"),
            "sunset": oa.get("sunset"),
            "moonrise": oa.get("moonrise"),
            "moonset": oa.get("moonset"),
            "moonPhase": oa.get("moonPhase"),
            "tempC": {
                "min": (
                    (tb.get("tempC") or {}).get("min")
                    if tb
                    else (oa.get("tempC") or {}).get("min")
                ),
                "max": (
                    (tb.get("tempC") or {}).get("max")
                    if tb
                    else (oa.get("tempC") or {}).get("max")
                ),
                "day": (oa.get("tempC") or {}).get("day")
                or (tb.get("tempC") or {}).get("day"),
                "night": (oa.get("tempC") or {}).get("night")
                or (tb.get("tempC") or {}).get("night"),
                "morn": (oa.get("tempC") or {}).get("morn")
                or (tb.get("tempC") or {}).get("morn"),
                "eve": (oa.get("tempC") or {}).get("eve")
                or (tb.get("tempC") or {}).get("eve"),
            },
            "pop": max((oa.get("pop") or 0), (tb.get("pop") or 0)) or None,
            "precipMm": max((oa.get("precipMm") or 0), (tb.get("precipMm") or 0))
            or None,
            "windKph": (tb.get("windKph") or oa.get("windKph")),
            "windGustKph": (tb.get("windGustKph") or oa.get("windGustKph")),
            "uv": oa.get("uv") if oa.get("uv") is not None else tb.get("uv"),
            "condition": oa.get("condition") or tb.get("condition"),
            "_src": "tomorrow",
        }
        daily_out.append(merged)

    # Alerts: prefer OWM (gov-issued). Deduplicate by title+window.
    alerts = []
    for al in owm.get("alerts") or []:
        key = (al.get("title"), al.get("start"), al.get("end"))
        if key not in {(x.get("title"), x.get("start"), x.get("end")) for x in alerts}:
            alerts.append(al)

    astronomy = owm.get("astronomy")  # prefer OWM for free-tier astronomy

    return {
        "lat": owm.get("lat") or tmr.get("lat"),
        "lon": owm.get("lon") or tmr.get("lon"),
        "timezone": owm.get("timezone") or tmr.get("timezone"),
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "current": pick_current(owm.get("current"), tmr.get("current")),
        "hourly": hourly_out,
        "daily": daily_out,
        "alerts": alerts,
        "astronomy": astronomy,
        "extra": {
            "owmUpdatedAt": owm.get("updatedAt"),
            "tmrUpdatedAt": tmr.get("updatedAt"),
        },
    }


# ---------------------------
# Public API for this module
# ---------------------------
def get_weather_for_location(
    location: Dict[str, Any], config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Fetch and cache MERGED weather for a location (city or lat/lon) using both providers.
    - Uses OpenWeather geocoding for city -> lat/lon
    - Calls both providers
    - Normalizes, merges, and saves the merged payload
    """
    owm_key = config["openweather_api_key"]
    tmr_key = config.get("tomorrow_api_key")
    units = config.get("units", "metric")

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
    owm_uni: Dict[str, Any] = {}
    if enabled_owm:
        try:
            owm_raw = fetch_openweather(lat, lon, owm_key, units=units)
            owm_uni = normalize_openweather(owm_raw)
        except Exception as e:
            logger.warning(
                f"OpenWeather fetch failed for {location['name']}: {e}"
            )

    tmr_uni: Dict[str, Any] = {}
    if enabled_tmr:
        try:
            tmr_raw = fetch_tomorrow(lat, lon, tmr_key, units=units)
            tmr_uni = normalize_tomorrow(tmr_raw)
        except Exception as e:
            logger.warning(
                f"Tomorrow.io fetch failed for {location['name']}: {e}"
            )

    if not owm_uni and not tmr_uni:
        # Both providers failed; let caller decide how to handle (usually serve stale cache)
        raise RuntimeError(
            f"All providers failed for {location['name']} (lat={lat}, lon={lon})."
        )

    merged = merge_weather(owm_uni or {}, tmr_uni or {})
    save_weather(location["name"], merged)
    return merged
