import os
import time
from typing import Optional

import yaml
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import uvicorn
from zoneinfo import ZoneInfo

from cache import get_cached_weather, get_cache_meta, read_cached_payload, cached_response, refresh_lock
from scheduler import start_scheduler
from weather import get_weather_for_location, daily_priority
from history import health as history_health
from verification import report as accuracy_report


# ---------- Config ----------
def load_config():
    """Load the configuration from config.yaml (or WEATHER_API_CONFIG env var)."""
    config_path = os.getenv("WEATHER_API_CONFIG", "config.yaml")
    if not os.path.exists(config_path):
        raise RuntimeError(
            f"Config file not found: {config_path}. "
            "Create one by copying config.yaml.example to config.yaml, "
            "or set WEATHER_API_CONFIG to a valid path."
        )
    with open(config_path, "r") as f:
        loaded = yaml.safe_load(f)
    for env, key in (("OPENWEATHER_API_KEY", "openweather_api_key"), ("TOMORROW_API_KEY", "tomorrow_api_key")):
        if env in os.environ:
            loaded[key] = os.environ[env]
    daily_priority(loaded)
    for loc in loaded.get("locations", []):
        if loc.get("timezone"):
            ZoneInfo(loc["timezone"])
        if loc.get("observation_station") and not loc.get("timezone"):
            raise ValueError("Observation locations require an explicit IANA timezone")
    for key, default, minimum in (("history_retention_days", 400, 2), ("observation_interval_minutes", 60, 10),
                                  ("daily_max_age_minutes", 90, 1), ("refresh_interval_minutes", 30, 1)):
        if not isinstance(loaded.get(key, default), (int, float)) or loaded.get(key, default) < minimum:
            raise ValueError(f"{key} must be at least {minimum}")
    return loaded


config = load_config()

# ---------- App ----------
app = FastAPI(title="Weather API Caching Proxy (Merged OWM + Tomorrow.io)")
_app_started_at = time.time()

def _find_location_in_config(name: str) -> Optional[dict]:
    for loc in config.get("locations", []):
        if loc.get("name") == name:
            return loc
    return None


@app.on_event("startup")
def startup_event():
    """FastAPI startup event: starts the weather fetch scheduler."""
    start_scheduler(config)


# ---------- Health endpoint ----------
@app.get("/healthz")
def healthz():
    """
    Basic readiness/liveness:
    - reports uptime
    - lists each configured location with cache status (exists/expired/age)
    - notes presence of provider API keys
    """
    uptime_seconds = int(time.time() - _app_started_at)
    locs = []
    for loc in config.get("locations", []):
        name = loc.get("name")
        meta = get_cache_meta(name)
        locs.append(
            {
                "name": name,
                "type": loc.get("type"),
                "cache": meta,
            }
        )

    try:
        history_status = history_health(config)
    except Exception:
        history_status = {"status": "unavailable"}
    return JSONResponse(
        content={
            "status": "ok",
            "uptime_seconds": uptime_seconds,
            "providers": {
                "openweather_key_present": bool(config.get("openweather_api_key")),
                "tomorrow_key_present": bool(config.get("tomorrow_api_key")),
            },
            "refresh_interval_minutes": config.get("refresh_interval_minutes", 30),
            "locations": locs,
            "verification": history_status,
        }
    )


# ---------- Weather endpoint (cache-bust + coalescing) ----------
@app.get("/weather/{location_name}")
def get_weather(
    location_name: str, refresh: int = Query(0, description="Set to 1 to force refresh")
):
    """
    Return cached weather for the given location name.
    If 'refresh=1' is provided (or cache is missing/expired), fetch upstream, with a per-location
    coalescing lock so only one fetch runs at a time for that location.
    """
    loc = _find_location_in_config(location_name)
    if not loc:
        raise HTTPException(
            status_code=404, detail=f"Unknown location: {location_name}"
        )

    # 1) Try cache (fast path)
    data = get_cached_weather(location_name)
    if data and not refresh:
        return JSONResponse(content=data)

    # 2) Coalesced refresh path
    lock = refresh_lock(location_name)
    acquired = lock.acquire(timeout=30)  # avoid deadlock; tweak as needed
    if not acquired:
        # Fallback: serve whatever is currently cached
        payload = read_cached_payload(location_name)
        if payload:
            return JSONResponse(content=cached_response(payload, refresh_failed=True))
        raise HTTPException(
            status_code=503, detail="Busy refreshing; try again shortly"
        )

    try:
        # Double-check if another request already refreshed while we were waiting for the lock
        if not refresh:
            latest = get_cached_weather(location_name)
            if latest:
                return JSONResponse(content=latest)

        # Perform the refresh (fetch → merge → save)
        merged = get_weather_for_location(loc, config)
        return JSONResponse(content=merged)
    except Exception:
        # If refresh failed, try to serve the last payload (even if expired) to be user-friendly
        fallback = read_cached_payload(location_name)
        if fallback and "data" in fallback:
            return JSONResponse(content=cached_response(fallback, refresh_failed=True))
        raise HTTPException(status_code=502, detail="Upstream fetch failed; no cached weather available")
    finally:
        try:
            lock.release()
        except Exception:
            pass


@app.get("/accuracy/{location_name}")
def get_accuracy(location_name: str, days: int = Query(30, description="Rolling window: 30, 90 or 365 days")):
    location = _find_location_in_config(location_name)
    if not location:
        raise HTTPException(status_code=404, detail="Unknown location")
    if days not in (30, 90, 365):
        raise HTTPException(status_code=422, detail="days must be 30, 90 or 365")
    try:
        return JSONResponse(content=accuracy_report(config, location, days))
    except Exception:
        raise HTTPException(status_code=503, detail="Accuracy history unavailable")


if __name__ == "__main__":
    uvicorn.run(app, host=config.get("host", "0.0.0.0"), port=config.get("port", 8080))
