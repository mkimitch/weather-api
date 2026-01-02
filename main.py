import os
import threading
import time
from typing import Dict, Optional

import yaml
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import uvicorn

from cache import get_cached_weather, get_cache_meta, read_cached_payload
from scheduler import start_scheduler
from weather import get_weather_for_location


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
        return yaml.safe_load(f)


config = load_config()

# ---------- App ----------
app = FastAPI(title="Weather API Caching Proxy (Merged OWM + Tomorrow.io)")
_app_started_at = time.time()

# A simple in-process lock per location to coalesce concurrent refreshes
_location_locks: Dict[str, threading.Lock] = {}
_location_locks_guard = threading.Lock()  # protects the dict itself


def _get_lock_for(location_name: str) -> threading.Lock:
    with _location_locks_guard:
        lock = _location_locks.get(location_name)
        if lock is None:
            lock = threading.Lock()
            _location_locks[location_name] = lock
        return lock


def _find_location_in_config(name: str) -> Optional[Dict]:
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
    lock = _get_lock_for(location_name)
    acquired = lock.acquire(timeout=30)  # avoid deadlock; tweak as needed
    if not acquired:
        # Fallback: serve whatever is currently cached
        data = get_cached_weather(location_name)
        if data:
            return JSONResponse(content=data)
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
    except Exception as e:
        # If refresh failed, try to serve the last payload (even if expired) to be user-friendly
        fallback = read_cached_payload(location_name)
        if fallback and "data" in fallback:
            return JSONResponse(content=fallback["data"])
        raise HTTPException(status_code=502, detail=f"Upstream fetch failed: {e}")
    finally:
        try:
            lock.release()
        except Exception:
            pass


if __name__ == "__main__":
    uvicorn.run(app, host=config.get("host", "0.0.0.0"), port=config.get("port", 8080))
