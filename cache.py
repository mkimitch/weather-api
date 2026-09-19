import os
import json
import tempfile
from datetime import datetime, timezone

CACHE_DIR = None
CACHE_EXPIRY_MINUTES = 30


def init_cache(config):
    """Initialize cache directory and expiry from config."""
    global CACHE_DIR, CACHE_EXPIRY_MINUTES
    CACHE_DIR = config.get("cache_dir", "./weather_cache")
    CACHE_EXPIRY_MINUTES = config.get("cache_expiry_minutes", 30)
    os.makedirs(CACHE_DIR, exist_ok=True)


def get_cache_path(location_name):
    return os.path.join(CACHE_DIR, f"{location_name}.json")


def cache_age(payload):
    ts = datetime.fromisoformat(payload["timestamp"])
    # Read legacy cache timestamps, which were naive UTC.
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return max(0, (datetime.now(timezone.utc) - ts).total_seconds())


def cached_response(payload, *, refresh_failed=False):
    """Attach serving metadata without changing when upstream data was obtained."""
    age = cache_age(payload)
    return {
        **payload["data"],
        "_cache": {
            "writtenAt": payload["timestamp"],
            "ageSeconds": int(age),
            "stale": age > CACHE_EXPIRY_MINUTES * 60,
            "refreshFailed": refresh_failed,
        },
    }


def save_weather(location_name, data):
    # Readers (HTTP and health checks) must never observe a partially written JSON file.
    payload = {"timestamp": datetime.now(timezone.utc).isoformat(),
               "data": {k: v for k, v in data.items() if k != "_cache"}}
    temp_path = None
    path = get_cache_path(location_name)
    mode = os.stat(path).st_mode & 0o777 if os.path.exists(path) else 0o644
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=CACHE_DIR,
                                         prefix=".weather-", suffix=".tmp", delete=False) as f:
            temp_path = f.name
            json.dump(payload, f, allow_nan=False)
        os.chmod(temp_path, mode)
        os.replace(temp_path, path)
    finally:
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)
    return cached_response(payload)


def get_cached_weather(location_name):
    payload = read_cached_payload(location_name)
    if payload is None or cache_age(payload) > CACHE_EXPIRY_MINUTES * 60:
        return None
    return cached_response(payload)


def get_cache_meta(location_name):
    """Return {'exists', 'expired', 'timestamp', 'age_seconds'} for a cached location."""
    payload = read_cached_payload(location_name)
    if payload is None:
        return {"exists": False, "expired": True, "timestamp": None, "age_seconds": None}
    age = cache_age(payload)
    return {"exists": True, "expired": age > CACHE_EXPIRY_MINUTES * 60,
            "timestamp": payload["timestamp"], "age_seconds": int(age)}


def read_cached_payload(location_name):
    """Return raw cache envelope or None, including for an interrupted legacy write."""
    try:
        with open(get_cache_path(location_name), "r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload.get("data"), dict):
            return None
        cache_age(payload)  # validate the timestamp before serving
        return payload
    except (FileNotFoundError, json.JSONDecodeError, KeyError, ValueError, TypeError, AttributeError):
        return None
