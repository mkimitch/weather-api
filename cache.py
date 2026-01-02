import os
import json
from datetime import datetime, timedelta

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


def save_weather(location_name, data):
    path = get_cache_path(location_name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"timestamp": datetime.utcnow().isoformat(), "data": data}, f)


def get_cached_weather(location_name):
    path = get_cache_path(location_name)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    ts = datetime.fromisoformat(obj["timestamp"])
    if datetime.utcnow() - ts > timedelta(minutes=CACHE_EXPIRY_MINUTES):
        return None
    return obj["data"]


def get_cache_meta(location_name):
    """Return {'exists', 'expired', 'timestamp', 'age_seconds'} for a cached location."""
    path = get_cache_path(location_name)
    if not os.path.exists(path):
        return {
            "exists": False,
            "expired": True,
            "timestamp": None,
            "age_seconds": None,
        }
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    ts = datetime.fromisoformat(obj["timestamp"])
    age = (datetime.utcnow() - ts).total_seconds()
    expired = (datetime.utcnow() - ts) > timedelta(minutes=CACHE_EXPIRY_MINUTES)
    return {
        "exists": True,
        "expired": expired,
        "timestamp": obj["timestamp"],
        "age_seconds": int(age),
    }


def read_cached_payload(location_name):
    """Return raw cache file content dict (timestamp + data) or None."""
    path = get_cache_path(location_name)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
