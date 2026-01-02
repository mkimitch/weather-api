import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from cache import init_cache
from weather import get_weather_for_location, RateLimitExceeded

logger = logging.getLogger("scheduler")


class WeatherScheduler:
    """Encapsulates weather data fetching, caching, and per-provider rate limiting."""

    def __init__(self, config: Dict):
        self.refresh_interval = config.get("refresh_interval_minutes", 30)
        self.api_keys = {
            "openweather": config.get("openweather_api_key"),
            "tomorrow": config.get("tomorrow_api_key"),
        }
        self.max_calls_per_24h = {
            "openweather": config.get(
                "max_calls_per_24h_openweather", config.get("max_calls_per_24h", 1000)
            ),
            "tomorrow": config.get("max_calls_per_24h_tomorrow", 500),
        }
        self.locations = config["locations"]
        self.api_call_timestamps: Dict[str, List[datetime]] = {
            "openweather": [],
            "tomorrow": [],
        }
        self.config = config
        init_cache(config)

    def _prune(self, provider: str):
        now = datetime.now(timezone.utc)
        self.api_call_timestamps[provider] = [
            ts
            for ts in self.api_call_timestamps[provider]
            if now - ts < timedelta(hours=24)
        ]

    def can_make_api_call(self, provider: str) -> bool:
        """True if an API call can be made for provider w/o exceeding 24h limit."""
        self._prune(provider)
        return (
            len(self.api_call_timestamps[provider]) < self.max_calls_per_24h[provider]
        )

    def record_api_call(self, provider: str):
        self.api_call_timestamps[provider].append(datetime.now(timezone.utc))

    def scheduler_loop(self):
        """Background loop: fetch merged weather for each location at interval."""
        while True:
            for loc in self.locations:
                # We only gate on OpenWeather being available; Tomorrow is optional
                if not self.can_make_api_call("openweather"):
                    logger.warning(
                        "OpenWeather API call limit reached, skipping fetch."
                    )
                    continue
                # Optional: if Tomorrow present and also limited, we still fetch OWM-only.
                if self.api_keys.get("tomorrow") and not self.can_make_api_call(
                    "tomorrow"
                ):
                    logger.warning(
                        "Tomorrow.io API call limit reached; merging OWM-only."
                    )

                try:
                    merged = get_weather_for_location(loc, self.config)
                    # Count calls: one for OWM always; one for Tomorrow if API key present
                    self.record_api_call("openweather")
                    if self.api_keys.get("tomorrow"):
                        self.record_api_call("tomorrow")
                    logger.info(
                        f"Fetched and cached weather for {loc['name']} (updatedAt={merged.get('updatedAt')})"
                    )
                except RateLimitExceeded:
                    logger.warning("Rate limit exceeded; skipping cycle.")
                except Exception as e:
                    logger.error(f"Error fetching weather for {loc['name']}: {e}")

            time.sleep(self.refresh_interval * 60)

    def start(self):
        thread = threading.Thread(target=self.scheduler_loop, daemon=True)
        thread.start()


def start_scheduler(config: Dict):
    scheduler = WeatherScheduler(config)
    scheduler.start()
