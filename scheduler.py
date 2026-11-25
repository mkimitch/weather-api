import logging
import threading
import time
from typing import Dict

from cache import init_cache
from weather import get_weather_for_location, RateLimitExceeded
from observations import collect_observations
from history import claim_job, finish_job, prune

logger = logging.getLogger("scheduler")


class WeatherScheduler:
    """Encapsulates weather data fetching, caching, and per-provider rate limiting."""

    def __init__(self, config: Dict):
        self.refresh_interval = config.get("refresh_interval_minutes", 30)
        self.locations = config["locations"]
        self.config = config
        init_cache(config)

    def refresh_location(self, loc):
        # The fetch path owns durable budgets, shared with HTTP refreshes.
        return get_weather_for_location(loc, self.config)

    def scheduler_loop(self):
        """Background loop: fetch merged weather for each location at interval."""
        while True:
            try:
                for loc in self.locations:
                    try:
                        merged = self.refresh_location(loc)
                        logger.info(
                            f"Fetched and cached weather for {loc['name']} (updatedAt={merged.get('updatedAt')})"
                        )
                    except RateLimitExceeded:
                        logger.warning("Rate limit exceeded; skipping cycle.")
                    except Exception as e:
                        logger.error("Error fetching weather for %s (%s)", loc["name"], type(e).__name__)

                try:
                    collect_observations(self.config)
                    if claim_job(self.config, "retention", 1440):
                        prune(self.config)
                        finish_job(self.config, "retention", "ok")
                except Exception as e:
                    logger.error("History maintenance failed (%s)", type(e).__name__)
                time.sleep(self.refresh_interval * 60)
            except Exception as e:
                logger.error("Scheduler loop error (%s)", type(e).__name__)
                time.sleep(60)

    def start(self):
        thread = threading.Thread(target=self.scheduler_loop, daemon=True)
        thread.start()


def start_scheduler(config: Dict):
    scheduler = WeatherScheduler(config)
    scheduler.start()
