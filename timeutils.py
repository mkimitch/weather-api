"""UTC instants and local calendar periods (including 23/25-hour days)."""
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


def utcnow():
    return datetime.now(timezone.utc)


def instant(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("An explicit timezone is required")
    return result.astimezone(timezone.utc)


def day_bounds(day, zone):
    day = date.fromisoformat(day)
    tz = ZoneInfo(zone)
    return (datetime.combine(day, time.min, tz).astimezone(timezone.utc),
            datetime.combine(day + timedelta(days=1), time.min, tz).astimezone(timezone.utc))


def local_noon(day, zone):
    return datetime.combine(date.fromisoformat(day), time(12), ZoneInfo(zone)).astimezone(timezone.utc)
