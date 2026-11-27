"""Control and inspect optional rich fields on a combined Timelines response."""

import fcntl
import logging
import math
import os
import re
import sqlite3
from collections import Counter
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from stat import S_ISREG
from typing import NamedTuple
from zoneinfo import ZoneInfo

from conditions import TOMORROW_SOLAR_FIELDS, tomorrow_code_mapping
from history import claim_tomorrow_activation, recent_tomorrow_raw
from timeutils import day_bounds, instant

logger = logging.getLogger(__name__)

BASE_FIELDS = (
    "temperature", "temperatureApparent", "humidity", "dewPoint", "pressureSurfaceLevel",
    "windSpeed", "windGust", "windDirection", "visibility", "uvIndex", "cloudCover",
    "precipitationProbability", "precipitationProbabilityAvg", "precipitationProbabilityMax",
    "rainIntensity", "snowIntensity", "sleetIntensity", "weatherCode", "temperatureMin",
    "temperatureMax",
)
TIMESTEPS = ("1h", "1d")
BOUNDED_WARNINGS = {"Timestep Bounded Field", "Time Bounded Field", "Location Bounded Field"}
MAX_WARNINGS = 12
HOUR = timedelta(hours=1)


class Activation(NamedTuple):
    mode: str
    token: str
    identity: tuple[int, int, int, int]


@contextmanager
def control_directory(path):
    """Serialize claims and publication with operator edits to a protected directory."""
    path = Path(path) if isinstance(path, (str, os.PathLike)) else Path()
    fd = None
    control = None
    try:
        if path.is_absolute() and ".." not in path.parts and path.suffix == ".control":
            if path.parent.resolve(strict=True) == path.parent:
                fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                info = os.fstat(fd)
                if info.st_uid in (0, os.geteuid()) and not (info.st_mode & 0o022):
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    control = fd, path.name
    except (OSError, ValueError, RuntimeError):
        pass
    try:
        yield control
    finally:
        if fd is not None:
            os.close(fd)


def read_control(control):
    if control is None:
        return None
    parent, name = control
    try:
        with os.fdopen(os.open(name, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW, dir_fd=parent), "rb") as stream:
            identity = os.fstat(stream.fileno())
            if (not S_ISREG(identity.st_mode) or identity.st_nlink != 1 or identity.st_mode & 0o022
                    or identity.st_uid not in (0, os.geteuid()) or identity.st_size > 48):
                return None
            marker = re.fullmatch(rb"(observe|publish) ([0-9a-f]{32})\n?", stream.read(49))
            if os.fstat(stream.fileno()).st_ctime_ns != identity.st_ctime_ns:
                return None
    except OSError:
        return None
    if marker is None:
        return None
    mode, token = (part.decode() for part in marker.groups())
    return Activation(mode, token, (identity.st_dev, identity.st_ino, identity.st_mtime_ns, identity.st_ctime_ns))


def prepare_acquisition(config, location, zone, now, path, attempt):
    """Use the generic body if the control or a comparable stored forecast is unavailable."""
    if not path or not zone or not config.get("history_enabled", True):
        return None, None
    with control_directory(path) as control:
        activation = read_control(control)
    if activation is None:
        return None, None
    try:
        saved = recent_tomorrow_raw(config, location, zone, now)
        if saved is None:
            logger.info("Rich acquisition requires a recent Tomorrow.io forecast")
            return None, None
        baseline, _ = read_timelines(saved)
        with control_directory(path) as control:
            if (read_control(control) == activation
                    and claim_tomorrow_activation(config, location["name"], activation.token, attempt, now)):
                return activation, baseline
    except (OSError, sqlite3.Error, ValueError, KeyError, OverflowError):
        logger.warning("Rich acquisition control or stored baseline unavailable")
    return None, None


def read_timelines(data):
    """Validate the two timeline shapes and convert labels to comparable UTC instants."""
    if not isinstance(data, dict) or not isinstance(data.get("timelines"), list):
        raise ValueError("missing timelines")
    rows, ends = {}, {}
    for timeline in data["timelines"]:
        if not isinstance(timeline, dict):
            raise ValueError("invalid timeline")
        step = timeline.get("timestep")
        if step not in TIMESTEPS:
            raise ValueError("unexpected timeline")
        intervals = timeline.get("intervals")
        if step in rows or not isinstance(intervals, list) or not intervals:
            raise ValueError("missing or duplicate timeline")
        rows[step] = {}
        for interval in intervals:
            if not isinstance(interval, dict) or not isinstance(interval.get("values"), dict):
                raise ValueError("invalid interval")
            timestamp = interval.get("startTime")
            if not isinstance(timestamp, str):
                raise ValueError("missing interval timestamp")
            point = instant(timestamp)
            if rows[step] and point <= next(reversed(rows[step])):
                raise ValueError("unordered or duplicate interval")
            rows[step][point] = interval["values"]
        if timeline.get("endTime") is not None:
            end = timeline["endTime"]
            if not isinstance(end, str):
                raise ValueError("invalid timeline end")
            ends[step] = instant(end)
            if ends[step] < next(reversed(rows[step])):
                raise ValueError("timeline ends before last interval")
    if set(rows) != set(TIMESTEPS):
        raise ValueError("incomplete timelines")
    return rows, ends


def baseline_value(field, value):
    if field == "weatherCode":
        return type(value) in (int, str)
    if type(value) not in (int, float):
        return False
    if field.startswith("precipitationProbability"):
        return 0 <= value <= 100
    if field in ("temperatureMin", "temperatureMax"):
        return -100 <= value <= 70
    return type(value) is int or math.isfinite(value)


def known_generic_code(value):
    entry = tomorrow_code_mapping("weatherCode", value)
    return bool(entry and entry.get("status", "known") == "known")


def has_core_data(step, values):
    """Apply the minimum generic forecast signal even without an overlapping row."""
    if known_generic_code(values.get("weatherCode")):
        return True
    if baseline_value("precipitationProbability", values.get("precipitationProbability")):
        return True
    if step == "1h":
        return baseline_value("temperature", values.get("temperature"))

    if any(baseline_value(field, values.get(field))
           for field in ("precipitationProbabilityAvg", "precipitationProbabilityMax")):
        return True
    low, high = values.get("temperatureMin"), values.get("temperatureMax")
    valid_low = baseline_value("temperatureMin", low)
    valid_high = baseline_value("temperatureMax", high)
    if valid_low and valid_high:
        return low <= high
    return valid_low or valid_high


def summarize_warnings(raw):
    warnings = raw.get("warnings", [])
    if not isinstance(warnings, list):
        return {"invalid": True, "blocking": True, "items": []}
    items = []
    for entry in warnings[:MAX_WARNINGS]:
        entry = entry if isinstance(entry, dict) else {}
        meta = entry.get("meta")
        meta = meta if isinstance(meta, dict) else {}
        field = meta.get("field")
        step = meta.get("timestep")
        kind = entry.get("type")
        code = entry.get("code")
        optional = isinstance(field, str) and field in TOMORROW_SOLAR_FIELDS
        if isinstance(step, list) and len(step) == 1:
            step = step[0]
        hourly_only = optional and kind == "Timestep Bounded Field" and step == "1h"
        items.append({
            "code": code if type(code) is int and 0 <= code <= 999999 else None,
            "type": kind if isinstance(kind, str) and kind in BOUNDED_WARNINGS else None,
            "field": field if optional or field in BASE_FIELDS else None,
            "timestep": step if step in TIMESTEPS else None,
            "range": {key: value for key in ("from", "to")
                      if isinstance(value := meta.get(key), str) and re.fullmatch(r"[+-]?\d{1,4}[mhd]", value)},
            "scope": "optional" if optional else "baseline_or_unknown",
            "blocking": not hourly_only,
        })
    return {"count": len(warnings), "items": items,
            "blocking": len(warnings) > MAX_WARNINGS or any(item["blocking"] for item in items)}


def field_state(values, field):
    if field not in values:
        return "missing"
    code = values[field]
    if code is None:
        return "null"
    if type(code) is not int:
        return "invalid"
    if code == 0:
        return "sentinel"
    entry = tomorrow_code_mapping(field, code)
    if not entry:
        return "unknown"
    return "disputed" if entry.get("status") == "unknown" else "known"


def field_counts(rows, field):
    counts = dict.fromkeys(("missing", "null", "invalid", "sentinel", "unknown", "disputed", "known"), 0)
    codes = Counter()
    for values in rows:
        counts[field_state(values, field)] += 1
        code = values.get(field)
        if type(code) is int and 0 <= code <= 99999:
            codes[code] += 1
    counts["codes"] = dict(sorted(codes.items())[:16])
    counts["additionalCodes"] = max(0, len(codes) - 16)
    return counts


def inspect_acquisition(raw, baseline, start, end, zone):
    """Compare with the latest saved baseline and check complete interior local days."""
    if not isinstance(raw, dict):
        return {"baselineIssues": ["invalid_response"], "publishable": False}
    warnings = summarize_warnings(raw)
    try:
        received, ends = read_timelines(raw.get("data"))
    except (ValueError, OverflowError):
        return {"baselineIssues": ["invalid_timelines"], "warnings": warnings, "publishable": False}

    issue_counts = Counter()
    reported_end = ends.get("1d", end)
    local_start = start.astimezone(ZoneInfo(zone)).date()
    local_end = end.astimezone(ZoneInfo(zone)).date()
    complete_days = {}
    for i in range(1, (local_end - local_start).days):
        label = (local_start + timedelta(days=i)).isoformat()
        midnight, following = day_bounds(label, zone)
        if following <= end:
            if midnight in received["1d"] and following <= reported_end:
                complete_days[label] = received["1d"][midnight]
            else:
                issue_counts["1d:missing_complete_day"] += 1

    hours = list(received["1h"])
    first_hour = hours[0]
    if (abs(first_hour - start) > HOUR or hours[-1] < end - 2 * HOUR
            or any(later - earlier != HOUR for earlier, later in zip(hours, hours[1:]))):
        issue_counts["1h:coverage_or_cadence"] += 1

    # Boundary daily rows can be incomplete. Only complete interior days need
    # this independent minimum, while overlap checks still apply as before.
    for step, rows in (("1h", received["1h"].values()), ("1d", complete_days.values())):
        unusable = sum(not has_core_data(step, values) for values in rows)
        if unusable:
            issue_counts[f"{step}:unusable_core_data"] = unusable

    midnight, _ = day_bounds(local_start.isoformat(), zone)
    for step, first in (("1h", first_hour), ("1d", midnight)):
        common = 0
        for timestamp, values in baseline[step].items():
            # Compare overlap with the requested forecast. The first hourly row
            # can legitimately roll to the next hour between provider calls.
            if timestamp < first or timestamp > end:
                continue
            candidate = received[step].get(timestamp)
            if candidate is None:
                issue_counts[f"{step}:missing_baseline_interval"] += 1
                continue
            common += 1
            for field in BASE_FIELDS:
                if baseline_value(field, values.get(field)) and not baseline_value(field, candidate.get(field)):
                    issue_counts[f"{step}:missing:{field}"] += 1
            old_code, new_code = values.get("weatherCode"), candidate.get("weatherCode")
            if (type(old_code) is int and type(new_code) is str
                    or known_generic_code(old_code) and type(new_code) is int and not known_generic_code(new_code)):
                issue_counts[f"{step}:invalid:weatherCode"] += 1
            if step == "1d" and all(baseline_value(field, record.get(field)) for record in (values, candidate)
                                    for field in ("temperatureMin", "temperatureMax")):
                if values["temperatureMin"] <= values["temperatureMax"] and candidate["temperatureMin"] > candidate["temperatureMax"]:
                    issue_counts["1d:invalid_temperature_pair"] += 1
        if not common:
            issue_counts[f"{step}:no_baseline_overlap"] += 1

    fields = {field: {
        step: field_counts(received[step].values(), field) for step in TIMESTEPS
    } | {"completeInteriorDays": field_counts(complete_days.values(), field)} for field in TOMORROW_SOLAR_FIELDS}
    available = {
        field: bool(complete_days) and counts["known"] > 0
        and counts["known"] + counts["disputed"] == len(complete_days)
        for field, info in fields.items() for counts in (info["completeInteriorDays"],)
    }
    return {
        "baselineIssues": dict(issue_counts),
        "warnings": warnings,
        "timelines": {step: {"rows": len(rows), "first": next(iter(rows)).isoformat(),
                             "last": next(reversed(rows)).isoformat(),
                             "reportedEnd": ends[step].isoformat() if step in ends else None}
                      for step, rows in received.items()},
        "completeInteriorDays": len(complete_days),
        "fields": fields,
        "available": available,
        "publishable": not issue_counts and not warnings["blocking"] and all(available.values()),
    }


def filter_rich_values(raw, *, publish):
    """Keep baseline data intact and avoid storing malformed optional values."""
    for timeline in raw["data"]["timelines"]:
        for interval in timeline.get("intervals") or ():
            values = interval["values"]
            for field in TOMORROW_SOLAR_FIELDS:
                if field in values and (not publish or (values[field] is not None and type(values[field]) is not int)):
                    del values[field]
