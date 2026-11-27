"""Provider evidence and versioned condition normalization."""

import json
from datetime import timedelta
from pathlib import Path

from timeutils import instant

SCHEMA_VERSION = 1
TOMORROW_SOLAR_FIELDS = {
    "weatherCodeFullDay": ("fullDay", "sunrise_to_sunrise"),
    "weatherCodeDay": ("daytime", "sunrise_to_sunset"),
    "weatherCodeNight": ("nighttime", "sunset_to_sunrise"),
}
TOMORROW_CONDITION_FIELDS = {"weatherCode", *TOMORROW_SOLAR_FIELDS}
CATALOGS = {
    name: json.loads((Path(__file__).parent / "catalogs" / f"{name}.json").read_text(encoding="utf-8"))
    for name in ("openweather", "tomorrow")
}
OWM_ICONS = {f"{code}{light}" for code in ("01", "02", "03", "04", "09", "10", "11", "13", "50")
             for light in ("d", "n")}


def tomorrow_code_mapping(field, code):
    if type(code) is not int:
        return {}
    return CATALOGS["tomorrow"]["conditionsByField"].get(field, {}).get(str(code), {})


def coverage(basis, start=None, end=None, local_date=None):
    return {"basis": basis, "start": start, "end": end, "localDate": local_date}


def interval_coverage(start, local_date=None):
    end = (instant(start) + timedelta(hours=1)).isoformat() if start else None
    return coverage("hourly", start, end, local_date)


def simple_value(value):
    if isinstance(value, str):
        return value
    if type(value) is int:
        return value
    return None


def text_value(value):
    return value if isinstance(value, str) else None


def cloud_cover_percent(value):
    return value if type(value) in (int, float) and 0 <= value <= 100 else None


def build_condition(provider, context, native, period, *, timestep, data_kind, fetched_at,
                    source_time, cloud_percent=None):
    if not native:
        return None
    cloud, phenomena, descriptions, mappings = [], [], [], []
    mixed_cloud = False
    light = None
    for index, evidence in enumerate(native):
        code = evidence["code"]
        if provider == "tomorrow":
            entry = tomorrow_code_mapping(evidence["field"], code)
        else:
            entry = CATALOGS[provider]["conditions"].get(str(code), {}) if type(code) is int else {}
        status = entry.get("status", "known" if entry else "unknown")
        mappings.append(status)
        mixed_cloud = mixed_cloud or entry.get("mixedCloudCover", False)
        text = entry.get("description") or evidence.get("description")
        if text and text not in descriptions:
            descriptions.append(text)
        for cover in entry.get("cloudCover", []):
            if cover not in cloud:
                cloud.append(cover)
        phenomena.extend({**event, "evidenceIndex": index} for event in entry.get("phenomena", []))
        if index == 0 and provider == "openweather" and evidence.get("icon") in OWM_ICONS:
            light = "night" if evidence["icon"].endswith("n") else "day"

    status = "known" if all(item == "known" for item in mappings) else (
        "partial" if any(item != "unknown" for item in mappings) else "unknown"
    )
    return {
        "context": context,
        "period": period,
        "cloudCover": {"classes": cloud, "mixed": mixed_cloud or len(cloud) > 1,
                       "percent": cloud_cover_percent(cloud_percent)},
        "phenomena": phenomena,
        "description": "; ".join(descriptions) or "Weather condition unavailable",
        "status": status,
        "lightHint": light,
        "source": {"provider": provider, "retrievedAt": fetched_at, "sourceTime": source_time,
                   "observedAt": None, "issuedAt": None, "timestep": timestep,
                   "dataKind": data_kind, "native": native},
    }


def openweather_condition(record, context, source_time, fetched_at, *, local_date=None):
    weather = record.get("weather")
    native = [
        {"field": "weather", "code": simple_value(entry.get("id")),
         "main": text_value(entry.get("main")), "description": text_value(entry.get("description")),
         "icon": text_value(entry.get("icon"))}
        for entry in weather if isinstance(entry, dict)
    ] if isinstance(weather, list) else []
    if context == "hourly":
        # One Call publishes a forecast timestamp without explicit interval boundaries.
        period = coverage("hourly")
    elif context == "current":
        period = coverage("instantaneous", source_time, source_time)
    else:
        # The daily timestamp labels a forecast day; it is not a documented coverage endpoint.
        period = coverage("unknown", local_date=local_date)
    return build_condition("openweather", context, native, period,
                           timestep="daily" if context == "fullDay" else context,
                           data_kind="unknown" if context == "current" else "forecast",
                           fetched_at=fetched_at, source_time=source_time,
                           cloud_percent=record.get("clouds"))


def tomorrow_condition(values, field, context, period, source_time, fetched_at, timestep):
    if field not in values or values[field] is None:
        return None
    # Generic cloud cover is separate from the richer solar windows.
    cloud_percent = values.get("cloudCover") if field == "weatherCode" and timestep in ("current", "1m", "1h") else None
    return build_condition("tomorrow", context, [{"field": field, "code": simple_value(values[field])}],
                           period, timestep=timestep, data_kind="unknown" if timestep == "current" else "forecast",
                           fetched_at=fetched_at, source_time=source_time, cloud_percent=cloud_percent)


def selectable(condition):
    return bool(condition and condition["status"] != "unknown" and (
        condition["phenomena"] or condition["cloudCover"]["classes"] or
        condition["cloudCover"]["mixed"]
    ))
