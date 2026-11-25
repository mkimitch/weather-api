"""Deterministic read-time verification. No score table to become stale after QC corrections."""
import json
import random
from collections import defaultdict
from datetime import date, timedelta
from statistics import mean

from history import database, location_identity
from observations import daily_observation
from timeutils import day_bounds, instant, utcnow

METRICS = {
    "tempMaxC": {"unit": "C", "metric": "MAE", "definition": "Daily forecast high vs adequately sampled station maximum; bias = forecast - observation"},
    "tempMinC": {"unit": "C", "metric": "MAE", "definition": "Daily forecast low vs adequately sampled station minimum; bias = forecast - observation"},
    "noonWindKph": {"unit": "km/h", "metric": "MAE", "definition": "Hourly forecast at local noon vs closest valid station reading within 10 minutes"},
    "pop": {"unit": "probability", "metric": "Brier", "definition": "Mean (probability - binary measurable-precipitation event)^2; only equivalent events permitted"},
    "precipMm": {"unit": "mm", "metric": "MAE", "definition": "Liquid-equivalent accumulation MAE and event contingency counts at >0.254 mm"},
    "condition": {"unit": "category", "metric": "accuracy", "definition": "Fraction of equal categories, only for an identical valid period and event definition"},
}
UNVERIFIABLE = {
    "pop": "Tomorrow daily average/max is not a daily event probability; OWM's measurable threshold is undocumented. No comparable daily Brier score or probability-bias claim.",
    "precipMm": "Tomorrow daily intensity is not accumulation; OWM rain-plus-snow liquid equivalence is not established. Missing station precipitation is not zero.",
    "condition": "Provider daily representative weather codes do not define a comparable measured daily category.",
}


def scores(pairs, metric):
    """Pure scoring kernel; caller must establish matching semantics/periods first."""
    if not pairs:
        return {"sampleCount": 0, "status": "insufficient_data"}
    if metric == "condition":
        return {"sampleCount": len(pairs), "accuracy": mean(p == o for p, o in pairs)}
    errors = [p - o for p, o in pairs]
    result = {"sampleCount": len(pairs), "bias": mean(errors), "mae": mean(abs(e) for e in errors)}
    if metric == "pop":
        if any(not 0 <= p <= 1 or o not in (0, 1) for p, o in pairs):
            raise ValueError("Brier scores require probabilities and binary events")
        result = {"sampleCount": len(pairs), "brierScore": mean(e * e for e in errors),
                  "meanProbability": mean(p for p, _ in pairs), "eventFrequency": mean(o for _, o in pairs),
                  "probabilityBias": mean(errors)}
    if metric == "precipMm":
        hits = sum(p > .254 and o > .254 for p, o in pairs)
        misses = sum(p <= .254 < o for p, o in pairs)
        false_alarms = sum(o <= .254 < p for p, o in pairs)
        result["events"] = {"thresholdMm": .254, "operator": ">", "hits": hits, "misses": misses,
                            "falseAlarms": false_alarms, "correctNegatives": sum(p <= .254 and o <= .254 for p, o in pairs),
                            "criticalSuccessIndex": hits / (hits + misses + false_alarms) if hits + misses + false_alarms else None}
    return result


def paired_evidence(events, metric):
    """Resample calendar-week clusters, not each correlated forecast refresh."""
    if not events:
        return {"sampleCount": 0, "status": "insufficient_data", "recommendation": None}
    differences = [a - b for _, a, b in events]  # Tomorrow absolute error minus OWM
    groups = defaultdict(list)
    for day, a, b in events:
        d = date.fromisoformat(day)
        groups[(d - timedelta(days=d.weekday())).isoformat()].append(a - b)
    result = {"sampleCount": len(events), "calendarWeeks": len(groups),
              "meanAbsoluteErrorDifferenceTomorrowMinusOpenweather": mean(differences),
              "confidenceInterval95": None, "status": "insufficient_data", "recommendation": None}
    if len(events) < 30 or len(groups) < 8:
        return result
    rng = random.Random(0)
    blocks = list(groups.values())
    distribution = sorted(mean(v for block in rng.choices(blocks, k=len(blocks)) for v in block) for _ in range(2000))
    low, high = distribution[49], distribution[1949]
    result.update(confidenceInterval95=[low, high], status="inconclusive")
    practical = 2 if metric == "noonWindKph" else .5
    if high < -practical:
        result.update(status="candidate_for_manual_review", recommendation="tomorrow")
    elif low > practical:
        result.update(status="candidate_for_manual_review", recommendation="openweather")
    return result


def report(config, location, days=30, now=None):
    if days not in (30, 90, 365):
        raise ValueError("days must be 30, 90 or 365")
    now = now or utcnow()
    with database(config) as db:
        latest = db.execute("SELECT timezone,lat,lon FROM responses WHERE location=? ORDER BY retrieved_at DESC LIMIT 1", (location["name"],)).fetchone()
        zone = location.get("timezone") or (latest[0] if latest else None)
        if not zone:
            return {"location": location["name"], "windowDays": days, "automaticSwitching": False,
                    "status": "insufficient_data", "warnings": ["An IANA location timezone is required before verification."]}
        from zoneinfo import ZoneInfo
        last_day = now.astimezone(ZoneInfo(zone)).date()
        first_day = last_day - timedelta(days=days)
        target = location
        if location.get("type") == "city" and latest:
            target = dict(location, lat=latest["lat"], lon=latest["lon"])
        identity = location_identity(target, zone)
        # Only a single version per provider/day/lead bucket; never count every refresh as evidence.
        rows = db.execute("""
            WITH candidates AS (
              SELECT r.provider,r.retrieved_at,f.target_date,f.lead_hours,f.data,
                     CAST(f.lead_hours / 24 AS INTEGER) AS bucket,
                     ROW_NUMBER() OVER (PARTITION BY r.provider,f.target_date,CAST(f.lead_hours / 24 AS INTEGER)
                                        ORDER BY r.retrieved_at DESC,r.id DESC) AS rank
              FROM forecasts f JOIN responses r ON r.id=f.response_id
              WHERE r.identity=? AND f.target_date>=? AND f.target_date<? AND f.lead_hours>=0
                AND r.retrieved_at<=f.valid_start AND f.valid_end<=?
            ) SELECT * FROM candidates WHERE rank=1
            """, (identity, first_day.isoformat(), last_day.isoformat(), now.isoformat())).fetchall()
        station = location.get("observation_station")
        start, _ = day_bounds(first_day.isoformat(), zone)
        observations = [json.loads(r[0]) for r in db.execute("SELECT data FROM observations WHERE station=? AND time>=? AND time<=?",
                                                            (station, start.isoformat(), now.isoformat()))] if station else []
        reliability = {}
        attempt_identity = location_identity(target)
        since = (now - timedelta(days=days)).isoformat()
        for provider in ("tomorrow", "openweather"):
            counts = db.execute("SELECT status,COUNT(*) AS n,SUM(daily_count) AS d,SUM(incomplete_days) AS i "
                                "FROM attempts WHERE request_type='forecast' AND identity=? AND provider=? "
                                "AND at>=? AND at<=? GROUP BY status",
                                (attempt_identity, provider, since, now.isoformat())).fetchall()
            statuses = {r["status"]: r["n"] for r in counts}
            total = sum(statuses.values())
            reliability[provider] = {"attempts": total, "statuses": statuses,
                                     "failureRate": statuses.get("failed", 0) / total if total else None,
                                     "returnedDailyRecords": sum(r["d"] or 0 for r in counts),
                                     "incompleteDailyRecords": sum(r["i"] or 0 for r in counts)}
    # Index once: avoid scanning a year's station records for every forecast day.
    by_day = defaultdict(list)
    for row in observations:
        local_day = instant(row["time"]).astimezone(ZoneInfo(zone)).date()
        by_day[local_day.isoformat()].append(row)
        # Midnight-ending precipitation belongs to the previous day as well.
        start, _ = day_bounds(local_day.isoformat(), zone)
        if instant(row["time"]) == start:
            by_day[(local_day - timedelta(days=1)).isoformat()].append(row)
    observed = {day: daily_observation(by_day[day], day, zone, now) for day in {r["target_date"] for r in rows}}
    selected = defaultdict(dict)
    skipped = defaultdict(int)
    for row in rows:
        data = json.loads(row["data"])
        if data.get("_period", {}).get("complete") is not True:
            skipped["incomplete_or_unknown_forecast_period"] += 1
            continue
        selected[(row["bucket"], row["target_date"])][row["provider"]] = (row, data)
    results = []
    for bucket in sorted({r["bucket"] for r in rows}):
        for metric in METRICS:
            if metric in UNVERIFIABLE:
                continue
            provider_pairs = {p: [] for p in ("tomorrow", "openweather")}
            paired_pairs = {p: [] for p in provider_pairs}
            eligible = {p: 0 for p in provider_pairs}
            paired_events = []
            unmatched = 0
            for (group, day), alternatives in selected.items():
                if group != bucket:
                    continue
                obs = observed[day].get(metric)
                values = {}
                for provider, (row, data) in alternatives.items():
                    eligible[provider] += 1
                    value = ((data.get("tempC") or {}).get("max" if metric == "tempMaxC" else "min")
                             if metric != "noonWindKph" else (data.get("noonWind") or {}).get("windKph"))
                    if value is not None and obs is not None:
                        provider_pairs[provider].append((value, obs))
                        values[provider] = value
                if len(values) == 2:
                    delta = abs((instant(alternatives["tomorrow"][0]["retrieved_at"]) - instant(alternatives["openweather"][0]["retrieved_at"])).total_seconds())
                    if delta <= 1800:
                        for p in values:
                            paired_pairs[p].append((values[p], obs))
                        paired_events.append((day, abs(values["tomorrow"] - obs), abs(values["openweather"] - obs)))
                    else:
                        unmatched += 1
            providers = {}
            for provider in provider_pairs:
                providers[provider] = {"allAvailable": scores(provider_pairs[provider], metric),
                                       "paired": scores(paired_pairs[provider], metric),
                                       "candidateDays": eligible[provider],
                                       "verifiedFraction": len(provider_pairs[provider]) / eligible[provider] if eligible[provider] else None}
            results.append({"variable": metric, "leadHours": {"minInclusive": bucket * 24, "maxExclusive": (bucket + 1) * 24},
                            "providers": providers, "pairedEvidence": paired_evidence(paired_events, metric),
                            "unpairedDueToRetrievalTime": unmatched})
    warnings = ["No automatic switching. Recommendations require manual review.",
                "Temperature extrema are derived from station samples, not official measured daily maxima/minima.",
                "KLVN is a spatial proxy: elevation, airport exposure and localized showers may differ from home.",
                "Retrieval time is used because provider issuance time is unavailable; no forecasts retrieved after local midnight are scored.",
                "Latest snapshot per day/provider/24-hour lead group; paired retrievals must be within 30 minutes.",
                "95% calendar-week cluster bootstrap intervals are exploratory, not adjusted for multiple comparisons or seasonal transfer."]
    if not station:
        warnings.append("No observation_station configured; no independent observations can be matched.")
    if not rows:
        warnings.append("No eligible historical forecasts yet. Historical forecasts cannot be reconstructed.")
    retention = int(config.get("history_retention_days", 400))
    if retention < days + 14:
        warnings.append("Retention may exclude long-lead forecasts in this reporting window.")
    return {"location": location["name"], "identity": json.loads(identity), "timezone": zone,
            "windowDays": days, "period": {"startDateInclusive": first_day.isoformat(), "endDateExclusive": last_day.isoformat()},
            "generatedAt": now.isoformat(), "automaticSwitching": False,
            "status": "ok" if any(r["pairedEvidence"]["sampleCount"] for r in results) else "insufficient_data",
            "observationSource": {"station": station, "kind": "measured; daily extrema derived", "settlingHours": 6},
            "observationCoverage": {"forecastTargetDays": len(observed),
                                    "temperatureVerifiableDays": sum(o["coverage"]["temperatureComplete"] for o in observed.values()),
                                    "precipitationVerifiableDays": sum(o["coverage"]["precipitationComplete"] for o in observed.values())},
            "definitions": METRICS, "leadTimeBasis": "hours from retrieval to local target-day midnight",
            "results": results, "unverifiable": UNVERIFIABLE, "excluded": dict(skipped),
            "reliability": reliability, "warnings": warnings}
