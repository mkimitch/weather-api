"""Precipitation probability metadata and comparison policy."""

from timeutils import instant

DAILY_POP_RANK = {
    "daily": 0,
    "precipitationProbabilityAvg": 1,
    "precipitationProbabilityMax": 2,
}


def probability_period(period, provider, *, retrieved_at):
    return {
        **period,
        "thresholdMm": 0.254 if provider == "tomorrow" else None,
        "eventDefinition": "liquid_equivalent_above_threshold" if provider == "tomorrow" else None,
        "issuedAt": None,
        "retrievedAt": retrieved_at,
    }


def covers_time(period, time):
    return bool(period and time and period.get("start") and period.get("end")
                and instant(period["start"]) <= instant(time) < instant(period["end"]))


def compare_probability(left, right):
    """Comparable only with a corroborated event, aggregation, coverage and issuance."""
    a, b = left.get("popPeriod") or {}, right.get("popPeriod") or {}
    basis_a, basis_b = left.get("popBasis"), right.get("popBasis")
    mismatch, missing = [], []
    known_bases = {"interval", *DAILY_POP_RANK}
    if basis_a in known_bases and basis_b in known_bases:
        if basis_a != basis_b:
            mismatch.append("statistical_basis")
    else:
        missing.append("statistical_basis")
    for field in ("basis", "start", "end", "thresholdMm", "eventDefinition", "issuedAt"):
        if a.get(field) in (None, "unknown") or b.get(field) in (None, "unknown"):
            missing.append(field)
            continue
        av, bv = a[field], b[field]
        if field in ("start", "end", "issuedAt"):
            av, bv = instant(av), instant(bv)
        if av != bv:
            mismatch.append(field)

    timestamp_a, timestamp_b = a.get("sourceTime"), b.get("sourceTime")
    timestamp_aligned = instant(timestamp_a) == instant(timestamp_b) if timestamp_a and timestamp_b else None
    status = "confirmed_incompatible" if mismatch else "unverified" if missing else "confirmed_comparable"
    return {"status": status, "reasons": mismatch or missing, "timestampAligned": timestamp_aligned}
