"""Analytics over observed usage and explicitly qualified API-equivalent estimates."""
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from collections import defaultdict
from math import fsum
import pricing

VERSION = "2.1"
MAX_DAYS = 31
FILTERS = ("machine", "model", "effort", "tier", "session")
METRICS = ("total_tokens", "input_tokens", "cached_input_tokens", "uncached_input_tokens", "output_tokens", "reasoning_output_tokens", "visible_output_tokens")
SEMANTICS = {
    "total_tokens": "input_tokens + output_tokens; observed response.completed events, not subscription billing",
    "cached_input_tokens": "Subset of input_tokens; counted once in total_tokens",
    "uncached_input_tokens": "input_tokens - cached_input_tokens",
    "reasoning_output_tokens": "Subset of output_tokens; counted once in total_tokens",
    "visible_output_tokens": "output_tokens - reasoning_output_tokens",
    "cache_total_pct": "100 * cached_input_tokens / total_tokens",
    "cache_input_pct": "100 * cached_input_tokens / input_tokens",
    "reasoning_total_pct": "100 * reasoning_output_tokens / total_tokens",
    "reasoning_output_pct": "100 * reasoning_output_tokens / output_tokens",
    "average_tokens": "total_tokens / response_completed_events; includes zero-output events unless filtered out",
    "unknown": "Missing modes stay unknown. Missing token subsets yield null aggregates, never invented zeroes.",
    "activity": "Recent receipt of native telemetry, not proof a machine is online or offline; no client heartbeat daemon.",
    "comparison": "Previous adjacent interval of equal duration, same filters. Coverage describes stored observations, not guaranteed collection completeness.",
    "effort": "Direct per-response model_reasoning_effort (or reasoning_effort on that same event); never inferred from current config or session startup.",
    "tier": "fast/priority mapped to fast; default/standard mapped to standard; other explicitly reported tiers retained; absent is unknown.",
    "causality": "Differences between sessions/models/modes are descriptive; tasks and context differ. Token totals do not prove cost, quality, or causal savings.",
    "pricing": pricing.CONTRACT,
    "hidden_machines": "Card visibility is a shared dashboard preference. Hiding never deletes usage, stops collection or changes analytical totals.",
}


def label(value):
    if isinstance(value, str) and 0 < len(value.strip()) <= 120:
        return value.strip().lower()
    return None


def tier_name(value):
    value = label(value)
    if value in ("fast", "priority"):
        return "fast"
    if value in ("default", "standard"):
        return "standard"
    return value or "unknown"


def enrich(event):
    e = dict(event)
    e["effort"] = label(e.get("effort")) or "unknown"
    e["service_tier"] = label(e.get("service_tier"))
    e["tier"] = tier_name(e["service_tier"])
    e["uncached_input_tokens"] = None if e.get("cached_input_tokens") is None else e["input_tokens"] - e["cached_input_tokens"]
    e["visible_output_tokens"] = None if e.get("reasoning_output_tokens") is None else e["output_tokens"] - e["reasoning_output_tokens"]
    e.update(pricing.estimate(e))
    return e


def ratio(numerator, denominator):
    if numerator is None or denominator in (None, 0):
        return None
    return round(100 * numerator / denominator, 4)


def summarize(events):
    result = {"events": len(events), "sessions": len({(e["machine"], e["session"]) for e in events})}
    for key in METRICS:
        values = [e.get(key) for e in events]
        result[key] = sum(values) if values and all(v is not None for v in values) else None
    result.update(
        cache_total_pct=ratio(result["cached_input_tokens"], result["total_tokens"]),
        cache_input_pct=ratio(result["cached_input_tokens"], result["input_tokens"]),
        reasoning_total_pct=ratio(result["reasoning_output_tokens"], result["total_tokens"]),
        reasoning_output_pct=ratio(result["reasoning_output_tokens"], result["output_tokens"]),
        average_tokens=round(result["total_tokens"] / len(events), 2) if events else None,
        zero_output_events=sum(e["output_tokens"] == 0 for e in events),
        effort_known_events=sum(e["effort"] != "unknown" for e in events),
        tier_known_events=sum(e["tier"] != "unknown" for e in events),
        first_event_ms=min((e["timestamp_ms"] for e in events), default=None),
        last_event_ms=max((e["timestamp_ms"] for e in events), default=None),
    )
    priced = [e for e in events if e.get("api_cost_usd") is not None]
    for key in pricing.FIELDS:
        result[key] = round(fsum(e[key] for e in priced), 12) if events and len(priced) == len(events) else None
    result.update(
        api_cost_known_usd=round(fsum(e["api_cost_usd"] for e in priced), 12) if priced else None,
        api_cost_breakdown_known_usd={key: round(fsum(e[key] for e in priced), 12) if priced else None for key in pricing.PARTS},
        api_priced_events=len(priced), api_unpriced_events=len(events)-len(priced),
        api_assumed_tier_events=sum(e.get("api_price_assumed_tier", False) for e in priced),
        api_price_coverage_pct=ratio(len(priced), len(events)),
        api_unpriced_reasons=dict(sorted((reason, sum(e.get("api_price_reason")==reason for e in events)) for reason in {e.get("api_price_reason") for e in events if e.get("api_price_reason")})),
    )
    return result


def matches(event, filters):
    return all(not filters.get(k) or event.get(k) == filters[k] for k in FILTERS) and (filters.get("zero_output") != "exclude" or event["output_tokens"] > 0)


def parse_query(params, now_ms):
    def one(key, default=None):
        values = params.get(key)
        if values is None:
            return default
        if len(values) != 1 or len(values[0]) > 240:
            raise ValueError("invalid query parameter: " + key)
        return values[0]
    # Unknown query names are rejected so an agent cannot silently analyze the wrong slice.
    allowed = set(FILTERS) | {"zero_output", "hours", "start", "end", "limit", "cursor", "timezone"}
    if set(params) - allowed:
        raise ValueError("unsupported query parameter")
    if one("start") is not None or one("end") is not None:
        if one("start") is None or one("end") is None or "hours" in params:
            raise ValueError("use either start+end or hours")
        start, end = int(one("start")), int(one("end"))
    else:
        hours = int(one("hours", "720"))
        if not 1 <= hours <= MAX_DAYS * 24:
            raise ValueError("period must be between 1 hour and 31 days")
        end = now_ms
        start = end - hours * 3600000
    if start < 0 or end <= start or end - start > MAX_DAYS * 86400000 or end > now_ms + 60000:
        raise ValueError("invalid interval; maximum 31 days")
    zone = one("timezone", "UTC")
    try:
        ZoneInfo(zone)
    except (ZoneInfoNotFoundError, ValueError):
        raise ValueError("invalid timezone")
    filters = {k: one(k) for k in FILTERS if one(k) is not None}
    zero = one("zero_output", "include")
    if zero not in ("include", "exclude"):
        raise ValueError("zero_output must be include or exclude")
    filters["zero_output"] = zero
    filters["timezone"] = zone
    return start, end, filters


def grouped(events, key):
    groups = defaultdict(list)
    for e in events:
        value = (e["machine"], e["session"]) if key == "session" else e[key]
        groups[value].append(e)
    rows = []
    for value, items in groups.items():
        row = {"key": value[1] if key == "session" else value, **summarize(items)}
        if key == "session":
            row.update(machine=value[0], models=sorted({e["model"] for e in items}), efforts=sorted({e["effort"] for e in items}), tiers=sorted({e["tier"] for e in items}))
        rows.append(row)
    return sorted(rows, key=lambda row: (-(row["total_tokens"] or 0), row["key"]))


def report(events, start, end, filters, now_ms, first_observed_ms, machine_activity):
    all_events = [enrich(e) for e in events]
    current_all = [e for e in all_events if start <= e["timestamp_ms"] < end]
    current = [e for e in current_all if matches(e, filters)]
    previous_start = start - (end - start)
    previous = [e for e in all_events if previous_start <= e["timestamp_ms"] < start and matches(e, filters)]
    totals, prior = summarize(current), summarize(previous)
    changes = {key: (round(100 * (totals[key] - prior[key]) / prior[key], 2) if totals[key] is not None and prior[key] not in (None, 0) else None) for key in (*METRICS, "api_cost_usd")}
    duration = end - start
    bucket_ms = 60000 if duration <= 3600000 else 3600000 if duration <= 3 * 86400000 else 86400000
    zone = ZoneInfo(filters.get("timezone", "UTC"))
    def timeline_for(items):
        buckets = defaultdict(list)
        ends = {}
        for e in items:
            if bucket_ms == 86400000:
                local = datetime.fromtimestamp(e["timestamp_ms"] / 1000, zone).replace(hour=0, minute=0, second=0, microsecond=0)
                key = int(local.timestamp() * 1000)
                ends[key] = int((local + timedelta(days=1)).timestamp() * 1000)
            else:
                key = (e["timestamp_ms"] // bucket_ms) * bucket_ms
                ends[key] = key + bucket_ms
            buckets[key].append(e)
        return [{"timestamp_ms": t, "end_ms": ends[t], **summarize(rows), "machines": [{"key": row["key"], **{k: row[k] for k in (*METRICS, "api_cost_usd", "api_cost_known_usd")}} for row in grouped(rows, "machine")]} for t, rows in sorted(buckets.items())]
    timeline = timeline_for(current)
    # Facets deliberately ignore active filters: a selected empty slice stays selected.
    facets = {key: sorted({e[key] for e in current_all}) for key in FILTERS if key != "session"}
    facets["machine"] = sorted(set(facets["machine"]) | {m["machine"] for m in machine_activity})
    top = sorted(current, key=lambda e: (e["timestamp_ms"], e["id"]), reverse=True)[:20]
    return {
        "schema_version": VERSION, "generated_at_ms": now_ms, "semantics": SEMANTICS, "pricing": pricing.CONTRACT,
        "period": {"start_ms": start, "end_ms": end, "previous_start_ms": previous_start, "previous_end_ms": start, "bucket_ms": bucket_ms},
        "filters": filters, "facets": facets, "totals": totals,
        "comparison": {"totals": prior, "change_pct": changes, "has_observations": bool(previous)},
        "coverage": {"first_observed_ms": first_observed_ms, "current_starts_before_first_observation": first_observed_ms is None or start < first_observed_ms, "previous_starts_before_first_observation": first_observed_ms is None or previous_start < first_observed_ms, "collection_completeness": "unknown", "effort_known_pct": ratio(totals["effort_known_events"], totals["events"]), "tier_known_pct": ratio(totals["tier_known_events"], totals["events"])},
        "timeline": timeline, "previous_timeline": timeline_for(previous),
        "breakdowns": {key: grouped(current, key) for key in ("machine", "model", "effort", "tier")},
        "sessions": grouped(current, "session")[:100], "session_count": totals["sessions"],
        "recent_events": top, "machine_activity": machine_activity,
    }
