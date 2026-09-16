"""API-equivalent token cost at the published rate card, not subscription billing."""
from decimal import Decimal

AS_OF = "2026-09-06"
SOURCE = "https://developers.openai.com/api/docs/pricing"
CACHE_SOURCE = "https://developers.openai.com/api/docs/guides/prompt-caching"
# USD per million tokens: ordinary input, cached reads, cache writes, all output.
# Explicit rows keep unsupported models/tiers unknown; no guessed model-name prefixes.
RATES = {
    "gpt-6-astra": {"standard": (10, 1, 12.5, 50), "fast": (20, 2, 25, 100), "flex": (5, .5, 6.25, 25), "long": True},
    "gpt-5.6-sol": {"standard": (4, .4, 5, 20), "fast": (8, .8, 10, 40), "flex": (2, .2, 2.5, 10), "long": True},
    "gpt-5.6-terra": {"standard": (2, .2, 2.5, 12), "fast": (4, .4, 5, 24), "flex": (1, .1, 1.25, 6), "long": True},
    "gpt-5.6-luna": {"standard": (.2, .02, .25, 1.2), "fast": (.4, .04, .5, 2.4), "flex": (.1, .01, .125, .6), "long": True},
    "gpt-5.5": {"standard": (5, .5, None, 30), "fast": (12.5, 1.25, None, 75), "flex": (2.5, .25, None, 15), "long": True},
    "gpt-5.4-mini": {"standard": (.75, .075, None, 4.5), "fast": (1.5, .15, None, 9), "flex": (.375, .0375, None, 2.25), "long": False},
}
ALIASES = {"gpt-5.5-2026-04-23": "gpt-5.5", "gpt-5.4-mini-2026-03-17": "gpt-5.4-mini", "gpt-5.6": "gpt-5.6-sol"}
PARTS = ("api_ordinary_input_usd", "api_cached_input_usd", "api_cache_write_usd", "api_output_usd")
FIELDS = ("api_cost_usd", *PARTS)
CONTRACT = {
    "currency": "USD", "as_of": AS_OF, "source": SOURCE, "cache_source": CACHE_SOURCE,
    "basis": "Revalue observed text tokens at this rate card, including historical events. API equivalent, not the paid Codex subscription or a historical invoice.",
    "formula": "((input-cached-cache_write)*input_rate + cached*cached_rate + cache_write*write_rate + output*output_rate)/1e6. Reasoning is already in output.",
    "unknown_tier": "Missing/auto service tier uses standard rates only as an explicit price assumption; observed tier stays unchanged.",
    "missing": "Unknown models, unsupported rate combinations or missing required counters are unpriced. api_cost_usd is null if any event is unpriced; api_cost_known_usd is the priced subtotal.",
    "long_context": "GPT-5.6/6: >272000 input tokens doubles input/cache rates and multiplies output by 1.5. GPT-5.5: any recorded >272000-input event marks the machine/session as long; unknown pre-collection context is not reconstructed. GPT-5.5 Fast long-context rate is unpublished and stays unpriced.",
    "excluded": "Tool-call fees, regional uplifts, taxes, negotiated discounts and subscription/credit conversion are excluded.",
}


def estimate(event):
    result = {key: None for key in FIELDS}
    result.update(api_price_assumed_tier=False, api_price_tier=None, api_price_context=None, api_price_reason=None)
    model = ALIASES.get(event.get("model"), event.get("model"))
    card = RATES.get(model)
    if card is None:
        result["api_price_reason"] = "unknown_model"
        return result
    reported = event.get("tier", "unknown")
    tier = "standard" if reported in ("unknown", "auto") else reported
    result.update(api_price_assumed_tier=reported in ("unknown", "auto"), api_price_tier=tier)
    rates = card.get(tier)
    if rates is None:
        result["api_price_reason"] = "unsupported_tier"
        return result
    input_count, cached, output = (event.get(k) for k in ("input_tokens", "cached_input_tokens", "output_tokens"))
    write = event.get("cache_write_input_tokens") if rates[2] is not None else 0
    if any(v is None for v in (input_count, cached, output, write)):
        result["api_price_reason"] = "missing_token_breakdown"
        return result
    if min(input_count, cached, output, write) < 0 or cached + write > input_count:
        result["api_price_reason"] = "invalid_token_breakdown"
        return result
    long = input_count > 272000 or (model == "gpt-5.5" and event.get("api_session_long_context", False))
    result["api_price_context"] = "long" if long else "short"
    if long and (not card["long"] or (model == "gpt-5.5" and tier == "fast")):
        result["api_price_reason"] = "unsupported_long_context"
        return result
    counts = (input_count-cached-write, cached, write, output)
    costs = []
    for index, (count, rate) in enumerate(zip(counts, rates)):
        factor = Decimal("1.5" if index == 3 else "2") if long else Decimal(1)
        costs.append(Decimal(count) * Decimal(str(rate or 0)) * factor / 1000000)
    result.update({key: float(value) for key, value in zip(PARTS, costs)})
    result["api_cost_usd"] = float(sum(costs))
    return result
