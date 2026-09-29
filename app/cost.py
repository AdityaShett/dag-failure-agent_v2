PRICING_USD_PER_M = {
    "gemini-2.5-flash": {"input": 0.30, "output": 2.50},
}


def usage_from_response(resp) -> dict:
    u = getattr(resp, "usage_metadata", None)
    thoughts = getattr(u, "thoughts_token_count", 0) or 0
    return {
        "input_tokens": getattr(u, "prompt_token_count", 0) or 0,
        "output_tokens": (getattr(u, "candidates_token_count", 0) or 0) + thoughts,
        "thinking_tokens": thoughts,
    }


def build_cost_record(model: str, calls: dict) -> dict:
    in_tok = sum(c["input_tokens"] for c in calls.values())
    out_tok = sum(c["output_tokens"] for c in calls.values())
    record = {"model": model, "calls": calls, "input_tokens": in_tok, "output_tokens": out_tok}
    rates = PRICING_USD_PER_M.get(model)
    if rates:
        in_cost = in_tok / 1e6 * rates["input"]
        out_cost = out_tok / 1e6 * rates["output"]
        record.update(rates_usd_per_m=rates, input_cost_usd=in_cost,
                      output_cost_usd=out_cost, total_cost_usd=in_cost + out_cost)
    return record