from app import diff_utils, store

SMALL_DIFF_LINES = 3
WEIGHT_FLOOR = 0.01


def apply_outcome(run: dict, merged: bool, proposed_fix: str = ""):
    shares = run.get("confidence_shares")
    confidence_score = run.get("confidence_score")
    if not shares or confidence_score is None:
        return

    cfg = store.get_weights()
    weights = dict(cfg["weights"])
    step = cfg["penalty"]

    small = merged and diff_utils.diff_line_count(proposed_fix) < SMALL_DIFF_LINES

    for name, share in shares.items():
        if name not in weights:
            continue
        if merged and not small:
            weights[name] += step * share
        elif merged and small:
            weights[name] -= cfg["penalty"] * share
        else:
            weights[name] -= cfg["penalty"] * confidence_score * share
        weights[name] = max(WEIGHT_FLOOR, weights[name])

    cfg["weights"] = weights
    store.save_weights(cfg)
