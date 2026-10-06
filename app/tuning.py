import logging

from app import diff_utils, store

logger = logging.getLogger(__name__)

SMALL_DIFF_LINES = 3
WEIGHT_FLOOR = 0.01


def apply_outcome(run: dict, merged: bool, proposed_fix: str = "", doc_id: str = None) -> dict:

    if run.get("diff_applied") is False:
        return {"applied": False, "reason": "no fix was proposed"}

    shares = run.get("confidence_shares")
    confidence_score = run.get("confidence_score")
    if not shares or confidence_score is None:
        return {"applied": False, "reason": "run has no confidence_shares/score"}

    small = bool(merged) and diff_utils.diff_line_count(proposed_fix) < SMALL_DIFF_LINES
    deltas = {}

    def mutate(cfg):
        step = cfg["penalty"]
        weights = dict(cfg["weights"])
        deltas.clear()
        for name, share in shares.items():
            if name not in weights:
                continue
            if merged and not small:
                change = step * share
            elif merged and small:
                change = 0.0
            else:
                change = -step * confidence_score * share
            new = max(WEIGHT_FLOOR, weights[name] + change)
            deltas[name] = round(new - weights[name], 6)
            weights[name] = new
        cfg["weights"] = weights
        return cfg

    cfg = store.update_weights(mutate)
    inactive = sorted(n for n, s in shares.items() if not s)
    branch = "reward" if (merged and not small) else "small_diff_penalty" if merged else "closed_penalty"

    store.append_weight_snapshot(
        cfg["weights"], source="outcome", run_doc_id=doc_id, merged=merged,
        deltas=deltas, inactive_signals=inactive, branch=branch,
    )
    summary = {"applied": True, "branch": branch, "deltas": deltas, "inactive_signals": inactive}
    if doc_id:
        store.upsert_run(doc_id, {"tuning": summary})
    logger.info("tuning: run=%s merged=%s branch=%s deltas=%s inactive=%s",
                doc_id, merged, branch, deltas, inactive)
    return summary