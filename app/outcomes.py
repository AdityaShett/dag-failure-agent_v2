import logging

from app import store, tuning

logger = logging.getLogger(__name__)


def record_outcome(doc_id: str, run: dict, merged: bool, source: str = "webhook") -> dict:

    new_status = "merged" if merged else "closed"
    moved = store.transition_status(doc_id, ("opened",), new_status, {"outcome_source": source})
    if moved is not True:
        logger.info("outcome skipped: run=%s current_status=%s", doc_id, moved)
        return {"status": "skipped", "reason": "run_not_in_opened_state", "run_status": moved}

    try:
        tuned = tuning.apply_outcome(
            run, merged=merged, proposed_fix=run.get("proposed_fix", ""), doc_id=doc_id)
    except Exception as e:                       # status is already correct; don't lose that
        logger.exception("tuning failed for run %s", doc_id)
        store.upsert_run(doc_id, {"tuning_error": f"{type(e).__name__}: {e}"[:500]})
        tuned = {"applied": False, "error": str(e)}

    return {"status": "recorded", "merged": merged, "tuning": tuned}