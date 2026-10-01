import difflib
import re

from app import store
from app.confidence import OUTCOME_VALUE

NAME_SIMILARITY_MIN = 0.6


def dag_family(dag_id: str) -> str:
    name = re.sub(r"^(test|dev|prod|staging)_", "", (dag_id or "").lower())
    return re.sub(r"(_[dv]?\d+|_(dev|prod|staging))+$", "", name)


def _similar_name(a, b) -> bool:
    fa, fb = dag_family(a), dag_family(b)
    return bool(fa and fb) and (
        fa == fb or difflib.SequenceMatcher(None, fa, fb).ratio() >= NAME_SIMILARITY_MIN
    )


def _strength(run, dag_id, exc, fp):
    same_dag = run.get("dag_id") == dag_id
    same_exc = bool(exc) and run.get("exception_type") == exc
    same_fp = bool(fp) and run.get("error_fingerprint") == fp
    if same_dag and same_exc:
        return 1.0, "same_dag_same_error"
    if same_fp:
        return 0.8, "same_error_fingerprint"
    if same_exc and _similar_name(run.get("dag_id"), dag_id):
        return 0.5, "similar_dag_same_error"
    if same_dag:
        return 0.3, "same_dag_other_error"
    return 0.0, None


def find_related_failures(current_doc_id, dag_id, exc, fingerprint):
    pool = {}
    for field, value, limit in (("dag_id", dag_id, 50),
                                ("error_fingerprint", fingerprint, 50),
                                ("exception_type", exc, 100)):
        if value:
            for run in store.query_runs(field, value, limit):
                pool[run["id"]] = run

    related = []
    for doc_id, run in pool.items():
        if doc_id == current_doc_id or run.get("status") not in ("merged", "closed") or run.get("diff_applied") is False:
            continue
        strength, reason = _strength(run, dag_id, exc, fingerprint)
        if strength:
            related.append({"id": doc_id, "dag_id": run.get("dag_id"),
                            "status": run["status"], "strength": strength, "reason": reason})
    return sorted(related, key=lambda r: -r["strength"])[:10]