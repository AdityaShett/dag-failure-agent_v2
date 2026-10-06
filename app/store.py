import json
import os
import time
from datetime import datetime, timezone

from google.cloud import firestore

_client = None
WEIGHTS_SEED_PATH = os.environ.get("WEIGHTS_SEED_PATH", "config/weights.json")
IN_PROGRESS = ("processing", "scoring", "generating")


def _db():
    global _client
    if _client is None:
        _client = firestore.Client()
    return _client


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _seed() -> dict:
    with open(WEIGHTS_SEED_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def run_id_for(run_id: str, task_id: str) -> str:
    safe = f"{run_id}-{task_id}"
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in safe)


def get_run(doc_id: str):
    snap = _db().collection("runs").document(doc_id).get()
    return snap.to_dict() if snap.exists else None


def upsert_run(doc_id: str, fields: dict):
    fields = dict(fields)
    fields["updated_at"] = _now()
    ref = _db().collection("runs").document(doc_id)
    if not ref.get().exists:
        fields.setdefault("created_at", fields["updated_at"])
    ref.set(fields, merge=True)


def claim_run(doc_id: str, fields: dict, stale_after: int, max_attempts: int) -> str:
    ref = _db().collection("runs").document(doc_id)

    @firestore.transactional
    def _txn(txn):
        snap = ref.get(transaction=txn)
        run = snap.to_dict() if snap.exists else {}
        status = run.get("status")
        if status in IN_PROGRESS:
            if time.time() - run.get("started_at", 0) < stale_after:
                return "in_progress"
        elif status and status != "error":
            return "duplicate_skipped"

        now = _now()
        attempts = run.get("attempts", 0) + 1
        if attempts > max_attempts:
            txn.set(ref, {"status": "error", "error": "gave up after repeated failures", "updated_at": now}, merge=True)
            return "error_giving_up"

        txn.set(ref, {
            **fields, "status": "processing", "attempts": attempts, "started_at": time.time(),
            "updated_at": now, "created_at": run.get("created_at", now),
        }, merge=True)
        return "claimed"

    return _txn(_db().transaction())


def transition_status(doc_id: str, from_statuses, to_status: str, extra: dict = None):
    db = _db()
    ref = db.collection("runs").document(doc_id)

    @firestore.transactional
    def _txn(txn):
        snap = ref.get(transaction=txn)
        if not snap.exists:
            return None
        current = (snap.to_dict() or {}).get("status")
        if current not in from_statuses:
            return current
        txn.update(ref, {**(extra or {}), "status": to_status, "updated_at": _now()})
        return True

    return _txn(db.transaction())


def find_by_pr_number(pr_number, github_repo: str = None):
    docs = []
    for candidate in (int(pr_number), str(pr_number)):
        docs = list(_db().collection("runs").where("pr_number", "==", candidate).limit(20).stream())
        if docs:
            break
    if not docs:
        return None, None

    if github_repo:
        same_repo = [d for d in docs if (d.to_dict().get("github_repo") or "").lower() == github_repo.lower()]
        docs = same_repo or docs
    docs.sort(key=lambda d: d.to_dict().get("status") != "opened")
    return docs[0].id, docs[0].to_dict()


def list_runs(limit: int = 200):
    docs = (
        _db().collection("runs")
        .order_by("updated_at", direction=firestore.Query.DESCENDING)
        .limit(limit)
        .stream()
    )
    results = []
    for d in docs:
        data = d.to_dict()
        data["id"] = d.id
        results.append(data)
    return results


def delete_run(doc_id: str):
    _db().collection("runs").document(doc_id).delete()


def query_runs(field: str, value, limit: int = 50):
    out = []
    for d in _db().collection("runs").where(field, "==", value).limit(limit).stream():
        data = d.to_dict()
        data["id"] = d.id
        out.append(data)
    return out


def get_weights() -> dict:
    ref = _db().collection("config").document("weights")
    snap = ref.get()
    seed = _seed()
    if snap.exists:
        cfg = snap.to_dict()
        for name, w in seed["weights"].items():
            cfg["weights"].setdefault(name, w)
        return cfg
    ref.set(seed)
    return seed


def save_weights(cfg: dict):
    _db().collection("config").document("weights").set(cfg, merge=True)


def update_weights(mutator) -> dict:
    db = _db()
    ref = db.collection("config").document("weights")
    seed = _seed()

    @firestore.transactional
    def _txn(txn):
        snap = ref.get(transaction=txn)
        cfg = snap.to_dict() if snap.exists else json.loads(json.dumps(seed))
        for name, w in seed["weights"].items():
            cfg["weights"].setdefault(name, w)
        new_cfg = mutator(cfg)
        txn.set(ref, new_cfg, merge=True)
        return new_cfg

    return _txn(db.transaction())


def append_weight_snapshot(weights: dict, source: str, run_doc_id=None, merged=None, **extra):
    _db().collection("weight_history").add({
        "ts": _now(),
        "weights": dict(weights), "source": source,
        "run_id": run_doc_id, "merged": merged,
        **extra,
    })


def list_weight_history(limit: int = 500):
    docs = (_db().collection("weight_history")
            .order_by("ts", direction=firestore.Query.DESCENDING).limit(limit).stream())
    return list(reversed([d.to_dict() for d in docs]))