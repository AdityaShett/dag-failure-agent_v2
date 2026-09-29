import json
import os
from datetime import datetime, timezone

from google.cloud import firestore

_client = None
WEIGHTS_SEED_PATH = os.environ.get("WEIGHTS_SEED_PATH", "config/weights.json")


def _db():
    global _client
    if _client is None:
        _client = firestore.Client()
    return _client


def run_id_for(run_id: str, task_id: str) -> str:
    safe = f"{run_id}-{task_id}"
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in safe)


def get_run(doc_id: str):
    snap = _db().collection("runs").document(doc_id).get()
    return snap.to_dict() if snap.exists else None


def upsert_run(doc_id: str, fields: dict):
    fields = dict(fields)
    fields["updated_at"] = datetime.now(timezone.utc).isoformat()
    ref = _db().collection("runs").document(doc_id)
    if not ref.get().exists:
        fields.setdefault("created_at", fields["updated_at"])
    ref.set(fields, merge=True)


def find_by_pr_number(pr_number: int):
    docs = (
        _db().collection("runs")
        .where("pr_number", "==", pr_number)
        .limit(1)
        .stream()
    )
    for d in docs:
        return d.id, d.to_dict()
    return None, None


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
    with open(WEIGHTS_SEED_PATH, "r", encoding="utf-8") as f:
        seed = json.load(f)
    if snap.exists:
        cfg = snap.to_dict()
        for name, w in seed["weights"].items():
            cfg["weights"].setdefault(name, w)   # picks up newly added signals
        return cfg
    ref.set(seed)
    return seed


def save_weights(cfg: dict):
    _db().collection("config").document("weights").set(cfg, merge=True)


def append_weight_snapshot(weights: dict, source: str, run_doc_id=None, merged=None):
    _db().collection("weight_history").add({
        "ts": datetime.now(timezone.utc).isoformat(),
        "weights": dict(weights), "source": source,
        "run_id": run_doc_id, "merged": merged,
    })


def list_weight_history(limit: int = 500):
    docs = (_db().collection("weight_history")
            .order_by("ts", direction=firestore.Query.DESCENDING).limit(limit).stream())
    return list(reversed([d.to_dict() for d in docs]))