import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from google.cloud import secretmanager

from app import outcomes, pipeline, reconcile, repos_store, store

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

_PROJECT = os.environ.get("GCP_PROJECT")
_secret_client = None
_webhook_secret = None


def _webhook_secret_value() -> str:
    global _secret_client, _webhook_secret
    if _webhook_secret is not None:
        return _webhook_secret
    if _secret_client is None:
        _secret_client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{_PROJECT}/secrets/github-webhook-secret/versions/latest"
    _webhook_secret = _secret_client.access_secret_version(name=name).payload.data.decode("utf-8")
    return _webhook_secret


def _signature_ok(body: bytes, signature: str) -> bool:
    """Check the HMAC; on mismatch drop the cached secret and retry once (handles secret rotation)."""
    global _webhook_secret
    sig_bytes = (signature or "").encode("utf-8")
    for _ in range(2):
        expected = "sha256=" + hmac.new(_webhook_secret_value().encode(), body, hashlib.sha256).hexdigest()
        if hmac.compare_digest(expected.encode("utf-8"), sig_bytes):
            return True
        _webhook_secret = None
    return False


@app.post("/failure")
async def failure(request: Request):
    envelope = await request.json()
    message = envelope.get("message") or {}
    try:
        payload = json.loads(base64.b64decode(message["data"]).decode("utf-8"))
    except Exception as e:
        # bad message: return 200 so Pub/Sub doesn't redeliver it forever
        logger.warning("failure: undecodable Pub/Sub message: %s", e)
        return {"status": "bad_message"}

    try:
        result = await asyncio.to_thread(
            pipeline.process_failure,
            payload.get("dag_id"), payload.get("task_id"),
            payload.get("run_id"), payload.get("try_number", 1),
        )
    except Exception as e:
        logger.exception(f"pipeline failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    if result.get("status") == "in_progress":
        raise HTTPException(status_code=503, detail="run in progress")
    return result


@app.post("/github-webhook")
async def github_webhook(request: Request):
    body = await request.body()
    signature = request.headers.get("X-Hub-Signature-256", "")
    if not _signature_ok(body, signature):
        logger.warning("webhook: invalid signature")
        raise HTTPException(status_code=401, detail="invalid signature")

    event = request.headers.get("X-GitHub-Event")
    if event != "pull_request":
        logger.info("webhook: ignored event=%s", event)
        return {"status": "ignored", "event": event}

    payload = json.loads(body)
    action = payload.get("action")
    if action != "closed":
        logger.info("webhook: ignored pull_request action=%s", action)
        return {"status": "ignored", "action": action}

    pr = payload["pull_request"]
    pr_number = int(pr["number"])
    merged = bool(pr.get("merged"))
    repo_full = (payload.get("repository") or {}).get("full_name")

    doc_id, run = store.find_by_pr_number(pr_number, repo_full)
    if not run:
        logger.warning("webhook: no run found for PR #%s in %s", pr_number, repo_full)
        return {"status": "no_matching_run", "pr_number": pr_number}

    result = outcomes.record_outcome(doc_id, run, merged=merged, source="webhook")
    logger.info("webhook: PR #%s merged=%s run=%s -> %s", pr_number, merged, doc_id, result.get("status"))
    return result


@app.post("/api/reconcile")
async def api_reconcile():
    report = reconcile.reconcile_open_prs()
    return {"checked": len(report), "results": report}


@app.get("/api/runs")
async def api_runs():
    return store.list_runs()


@app.delete("/api/runs/{doc_id}")
async def api_delete_run(doc_id: str):
    store.delete_run(doc_id)
    return {"status": "deleted", "id": doc_id}


@app.get("/api/weights")
async def api_weights():
    return store.get_weights()


@app.patch("/api/weights")
async def api_update_weights(request: Request):
    body = await request.json()
    cfg = store.get_weights()
    for key in ("weights", "penalty", "threshold"):
        if key in body:
            cfg[key] = body[key]
    store.save_weights(cfg)
    if "weights" in body:
        store.append_weight_snapshot(cfg["weights"], source="manual")
    return cfg


@app.get("/api/weight-history")
async def api_weight_history():
    return store.list_weight_history()


@app.get("/api/repos")
async def api_repos():
    return repos_store.load_repos()


@app.delete("/api/repos/{github_repo:path}")
async def api_delete_repo(github_repo: str):
    return repos_store.delete_repo(github_repo)


@app.post("/api/repos")
async def api_add_repo(request: Request):
    body = await request.json()
    return repos_store.add_repo(
        github_repo=body["github_repo"], token_secret=body["token_secret"],
        dag_path_template=body.get("dag_path_template", "dags/{dag_id}.py"),
        set_default=bool(body.get("set_default")),
    )


@app.get("/")
async def dashboard():
    return FileResponse(os.path.join(os.path.dirname(__file__), "dashboard", "index.html"))


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}