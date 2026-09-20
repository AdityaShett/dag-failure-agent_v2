import base64
import hashlib
import hmac
import json
import logging
import os

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from google.cloud import secretmanager

from app import pipeline, repos_store, store, tuning

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


@app.post("/failure")
async def failure(request: Request):
    envelope = await request.json()
    message = envelope.get("message", {})
    try:
        payload = json.loads(base64.b64decode(message.get("data", "")).decode("utf-8"))
    except Exception as e:
        logger.error(f"bad pubsub payload: {e}")
        return {"status": "error", "message": str(e)}

    try:
        result = pipeline.process_failure(
            dag_id=payload.get("dag_id"), task_id=payload.get("task_id"),
            run_id=payload.get("run_id"), try_number=payload.get("try_number", 1),
        )
    except Exception as e:
        logger.exception(f"pipeline failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    return result


@app.post("/github-webhook")
async def github_webhook(request: Request):
    body = await request.body()
    signature = request.headers.get("X-Hub-Signature-256", "")
    expected = "sha256=" + hmac.new(_webhook_secret_value().encode(), body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise HTTPException(status_code=401, detail="invalid signature")

    if request.headers.get("X-GitHub-Event") != "pull_request":
        return {"status": "ignored"}

    payload = json.loads(body)
    if payload.get("action") != "closed":
        return {"status": "ignored"}

    pr = payload["pull_request"]
    pr_number = pr["number"]
    merged = bool(pr.get("merged"))

    doc_id, run = store.find_by_pr_number(pr_number)
    if not run:
        return {"status": "no_matching_run"}

    store.upsert_run(doc_id, {"status": "merged" if merged else "closed"})
    tuning.apply_outcome(run, merged=merged, proposed_fix=run.get("proposed_fix", ""))

    return {"status": "recorded", "merged": merged}


@app.get("/api/runs")
async def api_runs():
    return store.list_runs()


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
    return cfg


@app.get("/api/repos")
async def api_repos():
    return repos_store.load_repos()


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