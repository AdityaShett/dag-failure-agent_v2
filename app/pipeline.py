import os

from google import genai

from app import confidence, context, github_ops, repos_store, store

_GENAI_CLIENT = None
_MODEL = os.environ.get("GEMINI_MODEL_NAME", "gemini-2.5-flash")
_PROJECT = os.environ.get("GCP_PROJECT")
_LOCATION = os.environ.get("GCP_LOCATION", "global")


def _genai():
    global _GENAI_CLIENT
    if _GENAI_CLIENT is None:
        _GENAI_CLIENT = genai.Client(vertexai=True, project=_PROJECT, location=_LOCATION)
    return _GENAI_CLIENT


def _ask_root_cause(dag_id, task_id, logs, source):
    prompt = (
        "You are an Airflow reliability engineer. Find the exact root cause of this "
        "task failure. Point to the specific log lines and code lines that prove it. "
        "If unclear, say so instead of guessing.\n\n"
        f"DAG: {dag_id} | Task: {task_id}\n\n--- LOGS ---\n{logs[-4000:]}\n\n"
        f"--- CODE ---\n{source}"
    )
    resp = _genai().models.generate_content(model=_MODEL, contents=prompt)
    return resp.text or ""


def _ask_fix(root_cause, source):
    prompt = (
        "Propose the smallest possible safe fix, as a git diff only.\n\n"
        "Before proposing anything, check whether the root cause actually matches the "
        "code shown below. If the described bug is not present in the current source, "
        "or you're not certain the exact lines you'd change still look this way, do NOT "
        "guess a diff — respond with NO_CONFIDENT_FIX instead.\n\n"
        "Respond in EXACTLY this format, nothing else:\nDIFF:\n<the git diff, or "
        "NO_CONFIDENT_FIX>\n\n"
        f"Root cause:\n{root_cause}\n\nCurrent code:\n{source}"
    )
    resp = _genai().models.generate_content(model=_MODEL, contents=prompt)
    text = resp.text or ""
    if "DIFF:" in text:
        return text.split("DIFF:", 1)[1].strip()
    return "NO_CONFIDENT_FIX"


def process_failure(dag_id: str, task_id: str, run_id: str, try_number: int = 1) -> dict:
    doc_id = store.run_id_for(run_id, task_id)
    if store.get_run(doc_id):
        return {"status": "duplicate_skipped"}

    repo_info = repos_store.resolve_repo(dag_id)
    github_repo, target_file = repo_info["github_repo"], repo_info["target_file"]

    logs = context.fetch_task_logs(
        context.build_task_log_filter(dag_id=dag_id, task_id=task_id, run_id=run_id)
    )
    source = context.fetch_dag_source(github_repo, target_file)

    store.upsert_run(doc_id, {
        "dag_id": dag_id, "task_id": task_id, "run_id": run_id,
        "github_repo": github_repo, "status": "scoring",
    })

    root_cause = _ask_root_cause(dag_id, task_id, logs, source)
    proposed_fix = _ask_fix(root_cause, source)

    signals = confidence.extract_signals(logs, source, dag_id, task_id)
    weights = store.get_weights()
    result = confidence.score(signals, weights["weights"])

    store.upsert_run(doc_id, {
        "signals": signals, "contributions": result["contributions"],
        "confidence_score": result["score"],
    })

    if proposed_fix.strip() == "NO_CONFIDENT_FIX" or result["score"] < weights["threshold"]:
        store.upsert_run(doc_id, {"status": "gated"})
        return {"status": "gated", "confidence_score": result["score"]}

    pr = github_ops.open_draft_pr(
        github_repo=github_repo, target_file=target_file, dag_id=dag_id, task_id=task_id,
        run_id=run_id, root_cause=root_cause, proposed_fix=proposed_fix,
        confidence_score=result["score"],
    )

    store.upsert_run(doc_id, {
        "status": "opened", "pr_number": pr["pr_number"], "pr_url": pr["pr_url"],
        "diff_applied": pr["diff_applied"], "fallback_reason": pr["fallback_reason"],
        "confidence_shares": result["shares"], "proposed_fix": proposed_fix,
    })

    return {"status": "opened", "pr_url": pr["pr_url"], "confidence_score": result["score"]}
