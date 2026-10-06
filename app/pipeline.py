import logging
import os
import re
import time

from google import genai
from google.genai import types

from app import confidence, context, cost, diff_utils, github_ops, history, repos_store, store

logger = logging.getLogger(__name__)

_GENAI_CLIENT = None
_MODEL = os.environ.get("GEMINI_MODEL_NAME", "gemini-2.5-flash")
_PROJECT = os.environ.get("GCP_PROJECT")
_LOCATION = os.environ.get("GCP_LOCATION", "global")

NO_FIX = "NO_CONFIDENT_FIX"
FIX_ATTEMPTS = 3
GEMINI_ATTEMPTS = 3
MAX_PROCESS_ATTEMPTS = 5
STALE_AFTER_SECONDS = 600
_ZERO_USAGE = {"input_tokens": 0, "output_tokens": 0, "thinking_tokens": 0}


def _genai():
    global _GENAI_CLIENT
    if _GENAI_CLIENT is None:
        _GENAI_CLIENT = genai.Client(vertexai=True, project=_PROJECT, location=_LOCATION)
    return _GENAI_CLIENT


def _retry(fn, attempts: int = 3):
    for i in range(attempts):
        try:
            return fn()
        except Exception:
            if i == attempts - 1:
                raise
            logger.exception("attempt %s failed, retrying", i + 1)
            time.sleep(2 ** i)


def _generate(prompt: str):
    for attempt in range(GEMINI_ATTEMPTS):
        try:
            resp = _genai().models.generate_content(
                model=_MODEL, contents=prompt,
                config=types.GenerateContentConfig(temperature=0.0),
            )
            if resp.text:
                return resp.text, cost.usage_from_response(resp)
        except Exception:
            logger.exception("gemini call failed")
        time.sleep(2 ** attempt)
    return "", dict(_ZERO_USAGE)


def _focus_logs(logs: str, before: int = 1000, after: int = 6000) -> str:
    idx = logs.rfind("Traceback (most recent call last)")
    if idx == -1:
        return logs[-4000:]
    return logs[max(0, idx - before): idx + after]


def _evidence(parsed: dict) -> str:
    frame = parsed.get("app_frame")
    if not parsed.get("exception_type") or not frame:
        return ""
    line = f"{parsed['exception_type']} raised at {frame['file']} line {frame['line']} in {frame['func']}"
    if frame.get("code"):
        line += f" -> `{frame['code']}`"
    return f"Parsed from the traceback: {line}"


def _ask_root_cause(dag_id, task_id, logs, source, evidence=""):
    prompt = (
        "You are an Airflow reliability engineer. Find the exact root cause of this "
        "task failure. Point to the specific log lines and code lines that prove it. "
        "If unclear, say so instead of guessing.\n\n"
        f"DAG: {dag_id} | Task: {task_id}\n\n"
        + (f"{evidence}\n\n" if evidence else "")
        + f"--- LOGS ---\n{_focus_logs(logs)}\n\n--- CODE ---\n{source}"
    )
    return _generate(prompt)


def _fix_prompt(root_cause, source, evidence, error):
    return (
        "Fix this Airflow DAG file with the smallest possible change.\n"
        "Respond with the COMPLETE corrected file only: no explanation, no markdown fences. "
        f"Change only what the root cause requires. If the bug cannot be fixed in this file, respond with {NO_FIX}.\n\n"
        + (f"Failure evidence:\n{evidence}\n\n" if evidence else "")
        + (f"Your previous answer was rejected because {error}. Try again.\n\n" if error else "")
        + f"Root cause:\n{root_cause}\n\nCurrent code:\n{source}"
    )


def _syntax_error(code: str) -> str:
    try:
        compile(code, "dag.py", "exec")
    except SyntaxError as e:
        return str(e)
    return ""


def _validate(text: str, source: str):
    match = re.search(r"```\w*\s*\n(.*?)\n```", text, re.S)
    code = (match.group(1) if match else text).strip("\n")
    if not code.strip() or NO_FIX in code:
        return "", "you returned no file; give your best minimal fix, a human reviews it before merging"
    eol = "\r\n" if "\r\n" in source else "\n"
    code = code.replace("\r\n", "\n").replace("\n", eol) + eol
    if code.splitlines() == source.splitlines():
        return "", "the file is identical to the original"
    error = _syntax_error(code)
    if error and not _syntax_error(source):
        return "", f"the file has a syntax error: {error}"
    return code, ""


def _ask_fix(root_cause, source, evidence, attempts):
    usage, error, code, raw, tried = dict(_ZERO_USAGE), "", "", "", 0
    for _ in range(attempts):
        raw, used = _generate(_fix_prompt(root_cause, source, evidence, error))
        tried += 1
        for key in usage:
            usage[key] += used[key]
        code, error = _validate(raw, source)
        if code:
            break
    return code, error, usage, raw, tried


def process_failure(dag_id: str, task_id: str, run_id: str, try_number: int = 1) -> dict:
    doc_id = store.run_id_for(run_id, task_id)
    claim = store.claim_run(
        doc_id, {"dag_id": dag_id, "task_id": task_id, "run_id": run_id},
        STALE_AFTER_SECONDS, MAX_PROCESS_ATTEMPTS)
    if claim != "claimed":
        return {"status": claim}

    try:
        repo_info = repos_store.resolve_repo(dag_id)
        github_repo, target_file = repo_info["github_repo"], repo_info["target_file"]
        store.upsert_run(doc_id, {"github_repo": github_repo})
        return _process(doc_id, dag_id, task_id, run_id, github_repo, target_file)
    except Exception as e:
        logger.exception("run %s failed", doc_id)
        store.upsert_run(doc_id, {"status": "error", "error": f"{type(e).__name__}: {e}"[:500]})
        raise


def _process(doc_id, dag_id, task_id, run_id, github_repo, target_file) -> dict:
    logs = context.fetch_task_logs(
        context.build_task_log_filter(dag_id=dag_id, task_id=task_id, run_id=run_id)
    )
    source = _retry(lambda: context.fetch_dag_source(github_repo, target_file))
    store.upsert_run(doc_id, {"status": "scoring"})

    parsed = confidence.parse_log(logs)
    exc = (parsed["exception_type"] or "").rsplit(".", 1)[-1]
    fingerprint = confidence.error_fingerprint(parsed)
    related = history.find_related_failures(doc_id, dag_id, exc, fingerprint)

    signals = confidence.extract_signals(logs, source, dag_id, task_id, history=related)
    weights = store.get_weights()
    threshold = weights["threshold"]
    result = confidence.score(signals, weights["weights"])

    line_signal = signals["line_number_matches_source"]
    frame = parsed.get("app_frame") or {}
    source_drift = bool(source.strip() and frame.get("code")) and line_signal == 0.0

    store.upsert_run(doc_id, {
        "signals": signals, "contributions": result["contributions"],
        "confidence_shares": result["shares"],
        "confidence_score": result["score"], "threshold": threshold,
        "exception_type": exc, "error_fingerprint": fingerprint,
        "history_matches": related, "log_chars": len(logs),
        "source_drift": source_drift,
    })

    evidence = _evidence(parsed)
    root_cause, rc_usage = _ask_root_cause(dag_id, task_id, logs, source, evidence)
    above = result["score"] >= threshold
    fixed, fix_error, fix_usage, fix_raw, tried = _ask_fix(
        root_cause, source, evidence, FIX_ATTEMPTS if above else 1)
    proposed_fix = diff_utils.make_diff(source, fixed, target_file) if fixed else NO_FIX
    store.upsert_run(doc_id, {
        "cost": cost.build_cost_record(_MODEL, {"root_cause": rc_usage, "fix": fix_usage}),
        "fix_attempts": tried, "proposed_fix": proposed_fix,
    })

    if not above:
        store.upsert_run(doc_id, {
            "status": "gated", "gate_reason": "below_threshold",
            "refused": not fixed, "below_threshold": True,
            "fix_raw": "" if fixed else fix_raw[:1000],
        })
        return {"status": "gated", "gate_reason": "below_threshold", "confidence_score": result["score"]}

    new_source = fixed or (
        source.rstrip("\n") + "\n\n# agent could not propose an automatic fix, see the PR description\n")
    pr = _retry(lambda: github_ops.open_draft_pr(
        github_repo=github_repo, target_file=target_file, dag_id=dag_id, task_id=task_id,
        run_id=run_id, doc_id=doc_id, root_cause=root_cause, new_source=new_source,
        proposed_fix=proposed_fix, fallback_reason=fix_error or None,
        confidence_score=result["score"],
    ))
    store.upsert_run(doc_id, {
        "status": "opened", "pr_number": int(pr["pr_number"]), "pr_url": pr["pr_url"],
        "diff_applied": pr["diff_applied"], "fallback_reason": pr["fallback_reason"],
    })
    return {"status": "opened", "pr_url": pr["pr_url"], "confidence_score": result["score"]}