import os
import re

from google import genai
from google.genai import types

from app import confidence, context, cost, github_ops, history, repos_store, store

_GENAI_CLIENT = None
_MODEL = os.environ.get("GEMINI_MODEL_NAME", "gemini-2.5-flash")
_PROJECT = os.environ.get("GCP_PROJECT")
_LOCATION = os.environ.get("GCP_LOCATION", "global")

NO_FIX = "NO_CONFIDENT_FIX"
FIX_ATTEMPTS = 2              # 2nd attempt only happens when the failing line is verified in source
MAX_PROCESS_ATTEMPTS = 3      # Pub/Sub redeliveries of a run that errored mid-pipeline
LINE_VERIFIED_MIN = 0.6       # line_number_matches_source >= this => failing code exists in source
_ZERO_USAGE = {"input_tokens": 0, "output_tokens": 0, "thinking_tokens": 0}


def _genai():
    global _GENAI_CLIENT
    if _GENAI_CLIENT is None:
        _GENAI_CLIENT = genai.Client(vertexai=True, project=_PROJECT, location=_LOCATION)
    return _GENAI_CLIENT


def _generate(prompt: str):
    # temperature 0: same logs + same source should give the same answer every time
    resp = _genai().models.generate_content(
        model=_MODEL, contents=prompt,
        config=types.GenerateContentConfig(temperature=0.0),
    )
    return resp.text or "", cost.usage_from_response(resp)


def _focus_logs(logs: str, before: int = 1000, after: int = 6000) -> str:
    """Give the model the traceback itself, not just the last N chars of a noisy log."""
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


def _parse_fix(text: str) -> str:
    """Return a clean diff string, or NO_FIX. Tolerates a missing 'DIFF:' header and code fences."""
    body = text.split("DIFF:", 1)[1] if "DIFF:" in text else text
    body = re.sub(r"^\s*```[a-zA-Z]*\s*\n", "", body.strip())
    body = re.sub(r"\n```\s*$", "", body).strip()
    if not body or NO_FIX in body:
        return NO_FIX
    if not re.search(r"^(diff --git|--- |\+\+\+ |@@)", body, re.M):
        return NO_FIX                    
    return body


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


def _fix_prompt(root_cause, source, evidence, insist):
    if insist:
        gate = ("The failing line has been mechanically verified to exist in the current "
                "source, so the bug IS present. Do not answer NO_CONFIDENT_FIX unless a safe "
                "fix is impossible without changing other files.\n\n")
    else:
        gate = ("Before proposing anything, check whether the root cause actually matches the "
                "code shown below. If the described bug is not present in the current source, "
                "or you're not certain the exact lines you'd change still look this way, do NOT "
                "guess a diff — respond with NO_CONFIDENT_FIX instead.\n\n")
    return (
        "Propose the smallest possible safe fix, as a git diff only.\n\n" + gate
        + "Respond in EXACTLY this format, nothing else:\nDIFF:\n<the git diff, or NO_CONFIDENT_FIX>\n\n"
        + (f"Failure evidence:\n{evidence}\n\n" if evidence else "")
        + f"Root cause:\n{root_cause}\n\nCurrent code:\n{source}"
    )


def _ask_fix(root_cause, source, evidence="", line_verified=False):
    total, raw, fix, attempts = dict(_ZERO_USAGE), "", NO_FIX, 0
    for attempt in range(FIX_ATTEMPTS):
        insist = attempt > 0
        if insist and not line_verified:
            break                          # nothing new to tell the model; a retry would be identical
        raw, usage = _generate(_fix_prompt(root_cause, source, evidence, insist))
        attempts += 1
        for k in total:
            total[k] += usage.get(k, 0)
        fix = _parse_fix(raw)
        if fix != NO_FIX:
            break
    return fix, total, {"attempts": attempts, "raw": raw}


# ---------------------------------------------------------------- pipeline

def process_failure(dag_id: str, task_id: str, run_id: str, try_number: int = 1) -> dict:
    doc_id = store.run_id_for(run_id, task_id)
    existing = store.get_run(doc_id)
    if existing and existing.get("status") != "error":
        return {"status": "duplicate_skipped"}

    attempts = (existing or {}).get("attempts", 0) + 1
    if attempts > MAX_PROCESS_ATTEMPTS:
        return {"status": "error_giving_up"}       # 200 so Pub/Sub stops redelivering

    repo_info = repos_store.resolve_repo(dag_id)
    github_repo, target_file = repo_info["github_repo"], repo_info["target_file"]
    store.upsert_run(doc_id, {
        "dag_id": dag_id, "task_id": task_id, "run_id": run_id,
        "github_repo": github_repo, "status": "processing", "attempts": attempts,
    })

    try:
        return _process(doc_id, dag_id, task_id, run_id, github_repo, target_file)
    except Exception as e:
        store.upsert_run(doc_id, {"status": "error", "error": f"{type(e).__name__}: {e}"[:500]})
        raise


def _process(doc_id, dag_id, task_id, run_id, github_repo, target_file) -> dict:
    logs = context.fetch_task_logs(
        context.build_task_log_filter(dag_id=dag_id, task_id=task_id, run_id=run_id)
    )
    source = context.fetch_dag_source(github_repo, target_file)
    store.upsert_run(doc_id, {"status": "scoring"})

    # ---- scoring: fully deterministic, does not depend on the LLM ----
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
    line_verified = line_signal >= LINE_VERIFIED_MIN
    # traceback names a concrete code line, but that line is nowhere in the source we fetched:
    # GitHub and the deployed DAG (Composer) have diverged.
    source_drift = bool(source.strip() and frame.get("code")) and line_signal == 0.0

    store.upsert_run(doc_id, {
        "signals": signals, "contributions": result["contributions"],
        "confidence_shares": result["shares"],
        "confidence_score": result["score"], "threshold": threshold,
        "exception_type": exc, "error_fingerprint": fingerprint,
        "history_matches": related, "log_chars": len(logs),
        "source_drift": source_drift,
    })

    # ---- LLM: root cause, then fix (fix is skipped when source has drifted) ----
    evidence = _evidence(parsed)
    root_cause, rc_usage = _ask_root_cause(dag_id, task_id, logs, source, evidence)
    if source_drift:
        proposed_fix, fix_usage, fix_meta = NO_FIX, dict(_ZERO_USAGE), {"attempts": 0, "raw": ""}
    else:
        proposed_fix, fix_usage, fix_meta = _ask_fix(root_cause, source, evidence, line_verified)
    run_cost = cost.build_cost_record(_MODEL, {"root_cause": rc_usage, "fix": fix_usage})
    store.upsert_run(doc_id, {"cost": run_cost, "fix_attempts": fix_meta["attempts"]})

    # ---- gate ----
    refused = proposed_fix == NO_FIX
    below_threshold = result["score"] < threshold
    if refused or below_threshold:
        if below_threshold:
            reason = "below_threshold"
        elif source_drift:
            reason = "source_out_of_sync"
        else:
            reason = "model_refused"
        store.upsert_run(doc_id, {
            "status": "gated", "gate_reason": reason,
            "refused": refused, "below_threshold": below_threshold,
            "proposed_fix": proposed_fix,
            "fix_raw": fix_meta["raw"][:1000] if refused else "",
        })
        return {"status": "gated", "gate_reason": reason, "confidence_score": result["score"]}

    pr = github_ops.open_draft_pr(
        github_repo=github_repo, target_file=target_file, dag_id=dag_id, task_id=task_id,
        run_id=run_id, root_cause=root_cause, proposed_fix=proposed_fix,
        confidence_score=result["score"],
    )
    store.upsert_run(doc_id, {
        "status": "opened", "pr_number": int(pr["pr_number"]), "pr_url": pr["pr_url"],
        "diff_applied": pr["diff_applied"], "fallback_reason": pr["fallback_reason"],
        "proposed_fix": proposed_fix,
    })
    return {"status": "opened", "pr_url": pr["pr_url"], "confidence_score": result["score"]}