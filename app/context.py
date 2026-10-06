import time
from datetime import datetime, timezone

from google.cloud import logging as cloud_logging

from app.confidence import parse_log


def _parse_failure_time_from_run_id(run_id: str) -> datetime:
    # vestigial: kept for call-site compatibility
    _, _, ts = (run_id or "").partition("__")
    if not ts:
        return datetime.now(timezone.utc)
    try:
        parsed = datetime.fromisoformat(ts)
    except ValueError:
        return datetime.now(timezone.utc)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def build_task_log_filter(dag_id: str, task_id: str, run_id: str = None,
                           lookback_minutes: int = 120) -> str:
    parts = [
        'resource.type="cloud_composer_environment"',
        'logName:"logs/airflow-worker"',
    ]
    if dag_id:
        parts.append(f'labels."workflow"="{dag_id}"')
    if task_id:
        parts.append(f'labels."task-id"="{task_id}"')
    if run_id:
        parts.append(f'labels."run-id"="{run_id}"')
    return " AND ".join(parts)


def _payload_to_text(payload) -> str:
    if isinstance(payload, str):
        return payload
    if isinstance(payload, dict):
        return payload.get("message") or payload.get("textPayload") or str(payload)
    return str(payload)


def _looks_complete(text: str) -> bool:
    """True once the log contains a parseable traceback (frames + exception line)."""
    parsed = parse_log(text)
    return bool(parsed["frames"] and parsed["exception_type"])


def fetch_task_logs(filter_str: str, max_retries: int = 6, base_delay: int = 2) -> str:

    client = cloud_logging.Client()
    best = ""

    for attempt in range(max_retries):
        try:
            entries = list(client.list_entries(
                filter_=filter_str, order_by=cloud_logging.DESCENDING, max_results=200))
            text = "\n".join(reversed([_payload_to_text(e.payload) for e in entries]))
            if len(text) > len(best):
                best = text
            if _looks_complete(text):
                return text
        except Exception:
            if attempt == max_retries - 1 and not best:
                raise
        if attempt < max_retries - 1:
            time.sleep(base_delay * (2 ** attempt))

    return best


def fetch_dag_source(github_repo: str, target_file: str, ref: str = None) -> str:
    from github import Auth, Github
    from app.github_ops import get_github_token

    auth = Auth.Token(get_github_token(github_repo))
    gh = Github(auth=auth)
    repo = gh.get_repo(github_repo)

    resolved_ref = ref or repo.default_branch
    try:
        contents = repo.get_contents(target_file, ref=resolved_ref)
    except Exception:
        if ref and ref != repo.default_branch:
            contents = repo.get_contents(target_file, ref=repo.default_branch)
        else:
            raise

    return contents.decoded_content.decode("utf-8")