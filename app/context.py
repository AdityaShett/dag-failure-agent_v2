import time
from datetime import datetime, timedelta, timezone

from google.cloud import logging as cloud_logging
from google.api_core.exceptions import ResourceExhausted


def _parse_failure_time_from_run_id(run_id: str) -> datetime:
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
    failure_time = _parse_failure_time_from_run_id(run_id or "")
    start = failure_time - timedelta(minutes=lookback_minutes)
    end = failure_time + timedelta(minutes=lookback_minutes)

    parts = [
        'resource.type="cloud_composer_environment"',
        f'timestamp >= "{start.astimezone(timezone.utc).isoformat()}"',
        f'timestamp <= "{end.astimezone(timezone.utc).isoformat()}"',
    ]
    if dag_id and task_id:
        parts.append(f'(textPayload:"{dag_id}" OR textPayload:"{task_id}")')
    elif dag_id:
        parts.append(f'textPayload:"{dag_id}"')
    elif task_id:
        parts.append(f'textPayload:"{task_id}"')
    return " AND ".join(parts)


def _payload_to_text(payload) -> str:
    if isinstance(payload, str):
        return payload
    if isinstance(payload, dict):
        return payload.get("message") or payload.get("textPayload") or str(payload)
    return str(payload)


def fetch_task_logs(filter_str: str) -> str:
    client = cloud_logging.Client()
    max_retries = 5
    base_delay = 2

    for attempt in range(max_retries):
        try:
            entries = list(
                client.list_entries(
                    filter_=filter_str,
                    order_by=cloud_logging.DESCENDING,
                    max_results=200,
                )
            )
            if entries:
                lines = [_payload_to_text(e.payload) for e in entries]
                return "\n".join(reversed(lines))
            if attempt < max_retries - 1:
                time.sleep(base_delay * (2 ** attempt))
                continue
            return ""
        except ResourceExhausted:
            if attempt == max_retries - 1:
                raise
            time.sleep(base_delay * (2 ** attempt))
        except Exception:
            if attempt == max_retries - 1:
                raise
            time.sleep(base_delay * (2 ** attempt))

    return ""


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
