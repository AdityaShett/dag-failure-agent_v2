import os
import uuid

from github import Auth, Github
from google.cloud import secretmanager

from app import repos_store
from app.diff_utils import apply_unified_diff

_secret_client = None
_token_cache = {}
_PROJECT_ID = os.environ.get("GCP_PROJECT")


def _secrets():
    global _secret_client
    if _secret_client is None:
        _secret_client = secretmanager.SecretManagerServiceClient()
    return _secret_client


def get_github_token(github_repo: str) -> str:
    if github_repo in _token_cache:
        return _token_cache[github_repo]

    data = repos_store.load_repos()
    entry = data.get(github_repo)
    if not entry:
        raise ValueError(f"'{github_repo}' is not registered — add it from the dashboard")

    secret_name = entry["token_secret"]
    version_name = f"projects/{_PROJECT_ID}/secrets/{secret_name}/versions/latest"
    token = _secrets().access_secret_version(name=version_name).payload.data.decode("utf-8")
    _token_cache[github_repo] = token
    return token


def open_draft_pr(github_repo: str, target_file: str, dag_id: str, task_id: str,
                   run_id: str, root_cause: str, proposed_fix: str,
                   confidence_score: float) -> dict:
    auth = Auth.Token(get_github_token(github_repo))
    gh = Github(auth=auth)
    repo = gh.get_repo(github_repo)

    base_branch = repo.default_branch
    base_sha = repo.get_branch(base_branch).commit.sha
    branch_name = f"agent-fix/{dag_id}-{uuid.uuid4().hex[:8]}"
    repo.create_git_ref(ref=f"refs/heads/{branch_name}", sha=base_sha)

    contents = repo.get_contents(target_file, ref=branch_name)
    current_source = contents.decoded_content.decode("utf-8")

    diff_applied = True
    fallback_reason = None
    try:
        patched_source = apply_unified_diff(current_source, proposed_fix)
    except Exception as e:
        diff_applied = False
        fallback_reason = f"{type(e).__name__}: {e}"
        patched_source = current_source + f"\n\n# agent fix could not be applied automatically\n# {fallback_reason}\n"

    repo.update_file(
        path=target_file,
        message=f"Agent-proposed fix for {task_id} failure ({run_id})",
        content=patched_source, sha=contents.sha, branch=branch_name,
    )

    tier = "high" if confidence_score >= 0.85 else "medium" if confidence_score >= 0.6 else "low"
    title_prefix = f"[agent][{tier}-confidence]" + ("" if diff_applied else " [no-diff-applied]")

    pr_body = (
        f"## Root cause\n{root_cause}\n\n"
        f"## Proposed fix\n```diff\n{proposed_fix}\n```\n\n"
        f"**Confidence score:** {confidence_score} ({tier})\n\n"
        f"**Diff applied:** {diff_applied}" + (f" ({fallback_reason})" if fallback_reason else "") + "\n\n"
        f"This pull request was opened automatically. A human review is required before merging.\n\n"
        f"Run: `{run_id}`  Task: `{task_id}`"
    )

    pr = repo.create_pull(
        title=f"{title_prefix} Fix for {dag_id}.{task_id} failure",
        body=pr_body, head=branch_name, base=base_branch, draft=True,
    )

    return {"pr_number": pr.number, "pr_url": pr.html_url, "diff_applied": diff_applied,
            "fallback_reason": fallback_reason}
