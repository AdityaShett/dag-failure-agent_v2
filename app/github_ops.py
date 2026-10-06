import os

from github import Auth, Github, GithubException
from google.cloud import secretmanager

from app import repos_store

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
                  run_id: str, doc_id: str, root_cause: str, new_source: str,
                  proposed_fix: str, fallback_reason, confidence_score: float) -> dict:
    repo = Github(auth=Auth.Token(get_github_token(github_repo))).get_repo(github_repo)
    base_branch = repo.default_branch
    branch_name = f"agent-fix/{doc_id}"

    try:
        repo.create_git_ref(ref=f"refs/heads/{branch_name}", sha=repo.get_branch(base_branch).commit.sha)
    except GithubException as e:
        if e.status != 422:
            raise

    contents = repo.get_contents(target_file, ref=branch_name)
    if contents.decoded_content.decode("utf-8") != new_source:
        repo.update_file(
            path=target_file,
            message=f"Agent-proposed fix for {task_id} failure ({run_id})",
            content=new_source, sha=contents.sha, branch=branch_name,
        )

    applied = fallback_reason is None
    tier = "high" if confidence_score >= 0.85 else "medium" if confidence_score >= 0.6 else "low"
    title_prefix = f"[agent][{tier}-confidence]" + ("" if applied else " [no-diff-applied]")
    fix_section = (
        f"```diff\n{proposed_fix}\n```" if applied else
        f"No automatic fix could be produced ({fallback_reason}). A comment was added to the file "
        "so this pull request exists; use the root cause above."
    )

    pr_body = (
        f"## Root cause\n{root_cause or 'The model did not return a root cause.'}\n\n"
        f"## Proposed fix\n{fix_section}\n\n"
        f"**Confidence score:** {confidence_score} ({tier})\n\n"
        f"**Diff applied:** {applied}\n\n"
        "This pull request was opened automatically. A human review is required before merging.\n\n"
        f"Run: `{run_id}`  Task: `{task_id}`"
    )

    open_prs = list(repo.get_pulls(state="open", head=f"{repo.owner.login}:{branch_name}"))
    pr = open_prs[0] if open_prs else repo.create_pull(
        title=f"{title_prefix} Fix for {dag_id}.{task_id} failure",
        body=pr_body, head=branch_name, base=base_branch, draft=True,
    )

    return {"pr_number": pr.number, "pr_url": pr.html_url, "diff_applied": applied,
            "fallback_reason": fallback_reason}