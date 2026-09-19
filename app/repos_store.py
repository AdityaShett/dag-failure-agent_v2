import json
import os

from google.cloud import storage

_BUCKET = os.environ.get("REPOS_CONFIG_BUCKET")
_BLOB_NAME = os.environ.get("REPOS_CONFIG_BLOB", "repos.json")
_client = None
_cache = None


def _bucket():
    global _client
    if _client is None:
        _client = storage.Client()
    return _client.bucket(_BUCKET)


def load_repos(force: bool = False) -> dict:
    global _cache
    if _cache is not None and not force:
        return _cache

    blob = _bucket().blob(_BLOB_NAME)
    if not blob.exists():
        _cache = {}
        return _cache

    _cache = json.loads(blob.download_as_text())
    return _cache


def save_repos(data: dict):
    global _cache
    blob = _bucket().blob(_BLOB_NAME)
    blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")
    _cache = data


def add_repo(github_repo: str, token_secret: str, dag_path_template: str, set_default: bool = False):
    data = load_repos(force=True)
    data[github_repo] = {"token_secret": token_secret, "dag_path_template": dag_path_template}
    if set_default or "default" not in data:
        data["default"] = github_repo
    save_repos(data)
    return data


def resolve_repo(dag_id: str) -> dict:
    data = load_repos()
    overrides = data.get("dag_overrides", {})
    github_repo = overrides.get(dag_id) or data.get("default")
    if not github_repo or github_repo not in data:
        raise ValueError("no default github repo registered — add one from the dashboard")
    template = data[github_repo].get("dag_path_template", "dags/{dag_id}.py")
    return {"github_repo": github_repo, "target_file": template.format(dag_id=dag_id)}
