import logging

from app import outcomes, store

logger = logging.getLogger(__name__)


def reconcile_open_prs(limit: int = 200) -> list:

    from github import Auth, Github
    from app.github_ops import get_github_token

    clients, report = {}, []
    for run in store.query_runs("status", "opened", limit):
        doc_id, repo, pr_number = run["id"], run.get("github_repo"), run.get("pr_number")
        entry = {"id": doc_id, "dag_id": run.get("dag_id"), "pr_number": pr_number}
        if not repo or pr_number is None:
            report.append({**entry, "result": "missing_pr_info"})
            continue
        try:
            if repo not in clients:
                clients[repo] = Github(auth=Auth.Token(get_github_token(repo)))
            pr = clients[repo].get_repo(repo).get_pull(int(pr_number))
        except Exception as e:
            logger.warning("reconcile: could not fetch PR #%s in %s: %s", pr_number, repo, e)
            report.append({**entry, "result": "github_error", "error": str(e)[:200]})
            continue

        if pr.state != "closed":
            report.append({**entry, "result": "still_open"})
            continue

        outcome = outcomes.record_outcome(doc_id, run, merged=bool(pr.merged), source="reconcile")
        report.append({**entry, "result": outcome["status"], "merged": bool(pr.merged)})
    return report