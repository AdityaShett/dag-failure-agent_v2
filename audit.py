import sys
from collections import Counter

from google.cloud import firestore

since = sys.argv[1] if len(sys.argv) > 1 else ""
client = firestore.Client(project="dag-failure-agent-v2")
runs = [{**d.to_dict(), "id": d.id} for d in client.collection("runs").stream()]
runs = [r for r in runs if r.get("created_at", "") >= since]

in_flight = ("processing", "scoring", "generating")
bad = []
for r in runs:
    status, score, threshold = r.get("status"), r.get("confidence_score"), r.get("threshold")
    if status == "error" or status in in_flight:
        bad.append((r["id"], f"stuck or failed: {status} ({r.get('error', '')})"))
    elif status == "gated" and (score is None or threshold is None or score >= threshold):
        bad.append((r["id"], f"gated but score {score} >= threshold {threshold}"))
    elif status != "gated" and not r.get("pr_url"):
        bad.append((r["id"], f"{status} with no PR"))

print(dict(Counter(r.get("status") for r in runs)))
print(f"no-fix PRs: {sum(1 for r in runs if r.get('diff_applied') is False)}")
print(f"runs checked: {len(runs)}  violations: {len(bad)}")
for run_id, reason in bad:
    print(f"  {run_id}: {reason}")
sys.exit(1 if bad else 0)
