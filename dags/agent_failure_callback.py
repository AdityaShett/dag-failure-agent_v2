import json
import os
from google.cloud import pubsub_v1

_PROJECT = (
    os.environ.get("DAG_AGENT_GCP_PROJECT")
    or os.environ.get("GCP_PROJECT")
    or "dag-failure-agent-v2"
)
_TOPIC = os.environ.get("AGENT_FAILURE_TOPIC", "dagfailures")


def notify_dag_failure_agent(context):
    ti = context["task_instance"]
    dag_run = context["dag_run"]

    payload = {
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": dag_run.run_id,
        "try_number": ti.try_number,
    }

    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(_PROJECT, _TOPIC)
        future = publisher.publish(topic_path, json.dumps(payload).encode("utf-8"))
        future.result(timeout=10)
    except Exception as e:
        print(f"WARNING: could not publish failure event: {e!r}")
