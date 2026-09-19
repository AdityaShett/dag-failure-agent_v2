import json
import os

from google.cloud import pubsub_v1

_PROJECT = os.environ.get("GCP_PROJECT")
_TOPIC = os.environ.get("AGENT_FAILURE_TOPIC", "dagfailures")

_publisher = pubsub_v1.PublisherClient()
_topic_path = _publisher.topic_path(_PROJECT, _TOPIC)


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
        future = _publisher.publish(_topic_path, json.dumps(payload).encode("utf-8"))
        future.result(timeout=10)
    except Exception as e:
        print(f"WARNING: could not publish failure event: {e!r}")
