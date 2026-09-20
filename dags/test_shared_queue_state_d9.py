import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from agent_failure_callback import notify_dag_failure_agent


def enqueue_jobs(jobs, queue=[]):
    for job in jobs:
        queue.append(job)
    return queue


def run_batch(queue):
    if len(queue) > 2:
        raise ValueError(f"queue exceeded capacity: {len(queue)} jobs, expected at most 2")
    return [job["id"] for job in queue]


def run_job_batch():
    first_batch = [{"id": "a"}, {"id": "b"}]
    queue1 = enqueue_jobs(first_batch)
    result1 = run_batch(queue1)
    print("first batch result:", result1)

    second_batch = [{"id": "c"}]
    queue2 = enqueue_jobs(second_batch)
    result2 = run_batch(queue2)
    print("second batch result:", result2)


with DAG(
    dag_id="test_shared_queue_state_d9",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0, "on_failure_callback": notify_dag_failure_agent},
    tags=["agent-test"],
) as dag:
    PythonOperator(
        task_id="run_job_batch",
        python_callable=run_job_batch,
    )
