import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from agent_failure_callback import notify_dag_failure_agent


def build_metrics_template():
    return {"processed": 0, "errors": 0}


def record_batch_result(metrics, batch_had_errors, batch_was_skipped):
    if batch_was_skipped:
        return
    metrics["processed"] += 1
    if batch_had_errors:
        metrics["errors"] += 1


def summarize_metrics(metrics):
    total = metrics["processed"]
    error_rate = metrics["errors"] / total if total else 0
    skipped = metrics["skipped"]
    return {"total": total, "error_rate": error_rate, "skipped": skipped}


def run_metrics_job():
    metrics = build_metrics_template()
    record_batch_result(metrics, batch_had_errors=True, batch_was_skipped=False)
    record_batch_result(metrics, batch_had_errors=False, batch_was_skipped=True)
    summary = summarize_metrics(metrics)
    print("summary:", summary)


with DAG(
    dag_id="test_metrics_gap_d1",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0, "on_failure_callback": notify_dag_failure_agent},
    tags=["agent-test"],
) as dag:
    PythonOperator(
        task_id="run_metrics_job",
        python_callable=run_metrics_job,
    )
