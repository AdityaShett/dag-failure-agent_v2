import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from agent_failure_callback import notify_dag_failure_agent


def parse_record_value(record):
    return int(record["value"])


def total_daily_volume(records):    total = 0
    for record in records:
        total += parse_record_value(record)
    return total


def run_volume_report():
    records = [{"value": 120}, {"value": "85"}, {"value": 40}]
    total = total_daily_volume(records)
    print("total volume:", total)


with DAG(
    dag_id="test_volume_parse_d4",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0, "on_failure_callback": notify_dag_failure_agent},
    tags=["agent-test"],
) as dag:
    PythonOperator(
        task_id="run_volume_report",
        python_callable=run_volume_report,
    )
