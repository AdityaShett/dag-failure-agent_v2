import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from agent_failure_callback import notify_dag_failure_agent


def fetch_readings(sensor_id):
    if sensor_id == "temp-04":
        return {"value": 71.2, "unit": "F"}
    return [{"value": 71.2, "unit": "F"}]


def collect_all_readings(sensor_ids):
    all_readings = []
    for sensor_id in sensor_ids:
        readings = fetch_readings(sensor_id)
        all_readings.append(readings)
    return all_readings


def average_value(readings):
    total = sum(r["value"] for r in readings)
    return total / len(readings)


def summarize_batch(all_readings):
    summaries = []
    for readings in all_readings:
        summaries.append(average_value(readings))
    return summaries


def run_sensor_summary():
    sensor_ids = ["temp-01", "temp-02", "temp-04"]
    all_readings = collect_all_readings(sensor_ids)
    summary = summarize_batch(all_readings)
    print("summary:", summary)


with DAG(
    dag_id="test_batch_type_drift_d8",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0, "on_failure_callback": notify_dag_failure_agent},
    tags=["agent-test"],
) as dag:
    PythonOperator(
        task_id="run_sensor_summary",
        python_callable=run_sensor_summary,
    )
