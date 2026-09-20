import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from agent_failure_callback import notify_dag_failure_agent


def look_up_service_tier():
    config = {"environment": "prod", "region": "us-central1"}
    print("looking up tier for", config["environment"])
    tier = config["service_tier"]
    print("tier:", tier)

with DAG(
    dag_id="test_kv_lookup",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0, "on_failure_callback": notify_dag_failure_agent},
    tags=["agent-test"],
) as dag:
    PythonOperator(
        task_id="lookup_service_tier",
        python_callable=look_up_service_tier,
    )