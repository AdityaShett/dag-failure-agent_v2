import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from agent_failure_callback import notify_dag_failure_agent


def read_setting(settings, key):
    return settings[key]


def run_final_flight():
    settings = {"mode": "batch", "region": "us-central1"}
    print("mode:", read_setting(settings, "mode"))
    print("owner:", read_setting(settings, "owner"))


with DAG(
    dag_id="test_final_flight_d13",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0, "on_failure_callback": notify_dag_failure_agent},
    tags=["agent-test"],
) as dag:
    PythonOperator(
        task_id="run_final_flight",
        python_callable=run_final_flight,
    )
