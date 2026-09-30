import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from agent_failure_callback import notify_dag_failure_agent


def run_inventory_retry():
    pending_items = [{"sku": "B7", "location": "east"}, {"location": "west"}]  # second entry missing "sku"
    for entry in pending_items:
        print("processing", entry["sku"])


with DAG(
    dag_id="test_inventory_sync_retry_d12",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0, "on_failure_callback": notify_dag_failure_agent},
    tags=["agent-test"],
) as dag:
    PythonOperator(
        task_id="run_inventory_retry",
        python_callable=run_inventory_retry,
    )


# agent fix could not be applied automatically
# UnidiffParseError: Hunk is shorter than expected
