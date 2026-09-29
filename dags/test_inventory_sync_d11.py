import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from agent_failure_callback import notify_dag_failure_agent


def sync_inventory_item(item):
    return item["sku"]


def run_inventory_sync():
    items = [{"sku": "A1", "qty": 5}, {"qty": 3}]  # second item is missing "sku" on purpose
    results = [sync_inventory_item(i) for i in items]
    print("synced:", results)


with DAG(
    dag_id="test_inventory_sync_d11",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0, "on_failure_callback": notify_dag_failure_agent},
    tags=["agent-test"],
) as dag:
    PythonOperator(
        task_id="run_inventory_sync",
        python_callable=run_inventory_sync,
    )
