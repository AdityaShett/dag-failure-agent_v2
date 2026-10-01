"""End-to-end proof DAG for dag-failure-agent.

Three tasks fail on purpose, each exercising a different path through the agent.
One task succeeds and must NOT create a run record.

  t1_missing_setting   KeyError, one-line fix       -> PR opened, small diff
  t2_unguarded_none    AttributeError, needs guard  -> PR opened, multi-line diff
  t3_upstream_outage   RuntimeError, not fixable    -> gated (refused / below threshold)
  t4_healthy           succeeds                     -> no run row
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# EDIT THIS LINE: copy the exact import and callback name used in dags/test_final_flight_d13.py
from agent_failure_callback import on_failure


def load_region_settings():
    settings = {"region": "us-east1", "batch_size": 500}
    batch_limit = settings["max_batch"]
    print(f"batch limit: {batch_limit}")


def find_customer(customer_id):
    directory = {1001: "acme", 1002: "globex"}
    return directory.get(customer_id)


def format_customer_label():
    customer = find_customer(1042)
    label = customer.upper()
    print(f"customer label: {label}")


def pull_warehouse_totals():
    upstream_status = 503
    raise RuntimeError(f"warehouse API returned HTTP {upstream_status}; upstream outage")


def healthy_task():
    print("nothing wrong here")


with DAG(
    dag_id="test_e2e_proof_d14",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0, "on_failure_callback": on_failure},
    tags=["agent-proof"],
) as dag:
    PythonOperator(task_id="t1_missing_setting", python_callable=load_region_settings)
    PythonOperator(task_id="t2_unguarded_none", python_callable=format_customer_label)
    PythonOperator(task_id="t3_upstream_outage", python_callable=pull_warehouse_totals)
    PythonOperator(task_id="t4_healthy", python_callable=healthy_task)
