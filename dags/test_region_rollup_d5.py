import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from agent_failure_callback import notify_dag_failure_agent

REGION_ORDER = ["west", "east", "central"]


def group_orders_by_region(orders):
    grouped = {region: [] for region in REGION_ORDER}
    for order in orders:
        grouped[order["region"]].append(order)
    return grouped


def compute_region_subtotal(orders):
    return sum(order["amount"] for order in orders)


def compute_region_totals(grouped_orders):
    totals = {}
    for region in REGION_ORDER:
        totals[region] = compute_region_subtotal(grouped_orders[region])
    return totals


def run_region_rollup():
    orders = [
        {"region": "west", "amount": 100},
        {"region": "south", "amount": 75},
        {"region": "east", "amount": 50},
    ]
    grouped = group_orders_by_region(orders)
    totals = compute_region_totals(grouped)
    print("totals:", totals)


with DAG(
    dag_id="test_region_rollup_d5",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0, "on_failure_callback": notify_dag_failure_agent},
    tags=["agent-test"],
) as dag:
    PythonOperator(
        task_id="run_region_rollup",
        python_callable=run_region_rollup,
    )
