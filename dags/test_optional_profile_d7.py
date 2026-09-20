import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from agent_failure_callback import notify_dag_failure_agent


def find_primary_address(customer):
    for addr in customer["addresses"]:
        if addr.get("is_primary"):
            return addr
    return None


def format_shipping_label(customer):
    address = find_primary_address(customer)
    return f"{customer['name']} -> {address.get('line')}"


def build_shipping_batch(customers):
    return [format_shipping_label(c) for c in customers]


def run_shipping_batch():
    customers = [
        {"name": "Nova Traders", "addresses": [{"line": "12 Elm St", "is_primary": True}]},
        {"name": "Kestrel Co", "addresses": [{"line": "88 Pine Ave", "is_primary": False}]},
        {"name": "Arden Ltd", "addresses": [{"line": "4 Oak Blvd", "is_primary": True}]},
    ]
    labels = build_shipping_batch(customers)
    print("labels:", labels)


with DAG(
    dag_id="test_optional_profile_d7",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0, "on_failure_callback": notify_dag_failure_agent},
    tags=["agent-test"],
) as dag:
    PythonOperator(
        task_id="run_shipping_batch",
        python_callable=run_shipping_batch,
    )
