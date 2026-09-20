import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from agent_failure_callback import notify_dag_failure_agent

DISCOUNT_CODES = {
    "WELCOME10": 0.10,
    "BULK20": 0.20,
}

BULK_THRESHOLD = 500


def lookup_discount_rate(code):
    return DISCOUNT_CODES[code]


def compute_stacked_discount(order):
    rate = lookup_discount_rate(order["code"])
    if order["subtotal"] >= BULK_THRESHOLD:
        rate = min(rate + 0.05, 0.5)
    return round(order["subtotal"] * rate, 2)


def apply_discounts(orders):
    return [
        {"order_id": o["order_id"], "discount": compute_stacked_discount(o)}
        for o in orders
    ]


def run_discount_batch():
    orders = [
        {"order_id": "A1", "code": "WELCOME10", "subtotal": 120},
        {"order_id": "A2", "code": "BULK20", "subtotal": 640},
        {"order_id": "A3", "code": "FALL25", "subtotal": 610},
    ]
    results = apply_discounts(orders)
    print("results:", results)


with DAG(
    dag_id="test_cross_batch_conflict_d10",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0, "on_failure_callback": notify_dag_failure_agent},
    tags=["agent-test"],
) as dag:
    PythonOperator(
        task_id="run_discount_batch",
        python_callable=run_discount_batch,
    )
