import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from agent_failure_callback import notify_dag_failure_agent


def line_total(order):
    gross = order["quantity"] * order["unit_price"]
    return gross - order.get("discount", 0.0)


def build_invoice(orders):
    totals = []
    for order in orders:
        totals.append(line_total(order))
    return round(sum(totals), 2)


def run_invoice_report():
    orders = [
        {"id": 1, "quantity": 3, "unit_price": 19.99, "discount": 5.0},
        {"id": 2, "quantity": 1, "unit_price": 249.0, "discount": 0.0},
        {"id": 3, "quantity": 12, "unit_price": 4.5},
    ]
    print("processing orders:", len(orders))
    total = build_invoice(orders)
    print("invoice total:", total)


with DAG(
    dag_id="test_order_totals_d15",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0, "on_failure_callback": notify_dag_failure_agent},
    tags=["agent-test"],
) as dag:
    PythonOperator(
        task_id="run_invoice_report",
        python_callable=run_invoice_report,
    )
