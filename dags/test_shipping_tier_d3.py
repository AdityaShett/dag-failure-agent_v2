import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from agent_failure_callback import notify_dag_failure_agent


def classify_shipment(weight_kg, is_international):
    if weight_kg > 50:
        category = "freight"
    elif is_international:
        category = "international_parcel"
    elif weight_kg > 20:
        category = "heavy_parcel"

    return category


def build_shipping_label(weight_kg, is_international):
    category = classify_shipment(weight_kg, is_international)
    return {"category": category, "weight_kg": weight_kg}


def run_labeling():
    label = build_shipping_label(weight_kg=12, is_international=False)
    print("label:", label)


with DAG(
    dag_id="test_shipping_tier_d3",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0, "on_failure_callback": notify_dag_failure_agent},
    tags=["agent-test"],
) as dag:
    PythonOperator(
        task_id="run_labeling",
        python_callable=run_labeling,
    )
