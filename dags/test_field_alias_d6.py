import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from agent_failure_callback import notify_dag_failure_agent


def normalize_contact(record):
    return {
        "name": record["name"],
        "email": record["email"],
    }


def build_contact_list(records):
    return [normalize_contact(r) for r in records]


def run_contact_sync():
    records = [
        {"name": "Priya Nair", "email": "priya@example.com"},
        {"name": "Sam Ortiz", "email_address": "sam@example.com"},
        {"name": "Jae Kim", "email": "jae@example.com"},
    ]
    contacts = build_contact_list(records)
    print("contacts:", contacts)


with DAG(
    dag_id="test_field_alias_d6",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0, "on_failure_callback": notify_dag_failure_agent},
    tags=["agent-test"],
) as dag:
    PythonOperator(
        task_id="run_contact_sync",
        python_callable=run_contact_sync,
    )
