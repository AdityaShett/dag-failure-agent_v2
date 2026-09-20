import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from agent_failure_callback import notify_dag_failure_agent


class Connection:
    def __init__(self, name):
        self.name = name

    def execute(self, query):
        return f"ran '{query}' on {self.name}"


_CONNECTION_CACHE = {}


def get_connection(name):
    return _CONNECTION_CACHE.get(name)


def run_query_on(name, query):
    conn = get_connection(name)
    return conn.execute(query)


def run_sync_job():
    result = run_query_on("warehouse", "SELECT 1")
    print("result:", result)


with DAG(
    dag_id="test_lazy_cache_d2",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0, "on_failure_callback": notify_dag_failure_agent},
    tags=["agent-test"],
) as dag:
    PythonOperator(
        task_id="run_sync_job",
        python_callable=run_sync_job,
    )
