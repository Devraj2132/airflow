from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from kubernetes.client import V1Pod, V1ObjectMeta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sample_dag",
    default_args=default_args,
    description="A simple sample DAG",
    schedule=timedelta(minutes=5),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    task1 = BashOperator(
        task_id="print_date",
        bash_command="date",
        executor_config={
            "pod_override": V1Pod(metadata=V1ObjectMeta())
        }
    )

    task2 = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello from Airflow DAG!'",
        executor_config={
            "pod_override": V1Pod(metadata=V1ObjectMeta())
        }
    )

    task1 >> task2
