from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sample_k8s_task",
    default_args=default_args,
    description="A single task DAG using KubernetesPodOperator",
    schedule_interval=None,  # Manual trigger or use cron
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["kubernetes", "example"],
) as dag:

    k8s_task = KubernetesPodOperator(
        task_id="run_date",
        name="run-date-pod",
        namespace="dagns",  # Namespace for this pod
        image="busybox:1.35",  # Lightweight container
        cmds=["sh", "-c"],
        arguments=["date && echo 'Hello from KubernetesPodOperator!'"],
        service_account_name="airflow-dag-sa",  # ServiceAccount in dagns
        get_logs=True,
    )
