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
    dag_id="sample_airflow_namespace",
    default_args=default_args,
    description="A DAG running in airflow namespace",
    schedule_interval=None,  # manual trigger or cron expression
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["kubernetes", "airflow-namespace"],
) as dag:

    airflow_ns_task = KubernetesPodOperator(
        task_id="run_airflow_ns_task",
        name="run-airflow-ns-pod",
        namespace="airflow",  # Pod runs in airflow namespace
        image="busybox:1.35",  # Lightweight container
        cmds=["sh", "-c"],
        arguments=["date && echo 'Hello from airflow namespace!'"],
        service_account_name="airflow-sa",  # ServiceAccount in airflow namespace
        get_logs=True,
    )
