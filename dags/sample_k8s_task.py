from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id="sample_k8s_task",
    default_args=default_args,
    description="KubernetesPodOperator DAG running every 30 seconds inside the pod",
    schedule_interval=None,  # Manual trigger or external trigger
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["kubernetes", "example"],
) as dag:

    k8s_task = KubernetesPodOperator(
        task_id="run_date_every_30s",
        name="run-date-pod",
        namespace="dagns",
        image="busybox:1.35",
        cmds=["sh", "-c"],
        arguments=[
            """
            while true; do
                date && echo 'Hello from KubernetesPodOperator!'
                sleep 30
            done
            """
        ],
        service_account_name="airflow-dag-sa",
        get_logs=True,
    )
