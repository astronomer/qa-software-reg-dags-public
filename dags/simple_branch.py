from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def predestine(**kwargs):
    kwargs["ti"].xcom_push(key="choice", value="foo")


def let_fates_decide(**kwargs):
    return kwargs["ti"].xcom_pull(key="choice")


with DAG(
    dag_id="simple_branch",
    start_date=days_ago(1),
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    tags=["core"],
) as dag:
    (
        PythonOperator(task_id="predestine", python_callable=predestine)
        >> BranchPythonOperator(
            task_id="let_fates_decide",
            python_callable=let_fates_decide,
        )
        >> [DummyOperator(task_id="foo"), DummyOperator(task_id="bar")]
    )
