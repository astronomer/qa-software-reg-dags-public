from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def sample_python_function(**kwargs):
    """
    Insert arbitrary python function here
    """
    print("hello")


dag = DAG('example_python_operator_erohit',
          max_active_runs=3,
          catchup=True,
          schedule_interval='@daily',
          default_args=default_args,
          tags=['core'])


with dag:
    start = DummyOperator(task_id='start')

    t1 = PythonOperator(
    task_id='python_function_ernest1',
    python_callable = sample_python_function,
    dag = dag)

    start >> t1