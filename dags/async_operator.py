import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.time_sensor import TimeSensorAsync
from airflow.utils.dates import days_ago
from airflow.utils.timezone import utcnow


default_args = {"start_date": days_ago(2)}


with DAG("async_operator", schedule_interval="@daily", default_args=default_args, tags=["core", "async", "extended_dags"]) as dag:
    sensor = TimeSensorAsync(
        task_id="sensor",
        target_time=(utcnow() + datetime.timedelta(seconds=10)).time(),
    )
    complete = DummyOperator(task_id="complete")
    sensor >> complete
