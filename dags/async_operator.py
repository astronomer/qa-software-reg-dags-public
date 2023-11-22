import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.time_sensor import TimeSensorAsync
from airflow.utils.dates import days_ago
from airflow.utils.timezone import utcnow
from airflow.sensors.bash import BashSensor

default_args = {"start_date": days_ago(0)}


with DAG("async_operator", schedule_interval="@daily", default_args=default_args, tags=["core", "async", "extended_dags"]) as dag:
    sensor = TimeSensorAsync(
        task_id="sensor",
        target_time=(utcnow() + datetime.timedelta(seconds=10)).time(),
    )
    task1 = BashSensor(task_id="sleep_10", bash_command="sleep 10")
    complete = DummyOperator(task_id="complete")
    sensor >> task1 >> complete
