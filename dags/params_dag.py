from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python_operator import PythonOperator

# unpausing causes this dag to run once, because there's only been
# one complete interval of 30 years since the epoch.
thirty_years = 1577862000  # seconds

with DAG(
    "params_int_fivetoten",
    start_date=datetime(1970, 1, 1),
    schedule_interval=timedelta(seconds=thirty_years),
    catchup=True,
    params={"x": Param(7, type="integer", minimum=5, maximum=10)},
    render_template_as_native_obj=True,
    doc_md=dedent(
        """
        # Purpose

        Check if scheduled runs receive default dagrun params.

        ## Steps

        1. Unpause
        2. Wait for dag run to complete
        3. Trigger a new dagrun without config
        4. Trigger a new dagrun with config, but make no changes
        5. Trigger a new dagrun with config, but change it to be some other valid value
        6. Trigger a new dagrun with config, but change it to be some invalid value

        ## Expectations

        - **2 - 5**: All tasks succeed
        - **6**: The dagrun is never created, user sees validation error in UI or gets error via API
        """
    ),
    tags=["core"]
) as use_params:

    def fail_if_invalid(x):
        print(x)
        assert 5 < x < 10

    PythonOperator(
        task_id="ref_param",
        python_callable=fail_if_invalid,
        op_args=["{{ params['x'] }}"],
    )
