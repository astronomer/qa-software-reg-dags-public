from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

from airflow.configuration import conf

namespace = conf.get("kubernetes", "NAMESPACE")

with DAG(
    dag_id="kpo_dag",
    start_date=datetime(1970, 1, 1),
    schedule_interval=None,
    # render_template_as_native_obj=True,
    tags=["core"]
) as dag:

    KubernetesPodOperator(
        task_id="cowsay_static",
        name="cowsay_statc",
        namespace=namespace,
        image="docker.io/rancher/cowsay",
        cmds=["cowsay"],
        arguments=["moo"],
        log_events_on_failure=True,
    )