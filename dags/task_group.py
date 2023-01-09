from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup


def create_section():
    """
    Create tasks in the outer section.
    """
    dummies = [DummyOperator(task_id=f"task-{i}") for i in range(2)]

    with TaskGroup("inside_section_1") as inside_section_1:
        _ = [
            DummyOperator(
                task_id=f"task-{i+1 }",
            )
            for i in range(3)
        ]

    with TaskGroup("inside_section_2") as inside_section_2:
        _ = [
            DummyOperator(
                task_id=f"task-{i+1}",
            )
            for i in range(3)
        ]

    dummies[-1] >> inside_section_1
    dummies[-2] >> inside_section_2


with DAG(dag_id="example_task_group", start_date=days_ago(2), tags=["core"]) as dag:
    start = DummyOperator(task_id="start")

    with TaskGroup("section_1", tooltip="Tasks for Section 1") as section_1:
        create_section()

    some_other_task = DummyOperator(task_id="some-other-task")

    with TaskGroup("section_2", tooltip="Tasks for Section 2") as section_2:
        create_section()

    end = DummyOperator(task_id="end")

    start >> section_1 >> some_other_task >> section_2 >> end
