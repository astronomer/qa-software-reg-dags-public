from airflow import Dataset
from datetime import datetime, timedelta

from airflow.decorators import task, dag
from airflow import settings
#import pause
import psycopg2
import textwrap

# when simple_dataset_source updates this dataset
# simple_dataset_sink will run
counter = Dataset("counter")


# we're using this table as a dataset
# airflow doesn't know about it, it just looks at the above object
update_template = textwrap.dedent(
    """
    BEGIN;

    -- make sure the table exists
    CREATE TABLE IF NOT EXISTS counter (
        source INT NOT NULL,
        sink INT NOT NULL
    );

    -- seed it if it's newly created
    INSERT INTO counter (source, sink)
    SELECT 0, 0
    WHERE NOT EXISTS (SELECT * FROM counter);

    -- increment based on coaller
    UPDATE counter
    SET {column} = {column} + 1;

    COMMIT;
    """
)


def get_cursor():
    "get a cursor on the airflow database"
    url = settings.engine.url
    host = url.host or "localhost"
    port = str(url.port or "5432")
    user = url.username or "postgres"
    password = url.password or "postgres"
    schema = url.database

    conn = psycopg2.connect(
        host=host, user=user, port=port, password=password, dbname=schema
    )
    conn.autocommit = True
    cursor = conn.cursor()
    return cursor


@task(outlets=[counter])
def increment_source():
    cursor = get_cursor()
    cursor.execute(update_template.format(column="source"))
    cursor.execute("SELECT * FROM counter;")
    print(cursor.fetchall())

    # if many of these are happening at once, have them all complete at the same moment
    # to maximize the likelihood of a collision
    # now = datetime.now()
    # pause_until_minutes = now.minute + 2
    # pause.until(
    #     datetime(now.year, now.month, now.day, now.hour, pause_until_minutes, 0)
    # )


@task
def increment_sink():
    cursor = get_cursor()
    cursor.execute(update_template.format(column="sink"))
    cursor.execute("SELECT * FROM counter;")
    print(cursor.fetchall())


@task
def reset():
    cursor = get_cursor()
    cursor.execute("DROP TABLE IF EXISTS counter;")


# run this before the test
@dag(start_date=datetime(1970, 1, 1), schedule_interval=None)
def simple_dataset_reset():
    reset()


# unpause this
def source_factory(i):
    @dag(
        dag_id=f"source_{i}",
        start_date=datetime(1970, 1, 1),
        schedule_interval=timedelta(days=365 * 30),
    )
    def source():
        increment_source()

    return source()


# expect this to run
@dag(schedule=[counter], start_date=datetime(1970, 1, 1))
def simple_dataset_sink():
    increment_sink()
    # task logs should say 1,1
    # source ran once
    # sink ran once


# the reset dag
reset_dag = simple_dataset_reset()

# 16 source dags
for i in range(16):
    globals()[str(i)] = source_factory(i)

# one sink dag
sink = simple_dataset_sink()
