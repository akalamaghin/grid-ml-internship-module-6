from airflow.sdk import DAG
from datetime import datetime, timedelta
from airflow.utils.state import State
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
import csv
from airflow.task.trigger_rule import TriggerRule
from custom_operators import XComExecStatusOperator


INPUT_FILE = "/var/tmp/airflow/data/ddos_dataset_10_rand.csv"
TARGET_DAG_ID = "dag_a"

POSTGRES_CONN_ID = "postgres_default"
TABLE_SCHEMA = "public"
TABLE_NAME = "ddos_data"

DAG_STATUS_TASKS = ["push_exec_status_succeed", "push_exec_status_failed"]

with DAG(
    dag_id="dag_b",
    start_date=datetime(2025, 10, 10),
    schedule='0 * * * 1-5',
    catchup=False
) as dag:
    
    def check_previous_runs(**context):
        ti = context["ti"]

        last_a_status = None
        for task_id in DAG_STATUS_TASKS:
            last_a_status = ti.xcom_pull(
                dag_id=TARGET_DAG_ID,
                task_ids=task_id,
                key="exec_status"
            )

            if last_a_status is not None:
                break
        
        if last_a_status is None:
            last_a_status = State.NONE

        if last_a_status != State.SUCCESS:
            print(f"Last dag_a exec_status: {last_a_status}. Will retry in 5 minutes...") # type: ignore
            return False

        prev_b_status = None
        for task_id in DAG_STATUS_TASKS:
            prev_b_status = ti.xcom_pull(
                dag_id="dag_b",
                task_ids=task_id,
                key="exec_status"
            )
            if prev_b_status is not None:
                break

        if prev_b_status is None:
            prev_b_status = State.NONE

        # From the task definition it is not clear if something
        # should depend on this status, so I'll just log it for now
        print(f"Previous dag_b exec_status: {prev_b_status}") # type: ignore

        return True

    check_previous_runs_task = PythonSensor(
        task_id="check_previous_runs",
        python_callable=check_previous_runs,
        poke_interval=60,
        timeout=5 * 60,
        retries=2,
        retry_delay=timedelta(minutes=5),
        mode="reschedule",
    )

    def check_table_exists(**context):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = (
            f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = '{TABLE_SCHEMA}'
                AND table_name = '{TABLE_NAME}'
            );
            """
        )
        result = hook.get_first(sql)
        table_exists = result[0] if result else False
        return "insert_rows" if table_exists else "create_table"
    
    check_table_exists_task = BranchPythonOperator(
        task_id="check_table_exists",
        python_callable=check_table_exists
    )

    def create_table(**context):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        with open(INPUT_FILE, newline="") as f:
            reader = csv.reader(f)
            columns = [col.lower().replace(" ", "_").replace("/", "_") for col in next(reader)]

        columns_sql = ",\n".join([f"{col} VARCHAR" for col in columns])
        sql = (
            f"""
            CREATE TABLE {TABLE_SCHEMA}.{TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                {columns_sql}
            );
            """
        )

        hook.run(sql)

        print(f"Table {TABLE_SCHEMA}.{TABLE_NAME} created with columns: ['id'] + {columns}")

    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table
    )

    def insert_rows(**context):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        with open(INPUT_FILE, newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = [name.lower().replace(" ", "_").replace("/", "_") for name in reader.fieldnames] # type: ignore

            n = 0
            for row in reader:
                cols = ", ".join(fieldnames)
                values = ", ".join([f"'{row[field]}'" for field in reader.fieldnames]) # type: ignore
                sql = (
                    f"""
                    INSERT INTO {TABLE_SCHEMA}.{TABLE_NAME} ({cols})
                    VALUES ({values});
                    """
                )
                hook.run(sql)
                n += 1

        print(f"Inserted {n} rows into {TABLE_SCHEMA}.{TABLE_NAME}")

    insert_rows_task = PythonOperator(
        task_id="insert_rows",
        python_callable=insert_rows,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    push_exec_status_failed_task = XComExecStatusOperator(
        task_id="push_exec_status_failed",
        failed=True,
        trigger_rule=TriggerRule.ALL_FAILED
    )

    push_exec_status_succeed_task = XComExecStatusOperator(
        task_id="push_exec_status_succeed",
        failed=False,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    (
        check_previous_runs_task
            >> check_table_exists_task
                >> [create_table_task, insert_rows_task]
    )   # type: ignore

    (
        create_table_task 
            >> insert_rows_task
                >> [push_exec_status_failed_task, push_exec_status_succeed_task]
    )   # type: ignore
