from airflow.sdk import DAG, Variable
from datetime import datetime, timedelta
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.task.trigger_rule import TriggerRule
import requests
import csv


INPUT_FILE = Variable.get(
    "SELECTED_DATA_FILE",
    "/var/tmp/airflow/data/ddos_dataset_10_rand.csv"
)

TARGET_DAG_ID = Variable.get("TARGET_DAG_ID", "dag_a")

POSTGRES_CONN_ID = Variable.get("POSTGRES_CONN_ID", "postgres_default")
TABLE_SCHEMA = Variable.get("TABLE_SCHEMA", "public")
TABLE_NAME = Variable.get("TABLE_NAME", "ddos_data")

AIRFLOW_URL_BASE = (
    f"http://{Variable.get("AIRFLOW_API_HOST", "airflow-apiserver")}:"
    f"{Variable.get("AIRFLOW_API_PORT", "8080")}/"
)
AIRFLOW_API_BASE = AIRFLOW_URL_BASE + "api/v2/"

AIRFLOW_USERNAME = Variable.get("AIRFLOW_USERNAME")
AIRFLOW_PASSWORD = Variable.get("AIRFLOW_PASSWORD")


with DAG(
    dag_id="dag_b",
    start_date=datetime(2025, 10, 10),
    schedule='0 * * * 1-5',
    catchup=False
) as dag:
    
    def get_airflow_token() -> str:
        """Authenticate in Airflow API to get a Bearer token."""

        url = f"{AIRFLOW_URL_BASE}auth/token"
        payload = {
            "username": AIRFLOW_USERNAME,
            "password": AIRFLOW_PASSWORD,
        }

        response = requests.post(url, json=payload, timeout=10)
        if not response.ok:
            raise RuntimeError(f"Failed to obtain auth token: {response.text}")

        token = response.json().get("access_token")
        if not token:
            raise RuntimeError("Token missing from response")
        
        return token

    def get_prev_dagrun(dag_id, offset = 0, limit = 1):
        """
        Fetch one of the previous DAG runs by dag_id from Airflow API,
        ordered by logical_date DESC.
        """
        
        token = get_airflow_token()

        url = f"{AIRFLOW_API_BASE}dags/{dag_id}/dagRuns"
        params = {
            "order_by": "-logical_date",
            "limit": limit,
            "offset": offset,
        }

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }

        response = requests.get(url, params=params, headers=headers, timeout=10)
        if not response.ok:
            raise RuntimeError(f"Failed to fetch runs for {dag_id}: {response.text}")

        data = response.json()
        runs = data.get("dag_runs", [])
        if not runs:
            return None

        return runs[0]

    def check_previous_runs(**context):
        """
        Check(?) the last run of this dag and check that
        the last run of <TARGET_DAG_ID> succeeded before proceeding.
        """

        # It's unclear what exactly "Have a sensor to check the state of the previous run of B..."
        # is meant to cover — whether it refers to ensuring no other instance is running, or
        # simply verifying that we can fetch the run preceding the current one.
        # For now, this implementation only logs the result.
        # If more specific behavior is required, please open an issue should to clarify.
        this_dag = context["dag"].dag_id
        last_this_run = get_prev_dagrun(this_dag, offset=1)
        if last_this_run:
            print(f"Prev {this_dag} run: {last_this_run['logical_date']} (state={last_this_run['state']})")
        else:
            print(f"Prev {this_dag} run: Not Found!")

        last_target_run = get_prev_dagrun(TARGET_DAG_ID)
        if last_target_run is None:
            print(f"Last {TARGET_DAG_ID} run: Not Found! Need to retry...")
            return False

        last_target_state = last_target_run["state"].lower()
        print(f"Last {TARGET_DAG_ID} run: {last_target_run['logical_date']} (state={last_target_state})")

        if last_target_state != "success":
            print(f"Last {TARGET_DAG_ID} run: Failed! Need to retry...")
            return False
        else:
            print(f"Last {TARGET_DAG_ID} run: Succeed! Proceeding...")
        
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

    def fmt_col_name(col):
        return col.lower().replace(" ", "_").replace("/", "_")

    def create_table(**context):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        with open(INPUT_FILE, newline="") as f:
            reader = csv.reader(f)
            columns = [fmt_col_name(col) for col in next(reader)]

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
            fieldnames = [fmt_col_name(name) for name in reader.fieldnames] # type: ignore

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

    (
        check_previous_runs_task
            >> check_table_exists_task
                >> [create_table_task, insert_rows_task]
    )   # type: ignore

    (
        create_table_task 
            >> insert_rows_task
    )   # type: ignore
