from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="test_dag",
    start_date=datetime(1, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    get_current_user_task = BashOperator(
        task_id="get_current_user",
        bash_command="whoami"
    )
