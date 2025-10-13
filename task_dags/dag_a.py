from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator
import random
import csv
from datetime import datetime


KEEP_COL_NAMES = Variable.get("KEEP_COL_NAMES", "Flow ID|Timestamp|Label").split('|')
INPUT_FILE = Variable.get("FULL_DATA_FILE", "/var/tmp/airflow/data/full_ddos_dataset.csv")
OUTPUT_FILE = Variable.get("SELECTED_DATA_FILE", "/var/tmp/airflow/data/ddos_dataset_10_rand.csv")


with DAG(
    dag_id="dag_a",
    start_date=datetime(2025, 10, 10),
    schedule='0 * * * 1-5',
    catchup=False
) as dag:
    
    def pick_10_rand_records(**context):
        with open(INPUT_FILE, "r", newline='') as fin, open(OUTPUT_FILE, "w", newline='') as fout:
            reader = csv.reader(fin)
            writer = csv.writer(fout)

            header = next(reader)
            col_indexes = [header.index(name) for name in KEEP_COL_NAMES]
            writer.writerow(KEEP_COL_NAMES)

            for _ in range(10):
                step = random.randint(1, 10000)

                for _ in range(step - 1):
                    next(reader, None)

                row = next(reader, None)
                if row:
                    writer.writerow([row[i] for i in col_indexes])
                else:
                    break

    pick_10_rand_records_task = PythonOperator(
        task_id="pick_10_rand_records",
        python_callable=pick_10_rand_records
    )
