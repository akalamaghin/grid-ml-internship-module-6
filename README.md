# Arteom Kalamaghin - Apache Airflow Practical Task

## Deployment

For convenience, Airflow was chosen to run in Docker.

### Environment

Before starting this setup with Docker Compose, you need to set all the essential environment variables. Create a .env file in the same folder as the .yaml file, paste the list of variables provided below, and fill in any missing values.

    AIRFLOW_UID=
    AIRFLOW_GUID=
    AIRFLOW__CORE__FERNET_KEY=
    AIRFLOW__WEBSERVER__SECRET_KEY=
    POSTGRES_USER=
    POSTGRES_PASSWORD=
    POSTGRES_DB=

* `AIRFLOW__WEBSERVER__SECRET_KEY` can be arbitrary string, but `AIRFLOW__CORE__FERNET_KEY` must follow a specific format and therefore needs to be properly generated:

        python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

* `AIRFLOW_UID` and `AIRFLOW_GUID` can be obtained with:

        echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GUID=0"

    on Unix-like systems.

### Starting Airflow

The Airflow can be deployed with Docker Compose by navigating to the `airflow/` directory and first running:

    docker-compose up airflow-init

to initalize the airflow environment and then starting it with:

    docker-compose up -d

to actually run Airflow.

Verify a successful startup by opening `http://localhost:8080/` in your browser. The default login credentials are `airflow:airflow` (which you can change by setting `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` in the .env file). On the _DAGs_ screen, you should see a test DAG already loaded.

### Additional Considerations

Make sure Docker has at least 2 CPU cores and 4 GB of RAM allocated to run this setup.

## Testing the Solution

### Loading the Dataset

Download final_dataset.csv from `https://www.kaggle.com/datasets/devendra416/ddos-datasets` rename the file to full_ddos_dataset.csv and put it in the `airflow/data/` folder.

### Loading DAGs

To load and tset the capstone project DAGs, place the .py files from the `task_dags/` into the `airflow/dags/` folder. Airflow will automatically load them within approximately 45 seconds.

### Setting Variables & Connections

To verify the project you'll need to setup a few connections and variables.

Navigate to Admin > Variables in the Airflow UI and create two variables: `AIRFLOW_USERNAME` and `AIRFLOW_PASSWORD`. Set their values to match the credentials you defined in your .env file under `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD`, or use the default ones if applicable.

Go to Admin > Connections and set up two connections:

* `fs_default`
    - Type: File (path)
    - In the Extra field, set the path to /
* `postgres_default`
    - Type: Postgres
    - Connection parameters: from your .yaml and .env files

### Running DAGs

That’s it! Simply unpause all the relevant DAGs.  
`dag_b` will be immediately scheduled since it requires at least one successful `dag_a` run.  
About one minute after `dag_a` finishes its execution, the second attempt of `dag_b` will yield the result.

To verify that the records were inserted into the database, run the following commands in your terminal:

    >> docker exec -it airflow-postgres-1 bash

    >> psql -U <YOUR_POSTGRES_USER_HERE> -d <YOUR_POSTGRES_DB_HERE>

    >> SELECT * FROM public.ddos_data;

Another way to verify the retry functionality is to modify the default `INPUT_FILE` in `dag_a.py`, run it once so it fails, then run `dag_b` to allow the sensor to reschedule itself.  
After that, revert the change and rerun `dag_a` to confirm successful execution.
