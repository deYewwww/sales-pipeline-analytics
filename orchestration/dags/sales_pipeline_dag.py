from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.standard.operators.bash import BashOperator

# Paths inside the container 
DBT_PROJECT_DIR = "/usr/local/airflow/include/dbt_pipeline"
DBT_PROFILES_DIR = "/usr/local/airflow/include/dbt_profiles"

default_args = {
    "owner": "chun_keat",
    "retries": 2,                          
    "retry_delay": timedelta(minutes=2)     # wait between retry 
}

with DAG(
    dag_id="sales_pipeline_analytics",
    start_date=datetime(2026,10,1),
    schedule="15 * * * *",                  # manual trigger while building; set a schedule on Day 9
    catchup=False,                          # don't create run for every missed day since start_date
    max_active_runs=1,                      # never 2 runs at once -> no 2 stream on 1 checkpoint 
    render_template_as_native_obj=True,     # let "{{ ... }}" become a real init, not the STRING
    default_args=default_args,
    tags=["portfolio", "databricks"]
) as dag:
    
    databricks_bronze_silver = DatabricksRunNowOperator(
        task_id="databricks_bronze_silver", 
        databricks_conn_id="databricks_default",
        job_id="{{ var.value.DATABRICKS_JOB_ID }}",     # read at RUN time, not parse time
        deferrable=True,                                # wait in the trigger, not a worker slot 
        execution_timeout=timedelta(minutes=30)         # kill it if hangs
    )

    dbt_build = BashOperator(
        task_id = "dbt_build",
        bash_command = (f"cd {DBT_PROJECT_DIR} && dbt build --profiles-dir {DBT_PROFILES_DIR} --target airflow"),
        retries = 1,
        execution_timeout = timedelta(minutes=20)
    )

    databricks_bronze_silver >> dbt_build