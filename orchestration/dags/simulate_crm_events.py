from datetime import datetime,timedelta
from airflow.sdk import DAG 
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    "owner": "chun_keat",
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id = "simulate_crm_events",
    start_date = datetime(2026, 10, 1),
    schedule = "*/30 * * * *",
    catchup = False,
    max_active_runs = 1,
    default_args = default_args,
    tags = ["portfolio", "source-simulator"]
) as dag:
    
    generate_events = BashOperator(
        task_id = "generate_events",
        bash_command = "cd /usr/local/airflow/include && python -m simulator.run_simulator --deal 5",
        execution_timeout = timedelta(minutes=5)
    )