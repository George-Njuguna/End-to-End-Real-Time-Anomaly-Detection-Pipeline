from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from pendulum import timezone

with DAG(
    dag_id="run_script_1_once",
    start_date=datetime(2024, 1, 1, tzinfo=timezone("Africa/Nairobi")),
    end_date=datetime(2025, 10, 23, 18, 2, tzinfo=timezone("Africa/Nairobi")),
    schedule="0 18 * * *",
    catchup=False
) as dag:

    run_script = BashOperator(
        task_id="run_script_1",
        bash_command="python /opt/airflow/fraud_scripts/script_1.py"
    )
