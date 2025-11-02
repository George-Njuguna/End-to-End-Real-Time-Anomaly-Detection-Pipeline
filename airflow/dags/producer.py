from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
from pendulum import timezone

with DAG(
    dag_id="run_producer",
    start_date=datetime(2025, 1, 1, tzinfo=timezone("Africa/Nairobi")),
    schedule="0 10 * * 2",
    retries=0,
    catchup=False
) as dag:

    run_script_4_producer = BashOperator(
        task_id="run_script_4",
        bash_command="python /opt/airflow/fraud_scripts/script_4_producer.py"
    )
