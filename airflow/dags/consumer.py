from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
from pendulum import timezone

with DAG(
    dag_id="run_consumer",
    start_date=datetime(2025, 1, 1, tzinfo=timezone("Africa/Nairobi")),
    schedule="0 10 * * 2",
    catchup=False
) as dag:

   run_script_5_consumer = BashOperator(
        task_id="run_script_5",
        bash_command="python /opt/airflow/fraud_scripts/script_5_consumer.py"
    )
