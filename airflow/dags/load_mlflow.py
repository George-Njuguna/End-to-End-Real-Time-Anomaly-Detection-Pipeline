from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from pendulum import timezone

with DAG(
    dag_id="run_script_2_n_3_every_week",
    start_date=datetime(2025, 1, 1, tzinfo=timezone("Africa/Nairobi")),
    schedule="0 10 * * 2",
    catchup=False
) as dag:

    run_script_2 = BashOperator(
        task_id="run_script_2",
        bash_command="python /opt/airflow/fraud_scripts/script_2.py"
    )

    run_script_3 = BashOperator(
        task_id="run_script_3",
        bash_command="python /opt/airflow/fraud_scripts/script_3.py"
    )

    run_script_2 >> run_script_3
