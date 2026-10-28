from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import pendulum

# Set Timezone to Tanzania
local_tz = pendulum.timezone("Africa/Dar_es_Salaam")

default_args = {
    'owner': 'softnet_softbi_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'tcb_bank_data_generator',
    default_args=default_args,
    description='Generates TCB Bank Synthetic Data',
    schedule_interval='0 */3 * * *',  # Runs at minute 0 past every 3rd hour
    start_date=datetime(2023, 1, 1, tzinfo=local_tz),
    catchup=False, # Don't run for past dates, start from now
    tags=['banking', 'spark'],
) as dag:

    # Task: Run the Python Orchestrator
    # We include /opt/bitnami/spark/output in the path just in case
    run_generation = BashOperator(
        task_id='run_spark_generation_logic',
        bash_command='python /opt/airflow/apps/main.py',
        env={
            'SPARK_MASTER': 'spark://spark-master:7077',
            'PYTHONPATH': '/opt/airflow/apps',
            'GENERATION_DAYS': '4',
            'DORIS_HOST': os.getenv('DORIS_HOST', '192.168.201.12'),
            'DORIS_PORT': os.getenv('DORIS_PORT', '9030'),
            'DORIS_HTTP_PORT': os.getenv('DORIS_HTTP_PORT', '8030'),
            'DORIS_DATABASE': os.getenv('DORIS_DATABASE', 'maduro_one_one'),
            'DORIS_USER': os.getenv('DORIS_USER', 'root'),
            'DORIS_PASSWORD': os.getenv('DORIS_PASSWORD', ''),
        }
    )

    run_generation
