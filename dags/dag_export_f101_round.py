from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'marina',
}

with DAG(
    dag_id='export_f101_round',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    export_to_csv = BashOperator(
        task_id='export_to_csv',
        bash_command='cd /home/marinaub/PycharmProjects/Csv_export_import && .venv/bin/python export_f101_round.py'
    )

    export_to_csv

