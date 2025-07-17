from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'marina',
}

with DAG(
    dag_id='import_f101_round_v2',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    import_from_csv = BashOperator(
        task_id='import_from_csv',
        bash_command='cd /home/marinaub/PycharmProjects/Csv_export_import && .venv/bin/python import_f101_round.py'
    )

    import_from_csv

