from sys import exec_prefix
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
#from python.papers_staging import papers_etl, progressbar
from datetime import datetime
from airflow.utils.dates import days_ago

with DAG(dag_id='arxiv_paper_etl',
         default_args={'owner': 'airflow'},
         schedule_interval='@daily',
         start_date=days_ago(1)
    ) as dag:

    arxiv_file_to_psql = BashOperator(
        task_id = 'arxiv_file_to_psql',
        bash_command = 'python /opt/airflow/dags/python/arxiv_papers_ETL.py'
    )

    arxiv_file_to_psql 