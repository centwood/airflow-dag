from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from time import sleep
from datetime import datetime

postgres_conn_id = ''
postgres_uri = ''
gitHubTokenID = ''



def my_func(p1, p2):
    print(p1)
    print(p2)
    

with DAG('python_dag_github', description='Python DAG', schedule_interval='*/5 * * * *', start_date=datetime(2018, 11, 1), catchup=False) as dag:
    dummy_task      = DummyOperator(task_id='dummy_task', retries=3)
    python_task     = PythonOperator(task_id='python_task', python_callable=my_func, op_args=[postgres_uri, gitHubTokenID])
dummy_task >> python_task