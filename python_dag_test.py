from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from time import sleep
from datetime import datetime

token = Variable.get("gitHubToken")
tokenStr = str(token)

def my_func(*op_args):
        print(op_args)
        return op_args[0]

with DAG('python_dag', description='Python DAG', schedule_interval='*/5 * * * *', start_date=datetime(2018, 11, 1), catchup=False) as dag:
        dummy_task      = DummyOperator(task_id='dummy_task', retries=3)
        python_task     = PythonOperator(task_id='python_task', python_callable=my_func, op_args=[tokenStr])

        dummy_task >> python_task