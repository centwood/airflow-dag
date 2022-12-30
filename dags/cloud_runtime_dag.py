from sys import exec_prefix
from airflow import DAGhelmhelm
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
#from python.papers_staging import papers_etl, progressbar
from datetime import datetime
from airflow.utils.dates import days_ago
from kubernetes import client, config
import base64
import os
from airflow.kubernetes.secret import Secret 
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.configuration import conf

#config.load_kube_config(context='rancher-desktop')

#load_incluster_config()

config.load_incluster_config()

v1     = client.CoreV1Api()
secret = v1.read_namespaced_secret("airflow-postgresdb", "airflow")
data   = secret.data

decodes = base64.b64decode(secret.data["pgurl"])


pgurl = decodes.decode('utf-8')

namespace = conf.get("kubernetes","airflow")

secret_all_keys  = Secret('env', None, 'airflow-secrets-2')





with DAG(dag_id='cloud_runtime_etl',
         default_args={'owner': 'airflow'},
         schedule_interval='@daily',
         start_date=days_ago(1)
    ) as dag:

    execute_sql_function_insert_to_table = PostgresOperator(
        task_id='execute_procedure_insert_cloud_runtime_job_test',
        AIRFLOW_CONN_POSTGRES_MASTER = pgurl,
        sql='''
        call insert_cloud_runtime_job_test();
        '''
    )

    execute_sql_function_insert_to_table

    #postgres_conn_id = 'localPSQL',