
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.utils.dates import days_ago
import sqlalchemy as db
from sqlalchemy import create_engine
from github import Github 
import pandas as pd 
from datetime import date
from airflow.models import Variable 
from airflow.providers import GitHub


def execute_process():

    #"""Local Connection String """
    #conn = PostgresHook(conn_id='postgres_local')
    #uri = conn.get_uri

    #conn_string_source = uri
    conn_string_source = 'postgresql://postgres:L45hTHHxM7hhR46PJlIo@localhost:5432/postgres'

    alchemyEngineSource = create_engine(conn_string_source)


    dbConnectionSource    = alchemyEngineSource.connect()

    #"""Access Token for GitHub"""
    ACCESS_TOKEN = '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'#Variable.get("gitHubToken")

    g = Github(ACCESS_TOKEN)


    #"""Querying the Postges Database to produce the list of Repos to monitor"""
    metadata = db.MetaData()

    githubMonOrg = db.Table('git_hub_monitoring_repository', metadata, autoload=True, autoload_with=alchemyEngineSource)

    query = db.select([githubMonOrg.columns.name])

    results = alchemyEngineSource.execute(query)


    #"""Loop Trough the Repos to get the count of stars, subscribers, forks"""
    for record in results:
        
         
        repo = g.get_repo(record.name)
        
        starcount = repo.get_stargazers().totalCount


        stargazerfilledcount = repo.get_watchers().totalCount


        forkcount = repo.get_forks().totalCount


        subscribercount = repo.get_subscribers().totalCount
    #Insert Github data into the postgres data

        df = pd.DataFrame()

        stargazerDF = pd.DataFrame([[starcount,repo.full_name,date.today()]],columns=['stargazercount','repoName','loadDate'])
        stargazerDF.to_sql('git_hub_stargazer', con = dbConnectionSource, method = 'multi', schema='public',  if_exists='append',chunksize=1000, index=False)
            
        watcherDF = pd.DataFrame([[stargazerfilledcount,repo.full_name,date.today()]],columns=['watchercount','repoName','loadDate'])
        watcherDF.to_sql('git_hub_watcher', con = dbConnectionSource, method = 'multi', schema='public',  if_exists='append',chunksize=1000, index=False)

        forkDF = pd.DataFrame([[forkcount,repo.full_name,date.today()]],columns=['forkcount','repoName','loadDate'])
        forkDF.to_sql('git_hub_fork', con = dbConnectionSource, method = 'multi', schema='public',  if_exists='append',chunksize=1000, index=False)

        subscriberDF = pd.DataFrame([[subscribercount,repo.full_name,date.today()]],columns=['subscribercount','repoName','loadDate'])
        subscriberDF.to_sql('git_hub_subscribers', con = dbConnectionSource, method = 'multi', schema='public',  if_exists='append',chunksize=1000, index=False)

with DAG(dag_id='github_import',
         default_args={'owner': 'airflow'},
         schedule_interval='@daily',
         start_date=days_ago(1)
    ) as dag:

    insert_github_into_table = PythonOperator(
        task_id='insert_github_into_table',
        python_callable=execute_process,
        
    )