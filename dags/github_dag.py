import sqlalchemy as db
from sqlalchemy import create_engine
from github import Github 
import pandas as pd 
from datetime import date
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from datetime import datetime

postgres_conn_id = PostgresHook(postgres_conn_id='postgres_local')
postgres_uri = postgres_conn_id.get_uri()
gitHubTokenID = Variable.get("gitHubToken")



def my_func(p1, p2):
    #"""Local Connection String """
    conn_string_source = p1  

    alchemyEngineSource = create_engine(conn_string_source)


    dbConnectionSource    = alchemyEngineSource.connect()

    #"""Access Token for GitHub"""
    ACCESS_TOKEN = p2

    g = Github(ACCESS_TOKEN)


    #"""Querying the Postges Database to produce the list of Repos to monitor"""
    metadata = db.MetaData()

    githubMonOrg = db.Table('git_hub_monitoring_repository', metadata, autoload=True, autoload_with=alchemyEngineSource)

    query = db.select([githubMonOrg.columns.name])

    results = alchemyEngineSource.execute(query)


    #"""Loop Trough the Repos to get the count of stars, subscribers, forks"""
    for record in results:
        
        repoName = '' 
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



    with DAG('python_dag_github', description='Python DAG', schedule_interval='*/5 * * * *', start_date=datetime(2018, 11, 1), catchup=False) as dag:
        dummy_task      = DummyOperator(task_id='dummy_task', retries=3)
        python_task     = PythonOperator(task_id='python_task', python_callable=my_func, op_args=[postgres_uri, gitHubTokenID])
    dummy_task >> python_task