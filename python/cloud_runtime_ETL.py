import os
import sys
import logging
import json
import psycopg2 as pg
from psycopg2.extras import Json
from psycopg2 import connect, Error
import pandas as pd 
from sqlalchemy import create_engine
import numpy as np
#import psycopg2.extras as extras
import psycopg2.extras
import sqlalchemy
#import progressbar
from kubernetes import client, config
import base64

config.load_kube_config()

v1     = client.CoreV1Api()
secret = v1.read_namespaced_secret("airflow-postgresdb", "airflow")
data   = secret.data

decodes = base64.b64decode(secret.data["pgurl"])


pgurl = decodes.decode('utf-8')


conn_string = pgurl

dbconn = pg.connect(pgurl)

db = create_engine(conn_string)

'''
connection = db.raw_connection()
cursor = connection.cursor()

cursor.execute
'''


results = db.execute('call insert_cloud_runtime_job_test();')

