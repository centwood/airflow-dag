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


#try:
#    import psycopg2 as pg
#    import psycopg2.extras
#except:
#    print('Install psycopg2')
#    exit(123)

#try:
#    import progressbar
#except:
#    print ('Install progressbar2')
#    exit(123)

import json

import logging
logger = logging.getLogger()


config.load_kube_config()

v1     = client.CoreV1Api()
secret = v1.read_namespaced_secret("airflow-postgresdb", "airflow")
data   = secret.data

decodes = base64.b64decode(secret.data["pgurl"])


pgurl = decodes.decode('utf-8')






conn_string = pgurl


#data_dir = "c:\papers"
data_dir = "/mnt/c/papers/arxiv_batch_461.json"
#dbconn = pg.connect(PG_CONN_STRING)

dbconn = pg.connect(pgurl)

logger.info("Loading data from '{}'".format(data_dir))

cursor = dbconn.cursor()

counter = 0
empty_files = []


#class ProgressInfo:
#
#    def __init__(self, dir):
#        files_no = 0
#        for (root, dirs, files) in os.walk(dir):
#            for file in files:
#                if file.endswith('.json'):
#                    files_no += 1
#        self.files_no = files_no
#        print ('Found {} files to process'.format(self.files_no))
#        self.bar = progressbar.ProgressBar(maxval=self.files_no,
#                widgets=[
#            ' [',
#            progressbar.Timer(),
#            '] [',
#            progressbar.ETA(),
#            '] ',
#            progressbar.Bar(),
#            ])
#
#    def update(self, counter):
#        self.bar.update(counter)


#pi = ProgressInfo(os.path.expanduser(data_dir))

for (root, dirs, files) in os.walk(os.path.expanduser(data_dir)):
    for f in files:
        fname = os.path.join(root, f)

        if not fname.endswith('.json'):
            continue
        with open(fname) as js:
            data = js.read()
            if not data:
                empty_files.append(fname)
                continue
            import json
            dd = json.loads(data)


            df = pd.DataFrame(dd)
#print(df)

            db = create_engine(conn_string)

            df['authors'] = list(map(lambda x: json.dumps(x), df['authors']))
            df['categories'] = list(map(lambda x: json.dumps(x), df['categories']))
            df['entry_id'] = list(map(lambda x: json.dumps(x), df['entry_id']))
            df['journal_ref'] = list(map(lambda x: json.dumps(x), df['journal_ref']))
            df['doi'] = list(map(lambda x: json.dumps(x), df['doi']))
            df['primary_category'] = list(map(lambda x: json.dumps(x), df['primary_category']))
            df['title'] = list(map(lambda x: json.dumps(x), df['title']))
            df['summary'] = list(map(lambda x: json.dumps(x), df['summary']))
            df['updated'] = list(map(lambda x: json.dumps(x), df['updated']))
            df['latex'] = list(map(lambda x: json.dumps(x), df['latex']))
            df['text'] = list(map(lambda x: json.dumps(x), df['text']))
            df['oa_alternate_host_venues'] = list(map(lambda x: json.dumps(x), df['oa_alternate_host_venues']))
            df['oa_authorships'] = list(map(lambda x: json.dumps(x), df['oa_authorships']))
            df['oa_authorships_authors_name_flat'] = list(map(lambda x: json.dumps(x), df['oa_authorships_authors_name_flat']))
            df['oa_authorships_authors_affiliation_raw'] = list(map(lambda x: json.dumps(x), df['oa_authorships_authors_affiliation_raw']))
            df['oa_authorships_authors_affiliations_flat'] = list(map(lambda x: json.dumps(x), df['oa_authorships_authors_affiliations_flat']))
            df['oa_cited_by_count'] = list(map(lambda x: json.dumps(x), df['oa_cited_by_count']))
            df['oa_host_venue'] = list(map(lambda x: json.dumps(x), df['oa_host_venue']))
            df['oa_host_venue_name'] = list(map(lambda x: json.dumps(x), df['oa_host_venue_name']))
            df['oa_host_venue_published'] = list(map(lambda x: json.dumps(x), df['oa_host_venue_published']))



#print(df.dtypes)

            df.to_sql('papers_stagging_19', schema='public', con = db, if_exists='append')