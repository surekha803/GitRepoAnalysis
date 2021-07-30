import csv

import csvwriter as csvwriter
import gcloud as gcloud
from  google.cloud import bigquery
from google.oauth2 import service_account
import google.auth
from google.cloud import bigquery_storage
from google.cloud import storage
import dask.dataframe as dd
import pandas as pd
import gcsfs
import os
from io import BytesIO
from pydata_google_auth.__main__ import login

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'github-project-310921-189911271191.json'
storage_client = storage.Client()


df = dd.read_csv('gcs://githubreposdata/github_data*.csv')
df1=df[['repo_id','actor_id']]
print(df1.head())
a1_grpbyrepoid_temp=df1.groupby(["repo_id","actor_id"])["actor_id"].count().reset_index(name="push_eventsby_contributor")
print(a1_grpbyrepoid_temp.head())

