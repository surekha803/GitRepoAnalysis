import csv
import itertools

import csvwriter as csvwriter
import numpy as np
from  google.cloud import bigquery
from google.oauth2 import service_account
import google.auth
from google.cloud import bigquery_storage
import json
import pandas as pd
from CoreContributors import pushevnts_byech_contri_forreqrepos,CC_forrequiredrepos
from graphviz import Graph
from pandas import DataFrame

credentials = service_account.Credentials.from_service_account_file(
    'C:\\Users\\iialab\\OneDrive - UNT System\\Documents\\Git_analysis\\github-project-310921-189911271191.json')
project_id = 'github-project-310921'

bqclient = bigquery.Client(credentials=credentials, project=project_id)
bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)
client = bigquery.Client(credentials= credentials,project=project_id)

# sampledatadf = pd.DataFrame({'repo_id':[254239,254239,951231,1799466],'actor_id':[90647,67020,10701,1306453]})
# print(sampledatadf.head())
# sampledatadf.to_gbq('githubproject_rawdataset.core_contributors_forallrepos_test',
#                                     project_id,
#                                     chunksize=None,
#                                     if_exists='append'
#                                     )


sampledatadf2 = pd.DataFrame({'main_repo_id':[206084],'PartA_refrepos':['254239,951231,1799466']})
print(sampledatadf2.head())
sampledatadf2.to_gbq('githubproject_rawdataset.PartA_ref_repos_temp_test',
                                    project_id,
                                    chunksize=None,
                                    if_exists='append'
                                    )