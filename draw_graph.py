# libraries
import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
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

print(client.project)
query_job = bqclient.query(""" select main_repo_id as repo_id,PartB_refrepos as partB_ref from github-project-310921.githubproject_rawdataset.PartB_ref_repos_final
 """).result().to_dataframe(bqstorage_client=bqstorageclient)
print(query_job.head())

G=nx.from_pandas_edgelist(query_job, 'repo_id', 'partB_ref', create_using=nx.DiGraph())

# Make the graph directed graph
nx.draw(G, with_labels=True, node_size=1500, alpha=0.3, arrows=True)
plt.show()