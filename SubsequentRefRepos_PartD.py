import numpy as np
import pandas as pandas
from  google.cloud import bigquery
from google.oauth2 import service_account
import google.auth
from google.cloud import bigquery_storage

credentials = service_account.Credentials.from_service_account_file(
    'C:\\Users\\iialab\\OneDrive - UNT System\\Documents\\Git_analysis\\github-project-310921-189911271191.json')
project_id = 'github-project-310921'

bqclient = bigquery.Client(credentials=credentials, project=project_id)
bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)
client = bigquery.Client(credentials= credentials,project=project_id)

partA_repos_forpartCinput = bqclient.query("""select  main_repo_id as PartA_mainrepo,PartA_refrepos as PartA_ref from github-project-310921.githubproject_rawdataset.PartA_ref_repos_temp_test """).result().to_dataframe(bqstorage_client=bqstorageclient)
print("partA repos in partcfile",partA_repos_forpartCinput.head())

CC_forallrepos_df = bqclient.query("""select  repo_id ,actor_id  from github-project-310921.githubproject_rawdataset.core_contributors_forallrepos_test """).result().to_dataframe(bqstorage_client=bqstorageclient)

# unnest_df=pandas.DataFrame({'PartA_mainrepo': np.repeat(partA_repos_forpartCinput.PartA_mainrepo.values,partA_repos_forpartCinput.PartA_ref.str.len()),
#                         'PartA_ref': np.concatenate(partA_repos_forpartCinput.PartA_ref.values)})

PartD_output_df=[]

for index, row in partA_repos_forpartCinput.iterrows():
    a=row['PartA_mainrepo']
    b=row['PartA_ref']
    print(b)
    print("type of b",type(b))
    b_list=b.split(",")
    print("type of b_list",type(b_list))
    print("""select  distinct repo.id as repo_id_A,actor.id as actor_id from """+"`"+"github-project-310921.githubproject_rawdataset.20*"""+"`"+"""
                where  type='PullRequestEvent' and repo.id in ("""+b+""")""")
    actorids_partA_refrepos_df2 = bqclient.query("""select  distinct repo.id as repo_id_A,actor.id as actor_id from """+"`"+"github-project-310921.githubproject_rawdataset.20*"""+"`"+"""
                  where  type='PullRequestEvent' and repo.id in ("""+b+""")""").result().to_dataframe(bqstorage_client=bqstorageclient)
    df2=actorids_partA_refrepos_df2.fillna(0)
    df2_final=df2.astype(int)
    print("df2_final",df2_final)
    list_of_actors=df2_final['actor_id'].tolist()
    print("!!!!!!!!!!!",list_of_actors)
    CC_output=CC_forallrepos_df[CC_forallrepos_df['actor_id'].isin(list_of_actors)]
    print("@@@@@@@@",CC_output)
    CC_output_final=CC_output[~CC_output['repo_id'].isin(b_list)]
    print("&&&&&&&",CC_output_final)
    result = df2_final.merge(CC_output_final, how='inner', on=['actor_id'])
    print("df2",df2.head())
    print("CC_output_final",CC_output_final.head())
    print("result",result)
    partD_repoids_output=result[["repo_id","repo_id_A"]]
    PartD_output_df.append(partD_repoids_output)

PartD_output_df=pandas.concat(PartD_output_df)
print("appended df",PartD_output_df.head())
PartD_output_df.to_gbq('githubproject_rawdataset.PartD_ref_repos_final_test',
                                        project_id,
                                        chunksize=None,
                                        if_exists='replace'
                                        )
print("!!!!!!!!!!! created table partD_repoids_output done !!!!!")