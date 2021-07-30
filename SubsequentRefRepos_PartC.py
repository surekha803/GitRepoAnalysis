import csv

import csvwriter as csvwriter
from  google.cloud import bigquery
from google.oauth2 import service_account
import google.auth
from google.cloud import bigquery_storage
import json
import pandas as pd
import json
import pickle
import ast
# from CoreContributors_forallRepos import pusheventsbycontri_forallrepos,Corecontributors_forallrepos
# from CitingRefReposPartA import *


credentials = service_account.Credentials.from_service_account_file(
    'C:\\Users\\iialab\\OneDrive - UNT System\\Documents\\Git_analysis\\github-project-310921-189911271191.json')
project_id = 'github-project-310921'

bqclient = bigquery.Client(credentials=credentials, project=project_id)
bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)
client = bigquery.Client(credentials= credentials,project=project_id)

# a1_grpbyrepoid_org_1=pusheventsbycontri_forallrepos()
# CC_forallrepos_df_partc =Corecontributors_forallrepos(a1_grpbyrepoid_org_1)

partB_repos_forpartCinput = bqclient.query("""select  main_repo_id as PartB_mainrepo,PartB_refrepos as PartB_ref from github-project-310921.githubproject_rawdataset.PartB_ref_repos_temp """).result().to_dataframe(bqstorage_client=bqstorageclient)
print("partB repos in partcfile",partB_repos_forpartCinput.head())

partA_repos_forpartCinput = bqclient.query("""select  main_repo_id as PartA_mainrepo,PartA_refrepos as PartA_ref from github-project-310921.githubproject_rawdataset.PartA_ref_repos_temp_test """).result().to_dataframe(bqstorage_client=bqstorageclient)
print("partA repos in partcfile",partA_repos_forpartCinput.head())

CC_forallrepos_df = bqclient.query("""select  repo_id ,actor_id  from github-project-310921.githubproject_rawdataset.core_contributors_forallrepos_test """).result().to_dataframe(bqstorage_client=bqstorageclient)
print("partB repos in partcfile",partB_repos_forpartCinput.head())

PartC_output_df = []

for index, row in partB_repos_forpartCinput.iterrows():
    main_repo_id=row['PartB_mainrepo']
    print("main_repo_id is",main_repo_id)
    PartB_refrepos = row['PartB_ref']
    print("PartB_refrepos is",PartB_refrepos)
    print(type(main_repo_id),type(PartB_refrepos))
    a_list = PartB_refrepos.split(",")
    map_object = map(int, a_list)
    list_of_integers = list(map_object)
    print("list_of_integers",list_of_integers,type(list_of_integers))
    actorids_partB_refrepos_df1=CC_forallrepos_df[CC_forallrepos_df['repo_id'].isin(list_of_integers)]
    print("@@@@@@@@@@@actorids_partB_refrepos_df1 output",actorids_partB_refrepos_df1.head())
    print("partA_repos_forpartCinput",partA_repos_forpartCinput.head())
    partA_refrepos=partA_repos_forpartCinput.loc[partA_repos_forpartCinput['PartA_mainrepo'] == main_repo_id]
# b_list=str(partA_refrepos).split(",")
# map_object_b = map(int, b_list)
# list_of_integers_b = list(map_object)
# print("PartA repo output",list_of_integers_b,type(list_of_integers_b))
# print("type pf partA_refrepos is",partA_refrepos,partA_refrepos.astype(str).values.flatten().tolist())
    print("type of partA_refrepos is",partA_refrepos,type(partA_refrepos))
    if partA_refrepos.empty == False and actorids_partB_refrepos_df1.empty == False:
        req_values=partA_refrepos.PartA_ref.str.split(',')
        print("###############",req_values,type(req_values))
        a=req_values.apply(pd.Series).stack().reset_index(drop = True)
        print("!!!!!!!!!!!!!!",a,type(a))
        b=list(a)
        print("type of b",type(b))
        str1 = list(map(int, b))
        print("^^^^^",type(str1),str1)
        str2=str(str1)[1:-1]
        print("(((((((",str2)
    # if partA_refrepos.empty == False and actorids_partB_refrepos_df1.empty == False:
    #     print("entered in to if loop")
    #     print("partA_refrepos is",partA_refrepos,"actorids_partB_refrepos_df1 is ",actorids_partB_refrepos_df1)
        print("""select  distinct repo.id as repo_id_A,actor.id as actor_id from """+"`"+"github-project-310921.githubproject_rawdataset.20*"""+"`"+"""
                where  type='PullRequestEvent' and repo.id in ("""+str2+""")""")
        actorids_partA_refrepos_df2 = bqclient.query("""select  distinct repo.id as repo_id_A,actor.id as actor_id from """+"`"+"github-project-310921.githubproject_rawdataset.20*"""+"`"+"""
                  where  type='PullRequestEvent' and repo.id in ("""+str2+""")""").result().to_dataframe(bqstorage_client=bqstorageclient)
        print("actorids_partA_refrepos_df2",actorids_partA_refrepos_df2)
        df2_final=actorids_partA_refrepos_df2.fillna(0)
        df_a=df2_final.astype(int)
        print("df2_final",df_a)
        result = actorids_partB_refrepos_df1.merge(df2_final, how='inner', on=['actor_id'])
        print("df1",actorids_partB_refrepos_df1.head())
        print("result",result)
        partC_repoids_output=result[["repo_id_A","repo_id"]]
        print("&&&&&&&&&&&&&&&&final part C output&&&&&&&&&&&&&&",partC_repoids_output,type(partC_repoids_output))
        PartC_output_df.append(partC_repoids_output)
PartC_output_df=pd.concat(PartC_output_df)
print("appended df",PartC_output_df.head())
PartC_output_df.to_csv(r'C:\Users\iialab\OneDrive - UNT System\Documents\Git_analysis\Subsequentwork_PartC_Outputfiles\\corecontributors.csv',index=False)




