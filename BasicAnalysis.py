import pandas as pd
import csv

df = pd.read_csv (r'C:\Users\iialab\OneDrive - UNT System\Documents\Git_analysis\git_analysis_file.csv')
print (df)
print(df.columns)
# creating a dataframe filter on Pushevent
newdf = df[(df.type == "PushEvent")]

a1_grpbyrepoid=newdf.groupby(["repo_id","actor_id"])["actor_id"].count().reset_index(name="push_eventsby_contributor")
#Write data of tota
a1_grpbyrepoid.to_csv(r'C:\Users\iialab\OneDrive - UNT System\Documents\Git_analysis\BasicAnalysis_Outputfiles\push_eventsby_each_contri.csv',index=False)


a1_grpbyrepoid_totalcont=a1_grpbyrepoid.groupby("repo_id").push_eventsby_contributor.sum().reset_index(name="Total_number_of_contributors")
a1_grpbyrepoid_totalcont.to_csv(r'C:\Users\iialab\OneDrive - UNT System\Documents\Git_analysis\BasicAnalysis_Outputfiles\total_number_of_contributors.csv',index=False)

a2=a1_grpbyrepoid[a1_grpbyrepoid['push_eventsby_contributor'] == a1_grpbyrepoid.groupby('repo_id')['push_eventsby_contributor'].transform('max')]
a3=a2.rename(columns={'push_eventsby_contributor':'Core_Contributors'})
a3.to_csv(r'C:\Users\iialab\OneDrive - UNT System\Documents\Git_analysis\BasicAnalysis_Outputfiles\corecontributors.csv',index=False)

val = 5
print(a1_grpbyrepoid_totalcont.head())
testlist=a1_grpbyrepoid_totalcont.values.tolist()
print("list",testlist)
with open(r'C:\Users\iialab\OneDrive - UNT System\Documents\Git_analysis\BasicAnalysis_Outputfiles\Teamsize.csv', 'w') as f_object:
    writer_object = csv.writer(f_object)
    for x in range(len(testlist)):
        if(int(testlist[x][1]) > 5 ):
            appnd_str1="Core contributor is belonged to large group"
            print("entered in to if loop",testlist[x])
            testlist[x].append(appnd_str1)
            writer_object.writerow(testlist[x])
        else:
            appnd_str2 = "Core contributor is belonged to small group"
            testlist[x].append(appnd_str2)
            print("entered in to else loop",testlist[x])
            writer_object.writerow(testlist[x])