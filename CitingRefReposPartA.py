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
import os
os.environ["PATH"] += os.pathsep + 'C:\\Program Files\\Graphviz\\bin'

credentials = service_account.Credentials.from_service_account_file(
    'C:\\Users\\iialab\\OneDrive - UNT System\\Documents\\Git_analysis\\github-project-310921-189911271191.json')
project_id = 'github-project-310921'

bqclient = bigquery.Client(credentials=credentials, project=project_id)
bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)
client = bigquery.Client(credentials= credentials,project=project_id)

print(client.project)
query_job_subset = bqclient.query("""
   SELECT repo.id as repo_id,actor.id as actor_id,type,created_at 
   FROM github-project-310921.githubproject_rawdataset.githubrepo_dataset where repo.name in 
    ("selfhub/selfhub","haskell/containers","fsharp/FAKE","kanaka/websockify","numenta/nupic.core","flynn/flynn","JuliaLang/julia","selfhub/selfhub","selfhub/selfhub","selfhub/selfhub","dealii/dealii","dealii/dealii","WhisperSystems/TextSecure","WhisperSystems/TextSecure","WhisperSystems/TextSecure","rackerlabs/otter","mono/MonoGame","nylira/prism-break","rakudo/rakudo","rakudo/rakudo","tsuru/tsuru","robfig/cron","PX4/Firmware","WhisperSystems/TextSecure","rust-lang/rust","flynn/flynn","blueboxgroup/ursula","selfhub/selfhub","EU-OSHA/osha-website","emberjs/data","collectiveidea/delayed_job","scottjehl/picturefill","scottjehl/picturefill","rust-lang/rfcs","idno/idno","tsuru/tsuru","matplotlib/matplotlib","rapid7/metasploit-framework","rapid7/metasploit-framework","caskroom/homebrew-cask","enovance/edeploy","dealii/dealii","selfhub/selfhub","selfhub/selfhub","enovance/edeploy","edx/edx-platform","mopidy/mopidy-spotify","yiisoft/yii2","emberjs/data","SlimeKnights/TinkersConstruct","d3athrow/vgstation13","sonata-project/SonataMediaBundle","rakudo/rakudo","opf/openproject","opf/openproject","opf/openproject","colszowka/simplecov","SceneIt/Thesis","twbs/bootstrap","alvarotrigo/fullPage.js","b4winckler/macvim","Microsoft/dotnet","TechCavern/WaveTact","TechCavern/WaveTact","Microsoft/dotnet","blueboxgroup/ursula","matplotlib/matplotlib","cowboy/dotfiles","emberjs/data","GoodBoyDigital/pixi.js","leapmotion/autowiring","rapid7/metasploit-framework","rethinkdb/rethinkdb","silverstripe/silverstripe-framework","neo4j/neo4j","django/django","allejo/LeagueOverseer","mavlink/mavlink","ReikaKalseki/DragonAPI","tomchristie/django-rest-framework","rapid7/metasploit-framework","iojs/io.js","Brickimedia/brickimedia","haskell/containers","emberjs/data","dfm/osrc","cocos2d/cocos2d-x","twitter/algebird","diydrones/ardupilot","cakephp/cakepackages","rust-lang/rust","rust-lang/rust","gradle/gradle","LMMS/lmms","LMMS/lmms","rust-lang/rust","FreeRDP/Remmina","hrydgard/ppsspp","mopidy/mopidy-spotify","rust-lang/rfcs","Famous/famous","emberjs/ember.js","docker/fig","ericam/true","leapmotion/autowiring","docker/docker","sugarlabs/sugar","TrinityCore/TrinityCore","LMMS/lmms","LMMS/lmms","diydrones/ardupilot","TrinityCore/TrinityCore","musescore/MuseScore","CocoaPods/Specs","enovance/edeploy","hadley/adv-r","openfl/openfl","NixOS/nixpkgs","opencrowbar/build-tools","opencrowbar/build-tools","sugarlabs/sugar","CleverRaven/Cataclysm-DDA","Semantic-Org/Semantic-UI","videojs/video.js","Reactive-Extensions/RxJS","dflydev/dflydev-embedded-composer","jashkenas/backbone","jarednova/timber","docker/machine","lodash/lodash","WordPress/WordPress","sagemath/sagecell","openfl/openfl","openfl/openfl","scotthartbti/android_packages_apps_Settings","deadlyvipers/dojo_rules","Rythal/Carbonite","NixOS/nixpkgs","haskell/vector","barryclark/jekyll-now","puppetlabs-operations/puppet-puppet","facebook/react","OpenRA/OpenRA","rust-lang/rfcs","tgstation/-tg-station","TrinityCore/TrinityCore","TrinityCore/TrinityCore","sstephenson/bats","HabitRPG/habitrpg","numenta/nupic.core","twosigma/beaker-notebook","twosigma/beaker-notebook","SpongePowered/SpongeAPI","twosigma/beaker-notebook","moneymanagerex/moneymanagerex","mozilla/togetherjs","grafana/grafana","lodash/lodash","D-Programming-Language/dmd","twbs/bootstrap","twbs/bootstrap","mcMMO-Dev/mcMMO","openfl/openfl","MiCode/patchrom","twbs/bootstrap","twbs/bootstrap","realm/realm-cocoa","collectiveidea/awesome_nested_set","twbs/bootstrap","twbs/bootstrap","twbs/bootstrap","emberjs/data","code-friends/CodeFriends","code-friends/CodeFriends","jlnr/gosu","JuliaLang/METADATA.jl","jlnr/gosu","emberjs/data","JuliaLang/METADATA.jl","twbs/bootstrap","django/django","puppetlabs-operations/puppet-puppet","puppetlabs-operations/puppet-puppet","PredictionIO/PredictionIO","cloudify-cosmo/cloudify-manager","cloudify-cosmo/cloudify-manager","twbs/bootstrap","twbs/bootstrap","twbs/bootstrap","twbs/bootstrap","ccoenraets/nodecellar","diydrones/ardupilot","NREL/OpenStudio","graphite-project/whisper","CleverRaven/Cataclysm-DDA","pkgcloud/pkgcloud","Joomla-Bible-Study/Joomla-Bible-Study","net-ssh/net-ssh","bjorn/tiled","HabitRPG/habitrpg","symfony/symfony","numenta/nupic.core","Valloric/YouCompleteMe","MachineMuse/MachineMusePowersuits","codeforamerica/codeforamerica.org","selfhub/selfhub","codeforamerica/codeforamerica.org","Merchello/Merchello","Merchello/Merchello","leapmotion/autowiring","Merchello/Merchello","jshint/fixmyjs","philc/vimium","moneymanagerex/moneymanagerex","ariya/phantomjs","NixOS/nix","ExactTarget/fuelux","selfhub/selfhub","selfhub/selfhub","jekyll/jekyll","openfl/lime","rwaldron/johnny-five","roboterclubaachen/xpcc","hrydgard/ppsspp","xamarin/mobile-samples","mesosphere/marathon","lodash/lodash","collectiveidea/awesome_nested_set","owncloud/documentation","Prinzhorn/skrollr","ruby/ruby","selfhub/selfhub","lodash/lodash","johnmccutchan/game_loop","openframeworks/openFrameworks","edx/edx-platform","roboterclubaachen/xpcc","OSU-Net/cyder","cython/cython","maidsafe/MaidSafe-Drive","clojure/clojurescript","rust-lang/rust","openfl/lime","selfhub/selfhub","DarkstarProject/darkstar","Unitech/PM2","mmistakes/so-simple-theme","mmistakes/so-simple-theme","owncloud/documentation","DarkstarProject/darkstar","saltstack/salt","symfony/symfony","tgstation/-tg-station","openfl/lime","Joomla-Bible-Study/Joomla-Bible-Study","jbevain/cecil","snapframework/heist","JuliaLang/julia","selfhub/selfhub","simbody/simbody","google/closure-compiler","peatio/peatio","roboterclubaachen/xpcc","inspircd/inspircd","TrinityCore/TrinityCore","cmangos/mangos-classic","Reactive-Extensions/RxJS","mozilla/bedrock","angular-ui/bootstrap","cython/cython","rspec/rspec-rails","rspec/rspec-rails","rspec/rspec-rails","leapmotion/autowiring","SemanticMediaWiki/SemanticMediaWiki","Automattic/jetpack","johnmccutchan/game_loop","scriptdev2/scriptdev2-classic","deadlyvipers/dojo_rules","mozilla/gecko-dev","ros-infrastructure/rep","Joomla-Bible-Study/Joomla-Bible-Study","phoenixframework/phoenix","paulirish/matchMedia.js","nodejitsu/node-http-proxy","deadlyvipers/dojo_rules","discourse/discourse","linuxmint/Cinnamon","thephpleague/factory-muffin","haskell/unix","pisilinux/PisiLinux","Joomla-Bible-Study/Joomla-Bible-Study","selfhub/selfhub","selfhub/selfhub","rspec/rspec-rails","selfhub/selfhub","MassiveCraft/Factions","symfony/symfony","SemanticMediaWiki/SemanticMediaWiki","saltstack/salt","WebKit/webkit","Tribler/tribler","thephpleague/factory-muffin","leapmotion/autowiring","edx/edx-platform","manastech/crystal","manastech/crystal","wayneeseguin/rvm","deadlyvipers/dojo_rules","Homebrew/homebrew","twbs/bootstrap","HabitRPG/habitrpg","clojure/clojurescript","nzakas/understandinges6","rust-lang/rust","w3c/banana-rdf","l3pp4rd/DoctrineExtensions","mono/monodevelop","SlimeKnights/TinkersConstruct","google/physical-web","selfhub/selfhub","elasticsearch/elasticsearch-analysis-kuromoji","HabitRPG/habitrpg","balderdashy/sails","twbs/bootstrap","paramiko/paramiko","rubinius/rubinius","rubinius/rubinius","libgit2/git2go","google/uribeacon","OvercastNetwork/Rotations","bitcoin/bitcoin","rethinkdb/rethinkdb","saltstack/salt","MovingBlocks/Terasology","d3athrow/vgstation13","projecthydra/sufia","Semantic-Org/Semantic-UI","projecthydra/sufia","blueboxgroup/ursula","projecthydra/sufia","hrydgard/ppsspp","projecthydra/sufia","leapmotion/autowiring","piwik/piwik","KerbalStuff/KerbalStuff","facebook/hhvm","saltstack/salt","snapframework/heist","exercism/xjavascript","emberjs/data","emberjs/data","jrburke/requirejs","emberjs/data","HabitRPG/habitrpg","ros/rosdistro","saltstack/salt","Unitech/PM2","blueboxgroup/ursula","HazyResearch/deepdive","projecthydra/sufia","twbs/bootstrap","projecthydra/sufia","rust-lang/rust","hrydgard/ppsspp","mjibson/goread","ftlabs/fastclick","Mudlet/Mudlet","numenta/nupic","rust-lang/rust","zwaldowski/BlocksKit","fxsjy/jieba","mixxxdj/mixxx","leapmotion/autowiring","saltstack/salt","cython/cython","blueboxgroup/ursula","mpv-player/mpv","cython/cython","Microsoft/dotnet","LolnetModPack/LolnetAdventurePack","numenta/nupic","tridge/cuav","robbyrussell/oh-my-zsh","tridge/cuav","laravel/cashier","start-jsk/rtmros_hironx","astropy/astropy","blueboxgroup/ursula","CocoaPods/Specs","NetEase/pomelo","Street-Meet/streetmeet","facebook/hhvm","johnmccutchan/game_loop","facebook/flux","iron/iron","selfhub/selfhub","selfhub/selfhub","midgetspy/Sick-Beard","KSP-CKAN/CKAN-GUI","commonsguy/cw-omnibus","BurntSushi/nflgame","gradle/gradle","mmistakes/so-simple-theme","mgcrea/angular-strap","blueboxgroup/ursula","aws/aws-sdk-ruby","stympy/faker","blueboxgroup/ursula","tridge/cuav","tridge/cuav","LearnBoost/mongoose","leapmotion/autowiring","requirejs/text","blueboxgroup/ursula","haskell/containers","ExactTarget/fuelux","tsuru/tsuru","ExactTarget/fuelux","PrinterLUA/FORGOTTENSERVER-ORTS","diydrones/ardupilot","tridge/cuav","tridge/cuav","xxv/android-lifecycle","KSP-CKAN/CKAN-GUI","sebastianbergmann/phpunit","hapijs/hapi","Zarel/Pokemon-Showdown","statsmodels/statsmodels","milkypostman/melpa","projecthydra/sufia","CleverRaven/Cataclysm-DDA","ziadoz/awesome-php","mozilla/MozStumbler","balanced/billy","saltstack/salt","JuliaLang/julia","rust-lang/rust","diaspora/diaspora","tridge/cuav","tridge/cuav","kennethreitz/python-guide","jashkenas/underscore","cloudfoundry/cf-release","svg/svgo","wakaleo/game-of-life","ExactTarget/fuelux","ExactTarget/fuelux","teeworlds/teeworlds","tsuru/tsuru","PCSX2/pcsx2","andrewlow/v8ppc","jenkinsci/puppet-jenkins","jclouds/jclouds","leapmotion/autowiring","vispy/vispy","rwaldron/johnny-five","square/SocketRocket","vispy/vispy","deis/deis","hrydgard/ppsspp","mozilla/MozStumbler","mozilla/MozStumbler","HabitRPG/habitrpg-shared","angular/angular.js","mozilla/MozStumbler","tsuru/tsuru","ExactTarget/fuelux","ExactTarget/fuelux","ExactTarget/fuelux","D-Programming-Language/phobos","blueboxgroup/ursula","felixge/node-ar-drone","resque/resque","saltstack/salt","saltstack/salt","nostra13/Android-Universal-Image-Loader","blueboxgroup/ursula","nostra13/Android-Universal-Image-Loader","HabitRPG/habitrpg-shared","d3athrow/vgstation13","Joomla-Bible-Study/Joomla-Bible-Study","BurntSushi/nflgame","iojs/io.js","tridge/cuav","tridge/cuav","ExactTarget/fuelux","tgstation/-tg-station","hashdist/hashdist","mishoo/UglifyJS","tgstation/-tg-station","HabitRPG/habitrpg","simbody/simbody","Famous/famous-angular","ExactTarget/fuelux","mopidy/mopidy-spotify","mopidy/mopidy-spotify","kalabox/kalabox","yeoman/generator-angular","TrinityCore/WowPacketParser","rust-lang/rust","blueboxgroup/ursula","saltstack/salt","coolwanglu/pdf2htmlEX","simbody/simbody","simbody/simbody","jlord/patchwork","romaonthego/RETableViewManager","hashdist/hashdist","mailgun/vulcand","mailgun/vulcand","ipython/ipython","smeighan/xLights","diydrones/ardupilot","rackt/react-router","atom/atom","omab/django-social-auth","CleverRaven/Cataclysm-DDA","Joomla-Bible-Study/Joomla-Bible-Study","jgm/pandoc","Joomla-Bible-Study/Joomla-Bible-Study","Guake/guake","mrkipling/maraschino","HabitRPG/habitrpg","ros/catkin","CleverRaven/Cataclysm-DDA","TechCavern/WaveTact","sethvargo/chefspec","Merchello/Merchello","JuliaLang/julia","simbody/simbody","Merchello/Merchello","saltstack/salt","Merchello/Merchello","sebastianbergmann/phpunit","sympy/sympy","spf13/hugo","sstephenson/ruby-build","minetest/minetest_game","tsuru/tsuru","piwik/piwik","piwik/piwik","minetest/minetest","piwik/piwik","ros/ros","mopidy/mopidy-spotify","neo4j/neo4j","docker/swarm","MinecraftForge/FML","kalabox/kalabox","codecombat/codecombat","d3athrow/vgstation13","wxWidgets/wxWidgets","Zarel/Pokemon-Showdown","Zarel/Pokemon-Showdown-Client","mozilla/gecko-dev","WayofTime/BloodMagic","Joomla-Bible-Study/Joomla-Bible-Study","openfl/lime","Homebrew/homebrew","Homebrew/homebrew","kalabox/kalabox","kalabox/kalabox","CleverRaven/Cataclysm-DDA","kalabox/kalabox","exercism/xjavascript","Unidata/thredds","google/traceur-compiler","manastech/crystal","burke/zeus","Codeception/Codeception","matplotlib/matplotlib","matplotlib/matplotlib","sonata-project/SonataMediaBundle","kalabox/kalabox","antirez/redis","RobotLocomotion/drake","overviewer/Minecraft-Overviewer","tgstation/-tg-station","tgstation/-tg-station","OpenROV/openrov-software","tgstation/-tg-station","goagent/goagent","cloudfoundry/cf-release","simbody/simbody","BurntSushi/nflgame","matplotlib/matplotlib","gever/bwx-adventure","numenta/nupic.core","kite-sdk/kite","kite-sdk/kite","tgstation/-tg-station","RuudBurger/CouchPotatoServer","kite-sdk/kite","nightscout/cgm-remote-monitor","arrayfire/arrayfire","clojure/clojurescript","EKGAPI/webAppEKGAPI","yiisoft/yii2","yiisoft/yii2","yiisoft/yii2","datawrapper/datawrapper","felixge/node-ar-drone","openmicroscopy/ome-documentation","cmangos/mangos-tbc","EKGAPI/webAppEKGAPI","start-jsk/rtmros_hironx","puppetlabs/puppet","wiremod/wire","tgstation/-tg-station","EKGAPI/webAppEKGAPI","rogerwang/node-webkit","cantino/huginn","rstudio/httpuv","rust-lang/rust","Gp2mv3/Syntheses","codecombat/codecombat","Homebrew/homebrew","hawtio/hawtio","tripit/slate","Homebrew/homebrew","PCSX2/pcsx2","clearsightstudio/ProMotion","mozilla/bedrock","CleverRaven/Cataclysm-DDA","rspec/rspec-rails","rspec/rspec-rails","parrot/parrot","Homebrew/homebrew","trentm/node-bunyan","rust-lang/rfcs","KSP-CKAN/NetKAN","Homebrew/homebrew","cocos2d/cocos2d-x","Zarel/Pokemon-Showdown","EKGAPI/webAppEKGAPI","MassiveCraft/Factions","simbody/simbody","hamamatsu-rb/hamamatsu-rb.github.com","hamamatsu-rb/hamamatsu-rb.github.com","MinecraftForge/FML","angular/material","rwaldron/johnny-five","Tribler/tribler","mozilla/bedrock","ocaml/ocaml.org","CleverRaven/Cataclysm-DDA","simbody/simbody","simbody/simbody","rspec/rspec-rails","thephpleague/factory-muffin","openwebwork/webwork2","Semantic-Org/Semantic-UI","github/hub","openwebwork/webwork2","s3tools/s3cmd","ZeroK-RTS/Zero-K","deadlyvipers/dojo_rules","aspnet/Home","emberjs/data","mozilla/gecko-dev","lodash/lodash","codecombat/codecombat","codecombat/codecombat","codecombat/codecombat","projecthydra/sufia","edx/edx-platform","s3tools/s3cmd","openfl/lime","mozilla/zamboni","mailgun/vulcand","statsmodels/statsmodels","rspec/rspec-rails","mozilla/bedrock","PCSX2/pcsx2","codecombat/codecombat","codecombat/codecombat","carhartl/jquery-cookie","EKGAPI/webAppEKGAPI","EKGAPI/webAppEKGAPI","facebook/react","square/PonyDebugger","lhorie/mithril.js","rtfd/readthedocs.org","MinecraftForge/FML","Gp2mv3/Syntheses","KerbalStuff/KerbalStuff","HaxeFoundation/hxcpp","tgstation/-tg-station","cytoscape/cytoscape-api","billie66/TLCL","kite-sdk/kite","kite-sdk/kite","cytoscape/cytoscape-impl","alibaba/dubbo","Elgg/Elgg","bitovi/canjs","mhacks/MHacks-Android","scikit-learn/scikit-learn","rspec/rspec-core","CleverRaven/Cataclysm-DDA") and  type='PushEvent' """).result().to_dataframe(bqstorage_client=bqstorageclient)

# print(query_job.loc[:,"repo"])
# print(pd.json_normalize(query_job.loc[:,"repo"]))
# print(query_job.head())
# df1 = pd.DataFrame(query_job['repo'].values.tolist())
# df1.columns = 'repo.'+ df1.columns
# print(df1.head())
#
# df3 = pd.DataFrame(query_job['actor'].values.tolist())
# df3.columns = 'actor.'+ df3.columns
# print(df3.head())
#
# df4 = pd.DataFrame(query_job['org'].values.tolist())
# df4.columns = 'org.'+ df4.columns
# print(df4.head())
#
# col = query_job.columns.difference(['repo','actor','org'])
# df = pd.concat([query_job[col], df1,df3,df4],axis=1)
# print (df)
query_job= query_job_subset.loc[1:50000]
print(query_job.count())
print(query_job['actor_id'].head())
dataset_forPartA=query_job
print(dataset_forPartA['actor_id'].head())
user_min_max_timedata_df=dataset_forPartA.groupby(["repo_id","actor_id"])["created_at"].agg(['min','max']).reset_index()

dataset_dict_fromCCfile=pushevnts_byech_contri_forreqrepos(query_job)
CC_dataset_fromCCfile=CC_forrequiredrepos(dataset_dict_fromCCfile)
print("**********CC_dataset_fromCCfile******",CC_dataset_fromCCfile.head())
print("********user_min_max_timedata_df*****",user_min_max_timedata_df.head())
# df2 = a1_grpbyrepoid.agg(Minimum_Date=('created_at', np.min), Maximum_Date=('created_at', np.max))
# print(df2.head())

result = user_min_max_timedata_df.merge(CC_dataset_fromCCfile, how='inner', left_on=['repo_id', 'actor_id'], right_on=['repo_id', 'actor_id'])
print(result.head())
user_min_max_timedata_df.to_csv(r'C:\\Users\\iialab\\OneDrive - UNT System\\Documents\\Git_analysis\\CitingRefRepos_PartA_Outputfiles\\min_max.csv',index=False)
result.to_csv(r'C:\\Users\\iialab\\OneDrive - UNT System\Documents\Git_analysis\\CitingRefRepos_PartA_Outputfiles\\combined_df.csv',index=False)
# file='C:\\Users\\iialab\\OneDrive - UNT System\\Documents\\Git_analysis\\'
edges_list = []
references_list=[]
final_ref_list=[]
from collections import defaultdict
PartA_refrepos = defaultdict(list)
list_count=0
a=result['repo_id'].unique()
print(type(a))
b=a.tolist()
print("$$$$$$$$$$$$",b)


def unique(list1):
    x = np.array(list1)
    print("unique values",np.unique(x))
    y=np.unique(x).tolist()
    return y


for id in b:
    # g = Graph('G', filename='simple_path1_'+str(id)+'.png', engine='fdp')
    required_df= result.loc[result['repo_id'] == id]
    print(required_df.head())
    for index, row in required_df.iterrows():
        print(row['repo_id'], row['actor_id'], row['min'], row['max'])
        print(client.project)
        print(""" select  distinct repo.id as ref_repo from """+"`"+"github-project-310921.githubproject_rawdataset.20*"""+"`"+""" where created_at > '""" +str(row['min'])+"' and created_at < '""" + str(row['max']) +"' and repo.id !="""+str(row['repo_id']) +" and actor.id="+ str(row['actor_id']) +" and type='PullRequestEvent'""")
        query_job_temp = bqclient.query("""
                  select  distinct repo.id as ref_repo from """+"`"+"github-project-310921.githubproject_rawdataset.20*"""+"`"+""" where created_at > '""" +str(row['min'])+"' and created_at < '""" + str(row['max']) +"' and repo.id !="""+str(row['repo_id']) +" and actor.id="+ str(row['actor_id']) +" and type='PullRequestEvent'""").result().to_dataframe(bqstorage_client=bqstorageclient)
        edge_list=query_job_temp.stack().tolist()
        print("edgelist",edge_list)
        references_list.append(edge_list)
        for edge in edge_list:
            print(len(edge_list),edge)
            # g.edge(str(id),str(edge))
    # g.view()
    print("ref list",references_list)
    List_flat = list(itertools.chain(*references_list))
    print("list flat",List_flat)
    unique_list_flat=unique(List_flat)
    print("repo list",unique_list_flat)
    if(len(unique_list_flat)!=0):
        uni_repos=unique_list_flat
        print("@@@@@@@@@@ after removing the extra braces@@@@@@@@@@@@",uni_repos)
        PartA_refrepos[id].append(uni_repos)
    final_ref_list.append([row['repo_id'],len(uni_repos)])
    print("#############final ref list ",final_ref_list)
    print("########PartA_refrepos######",PartA_refrepos)
    references_list.clear()
    list_count=0

print("PartA_refrepos in PartA file",PartA_refrepos)
ref_dataframe = DataFrame (final_ref_list,columns=['repo_id','count_of_repo_references'])

print(ref_dataframe.head())
ref_dataframe.to_csv(r'C:\\Users\\iialab\\OneDrive - UNT System\\Documents\\Git_analysis\\CitingRefRepos_PartA_Outputfiles\\repo_ref_df.csv',index=False)
filename_PartA_refrepos = "C:\\Users\\iialab\\OneDrive - UNT System\\Documents\\Git_analysis\\CitingRefRepos_PartA_Outputfiles\\PartA_refrepos.txt"
j=0
with open(filename_PartA_refrepos,'w') as output:
    writer = csv.writer(output)
    for k, v in PartA_refrepos.items():
        writer.writerow([k] + v)
        j=j+1


# table = 'githubproject_rawdataset.PartA_refrepos_count'
# job = client.load_table_from_dataframe(ref_dataframe, table)
#
# print ("{:<10} {:<10}".format('repo_id', 'count'))


ref_dataframe.to_gbq('githubproject_rawdataset.PartA_refrepos_count_test',
                      project_id,
                     chunksize=None,
                     if_exists='replace'
                     )
print("!!!!!!!!!!! created table PartA_refrepos_count_test done !!!!!")

partA_refrepos_list_org = []
for repoid in PartA_refrepos.keys():
    for value in PartA_refrepos[repoid]:
        print("###value",value,type(value))
        for each_value in value:
            partA_refrepos_list_org.append([repoid,each_value])
            print(partA_refrepos_list_org)

final_partA_refrepos_list_df_org = pd.DataFrame(partA_refrepos_list_org, columns =['main_repo_id', 'PartA_refrepos'])
print(final_partA_refrepos_list_df_org.head())


final_partA_refrepos_list_df_org.to_gbq('githubproject_rawdataset.PartA_ref_repos_final_test',
                     project_id,
                     chunksize=None,
                     if_exists='replace'
                     )
print("!!!!!!!!!!! created table PartA_ref_repos_final_test done !!!!!")

# The below code generates the dataframe with main repoid and ref repos as two diff columns note:should convert unique repo variable to str
partA_refrepos_list_temp = []
for repoid in PartA_refrepos.keys():
    for value in PartA_refrepos[repoid]:
        print("###value",value,type(value))
        # res=str(value)[1:-1]
        partA_refrepos_list_temp.append([repoid,value])
        print(partA_refrepos_list_temp)

final_partA_refrepos_list_df = pd.DataFrame(partA_refrepos_list_temp, columns =['main_repo_id', 'PartA_refrepos'])
print(final_partA_refrepos_list_df.head())

final_partA_refrepos_list_df.to_gbq('githubproject_rawdataset.PartA_ref_repos_temp_test1',
                                        project_id,
                                        chunksize=None,
                                        if_exists='replace'
                                        )

print("!!!!!!!!!!! created table PartA_ref_repos_temp_test done !!!!!")
# The below code is tocreate table with PartA_refrepos data
print("PartA_refrepos.items() are",PartA_refrepos.items())
# print each data item.
PartA_refrepos_df = pd.DataFrame()
PartA_refrepos_df['focal_repo']=PartA_refrepos.keys()
PartA_refrepos_df['reference_repos']=str(PartA_refrepos.values())

PartA_refrepos_df.to_gbq('githubproject_rawdataset.PartA_refrepos',
                     project_id,
                     chunksize=None,
                     if_exists='replace'
                     )

# for key, value in PartA_refrepos.items():
#     repo_id = key
#     count = str(value)
#     print ("{:<10} {:<10}".format(repo_id, count))
# for index, row in result.iterrows():
#     print(row['repo.id'], row['actor.id'], row['min'], row['max'])
#     print(client.project)
#     G.add_node(row['repo.id'])
#     print(""" select  distinct repo.id as ref_repoid from github-project-310921.githubproject_rawdataset.2011_data where created_at > '""" +str(row['min'])+"' and created_at < '""" + str(row['max']) +"' and repo.id !="""+str(row['repo.id']) +" and actor.id="+ str(row['actor.id']) +" and type='PullRequestEvent'""")
#     query_job = bqclient.query("""
#          select  distinct repo.id as ref_repoid from github-project-310921.githubproject_rawdataset.2011_data where created_at > '""" +str(row['min'])+"' and created_at < '""" + str(row['max']) +"' and repo.id !="""+str(row['repo.id']) +" and actor.id="+ str(row['actor.id']) +" and type='PullRequestEvent'""").result().to_dataframe(bqstorage_client=bqstorageclient)
#     edge_list=query_job.values.tolist()
#     print(len(edges_list))
#     references.append([row['repo.id'],len(edge_list)])
#     print(references)
#     G.add_edge(*edges_list)
#     print("nodes",G.nodes())
#     print("edges",G.edges())

