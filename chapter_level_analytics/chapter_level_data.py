#! /usr/bin/env python

import sys
import re 
import csv
import MySQLdb as mdb
import pandas as pd
from openpyxl import load_workbook
import time
import collections
from simple_salesforce import Salesforce
from dateutil.relativedelta import relativedelta

sys.path.insert(0,'/home/analytics/analytics_sandbox/python_libs');
from common_libs import *
from create_mysql import *
from attask_libs import * 
from attask_api import StreamClient, ObjCode, AtTaskObject
from sfdc_libs import *

cur_datetime = datetime.now()
execfile('/home/analytics/analytics_sandbox/python_libs/stuff.py')

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',400)

con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

UPDATE_ATTASK_LINEITEM = False 
GET_ACTIVITY = False

start = time.time()

def find_percentage(df,X,Y,name):

	value_per = []
	for i in range(0,len(df)):
		try:
			value_per.append(float(df.ix[i][X]) / float(df.ix[i][Y]) ) 
		except:
			value_per.append(None) 

	df = df.join(pd.DataFrame(value_per))
	df = df.rename(columns={0:name})
	return df

##############################################
##############################################
##############################################
############### Main Program #################
##############################################
##############################################
##############################################

###################################
# Begin grabbing g2 data
###################################

######################################
# Find current published videos in G2
######################################
query = "SELECT GROUP_CONCAT(distinct T.account_id) as account_id,GROUP_CONCAT(distinct T.video_version_id) as video_version_id, \
						GROUP_CONCAT(distinct T.trackable_id) as video_id,GROUP_CONCAT(distinct slide_id) as slide_id, \
						MIN(T.slide_id) as slide_id,parent_id,session_id,T.user_id,COUNT(distinct T.parent_id) as Nparent, \
						COUNT(T.parent_id) as Nrecord,COUNT(distinct T.trackable_id) as Nvideo,COUNT(distinct T.slide_id) as Nslide, \
						MIN(T.client_created_at) as min_time,MAX(T.client_created_at) as max_time, \
						TIMESTAMPDIFF(SECOND,MIN(T.client_created_at),MAX(T.client_created_at)) as viewing_time  \
					FROM  \
						guidespark2_prod.activities T \
					WHERE  \
						T.type IN ('PlaybackActivity') AND T.is_preview IN ('active') AND T.account_id IS NOT NULL \
						AND T.account_id=216 AND T.parent_id= 8255 \
						AND T.client_created_at IS NOT NULL AND T.parent_id IS NOT NULL GROUP BY T.parent_id,T.slide_id"

#query = "SELECT account_id,parent_id,trackable_id,video_version_id,trackable_type,created_at,type,route,client_action,old_value,new_value,slide_id,absolute_time,relative_time \
#					FROM guidespark2_prod.activities \
#					WHERE type='PlaybackActivity' AND is_preview='active' AND client_action NOT IN ('_PLAYBACK_ENDED','_VIDEO_BUFFERING','VIDEO_BUFFERING_DONE') AND parent_id < 15000 \
#					ORDER BY parent_id,created_at" 
query = "SELECT parent_id,created_at,client_action,old_value,new_value,slide_id,absolute_time,relative_time \
					FROM guidespark2_prod.activities \
					WHERE type='PlaybackActivity' AND is_preview='active' AND client_action NOT IN ('_PLAYBACK_ENDED','_VIDEO_BUFFERING','_VIDEO_BUFFERING_DONE') AND parent_id < 2000000 \
					ORDER BY parent_id,created_at" 
g2_chapter_info_df = pd.read_sql(query,con)
g2_chapter_info_ORIG_df = g2_chapter_info_df.copy(deep=True)
printf("[chapter_level_analytics.py]: G2 ... Query Time ... %.2f secs\n",time.time() - start) 

#unique_chapter = list(set(g2_chapter_info_df.slide_id))
#for x in g2_chapter_info_df[pd.isnull(g2_chapter_info_df['old_value']) == True].index:
#	g2_chapter_info_df.loc[x,'old_value'] = g2_chapter_info_df.ix[x]['slide_id']
#	g2_chapter_info_df.loc[x,'new_value'] = g2_chapter_info_df.ix[x]['slide_id']

cols = g2_chapter_info_df.columns
chapter_update_df = pd.DataFrame(index=np.arange(100000),columns=cols)
Nloc = 0

unique_parent = sorted(list(set(g2_chapter_info_df['parent_id'])))
printf("[chapter_level_analytics.py]: Unique Parent Ids ... %4s\n",len(unique_parent)) 

parent_id = []
client_action = []
old_value = []
new_value = []
new_value = []
slide_id = []
absolute_time = []
relative_time = []
created_at = []
for ppp in range(0,len(unique_parent)):

	printf("[chapter_level_analytics.py]: PARENT ID . %8s of %8s . %10s",ppp+1,len(unique_parent),unique_parent[ppp]) 

	cur_idx = all_indices(unique_parent[ppp],g2_chapter_info_df['parent_id'])
	cur_length = len(cur_idx)-1
	for i in range(0,cur_length):

		idx = cur_idx[i]
		if (g2_chapter_info_df.ix[idx]['client_action'] == '_AUTO_NEXT_SLIDE'):
			if (g2_chapter_info_df.ix[idx]['absolute_time'] == 0):
				g2_chapter_info_df.loc[idx,'absolute_time'] = g2_chapter_info_df.ix[idx-1]['absolute_time'] + g2_chapter_info_df.ix[idx]['relative_time'] - g2_chapter_info_df.ix[idx-1]['relative_time'] 
			else:
				g2_chapter_info_df.loc[idx,'relative_time'] = 0 

	for i in range(0,cur_length):

		idx = cur_idx[i]
	
		cur_parent_id = g2_chapter_info_df.ix[idx]['parent_id']
		cur_client_action = 'N' ##g2_chapter_info_df.ix[idx]['client_action']
		cur_old_value = g2_chapter_info_df.ix[idx]['old_value']
		cur_new_value = g2_chapter_info_df.ix[idx]['new_value']
		cur_slide = g2_chapter_info_df.ix[idx]['slide_id']
		cur_created_at = g2_chapter_info_df.ix[idx]['created_at'] 	

		time_diff = int(g2_chapter_info_df.ix[idx+1]['absolute_time'] - g2_chapter_info_df.ix[idx]['absolute_time'] - 1)
		absolute_time_base = g2_chapter_info_df.ix[idx]['absolute_time']	
		relative_time_base = g2_chapter_info_df.ix[idx]['relative_time']	
		for j in range(0,time_diff):   
			parent_id.append(cur_parent_id)
			client_action.append('N') ##g2_chapter_info_df.ix[idx]['client_action']
			old_value.append(cur_old_value)
			new_value.append(cur_new_value)
			slide_id.append(cur_slide)
			created_at.append(cur_created_at + timedelta(seconds = (j+1))) 	
			absolute_time.append(absolute_time_base + (j+1)) 	
			relative_time.append(relative_time_base + (j+1)) 	

		
	printf(" . Nrecords = %4s . %.2f secs\n",len(cur_idx),time.time() - start) 

g2_chapter_update_df = pd.merge(pd.DataFrame(parent_id).rename(columns={0:'parent_id'}),pd.DataFrame(created_at).rename(columns={0:'created_at'}),'left',left_index=True,right_index=True) 
g2_chapter_update_df = pd.merge(g2_chapter_update_df,pd.DataFrame(client_action).rename(columns={0:'client_action'}),'left',left_index=True,right_index=True) 
g2_chapter_update_df = pd.merge(g2_chapter_update_df,pd.DataFrame(old_value).rename(columns={0:'old_value'}),'left',left_index=True,right_index=True) 
g2_chapter_update_df = pd.merge(g2_chapter_update_df,pd.DataFrame(new_value).rename(columns={0:'new_value'}),'left',left_index=True,right_index=True) 
g2_chapter_update_df = pd.merge(g2_chapter_update_df,pd.DataFrame(slide_id).rename(columns={0:'slide_id'}),'left',left_index=True,right_index=True) 
g2_chapter_update_df = pd.merge(g2_chapter_update_df,pd.DataFrame(absolute_time).rename(columns={0:'absolute_time'}),'left',left_index=True,right_index=True) 
g2_chapter_update_df = pd.merge(g2_chapter_update_df,pd.DataFrame(relative_time).rename(columns={0:'relative_time'}),'left',left_index=True,right_index=True) 
g2_chapter_info_df = g2_chapter_info_df.append(g2_chapter_update_df, ignore_index=True)		

g2_chapter_info_df = g2_chapter_info_df.sort(['parent_id', 'created_at'], ascending=[1, 1]).reset_index(drop=True)		
g2_chapter_info_df.to_csv('test.csv')

printf("[chapter_level_analytics.py]: G2 ... Query Time ... %.2f secs\n",time.time() - start) 

printf("\n[chapter_level_analytics.py]: Total Time to Complete: %6.2f days",float(time.time()-start) / float(len(g2_chapter_info_ORIG_df)) * float(18) * float(3500000) / float(86400)) 

#####################################################
# Calculate static library completion
# 1) Use g2_videos_df
# 2) Groupby sfdc
# 3) Get video counts
#####################################################
#g2_videos_NOTNULL_df = g2_videos_df[(g2_videos_df.sfdc != 'test') & (g2_videos_df.sfdc != '') & (pd.notnull(g2_videos_df.sfdc))].copy(deep=True).reset_index()
#
#g2_act_video_count_df = g2_videos_NOTNULL_df[['sfdc','account_id','video_id']].groupby(['sfdc']) \
#                        .agg({"account_id":pd.Series.nunique,"video_id":pd.Series.nunique,"sfdc": "count"  }) \
#                        .rename(columns = {'account_id':'g2_Nmicro_cur','video_id':'g2_Nvideo_cur_distinct','sfdc':'g2_Nvideo_cur'}).reset_index()
#
#g2_act_video_count_df['sfdc_18'] = [x for x in g2_act_video_count_df.sfdc] ### Change AccountId to 15 characters
#g2_act_video_count_df['sfdc'] = [x[0:15] for x in g2_act_video_count_df.sfdc] ### Change AccountId to 15 characters
#
##g2_act_micro_df = g2_videos_NOTNULL_df.groupby(['sfdc']) \
##					.apply(lambda x: list(set(map(str,map(int,x.account_id) ))) ).reset_index().rename(columns={0:'g2_micro_list'})  
##g2_act_micro_df['sfdc_18'] = [x for x in g2_act_micro_df.sfdc] ### Change AccountId to 15 characters
##g2_act_micro_df['sfdc'] = [x[0:15] for x in g2_act_micro_df.sfdc] ### Change AccountId to 15 characters
#
#g2_act_video_count_df = pd.merge(account_df[['Id']],g2_act_video_count_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1).drop('sfdc_18',1).fillna(0)
#account_df = pd.merge(account_df,g2_act_video_count_df,'left',left_on='Id',right_on='Id')
#account_df['g2_library_completion_%_cur'] = [x for x in (account_df['g2_Nvideo_cur'] / account_df['Nvideo_cur']) ]
##account_df = pd.merge(account_df,g2_act_micro_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1).drop('sfdc_18',1)
#
#printf("[update_churn_model_data.py]: Salesforce ... No of Accounts: %s\n",len(account_df))
#
######################################################
##
## Calculate library completion from g2
##
## 1) Find all views in year 1 of each subscription
## 2) Find unique videos within those views
## 3) Unique Videos = Nvideo complete in year 1
######################################################
#
#printf("[update_churn_model_data.py]: G2 ... Merge Stats ... ")
####### All Time G2 stats ######
#g2_act_complete_stats_df = g2_output_df[['sfdc','account_id','trackable_id','user_id','parent_id']].groupby(['sfdc']) \
#						.agg({"account_id":pd.Series.nunique,"trackable_id":pd.Series.nunique,"user_id":pd.Series.nunique,"parent_id": "count"  }) \
#						.rename(columns = {'account_id':'g2_Nmicro_Total','trackable_id':'g2_Nvideo_Total','user_id':'g2_Nuser_Total','parent_id':'g2_Nview_Total'}).reset_index()
#
## Add all accounts
#g2_act_complete_stats_df = pd.merge(g2_act_complete_stats_df,g2_accounts_df[['sfdc']],'right',left_on='sfdc',right_on='sfdc').fillna(0)
#
#g2_act_1mo_stats_df = calc_day_details_G2(g2_output_df,31,'1mo',['min_time'],True,g2_act_complete_stats_df)
#g2_act_2mo_stats_df = calc_day_details_G2(g2_output_df,61,'2mo',['min_time'],True,g2_act_complete_stats_df)
#g2_act_3mo_stats_df = calc_day_details_G2(g2_output_df,91,'3mo',['min_time'],True,g2_act_complete_stats_df)
#g2_act_4mo_stats_df = calc_day_details_G2(g2_output_df,122,'4mo',['min_time'],True,g2_act_complete_stats_df)
#g2_act_5mo_stats_df = calc_day_details_G2(g2_output_df,152,'5mo',['min_time'],True,g2_act_complete_stats_df)
#g2_act_6mo_stats_df = calc_day_details_G2(g2_output_df,182,'6mo',['min_time'],True,g2_act_complete_stats_df)
#g2_act_7mo_stats_df = calc_day_details_G2(g2_output_df,212,'7mo',['min_time'],True,g2_act_complete_stats_df)
#g2_act_8mo_stats_df = calc_day_details_G2(g2_output_df,243,'8mo',['min_time'],True,g2_act_complete_stats_df)
#g2_act_9mo_stats_df = calc_day_details_G2(g2_output_df,273,'9mo',['min_time'],True,g2_act_complete_stats_df)
#g2_act_10mo_stats_df = calc_day_details_G2(g2_output_df,304,'10mo',['min_time'],True,g2_act_complete_stats_df)
#g2_act_11mo_stats_df = calc_day_details_G2(g2_output_df,334,'11mo',['min_time'],True,g2_act_complete_stats_df)
#g2_act_12mo_stats_df = calc_day_details_G2(g2_output_df,365,'12mo',['min_time'],True,g2_act_complete_stats_df)
#g2_act_15mo_stats_df = calc_day_details_G2(g2_output_df,456,'15mo',['min_time'],True,g2_act_complete_stats_df)
#g2_act_18mo_stats_df = calc_day_details_G2(g2_output_df,547,'18mo',['min_time'],True,g2_act_complete_stats_df)
#g2_act_21mo_stats_df = calc_day_details_G2(g2_output_df,638,'21mo',['min_time'],True,g2_act_complete_stats_df)
#
###################
## Combine Results
###################
#account_df = pd.merge(account_df,g2_act_1mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_act_2mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_act_3mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_act_4mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_act_5mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_act_6mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_act_7mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_act_8mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_act_9mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_act_10mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_act_11mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_act_12mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_act_15mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_act_18mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_act_21mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_act_complete_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df['Nvideo_g2_SFDC_12mo_diff'] = [x for x in (account_df['Nvideo_12mo'] - account_df['g2_Nvideo_12mo']) ]
#account_df['Nvideo_g2_SFDC_12mo_%'] = [x for x in (account_df['g2_Nvideo_12mo'] / account_df['Nvideo_12mo']) ]
#
#printf("%.2f sec\n",time.time() - start)
#
###################################
## Add Admin Usage since 1/1/2014
###################################
#query = 'SELECT LEFT(A.sfdc,15) as sfdc,COUNT(distinct A.id) as g2_admin_micro_Total, \
#		COUNT(distinct B.user_id) as g2_admin_user_Total,COUNT(distinct B.action) as g2_admin_action_Total, \
#		COUNT(B.user_id) as g2_admin_report_Total \
#		FROM guidespark2_prod.accounts A \
#		LEFT JOIN sandbox_prod.TMP_ADMIN_ACCESS B ON A.id=B.account_id \
#		GROUP BY LEFT(A.sfdc,15)'
#g2_admin_usage_df = pd.read_sql(query,con)
#
#query = 'SELECT * FROM sandbox_prod.TMP_ADMIN_ACCESS'
#g2_admin_usage_all_df = pd.read_sql(query,con)
#g2_admin_all_df = pd.merge(g2_admin_usage_all_df,account_df[['Id','MSA_Effective_Date']],'left',left_on='sfdc',right_on='Id').drop('Id',1)
#
#g2_admin_1mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,31,'1mo',['created_at'],True,g2_act_complete_stats_df)
#g2_admin_2mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,61,'2mo',['created_at'],True,g2_act_complete_stats_df)
#g2_admin_3mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,91,'3mo',['created_at'],True,g2_act_complete_stats_df)
#g2_admin_4mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,122,'4mo',['created_at'],True,g2_act_complete_stats_df)
#g2_admin_5mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,152,'5mo',['created_at'],True,g2_act_complete_stats_df)
#g2_admin_6mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,182,'6mo',['created_at'],True,g2_act_complete_stats_df)
#g2_admin_7mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,212,'7mo',['created_at'],True,g2_act_complete_stats_df)
#g2_admin_8mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,243,'8mo',['created_at'],True,g2_act_complete_stats_df)
#g2_admin_9mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,273,'9mo',['created_at'],True,g2_act_complete_stats_df)
#g2_admin_10mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,304,'10mo',['created_at'],True,g2_act_complete_stats_df)
#g2_admin_11mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,334,'11mo',['created_at'],True,g2_act_complete_stats_df)
#g2_admin_12mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,365,'12mo',['created_at'],True,g2_act_complete_stats_df)
#g2_admin_15mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,456,'15mo',['created_at'],True,g2_act_complete_stats_df)
#g2_admin_18mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,547,'18mo',['created_at'],True,g2_act_complete_stats_df)
#g2_admin_21mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,638,'21mo',['created_at'],True,g2_act_complete_stats_df)
#
###################
## Combine Results
###################
#account_df = pd.merge(account_df,g2_admin_1mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_admin_2mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_admin_3mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_admin_4mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_admin_5mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_admin_6mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_admin_7mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_admin_8mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_admin_9mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_admin_10mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_admin_11mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_admin_12mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_admin_15mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_admin_18mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_admin_21mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_admin_usage_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#
###################################
## Edits info since 7/1/2014
###################################
#query = "SELECT * FROM edits_prod.TMP_VD1_EDITS" 
#g2_vd1_edits_df = pd.read_sql(query,con)
#g2_vd1_edits_df = pd.merge(g2_vd1_edits_df,account_df[['Id','MSA_Effective_Date']],'left',left_on='sfdc',right_on='Id').drop('Id',1)
#
#query = "SELECT * FROM edits_prod.TMP_ALL_CUSTOMER_TOUCH_EDITS" 
#g2_all_edits_df = pd.read_sql(query,con)
#g2_all_edits_df = pd.merge(g2_all_edits_df,account_df[['Id','MSA_Effective_Date']],'left',left_on='sfdc',right_on='Id').drop('Id',1)
#
####### Complete Stats #######
#g2_vd1_edit_complete_df = g2_vd1_edits_df[['sfdc','account_id','video_id','video_version_id','Ncnt_preview','Ncnt_qc']].groupby(['sfdc']) \
#						.agg({"account_id":pd.Series.nunique,"video_id":pd.Series.nunique,"video_version_id":pd.Series.nunique, \
#						"Ncnt_preview":"mean","Ncnt_qc": "mean"  }) \
#						.rename(columns = {'account_id':'g2_act_vd1','video_id':'g2_Nvideo_vd1','video_version_id':'g2_Nversion_vd1', \
#											'Ncnt_preview':'g2_avg_edits_preview_vd1','Ncnt_qc':'g2_avg_edits_qc_vd1'}).reset_index()
#
#g2_all_edit_complete_df = g2_all_edits_df[['sfdc','account_id','video_id','video_version_id','Ncnt_preview','Ncnt_qc']].groupby(['sfdc']) \
#						.agg({"account_id":pd.Series.nunique,"video_id":pd.Series.nunique,"video_version_id":pd.Series.nunique, \
#						"Ncnt_preview":"mean","Ncnt_qc": "mean"  }) \
#						.rename(columns = {'account_id':'g2_act_all','video_id':'g2_Nvideo_all','video_version_id':'g2_Nversion_all', \
#											'Ncnt_preview':'g2_avg_edits_preview_all','Ncnt_qc':'g2_avg_edits_qc_all'}).reset_index()
#
#g2_vd1_edit_1mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,31,'1mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
#g2_vd1_edit_2mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,61,'2mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
#g2_vd1_edit_3mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,91,'3mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
#g2_vd1_edit_4mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,122,'4mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
#g2_vd1_edit_5mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,152,'5mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
#g2_vd1_edit_6mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,182,'6mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
#g2_vd1_edit_7mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,212,'7mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
#g2_vd1_edit_8mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,243,'8mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
#g2_vd1_edit_9mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,273,'9mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
#g2_vd1_edit_10mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,304,'10mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
#g2_vd1_edit_11mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,334,'11mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
#g2_vd1_edit_12mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,365,'12mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
#g2_vd1_edit_15mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,456,'15mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
#g2_vd1_edit_18mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,547,'18mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
#g2_vd1_edit_21mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,638,'21mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
#
#g2_all_edit_1mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,31,'1mo_all',['video_created_at'],True,g2_all_edit_complete_df)
#g2_all_edit_2mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,61,'2mo_all',['video_created_at'],True,g2_all_edit_complete_df)
#g2_all_edit_3mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,91,'3mo_all',['video_created_at'],True,g2_all_edit_complete_df)
#g2_all_edit_4mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,122,'4mo_all',['video_created_at'],True,g2_all_edit_complete_df)
#g2_all_edit_5mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,152,'5mo_all',['video_created_at'],True,g2_all_edit_complete_df)
#g2_all_edit_6mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,182,'6mo_all',['video_created_at'],True,g2_all_edit_complete_df)
#g2_all_edit_7mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,212,'7mo_all',['video_created_at'],True,g2_all_edit_complete_df)
#g2_all_edit_8mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,243,'8mo_all',['video_created_at'],True,g2_all_edit_complete_df)
#g2_all_edit_9mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,273,'9mo_all',['video_created_at'],True,g2_all_edit_complete_df)
#g2_all_edit_10mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,304,'10mo_all',['video_created_at'],True,g2_all_edit_complete_df)
#g2_all_edit_11mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,334,'11mo_all',['video_created_at'],True,g2_all_edit_complete_df)
#g2_all_edit_12mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,365,'12mo_all',['video_created_at'],True,g2_all_edit_complete_df)
#g2_all_edit_15mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,456,'15mo_all',['video_created_at'],True,g2_all_edit_complete_df)
#g2_all_edit_18mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,547,'18mo_all',['video_created_at'],True,g2_all_edit_complete_df)
#g2_all_edit_21mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,638,'21mo_all',['video_created_at'],True,g2_all_edit_complete_df)
#
###################
## Combine Results
###################
#account_df = pd.merge(account_df,g2_vd1_edit_complete_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_1mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_2mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_3mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_4mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_5mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_6mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_7mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_8mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_9mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_10mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_11mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_12mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_15mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_18mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_21mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_complete_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_1mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_2mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_3mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_4mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_5mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_6mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_7mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_8mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_9mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_10mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_11mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_12mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_15mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_18mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_21mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#
#
################################################
## Edits 1st video only ... info since 7/1/2014
################################################
#query = "SELECT * FROM edits_prod.TMP_VD1_EDITS_1stVideo" 
#g2_vd1_edits_1st_df = pd.read_sql(query,con)
#g2_vd1_edits_1st_df = pd.merge(g2_vd1_edits_1st_df,account_df[['Id','MSA_Effective_Date']],'left',left_on='sfdc',right_on='Id').drop('Id',1)
#
#query = "SELECT * FROM edits_prod.TMP_ALL_CUSTOMER_TOUCH_EDITS_1stVideo" 
#g2_all_edits_1st_df = pd.read_sql(query,con)
#g2_all_edits_1st_df = pd.merge(g2_all_edits_1st_df,account_df[['Id','MSA_Effective_Date']],'left',left_on='sfdc',right_on='Id').drop('Id',1)
#
####### Complete Stats #######
#g2_vd1_edit_1st_complete_df = g2_vd1_edits_1st_df[['sfdc','account_id','video_id','video_version_id','Ncnt_preview','Ncnt_qc']].groupby(['sfdc']) \
#						.agg({"account_id":pd.Series.nunique,"video_id":pd.Series.nunique,"video_version_id":pd.Series.nunique, \
#						"Ncnt_preview":"mean","Ncnt_qc": "mean"  }) \
#						.rename(columns = {'account_id':'g2_act_1st_vd1','video_id':'g2_Nvideo_1st_vd1','video_version_id':'g2_Nversion_1st_vd1', \
#											'Ncnt_preview':'g2_avg_edits_preview_1st_vd1','Ncnt_qc':'g2_avg_edits_qc_1st_vd1'}).reset_index()
#
#g2_all_edit_1st_complete_df = g2_all_edits_1st_df[['sfdc','account_id','video_id','video_version_id','Ncnt_preview','Ncnt_qc']].groupby(['sfdc']) \
#						.agg({"account_id":pd.Series.nunique,"video_id":pd.Series.nunique,"video_version_id":pd.Series.nunique, \
#						"Ncnt_preview":"mean","Ncnt_qc": "mean"  }) \
#						.rename(columns = {'account_id':'g2_act_1st_all','video_id':'g2_Nvideo_1st_all','video_version_id':'g2_Nversion_1st_all', \
#											'Ncnt_preview':'g2_avg_edits_preview_1st_all','Ncnt_qc':'g2_avg_edits_qc_1st_all'}).reset_index()
#
##g2_vd1_edit_1st_1mo_stats_df  = calc_day_details_G2edits(g2_vd1_edits_1st_df,31,'1st_1mo_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
##g2_vd1_edit_1st_3mo_stats_df  = calc_day_details_G2edits(g2_vd1_edits_1st_df,91,'1st_3mo_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
##g2_vd1_edit_1st_6mo_stats_df  = calc_day_details_G2edits(g2_vd1_edits_1st_df,182,'1st_6mo_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
##g2_vd1_edit_1st_9mo_stats_df  = calc_day_details_G2edits(g2_vd1_edits_1st_df,273,'1st_9mo_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
##g2_vd1_edit_1st_12mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,365,'1st_12mo_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
##g2_all_edit_1st_1mo_stats_df  = calc_day_details_G2edits(g2_all_edits_1st_df,31,'1st_1mo_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
##g2_all_edit_1st_3mo_stats_df  = calc_day_details_G2edits(g2_all_edits_1st_df,91,'1st_3mo_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
##g2_all_edit_1st_6mo_stats_df  = calc_day_details_G2edits(g2_all_edits_1st_df,182,'1st_6mo_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
##g2_all_edit_1st_9mo_stats_df  = calc_day_details_G2edits(g2_all_edits_1st_df,273,'1st_9mo_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
##g2_all_edit_1st_12mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,365,'1st_12mo_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
#
#g2_vd1_edit_1st_1mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,31,'1mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
#g2_vd1_edit_1st_2mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,61,'2mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
#g2_vd1_edit_1st_3mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,91,'3mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
#g2_vd1_edit_1st_4mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,122,'4mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
#g2_vd1_edit_1st_5mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,152,'5mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
#g2_vd1_edit_1st_6mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,182,'6mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
#g2_vd1_edit_1st_7mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,212,'7mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
#g2_vd1_edit_1st_8mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,243,'8mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
#g2_vd1_edit_1st_9mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,273,'9mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
#g2_vd1_edit_1st_10mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,304,'10mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
#g2_vd1_edit_1st_11mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,334,'11mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
#g2_vd1_edit_1st_12mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,365,'12mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
#g2_vd1_edit_1st_15mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,456,'15mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
#g2_vd1_edit_1st_18mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,547,'18mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
#g2_vd1_edit_1st_21mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,638,'21mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
#
#g2_all_edit_1st_1mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,31,'1mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
#g2_all_edit_1st_2mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,61,'2mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
#g2_all_edit_1st_3mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,91,'3mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
#g2_all_edit_1st_4mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,122,'4mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
#g2_all_edit_1st_5mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,152,'5mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
#g2_all_edit_1st_6mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,182,'6mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
#g2_all_edit_1st_7mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,212,'7mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
#g2_all_edit_1st_8mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,243,'8mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
#g2_all_edit_1st_9mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,273,'9mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
#g2_all_edit_1st_10mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,304,'10mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
#g2_all_edit_1st_11mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,334,'11mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
#g2_all_edit_1st_12mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,365,'12mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
#g2_all_edit_1st_15mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,456,'15mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
#g2_all_edit_1st_18mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,547,'18mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
#g2_all_edit_1st_21mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,638,'21mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
#
##g2_vd1_edit_1st_1mo_stats_df_NEW.to_csv('g2_1moN_vd1.csv')
##g2_vd1_edit_1st_3mo_stats_df_NEW.to_csv('g2_3moN_vd1.csv')
##g2_vd1_edit_1st_6mo_stats_df_NEW.to_csv('g2_6moN_vd1.csv')
##g2_vd1_edit_1st_9mo_stats_df_NEW.to_csv('g2_9moN_vd1.csv')
##g2_vd1_edit_1st_12mo_stats_df_NEW.to_csv('g2_12moN_vd1.csv')
##g2_all_edit_1st_1mo_stats_df_NEW.to_csv('g2_1moN_all.csv')
##g2_all_edit_1st_3mo_stats_df_NEW.to_csv('g2_3moN_all.csv')
##g2_all_edit_1st_6mo_stats_df_NEW.to_csv('g2_6moN_all.csv')
##g2_all_edit_1st_9mo_stats_df_NEW.to_csv('g2_9moN_all.csv')
##g2_all_edit_1st_12mo_stats_df_NEW.to_csv('g2_12moN_all.csv')
#
###################
## Combine Results
###################
#account_df = pd.merge(account_df,g2_vd1_edit_1st_complete_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_1st_1mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_1st_2mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_1st_3mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_1st_4mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_1st_5mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_1st_6mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_1st_7mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_1st_8mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_1st_9mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_1st_10mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_1st_11mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_1st_12mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_1st_15mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_1st_18mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_vd1_edit_1st_21mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_1st_complete_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_1st_1mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_1st_2mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_1st_3mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_1st_4mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_1st_5mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_1st_6mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_1st_7mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_1st_8mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_1st_9mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_1st_10mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_1st_11mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_1st_12mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_1st_15mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_1st_18mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#account_df = pd.merge(account_df,g2_all_edit_1st_21mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
#
#account_df.to_csv('./output/account_df.csv',encoding='utf-8',columns=header)
#
#
#tmp_header = [
#'Id',
#'AccountId_18',
#'Name',
#'MSA_Effective_Date',
#'churn_int',
#'Total_Nmbr_Videos__c',
#'Nopportunity_All',
#'Nopportunity_Total',
#'Nvideo_Total',
#'Nopportunity_cur',
#'NlineItem_cur',
#'NlineItem_Initial',
#'Nvideo_cur',
#'g2_Nvideo_cur',
#'g2_library_completion_%_cur']
#library_completion_df = account_df[tmp_header].copy(deep=True)
#library_completion_df = library_completion_df.rename(columns={'Nopportunity_cur':'sfdc_Nopportunity_cur','NlineItem_Initial':'sfdc_NlineItem_Initial','NlineItem_cur':'sfdc_NlineItem_cur','Nvideo_cur':'sfdc_Nvideo_cur' })
#library_completion_df = library_completion_df.rename(columns={'Nopportunity_All':'sfdc_Nopportunity_All'})
#library_completion_df = library_completion_df.rename(columns={'Nopportunity_Total':'sfdc_Nopportunity_NewUpsell','Nvideo_Total':'sfdc_Nvideo_NewUpsell'})
#library_completion_df = library_completion_df.rename(columns={'Total_Nmbr_Videos__c':'Cambria_sfdc_Nvideo'})
#
#library_completion_header = [
#'AccountId_18',
#'Name',
#'MSA_Effective_Date',
#'churn_int',
#'sfdc_NlineItem_Initial',
#'sfdc_Nopportunity_All',
#'sfdc_Nopportunity_NewUpsell',
#'sfdc_Nopportunity_cur',
#'Cambria_sfdc_Nvideo', 
#'sfdc_Nvideo_NewUpsell', 
#'sfdc_Nvideo_cur', 
#'g2_Nvideo_cur',
#'g2_library_completion_%_cur']
#library_completion_df.to_csv('./output/library_completion_' + cur_datetime.strftime('%Y%m%d') +'.csv',encoding='utf-8',columns=library_completion_header)
#
