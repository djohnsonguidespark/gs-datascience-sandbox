#! /usr/bin/env python

import sys
import re 
import csv
from decimal import Decimal
import MySQLdb as mdb
import pandas as pd
from scipy import stats 
from openpyxl import load_workbook
import time
import collections
from matplotlib.pyplot import *
import itertools

from benchmark_common_libs import *
from benchmark_create_mysql import *
from aer_libs import *

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',50)

cur_datetime = datetime.now()
cur_date = datetime.now().date()

DBNAME = "benchmark_prod"
REACH_TABLE = "TMP_REACH_ALL"
EFF_TABLE = "TMP_EFFECTIVENESS_ALL"
ACCOUNT_REACH_TABLE = "AER_REACH_SUMMARY_DATEINTERVAL_account"
ACCOUNT_VIDEO_REACH_TABLE = "AER_REACH_SUMMARY_VIDEO_DATEINTERVAL_account"
ACCOUNT_REACH_TABLE_OUT = "AER_REACH_SUMMARY_DATEINTERVAL_account_benchmark"
ACCOUNT_VIDEO_REACH_TABLE_OUT = "AER_REACH_SUMMARY_VIDEO_DATEINTERVAL_account_benchmark"
ACCOUNT_EFF_TABLE_OUT = "AER_EFFECTIVENESS_SUMMARY_account_benchmark"
ACCOUNT_VIDEO_EFF_TABLE_OUT = "AER_EFFECTIVENESS_SUMMARY_VIDEO_account_benchmark"
#ACCOUNT_REACH_TABLE_OUT = "AER_REACH_SUMMARY_DATEINTERVAL_account_benchmark_NEW"
#ACCOUNT_VIDEO_REACH_TABLE_OUT = "AER_REACH_SUMMARY_VIDEO_DATEINTERVAL_account_benchmark_NEW"
#ACCOUNT_EFF_TABLE_OUT = "AER_EFFECTIVENESS_SUMMARY_account_benchmark_NEW"
#ACCOUNT_VIDEO_EFF_TABLE_OUT = "AER_EFFECTIVENESS_SUMMARY_VIDEO_account_benchmark_NEW"

####################
# Read in all data
####################
con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

printf("[percentile.py] Query DBs\n")
query_eff = "SELECT parent_id,account_id,account_name,BEEs,target_audience,video_title,video_category,industry_name,min_time, \
					video_id,effectiveness_absolute,global_slide_view_percentage,max_absolute_time,max_absolute_time_percentage FROM %s.%s" % (DBNAME,EFF_TABLE)
query_timeframe = "SELECT distinct cur_year from %s.%s WHERE cur_year = '1-Year' AND BEEs IS NOT NULL" % (DBNAME,ACCOUNT_REACH_TABLE)
query_account_reach = "SELECT * from %s.%s WHERE cur_year = '1-Year' AND BEEs IS NOT NULL" % (DBNAME,ACCOUNT_REACH_TABLE)
query_account_video_reach = "SELECT * from %s.%s WHERE cur_year = '1-Year' AND BEEs IS NOT NULL" % (DBNAME,ACCOUNT_VIDEO_REACH_TABLE)

start = time.time()

######################################
# Grab the unique timeframes 
######################################
gs_timeframe_df = pd.read_sql(query_timeframe,con)

######################################
# Compute effectiveness median
######################################
eff_df = pd.read_sql(query_eff,con)
eff_df[['effectiveness_absolute','global_slide_view_percentage','max_absolute_time','max_absolute_time_percentage']] = eff_df[['effectiveness_absolute','global_slide_view_percentage','max_absolute_time','max_absolute_time_percentage']].astype(float)
printf("[percentile.py] Effectiveness complete ... %.3f sec\n",time.time()-start)

timeframe_days = 365
for i in range(0,len(gs_timeframe_df)):
	if (gs_timeframe_df.cur_year[i] == '1-Year'):
		timeframe_days = 365
	elif (gs_timeframe_df.cur_year[i] == '6-Month'):
		timeframe_days = 182 

	#tmp_eff_df = eff_df[(eff_df['min_time'] >= (cur_date-timedelta(days=365)) ) & (eff_df['min_time'] <= (cur_date-timedelta(days=1)) )]
	tmp_eff_df = eff_df[(eff_df['min_time'] > (cur_date-timedelta(days=timeframe_days)) )]

	####################
	## Account-Level
	####################
	## Summarize (groupby commands)
	account_name_eff_df = tmp_eff_df[['account_id','account_name','industry_name','BEEs']].drop_duplicates().set_index('account_id').sort_index()
	median_eff_df = tmp_eff_df[['account_id','effectiveness_absolute']].groupby('account_id').median().rename(columns={'effectiveness_absolute':'MEDIAN_effectiveness_absolute'}) 
	mean_eff_df = tmp_eff_df[['account_id','effectiveness_absolute']].groupby('account_id').mean().rename(columns={'effectiveness_absolute':'AVG_effectiveness_absolute'}) 
	count_eff_df = tmp_eff_df[['account_id','parent_id']].groupby('account_id').count().rename(columns={'parent_id':'Nparent'}) 
	count_Nvideo_eff_df = pd.DataFrame(tmp_eff_df[['account_id','video_id']].groupby('account_id').video_id.nunique()).rename(columns={'video_id':'Nvideo'}).rename(columns={0:'Nvideo'}) 

	## Merge dataframes to create summary data frame
	cur_year_eff_df = pd.DataFrame([gs_timeframe_df.cur_year[i]] * len(account_name_eff_df)).rename(columns={0:'cur_year'})
	tmp_account_eff_df = cur_year_eff_df.join(pd.DataFrame(list(account_name_eff_df.index))).rename(columns={0:'account_id'})
	tmp_account_eff_df = pd.merge(tmp_account_eff_df,account_name_eff_df,how='left',left_on='account_id',right_index=True)
	tmp_account_eff_df = pd.merge(tmp_account_eff_df,count_eff_df,how='left',left_on='account_id',right_index=True)
	tmp_account_eff_df = pd.merge(tmp_account_eff_df,count_Nvideo_eff_df,how='left',left_on='account_id',right_index=True)
	tmp_account_eff_df = pd.merge(tmp_account_eff_df,median_eff_df,how='left',left_on='account_id',right_index=True)
	tmp_account_eff_df = pd.merge(tmp_account_eff_df,mean_eff_df,how='left',left_on='account_id',right_index=True)

	if (i == 0):
		account_eff_df = tmp_account_eff_df
	else:
		account_eff_df = account_eff_df.append(tmp_account_eff_df,ignore_index = True)

	####################
	## Video-Level
	####################
	## Summarize (groupby commands)
	#account_name_video_eff_df = tmp_eff_df[['account_id','account_name','industry_name','BEEs','video_id','video_title','video_category']].groupby(['account_id','video_id']).agg(lambda x:list(x)[0])
	account_name_video_eff_df = tmp_eff_df[['account_id','account_name','industry_name','BEEs','video_id','video_title','video_category']].drop_duplicates().set_index(['account_id','video_id']).sort_index()
	median_video_eff_df = tmp_eff_df[['account_id','video_id','effectiveness_absolute']].groupby(['account_id','video_id']).median().rename(columns={'effectiveness_absolute':'MEDIAN_effectiveness_absolute'}) 
	mean_video_eff_df = tmp_eff_df[['account_id','video_id','effectiveness_absolute']].groupby(['account_id','video_id']).mean().rename(columns={'effectiveness_absolute':'AVG_effectiveness_absolute'}) 
	count_video_eff_df = tmp_eff_df[['account_id','video_id','parent_id']].groupby(['account_id','video_id']).count().rename(columns={'parent_id':'Nparent'}) 
	
	## Merge dataframes to create summary data frame
	cur_year_video_eff_df = pd.DataFrame([gs_timeframe_df.cur_year[i]] * len(account_name_video_eff_df)).rename(columns={0:'cur_year'})
	tmp_account_video_eff_df = cur_year_video_eff_df.join(pd.DataFrame(list(account_name_video_eff_df.index))).rename(columns={0:'account_id'}).rename(columns={1:'video_id'}) 
	tmp_account_video_eff_df = pd.merge(tmp_account_video_eff_df,account_name_video_eff_df,how='left',left_on=['account_id','video_id'],right_index=True)
	tmp_account_video_eff_df = pd.merge(tmp_account_video_eff_df,count_video_eff_df,how='left',left_on=['account_id','video_id'],right_index=True)
	tmp_account_video_eff_df = pd.merge(tmp_account_video_eff_df,median_video_eff_df,how='left',left_on=['account_id','video_id'],right_index=True)
	tmp_account_video_eff_df = pd.merge(tmp_account_video_eff_df,mean_video_eff_df,how='left',left_on=['account_id','video_id'],right_index=True)

	if (i == 0):
		account_video_eff_df = tmp_account_video_eff_df
	else:
		account_video_eff_df = account_video_eff_df.append(tmp_account_video_eff_df,ignore_index = True)

#account_eff_df.to_csv('/media/sf_transfer/account_eff_df.csv')	
#account_video_eff_df.to_csv('/media/sf_transfer/account_video_eff_df.csv')	
account_video_eff_df = pd.merge(account_video_eff_df,account_eff_df[['cur_year','account_id','Nvideo']],how='left',left_on=['cur_year','account_id'],right_on=['cur_year','account_id']).sort(['cur_year','account_id']).reset_index(drop=True) 

###############################
# Begin to summarize accounts
###############################
account_reach_df = pd.read_sql(query_account_reach,con)
account_video_reach_df = pd.read_sql(query_account_video_reach,con)
account_video_reach_df = account_video_reach_df.drop('Nvideo',1)   
account_video_reach_df = pd.merge(account_video_reach_df,account_reach_df[['cur_year','account_id','Nvideo']],how='left',left_on=['cur_year','account_id'],right_on=['cur_year','account_id']).sort(['cur_year','account_id']).reset_index(drop=True) 
#account_eff_df = pd.read_sql(query_account_eff,con)
#account_video_eff_df = pd.read_sql(query_account_video_eff,con)

account_reach_df[['PARENT_reach','USER_reach','AVG_target_audience']] = account_reach_df[['PARENT_reach','USER_reach','AVG_target_audience']].astype(float)
account_video_reach_df[['PARENT_reach','USER_reach']] = account_video_reach_df[['PARENT_reach','USER_reach']].astype(float)
account_eff_df[['AVG_effectiveness_absolute','MEDIAN_effectiveness_absolute']] = account_eff_df[['AVG_effectiveness_absolute','MEDIAN_effectiveness_absolute']].astype(float)
account_video_eff_df[['AVG_effectiveness_absolute','MEDIAN_effectiveness_absolute']] = account_video_eff_df[['AVG_effectiveness_absolute','MEDIAN_effectiveness_absolute']].astype(float)

####################################
# Create BEE / library_bin
####################################
account_reach_df = create_BEE_bin(account_reach_df)
account_reach_df = create_Library_bin(account_reach_df)
account_video_reach_df = create_BEE_bin(account_video_reach_df)
account_video_reach_df = create_Library_bin(account_video_reach_df)
account_eff_df = create_BEE_bin(account_eff_df)
account_eff_df = create_Library_bin(account_eff_df)
account_video_eff_df = create_BEE_bin(account_video_eff_df)
account_video_eff_df = create_Library_bin(account_video_eff_df)

#########################################
# Remove all None's prior to calculations
#########################################
account_reach_NoNA_df = account_reach_df[~isnan(account_reach_df['USER_reach'])].reset_index(drop=True) 
account_video_reach_NoNA_df = account_video_reach_df[~isnan(account_video_reach_df['USER_reach'])].reset_index(drop=True) 
account_eff_NoNA_df = account_eff_df[~isnan(account_eff_df['MEDIAN_effectiveness_absolute'])].reset_index(drop=True) 
account_video_eff_NoNA_df = account_video_eff_df[~isnan(account_video_eff_df['MEDIAN_effectiveness_absolute'])].reset_index(drop=True) 

######################################
# Perform for each timeframe
######################################
account_reach_NoNA_df['gs_percentile'] = ['NA'] * len(account_reach_NoNA_df)
account_reach_NoNA_df['industry_percentile'] = ['NA'] * len(account_reach_NoNA_df)
account_reach_NoNA_df['bee_percentile'] = ['NA'] * len(account_reach_NoNA_df)
account_reach_NoNA_df['Nvideo_percentile'] = ['NA'] * len(account_reach_NoNA_df)
account_video_reach_NoNA_df['gs_video_percentile'] = ['NA'] * len(account_video_reach_NoNA_df)
account_video_reach_NoNA_df['industry_video_percentile'] = ['NA'] * len(account_video_reach_NoNA_df)
account_video_reach_NoNA_df['bee_video_percentile'] = ['NA'] * len(account_video_reach_NoNA_df)
account_video_reach_NoNA_df['Nvideo_video_percentile'] = ['NA'] * len(account_video_reach_NoNA_df)
account_eff_NoNA_df['gs_percentile'] = ['NA'] * len(account_eff_NoNA_df)
account_eff_NoNA_df['industry_percentile'] = ['NA'] * len(account_eff_NoNA_df)
account_eff_NoNA_df['bee_percentile'] = ['NA'] * len(account_eff_NoNA_df)
account_eff_NoNA_df['Nvideo_percentile'] = ['NA'] * len(account_eff_NoNA_df)
account_video_eff_NoNA_df['gs_video_percentile'] = ['NA'] * len(account_video_eff_NoNA_df)
account_video_eff_NoNA_df['industry_video_percentile'] = ['NA'] * len(account_video_eff_NoNA_df)
account_video_eff_NoNA_df['bee_video_percentile'] = ['NA'] * len(account_video_eff_NoNA_df)
account_video_eff_NoNA_df['Nvideo_video_percentile'] = ['NA'] * len(account_video_eff_NoNA_df)

account_reach_NoNA_df['USER_reach_corrected'] = [x if x <= 1 else 1.000 for x in account_reach_NoNA_df['USER_reach']]
account_video_reach_NoNA_df['USER_reach_corrected'] = [x if x <= 1 else 1.000 for x in account_video_reach_NoNA_df['USER_reach']]

gs_reach = {} 
gs_eff = {} 
for i in range(0,len(gs_timeframe_df)):
	cur_timeframe = gs_timeframe_df['cur_year'][i]

	cur_reach_NoNA_df = account_reach_NoNA_df[(account_reach_NoNA_df['cur_year'] == cur_timeframe) & (account_reach_NoNA_df['in_benchmark'] == 1)].reset_index()
	cur_video_reach_NoNA_df = account_video_reach_NoNA_df[(account_video_reach_NoNA_df['cur_year'] == cur_timeframe) & (account_video_reach_NoNA_df['in_benchmark'] == 1)].reset_index()
	cur_eff_NoNA_df = account_eff_NoNA_df[(account_eff_NoNA_df['cur_year'] == cur_timeframe)].reset_index()
	cur_video_eff_NoNA_df = account_video_eff_NoNA_df[(account_video_eff_NoNA_df['cur_year'] == cur_timeframe)].reset_index()

	#cur_reach_NoNA_df = account_reach_NoNA_df[(account_reach_NoNA_df['cur_year'] == cur_timeframe)].reset_index()
	#cur_video_reach_NoNA_df = account_video_reach_NoNA_df[(account_video_reach_NoNA_df['cur_year'] == cur_timeframe)].reset_index()
	#cur_eff_NoNA_df = account_eff_NoNA_df[(account_eff_NoNA_df['cur_year'] == cur_timeframe)].reset_index()
	#cur_video_eff_NoNA_df = account_video_eff_NoNA_df[(account_video_eff_NoNA_df['cur_year'] == cur_timeframe)].reset_index()

	###############################
	# Create percentiles 
	###############################

	############################
	# GS percentiles
	############################
	## 1a) Account-Level
	gs_reach_percentile = [stats.percentileofscore(cur_reach_NoNA_df['USER_reach_corrected'],a,'weak') for a in cur_reach_NoNA_df['USER_reach_corrected']] 
	for j in range(0,len(gs_reach_percentile)):
		account_reach_NoNA_df['gs_percentile'][cur_reach_NoNA_df['index'][j]] = gs_reach_percentile[j]

	gs_eff_percentile = [stats.percentileofscore(cur_eff_NoNA_df['MEDIAN_effectiveness_absolute'],a,'weak') for a in cur_eff_NoNA_df['MEDIAN_effectiveness_absolute']] 
	for j in range(0,len(gs_eff_percentile)):
		account_eff_NoNA_df['gs_percentile'][cur_eff_NoNA_df['index'][j]] = gs_eff_percentile[j]

	## 1b) Account & Video-Level 
	gs_video_reach_percentile = create_percentile_bin(cur_video_reach_NoNA_df,'video_category','USER_reach_corrected')
	for j in range(0,len(gs_video_reach_percentile)):
		account_video_reach_NoNA_df['gs_video_percentile'][cur_video_reach_NoNA_df['index'][j]] = gs_video_reach_percentile[j]

	gs_video_eff_percentile = create_percentile_bin(cur_video_eff_NoNA_df,'video_category','MEDIAN_effectiveness_absolute')
	for j in range(0,len(gs_video_eff_percentile)):
		account_video_eff_NoNA_df['gs_video_percentile'][cur_video_eff_NoNA_df['index'][j]] = gs_video_eff_percentile[j]

	############################
	# Industry percentiles
	############################
	## 2a) Account-Level 
	industry_reach_percentile = create_percentile_bin(cur_reach_NoNA_df,'industry_name','USER_reach_corrected')
	for j in range(0,len(industry_reach_percentile)):
		account_reach_NoNA_df['industry_percentile'][cur_reach_NoNA_df['index'][j]] = industry_reach_percentile[j]

	industry_eff_percentile = create_percentile_bin(cur_eff_NoNA_df,'industry_name','MEDIAN_effectiveness_absolute')
	for j in range(0,len(industry_eff_percentile)):
		account_eff_NoNA_df['industry_percentile'][cur_eff_NoNA_df['index'][j]] = industry_eff_percentile[j]

	## 2b) Account & Video-Level 
	industry_video_reach_percentile = create_percentile_bin_2D(cur_video_reach_NoNA_df,'video_category','industry_name','USER_reach_corrected')
	for j in range(0,len(industry_video_reach_percentile)):
		account_video_reach_NoNA_df['industry_video_percentile'][cur_video_reach_NoNA_df['index'][j]] = industry_video_reach_percentile[j]

	industry_video_eff_percentile = create_percentile_bin_2D(cur_video_eff_NoNA_df,'video_category','industry_name','MEDIAN_effectiveness_absolute')
	for j in range(0,len(industry_video_eff_percentile)):
		account_video_eff_NoNA_df['industry_video_percentile'][cur_video_eff_NoNA_df['index'][j]] = industry_video_eff_percentile[j]

	############################
	# BEE percentiles
	############################
	## 3a) Account-Level 
	bee_reach_percentile = create_percentile_bin(cur_reach_NoNA_df,'BEE_bin','USER_reach_corrected')
	for j in range(0,len(bee_reach_percentile)):
		account_reach_NoNA_df['bee_percentile'][cur_reach_NoNA_df['index'][j]] = bee_reach_percentile[j]

	bee_eff_percentile = create_percentile_bin(cur_eff_NoNA_df,'BEE_bin','MEDIAN_effectiveness_absolute')
	for j in range(0,len(bee_eff_percentile)):
		account_eff_NoNA_df['bee_percentile'][cur_eff_NoNA_df['index'][j]] = bee_eff_percentile[j]

	## 3b) Account & Video-Level 
	bee_video_reach_percentile = create_percentile_bin_2D(cur_video_reach_NoNA_df,'video_category','BEE_bin','USER_reach_corrected')
	for j in range(0,len(bee_video_reach_percentile)):
		account_video_reach_NoNA_df['bee_video_percentile'][cur_video_reach_NoNA_df['index'][j]] = bee_video_reach_percentile[j]

	bee_video_eff_percentile = create_percentile_bin_2D(cur_video_eff_NoNA_df,'video_category','BEE_bin','MEDIAN_effectiveness_absolute')
	for j in range(0,len(bee_video_eff_percentile)):
		account_video_eff_NoNA_df['bee_video_percentile'][cur_video_eff_NoNA_df['index'][j]] = bee_video_eff_percentile[j]

	############################
	# Nvideo percentiles
	############################
	## 4a) Account-Level 
	Nvideo_reach_percentile = create_percentile_bin(cur_reach_NoNA_df,'Nvideo_bin','USER_reach_corrected')
	for j in range(0,len(Nvideo_reach_percentile)):
		account_reach_NoNA_df['Nvideo_percentile'][cur_reach_NoNA_df['index'][j]] = Nvideo_reach_percentile[j]

	Nvideo_eff_percentile = create_percentile_bin(cur_eff_NoNA_df,'Nvideo_bin','MEDIAN_effectiveness_absolute')
	for j in range(0,len(Nvideo_eff_percentile)):
		account_eff_NoNA_df['Nvideo_percentile'][cur_eff_NoNA_df['index'][j]] = Nvideo_eff_percentile[j]

	## 4b) Account & Video-Level 
	Nvideo_video_reach_percentile = create_percentile_bin_2D(cur_video_reach_NoNA_df,'video_category','Nvideo_bin','USER_reach_corrected')
	for j in range(0,len(Nvideo_video_reach_percentile)):
		account_video_reach_NoNA_df['Nvideo_video_percentile'][cur_video_reach_NoNA_df['index'][j]] = Nvideo_video_reach_percentile[j]

	Nvideo_video_eff_percentile = create_percentile_bin_2D(cur_video_eff_NoNA_df,'video_category','Nvideo_bin','MEDIAN_effectiveness_absolute')
	for j in range(0,len(Nvideo_video_eff_percentile)):
		account_video_eff_NoNA_df['Nvideo_video_percentile'][cur_video_eff_NoNA_df['index'][j]] = Nvideo_video_eff_percentile[j]

	########################################
	# Calculate Global Account Median values 
	########################################
	gs_reach['cur_year'] = cur_timeframe
	gs_reach['MEDIAN_USER_reach'] = np.median(cur_reach_NoNA_df['USER_reach_corrected'])  # Remove all None's prior to median calculation
	gs_eff['cur_year'] = cur_timeframe
	gs_eff['MEDIAN_MEDIAN_effectiveness_absolute'] = np.median(cur_eff_NoNA_df['MEDIAN_effectiveness_absolute'])  # Remove all None's prior to median calculation
	if (i == 0):
		gs_reach_df = pd.DataFrame(gs_reach,index=[0])
		gs_eff_df = pd.DataFrame(gs_eff,index=[0])
		industry_reach_df = create_median_benchmark(cur_reach_NoNA_df,'industry_name',cur_timeframe,'cur_year')
		bee_reach_df = create_median_benchmark(cur_reach_NoNA_df,'BEE_bin',cur_timeframe,'cur_year')
		Nvideo_reach_df = create_median_benchmark(cur_reach_NoNA_df,'Nvideo_bin',cur_timeframe,'cur_year')
		industry_eff_df = create_median_benchmark(cur_eff_NoNA_df,'industry_name',cur_timeframe,'cur_year')
		bee_eff_df = create_median_benchmark(cur_eff_NoNA_df,'BEE_bin',cur_timeframe,'cur_year')
		Nvideo_eff_df = create_median_benchmark(cur_eff_NoNA_df,'Nvideo_bin',cur_timeframe,'cur_year')
	else:
		gs_reach_df = gs_reach_df.append(pd.DataFrame(gs_reach,index=[0]),ignore_index=True )
		gs_eff_df = gs_eff_df.append(pd.DataFrame(gs_eff,index=[0]),ignore_index=True )
		industry_reach_df = industry_reach_df.append(create_median_benchmark(cur_reach_NoNA_df,'industry_name',cur_timeframe,'cur_year'),ignore_index=False)
		bee_reach_df = bee_reach_df.append(create_median_benchmark(cur_reach_NoNA_df,'BEE_bin',cur_timeframe,'cur_year'),ignore_index=False)
		Nvideo_reach_df = Nvideo_reach_df.append(create_median_benchmark(cur_reach_NoNA_df,'Nvideo_bin',cur_timeframe,'cur_year'),ignore_index=False)
		industry_eff_df = industry_eff_df.append(create_median_benchmark(cur_eff_NoNA_df,'industry_name',cur_timeframe,'cur_year'),ignore_index=False)
		bee_eff_df = bee_eff_df.append(create_median_benchmark(cur_eff_NoNA_df,'BEE_bin',cur_timeframe,'cur_year'),ignore_index=False)
		Nvideo_eff_df = Nvideo_eff_df.append(create_median_benchmark(cur_eff_NoNA_df,'Nvideo_bin',cur_timeframe,'cur_year'),ignore_index=False)

	###############################################
	# Calculate Global Account-Video Median values 
	###############################################
	gs_reach['cur_year'] = cur_timeframe
	gs_reach['MEDIAN_USER_reach'] = np.median(cur_reach_NoNA_df['USER_reach_corrected'])  # Remove all None's prior to median calculation
	gs_eff['cur_year'] = cur_timeframe
	gs_eff['MEDIAN_MEDIAN_effectiveness_absolute'] = np.median(cur_eff_NoNA_df['MEDIAN_effectiveness_absolute'])  # Remove all None's prior to median calculation
	if (i == 0):
		gs_video_reach_df = create_median_benchmark(cur_video_reach_NoNA_df,'video_category',cur_timeframe,'cur_year')
		gs_video_eff_df = create_median_benchmark(cur_video_eff_NoNA_df,'video_category',cur_timeframe,'cur_year')

		unique_video = sorted(list(set(cur_video_reach_NoNA_df['video_category'])))
		for j in range(0,len(unique_video)):
			## Get current video category
			tmp_video_reach_df = cur_video_reach_NoNA_df[cur_video_reach_NoNA_df['video_category'] == unique_video[j]].reset_index(drop=True)
			## Calculate Account-Video reach benchmarks
			if (j == 0):
				industry_video_reach_df = create_median_benchmark(tmp_video_reach_df,'industry_name',cur_timeframe,'cur_year')
				industry_video_reach_df['video_category'] = [unique_video[j]] * len(industry_video_reach_df)
				bee_video_reach_df = create_median_benchmark(tmp_video_reach_df,'BEE_bin',cur_timeframe,'cur_year')
				bee_video_reach_df['video_category'] = [unique_video[j]] * len(bee_video_reach_df)
				Nvideo_video_reach_df = create_median_benchmark(tmp_video_reach_df,'Nvideo_bin',cur_timeframe,'cur_year')
				Nvideo_video_reach_df['video_category'] = [unique_video[j]] * len(Nvideo_video_reach_df)
			else:
				tmp_industry_video_reach_df = create_median_benchmark(tmp_video_reach_df,'industry_name',cur_timeframe,'cur_year')
				tmp_industry_video_reach_df['video_category'] = [unique_video[j]] * len(tmp_industry_video_reach_df)
				industry_video_reach_df = industry_video_reach_df.append(tmp_industry_video_reach_df,ignore_index=True)
		
				tmp_bee_video_reach_df = create_median_benchmark(tmp_video_reach_df,'BEE_bin',cur_timeframe,'cur_year')
				tmp_bee_video_reach_df['video_category'] = [unique_video[j]] * len(tmp_bee_video_reach_df)
				bee_video_reach_df = bee_video_reach_df.append(tmp_bee_video_reach_df,ignore_index=True)

				tmp_Nvideo_video_reach_df = create_median_benchmark(tmp_video_reach_df,'Nvideo_bin',cur_timeframe,'cur_year')
				tmp_Nvideo_video_reach_df['video_category'] = [unique_video[j]] * len(tmp_Nvideo_video_reach_df)
				Nvideo_video_reach_df = Nvideo_video_reach_df.append(tmp_Nvideo_video_reach_df,ignore_index=True)


			## Get current video category
			tmp_video_eff_df = cur_video_eff_NoNA_df[cur_video_eff_NoNA_df['video_category'] == unique_video[j]].reset_index(drop=True)
			## Calculate Account-Video reach benchmarks
			if (j == 0):
				industry_video_eff_df = create_median_benchmark(tmp_video_eff_df,'industry_name',cur_timeframe,'cur_year')
				industry_video_eff_df['video_category'] = [unique_video[j]] * len(industry_video_eff_df)
				bee_video_eff_df = create_median_benchmark(tmp_video_eff_df,'BEE_bin',cur_timeframe,'cur_year')
				bee_video_eff_df['video_category'] = [unique_video[j]] * len(bee_video_eff_df)
				Nvideo_video_eff_df = create_median_benchmark(tmp_video_eff_df,'Nvideo_bin',cur_timeframe,'cur_year')
				Nvideo_video_eff_df['video_category'] = [unique_video[j]] * len(Nvideo_video_eff_df)
			else:
				tmp_industry_video_eff_df = create_median_benchmark(tmp_video_eff_df,'industry_name',cur_timeframe,'cur_year')
				tmp_industry_video_eff_df['video_category'] = [unique_video[j]] * len(tmp_industry_video_eff_df)
				industry_video_eff_df = industry_video_eff_df.append(tmp_industry_video_eff_df,ignore_index=True)
		
				tmp_bee_video_eff_df = create_median_benchmark(tmp_video_eff_df,'BEE_bin',cur_timeframe,'cur_year')
				tmp_bee_video_eff_df['video_category'] = [unique_video[j]] * len(tmp_bee_video_eff_df)
				bee_video_eff_df = bee_video_eff_df.append(tmp_bee_video_eff_df,ignore_index=True)

				tmp_Nvideo_video_eff_df = create_median_benchmark(tmp_video_eff_df,'Nvideo_bin',cur_timeframe,'cur_year')
				tmp_Nvideo_video_eff_df['video_category'] = [unique_video[j]] * len(tmp_Nvideo_video_eff_df)
				Nvideo_video_eff_df = Nvideo_video_eff_df.append(tmp_Nvideo_video_eff_df,ignore_index=True)

	else:
		gs_video_reach_df = gs_video_reach_df.append(create_median_benchmark(cur_video_reach_NoNA_df,'video_category',cur_timeframe,'cur_year'),ignore_index=True )
		gs_video_eff_df   = gs_video_eff_df.append(create_median_benchmark(cur_video_eff_NoNA_df,'video_category',cur_timeframe,'cur_year'),ignore_index=True )

		unique_video = sorted(list(set(cur_video_reach_NoNA_df['video_category'])))
		for j in range(0,len(unique_video)):
			## Get current video category
			tmp_video_reach_df = cur_video_reach_NoNA_df[cur_video_reach_NoNA_df['video_category'] == unique_video[j]].reset_index(drop=True)

			## Calculate Account-Video reach benchmarks
			tmp_industry_video_reach_df = create_median_benchmark(tmp_video_reach_df,'industry_name',cur_timeframe,'cur_year')
			tmp_industry_video_reach_df['video_category'] = [unique_video[j]] * len(tmp_industry_video_reach_df)
			industry_video_reach_df = industry_video_reach_df.append(tmp_industry_video_reach_df,ignore_index=True)
		
			tmp_bee_video_reach_df = create_median_benchmark(tmp_video_reach_df,'BEE_bin',cur_timeframe,'cur_year')
			tmp_bee_video_reach_df['video_category'] = [unique_video[j]] * len(tmp_bee_video_reach_df)
			bee_video_reach_df = bee_video_reach_df.append(tmp_bee_video_reach_df,ignore_index=True)

			tmp_Nvideo_video_reach_df = create_median_benchmark(tmp_video_reach_df,'Nvideo_bin',cur_timeframe,'cur_year')
			tmp_Nvideo_video_reach_df['video_category'] = [unique_video[j]] * len(tmp_Nvideo_video_reach_df)
			Nvideo_video_reach_df = Nvideo_video_reach_df.append(tmp_Nvideo_video_reach_df,ignore_index=True)


			## Get current video category
			tmp_video_eff_df = cur_video_eff_NoNA_df[cur_video_eff_NoNA_df['video_category'] == unique_video[j]].reset_index(drop=True)
			## Calculate Account-Video reach benchmarks
			tmp_industry_video_eff_df = create_median_benchmark(tmp_video_eff_df,'industry_name',cur_timeframe,'cur_year')
			tmp_industry_video_eff_df['video_category'] = [unique_video[j]] * len(tmp_industry_video_eff_df)
			industry_video_eff_df = industry_video_eff_df.append(tmp_industry_video_eff_df,ignore_index=True)
		
			tmp_bee_video_eff_df = create_median_benchmark(tmp_video_eff_df,'BEE_bin',cur_timeframe,'cur_year')
			tmp_bee_video_eff_df['video_category'] = [unique_video[j]] * len(tmp_bee_video_eff_df)
			bee_video_eff_df = bee_video_eff_df.append(tmp_bee_video_eff_df,ignore_index=True)

			tmp_Nvideo_video_eff_df = create_median_benchmark(tmp_video_eff_df,'Nvideo_bin',cur_timeframe,'cur_year')
			tmp_Nvideo_video_eff_df['video_category'] = [unique_video[j]] * len(tmp_Nvideo_video_eff_df)
			Nvideo_video_eff_df = Nvideo_video_eff_df.append(tmp_Nvideo_video_eff_df,ignore_index=True)

#####################################################
# Merge the actual benchmark values (non-percentile)
#####################################################
#account_reach_NoNA_df = pd.merge(account_reach_NoNA_df,gs_reach_df[['cur_year','MEDIAN_USER_reach']], \
#							how='left',left_on=['cur_year'],right_on=['cur_year']) \
#							.rename(columns={'MEDIAN_USER_reach':'gs_reach'})
#account_reach_NoNA_df = pd.merge(account_reach_NoNA_df,industry_reach_df[['cur_year','industry_name','MEDIAN_USER_reach']], \
#							how='left',left_on=['cur_year','industry_name'],right_on=['cur_year','industry_name']) \
#							.rename(columns={'MEDIAN_USER_reach':'industry_reach'})

######################################################################
# Account-Level ... Merge the actual benchmark values (non-percentile)
######################################################################
account_reach_NoNA_df = pd.merge(account_reach_NoNA_df,gs_reach_df[['cur_year','MEDIAN_USER_reach']], \
							how='left',left_on=['cur_year'],right_on=['cur_year']) \
							.rename(columns={'MEDIAN_USER_reach':'gs_reach'})
account_reach_NoNA_df = pd.merge(account_reach_NoNA_df,industry_reach_df[['cur_year','industry_name','MEDIAN_USER_reach']], \
							how='left',left_on=['cur_year','industry_name'],right_on=['cur_year','industry_name']) \
							.rename(columns={'MEDIAN_USER_reach':'industry_reach'})
account_reach_NoNA_df = pd.merge(account_reach_NoNA_df,bee_reach_df[['cur_year','BEE_bin','MEDIAN_USER_reach']], \
							how='left',left_on=['cur_year','BEE_bin'],right_on=['cur_year','BEE_bin']) \
							.rename(columns={'MEDIAN_USER_reach':'bee_reach'})
account_reach_NoNA_df = pd.merge(account_reach_NoNA_df,Nvideo_reach_df[['cur_year','Nvideo_bin','MEDIAN_USER_reach']], \
							how='left',left_on=['cur_year','Nvideo_bin'],right_on=['cur_year','Nvideo_bin']) \
							.rename(columns={'MEDIAN_USER_reach':'Nvideo_reach'})

account_eff_NoNA_df = pd.merge(account_eff_NoNA_df,gs_eff_df[['cur_year','MEDIAN_MEDIAN_effectiveness_absolute']], \
							how='left',left_on=['cur_year'],right_on=['cur_year']) \
							.rename(columns={'MEDIAN_MEDIAN_effectiveness_absolute':'gs_effectiveness'})
account_eff_NoNA_df = pd.merge(account_eff_NoNA_df,industry_eff_df[['cur_year','industry_name','MEDIAN_MEDIAN_effectiveness_absolute']], \
							how='left',left_on=['cur_year','industry_name'],right_on=['cur_year','industry_name']) \
							.rename(columns={'MEDIAN_MEDIAN_effectiveness_absolute':'industry_effectiveness'})
account_eff_NoNA_df = pd.merge(account_eff_NoNA_df,bee_eff_df[['cur_year','BEE_bin','MEDIAN_MEDIAN_effectiveness_absolute']], \
							how='left',left_on=['cur_year','BEE_bin'],right_on=['cur_year','BEE_bin']) \
							.rename(columns={'MEDIAN_MEDIAN_effectiveness_absolute':'bee_effectiveness'})
account_eff_NoNA_df = pd.merge(account_eff_NoNA_df,Nvideo_eff_df[['cur_year','Nvideo_bin','MEDIAN_MEDIAN_effectiveness_absolute']], \
							how='left',left_on=['cur_year','Nvideo_bin'],right_on=['cur_year','Nvideo_bin']) \
							.rename(columns={'MEDIAN_MEDIAN_effectiveness_absolute':'Nvideo_effectiveness'})

######################################################################
# Account-Video Level ... Merge the actual benchmark values (non-percentile)
######################################################################
account_video_reach_NoNA_df = pd.merge(account_video_reach_NoNA_df,gs_video_reach_df[['cur_year','video_category','MEDIAN_USER_reach']], \
							how='left',left_on=['cur_year','video_category',],right_on=['cur_year','video_category',]) \
							.rename(columns={'MEDIAN_USER_reach':'gs_video_reach'})
account_video_reach_NoNA_df = pd.merge(account_video_reach_NoNA_df,industry_video_reach_df[['cur_year','video_category','industry_name','MEDIAN_USER_reach']], \
							how='left',left_on=['cur_year','video_category','industry_name'],right_on=['cur_year','video_category','industry_name']) \
							.rename(columns={'MEDIAN_USER_reach':'industry_video_reach'})
account_video_reach_NoNA_df = pd.merge(account_video_reach_NoNA_df,bee_video_reach_df[['cur_year','video_category','BEE_bin','MEDIAN_USER_reach']], \
							how='left',left_on=['cur_year','video_category','BEE_bin'],right_on=['cur_year','video_category','BEE_bin']) \
							.rename(columns={'MEDIAN_USER_reach':'bee_video_reach'})
account_video_reach_NoNA_df = pd.merge(account_video_reach_NoNA_df,Nvideo_video_reach_df[['cur_year','video_category','Nvideo_bin','MEDIAN_USER_reach']], \
							how='left',left_on=['cur_year','video_category','Nvideo_bin'],right_on=['cur_year','video_category','Nvideo_bin']) \
							.rename(columns={'MEDIAN_USER_reach':'Nvideo_video_reach'})

account_video_eff_NoNA_df = pd.merge(account_video_eff_NoNA_df,gs_video_eff_df[['cur_year','video_category','MEDIAN_MEDIAN_effectiveness_absolute']], \
							how='left',left_on=['cur_year','video_category',],right_on=['cur_year','video_category',]) \
							.rename(columns={'MEDIAN_MEDIAN_effectiveness_absolute':'gs_video_effectiveness'})
account_video_eff_NoNA_df = pd.merge(account_video_eff_NoNA_df,industry_video_eff_df[['cur_year','video_category','industry_name','MEDIAN_MEDIAN_effectiveness_absolute']], \
							how='left',left_on=['cur_year','video_category','industry_name'],right_on=['cur_year','video_category','industry_name']) \
							.rename(columns={'MEDIAN_MEDIAN_effectiveness_absolute':'industry_video_effectiveness'})
account_video_eff_NoNA_df = pd.merge(account_video_eff_NoNA_df,bee_video_eff_df[['cur_year','video_category','BEE_bin','MEDIAN_MEDIAN_effectiveness_absolute']], \
							how='left',left_on=['cur_year','video_category','BEE_bin'],right_on=['cur_year','video_category','BEE_bin']) \
							.rename(columns={'MEDIAN_MEDIAN_effectiveness_absolute':'bee_video_effectiveness'})
account_video_eff_NoNA_df = pd.merge(account_video_eff_NoNA_df,Nvideo_video_eff_df[['cur_year','video_category','Nvideo_bin','MEDIAN_MEDIAN_effectiveness_absolute']], \
							how='left',left_on=['cur_year','video_category','Nvideo_bin'],right_on=['cur_year','video_category','Nvideo_bin']) \
							.rename(columns={'MEDIAN_MEDIAN_effectiveness_absolute':'Nvideo_video_effectiveness'})

#test_df = account_reach_NoNA_df[['cur_year','account_id','account_name','USER_reach','gs_percentile','industry_percentile','bee_percentile','Nvideo_percentile']]

#########################################################################
# Fit extra accounts into the percentile ... accounts that were excluded
#
# REACH (MKTG_effectiveness ONLY) 
#########################################################################
for i in range(0,len(gs_timeframe_df)):
	cur_timeframe = gs_timeframe_df['cur_year'][i]

	############################
	# GS percentiles
	############################
	## 1a) Account-Level

	############################
	## Average values below min
	############################
	min_USER_reach = min(account_reach_NoNA_df['USER_reach_corrected'][(account_reach_NoNA_df['in_benchmark'] == 1) \
														   & (account_reach_NoNA_df['cur_year'] == cur_timeframe)])
	min_gs_reach_percentile = min(account_reach_NoNA_df['gs_percentile'][(account_reach_NoNA_df['in_benchmark'] == 1) \
																	   & (account_reach_NoNA_df['cur_year'] == cur_timeframe)])/100
	gs_reach_out_df = account_reach_NoNA_df[(account_reach_NoNA_df['in_benchmark'] == 0) \
										  & (account_reach_NoNA_df['USER_reach_corrected'] <= min_USER_reach) \
										  & (account_reach_NoNA_df['cur_year'] == cur_timeframe)]

	for j in range(0,len(gs_reach_out_df)):
		percent_value = 100*(gs_reach_out_df['USER_reach_corrected'][gs_reach_out_df.index[j]] ) * min_gs_reach_percentile / min_USER_reach
		account_reach_NoNA_df['gs_percentile'][gs_reach_out_df.index[j]] = percent_value 

	#gs_reach_percentile_out = [stats.percentileofscore(gs_reach_out_df['USER_reach'],a,'weak')*min_gs_reach_percentile for a in gs_reach_out_df['USER_reach']]
	#for j in range(0,len(gs_reach_percentile_out)):
	#	account_reach_NoNA_df['gs_percentile'][gs_reach_out_df.index[j]] = gs_reach_percentile_out[j]

	############################
	## Average values above max
	############################
	max_USER_reach = max(account_reach_NoNA_df['USER_reach_corrected'][(account_reach_NoNA_df['in_benchmark'] == 1) \
														   & (account_reach_NoNA_df['cur_year'] == cur_timeframe)])
	max_gs_reach_percentile = max(account_reach_NoNA_df['gs_percentile'][(account_reach_NoNA_df['in_benchmark'] == 1) \
																	   & (account_reach_NoNA_df['cur_year'] == cur_timeframe)])/100
	gs_reach_out_df = account_reach_NoNA_df[(account_reach_NoNA_df['in_benchmark'] == 0) \
										  & (account_reach_NoNA_df['USER_reach_corrected'] >= max_USER_reach) \
										  & (account_reach_NoNA_df['cur_year'] == cur_timeframe)]

	for j in range(0,len(gs_reach_out_df)):
		account_reach_NoNA_df['gs_percentile'][gs_reach_out_df.index[j]] = 100*max_gs_reach_percentile 

	#gs_reach_percentile_out = [stats.percentileofscore(gs_reach_out_df['USER_reach'],a,'weak')*min_gs_reach_percentile for a in gs_reach_out_df['USER_reach']]
	#for j in range(0,len(gs_reach_percentile_out)):
	#	account_reach_NoNA_df['gs_percentile'][gs_reach_out_df.index[j]] = gs_reach_percentile_out[j]

	################################
	## Average values in the center 
	################################
	account_reach_sort_df =	account_reach_NoNA_df.sort(['USER_reach_corrected']).reset_index()
	account_reach_sort_NA_df = account_reach_sort_df[(account_reach_sort_df['gs_percentile'] == 'NA')]
	account_reach_sort_NoNA = account_reach_sort_df[(account_reach_sort_df['gs_percentile'] != 'NA')].index
	gs_reach_percentile_out_NA = []	
	for j in range(0,len(account_reach_sort_NA_df)):
		Nidx = account_reach_sort_NA_df.index[j]
		Nidx_low = max([x for x in account_reach_sort_NoNA if x < Nidx])
		Nidx_high = min([x for x in account_reach_sort_NoNA if x > Nidx])
		percent_value = account_reach_sort_df.ix[Nidx_low]['gs_percentile'] \
						+ (account_reach_sort_df.ix[Nidx]['USER_reach_corrected'] - account_reach_sort_df.ix[Nidx_low]['USER_reach_corrected']) \
						* (account_reach_sort_df.ix[Nidx_high]['gs_percentile'] - account_reach_sort_df.ix[Nidx_low]['gs_percentile']) \
						/ (account_reach_sort_df.ix[Nidx_high]['USER_reach_corrected'] - account_reach_sort_df.ix[Nidx_low]['USER_reach_corrected'])	
		account_reach_sort_df.loc[Nidx,'gs_percentile'] = percent_value
		account_reach_NoNA_df.loc[account_reach_sort_NA_df['index'][Nidx],'gs_percentile'] = percent_value
		
	## 1b) Account & Video-Level 
	#gs_video_reach_percentile = create_percentile_bin(cur_video_reach_NoNA_df,'video_category','USER_reach')
	#for j in range(0,len(gs_video_reach_percentile)):
	#		account_video_reach_NoNA_df['gs_video_percentile'][cur_video_reach_NoNA_df['index'][j]] = gs_video_reach_percentile[j]
	

	############################
	# Industry percentiles
	############################
	## 2a) Account-Level 
	account_reach_NoNA_df = add_non_benchmark_values(cur_timeframe,account_reach_NoNA_df,'USER_reach_corrected','industry_name','industry_percentile') 

	############################
	# BEE percentiles
	############################
	## 3a) Account-Level 
	account_reach_NoNA_df = add_non_benchmark_values(cur_timeframe,account_reach_NoNA_df,'USER_reach_corrected','BEE_bin','bee_percentile') 

	############################
	# Library_size percentiles
	############################
	## 4a) Account-Level 
	account_reach_NoNA_df = add_non_benchmark_values(cur_timeframe,account_reach_NoNA_df,'USER_reach_corrected','Nvideo_bin','Nvideo_percentile') 

	############################
	# Video GS percentiles
	############################
	account_video_reach_NoNA_df = add_non_benchmark_values(cur_timeframe,account_video_reach_NoNA_df,'USER_reach_corrected','video_category','gs_video_percentile') 

	unique_video = list(set(account_video_reach_NoNA_df.video_category))
	for j in range(0,len(unique_video)):

		printf("[percentile.py] %3s of %3s ... Video Category = %40s ... %.2f\n",j+1,len(unique_video),unique_video[j],time.time()-start)

		cur_account_video_reach_NoNA_df = account_video_reach_NoNA_df[(account_video_reach_NoNA_df['video_category'] == unique_video[j])].copy(deep=True)	

		############################
		# Industry percentiles
		############################
		## 2a) Account-Level 
		cur_account_video_reach_NoNA_df = add_non_benchmark_values(cur_timeframe,cur_account_video_reach_NoNA_df,'USER_reach_corrected','industry_name','industry_video_percentile') 
		for k in range(0,len(cur_account_video_reach_NoNA_df)):
			#printf("[percentile.py] %3s of %3s\n",k,len(cur_account_video_reach_NoNA_df))
			account_video_reach_NoNA_df.loc[cur_account_video_reach_NoNA_df.index[k],'industry_video_percentile'] = cur_account_video_reach_NoNA_df.ix[cur_account_video_reach_NoNA_df.index[k]]['industry_video_percentile'] 
	
		############################
		# BEE percentiles
		############################
		## 3a) Account-Level 
		cur_account_video_reach_NoNA_df = add_non_benchmark_values(cur_timeframe,cur_account_video_reach_NoNA_df,'USER_reach_corrected','BEE_bin','bee_video_percentile') 
		for k in range(0,len(cur_account_video_reach_NoNA_df)):
			#printf("[percentile.py] %3s of %3s\n",k,len(cur_account_video_reach_NoNA_df))
			account_video_reach_NoNA_df.loc[cur_account_video_reach_NoNA_df.index[k],'bee_video_percentile'] = cur_account_video_reach_NoNA_df.ix[cur_account_video_reach_NoNA_df.index[k]]['bee_video_percentile'] 

		############################
		# Library_size percentiles
		############################
		## 4a) Account-Level 
		cur_account_video_reach_NoNA_df = add_non_benchmark_values(cur_timeframe,cur_account_video_reach_NoNA_df,'USER_reach_corrected','Nvideo_bin','Nvideo_video_percentile') 
		for k in range(0,len(cur_account_video_reach_NoNA_df)):
			#printf("[percentile.py] %3s of %3s\n",k,len(cur_account_video_reach_NoNA_df))
			account_video_reach_NoNA_df.loc[cur_account_video_reach_NoNA_df.index[k],'Nvideo_video_percentile'] = cur_account_video_reach_NoNA_df.ix[cur_account_video_reach_NoNA_df.index[k]]['Nvideo_video_percentile'] 

#		sys.exit()
##
##account_reach_NoNA_df = pd.merge(account_reach_NoNA_df,gs_reach_df[['cur_year','MEDIAN_USER_reach']], \
##							how='left',left_on=['cur_year'],right_on=['cur_year']) \
##							.rename(columns={'MEDIAN_USER_reach':'gs_reach'})
##account_reach_NoNA_df = pd.merge(account_reach_NoNA_df,industry_reach_df[['cur_year','industry_name','MEDIAN_USER_reach']], \
##							how='left',left_on=['cur_year','industry_name'],right_on=['cur_year','industry_name']) \
##							.rename(columns={'MEDIAN_USER_reach':'industry_reach'})
##account_reach_NoNA_df = pd.merge(account_reach_NoNA_df,bee_reach_df[['cur_year','BEE_bin','MEDIAN_USER_reach']], \
##							how='left',left_on=['cur_year','BEE_bin'],right_on=['cur_year','BEE_bin']) \
##							.rename(columns={'MEDIAN_USER_reach':'bee_reach'})
##account_reach_NoNA_df = pd.merge(account_reach_NoNA_df,Nvideo_reach_df[['cur_year','Nvideo_bin','MEDIAN_USER_reach']], \
##							how='left',left_on=['cur_year','Nvideo_bin'],right_on=['cur_year','Nvideo_bin']) \
##							.rename(columns={'MEDIAN_USER_reach':'Nvideo_reach'})

##############
# Export all 
##############
printf("[percentile.py] WRITE %s ... %.3f sec\n",ACCOUNT_REACH_TABLE_OUT,time.time()-start)
drop_mysql_table(con,DBNAME,ACCOUNT_REACH_TABLE_OUT)
create_ACCOUNT_REACH_mysql_table(con,DBNAME,ACCOUNT_REACH_TABLE_OUT)
insert_into_ACCOUNT_REACH_mysql_DB(con,DBNAME,ACCOUNT_REACH_TABLE_OUT,account_reach_NoNA_df)

printf("[percentile.py] WRITE %s ... %.3f sec\n",ACCOUNT_EFF_TABLE_OUT,time.time()-start)
drop_mysql_table(con,DBNAME,ACCOUNT_EFF_TABLE_OUT)
create_ACCOUNT_EFF_mysql_table(con,DBNAME,ACCOUNT_EFF_TABLE_OUT)
insert_into_ACCOUNT_EFF_mysql_DB(con,DBNAME,ACCOUNT_EFF_TABLE_OUT,account_eff_NoNA_df)

printf("[percentile.py] WRITE %s ... %.3f sec\n",ACCOUNT_VIDEO_REACH_TABLE_OUT,time.time()-start)
drop_mysql_table(con,DBNAME,ACCOUNT_VIDEO_REACH_TABLE_OUT)
create_ACCOUNT_VIDEO_REACH_mysql_table(con,DBNAME,ACCOUNT_VIDEO_REACH_TABLE_OUT)
insert_into_ACCOUNT_VIDEO_REACH_mysql_DB(con,DBNAME,ACCOUNT_VIDEO_REACH_TABLE_OUT,account_video_reach_NoNA_df)

printf("[percentile.py] WRITE %s ... %.3f sec\n",ACCOUNT_VIDEO_EFF_TABLE_OUT,time.time()-start)
drop_mysql_table(con,DBNAME,ACCOUNT_VIDEO_EFF_TABLE_OUT)
create_ACCOUNT_VIDEO_EFF_mysql_table(con,DBNAME,ACCOUNT_VIDEO_EFF_TABLE_OUT)
insert_into_ACCOUNT_VIDEO_EFF_mysql_DB(con,DBNAME,ACCOUNT_VIDEO_EFF_TABLE_OUT,account_video_eff_NoNA_df)

### Output results
#account_reach_NoNA_df.to_csv('./output/account_reach_NoNA_' + cur_datetime.strftime('%Y%m%d') + '.csv')
#account_eff_NoNA_df.to_csv('./output/account_eff_NoNA_' + cur_datetime.strftime('%Y%m%d') + '.csv')
#account_video_reach_NoNA_df.to_csv('./output/account_video_reach_NoNA_' + cur_datetime.strftime('%Y%m%d') + '.csv')
#account_video_eff_NoNA_df.to_csv('./output/account_video_eff_NoNA_' + cur_datetime.strftime('%Y%m%d') + '.csv')

