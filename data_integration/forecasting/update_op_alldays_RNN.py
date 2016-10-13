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
from inspect import currentframe, getframeinfo

start = time.time()
cur_datetime = datetime.now()
execfile('/home/analytics/analytics_sandbox/python_libs/stuff.py')

# Logging
import log_libs as log
LOG = log.init_logging()

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',400)

con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

DEBUG = False  # True --> This is for a single / group of unique Opportunity Ids

##############################################
##############################################
##############################################
############### Main Program #################
##############################################
##############################################
##############################################

sdata_timeline_df = pd.read_csv('./output/sdata_op_history_RSF.csv')
sdata_timeline_df = sdata_timeline_df.drop('Unnamed: 0',1)
sdata_timeline_df['CreatedDate'] = pd.to_datetime(sdata_timeline_df['CreatedDate'])
sdata_timeline_df['Op_CreatedDate'] = pd.to_datetime(sdata_timeline_df['Op_CreatedDate'])

baseline_cols 	= ['AccountId_18','OpportunityId','OpportunityType','Op_CreatedDate','final_day'] 
time_cols	 	= ['tstart','tstop'] 
increment_cols	= ['s1','s2','s3','s4','s5'] 
other_cols 		= ['stageback','close_change','close_push','close_pullin','amount_change','amount_up','amount_down','amount_per'] 
allday_timeline_df = sdata_timeline_df[baseline_cols + time_cols + increment_cols + other_cols].copy(deep=True)

if (DEBUG == False):
	unique_op = list(set(allday_timeline_df['OpportunityId']))
else:
	unique_op = ['0065000000YpyPf']

for i in range(0,len(unique_op)):

	if ((i % 100) == 99):
		LOG.info("ADD ALL DAYS: Unique Op ... {:>5} of {:>5} ... {:.2f} sec".format(i+1,len(unique_op),time.time()-start) ) 
	
	idx = all_indices_CASE_SENSITIVE(unique_op[i],allday_timeline_df['OpportunityId'])
	output_df = pd.DataFrame(index=range(0,int(allday_timeline_df.ix[idx[0]]['final_day'])+1),columns = [baseline_cols + increment_cols + other_cols]).fillna(0)

	#######################################
	# Update all parameters (not won/loss)	
	#######################################
	output_df['OpportunityId']	 	= unique_op[i]
	output_df['OpportunityType']	= allday_timeline_df.ix[idx[0]]['OpportunityType']
	output_df['Op_CreatedDate']		= allday_timeline_df.ix[idx[0]]['Op_CreatedDate']
	output_df['final_day'] 			= int(allday_timeline_df.ix[idx[0]]['final_day'])
	output_df['time']				= range(0,int(allday_timeline_df.ix[idx[0]]['final_day'])+1) 
	for j in range(0,len(idx)-1): 
		Imin = int(allday_timeline_df.ix[idx[j]]['tstart'])
		Imax = int(allday_timeline_df.ix[idx[j]]['tstop'])

		for k in range(0,len(increment_cols)):
			if (allday_timeline_df.ix[idx[j]][increment_cols[k]] != allday_timeline_df.ix[idx[j+1]][increment_cols[k]]):  
				for m in range(0,(Imax+1-Imin)):
					output_df.loc[Imin+m,increment_cols[k]]	= allday_timeline_df.ix[idx[j]][increment_cols[k]] + m 
			else:
				output_df.loc[range(Imin,(Imax+1)),increment_cols[k]] 		= allday_timeline_df.ix[idx[j]][increment_cols[k]]

		for k in range(0,len(other_cols)):
			output_df.loc[range(Imin,(Imax+1)),other_cols[k]] 	= allday_timeline_df.ix[idx[j]][other_cols[k]]

	### Add final output for 'other_cols'
	for k in range(0,len(other_cols)):
		output_df.loc[int(allday_timeline_df.ix[idx[len(idx)-1]]['tstart']),other_cols[k]] 	= allday_timeline_df.ix[idx[len(idx)-1]][other_cols[k]]

	if (i == 0):
		allday_df = output_df.copy(deep=True)
	else:
		allday_df = allday_df.append(output_df,ignore_index=True)
	
######################
# Add won/loss values
######################
won_df = sdata_timeline_df[['OpportunityId','final_day','won']].groupby(['OpportunityId','final_day']).sum().reset_index()
won_df['won'][won_df.won > 0] = 1
lost_df = sdata_timeline_df[['OpportunityId','final_day','lost']].groupby(['OpportunityId','final_day']).sum().reset_index()
lost_df['lost'][lost_df.lost > 0] = 1

allday_df = pd.merge(allday_df,won_df,'left',left_on=['OpportunityId','time'],right_on=['OpportunityId','final_day'])
allday_df = allday_df.drop('final_day_y',1)
allday_df = allday_df.rename(columns={'final_day_x':'final_day'})
allday_df = pd.merge(allday_df,lost_df,'left',left_on=['OpportunityId','time'],right_on=['OpportunityId','final_day'])
allday_df = allday_df.drop('final_day_y',1)
allday_df = allday_df.rename(columns={'final_day_x':'final_day'})
allday_df['won'] = allday_df['won'].fillna(0)
allday_df['lost'] = allday_df['lost'].fillna(0)

allday_df['stageback'] = [int(x) for x in allday_df['stageback']] 
allday_df['close_change'] = [int(x) for x in allday_df['close_change']] 
allday_df['close_push'] = [int(x) for x in allday_df['close_push']] 
allday_df['close_pullin'] = [int(x) for x in allday_df['close_pullin']] 
allday_df['amount_change'] = [int(x) for x in allday_df['amount_change']] 
allday_df['amount_up'] = [int(x) for x in allday_df['amount_up']] 
allday_df['amount_down'] = [int(x) for x in allday_df['amount_down']] 

output_col = baseline_cols + ['won','lost','time'] + increment_cols + other_cols
if (DEBUG == False):
	allday_df.to_csv('./output/sdata_allday_RNN.csv',columns=output_col,encoding='utf-8')
else:
	allday_df.to_csv('./output/sdata_allday_RNN_TEST.csv',columns=output_col,encoding='utf-8')

