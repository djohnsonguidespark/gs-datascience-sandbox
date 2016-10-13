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

DEBUG = True  # True --> This is for a single / group of unique Account Ids

##############################################
##############################################
##############################################
############### Main Program #################
##############################################
##############################################
##############################################

sdata_timeline_df = pd.read_csv('./output/sdata_all_history_RSF.csv',index_col=[0]).reset_index(drop=True)
sdata_timeline_df['CreatedDate'] = pd.to_datetime(sdata_timeline_df['CreatedDate'])
sdata_timeline_df['Act_CreatedDate'] = pd.to_datetime(sdata_timeline_df['Act_CreatedDate'])

baseline_cols 	= ['AccountId_18','Act_CreatedDate','final_day'] 
time_cols	 	= ['tstart','tstop'] 
increment_cols	= ['s1','s2','s3','s4','s5'] 
other_cols 		= ['stageback','close_change','close_push','close_pullin','amount_change','amount_up','amount_down','amount_per',
					'Nmtg_total','Nmtg_cancel_total','Nmtg_noshow_total','Nmtg_completed_total','Nemail_total',
					'Ncontact_customer','Ncontact_guidespark','Nfillform_total','Nfillform_good_total','Nfillform_bad_total',
					'Ncall_total','Nop_created','Nop_lost'] 


allday_timeline_df = sdata_timeline_df[baseline_cols + time_cols + increment_cols + other_cols].copy(deep=True)
final_day_df = allday_timeline_df[['AccountId_18','tstop']].groupby('AccountId_18').max().reset_index()

for iii in range(0,len(final_day_df)):
	LOG.info('{:>4} ... {:.2f} sec'.format(iii,time.time()-start) )
	idx = all_indices_CASE_SENSITIVE(final_day_df.ix[iii]['AccountId_18'],allday_timeline_df['AccountId_18'])
	output_df = pd.DataFrame(index=range(0,int(final_day_df.ix[iii]['tstop'])+1),columns = [baseline_cols + increment_cols + other_cols])
  
	#######################################
	# Update all parameters (not won/loss)  
	#######################################
	output_df['AccountId_18']       = final_day_df.ix[iii]['AccountId_18']
	output_df['Act_CreatedDate']    = allday_timeline_df.ix[idx[0]]['Act_CreatedDate']
	output_df['tstop']              = int(allday_timeline_df.ix[idx[0]]['tstop'])
	output_df['final_day']          = int(allday_timeline_df.ix[idx[0]]['final_day'])
	output_df['time']               = range(0,int(max(allday_timeline_df.ix[idx]['tstop']))+1)

	if (iii == 0):
		allday_df = output_df.copy(deep=True)
	else:
		allday_df = allday_df.append(output_df,ignore_index=True)
	
sys.exit()

if (DEBUG == False):
	unique_account = list(set(allday_timeline_df['AccountId_18']))
else:
	unique_account = ['00138000015y2EXAAY']

#tmp_df = all_timeline_df.groupby(['AccountId_18'])[out_cols[iii]].transform(lambda grp: grp.fillna(method='ffill'))

for i in range(0,len(unique_account)):

	#if ((i % 100) == 99):
	LOG.info("ADD ALL DAYS: Unique Op ... {:>5} of {:>5} ... {:.2f} sec".format(i+1,len(unique_account),time.time()-start) ) 
	
	idx = all_indices_CASE_SENSITIVE(unique_account[i],allday_timeline_df['AccountId_18'])
	output_df = pd.DataFrame(index=range(0,int(max(allday_timeline_df.ix[idx]['tstop']))+1),columns = [baseline_cols + increment_cols + other_cols]).fillna(0)

	#######################################
	# Update all parameters (not won/loss)	
	#######################################
	output_df['AccountId_18']	 	= unique_account[i]
	output_df['Act_CreatedDate']	= allday_timeline_df.ix[idx[0]]['Act_CreatedDate']
	output_df['tstop'] 				= int(allday_timeline_df.ix[idx[0]]['tstop'])
	output_df['final_day'] 			= int(allday_timeline_df.ix[idx[0]]['final_day'])
	output_df['time']				= range(0,int(max(allday_timeline_df.ix[idx]['tstop']))+1) 

	if (i == 0):
		allday_df = output_df.copy(deep=True)
	else:
		allday_df = allday_df.append(output_df,ignore_index=True)
	
#	for j in range(0,len(idx)): 
#		Imin = int(allday_timeline_df.ix[idx[j]]['tstart'])
#		Imax = int(allday_timeline_df.ix[idx[j]]['tstop'])
#
#		if (j < (len(idx)-1)):
#			for k in range(0,len(increment_cols)):
#				if (allday_timeline_df.ix[idx[j]][increment_cols[k]] != allday_timeline_df.ix[idx[j+1]][increment_cols[k]]):  
#					for m in range(0,(Imax+1-Imin)):
#						output_df.loc[Imin+m,increment_cols[k]]	= allday_timeline_df.ix[idx[j]][increment_cols[k]] + m 
#				else:
#					output_df.loc[range(Imin,(Imax+1)),increment_cols[k]] = allday_timeline_df.ix[idx[j]][increment_cols[k]]
#
#			for k in range(0,len(other_cols)):
#				output_df.loc[range(Imin,(Imax+1)),other_cols[k]] 	= allday_timeline_df.ix[idx[j]][other_cols[k]]
#		else:
#			### Add final output for when tstart != tstop
#			for k in range(0,len(increment_cols)):
#				if (allday_timeline_df.ix[idx[j-1]][increment_cols[k]] != allday_timeline_df.ix[idx[j]][increment_cols[k]]):  
#					for m in range(0,(Imax+1-Imin)):
#						output_df.loc[Imin+m,increment_cols[k]]	= allday_timeline_df.ix[idx[j-1]][increment_cols[k]] + m 
#				else:
#					output_df.loc[range(Imin,(Imax+1)),increment_cols[k]] = allday_timeline_df.ix[idx[j-1]][increment_cols[k]]
#
#			for k in range(0,len(other_cols)):
#				output_df.loc[range(Imin,(Imax+1)),other_cols[k]] 	= allday_timeline_df.ix[idx[j-1]][other_cols[k]]
#
#	### Add final output for 'other_cols'
#	for k in range(0,len(other_cols)):
#		output_df.loc[int(allday_timeline_df.ix[idx[len(idx)-1]]['tstop']),other_cols[k]] 	= allday_timeline_df.ix[idx[len(idx)-1]][other_cols[k]]
#
#	if (i == 0):
#		allday_df = output_df.copy(deep=True)
#	else:
#		allday_df = allday_df.append(output_df,ignore_index=True)
	

######################
# Add won/loss values
######################
won_df = sdata_timeline_df[['AccountId_18','tstop','won']].groupby(['AccountId_18','tstop']).sum().reset_index()
won_df['won'][won_df.won > 0] = 1
lost_df = sdata_timeline_df[['AccountId_18','tstop','lost']].groupby(['AccountId_18','tstop']).sum().reset_index()
lost_df['lost'][lost_df.lost > 0] = 1

allday_df = pd.merge(allday_df,won_df,'left',left_on=['AccountId_18','time'],right_on=['AccountId_18','tstop'])
allday_df = allday_df.drop('tstop_y',1)
allday_df = allday_df.rename(columns={'tstop_x':'tstop'})
allday_df = pd.merge(allday_df,lost_df,'left',left_on=['AccountId_18','time'],right_on=['AccountId_18','tstop'])
allday_df = allday_df.drop('tstop_y',1)
allday_df = allday_df.rename(columns={'tstop_x':'tstop'})
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
	allday_df.to_csv('./output/sdata_act_allday_RNN.csv',columns=output_col,encoding='utf-8')
else:
	allday_df.to_csv('./output/sdata_act_allday_RNN_TEST_NEW.csv',columns=output_col,encoding='utf-8')

