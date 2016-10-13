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

CASE = int(sys.argv[1])

ACT_START = 10*(CASE-1)
ACT_END = 10*(CASE)

if (ACT_START == 0):
	ACT_START = 1

##############################################
##############################################
##############################################
############### Main Program #################
##############################################
##############################################
##############################################

TIME_BIN_DELTA = 14

sdata_timeline_df = pd.read_csv('./output/sdata_all_history_RSF.csv')
sdata_timeline_df = sdata_timeline_df.drop('Unnamed: 0',1)
sdata_timeline_df['CreatedDate'] = pd.to_datetime(sdata_timeline_df['CreatedDate'])
sdata_timeline_df['Act_CreatedDate'] = pd.to_datetime(sdata_timeline_df['Act_CreatedDate'])

unique_account = list(set(sdata_timeline_df['AccountId_18']))

if (len(unique_account) == 1):
	print_op = unique_account
else:
	print_op = ['0063800000apZD9']

update_cols = ['s1','s2','s3','s4','s5','s6','s7']
print_cols = ['AccountId_18','OpportunityId','won','lost','CreatedDate','tstart','tstop','Act_CreatedDate','final_day','event','OpportunityType'] + update_cols + ['stageback']

won_loss_df = sdata_timeline_df[['AccountId_18','tstop','won','lost']][(sdata_timeline_df['won'] > 0) | (sdata_timeline_df['lost'] > 0)]

for ppp in range(ACT_START,ACT_END):
	new_datetime = cur_datetime - timedelta(days=TIME_BIN_DELTA*ppp) - timedelta(hours = 8) ## deals with UTC vs PST 

	LOG.info('Case {:>3}: {:>3} of {:>3} ... Date: {:>8} ... {:.2f} sec'.format(CASE,ppp,ACT_END,new_datetime.strftime('%Y%m%d'),time.time()-start ) )

	cv_df = pd.DataFrame(columns = sdata_timeline_df.columns)
	for i in range(0,len(unique_account)):

		if ((i % 500) == 499):
			LOG.info("Case {:>3} STAGE DURATION: Unique Op ... {:>5} of {:>5} ... {:.2f} sec".format(CASE,i+1,len(unique_account),time.time()-start))
	
		idx = all_indices_CASE_SENSITIVE(unique_account[i],sdata_timeline_df['AccountId_18'])

		test_before_df = sdata_timeline_df.ix[idx][(sdata_timeline_df['CreatedDate'] <= new_datetime)]	
		test_after_df = sdata_timeline_df.ix[idx][(sdata_timeline_df['CreatedDate'] > new_datetime)]	

		test = 0 
		if (len(test_before_df) > 0):
			prev_before_idx = test_before_df.index[len(test_before_df)-2]  
			max_before_idx = test_before_df.index[len(test_before_df)-1]  

			###################################
			# If you fall between two dates
			###################################
			if (len(test_after_df) > 0):
				min_after_idx = min(test_after_df.index)  
	
				test_before_df.loc[max_before_idx,'tstop'] = (new_datetime - test_before_df.ix[max_before_idx]['Act_CreatedDate']).days
				test_before_df.loc[:,'final_day'] = test_before_df.ix[max_before_idx]['tstop']
				test_before_df.loc[max_before_idx,'won'] = 0
				test_before_df.loc[max_before_idx,'lost'] = 0

				#### FOR RSF ONLY
				test_before_df = test_before_df.append(test_before_df.ix[max_before_idx],ignore_index=True)

				## Get index
				cur_idx = len(test_before_df)-1
			
				## Update data 
				for j in range(0,len(update_cols)):
					if (test_before_df.ix[cur_idx][update_cols[j]] != test_after_df.ix[min_after_idx][update_cols[j]]):   
						test_before_df.loc[cur_idx,update_cols[j]] = test_before_df.ix[cur_idx-1][update_cols[j]] + (new_datetime - test_before_df.ix[cur_idx]['CreatedDate']).days

				test_before_df.loc[cur_idx,'tstart'] = int(test_before_df.ix[cur_idx]['final_day'])
				test_before_df.loc[cur_idx,'CreatedDate'] = test_before_df.ix[cur_idx]['Act_CreatedDate'] + timedelta(days = int(test_before_df.ix[cur_idx]['final_day']))
	
				cv_df = cv_df.append(test_before_df,ignore_index=True)

			##########################################
			# Between final CreatedDate and final_day 
			##########################################
			elif (len(test_after_df) == 0):
				final_date = test_before_df.ix[max_before_idx]['CreatedDate']
				tdelta_days = test_before_df.ix[max_before_idx]['tstop'] - test_before_df.ix[max_before_idx]['tstart']
				if (new_datetime < (final_date + timedelta(days=tdelta_days)) ):
		
					test_before_df.loc[max_before_idx,'won'] = 0
					test_before_df.loc[max_before_idx,'lost'] = 0
					test_before_df.loc[max_before_idx,'tstop'] = (new_datetime - test_before_df.ix[max_before_idx]['Act_CreatedDate']).days
					test_before_df.loc[:,'final_day'] = test_before_df.ix[max_before_idx]['tstop']

				cv_df = cv_df.append(test_before_df,ignore_index=True)

	cv_idx = all_indices_CASE_SENSITIVE(print_op[0],cv_df['AccountId_18'])
	if (len(cv_idx) > 0):
		idx = all_indices_CASE_SENSITIVE(print_op[0],sdata_timeline_df['AccountId_18'])
		LOG.info("new_datetime = {}\n{}".format(new_datetime,sdata_timeline_df.ix[idx][print_cols]))
		LOG.info("new_datetime = {}\n{}".format(new_datetime,cv_df.ix[cv_idx][print_cols]))

	cv_df.to_csv('./output/sdata_all_history_RSF_' + new_datetime.strftime('%Y%m%d') + '.csv',encoding='utf-8')

