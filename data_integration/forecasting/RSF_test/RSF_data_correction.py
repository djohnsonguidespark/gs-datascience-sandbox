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
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta

sys.path.insert(0,'/home/analytics/analytics_sandbox/python_libs');
import common_libs as cm 

# Logging
import log_libs as log
LOG = log.init_logging()

start = time.time()

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',400)

NFILES = 70
##############################################
##############################################
##############################################
############### Main Program #################
##############################################
##############################################
##############################################

def main():
	cur_datetime = datetime.date(datetime.now() - timedelta(days=1))  # Remove timedelta later

	for i in range(0,NFILES):
		file_date = cur_datetime - timedelta(days = i*14)
		file_name = 'sdata_all_history_RSFcorrected_' + file_date.strftime('%Y%m%d') + '.csv' 
		LOG.info('i = {:>3} of {:>3} ... Output File: {} . {:.2f} sec'.format(i,NFILES-1,file_name,time.time()-start ) )
 
		input_df = pd.read_csv('../../output/sdata_all_history_RSF_' + file_date.strftime('%Y%m%d') + '.csv',index_col=[0])

		##### Set all op_cols to 0 for accounts that do not have an opportunity yet
		update_cols = ['close_change','close_push','close_pullin','stageback','amount_change','amount_up','amount_down','amount_per',]

		for iii in range(0,len(update_cols)):
			input_df[update_cols[iii]] = input_df[update_cols[iii]].fillna(0)

		##### Remove NaN Stage Durations
		input_df = input_df[pd.isnull(input_df['s1']) == False].reset_index(drop=True) 

		###########################
		# Filter after all WINS
		###########################
		##### Find won initial ops
		wonlost_df = input_df['AccountId_18'][(input_df['won'] == 1)]

		##### Filter out all points after won
		won_tstop_df = input_df[['AccountId_18','tstop']][input_df['won'] == 1].groupby('AccountId_18').min()
		won_tstop_df = won_tstop_df.rename(columns={'tstop':'won_tstop'})
		input_df = pd.merge(input_df,won_tstop_df,'left',left_on='AccountId_18',right_index=True)
 
		####### Filter values #######
		input_df = input_df[(input_df['tstop'] <= input_df['won_tstop']) | (pd.isnull(input_df['won_tstop']) == True)]

		input_df = input_df.drop(['won_tstop'],1)

		###########################
		# Filter after all LOSSES
		###########################
		##### Find won initial ops
		#wonlost_df = input_df['AccountId_18'][(input_df['lost'] >= 1)]

		##### Filter out all points after won
		#lost_tstop_df = input_df[['AccountId_18','tstop']][input_df['lost'] >= 1].groupby('AccountId_18').min()
		#lost_tstop_df = lost_tstop_df.rename(columns={'tstop':'lost_tstop'})
		#input_df = pd.merge(input_df,lost_tstop_df,'left',left_on='AccountId_18',right_index=True)
 
		####### Filter values #######
		#input_df = input_df[(input_df['tstop'] <= input_df['lost_tstop']) | (pd.isnull(input_df['lost_tstop']) == True)]

		#input_df = input_df.drop(['lost_tstop'],1)

		input_df.to_csv('./input/' + file_name,encoding='utf-8')

if __name__ == "__main__":
	main()

