#! /usr/bin/env python

from __future__ import unicode_literals
import sys
import re 
import csv
import MySQLdb as mdb
import pandas as pd
from openpyxl import load_workbook
import time
from datetime import *
import collections
from simple_salesforce import Salesforce
from dateutil.relativedelta import relativedelta

sys.path.insert(0,'/home/analytics/analytics_sandbox/python_libs');
from churn_libs import *
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

start = time.time()

UPDATE_SFDC_ACTIVITY = False
FILTER_LIBRARY_SUBSCRIPTION_ACCOUNTS = False 

NVIDEO = True 
UPSELL_DOWNTICK_SWAP = True
CSM_STATS  = True
NPS_STATS = True
G2_FIRST_VIEW = True
G2_ADMIN_USAGE = True
G2_USAGE = True
G2_EDITS = True
WF_DELIVERY_TIME = True
SFDC_ACTIVITY = True

LIBRARY_COMPLETION_THRESHOLD = 90 
CSM_CHANGE_THRESHOLD = 14 
#BASE_DATE = 'initial_op_date'
#BASE_DATE = 'MSA_Effective_Date__c'
BASE_DATE = 'actual_MSA_Effective_Date'
TEST_CASE = False 
SINGLE_ACCOUNT = '0015000000f9lMA'

REMOVE_ACCOUNTS = [
'0015000000rfVsc',
'0015000000p6idX',
'0015000000lhAZv',
'0015000000l071u',
'0015000000muSBa',
'0015000000k0P4T'] 

#0015000000rfVsc	Remove ... g1 only
#0015000000p6idX	CTI ... unclear what account on g2
#0015000000lhAZv	On the border ... g1 only
#0015000000l071u	Jackson Family ... g1 only
#0015000000muSBa	Realty in motion ... g1 only
#0015000000k0P4T	Reputation.com ... never started

NON_EVERGREEN_ACCOUNTS = [
'0015000000f9lMWAAY',
'0015000000qPmTxAAK',
'0015000000ryBOUAA2',
'0015000000k3yHQAAY',
'0015000000stnMMAAY',
'0015000000styi7AAA',
'0015000000stxbiAAA',
'0015000000fBVQoAAO',
'0015000000f9lMsAAI',
'0015000000f9lLEAAY',
'0015000000su9CMAAY',
]

#def filelog(str_out):
#	printf("[%s]: %s\n",str_out)

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

start = time.time()

####################################################################
####################################################################
####################################################################
####################################################################
####################################################################
######################## 1) Read data ##############################
####################################################################
####################################################################
####################################################################
####################################################################
####################################################################

printf("\n\n")
printf("[churn_timeseries.py]: ##########################################################\n")
printf("[churn_timeseries.py]: ####################### 1) Read Data #####################\n")
printf("[churn_timeseries.py]: ##########################################################\n\n")

######################################
# Read SFDC account-level data 
# Get MSA Effective Date
######################################

########## SIMPLE SALESFORCE ##############
#sf = Salesforce(username='djohnson@guidespark.com', password=pwd,security_token=token)
#account_df = read_sfdc_accounts(sf)
#account_msa_df = account_df[['Id','AccountId_18','MSA_Effective_Date']]

account_df = pd.read_csv('./output/account_df.csv')
sf_product_df = pd.read_csv('./output/sf_product_df.csv')
solution_lookup_df = sf_product_df[['Product2Id','PricebookEntry','Video_Category__c']].drop_duplicates().reset_index(drop=True) 
solution_lookup_df.loc[all_indices_CASE_SENSITIVE('Work-Life',solution_lookup_df['Video_Category__c']),'Video_Category__c'] = 'Benefits'
solution_lookup_df.loc[all_indices_CASE_SENSITIVE('Onboarding',solution_lookup_df['Video_Category__c']),'Video_Category__c'] = 'Benefits'
solution_lookup_df.loc[all_indices_CASE_SENSITIVE('Career Development',solution_lookup_df['Video_Category__c']),'Video_Category__c'] = 'Performance Management'
solution_lookup_df.loc[all_indices_CASE_SENSITIVE('Systems Training',solution_lookup_df['Video_Category__c']),'Video_Category__c'] = 'Compliance'
solution_df = solution_lookup_df[['Video_Category__c']].drop_duplicates().reset_index(drop=True).rename(columns={'Video_Category__c':'solution'})

sf_product_lookup_df = sf_product_df[['AccountId','sf_account_name','LineItemId']].drop_duplicates().reset_index(drop=True)
#cancellation_notice_df = pd.read_csv('./output/Contraction_5.06.16_DKJ.csv')
cancellation_notice_df = pd.read_csv('./output/Contraction_6.06.16_DKJ.csv')
sfdc_opportunity_history_df = pd.read_csv('./output/sfdc_opportunity_history_' + cur_datetime.strftime('%Y%m%d') + '.csv')
account_product_line_df = sf_product_df[['AccountId','Branding2__c']].groupby(['AccountId']).first().reset_index()
account_msa_df = sf_product_df[['AccountId','MSA_Effective_Date__c']].drop_duplicates().reset_index(drop=True).rename(columns={'AccountId':'sfdc'})
account_msa_df['MSA_Effective_Date__c'] = pd.to_datetime(account_msa_df['MSA_Effective_Date__c']) 
sfdc_activity_df = pd.read_csv('../sfdc_activity.csv').drop('Unnamed: 0',1)
sfdc_activity_df['ActivityDate'] = pd.to_datetime(sfdc_activity_df['ActivityDate'])

for i in range(0,len(cancellation_notice_df)):
	try:
		cancellation_notice_df.loc[i,'sfdc'] = cancellation_notice_df.ix[i]['sfdc'][0:15]
	except Exception as e:
		printf("[churn_timeseries.py]: Line %s: i = %4d: %s\n",sys.exc_traceback.tb_lineno,i,e)
		cancellation_notice_df.loc[i,'sfdc'] = None

account_df['MSA_Effective_Date__c'] = pd.to_datetime(account_df['MSA_Effective_Date__c']) 
account_df['Cancellation_Notice_Received__c'] = pd.to_datetime(account_df['Cancellation_Notice_Received__c']) 

###########################################################
# Add new churned customers
# POST 2016-05 fix ... now all customers tracked in SFDC
###########################################################
cancellation_notice_POST052016_df = account_df.ix[pd.notnull(account_df['Cancellation_Notice_Received__c']) == True][['Name','Id','Yearly_Client_ARR__c','Cancellation_Notice_Received__c']].reset_index(drop=True)
cancellation_notice_POST052016_df = cancellation_notice_POST052016_df.ix[cancellation_notice_POST052016_df['Cancellation_Notice_Received__c'] > '2016-05-31 11:59:59'].reset_index(drop=True) 
cancellation_notice_POST052016_df['Yearly_Client_ARR__c'] = [-x for x in cancellation_notice_POST052016_df['Yearly_Client_ARR__c']]
cancellation_notice_POST052016_df = cancellation_notice_POST052016_df.rename(columns={'Name':'AccountName','Id':'sfdc','Yearly_Client_ARR__c':'Total_Lost_ARR','Cancellation_Notice_Received__c':'Cancellation_Notice_Received'})
tmp_df = len(cancellation_notice_POST052016_df) * ['2020-01-01 00:00:00']
cancellation_notice_POST052016_df = pd.merge(cancellation_notice_POST052016_df,pd.DataFrame(tmp_df),'left',left_index=True,right_index=True).rename(columns={0:'ChurnDate'})
cancellation_notice_POST052016_df['ChurnDate'] = pd.to_datetime(cancellation_notice_POST052016_df['ChurnDate'])

cancellation_notice_df = cancellation_notice_df.append(cancellation_notice_POST052016_df,ignore_index=True).reset_index(drop=True)

########################
# Read in product data 
########################

try:
	library_completion_df = pd.read_csv('./output/library_completion_' + cur_datetime.strftime('%Y%m%d') + '.csv')
except Exception as e:
	printf("[churn_timeseries.py]: Line %s: i = %4d: %s\n",sys.exc_traceback.tb_lineno,i,e)
	printf("[churn_timeseries.py]: PROGRAM WILL TERMINATE\n")
	sys.exit()

library_completion_df['AccountId'] = [x[0:15] for x in library_completion_df.AccountId_18]

GLOBAL_product_progression_df = pd.read_csv('./output/GLOBAL_product_progression_' + cur_datetime.strftime('%Y%m%d') + '.csv',encoding='utf-8').drop('Unnamed: 0',1)

GLOBAL_product_progression_TIME_df = GLOBAL_product_progression_df[(GLOBAL_product_progression_df['Initial_Close_Date'] != 'op_id')].reset_index(drop=True)
#GLOBAL_product_progression_TIME_df = GLOBAL_product_progression_TIME_df[(GLOBAL_product_progression_TIME_df['Initial_Close_Date'] != 'op_type')].reset_index(drop=True)
GLOBAL_product_progression_TIME_df = GLOBAL_product_progression_TIME_df[(GLOBAL_product_progression_TIME_df['Initial_Close_Date'] != 'close_date')].reset_index(drop=True)

######################
# Add solutions
######################
GLOBAL_product_progression_TIME_df = pd.merge(GLOBAL_product_progression_TIME_df,solution_lookup_df[['Product2Id','Video_Category__c']],'left',left_on='Product2Id',right_on='Product2Id').rename(columns={'Video_Category__c':'solution'})

timeseries_df = pd.DataFrame(columns = ['cur_date','event','account_name','sfdc','account_id','video_id'])
cur_date = []
event = []
library_size = []
account_name = []
sfdc = []
account_id = []
initial_op_date = []
video_id = []
op_type = []
downtick_count = []
cur_titles = []
cur_products = []
titles_lost = []
products_lost = []
solution_count = []
benefits_count = []
compensation_count = []
fwa_count = []
perf_mgmt_count = []
compliance_count = []
accounts = list(set(GLOBAL_product_progression_TIME_df['AccountId_18']))

for i in range(0,len(accounts)):

	if ((i % 100) == 99):
		printf("[churn_timeseries.py]: Account ... %5s of %5s ... %.2f sec\n",i+1,len(accounts),time.time()-start)

	tmp_df = GLOBAL_product_progression_TIME_df.ix[all_indices_CASE_SENSITIVE(accounts[i],GLOBAL_product_progression_TIME_df['AccountId_18'])].reset_index(drop=True)

	idx_close_date = tmp_df[(tmp_df['Initial_Close_Date'] == 'subscription_start_date')].index[0]
	colTOTAL = len(tmp_df.columns)
	col0 = tmp_df.columns.get_loc('op_000')

	op_df = pd.DataFrame(tmp_df.ix[idx_close_date]).reset_index()
	op_df = op_df[(op_df['index'].str.contains('op')) & (~op_df['index'].str.contains('Product'))]
	colN = min(all_indices_CASE_SENSITIVE('subscription_start_date',op_df[idx_close_date])) + col0 

	#####################
	# Fill in dataframe
	#####################
	Ndowntick = 0
	str_cur_titles  = ""
	str_cur_products  = ""
	str_titles_lost  = ""
	str_product_lost = ""
	for j in range(col0,colN):
		cur_col = tmp_df.columns[j]
		#timeseries_df.append(pd.DataFrame([tmp_df.ix[0][cur_col],'product_change','',tmp_df.ix[0]['AccountId_18'],'',tmp_df.ix[3:len(tmp_df)][cur_col].count() ]),ignore_index=True)

		cur_solution_df = tmp_df.ix[3:len(tmp_df)][['solution',cur_col]]
		cur_solution_NoNA_df = cur_solution_df[pd.notnull(cur_solution_df[cur_col])==True]
		cur_solution_GROUP_df = cur_solution_NoNA_df.groupby('solution').agg({cur_col:"count"}).reset_index()
		cur_solution_ALL_df = pd.merge(solution_df,cur_solution_GROUP_df,'left',left_on='solution',right_on='solution').fillna(0)

		titles_new = pd.isnull(tmp_df.ix[3:len(tmp_df)][cur_col])			
		pricebook_list = []
		product_list = []
		for k in range(0,len(titles_new)):
			idx = list(titles_new.index)[k]
			if (titles_new[idx] == False):
				pricebook_list.append(tmp_df.ix[idx]['PricebookEntry'])
				product_list.append(tmp_df.ix[idx]['Product2Id'])

		if (j == col0):
			str_cur_titles   = str(';'.join(pricebook_list))
			str_cur_products = str(';'.join(product_list)) 
		else:
			str_cur_titles   = str_cur_titles + ';' + str(';'.join(pricebook_list))
			str_cur_products = str_cur_products + ';' + str(';'.join(product_list)) 

		if ('Downtick' in tmp_df.ix[0][cur_col] or 'Cancel' in tmp_df.ix[0][cur_col]):
			titles_old = pd.isnull(tmp_df.ix[3:len(tmp_df)][tmp_df.columns[j-1]])
			idx_change = []
			pricebook_change = []
			product_change = []
			for k in range(0,len(titles_new)):
				idx = list(titles_new.index)[k]
				if (titles_new[idx] == True and titles_old[idx] == False):
					idx_change.append(idx)
					pricebook_change.append(tmp_df.ix[idx]['PricebookEntry'])
					product_change.append(tmp_df.ix[idx]['Product2Id'])
			if (len(str_titles_lost) == 0):
				str_titles_lost = str(';'.join(pricebook_change))
				str_products_lost = str(';'.join(product_change)) 
			else:
				str_titles_lost = str_titles_lost + ';' + str(';'.join(pricebook_change))
				str_products_lost = str_products_lost + ';' + str(';'.join(product_change)) 
			titles_lost.append(str_titles_lost)
			products_lost.append(str_products_lost)
			Ndowntick = Ndowntick + len(product_change)
		else:
			titles_lost.append(None)
			products_lost.append(None)

		solution_count.append(len(cur_solution_GROUP_df))
		benefits_count.append( int(cur_solution_ALL_df[cur_solution_ALL_df['solution'] == 'Benefits'][cur_col]) )
		compensation_count.append( int(cur_solution_ALL_df[cur_solution_ALL_df['solution'] == 'Compensation'][cur_col]) )
		fwa_count.append( int(cur_solution_ALL_df[cur_solution_ALL_df['solution'] == 'Financial Wellness'][cur_col]) )
		perf_mgmt_count.append( int(cur_solution_ALL_df[cur_solution_ALL_df['solution'] == 'Performance Management'][cur_col]) )
		compliance_count.append( int(cur_solution_ALL_df[cur_solution_ALL_df['solution'] == 'Compliance'][cur_col]) )

		cur_titles.append(str_cur_titles)
		cur_products.append(str_cur_products)
		downtick_count.append(Ndowntick)
		op_type.append(tmp_df.ix[0][cur_col])
		initial_op_date.append(tmp_df.ix[1][col0])
		cur_date.append(tmp_df.ix[1][cur_col])
		event.append('library_size_change')
		library_size.append(tmp_df.ix[3:len(tmp_df)][cur_col].count())
		account_name.append('')
		sfdc.append(tmp_df.ix[0]['AccountId_18'][0:15])
		account_id.append('')
		video_id.append('')
	
library_subscription_df = pd.merge(pd.DataFrame(sfdc).rename(columns={0:'sfdc'}),pd.DataFrame(event).rename(columns={0:'event'}),'left',left_index=True,right_index=True)
library_subscription_df = pd.merge(library_subscription_df,account_msa_df[['sfdc','MSA_Effective_Date__c']],'left',left_on='sfdc',right_on='sfdc')
library_subscription_df = pd.merge(library_subscription_df,pd.DataFrame(initial_op_date).rename(columns={0:'initial_op_date'}),'left',left_index=True,right_index=True)
library_subscription_df = pd.merge(library_subscription_df,pd.DataFrame(cur_date).rename(columns={0:'cur_date'}),'left',left_index=True,right_index=True)
library_subscription_df = pd.merge(library_subscription_df,pd.DataFrame(op_type).rename(columns={0:'op_type'}),'left',left_index=True,right_index=True)
library_subscription_df = pd.merge(library_subscription_df,pd.DataFrame(downtick_count).rename(columns={0:'downtick_count'}),'left',left_index=True,right_index=True)
library_subscription_df = pd.merge(library_subscription_df,pd.DataFrame(titles_lost).rename(columns={0:'titles_lost'}),'left',left_index=True,right_index=True)
library_subscription_df = pd.merge(library_subscription_df,pd.DataFrame(products_lost).rename(columns={0:'products_lost'}),'left',left_index=True,right_index=True)
library_subscription_df = pd.merge(library_subscription_df,pd.DataFrame(cur_titles).rename(columns={0:'cur_titles'}),'left',left_index=True,right_index=True)
library_subscription_df = pd.merge(library_subscription_df,pd.DataFrame(cur_products).rename(columns={0:'cur_products'}),'left',left_index=True,right_index=True)
library_subscription_df = pd.merge(library_subscription_df,pd.DataFrame(solution_count).rename(columns={0:'Nsolution'}),'left',left_index=True,right_index=True)
library_subscription_df = pd.merge(library_subscription_df,pd.DataFrame(benefits_count).rename(columns={0:'Nbenefits'}),'left',left_index=True,right_index=True)
library_subscription_df = pd.merge(library_subscription_df,pd.DataFrame(compensation_count).rename(columns={0:'Ncompensation'}),'left',left_index=True,right_index=True)
library_subscription_df = pd.merge(library_subscription_df,pd.DataFrame(fwa_count).rename(columns={0:'Nfwa'}),'left',left_index=True,right_index=True)
library_subscription_df = pd.merge(library_subscription_df,pd.DataFrame(perf_mgmt_count).rename(columns={0:'Nperf_mgmt'}),'left',left_index=True,right_index=True)
library_subscription_df = pd.merge(library_subscription_df,pd.DataFrame(compliance_count).rename(columns={0:'Ncompliance'}),'left',left_index=True,right_index=True)
library_subscription_df = pd.merge(library_subscription_df,pd.DataFrame(library_size).rename(columns={0:'library_size'}),'left',left_index=True,right_index=True)
library_subscription_df = pd.merge(library_subscription_df,library_completion_df[['AccountId','churn_int','Cambria_algo_delta']],'left',left_on='sfdc',right_on='AccountId').drop('AccountId',1)
library_subscription_df['cur_date'] = pd.to_datetime(library_subscription_df['cur_date']) 
library_subscription_df['initial_op_date'] = pd.to_datetime(library_subscription_df['initial_op_date']) 

##################################################################################################
# Need to say upsell occurred, but library count is not added until LIBRARY_COMPLETION_THRESHOLD
##################################################################################################
#for i in range(0,len(library_subscription_df)):
#	if ('Upsell' in library_subscription_df.ix[i]['op_type']):
#		library_subscription_df.loc[i,'cur_date'] = library_subscription_df.ix[i]['cur_date'] + timedelta(days = LIBRARY_COMPLETION_THRESHOLD)
#
#library_subscription_df = library_subscription_df.sort(['sfdc','cur_date']).reset_index(drop=True)

####################################
# Set MSA_Effective_Date
####################################
actual_msa_date = []
for i in range(0,len(library_subscription_df)):
	if (library_subscription_df.ix[i]['MSA_Effective_Date__c'] > library_subscription_df.ix[i]['initial_op_date']):	
		actual_msa_date.append(library_subscription_df.ix[i]['initial_op_date'])
	else:	
		actual_msa_date.append(library_subscription_df.ix[i]['MSA_Effective_Date__c'])
library_subscription_df[BASE_DATE] = actual_msa_date

account_msa_df = pd.merge(account_msa_df,library_subscription_df[['sfdc',BASE_DATE,'initial_op_date']] \
										.copy(deep=True).drop_duplicates().reset_index(drop=True), \
										'left',left_on='sfdc',right_on='sfdc')

##############################################################
# Remove accounts that DO NOT MATCH between Algo and Cambria
##############################################################


library_subscription_df = pd.merge(library_subscription_df,cancellation_notice_df[['sfdc','Total_Lost_ARR','Cancellation_Notice_Received','ChurnDate']],'left',left_on='sfdc',right_on='sfdc')
library_subscription_df['Cancellation_Notice_Received'] = pd.to_datetime(library_subscription_df['Cancellation_Notice_Received']) 
library_subscription_df['ChurnDate'] = pd.to_datetime(library_subscription_df['ChurnDate']) 

NON_EVERGREEN_ACCOUNTS = [x[0:15] for x in NON_EVERGREEN_ACCOUNTS]

library_subscription_df['non_evergreen'] = [0] * len(library_subscription_df)
library_subscription_df.loc[library_subscription_df[library_subscription_df['sfdc'].isin(NON_EVERGREEN_ACCOUNTS)].index,'non_evergreen'] = 1

if (FILTER_LIBRARY_SUBSCRIPTION_ACCOUNTS == True):
	#library_subscription_df = library_subscription_df[(library_subscription_df['Cambria_algo_delta'] == 0)].reset_index(drop=True)  
	library_subscription_df = library_subscription_df[((library_subscription_df['Cambria_algo_delta'] >= -2) & (library_subscription_df['Cambria_algo_delta'] <= 2)) \
														| (library_subscription_df['sfdc'].isin(NON_EVERGREEN_ACCOUNTS))].reset_index(drop=True)  

library_subscription_df['lifetime_day'] = calc_lifetime_day(library_subscription_df,'cur_date',BASE_DATE)

########################################################################################################
# Compute cur_day for each account
# 1) If Cancellation and library_size = 0, then use Cancellation day (means the account has churned)
# Else
# 2) Use cur_day
########################################################################################################
#cancelled_accounts_df = library_subscription_df.ix[(library_subscription_df.op_type == 'Cancellation') & (library_subscription_df.library_size == 0)].copy(deep=True).reset_index()
cancelled_accounts_NOTICE_df = library_subscription_df.ix[pd.notnull(library_subscription_df.Cancellation_Notice_Received)][['sfdc','Cancellation_Notice_Received']].drop_duplicates().copy(deep=True).reset_index(drop=True)

##################################
# Add churn date flag
##################################	
library_subscription_df['churn'] = [0] * len(library_subscription_df)
#library_subscription_df.loc[cancelled_accounts_df['index'],'churn'] = 1 

final_day = []
library_subscription_df['final_day'] = [None] * len(library_subscription_df) 
library_accounts = list(set(library_subscription_df.sfdc))
for i in range(0,len(library_accounts)):
	idx = all_indices_CASE_SENSITIVE(library_accounts[i],library_subscription_df.sfdc)
	base_day = library_subscription_df.ix[idx[0]][BASE_DATE] 
#	if (len(all_indices_CASE_SENSITIVE(library_accounts[i],cancelled_accounts_df.sfdc)) != 0):
#		final_idx = all_indices_CASE_SENSITIVE(library_accounts[i],cancelled_accounts_df.sfdc)[0]
#		try:
#			library_subscription_df.loc[idx,'final_day'] = cancelled_accounts_df.ix[final_idx]['lifetime_day']
#		except:
#			library_subscription_df.loc[idx,'final_day'] = None 
#	else:    #### Make final day = the current day since they have not churned
#		try:
#			library_subscription_df.loc[idx,'final_day'] = (cur_datetime - base_day).days 
#		except:
#			library_subscription_df.loc[idx,'final_day'] = None
	### Override with the cancel notices
	if (len(all_indices_CASE_SENSITIVE(library_accounts[i],cancelled_accounts_NOTICE_df.sfdc)) != 0):
		cancel_idx = all_indices_CASE_SENSITIVE(library_accounts[i],cancelled_accounts_NOTICE_df.sfdc)[0]
		try:
			final_day = cancelled_accounts_NOTICE_df.ix[cancel_idx]['Cancellation_Notice_Received']
			library_subscription_df.loc[idx,'final_day'] = (final_day - base_day).days
		except Exception as e:
			printf("[churn_timeseries.py]: Line %s: i = %4d: %s\n",sys.exc_traceback.tb_lineno,i,e)
			library_subscription_df.loc[idx,'final_day'] = None 
	else:    #### Make final day = the current day since they have not churned
		try:
			library_subscription_df.loc[idx,'final_day'] = (cur_datetime - base_day).days 
		except Exception as e:
			printf("[churn_timeseries.py]: Line %s: i = %4d: %s\n",sys.exc_traceback.tb_lineno,i,e)
			library_subscription_df.loc[idx,'final_day'] = None

printf("[churn_timeseries.py]: Library Subscription Complete ... %.2f sec\n",time.time()-start)

##########################################
# Grab account history
##########################################

if (CSM_STATS == True):

	account_history_df = pd.read_csv('./output/sfdc_account_history_20160414.csv').drop('Unnamed: 0',1)
	account_history_df = account_history_df.sort(['AccountId','CreatedDate']).reset_index(drop=True)
	account_history_df['AccountId_18'] = account_history_df['AccountId']
	account_history_df['AccountId'] = [x[0:15] for x in account_history_df['AccountId']]
	account_history_df = account_history_df.rename(columns={'AccountId':'sfdc'})
	account_history_df = pd.merge(account_history_df,account_msa_df[['sfdc',BASE_DATE]],'left',left_on='sfdc',right_on='sfdc') 

	account_history_df['cur_date'] = pd.to_datetime(account_history_df['CreatedDate']) 
	account_history_df['lifetime_day'] = calc_lifetime_day(account_history_df,'cur_date',BASE_DATE)

	csm_history_df = account_history_df[(account_history_df.Field == 'Customer_Success_Manager1__c')].copy(deep=True)
	csm_history_df = csm_history_df.groupby(['sfdc','CreatedDate']).first().reset_index()
	csm_history_df['event'] = ['csm_change'] * len(csm_history_df)

	idx_delete = []
	history_accounts = list(set(account_history_df.sfdc))
	for i in range(0,len(history_accounts)):
		idx = all_indices_CASE_SENSITIVE(history_accounts[i],csm_history_df.sfdc)
		if (len(idx) > 0):
			cur_lifetime_day = csm_history_df.ix[idx[0]]['lifetime_day']
			for j in range(1,len(idx)):
				if ( (csm_history_df.ix[idx[j]]['lifetime_day'] - cur_lifetime_day) < CSM_CHANGE_THRESHOLD):	
					idx_delete.append(idx[j])
				else:
					cur_lifetime_day = csm_history_df.ix[idx[j]]['lifetime_day']	

	csm_history_df = csm_history_df.drop(csm_history_df.index[idx_delete]).reset_index(drop=True)

	merged_history_df = account_history_df[(account_history_df.Field == 'accountMerged')].copy(deep=True).sort(['sfdc','CreatedDate'])
	merged_history_df.to_csv('./output/merged_history_' + cur_datetime.strftime('%Y%m%d') + '.csv')

	printf("[churn_timeseries.py]: CSM / Merged History ... %.2f sec\n",time.time() - start)

######################################
# Read in NPS data from 2014/2015 
######################################
if (NPS_STATS == True):
	NPS2014_df = pd.read_csv('./input_data/NPS2014.csv')
	NPS2014_df = NPS2014_df.rename(columns={'CSAT':'CSAT_Text','AccountId':'sfdc'})
	NPS2014_df['cur_year'] = [2014] * len(NPS2014_df)
	NPS2014_df['CSAT'] = [None] * len(NPS2014_df)
	for i in range(0,len(NPS2014_df)):
		if (NPS2014_df.ix[i]['CSAT_Text'] == 'Extremely Dissatisfied'):
			NPS2014_df.loc[i,'CSAT'] = 1 
		if (NPS2014_df.ix[i]['CSAT_Text'] == 'Somewhat Dissatisfied'):
			NPS2014_df.loc[i,'CSAT'] = 2 
		if (NPS2014_df.ix[i]['CSAT_Text'] == 'Neutral'):
			NPS2014_df.loc[i,'CSAT'] = 3 
		if (NPS2014_df.ix[i]['CSAT_Text'] == 'Somewhat Satisfied'):
			NPS2014_df.loc[i,'CSAT'] = 4
		if (NPS2014_df.ix[i]['CSAT_Text'] == 'Extremely Satisfied'):
			NPS2014_df.loc[i,'CSAT'] = 5 

	NPS2015_df = pd.read_csv('./input_data/NPS2015.csv')
	NPS2015_df = NPS2015_df.rename(columns={'AccountId':'sfdc'})
	NPS2015_df['cur_year'] = [2015] * len(NPS2015_df)

	NPS2014_df['cur_date'] = [datetime.strptime(x.split(' ')[0],'%m/%d/%y') for x in NPS2014_df['DateReceived']]
	NPS2015_df['cur_date'] = [datetime.strptime(x.split(' ')[0],'%m/%d/%y') for x in NPS2015_df['DateReceived']]

	NPS_df = NPS2014_df[['sfdc','cur_year','cur_date','NPS','CSAT']].append(NPS2015_df[['sfdc','cur_year','cur_date','NPS','CSAT']],ignore_index=True).sort(['sfdc','cur_date']).reset_index(drop=True)
	NPS_df = NPS_df[(pd.notnull(NPS_df.sfdc)==True)]
	NPS_df = pd.merge(NPS_df,account_msa_df[['sfdc',BASE_DATE]],'left',left_on='sfdc',right_on='sfdc') 

	NPS_df['lifetime_day'] = calc_lifetime_day(NPS_df,'cur_date',BASE_DATE)
	NPS_df['event'] = ['nps_survey'] * len(NPS_df)
	NPS_df['CSAT'] = [int(x) for x in NPS_df.CSAT]
	NPS_df.to_csv('./output/NPS_df.csv')

	###############################################
	# For NPS/CSAT values, use FINAL SURVEY DATE for each year
	###############################################
	NPS_summary_df = NPS_df[['sfdc',BASE_DATE,'cur_date','cur_year','NPS','CSAT']].groupby(['sfdc','cur_year']) \
							.agg({"NPS":"mean","CSAT":"mean","cur_date":"max","cur_year":"count",BASE_DATE:"max"}) \
							.rename(columns={'NPS':'mean_NPS','CSAT':'mean_CSAT','cur_year':'Nsurvey'}) \
							.reset_index().sort(['sfdc','cur_year']) 

	NPS_summary_df['lifetime_day'] = calc_lifetime_day(NPS_summary_df,'cur_date',BASE_DATE)
	NPS_summary_df['event'] = ['nps_value'] * len(NPS_summary_df)
	NPS_summary_df.to_csv('./output/NPS_summary_df.csv')

	printf("[churn_timeseries.py]: NPS details ... %.2f sec\n",time.time() - start)

######################################
# Get video count progression from G2 
######################################
query = "SELECT * FROM sandbox_prod.TMP_COMPLETED_VIDEO_EVENT WHERE sfdc NOT IN ('test','') ORDER BY account_id,cur_date"
g2_completed_videos_df = pd.read_sql(query,con)
g2_completed_videos_df['sfdc'] = [x[0:15] for x in g2_completed_videos_df['sfdc']]
g2_completed_videos_df = pd.merge(g2_completed_videos_df,account_msa_df[['sfdc',BASE_DATE]],'left',left_on='sfdc',right_on='sfdc')
g2_completed_videos_df['lifetime_day'] = calc_lifetime_day(g2_completed_videos_df,'cur_date',BASE_DATE)

#### Remove demo videos
g2_completed_videos_df = g2_completed_videos_df[~(g2_completed_videos_df.video_title.str.contains('DEMO', case=False))].reset_index(drop=True)

if (G2_FIRST_VIEW == True):
	query = "SELECT sfdc as sfdc_18,LEFT(sfdc,15) as sfdc,MIN(min_time) as initial_view_date FROM benchmark_prod.TMP_REACH_ALL GROUP BY HEX(sfdc)"
	g2_initial_view_df = pd.read_sql(query,con)
	g2_initial_view_df = g2_initial_view_df.drop('sfdc_18',1)
	printf("[churn_timeseries.py]: G2 Reach  ... Query Time ... %.2f sec\n",time.time() - start)
	#g2_initial_view_df = pd.merge(g2_initial_view_df,account_df,'left',left_on='sfdc',right_on='AccountId')
	g2_initial_view_df = pd.merge(g2_initial_view_df,account_msa_df[['sfdc',BASE_DATE]],'left',left_on='sfdc',right_on='sfdc')

	date_diff_30 = []
	date_diff_90 = []
	for i in range(0,len(g2_initial_view_df)):
		try:
			date_diff_30.append( int(float((g2_initial_view_df.ix[i]['initial_view_date'] - g2_initial_view_df.ix[i][BASE_DATE]).days)/30.42) + 1)
		except Exception as e:
			#printf("[churn_timeseries.py]: Line %s: i = %4d: %s\n",sys.exc_traceback.tb_lineno,i,e)
			date_diff_30.append(None)
		try:
			date_diff_90.append( int(float((g2_initial_view_df.ix[i]['initial_view_date'] - g2_initial_view_df.ix[i][BASE_DATE]).days)/91.25) + 1)
		except Exception as e:
			#printf("[churn_timeseries.py]: Line %s: i = %4d: %s\n",sys.exc_traceback.tb_lineno,i,e)
			date_diff_90.append(None)
	g2_initial_view_df['first_view_month'] = date_diff_30
	g2_initial_view_df['first_view_qtr'] = date_diff_90

	printf("[churn_timeseries.py]: G2 Videos ... Query Time ... %.2f sec\n",time.time() - start)

##################################
# Add Admin Usage since 1/1/2014
##################################
if (G2_ADMIN_USAGE == True):
	query = 'SELECT * FROM sandbox_prod.TMP_ADMIN_ACCESS'
	g2_admin_usage_all_df = pd.read_sql(query,con)
	g2_admin_all_df = pd.merge(g2_admin_usage_all_df,account_msa_df[['sfdc',BASE_DATE]],'left',left_on='sfdc',right_on='sfdc')
	g2_admin_all_df['cur_date'] = [x.date() for x in g2_admin_all_df.created_at]

	g2_admin_all_df['lifetime_day'] = calc_lifetime_day(g2_admin_all_df,'created_at',BASE_DATE)
	g2_admin_all_df['event'] = ['admin_usage'] * len(g2_admin_all_df)
	g2_admin_all_df = g2_admin_all_df.sort(['sfdc','created_at']).reset_index(drop=True)  

	g2_admin_summary_df = g2_admin_all_df[['sfdc','cur_date','lifetime_day','user_id','event','action']] \
									.groupby(['sfdc','cur_date','lifetime_day','event']) \
									.agg({'user_id':pd.Series.nunique,'event':"count"}) \
									.rename(columns={'user_id':'Nuser','event':'Nreport'}) \
									.reset_index().sort(['sfdc','cur_date'])
	g2_admin_summary_df = g2_admin_summary_df[pd.notnull(g2_admin_summary_df.sfdc) == True]  

	# Gives the 1st day of a unique_user on the platform (looking at a report) ... groupby 'account_id' would separate SAME USER / DIFFERENT MICROSITE
	g2_admin_unique_user_df = g2_admin_all_df[['sfdc','account_id','account_name','user_id','lifetime_day']] \
										.groupby(['sfdc','user_id']).first().reset_index() \
										.sort(['sfdc','lifetime_day']).reset_index(drop=True)  


	printf("[churn_timeseries.py]: G2 Admin Usage ... Query Time ... %.2f sec\n",time.time() - start)

#################
# Usage Data
#################
if (G2_USAGE == True):
	#g2_output_df = pd.read_sql('SELECT sfdc,account_id,DATE(min_time) as min_time,trackable_id,user_id,parent_id FROM benchmark_prod.TMP_REACH_ALL',con)
	query = "SELECT YEARWEEK(min_time) as yearweek,DATE_ADD(STR_TO_DATE(CONCAT(YEARWEEK(min_time),' Sunday'),'%X%V %W'), INTERVAL 6 DAY) as cur_date,sfdc,account_id, \
					COUNT(distinct DATE(min_time)) as Nview_unique_day,COUNT(distinct account_id) as Naccount,COUNT(distinct trackable_id) as Nvideo, \
					COUNT(distinct user_id) as Nuser,COUNT(distinct parent_id) as Nview \
					FROM benchmark_prod.TMP_REACH_ALL WHERE sfdc IS NOT NULL AND SFDC != '' GROUP BY HEX(sfdc),YEARWEEK(min_time)"
	g2_usage_df = pd.read_sql(query,con)
	g2_usage_df['cur_date'] = pd.to_datetime(g2_usage_df['cur_date'])
	g2_usage_df = pd.merge(g2_usage_df,account_msa_df[['sfdc',BASE_DATE]],'left',left_on='sfdc',right_on='sfdc')
	g2_usage_df['lifetime_day'] = calc_lifetime_day(g2_usage_df,'cur_date',BASE_DATE)
	g2_usage_df['event'] = ['g2_usage'] * len(g2_usage_df) 

	###### All Time G2 stats ######
	#g2_act_complete_stats_df = g2_usage_df[['sfdc','account_id','trackable_id','user_id','parent_id']].groupby(['sfdc']) \
	#                        .agg({"account_id":pd.Series.nunique,"trackable_id":pd.Series.nunique,"user_id":pd.Series.nunique,"parent_id": "count"  }) \
	#                        .rename(columns = {'account_id':'g2_Nmicro_Total','trackable_id':'g2_Nvideo_Total','user_id':'g2_Nuser_Total','parent_id':'g2_Nview_Total'}).reset_index()

	printf("[churn_timeseries.py]: G2 Reach Data ... Query Time ... %.2f sec\n",time.time() - start)

##################################
# Edits info since 7/1/2014
##################################
if (G2_EDITS == True):
	query = "SELECT * FROM edits_prod.TMP_VD1_EDITS"
	g2_vd1_edits_df = pd.read_sql(query,con)
	g2_vd1_edits_df = pd.merge(g2_vd1_edits_df,account_msa_df[['sfdc',BASE_DATE]],'left',left_on='sfdc',right_on='sfdc')
	g2_vd1_edits_df = g2_vd1_edits_df.rename(columns = {'Ncnt_preview':'Ncnt_preview_VD1'})   

	g2_vd1_edits_df = g2_vd1_edits_df[pd.isnull(g2_vd1_edits_df.sfdc) == False].reset_index(drop=True)
	g2_vd1_edits_df = g2_vd1_edits_df.rename(columns={'published_date':'cur_date'})
	g2_vd1_edits_df['lifetime_day'] = calc_lifetime_day(g2_vd1_edits_df,'cur_date',BASE_DATE)
	g2_vd1_edits_df['event'] = ['g2_vd1_edits'] * len(g2_vd1_edits_df) 
	
	query = "SELECT * FROM edits_prod.TMP_ALL_CUSTOMER_TOUCH_EDITS"
	g2_all_edits_df = pd.read_sql(query,con)
	g2_all_edits_df = pd.merge(g2_all_edits_df,account_msa_df[['sfdc',BASE_DATE]],'left',left_on='sfdc',right_on='sfdc')
	g2_all_edits_df = g2_all_edits_df.rename(columns = {'Ncnt_preview':'Ncnt_preview_ALL'})   
	
	g2_all_edits_df = g2_all_edits_df[pd.isnull(g2_all_edits_df.sfdc) == False].reset_index(drop=True)
	g2_all_edits_df = g2_all_edits_df.rename(columns={'published_date':'cur_date'})
	g2_all_edits_df['lifetime_day'] = calc_lifetime_day(g2_all_edits_df,'cur_date',BASE_DATE)
	g2_all_edits_df['event'] = ['g2_all_edits'] * len(g2_all_edits_df) 
	

	###### Complete Stats #######
	#g2_vd1_edit_complete_df = g2_vd1_edits_df[['sfdc','account_id','video_id','video_version_id','Ncnt_preview','Ncnt_qc']].groupby(['sfdc']) \
	#                        .agg({"account_id":pd.Series.nunique,"video_id":pd.Series.nunique,"video_version_id":pd.Series.nunique, \
	#                        "Ncnt_preview":"mean","Ncnt_qc": "mean"  }) \
	#                        .rename(columns = {'account_id':'g2_act_vd1','video_id':'g2_Nvideo_vd1','video_version_id':'g2_Nversion_vd1', \
	#                                            'Ncnt_preview':'g2_avg_edits_preview_vd1','Ncnt_qc':'g2_avg_edits_qc_vd1'}).reset_index()
	#
	#g2_all_edit_complete_df = g2_all_edits_df[['sfdc','account_id','video_id','video_version_id','Ncnt_preview','Ncnt_qc']].groupby(['sfdc']) \
	#                        .agg({"account_id":pd.Series.nunique,"video_id":pd.Series.nunique,"video_version_id":pd.Series.nunique, \
	#                        "Ncnt_preview":"mean","Ncnt_qc": "mean"  }) \
	#                        .rename(columns = {'account_id':'g2_act_all','video_id':'g2_Nvideo_all','video_version_id':'g2_Nversion_all', \
	#                                            'Ncnt_preview':'g2_avg_edits_preview_all','Ncnt_qc':'g2_avg_edits_qc_all'}).reset_index()

	#WF_delivery_times_df = pd.merge(WF_delivery_times_df,WF_sfdc_master_lookup_df[['companyID','sfdc']],'left',left_on='companyID',right_on='companyID')
	#WF_delivery_times_df = pd.merge(WF_delivery_times_df,account_msa_df[['sfdc',BASE_DATE]],'left',left_on='sfdc',right_on='sfdc')
	#WF_delivery_times_df['lifetime_day'] = calc_lifetime_day(WF_delivery_times_df,'cur_date',BASE_DATE)
	#WF_delivery_times_df['event'] = ['WF_delivery_time'] * len(WF_delivery_times_df) 

	printf("[churn_timeseries.py]: G2 Edit Data ... Query Time ... %.2f sec\n",time.time() - start)

##################################
# Production Times 
##################################
if (WF_DELIVERY_TIME == True):
	query = 'SELECT projectID,DE_Line_Item_ID,DE_Opportunity_ID,companyID,company_name,name,entryDate,actualCompletionDate as cur_date,\
					TIMESTAMPDIFF(DAY,entryDate,actualCompletionDate) as delivery_days FROM attask.projects \
   	      			WHERE input_date_id IN (SELECT max(id) FROM attask.input_date WHERE day = "Sun") \
						AND actualCompletionDate != "0000-00-00 00:00:00" \
   	     				AND upper(name) NOT LIKE "%REFRESH%" AND upper(name) NOT LIKE "%%PDATE%" \
						AND upper(name) NOT LIKE "%ACCOUNT MANAGEMENT%" AND upper(name) NOT LIKE "%STYLE GUIDE%" \
			        ORDER BY company_name,actualCompletionDate'

	WF_delivery_times_df = pd.read_sql(query,con)
	
	WF_company_lookup_df = WF_delivery_times_df[['companyID','company_name','DE_Line_Item_ID']].drop_duplicates().reset_index(drop=True)
	
	WF_sfdc_match_df = pd.merge(WF_delivery_times_df,sf_product_lookup_df[['LineItemId','AccountId','sf_account_name']],'left',left_on='DE_Line_Item_ID',right_on='LineItemId')
	printf("[churn_timeseries.py]: WF/SFDC Match Complete ... Query Time ... %.2f sec\n",time.time() - start)
	
	WF_sfdc_master_df = WF_sfdc_match_df[['companyID','AccountId','company_name','sf_account_name']].drop_duplicates().sort('companyID').reset_index()
	WF_sfdc_master_df = WF_sfdc_master_df[(WF_sfdc_master_df['companyID'] != '51365906000790f5fa3d6644a8027728')].reset_index(drop=True)  ## Remove Guidespark  
	
	##################################################
	# Clean Data
	# 1) Remove NaN for all accounts that HAVE A MATCH
	# 2) Add all accounts that DO NOT HAVE A MATCH
	##################################################
	## 1) 
	WF_sfdc_master_NONULL_df = WF_sfdc_master_df[(pd.isnull(WF_sfdc_master_df['AccountId']) == False)]
	
	## 2)
	WF_account_NONULL = list(set(WF_sfdc_master_df[pd.isnull(WF_sfdc_master_df['AccountId']) == False]['companyID']))
	WF_sfdc_master_NULL_df = WF_sfdc_master_df[~WF_sfdc_master_df['companyID'].isin(WF_account_NONULL)]
	
	WF_sfdc_master_lookup_df = WF_sfdc_master_NONULL_df.append(WF_sfdc_master_NULL_df,ignore_index=True).reset_index(drop=True)
	WF_sfdc_master_lookup_df = WF_sfdc_master_lookup_df.rename(columns={'AccountId':'sfdc'})
	
	WF_sfdc_master_lookup_df.to_csv('./output/WF_sfdc_lookup_df.csv',encoding='utf-8')
	
	WF_delivery_times_df = pd.merge(WF_delivery_times_df,WF_sfdc_master_lookup_df[['companyID','sfdc']],'left',left_on='companyID',right_on='companyID')
	WF_delivery_times_df = pd.merge(WF_delivery_times_df,account_msa_df[['sfdc',BASE_DATE]],'left',left_on='sfdc',right_on='sfdc')
	WF_delivery_times_df['lifetime_day'] = calc_lifetime_day(WF_delivery_times_df,'cur_date',BASE_DATE)
	WF_delivery_times_df['event'] = ['WF_delivery_time'] * len(WF_delivery_times_df) 
	
	##### DEBUGGING PURPOSES ... Query for the companyIDs that do not match
	#query = "SELECT name,entryDate,DE_Line_Item_ID,DE_Opportunity_ID from projects \
	#				WHERE input_date_id = '247' AND UPPER(name) NOT LIKE '%STYLE%GUIDE%' AND UPPER(name) NOT LIKE '%REFRESH%' AND companyID IN (' \
	#				" + "','".join(WF_sfdc_master_NULL_df['companyID']) + "')"
	#WF_sfdc_mismatch_df = pd.read_sql(query,con)
	
	count_df = WF_sfdc_master_lookup_df.groupby('sfdc').count().reset_index()
	act = list(set(count_df[(count_df['index'] > 1)]['sfdc']))
	WF_sfdc_duplicates_df = WF_sfdc_master_lookup_df[(WF_sfdc_master_lookup_df.sfdc.isin(act))].sort('sfdc')
	#WF_delivery_times_df[(WF_delivery_times_df.companyID == '5417424400305c87a8f836f730d6191b')]
	
	printf("[churn_timeseries.py]: WF Production Time Complete ... Query Time ... %.2f sec\n",time.time() - start)
	
########################
# sfdc activity history 
########################
if (SFDC_ACTIVITY == True):

	if (UPDATE_SFDC_ACTIVITY == True):
		sfdc_activity_df['AccountId_18'] = sfdc_activity_df['AccountId']
		sfdc_activity_df['sfdc'] = [x[0:15] for x in sfdc_activity_df.AccountId_18]
		sfdc_activity_df = pd.merge(sfdc_activity_df,account_msa_df[['sfdc',BASE_DATE]],'left',left_on='sfdc',right_on='sfdc')
		sfdc_activity_df['lifetime_day'] = calc_lifetime_day(sfdc_activity_df,'ActivityDate',BASE_DATE)
		sfdc_activity_df.to_csv('../sfdc_activity.csv')

	sfdc_activity_df = sfdc_activity_df[sfdc_activity_df['ActivityType'].isin(['1_','Email'])].reset_index(drop=True)

	sfdc_activity_count_df = sfdc_activity_df[['sfdc','ActivityDate','Description']].groupby(['sfdc','ActivityDate']).count().reset_index()	

	#######################
	# Find all Saturdays
	#######################
	saturday0 = datetime(2008,1,1,0,0,0).toordinal()
	saturday1 = datetime.fromordinal(saturday0 - (saturday0 % 7) - 1 )
	today = datetime.today().toordinal()
	cur_saturday = datetime.fromordinal(today - (today % 7) - 1)
	start_saturday = []
	end_saturday = []
	i=1
	while True:
		if ( (saturday1 + timedelta(days = 7*i) ) <= cur_saturday):
			start_saturday.append(saturday1 + timedelta(days = 7*(i-1)) )
			end_saturday.append(saturday1 + timedelta(days = 7*i) )
			i = i + 1
		else:
			break
	saturday_df = pd.merge(pd.DataFrame(start_saturday).rename(columns={0:'start_saturday'}), pd.DataFrame(end_saturday).rename(columns={0:'end_saturday'}),'left',left_index=True,right_index=True)
	
	cur_saturday = []
	for i in range(0,len(sfdc_activity_count_df)):
		
		if ((i % 10000) == 9999):
			printf("[churn_timeseries.py]: Find Saturday ... %5s of %5s ... %.2f sec\n",i+1,len(sfdc_activity_count_df),time.time()-start)
	 
		idx = saturday_df[(saturday_df['start_saturday'] < sfdc_activity_count_df.ix[i]['ActivityDate']) & (saturday_df['end_saturday'] >= sfdc_activity_count_df.ix[i]['ActivityDate'])].index
		try:
			cur_saturday.append(saturday_df.ix[idx[0]]['end_saturday'])
		except:
			cur_saturday.append(None)
			
	sfdc_activity_count_df['saturday'] = cur_saturday
	
	sfdc_activity_weekly_df = sfdc_activity_count_df.groupby(['sfdc','saturday']).sum().rename(columns={'Description':'Nactivity'}).reset_index()	

	activity_accounts = list(set(sfdc_activity_weekly_df.sfdc))
	all_activity_dates_df = []
	for i in range(0,len(activity_accounts)):
		tmp_dates_df = pd.merge(pd.DataFrame([activity_accounts[i]] * len(saturday_df)).rename(columns = {0:'sfdc'}), pd.DataFrame(saturday_df['end_saturday']).rename(columns = {0:'sfdc'}),'left',left_index=True,right_index=True )
		if (i == 0):
			all_activity_dates_df = tmp_dates_df.copy(deep=True)
		else:
			all_activity_dates_df = all_activity_dates_df.append(tmp_dates_df,ignore_index=True  )
	
	all_activity_dates_df = pd.merge(all_activity_dates_df,sfdc_activity_weekly_df,'left',left_on=(['sfdc','end_saturday']),right_on=(['sfdc','saturday']) ).drop('saturday',1).fillna(0).rename(columns = {'end_saturday':'cur_date'})
	all_activity_dates_df['event'] = ['sfdc_activity'] * len(all_activity_dates_df)
	all_activity_dates_df = pd.merge(all_activity_dates_df,account_msa_df[['sfdc',BASE_DATE]],'left',left_on='sfdc',right_on='sfdc')
	all_activity_dates_df['lifetime_day'] = calc_lifetime_day(all_activity_dates_df,'cur_date',BASE_DATE)

	all_activity_dates_df = all_activity_dates_df[(all_activity_dates_df['lifetime_day'] >= 0)].reset_index(drop=True)

	printf("[churn_timeseries.py]: SFDC Activity Data ... Query Time ... %.2f sec\n",time.time() - start)

##########################################################
# Combine into 1 dataframe 
##########################################################

timeline_df = library_subscription_df.append(g2_completed_videos_df).sort(['sfdc','lifetime_day']).reset_index(drop=True)
if (CSM_STATS == True):
	timeline_df = timeline_df.append(csm_history_df[['sfdc','cur_date','event','lifetime_day']],ignore_index=True).sort(['sfdc','lifetime_day']).reset_index(drop=True)
if (NPS_STATS == True):
	timeline_df = timeline_df.append(NPS_df[['sfdc','cur_date','event','lifetime_day','NPS','CSAT']],ignore_index=True).sort(['sfdc','lifetime_day']).reset_index(drop=True)
if (G2_ADMIN_USAGE == True):
	timeline_df = timeline_df.append(g2_admin_summary_df[['sfdc','cur_date','event','lifetime_day','Nreport']],ignore_index=True).sort(['sfdc','lifetime_day']).reset_index(drop=True)
if (G2_USAGE == True):
	timeline_df = timeline_df.append(g2_usage_df[['sfdc','cur_date','event','lifetime_day','Nview_unique_day','Nview']],ignore_index=True).sort(['sfdc','lifetime_day']).reset_index(drop=True)
if (G2_EDITS == True):
	timeline_df = timeline_df.append(g2_vd1_edits_df[['sfdc','cur_date','event','lifetime_day','Ncnt_preview_VD1']],ignore_index=True).sort(['sfdc','lifetime_day']).reset_index(drop=True)
	timeline_df = timeline_df.append(g2_all_edits_df[['sfdc','cur_date','event','lifetime_day','Ncnt_preview_ALL']],ignore_index=True).sort(['sfdc','lifetime_day']).reset_index(drop=True)
if (SFDC_ACTIVITY == True):
	timeline_df = timeline_df.append(all_activity_dates_df[['sfdc','cur_date','event','lifetime_day','Nactivity']],ignore_index=True).sort(['sfdc','lifetime_day']).reset_index(drop=True)
timeline_accounts = list(set(timeline_df.sfdc))

if (WF_DELIVERY_TIME == True):
	timeline_df = timeline_df.append(WF_delivery_times_df[['sfdc','cur_date','event','lifetime_day','delivery_days']],ignore_index=True).sort(['sfdc','lifetime_day']).reset_index(drop=True)

timeline_accounts_WF = list(set(timeline_df.sfdc))
extra_sfdc = extra_val(timeline_accounts_WF,timeline_accounts)
if (len(extra_sfdc) > 0):
	timeline_df = timeline_df[~timeline_df['sfdc'].isin(extra_sfdc)].reset_index(drop=True)

timeline_df = timeline_df[(timeline_df['sfdc'] != '')].reset_index(drop=True)
timeline_df = timeline_df[(timeline_df['sfdc'] != 'test')].reset_index(drop=True)

timeline_df = timeline_df.drop('final_day',1)
timeline_df = timeline_df.drop('churn',1)
timeline_df = timeline_df.drop('churn_int',1)
timeline_df = timeline_df.drop('Cambria_algo_delta',1)
timeline_df = timeline_df.drop('non_evergreen',1)

library_details_df = library_subscription_df[['sfdc','final_day','churn_int','churn','Cambria_algo_delta','non_evergreen']].drop_duplicates().reset_index(drop=True) 
timeline_df = pd.merge(timeline_df,library_details_df,'left',left_on='sfdc',right_on='sfdc')

#timeline_df = timeline_df.append(NPS_summary_df[['sfdc','cur_date','event','lifetime_day','mean_NPS','mean_CSAT']],ignore_index=True).sort(['sfdc','lifetime_day']).reset_index(drop=True)

#####################
# Grab all accounts
#####################
timeline_accounts = list(set(timeline_df.sfdc))
if (TEST_CASE == True):
	timeline_df = timeline_df[(timeline_df.sfdc == SINGLE_ACCOUNT)].reset_index(drop=True)
	timeline_accounts = list(set(timeline_df.sfdc))

printf("[churn_timeseries.py]: Data Merged (timeline_df) ... %.2f sec\n",time.time()-start)

####################################################################
####################################################################
####################################################################
####################################################################
####################################################################
######################### 2) Clean data ############################
####################################################################
####################################################################
####################################################################
####################################################################
####################################################################

printf("\n\n")
printf("[churn_timeseries.py]: ##########################################################\n")
printf("[churn_timeseries.py]: ####################### 2) Clean Data ####################\n")
printf("[churn_timeseries.py]: ##########################################################\n\n")

##########################################
# 1) Create video count for each account
##########################################
if (NVIDEO == True): 
	cur_account_videos = [None] * len(timeline_df)
	for i in range(0,len(timeline_accounts)):
		idx = all_indices_CASE_SENSITIVE(timeline_accounts[i],timeline_df.sfdc)
		Nvideo = 0
		for j in range(0,len(idx)):
			if (timeline_df.ix[idx[j]]['event'] == 'g2_publish_date'):	
				Nvideo = Nvideo + 1
			elif (timeline_df.ix[idx[j]]['event'] == 'g2_close_date'):	
				Nvideo = Nvideo - 1
			cur_account_videos[idx[j]] = Nvideo
	
	timeline_df['Nvideo'] = cur_account_videos 

	printf("[churn_timeseries.py]: Data Cleaned: Nvideo ... %.2f sec\n",time.time()-start)

#############################################################################################
# 2) Add # of upsells
#  time-dependent variable ... add cumulative total to each time input (0->1->2->3 etc...)
#############################################################################################
if (UPSELL_DOWNTICK_SWAP == True):
	Nupsell_ops = [None] * len(timeline_df)
	Ndowntick_ops = [None] * len(timeline_df)
	Ndowntick_count = [None] * len(timeline_df)
	Ncur_titles = [None] * len(timeline_df)
	Ncur_products = [None] * len(timeline_df)
	Ntitles_lost = [None] * len(timeline_df)
	Nproducts_lost = [None] * len(timeline_df)
	Nsolution = [None] * len(timeline_df)
	Nbenefits = [None] * len(timeline_df)
	Ncompensation = [None] * len(timeline_df)
	Nfwa = [None] * len(timeline_df)
	Ncompliance = [None] * len(timeline_df)
	Nperf_mgmt = [None] * len(timeline_df)
	Nswap_ops = [None] * len(timeline_df)
	for i in range(0,len(timeline_accounts)):
		idx = all_indices_CASE_SENSITIVE(timeline_accounts[i],timeline_df.sfdc)
		Nupsell = 0
		Nswap = 0
		Ndowntick = 0
		downtick_count = 0
		cur_titles = "" 
		cur_products = "" 
		titles_lost = "" 
		products_lost = "" 
		solution = 0
		benefits = 0 
		compensation = 0
		fwa = 0 
		compliance = 0
		perf_mgmt = 0
		for j in range(0,len(idx)):
			if (pd.isnull(timeline_df.ix[idx[j]]['op_type']) == False):	
				if ('Upsell' in timeline_df.ix[idx[j]]['op_type']):	
					Nupsell = Nupsell + 1
				if ('Downtick' in timeline_df.ix[idx[j]]['op_type']):	
					Ndowntick = Ndowntick + 1
					downtick_count = downtick_count + timeline_df.ix[idx[j]]['downtick_count'] 
					if (len(titles_lost) == 0):
						titles_lost = timeline_df.ix[idx[j]]['titles_lost'] 
						products_lost = timeline_df.ix[idx[j]]['products_lost'] 
					else:
						titles_lost = titles_lost + ";" + timeline_df.ix[idx[j]]['titles_lost'] 
						products_lost = products_lost + ";" + timeline_df.ix[idx[j]]['products_lost'] 
				if ('Cancel' in timeline_df.ix[idx[j]]['op_type']):	
					Ndowntick = Ndowntick + 1
					downtick_count = downtick_count + timeline_df.ix[idx[j]]['downtick_count'] 
					if (len(titles_lost) == 0):
						titles_lost = timeline_df.ix[idx[j]]['titles_lost'] 
						products_lost = timeline_df.ix[idx[j]]['products_lost'] 
					else:
						titles_lost = titles_lost + ";" + timeline_df.ix[idx[j]]['titles_lost'] 
						products_lost = products_lost + ";" + timeline_df.ix[idx[j]]['products_lost'] 
				if ('Video Swap' in timeline_df.ix[idx[j]]['op_type']):	
					Nswap = Nswap + 1
				if (len(cur_titles) == 0):
					cur_titles = timeline_df.ix[idx[j]]['cur_titles'] 
					cur_products = timeline_df.ix[idx[j]]['cur_products'] 
				else:
					cur_titles = cur_titles + ";" + timeline_df.ix[idx[j]]['cur_titles'] 
					cur_products = cur_products + ";" + timeline_df.ix[idx[j]]['cur_products'] 
				solution = timeline_df.ix[idx[j]]['Nsolution'] 
				benefits = timeline_df.ix[idx[j]]['Nbenefits'] 
				compensation = timeline_df.ix[idx[j]]['Ncompensation'] 
				fwa = timeline_df.ix[idx[j]]['Nfwa'] 
				perf_mgmt = timeline_df.ix[idx[j]]['Nperf_mgmt'] 
				compliance = timeline_df.ix[idx[j]]['Ncompliance'] 
			Nupsell_ops[idx[j]] = Nupsell
			Ndowntick_ops[idx[j]] = Ndowntick
			Ndowntick_count[idx[j]] = downtick_count
			Ncur_titles[idx[j]] = cur_titles
			Ncur_products[idx[j]] = cur_products
			Ntitles_lost[idx[j]] = titles_lost
			Nproducts_lost[idx[j]] = products_lost
			Nsolution[idx[j]] = solution
			Nbenefits[idx[j]] = benefits
			Ncompensation[idx[j]] = compensation
			Nfwa[idx[j]] = fwa
			Nperf_mgmt[idx[j]] = perf_mgmt
			Ncompliance[idx[j]] = compliance
			Nswap_ops[idx[j]] = Nswap
	
	timeline_df['Nupsell'] = Nupsell_ops 
	timeline_df['Ndowntick'] = Ndowntick_ops 
	timeline_df['Ndowntick_count'] = Ndowntick_count 
	timeline_df['Ndowntick'] = Ndowntick_ops 
	timeline_df['Ndowntick_count'] = Ndowntick_count 
	timeline_df['cur_titles'] = Ncur_titles
	timeline_df['cur_products'] = Ncur_products
	timeline_df['titles_lost'] = Ntitles_lost 
	timeline_df['products_lost'] = Nproducts_lost 
	timeline_df['Nsolution'] = Nsolution 
	timeline_df['Nbenefits'] = Nbenefits 
	timeline_df['Ncompensation'] = Ncompensation 
	timeline_df['Nfwa'] = Nfwa 
	timeline_df['Nperf_mgmt'] = Nperf_mgmt 
	timeline_df['Ncompliance'] = Ncompliance 
	timeline_df['Nswap'] = Nswap_ops 

	printf("[churn_timeseries.py]: Data Cleaned: Upsell/Downtick/Swap ... %.2f sec\n",time.time()-start)

###############################
# 3) Find CSM changes
###############################
if (CSM_STATS == True):
	csm_change = [None] * len(timeline_df)
	for i in range(0,len(timeline_accounts)):
		idx = all_indices_CASE_SENSITIVE(timeline_accounts[i],timeline_df.sfdc)
		Ncsm_change = 0
		for j in range(0,len(idx)):
			if (timeline_df.ix[idx[j]]['event'] == 'csm_change'):	
				Ncsm_change = Ncsm_change + 1
	
			csm_change[idx[j]] = Ncsm_change
	
	timeline_df['Ncsm_change'] = csm_change 
	
	printf("[churn_timeseries.py]: Data Cleaned: CSM stats ... %.2f sec\n",time.time()-start)
	
###############################
# 4) Add NPS/CSAT values  
###############################
if (NPS_STATS == True):
	nps = [None] * len(timeline_df)
	nps_value  = [0] * len(timeline_df)
	csat_value = [0] * len(timeline_df)
	for i in range(0,len(timeline_accounts)):
		idx = all_indices_CASE_SENSITIVE(timeline_accounts[i],timeline_df.sfdc)
		Nnps = 0
		cur_NPS = 0
		cur_CSAT = 0
		for j in range(0,len(idx)):
			if (timeline_df.ix[idx[j]]['event'] == 'nps_survey'):	
				Nnps = Nnps + 1
				try:
					cur_NPS = float(NPS_summary_df[(NPS_summary_df.sfdc == timeline_accounts[i]) & (NPS_summary_df.lifetime_day == timeline_df.ix[idx[j]]['lifetime_day'])]['mean_NPS'])
				except:
					cur_NPS = timeline_df.ix[idx[j]]['NPS']
				try:
					cur_CSAT = float(NPS_summary_df[(NPS_summary_df.sfdc == timeline_accounts[i]) & (NPS_summary_df.lifetime_day == timeline_df.ix[idx[j]]['lifetime_day'])]['mean_CSAT'])
				except:
					cur_CSAT = timeline_df.ix[idx[j]]['CSAT']
					
			nps_value[idx[j]] = cur_NPS 
			csat_value[idx[j]] = cur_CSAT 
			nps[idx[j]] = Nnps
	
	timeline_df['Nnps_survey'] = nps 
	timeline_df['NPS'] = nps_value 
	timeline_df['CSAT'] = csat_value 
	
	printf("[churn_timeseries.py]: Data Cleaned: NPS stats ... %.2f sec\n",time.time()-start)
	
#######################
# 5) Update admin usage
#######################
if (G2_ADMIN_USAGE == True):
	Nadmin_usage_day  = [0] * len(timeline_df)
	Nreport = [0] * len(timeline_df)
	for i in range(0,len(timeline_accounts)):
		idx = all_indices_CASE_SENSITIVE(timeline_accounts[i],timeline_df.sfdc)
		admin_usage_day = 0
		report_number = 0
		for j in range(0,len(idx)):
			if (timeline_df.ix[idx[j]]['event'] == 'admin_usage'):	
				admin_usage_day = admin_usage_day + 1
				report_number = report_number + timeline_df.ix[idx[j]]['Nreport']
	
			Nadmin_usage_day[idx[j]] = admin_usage_day 
			Nreport[idx[j]] = report_number
	
	timeline_df['Nadmin_usage_day'] = Nadmin_usage_day
	timeline_df['Nreport_total'] = Nreport
	
	timeline_df['Nunique_user'] = [0] * len(timeline_df)
	for i in range(0,len(timeline_accounts)):
		idx_unique = all_indices_CASE_SENSITIVE(timeline_accounts[i],g2_admin_unique_user_df.sfdc)
		Nuser = 0 
		for j in range(0,len(idx_unique)):
			Nuser = Nuser + 1
			idx = timeline_df[(timeline_df.sfdc == g2_admin_unique_user_df.ix[idx_unique[j]]['sfdc']) \
						   & (timeline_df.lifetime_day >= g2_admin_unique_user_df.ix[idx_unique[j]]['lifetime_day'])].index
			timeline_df.loc[idx,'Nunique_user'] = Nuser
	
	printf("[churn_timeseries.py]: Data Cleaned: G2 Admin Usage ... %.2f sec\n",time.time()-start)
	
##########################
# 6) Update g2 usage info
##########################
if (G2_USAGE == True):
	Nview_day  = [0] * len(timeline_df)
	Nview_total  = [0] * len(timeline_df)
	Nview_weekly  = [0] * len(timeline_df)
	for i in range(0,len(timeline_accounts)):
		idx = all_indices_CASE_SENSITIVE(timeline_accounts[i],timeline_df.sfdc)
		view_day = 0
		view_total = 0
		view_weekly = 0
		for j in range(0,len(idx)):
			if (timeline_df.ix[idx[j]]['event'] == 'g2_usage'):	
				view_day = view_day + timeline_df.ix[idx[j]]['Nview_unique_day']
				view_total = view_total + timeline_df.ix[idx[j]]['Nview']
				view_weekly = timeline_df.ix[idx[j]]['Nview']
	
			Nview_day[idx[j]] = view_day 
			Nview_total[idx[j]] = view_total
			Nview_weekly[idx[j]] = view_weekly
	
	timeline_df['Nview_unique_day'] = Nview_day
	timeline_df['Nview_total'] = Nview_total
	timeline_df['Nview_weekly'] = Nview_weekly
	
	printf("[churn_timeseries.py]: Data Cleaned: G2 Usage ... %.2f sec\n",time.time()-start)
	
##########################
# 7) Update g2_edits info
###########################
if (G2_EDITS == True):
	Nedits_touch_average_vd1  = [0] * len(timeline_df)
	Nedits_touch_average_all  = [0] * len(timeline_df)
	for i in range(0,len(timeline_accounts)):
		idx = all_indices_CASE_SENSITIVE(timeline_accounts[i],timeline_df.sfdc)
		edits_touch_vd1 = 0
		edits_touch_average_vd1 = 0
		edits_touch_all = 0
		edits_touch_average_all = 0
		for j in range(0,len(idx)):
			if (timeline_df.ix[idx[j]]['event'] == 'g2_vd1_edits'):	
				edits_touch_vd1 = edits_touch_vd1 + 1
				edits_touch_average_vd1 = float(edits_touch_average_vd1*float(edits_touch_vd1-1) + timeline_df.ix[idx[j]]['Ncnt_preview_VD1']) / float(edits_touch_vd1)
			if (timeline_df.ix[idx[j]]['event'] == 'g2_all_edits'):	
				edits_touch_all = edits_touch_all + 1
				edits_touch_average_all = float(edits_touch_average_all*float(edits_touch_all-1) + timeline_df.ix[idx[j]]['Ncnt_preview_ALL']) / float(edits_touch_all)
	
			Nedits_touch_average_vd1[idx[j]] = edits_touch_average_vd1
			Nedits_touch_average_all[idx[j]] = edits_touch_average_all
	
	timeline_df['Nedits_vd1'] = Nedits_touch_average_vd1
	timeline_df['Nedits_all'] = Nedits_touch_average_all

	printf("[churn_timeseries.py]: Data Cleaned: G2 Edits ... %.2f sec\n",time.time()-start)

####################################################################
# 8) WF Average delivery times (actualCompletioDate - entryDate)
####################################################################
if (WF_DELIVERY_TIME == True):
	Nvideo_delivered_WF  = [0] * len(timeline_df)
	Nvideo_delivery_time  = [0] * len(timeline_df)
	Nfirst_video_day  = [0] * len(timeline_df)
	for i in range(0,len(timeline_accounts)):
		idx = all_indices_CASE_SENSITIVE(timeline_accounts[i],timeline_df.sfdc)
		video_delivered_WF = 0
		video_delivery_time = 0
		first_video_day = -1 
		for j in range(0,len(idx)):
			if (timeline_df.ix[idx[j]]['event'] == 'WF_delivery_time'):	
				video_delivered_WF = video_delivered_WF + 1
				if (video_delivered_WF == 1):
					first_video_day = timeline_df.ix[idx[j]]['lifetime_day']
				video_delivery_time = float(video_delivery_time*float(video_delivered_WF-1) + timeline_df.ix[idx[j]]['delivery_days']) / float(video_delivered_WF)
	
			Nvideo_delivered_WF[idx[j]] = video_delivered_WF 
			Nvideo_delivery_time[idx[j]] = video_delivery_time
			Nfirst_video_day[idx[j]] = first_video_day 
	
	timeline_df['first_video_day'] = Nfirst_video_day
	timeline_df['Nvideo_delivered_WF'] = Nvideo_delivered_WF
	timeline_df['Nvideo_delivery_time'] = Nvideo_delivery_time
	
	printf("[churn_timeseries.py]: Data Cleaned: WF delivery times ... %.2f sec\n",time.time()-start)

####################################################################
# 9) SFDC weekly activities 
####################################################################
if (SFDC_ACTIVITY == True):
	Nactivity_weekly  = [0] * len(timeline_df)
	Nactivity_total  = [0] * len(timeline_df)
	for i in range(0,len(timeline_accounts)):
		idx = all_indices_CASE_SENSITIVE(timeline_accounts[i],timeline_df.sfdc)
		activity_total = 0
		for j in range(0,len(idx)):
			activity_weekly = 0
			if (timeline_df.ix[idx[j]]['event'] == 'sfdc_activity'):	
				activity_weekly = timeline_df.ix[idx[j]]['Nactivity']
				activity_total = activity_total + timeline_df.ix[idx[j]]['Nactivity']
	
			Nactivity_weekly[idx[j]] = activity_weekly 
			Nactivity_total[idx[j]]  = activity_total 
	
	timeline_df['Nactivity_weekly'] = Nactivity_weekly
	timeline_df['Nactivity_total']  = Nactivity_total
	
	printf("[churn_timeseries.py]: Data Cleaned: SFDC Activities ... %.2f sec\n",time.time()-start)

#######################################
# 10) Fill in library size that are NaN
#######################################

Nodd = 0
odd_accounts = []
for i in range(0,len(timeline_accounts)):

	if ((i % 100) == 99):
		printf("[churn_timeseries.py]: Account ... %5s of %5s ... tdata_timeline_df scrubbing ... %.2f sec\n",i+1,len(timeline_accounts),time.time()-start)

	idx = all_indices_CASE_SENSITIVE(timeline_accounts[i],timeline_df.sfdc)
	cur_video = timeline_df.ix[idx[0]]['library_size']
	#cur_churn = timeline_df.ix[idx[0]]['churn_int']
	#cur_final_day = timeline_df.ix[idx[0]]['final_day']
	#if (pd.isnull(cur_video) == False):
	#for j in range(1,len(idx)):
	for j in range(0,len(idx)):
		if (pd.isnull(timeline_df.ix[idx[j]]['library_size']) == True):	
			timeline_df.loc[idx[j],'library_size'] = cur_video
		else: 	
			cur_video = timeline_df.ix[idx[j]]['library_size']
#		if (pd.isnull(timeline_df.ix[idx[j]]['churn_int']) == True):	
#			timeline_df.loc[idx[j],'churn_int'] = cur_churn
#		else: 	
#			cur_churn = timeline_df.ix[idx[j]]['churn_int']
#		if (pd.isnull(timeline_df.ix[idx[j]]['final_day']) == True):	
#			timeline_df.loc[idx[j],'final_day'] = cur_final_day 
#		else: 	
#			cur_final_day = timeline_df.ix[idx[j]]['final_day']
#		if (pd.isnull(timeline_df.ix[idx[j]]['churn']) == True):	
#			timeline_df.loc[idx[j],'churn'] = 0 
	#else:
	#	Nodd = Nodd + 1
	#	odd_accounts.append(timeline_accounts[i])
	#	printf('%4s ... %s\n',Nodd,timeline_accounts[i])
		#sys.exit()

printf("[churn_timeseries.py]: tdata_timeline_df is scrubbed/updated ... %.2f sec\n",time.time()-start)

####################################################################
####################################################################
####################################################################
####################################################################
####################################################################
################ 3) Put Data is Tmerge Format ######################
####################################################################
####################################################################
####################################################################
####################################################################
####################################################################

printf("\n\n")
printf("[churn_timeseries.py]: ##########################################################\n")
printf("[churn_timeseries.py]: ############# 3) Put Data in tmerge Format (R) ###########\n")
printf("[churn_timeseries.py]: ##########################################################\n\n")


MODEL_INDEPENDENT_VARIABLES = ['library_size']
if (NVIDEO == True): 
	MODEL_INDEPENDENT_VARIABLES.append('Nvideo')
if (UPSELL_DOWNTICK_SWAP == True):
	MODEL_INDEPENDENT_VARIABLES.append('Nupsell')
	MODEL_INDEPENDENT_VARIABLES.append('Ndowntick')
	MODEL_INDEPENDENT_VARIABLES.append('Ndowntick_count')
	#MODEL_INDEPENDENT_VARIABLES.append('cur_titles')
	#MODEL_INDEPENDENT_VARIABLES.append('cur_products')
	MODEL_INDEPENDENT_VARIABLES.append('titles_lost')
	MODEL_INDEPENDENT_VARIABLES.append('products_lost')
	MODEL_INDEPENDENT_VARIABLES.append('Nsolution')
	MODEL_INDEPENDENT_VARIABLES.append('Nbenefits')
	MODEL_INDEPENDENT_VARIABLES.append('Ncompensation')
	MODEL_INDEPENDENT_VARIABLES.append('Nfwa')
	MODEL_INDEPENDENT_VARIABLES.append('Nperf_mgmt')
	MODEL_INDEPENDENT_VARIABLES.append('Ncompliance')
	MODEL_INDEPENDENT_VARIABLES.append('Nswap')
if (CSM_STATS  == True):
	MODEL_INDEPENDENT_VARIABLES.append('Ncsm_change') 
if (NPS_STATS == True):
	MODEL_INDEPENDENT_VARIABLES.append('Nnps_survey')
	MODEL_INDEPENDENT_VARIABLES.append('NPS')
	MODEL_INDEPENDENT_VARIABLES.append('CSAT')
if (G2_ADMIN_USAGE == True):
	MODEL_INDEPENDENT_VARIABLES.append('Nadmin_usage_day')
	MODEL_INDEPENDENT_VARIABLES.append('Nreport_total')
if (G2_USAGE == True):
	MODEL_INDEPENDENT_VARIABLES.append('Nunique_user')
	MODEL_INDEPENDENT_VARIABLES.append('Nview_unique_day')
	MODEL_INDEPENDENT_VARIABLES.append('Nview_total')
	MODEL_INDEPENDENT_VARIABLES.append('Nview_weekly')
if (G2_EDITS == True):
	MODEL_INDEPENDENT_VARIABLES.append('Nedits_vd1')
	MODEL_INDEPENDENT_VARIABLES.append('Nedits_all')
if (WF_DELIVERY_TIME == True):
	MODEL_INDEPENDENT_VARIABLES.append('first_video_day')
	MODEL_INDEPENDENT_VARIABLES.append('Nvideo_delivered_WF')
	MODEL_INDEPENDENT_VARIABLES.append('Nvideo_delivery_time')
if (SFDC_ACTIVITY == True):
	MODEL_INDEPENDENT_VARIABLES.append('Nactivity_weekly')
	MODEL_INDEPENDENT_VARIABLES.append('Nactivity_total')
	
#ALL_VARIABLES = ['sfdc','churn_int','churn','lifetime_day','final_day','Cancellation_Notice_Received','event','op_type','video_id','video_title']
TDATA_VARIABLES = ['sfdc','churn_int','churn','lifetime_day','final_day','event','Total_Lost_ARR','Cancellation_Notice_Received','ChurnDate','op_type','video_id','video_title','Cambria_algo_delta','non_evergreen']
tdata_timeline_df = timeline_df[TDATA_VARIABLES + MODEL_INDEPENDENT_VARIABLES]

### Do we need to set churn library_size to PRIOR library_size
SDATA_VARIABLES = TDATA_VARIABLES
SDATA_VARIABLES.remove('churn_int')
#SDATA_VARIABLES.remove('Cancellation_Notice_Received')
SDATA_VARIABLES.remove('op_type')
SDATA_VARIABLES.remove('video_id')
SDATA_VARIABLES.remove('video_title')

sdata_timeline_df = []
for i in range(0,len(timeline_accounts)):

	if ((i % 100) == 99):
		printf("[churn_timeseries.py]: Account ... %5s of %5s ... sdata_timeline_df creation ... %.2f sec\n",i+1,len(timeline_accounts),time.time()-start)

	#####################################
	# 1) Extract data from each account
	#####################################
	tmp_tdata_timeline_df = tdata_timeline_df.ix[all_indices_CASE_SENSITIVE(timeline_accounts[i],tdata_timeline_df.sfdc)]
	tmp_tdata_timeline_df = tmp_tdata_timeline_df.drop(['video_id','video_title'],1)
	#printf('%s\n\n',tmp_tdata_timeline_df)

	################################################################################################################
	# 2) Group records with the SAME lifetime_day
	#		Find the MAXIMUM INDEX (Nmax_idx) for the MAXIMUM RECORD for groups that have the same lifetime_day
	################################################################################################################
	tmp_sdata_timeline_MAXIDX_df = tmp_tdata_timeline_df[['sfdc','lifetime_day']].reset_index().groupby(['sfdc','lifetime_day'],as_index=False).agg({'index':max}).rename(columns={'index':'Nmax_idx'})
	tmp_sdata_timeline_df = tmp_tdata_timeline_df.ix[tmp_sdata_timeline_MAXIDX_df['Nmax_idx']][SDATA_VARIABLES + MODEL_INDEPENDENT_VARIABLES].reset_index(drop=True)
	
	########################################
	# 3) Create tstart / tstop R framework 
	########################################
	if (len(tmp_sdata_timeline_df) > 0):
		tmp_sdata_timeline_df.insert(3,'tstop',[None] * len(tmp_sdata_timeline_df))
		for j in range(0,len(tmp_sdata_timeline_df)-1):
			Nidx = tmp_sdata_timeline_df.index[j]
			N1idx = tmp_sdata_timeline_df.index[j+1]
			if (tmp_sdata_timeline_df.ix[N1idx]['lifetime_day'] < tmp_sdata_timeline_df.ix[N1idx]['final_day']):
				tmp_sdata_timeline_df.loc[Nidx,'tstop'] = tmp_sdata_timeline_df.ix[N1idx]['lifetime_day']
			else:
				tmp_sdata_timeline_df.loc[Nidx,'tstop'] = tmp_sdata_timeline_df.ix[N1idx]['final_day']
		cur_idx = tmp_sdata_timeline_df.index[len(tmp_sdata_timeline_df)-1]  
		tmp_sdata_timeline_df.loc[cur_idx,'tstop'] = tmp_sdata_timeline_df.ix[cur_idx]['final_day']

		#########################################################
		# 4) Grab value for all variables where index = Nmax_idx 
		#########################################################
		#tmp_sdata_timeline_df['Nvideo'] = [tmp_tdata_timeline_df.ix[x]['Nvideo'] for x in tmp_sdata_timeline_df['Nmax_idx']]
		#tmp_sdata_timeline_df['Nview_total'] = [tmp_tdata_timeline_df.ix[x]['Nview_total'] for x in tmp_sdata_timeline_df['Nmax_idx']]
		#tmp_sdata_timeline_df['library_completion_per'] = tmp_sdata_timeline_df['Nvideo'] / tmp_sdata_timeline_df['library_size']

		#######################################################
		# 1) Remove any records AFTER 'churn' date (i.e. full cancellation)
		# 2) Remove cancellation RECORD and add 'churn=1' to previous record 
		# 3) Correct where tstart = tstop
		#######################################################
		# 1) 
		tmp_sdata_timeline_df =	tmp_sdata_timeline_df[(tmp_sdata_timeline_df.lifetime_day <= tmp_sdata_timeline_df.tstop)].reset_index(drop=True)
		# 2) 
		if (len(tmp_sdata_timeline_df[(tmp_sdata_timeline_df['churn'] == 1)].index) == 1):
			churn_idx = tmp_sdata_timeline_df[(tmp_sdata_timeline_df['churn'] == 1)].index[0] 
			tmp_sdata_timeline_df.loc[churn_idx-1,'churn'] = 1 
			tmp_sdata_timeline_df =	tmp_sdata_timeline_df[(tmp_sdata_timeline_df.index != churn_idx)]   	
		elif (len(tmp_sdata_timeline_df[(tmp_sdata_timeline_df['churn'] == 1)].index) > 1):
			printf("[churn_timeseries.py] %s\n",tmp_sdata_timeline_df)

		# 3)
		tmp_sdata_timeline_df = tmp_sdata_timeline_df[(tmp_sdata_timeline_df['lifetime_day'] != tmp_sdata_timeline_df['tstop'])]

		if (i==0):
			sdata_timeline_df = tmp_sdata_timeline_df
		else:
			sdata_timeline_df = sdata_timeline_df.append(tmp_sdata_timeline_df,ignore_index=True)

###### Correct library_completion_per ... set to 0 for all churn = 1 values
#sdata_timeline_df.loc[sdata_timeline_df[(sdata_timeline_df.churn == 1)].index,'library_completion_per'] = 0.0

#########################################
# Added churned account flag
#########################################
final_record_per_sfdc = sdata_timeline_df['sfdc'].reset_index().groupby('sfdc').last().reset_index() 
cancelled_accounts_NOTICE_df = pd.merge(cancelled_accounts_NOTICE_df,final_record_per_sfdc,'left',left_on='sfdc',right_on='sfdc') 
cancelled_accounts_NOTICE_df = cancelled_accounts_NOTICE_df[(pd.notnull(cancelled_accounts_NOTICE_df['index']) == True)].reset_index(drop=True)

sdata_timeline_df.loc[cancelled_accounts_NOTICE_df['index'],'churn'] = 1 

##### Remove any NULL sfdc id records
sdata_timeline_df = sdata_timeline_df[(pd.notnull(sdata_timeline_df.sfdc) == True)]

#################################################################
# 1) Add unique 'id' column for R input
#################################################################
unique_df = sdata_timeline_df[['sfdc']].drop_duplicates().reset_index(drop=True)
unique_df = pd.merge(unique_df,pd.DataFrame(range(1,len(unique_df)+1)).rename(columns={0:'id'}),'left',left_index=True,right_index=True)
sdata_timeline_df = pd.merge(sdata_timeline_df,unique_df,'left',left_on='sfdc',right_on='sfdc')
sdata_timeline_df = sdata_timeline_df.rename(columns = {'lifetime_day':'tstart'})

####################################
# Merge account strata of interest
####################################
sdata_timeline_df = pd.merge(sdata_timeline_df,account_msa_df[['sfdc',BASE_DATE]],'left',left_on='sfdc',right_on='sfdc') 
sdata_timeline_df = pd.merge(sdata_timeline_df,account_df[['Id','HCR_Only__c','Initial_Term_Length__c','NlineItem_Initial','industry','Benefits_Eligible_Employees__c']],'left',left_on='sfdc',right_on='Id').drop('Id',1) 
sdata_timeline_df = pd.merge(sdata_timeline_df,account_product_line_df,'left',left_on='sfdc',right_on='AccountId') 
if (G2_FIRST_VIEW == True):
	sdata_timeline_df = pd.merge(sdata_timeline_df,g2_initial_view_df[['sfdc','first_view_month','first_view_qtr']],'left',left_on='sfdc',right_on='sfdc') 

################################################
# Filter out noisy accounts ... REMOVE_ACCOUNTS
################################################
tdata_timeline_df = tdata_timeline_df[~tdata_timeline_df['sfdc'].isin(REMOVE_ACCOUNTS)].reset_index(drop=True)
sdata_timeline_df = sdata_timeline_df[~sdata_timeline_df['sfdc'].isin(REMOVE_ACCOUNTS)].reset_index(drop=True)

#############################
# Added to deal with duplicate sfdc_ids ... 
# 0015000000styIa 
# 0015000000slNI1
#############################
sdata_timeline_df = sdata_timeline_df.drop_duplicates(['sfdc','tstart','tstop','event']).reset_index(drop=True)

sdata_timeline_df['library_completion_per'] = sdata_timeline_df['Nvideo'] / sdata_timeline_df['library_size']
sdata_timeline_df.loc[(pd.isnull(sdata_timeline_df['library_completion_per']) == True),'library_completion_per'] = -1	
sdata_timeline_df.loc[sdata_timeline_df['library_completion_per'] == float('Inf'),'library_completion_per'] = -1

tdata_timeline_df.to_csv('./output/tdata_timeline_df.csv')
sdata_timeline_df.to_csv('./output/sdata_timeline_df.csv')

printf("[churn_timeseries.py]: sdata_timeline_df is scrubbed/updated ... %.2f sec\n",time.time()-start)

################################################
# Print out final value for each account 
################################################
sdata_timeline_MAXIDX_df = sdata_timeline_df[['sfdc']].reset_index().groupby(['sfdc'],as_index=False).agg({'index':max}).rename(columns={'index':'Nmax_idx'})
sdata_timeline_MAX_RECORD_ONLY_df = sdata_timeline_df.ix[sdata_timeline_MAXIDX_df['Nmax_idx']].reset_index(drop=True)
sdata_timeline_MAX_RECORD_ONLY_df.to_csv('./output/sdata_timeline_MAX_RECORD_ONLY_df.csv')

################################################
# Output 3rd / 4th year strata
################################################
MODELTIME_3rd = 730
MODELTIME_4th = 1095 

sdata_timeline_df = sdata_timeline_df.reset_index().rename(columns={'index':'csv_index'})

sdata_timeline_filter_3rd_df = sdata_timeline_df[sdata_timeline_df['tstart'] > MODELTIME_3rd].reset_index()
sdata_timeline_filter_4th_df = sdata_timeline_df[sdata_timeline_df['tstart'] > MODELTIME_4th].reset_index()

sdata_timeline_filter_PRERECORD_3rd_df = sdata_timeline_filter_3rd_df[['sfdc','tstart','index']] \
                                    .groupby(['sfdc']).agg({'index':min}) \
                                    .rename(columns={'index':'min_index'}) \
									.reset_index()
sdata_timeline_filter_PRERECORD_4th_df = sdata_timeline_filter_4th_df[['sfdc','tstart','index']] \
                                    .groupby(['sfdc']).agg({'index':min}) \
                                    .rename(columns={'index':'min_index'}) \
									.reset_index()
sdata_timeline_filter_PRERECORD_3rd_df['min_index'] = [x-1 for x in sdata_timeline_filter_PRERECORD_3rd_df['min_index']]   
sdata_timeline_filter_PRERECORD_4th_df['min_index'] = [x-1 for x in sdata_timeline_filter_PRERECORD_4th_df['min_index']]   

sdata_timeline_filter_MININDEX_3rd_df = pd.merge(sdata_timeline_df,sdata_timeline_filter_PRERECORD_3rd_df,'right',left_on='sfdc',right_on='sfdc')
sdata_timeline_filter_MININDEX_4th_df = pd.merge(sdata_timeline_df,sdata_timeline_filter_PRERECORD_4th_df,'right',left_on='sfdc',right_on='sfdc')

sdata_timeline_3rd_df = sdata_timeline_filter_MININDEX_3rd_df[sdata_timeline_filter_MININDEX_3rd_df['csv_index'] >= sdata_timeline_filter_MININDEX_3rd_df['min_index']].reset_index(drop=True)
sdata_timeline_4th_df = sdata_timeline_filter_MININDEX_4th_df[sdata_timeline_filter_MININDEX_4th_df['csv_index'] >= sdata_timeline_filter_MININDEX_4th_df['min_index']].reset_index(drop=True)

sdata_timeline_BASELINE_3rd_df = sdata_timeline_3rd_df[sdata_timeline_3rd_df['csv_index'] == sdata_timeline_3rd_df['min_index']].reset_index(drop=True)
sdata_timeline_BASELINE_4th_df = sdata_timeline_4th_df[sdata_timeline_4th_df['csv_index'] == sdata_timeline_4th_df['min_index']].reset_index(drop=True)

sdata_timeline_3rd_df['tstart'] = sdata_timeline_3rd_df['tstart'] - MODELTIME_3rd
sdata_timeline_4th_df['tstart'] = sdata_timeline_4th_df['tstart'] - MODELTIME_4th
sdata_timeline_3rd_df['tstop'] = sdata_timeline_3rd_df['tstop'] - MODELTIME_3rd
sdata_timeline_4th_df['tstop'] = sdata_timeline_4th_df['tstop'] - MODELTIME_4th
sdata_timeline_3rd_df['final_day'] = sdata_timeline_3rd_df['final_day'] - MODELTIME_3rd
sdata_timeline_4th_df['final_day'] = sdata_timeline_4th_df['final_day'] - MODELTIME_4th

var_reset = ['Nupsell',
'Ndowntick',
'Ndowntick_count',
'Nswap',
'Ncsm_change',
'Nnps_survey',
'Nadmin_usage_day',
'Nreport_total',
'Nunique_user',
'Nview_unique_day',
'Nview_total',
'Nedits_vd1',
'Nedits_all',
'Nactivity_total']

unique_sfdc = list(set(sdata_timeline_3rd_df.sfdc)) 
for i in range(0,len(unique_sfdc)):
	if ((i % 50) == 49):
		printf('[churn_timeseries.py] %4s of %4s ... 3rd year correction ... %.2f\n',i+1,len(unique_sfdc),time.time()-start)

	sdata_idx = all_indices_CASE_SENSITIVE(unique_sfdc[i],sdata_timeline_3rd_df.sfdc)
	baseline_idx = all_indices_CASE_SENSITIVE(unique_sfdc[i],sdata_timeline_BASELINE_3rd_df.sfdc)
	for j in range(0,len(var_reset)):
		for k in range(0,len(sdata_idx)):
			sdata_timeline_3rd_df.loc[sdata_idx[k],var_reset[j]] = float(sdata_timeline_3rd_df.ix[sdata_idx[k]][var_reset[j]]) - float(sdata_timeline_BASELINE_3rd_df.ix[baseline_idx][var_reset[j]])
sdata_timeline_3rd_df.to_csv('./output/sdata_timeline_3rd_df.csv')

unique_sfdc = list(set(sdata_timeline_4th_df.sfdc)) 
for i in range(0,len(unique_sfdc)):
	if ((i % 50) == 49):
		printf('[churn_timeseries.py] %4s of %4s ... 4th year correction ... %.2f\n',i+1,len(unique_sfdc),time.time()-start)

	sdata_idx = all_indices_CASE_SENSITIVE(unique_sfdc[i],sdata_timeline_4th_df.sfdc)
	baseline_idx = all_indices_CASE_SENSITIVE(unique_sfdc[i],sdata_timeline_BASELINE_4th_df.sfdc)
	for j in range(0,len(var_reset)):
		for k in range(0,len(sdata_idx)):
			sdata_timeline_4th_df.loc[sdata_idx[k],var_reset[j]] = float(sdata_timeline_4th_df.ix[sdata_idx[k]][var_reset[j]]) - float(sdata_timeline_BASELINE_4th_df.ix[baseline_idx][var_reset[j]])
sdata_timeline_4th_df.to_csv('./output/sdata_timeline_4th_df.csv')

