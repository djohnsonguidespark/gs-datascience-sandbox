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

# Logging
import log_libs as log
LOG = log.init_logging()

cur_datetime = datetime.now() - timedelta(hours = 8)
execfile('/home/analytics/analytics_sandbox/python_libs/stuff.py')

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',400)

con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

UPDATE_ATTASK_LINEITEM = False 

CREATE_NEW_SUMMARY_DATA = True
OP_HISTORY = True 
STAGE_DURATION = True
CLOSE_DATE = True
DEAL_SIZE = True

DEBUG = False  # True --> This is for a single / group of unique Opportunity Ids

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

########## SIMPLE SALESFORCE ##############
sf = Salesforce(username='djohnson@guidespark.com', password=pwd,security_token=token)

###################
# Print all tables
###################
objects = [] 
for x in sf.describe()["sobjects"]:
	objects.append(x["label"])
objects.sort()
pd.DataFrame(objects).rename(columns={0:'sfdc_object'}).to_csv("./output/sfdc_objects.csv")

##########################
# Define Columns
##########################
stage_cols = ['0','1','2','3','4','5','6','7','8']
ex_cols = ['stageback','won','lost']
close_cols = ['close_change','close_push','close_pullin']
amount_cols = ['amount_change','amount_up','amount_down','amount_per']

if (CREATE_NEW_SUMMARY_DATA == True):
	#########################################
	## op_df ... Query Opportunity Database 
	#########################################
	if (OP_HISTORY == True):
		LOG.info("Salesforce ... Query Opportunity Object ... {:.2f} sec".format(time.time()-start))
	
		query = "SELECT Id,AccountId,OwnerId,Owner.Name,Name,Amount,Type,LeadSource,Close_Date__c,Subscription_Start_Date__c,Renewal_Date__c, \
					LastActivityDate,StageName,IsWon,No_of_Videos__c, \
					Account.Name,Account.Industry,Account.Industry_Text__c,Account.Sic,Account.All_US_SIC_Codes__c,Account.NaicsCode,Account.All_NAICS_Codes__c, \
					Account.Customer_Lifespan__c,Account.Yearly_Client_ARR__c,Account.Customer_Success_Manager1__c, \
					Primary_Contact__c,Product_Line__c,ARR__c,Churn_Date__c,Initial_Term__c, \
					Account.AnnualRevenue,Account.ARR_At_Risk__c,Account.Account_Health__c, \
					Account.Health_Category__c,Account.Health_Category_Reason__c, \
					Account.Account_Status__c,Account.NumberOfEmployees, \
					Account.Total_Employees__c,Account.MSA_Effective_Date__c,Benefits_Eligible_Employees__c,Upgrade__c FROM Opportunity" ## WHERE IsWon = TRUE"
		op_output = sf.query_all(query)
		op_df = pd.DataFrame(op_output['records']).drop('attributes',1)
		op_df['Id'] = [x[0:15] for x in op_df.Id] ### Change OpportunityId to 15 characters
		op_df['AccountId_18'] = [x for x in op_df.AccountId]
		op_df['AccountId'] = [x[0:15] for x in op_df.AccountId] ### Change AccountId to 15 characters
		op_df = op_df.join(pd.DataFrame(map((lambda item: item['Account']['MSA_Effective_Date__c']),op_output['records'])))
		op_df = op_df.rename(columns={0:'MSA_Effective_Date__c'})
		op_df = op_df.join(pd.DataFrame(map((lambda item: item['Account']['Name']),op_output['records'])))
		op_df = op_df.rename(columns={0:'AccountName'})
		op_df = op_df.join(pd.DataFrame(map((lambda item: item['Owner']['Name']),op_output['records']))).drop('Owner',1)
		op_df = op_df.rename(columns={0:'Owner'})
		op_df = op_df.drop('Account',1)
	
		### Convert objects to date elements
		op_df['MSA_Effective_Date'] = pd.to_datetime(op_df['MSA_Effective_Date__c'])
		op_df['Close_Date'] = pd.to_datetime(op_df['Close_Date__c'])
	
		op_df_BAK = op_df.copy(deep=True)
	
		#################################################################
		# Find all accounts that either won or lost an opportunity
		#################################################################
		op_wonlost_df = op_df[['AccountId_18','AccountName','Type','StageName','Close_Date__c']][(op_df['StageName'].str.contains('Lost')) | (op_df['StageName'].str.contains('Won'))].reset_index(drop=True)
		account_wonlost_df = op_wonlost_df[['AccountId_18','AccountName']].drop_duplicates().reset_index(drop=True)
	
		Nop_bin = len(account_wonlost_df)/500
		for j in range(0,Nop_bin+1):	
			LOG.info("{:>5} of {} ... {},{}".format(j*500,len(account_wonlost_df),(j*500),((j+1)*500-1) ))
			account_test_df = read_IN_sfdc_accounts(sf,"','".join(account_wonlost_df.ix[(j*500):((j+1)*500-1)]['AccountId_18']))
	
			if (j == 0):
				account_df = account_test_df.copy(deep=True)
			else:
				account_df = account_df.append(account_test_df,ignore_index=True)
	
		#################################
		## Get opportunity history ...
		#################################
		LOG.info("Touch Up Opportunity History ... {:.2f} sec".format(time.time()-start))
	
		opportunity_history_df = sfdc_opportunity_history_query_account(sf,account_df[['AccountId_18','Name']])
	
		opportunity_history_df['CreatedDate'] = pd.to_datetime(opportunity_history_df['CreatedDate'])
		op_created_date_df = opportunity_history_df[['OpportunityId','CreatedDate']][(opportunity_history_df.Field == 'created') | (opportunity_history_df.Field == 'opportunityCreatedFromLead')].rename(columns={'CreatedDate':'Op_CreatedDate'}).reset_index().rename(columns={'index':'op_index'})
		op_close_df = opportunity_history_df['OpportunityId'][(opportunity_history_df.Field == 'ForecastCategoryName') & (opportunity_history_df.NewValue == 'Closed')].drop_duplicates().reset_index().drop('index',1)
		op_close_df = pd.merge(op_close_df,pd.DataFrame([1]*len(op_close_df)).rename(columns={0:'op_closed'}),'left',left_index=True,right_index=True)
		
		op_stage_df = opportunity_history_df[['OpportunityId','CreatedDate','Field','NewValue','OldValue']][(opportunity_history_df.Field == 'StageName')].reset_index()
		op_stage_first_df = op_stage_df[['OpportunityId','CreatedDate','NewValue','OldValue']].groupby('OpportunityId').first().reset_index()
		op_created_date_df = pd.merge(op_created_date_df,op_stage_first_df[['OpportunityId','OldValue']],'left',left_on='OpportunityId',right_on='OpportunityId')
		op_created_date_df['OldValue'] = op_created_date_df['OldValue'].fillna('1) Qualify & Discover')
		
		for i in range(0,len(op_created_date_df)):
			opportunity_history_df.loc[op_created_date_df.ix[i]['op_index'],'NewValue'] = op_created_date_df.ix[i]['OldValue']	
		
		opportunity_history_df = pd.merge(opportunity_history_df,op_created_date_df[['OpportunityId','Op_CreatedDate']],'left',left_on='OpportunityId',right_on='OpportunityId')   
		opportunity_history_df['lifetime_day'] = (opportunity_history_df['CreatedDate'] - opportunity_history_df['Op_CreatedDate']).astype('timedelta64[D]') 
		
		opportunity_history_df = pd.merge(opportunity_history_df,op_close_df,'left',left_on='OpportunityId',right_on='OpportunityId').fillna(0)   
		opportunity_history_df = pd.merge(opportunity_history_df,op_df[['Id','ARR__c','Name','LeadSource','OwnerId']],'left',left_on='OpportunityId',right_on='Id')
		opportunity_history_df = pd.merge(opportunity_history_df,account_df[['Id','NaicsCode','Initial_Term_Length__c','NumberOfEmployees','Benefits_Eligible_Employees__c','Product_Line__c']],'left',left_on='AccountId',right_on='Id')
	
		#############################
		# Remove Yr 2 / etc ... ops
		#############################
		opportunity_history_df = opportunity_history_df[(opportunity_history_df.Name.str.contains('Year 2') == False) & \
													  (opportunity_history_df.Name.str.contains('Year 3') == False) & \
													  (opportunity_history_df.Name.str.contains('Year 4') == False) & \
													  (opportunity_history_df.Name.str.contains('Year 5') == False) & \
													  (opportunity_history_df.Name.str.contains('Year 6') == False)].reset_index(drop=True)	
	
		#############################
		# Remove Dell Boomi info
		#############################
		opportunity_history_df = opportunity_history_df[(opportunity_history_df.Field != 'Boomi_WF_Project_Created__c')].reset_index(drop=True)
	
		####################
		# Set current month
		####################
		opportunity_history_df['month_created'] = [x.month for x in opportunity_history_df['Op_CreatedDate']]
		opportunity_history_df = opportunity_history_df.rename(columns={'Field':'event'}) 
	
		opportunity_history_df_BAK = opportunity_history_df.copy(deep=True)
	
	else:
		op_df = op_df_BAK.copy(deep=True)
		opportunity_history_df = opportunity_history_df_BAK.copy(deep=True)
	
	###################################
	# Put data in Cox Regression format
	# 1) Get date range for each stage
	# 2) Mark if a stage goes backwards
	#
	###################################
	
	if (DEBUG == False):
		unique_op = list(set(opportunity_history_df.OpportunityId))
	else:
		unique_op = ['0065000000YpyPf']
		#unique_op = ['0063800000apZD9','0065000000YpYTb' ]
		#unique_op = ['0063800000apZD9']
		#unique_op = ['0065000000YpYTb']
		#unique_op = ['0065000000RV3VH']
		#unique_op = ['0063800000aAnWW']
		#unique_op = ['0063800000aC8Ej']
		#unique_op = ['0063800000YsUgO']
		#unique_op = ['0063800000ZgnJW']    ### Odd outlier ... final Closed Lost not posting properly ... my guess is that it is before the final record
		#0063800000bAYJD
		#0063800000bxpUg
		#0063800000ZgnJW
		#0063800000ZIVF9
		#unique_op = ['0063800000YTQYJ']
	
	###########################
	# Create Final Stage DF
	###########################
	final_stage = pd.DataFrame(unique_op).rename(columns={0:'OpportunityId'})
	final_stage['final_stage'] = ['0'] * len(final_stage)
	
	###############################
	###############################
	# 1) Update StageName info
	###############################
	###############################
	
	if (STAGE_DURATION == True):
		LOG.info("STAGE DURATION START ... {:.2f} sec".format(time.time()-start))
		op_output_df = opportunity_history_df[['OpportunityId','CreatedDate','Op_CreatedDate','lifetime_day','event','NewValue','OldValue']] \
								.reset_index().rename(columns={'index':'op_index'})
		#op_output_df = opportunity_history_df[['OpportunityId','CreatedDate','Op_CreatedDate','lifetime_day','event','NewValue','OldValue']] \
		#						[(opportunity_history_df.event == 'created') | (opportunity_history_df.event == 'opportunityCreatedFromLead') | (opportunity_history_df.event == 'StageName')] \
		#						.reset_index().rename(columns={'index':'op_index'})
	
	#	op_output_df['NEW_stagenum'] = [x[0] for x in op_output_df.NewValue]
	#	op_output_df['OLD_stagenum'] = [str(x)[0] for x in op_output_df.OldValue]
	
		stage_time_df = pd.DataFrame(0,index=list(range(0,len(opportunity_history_df))),columns=(stage_cols + ex_cols))
		#stage_time_df['stageback'] = [0] * len(opportunity_history_df)
		
		for i in range(0,len(unique_op)):
			if ((i % 500) == 499):
				LOG.info("STAGE DURATION: Unique Op ... {:>5} of {:>5} ... {:.2f} sec".format(i+1,len(unique_op),time.time()-start))
		
			idx = all_indices_CASE_SENSITIVE(unique_op[i],op_output_df['OpportunityId'])
	
			NEW_stagenum = '0'
			OLD_stagenum = '0'
			cur_stage_date = op_output_df.ix[idx[0]]['Op_CreatedDate']
	
			if (len(idx) > 0):
				stage_time_df.loc[op_output_df.ix[idx[0]]['op_index'],OLD_stagenum] = 0
				NEW_stagenum = str(op_output_df.ix[idx[0]]['NewValue'])[0]
				OLD_stagenum = str(op_output_df.ix[idx[0]]['OldValue'])[0]
				#stage_time_df.loc[op_output_df.ix[idx[0]]['op_index'],op_output_df.ix[idx[0]]['OLD_stagenum']] = 0
				Nstageback = 0
				for j in range(1,len(idx)):
					cur_event = op_output_df.ix[idx[j]]['event']
					#LOG.info("{:>5} ... ".format(idx[j]))
					cur_idx = op_output_df.ix[idx[j]]['op_index']
					prev_idx = op_output_df.ix[idx[j-1]]['op_index']
					if ((cur_event == 'created') | (cur_event == 'opportunityCreatedFromLead') | (cur_event == 'StageName')):
						NEW_stagenum = str(op_output_df.ix[idx[j]]['NewValue'])[0]
						OLD_stagenum = str(op_output_df.ix[idx[j]]['OldValue'])[0]
						#LOG.info("In-Loop ... OLD,NEW = {:>1},{:>1}".format(OLD_stagenum,NEW_stagenum))
			
						##################################################
						# Update changing element / reset stage base date
						##################################################
						stage_time_df.loc[cur_idx,OLD_stagenum] = stage_time_df.ix[prev_idx][OLD_stagenum] \
																					+ (op_output_df.ix[cur_idx]['CreatedDate'] - op_output_df.ix[prev_idx]['CreatedDate']).days
						cur_stage_date = op_output_df.ix[cur_idx]['CreatedDate']
						#stage_time_df.loc[cur_idx,op_output_df.ix[idx[j]]['OLD_stagenum']] = stage_time_df.ix[prev_idx][op_output_df.ix[idx[j]]['OLD_stagenum']] \
						#															+ (op_output_df.ix[idx[j]]['CreatedDate'] - op_output_df.ix[idx[j-1]]['CreatedDate']).days
						###############################
						# Update other elements
						###############################
						cur_stage_cols = extra_val(stage_cols,OLD_stagenum)
						stage_time_df.loc[op_output_df.ix[cur_idx]['op_index'],cur_stage_cols] = stage_time_df.ix[prev_idx][cur_stage_cols]
		
						###############################
						# Update 'stageback' 
						###############################
						if (NEW_stagenum < OLD_stagenum):
							Nstageback = Nstageback + 1
							#stage_time_df.loc[cur_idx,NEW_stagenum] = stage_time_df.ix[prev_idx][NEW_stagenum]
						stage_time_df.loc[op_output_df.ix[cur_idx]['op_index'],'stageback'] = Nstageback
		
						###############################
						# Update 'won' & 'lost' 
						###############################
						if ('Closed Won' in str(op_output_df.ix[cur_idx]['NewValue'])):
							stage_time_df.loc[op_output_df.ix[cur_idx]['op_index'],'won'] = 1
						if ('Closed Lost' in str(op_output_df.ix[cur_idx]['NewValue'])):
							stage_time_df.loc[op_output_df.ix[cur_idx]['op_index'],'lost'] = 1
	#					if (op_output_df.ix[idx[j]]['NEW_stagenum'] == '6'):
	#						stage_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'won'] = 1
	#					if (op_output_df.ix[idx[j]]['NEW_stagenum'] == '7'):
	#						stage_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'lost'] = 1
	
					else:	
						#LOG.info("No-Loop ... OLD,NEW = {:>1},{:>1}".format(OLD_stagenum,NEW_stagenum))
						if (NEW_stagenum >= OLD_stagenum):
							if (stage_time_df.ix[prev_idx][NEW_stagenum] > 0):
								stage_time_df.loc[cur_idx,NEW_stagenum] = stage_time_df.ix[prev_idx][NEW_stagenum] \
																					+ (op_output_df.ix[idx[j]]['CreatedDate'] - cur_stage_date).days
								#cur_stage_date = op_output_df.ix[cur_idx]['CreatedDate']
							else:
								stage_time_df.loc[cur_idx,NEW_stagenum] = (op_output_df.ix[cur_idx]['CreatedDate'] - cur_stage_date).days  
						else:
							stage_time_df.loc[cur_idx,NEW_stagenum] = stage_time_df.ix[prev_idx][NEW_stagenum] \
																					+ (op_output_df.ix[idx[j]]['CreatedDate'] - cur_stage_date).days
						cur_stage_date = op_output_df.ix[cur_idx]['CreatedDate']
	
						###############################
						# Update other elements
						###############################
						cur_stage_cols = extra_val(stage_cols,NEW_stagenum)
						stage_time_df.loc[op_output_df.ix[cur_idx]['op_index'],cur_stage_cols] = stage_time_df.ix[prev_idx][cur_stage_cols]
	
				final_stage.loc[all_indices_CASE_SENSITIVE(unique_op[i],final_stage['OpportunityId']),'final_stage'] = 's' + str(NEW_stagenum)
		
				#	if (cur_idx == 18583):
				#		LOG.info("%s".format(stage_time_df.ix[idx][['1','2','3','4','5','6','7','stageback']]))
				#		sys.exit()
		
				
		timeline_df = pd.merge(opportunity_history_df,stage_time_df,'left',left_index=True,right_index=True)
	
		LOG.info("STAGE DURATION END   ... {:.2f} sec".format(time.time()-start))
		
		LOG.info("\n{}".format(timeline_df[['AccountId_18','OpportunityId','CreatedDate','Op_CreatedDate','lifetime_day','event','NewValue','OldValue'] + stage_cols + ex_cols] \
		                            [(timeline_df.OpportunityId == unique_op[0])]))
	
	###############################
	###############################
	# 2) Close Date 
	###############################
	###############################
	
	if (CLOSE_DATE == True):
		op_output_df = opportunity_history_df[['OpportunityId','CreatedDate','Op_CreatedDate','lifetime_day','event','NewValue','OldValue']] \
							[(opportunity_history_df.event == 'CloseDate')] \
							.reset_index().rename(columns={'index':'op_index'})
	
		op_output_df['NewValue'] = pd.to_datetime(op_output_df['NewValue'])
		op_output_df['OldValue'] = pd.to_datetime(op_output_df['OldValue'])
	
		close_time_df = pd.DataFrame(0,index=list(range(0,len(opportunity_history_df))),columns=(close_cols))
	
		#unique_op = list(set(op_output_df.OpportunityId))
		#unique_op = ['0065000000YpYTb']
		#unique_op = ['0063800000anDZp']
	
		for i in range(0,len(unique_op)):
			if ((i % 500) == 499):
				LOG.info("CLOSE DATE: Unique Op ... {:>5} of {:>5} ... {:.2f} sec".format(i+1,len(unique_op),time.time()-start))
		
			idx = all_indices_CASE_SENSITIVE(unique_op[i],op_output_df['OpportunityId'])
	
			if (len(idx) > 0):
				Nclose_change = 0
				Nclose_push = 0
				Nclose_pullin = 0
				for j in range(0,len(idx)):
					######################################
					# Check if date is > or < than current
					######################################
					if (op_output_df.ix[idx[j]]['NewValue'] > op_output_df.ix[idx[j]]['OldValue']):
						Nclose_change = Nclose_change + 1
						Nclose_push = Nclose_push + 1
					elif (op_output_df.ix[idx[j]]['NewValue'] < op_output_df.ix[idx[j]]['OldValue']):
						Nclose_change = Nclose_change + 1
						Nclose_pullin = Nclose_pullin + 1
	
					close_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'close_change'] = Nclose_change
					close_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'close_push']   = Nclose_push
					close_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'close_pullin'] = Nclose_pullin
		
		timeline_df = pd.merge(timeline_df,close_time_df,'left',left_index=True,right_index=True)
		
		LOG.info("\n{}".format(timeline_df[['AccountId_18','OpportunityId','CreatedDate','Op_CreatedDate','lifetime_day','event','NewValue','OldValue'] + close_cols] \
									[(timeline_df.OpportunityId == unique_op[0]) & (timeline_df.event == 'CloseDate') ]) )
	
	###################
	###################
	# 3) Amount 
	###################
	###################
	
	if (DEAL_SIZE == True):
		op_output_df = opportunity_history_df[['OpportunityId','CreatedDate','Op_CreatedDate','lifetime_day','event','NewValue','OldValue']] \
							[(opportunity_history_df.event == 'Amount')] \
							.reset_index().rename(columns={'index':'op_index'})
	
		amount_time_df = pd.DataFrame(0,index=list(range(0,len(opportunity_history_df))),columns=(amount_cols))
	
		#unique_op = list(set(op_output_df.OpportunityId))
		#unique_op = ['0065000000YpYTb']
		#unique_op = ['0063800000anDZp']
	
		for i in range(0,len(unique_op)):
			if ((i % 500) == 499):
				LOG.info("DEAL SIZE: Unique Op ... {:>5} of {:>5} ... {:.2f} sec".format(i+1,len(unique_op),time.time()-start) )
		
			idx = all_indices_CASE_SENSITIVE(unique_op[i],op_output_df['OpportunityId'])
			if (len(idx) > 0):
				Namount_change = 0
				Namount_up = 0
				Namount_down = 0
				cur_lifetime_day = 0
				cur_amount = 0
				for j in range(0,len(idx)):
					######################################
					# Check if date is > or < than current
					######################################
					if (op_output_df.ix[idx[j]]['NewValue'] > op_output_df.ix[idx[j]]['OldValue']):
						Namount_change = Namount_change + 1
						Namount_up = Namount_up + 1
					elif (op_output_df.ix[idx[j]]['NewValue'] < op_output_df.ix[idx[j]]['OldValue']):
						Namount_change = Namount_change + 1
						Namount_down = Namount_down + 1
	
					amount_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'amount_change'] = Namount_change
					amount_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'amount_up']   = Namount_up
					amount_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'amount_down'] = Namount_down
					if (op_output_df.ix[idx[j]]['lifetime_day'] != cur_lifetime_day): 
						cur_lifetime_day = op_output_df.ix[idx[j]]['lifetime_day']
						cur_amount = int(float(op_output_df.ix[idx[j]]['OldValue']))
						if (int(float(op_output_df.ix[idx[j]]['OldValue'])) != 0): 
							amount_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'amount_per'] = float(op_output_df.ix[idx[j]]['NewValue']) / float(op_output_df.ix[idx[j]]['OldValue']) 
						else:
							amount_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'amount_per'] = 0  
					else:
						if (cur_amount != 0): 
							amount_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'amount_per'] = float(op_output_df.ix[idx[j]]['NewValue']) / cur_amount 
						else:
							amount_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'amount_per'] = 0  
		
		timeline_df = pd.merge(timeline_df,amount_time_df,'left',left_index=True,right_index=True)
		
		LOG.info("\n{}".format(timeline_df[['AccountId_18','OpportunityId','CreatedDate','Op_CreatedDate','lifetime_day','event','NewValue','OldValue'] + amount_cols] \
									[(timeline_df.OpportunityId == unique_op[0]) & (timeline_df.event == 'Amount') ]) )
	
	######################################################
	######################################################
	######################################################
	#################### Clean Data ######################  
	######################################################
	######################################################
	######################################################
	op_idx = min(all_indices_CASE_SENSITIVE('0065000000YpYTb',timeline_df['OpportunityId']))
		
	## STAGE DURATION 
	if (STAGE_DURATION == True):
		#out_cols = stage_cols + ex_cols
		out_cols = ex_cols
		for i in range(0,len(unique_op)):
	
			if ((i % 500) == 499):
				LOG.info("STAGE_DURATION: Clean Data ... {:>5} of {:>5} ... {:.2f} sec".format(i+1,len(unique_op),time.time()-start) )
	
			Ncol_val = [0] * len(timeline_df)
	
			idx = all_indices_CASE_SENSITIVE(unique_op[i],timeline_df['OpportunityId'])
		
			for j in range(0,len(idx)):
				if (timeline_df.ix[idx[j]]['event'] == 'StageName'):
					for k in range(0,len(out_cols)):
						Ncol_val[k] = timeline_df.ix[idx[j]][out_cols[k]]
	
				for k in range(0,len(out_cols)):
					timeline_df.loc[idx[j],out_cols[k]] = Ncol_val[k]
	
		LOG.info("\n{}".format(timeline_df[['AccountId_18','OpportunityId','CreatedDate','Op_CreatedDate','lifetime_day','event','NewValue','OldValue'] + stage_cols + ex_cols] \
									[(timeline_df.OpportunityId == unique_op[0])]) )
	
	LOG.info("Data Cleaned: SFDC Activities ... {:.2f} sec".format(time.time()-start))
	
	
	## CLOSE_DATE
	if (CLOSE_DATE == True):
		out_cols = close_cols
		for i in range(0,len(unique_op)):
	
			if ((i % 500) == 499):
				LOG.info("CLOSE_DATE: Clean Data ... {:>5} of {:>5} ... {:.2f} sec".format(i+1,len(unique_op),time.time()-start) )
	
			Ncol_val = [0] * len(timeline_df)
	
			idx = all_indices_CASE_SENSITIVE(unique_op[i],timeline_df['OpportunityId'])
		
			for j in range(0,len(idx)):
				if (timeline_df.ix[idx[j]]['event'] == 'CloseDate'):
					for k in range(0,len(out_cols)):
						Ncol_val[k] = timeline_df.ix[idx[j]][out_cols[k]]
	
				for k in range(0,len(out_cols)):
					timeline_df.loc[idx[j],out_cols[k]] = Ncol_val[k]
	
		LOG.info("\n{}".format(timeline_df[['AccountId_18','OpportunityId','CreatedDate','Op_CreatedDate','lifetime_day','event','NewValue','OldValue'] + close_cols + ex_cols] \
									[(timeline_df.OpportunityId == unique_op[0])]) )
	
	LOG.info("Data Cleaned: SFDC Activities ... {:.2f} sec".format(time.time()-start) )
	
	## DEAL_SIZE 
	if (DEAL_SIZE == True):
		out_cols = amount_cols
		for i in range(0,len(unique_op)):
	
			if ((i % 500) == 499):
				LOG.info("DEAL_SIZE: Clean Data ... {:>5} of {:>5} ... {:.2f} sec".format(i+1,len(unique_op),time.time()-start) )
	
			Ncol_val = [0] * len(timeline_df)
	
			idx = all_indices_CASE_SENSITIVE(unique_op[i],timeline_df['OpportunityId'])
		
			for j in range(0,len(idx)):
				if (timeline_df.ix[idx[j]]['event'] == 'Amount'):
					for k in range(0,len(out_cols)):
						Ncol_val[k] = timeline_df.ix[idx[j]][out_cols[k]]
	
				for k in range(0,len(out_cols)):
					timeline_df.loc[idx[j],out_cols[k]] = Ncol_val[k]
	
		LOG.info("\n{}".format(timeline_df[['AccountId_18','OpportunityId','CreatedDate','Op_CreatedDate','lifetime_day','event','NewValue','OldValue'] + amount_cols + ex_cols] \
									[(timeline_df.OpportunityId == unique_op[0])]) )
	
	LOG.info("Data Cleaned: SFDC Activities ... {:.2f} sec".format(time.time()-start) )
	
	#################
	# Add final day
	#################
	final_day = []
	timeline_df['final_day'] = [None] * len(timeline_df)
	for i in range(0,len(unique_op)):
	
		if ((i % 500) == 499):
			LOG.info("Add Final Day ... {:>5} of {:>5} ... {:.2f} sec".format(i+1,len(unique_op),time.time()-start) )
	
		idx = all_indices_CASE_SENSITIVE(unique_op[i],timeline_df['OpportunityId'])
		
		base_day = timeline_df.ix[idx[0]]['Op_CreatedDate']
		if (timeline_df.ix[idx[len(idx)-1]]['won'] != 0):
			final_idx = max(all_indices_CASE_SENSITIVE(unique_op[i],timeline_df['OpportunityId']))
			try:
				timeline_df.loc[idx,'final_day'] = timeline_df.ix[final_idx]['lifetime_day']
			except:
				timeline_df.loc[idx,'final_day'] = None
		elif ('Lost' in str(timeline_df.ix[idx[len(idx)-1]]['NewValue']) or 'Omitted' in str(timeline_df.ix[idx[len(idx)-1]]['NewValue']) ):
			final_idx = max(all_indices_CASE_SENSITIVE(unique_op[i],timeline_df['OpportunityId']))
			try:
				timeline_df.loc[idx,'final_day'] = timeline_df.ix[final_idx]['lifetime_day']
			except:
				timeline_df.loc[idx,'final_day'] = None
		else:   #### Make final day = the current day since they have not churned
			try:
				timeline_df.loc[idx,'final_day'] = (cur_datetime - base_day).days
			except:
				timeline_df.loc[idx,'final_day'] = None
	
	timeline_df.to_csv('./output/sfdc_opportunity_history_' + cur_datetime.strftime('%Y%m%d') + '.csv',encoding='utf-8')
	
else:
	timeline_df = pd.read_csv('./output/sfdc_opportunity_history_' + cur_datetime.strftime('%Y%m%d') + '.csv',index_col=[0])
	unique_op = list(set(timeline_df.OpportunityId))

	###########################
	# Create Final Stage DF
	###########################
	final_stage = pd.DataFrame(unique_op).rename(columns={0:'OpportunityId'})
	final_stage['final_stage'] = ['0'] * len(final_stage)
	
won_df = timeline_df[['OpportunityId','final_day','won']].groupby(['OpportunityId','final_day']).sum().reset_index()
won_df['won'][won_df.won > 0] = 1
lost_df = timeline_df[['OpportunityId','final_day','lost']].groupby(['OpportunityId','final_day']).sum().reset_index()
lost_df['lost'][lost_df.lost > 0] = 1

######################################################
######################################################
######################################################
################### Output Data ######################  
######################################################
######################################################
######################################################

timeline_df = timeline_df.rename(columns={'1':'s1'})
timeline_df = timeline_df.rename(columns={'2':'s2'})
timeline_df = timeline_df.rename(columns={'3':'s3'})
timeline_df = timeline_df.rename(columns={'4':'s4'})
timeline_df = timeline_df.rename(columns={'5':'s5'})
timeline_df = timeline_df.rename(columns={'6':'s6'})
timeline_df = timeline_df.rename(columns={'7':'s7'})
timeline_df = timeline_df.rename(columns={'8':'s8'})

MODEL_INDEPENDENT_VARIABLES = []
if (STAGE_DURATION  == True):
	MODEL_INDEPENDENT_VARIABLES.append('s1')
	MODEL_INDEPENDENT_VARIABLES.append('s2')
	MODEL_INDEPENDENT_VARIABLES.append('s3')
	MODEL_INDEPENDENT_VARIABLES.append('s4')
	MODEL_INDEPENDENT_VARIABLES.append('s5')
	MODEL_INDEPENDENT_VARIABLES.append('s6')
	MODEL_INDEPENDENT_VARIABLES.append('s7')
	MODEL_INDEPENDENT_VARIABLES.append('stageback')
	MODEL_INDEPENDENT_VARIABLES.append('lost')

if (CLOSE_DATE == True):
	for i in range(0,len(close_cols)):
		MODEL_INDEPENDENT_VARIABLES.append(close_cols[i])
if (DEAL_SIZE == True):
	for i in range(0,len(amount_cols)):
		MODEL_INDEPENDENT_VARIABLES.append(amount_cols[i])

#ALL_VARIABLES = ['sfdc','won_int','won','lifetime_day','final_day','Cancellation_Notice_Received','event','op_type','video_id','video_title']
#TDATA_VARIABLES = ['OpportunityId','won','lifetime_day','final_day','event','OpportunityType','Product_Line__c','NaicsCode']
TDATA_VARIABLES = ['AccountId_18','OpportunityId','won','CreatedDate','Op_CreatedDate','lifetime_day','final_day','event','OpportunityType','Product_Line__c','NaicsCode','NumberOfEmployees','LeadSource','month_created']
tdata_timeline_df = timeline_df[TDATA_VARIABLES + MODEL_INDEPENDENT_VARIABLES]

### Do we need to set won library_size to PRIOR library_size
SDATA_VARIABLES = TDATA_VARIABLES

###################################################
# Output for cox regression
###################################################
sdata_timeline_df = []
for i in range(0,len(unique_op)):

	if ((i % 500) == 499):
		LOG.info("Opportunity ... {:>5} of {:>5} ... sdata_timeline_df creation ... {:.2f} sec".format(i+1,len(unique_op),time.time()-start) )

	#####################################
	# 1) Extract data from each account
	#####################################
	tmp_tdata_timeline_df = tdata_timeline_df.ix[all_indices_CASE_SENSITIVE(unique_op[i],tdata_timeline_df.OpportunityId)]
	#tmp_tdata_timeline_df = tmp_tdata_timeline_df.drop(['video_id','video_title'],1)

	################################################################################################################
	# 2) Group records with the SAME lifetime_day
	#	  Find the MAXIMUM INDEX (Nmax_idx) for the MAXIMUM RECORD for groups that have the same lifetime_day
	################################################################################################################
	tmp_sdata_timeline_MAXIDX_df = tmp_tdata_timeline_df[['OpportunityId','lifetime_day']].reset_index().groupby(['OpportunityId','lifetime_day'],as_index=False).agg({'index':max}).rename(columns={'index':'Nmax_idx'})
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
		# 1) Remove any records AFTER 'won' date (i.e. full cancellation)
		# 2) Remove cancellation RECORD and add 'won=1' to previous record 
		# 3) Correct where tstart = tstop
		#######################################################
		# 1) 
		tmp_sdata_timeline_df = tmp_sdata_timeline_df[(tmp_sdata_timeline_df.lifetime_day <= tmp_sdata_timeline_df.tstop)].reset_index(drop=True)
		# 2) 
		#if (len(tmp_sdata_timeline_df[(tmp_sdata_timeline_df['won'] == 1)].index) == 1):
		#   won_idx = tmp_sdata_timeline_df[(tmp_sdata_timeline_df['won'] == 1)].index[0]
		#   tmp_sdata_timeline_df.loc[won_idx-1,'won'] = 1
		#   tmp_sdata_timeline_df = tmp_sdata_timeline_df[(tmp_sdata_timeline_df.index != won_idx)]
		#elif (len(tmp_sdata_timeline_df[(tmp_sdata_timeline_df['won'] == 1)].index) > 1):
		#   LOG.info("[won_timeseries.py] %s".format(tmp_sdata_timeline_df))
		# 3)

		### output for cox regression (no lifetime_day = tstop)
		tmp_sdata_timeline_COX_df = tmp_sdata_timeline_df.copy(deep=True)
		tmp_sdata_timeline_COX_df = tmp_sdata_timeline_COX_df[(tmp_sdata_timeline_COX_df['lifetime_day'] != tmp_sdata_timeline_COX_df['tstop'])]

		if (len(tmp_sdata_timeline_COX_df) > 0):
			if (i==0):
				sdata_timeline_COX_df = tmp_sdata_timeline_COX_df
			else:
				sdata_timeline_COX_df = sdata_timeline_COX_df.append(tmp_sdata_timeline_COX_df,ignore_index=True)

		### output for random survival forest (lifetime_day = tstop ... OK ... gives the final values for stage lengths)
		if (len(tmp_sdata_timeline_df) > 0):
			if (i==0):
				sdata_timeline_df = tmp_sdata_timeline_df
			else:
				sdata_timeline_df = sdata_timeline_df.append(tmp_sdata_timeline_df,ignore_index=True)

### output for cox regression (no lifetime_day = tstop)
sdata_timeline_COX_df = pd.merge(sdata_timeline_COX_df,won_df,'left',left_on=['OpportunityId','tstop'],right_on=['OpportunityId','final_day'])
sdata_timeline_COX_df['won_x'][(sdata_timeline_COX_df.won_y == 1)] = 1
sdata_timeline_COX_df = sdata_timeline_COX_df.drop({'won_y','final_day_y'},1).rename(columns={'won_x':'won','final_day_x':'final_day','lifetime_day':'tstart'})
sdata_timeline_COX_df = pd.merge(sdata_timeline_COX_df,lost_df,'left',left_on=['OpportunityId','tstop'],right_on=['OpportunityId','final_day'])
sdata_timeline_COX_df['lost_x'][(sdata_timeline_COX_df.lost_y == 1)] = 1
sdata_timeline_COX_df = sdata_timeline_COX_df.drop({'lost_y','final_day_y'},1).rename(columns={'lost_x':'lost','final_day_x':'final_day','lifetime_day':'tstart'})
sdata_timeline_COX_df = sdata_timeline_COX_df.sort(['OpportunityId','CreatedDate']).reset_index(drop=True) 
sdata_timeline_COX_df.to_csv('./output/sdata_op_history_COX.csv')

#################################
# RSF ... add final information
#################################
### 1) output for random survival forest (lifetime_day = tstop ... OK ... gives the final values for stage lengths)
sdata_timeline_df = pd.merge(sdata_timeline_df,won_df,'left',left_on=['OpportunityId','tstop','lifetime_day'],right_on=['OpportunityId','final_day','final_day'])
sdata_timeline_df['won_x'][(sdata_timeline_df.won_y == 1)] = 1
sdata_timeline_df = sdata_timeline_df.drop({'won_y','final_day_y'},1).rename(columns={'won_x':'won','final_day_x':'final_day','lifetime_day':'tstart'})
sdata_timeline_df = pd.merge(sdata_timeline_df,lost_df,'left',left_on=['OpportunityId','tstop','tstart'],right_on=['OpportunityId','final_day','final_day'])
sdata_timeline_df['lost_x'][(sdata_timeline_df.lost_y == 1)] = 1
sdata_timeline_df = sdata_timeline_df.drop({'lost_y','final_day_y'},1).rename(columns={'lost_x':'lost','final_day_x':'final_day','lifetime_day':'tstart'})

### 2) Add final stage time for those that did not WIN or LOSE
op_out = list(set(sdata_timeline_df.OpportunityId))
for i in range(0,len(op_out)):

	if ((i % 500) == 499):
		LOG.info("Opportunity ... {:>5} of {:>5} ... RSF final record update ... {:.2f} sec".format(i+1,len(unique_op),time.time()-start) )

	sidx = all_indices_CASE_SENSITIVE(op_out[i],final_stage['OpportunityId'])
	cur_stage = final_stage.ix[sidx[0]]['final_stage']  
	idx = all_indices_CASE_SENSITIVE(op_out[i],sdata_timeline_df['OpportunityId'])
	max_idx = max(idx)

	if (cur_stage != 's8'):
		if ((sdata_timeline_df.ix[max_idx]['won'] == 0) & (sdata_timeline_df.ix[max_idx]['lost'] == 0)):   
			## Add new row
			sdata_timeline_df = sdata_timeline_df.append(sdata_timeline_df.ix[max_idx],ignore_index=True)
			## Get index
			cur_idx = len(sdata_timeline_df)-1  
	
			## Update data 
			sdata_timeline_df.loc[cur_idx,'tstart'] = int(sdata_timeline_df.ix[cur_idx]['final_day'])
			sdata_timeline_df.loc[cur_idx,cur_stage] = int(sdata_timeline_df.ix[cur_idx][cur_stage] + sdata_timeline_df.ix[max_idx]['final_day'] - sdata_timeline_df.ix[max_idx]['tstart'])
			sdata_timeline_df.loc[cur_idx,'CreatedDate'] = sdata_timeline_df.ix[cur_idx]['Op_CreatedDate'] + timedelta(days = int(sdata_timeline_df.ix[cur_idx]['final_day']))

sdata_timeline_df = sdata_timeline_df.sort(['OpportunityId','CreatedDate']).reset_index(drop=True) 
if (DEBUG == False):
	sdata_timeline_df.to_csv('./output/sdata_op_history_RSF_' + cur_datetime.strftime('%Y%m%d') + '_ORIG.csv',encoding='utf-8')
	sdata_timeline_df.to_csv('./output/sdata_op_history_RSF_' + cur_datetime.strftime('%Y%m%d') + '.csv',encoding='utf-8')
	sdata_timeline_df.to_csv('./output/sdata_op_history_RSF.csv',encoding='utf-8')
else:
	sdata_timeline_df.to_csv('./output/sdata_op_history_RSF_' + cur_datetime.strftime('%Y%m%d') + '_TEST.csv',encoding='utf-8')

##############################################
##############################################
##############################################
final_cols = ['s'+x for x in stage_cols] 
final_cols = ['s'+x for x in stage_cols] 
final_cols.remove('s0') 
final_cols.remove('s8') 
LOG.info("%s".format(sdata_timeline_df[['AccountId_18','OpportunityId','CreatedDate','Op_CreatedDate','tstart','tstop','final_day','event'] + final_cols + ex_cols] \
						        [(sdata_timeline_df.OpportunityId == unique_op[0])]) )

