#! /usr/bin/env python
from __future__ import print_function

import sys
import re
import json
import csv
import MySQLdb as mdb
import pandas as pd
import numpy as np 
import math 
import time
from common_libs import *
from inspect import currentframe, getframeinfo

sys.path.insert(0,'/home/djohnson/analytics/python_libs');

# Logging
import log_libs as log
LOG = log.init_logging()

#######################################################################
# Created by DKJ ... 1/6/2016
# This reads from the sfdc activities history
# 1) User input a dataframe with sfdc account id and sfdc account name
# 2) the entire activities history for all accounts is returned
#######################################################################
def sfdc_activity_query_account(sf,account_df):

	start = time.time()

	########################################
	# activity_df ... Activities History
	########################################
	activities_df = pd.DataFrame() 
	for i in range(0,len(account_df)):

		all_records = False
		activities_tmp = {}	
		cur_query = 1
		while(not all_records):

			if (len(activities_tmp) == 0):
				CUR_DATETIME = '2001-01-01T00:00:00.000+0000'
			else:
				CUR_DATETIME = activities_tmp['CreatedDate'][len(activities_tmp['CreatedDate'])-1]

			try:	
				LOG.info("Activities History: {:>4} of {:>4} . Q{:>2} . {:>60} . ".format(i+1,len(account_df),cur_query,account_df.ix[i]['Name']) )
			except Exception as e:
				LOG.info("i = {:>4}: {}".format(i,e))

#			query = "SELECT (SELECT Id,Account.Id,Account.Name,ActivityDate,ActivityType,CallDisposition,CallDurationInSeconds, \
#							Description,IsTask,OwnerId,Owner.Name, \
#							CreatedById,CreatedDate, \
#							LastModifiedById,LastModifiedDate,WhoId,WhatId,Subject, \
#							Meeting_Outcome__c,Meeting_Location__c,Meeting_Source__c,Converted_to_Opportunity__c \
#							FROM ActivityHistories WHERE CreatedDate > %s ORDER BY CreatedDate) \
#							FROM Account \
#							WHERE Id = '%s' \
#							AND (Account_Status__c = 'Current Client' OR Account_Status__c = 'Channel Partner & Client' \
#							OR Account_Status__c = 'Financial Channel Partner & Client' OR Account_Status__c = 'Broker Channel Partner & Client' \
#							OR Account_Status__c = 'Past Client')" % (CUR_DATETIME,account_df.ix[i]['Id'])
#
			query = "SELECT (SELECT Id,Account.Id,Account.Name,ActivityDate,ActivityType,CallDisposition,CallDurationInSeconds, \
							Description,IsTask,OwnerId,Owner.Name, \
							CreatedById,CreatedDate, \
							LastModifiedById,LastModifiedDate,WhoId,WhatId,Subject, \
							Meeting_Outcome__c,Meeting_Location__c,Meeting_Source__c,Converted_to_Opportunity__c \
							FROM ActivityHistories WHERE CreatedDate > %s ORDER BY CreatedDate) \
							FROM Account \
							WHERE Id = '%s'" % (CUR_DATETIME,account_df.ix[i]['Id'])


			try:
				activities_output = sf.query_all(query)
			except:
				activities_output = sf.query_all(query)
				
			activities_tmp = {}	
			try:
				activities_tmp['sf_act_name'] = map((lambda item: item['Account']['Name']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['ActivityId'] = map((lambda item: item['Id']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['ActivityDate'] = map((lambda item: item['ActivityDate']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['ActivityType'] = map((lambda item: item['ActivityType']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['CallDisposition'] = map((lambda item: item['CallDisposition']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['CallDurationInSeconds'] = map((lambda item: item['CallDurationInSeconds']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['Description'] = map((lambda item: item['Description']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['IsTask'] = map((lambda item: item['IsTask']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['OwnerId'] = map((lambda item: item['OwnerId']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['OwnerName'] = map((lambda item: item['Owner']['Name']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['CreatedById'] = map((lambda item: item['CreatedById']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['CreatedDate'] = map((lambda item: item['CreatedDate']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['LastModifiedById'] = map((lambda item: item['LastModifiedById']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['LastModifiedDate'] = map((lambda item: item['LastModifiedDate']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['WhoId'] = map((lambda item: item['WhoId']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['WhatId'] = map((lambda item: item['WhatId']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['Subject'] = map((lambda item: item['Subject']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['MeetingOutcome'] = map((lambda item: item['Meeting_Outcome__c']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['MeetingSource'] = map((lambda item: item['Meeting_Source__c']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['MeetingLocation'] = map((lambda item: item['Meeting_Location__c']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['ConvertedOp'] = map((lambda item: item['Converted_to_Opportunity__c']),activities_output['records'][0]['ActivityHistories']['records'])

				account_id = []
				for j in range(0,len(activities_tmp['sf_act_name'])):
					#account_id.append( activities_output['records'][0]['Id'] )
					account_id.append(list(set(map((lambda item: item['Account']['Id']),activities_output['records'][0]['ActivityHistories']['records'])))[0])
				activities_tmp['AccountId'] = account_id 

			except Exception as e:
				LOG.info("i = {:>4}: {}".format(i,e))
				activities_tmp['AccountId'] = account_df.ix[i]['Id']
				#activities_tmp['sf_act_name'] = map((lambda item: item['Name']),activities_output['records'])
				#activities_tmp['sf_act_name'] = map((lambda item: item['Account']['Name']),activities_output['records'][0]['ActivityHistories']['records'])
				activities_tmp['sf_act_name'] = None
				activities_tmp['ActivityId'] = None
				activities_tmp['ActivityDate'] = None 
				activities_tmp['ActivityType'] = None 
				activities_tmp['CallDisposition'] = None 
				activities_tmp['CallDurationInSeconds'] = None 
				activities_tmp['Description'] = None 
				activities_tmp['IsTask'] = None 
				activities_tmp['OwnerId'] = None 
				activities_tmp['OwnerName'] = None 
				activities_tmp['CreatedById'] = None 
				activities_tmp['CreatedDate'] = None 
				activities_tmp['LastModifiedById'] = None 
				activities_tmp['LastModifiedDate'] = None
				activities_tmp['WhoId'] = None 
				activities_tmp['WhatId'] = None 
				activities_tmp['Subject'] = None 
				activities_tmp['MeetingOutcome'] = None 
				activities_tmp['MeetingSource'] = None 
				activities_tmp['MeetingLocation'] = None 
				activities_tmp['ConvertedOp'] = None 

			if (len(activities_tmp) > 0):
				if (len(activities_df) == 0):
					activities_df = pd.DataFrame(activities_tmp)
					activities_OLD_df = pd.DataFrame(activities_tmp)
				else:	 
					try:
						activities_df = activities_df.append(pd.DataFrame(activities_tmp),ignore_index = True)
					except:
						activities_df = activities_df.append(pd.DataFrame(activities_tmp,index=[0]),ignore_index = True)
			try:
				LOG.info("(Actual,totalSize,delta) = ({:>5},{:>5},{:>5}) ... {:.2f} sec".format(len(activities_tmp['AccountId']),activities_output['records'][0]['ActivityHistories']['totalSize'],\
																			len(activities_tmp['AccountId'])-activities_output['records'][0]['ActivityHistories']['totalSize'],time.time() - start) )
			except:
				LOG.info("(Actual,totalSize,delta) = ({:>5},   NA,   NA) ... {:.2f} sec".format(len(activities_tmp['AccountId']),time.time() - start))

			try:	
				if (len(activities_tmp['AccountId']) == activities_output['records'][0]['ActivityHistories']['totalSize']):
					all_records = True	
				else:
					cur_query = cur_query + 1		 
			except:
				all_records = True

	return(activities_df)
 
#######################################################################
# Created by DKJ ... 1/6/2016
# This reads from the sfdc account history
# 1) User input a dataframe with sfdc account id and sfdc account name
# 2) the entire activities history for all accounts is returned
#######################################################################
def sfdc_account_history_query_account(sf,account_df):

	start = time.time()

	########################################
	# activity_df ... Activities History
	########################################
	account_history_df = pd.DataFrame() 
	for i in range(0,len(account_df)):

		all_records = False
		account_tmp = {}	
		cur_query = 1
		while(not all_records):

			if (len(account_tmp) == 0):
				CUR_DATETIME = '2001-01-01T00:00:00.000+0000'
			else:
				CUR_DATETIME = activities_tmp['CreatedDate'][len(activities_tmp['CreatedDate'])-1]
	
			LOG.info("Account History: {:>4} of {:>4} . Q{:>2} . {:>60} . ".format(i+1,len(account_df),cur_query,account_df.ix[i]['Name']))
			query = "SELECT AccountId,Account.Name,CreatedDate,CreatedBy.Id,CreatedBy.Name,Field,OldValue,NewValue \
			               FROM AccountHistory WHERE AccountId = '%s'" % (account_df.ix[i]['AccountId_18']) 
			               ##FROM AccountHistory WHERE AccountId = '%s' AND CreatedDate > %s" % (account_df.ix[i]['Id'],CUR_DATETIME) 

			try:
				account_output = sf.query_all(query)
			except Exception as e:
				LOG.info("i = {:>4}: {}".format(i,e))
				account_output = sf.query_all(query)
				
			account_tmp = {}	
			try:
				account_tmp['AccountId'] = map((lambda item: item['AccountId']),account_output['records'])
				account_tmp['AccountName'] = map((lambda item: item['Account']['Name']),account_output['records'])
				account_tmp['CreatedDate'] = [datetime_SF_to_Mysql(x) for x in map((lambda item: item['CreatedDate']),account_output['records'])]
				account_tmp['CreatedById'] = map((lambda item: item['CreatedBy']['Id']),account_output['records'])
				account_tmp['CreatedByName'] = map((lambda item: item['CreatedBy']['Name']),account_output['records'])
				account_tmp['Field'] = map((lambda item: item['Field']),account_output['records'])
				account_tmp['OldValue'] = map((lambda item: item['OldValue']),account_output['records'])
				account_tmp['NewValue'] = map((lambda item: item['NewValue']),account_output['records'])

			except:
				LOG.info("No Data")

			if (len(account_history_df) == 0):
				account_history_df = pd.DataFrame(account_tmp)
				account_history_OLD_df = pd.DataFrame(account_tmp)
			else:	 
				account_history_df = account_history_df.append(pd.DataFrame(account_tmp),ignore_index = True)
	
			try:
				LOG.info("(Actual,totalSize,delta) = ({:>5},{:>5},{:>5}) ... {:.2f} sec".format(len(account_tmp['AccountId']),account_output['totalSize'],\
																			len(account_tmp['AccountId'])-account_output['totalSize'],time.time() - start) )
			except:
				LOG.info("(Actual,totalSize,delta) = ({:>5},   NA,   NA) ... {:.2f} sec\n",len(account_df.ix[i]['AccountId_18']),time.time() - start)

			try:	
				if (len(account_tmp['AccountId']) == account_output['totalSize']):
					all_records = True	
				else:
					cur_query = cur_query + 1		 
			except:
				all_records = True

	account_history_df['AccountId_18'] = [x for x in account_history_df.AccountId]
	account_history_df['AccountId'] = [x[0:15] for x in account_history_df.AccountId] ### Change AccountId to 15 characters
	account_history_df = account_history_df.sort(['AccountId_18','CreatedDate']).reset_index(drop=True)

	return(account_history_df)

def sfdc_opportunity_history_query_account(sf,account_df):

	start = time.time()

	########################################
	# activity_df ... Activities History
	########################################
	opportunity_history_df = pd.DataFrame() 
	for i in range(0,len(account_df)):

		all_records = False
		opportunity_tmp = {}	
		cur_query = 1
		while(not all_records):

			if (len(opportunity_tmp) == 0):
				CUR_DATETIME = '2001-01-01T00:00:00.000+0000'
			else:
				CUR_DATETIME = activities_tmp['CreatedDate'][len(activities_tmp['CreatedDate'])-1]
	
			LOG.info("Opportunity History: {:>4} of {:>4} . Q{:>2} . {:>60} . ".format(i+1,len(account_df),cur_query,account_df.ix[i]['Name'].encode('utf-8').strip()) )
			query = "SELECT OpportunityId,Opportunity.Account.Id,Opportunity.Account.Name,CreatedDate,CreatedBy.Id,CreatedBy.Name,Opportunity.Type,Field,OldValue,NewValue \
			               FROM OpportunityFieldHistory WHERE Opportunity.Account.Id = '%s'" % (account_df.ix[i]['AccountId_18']) 
			               ##FROM AccountHistory WHERE AccountId = '%s' AND CreatedDate > %s" % (opportunity_df.ix[i]['Id'],CUR_DATETIME) 

			try:
				opportunity_output = sf.query_all(query)
			except Exception as e:
				LOG.info("i = {:>4}: {}".format(i,e) )
				opportunity_output = sf.query_all(query)
				
			opportunity_tmp = {}	
			try:
				opportunity_tmp['OpportunityId'] = map((lambda item: item['OpportunityId']),opportunity_output['records'])
				opportunity_tmp['AccountId'] = map((lambda item: item['Opportunity']['Account']['Id']),opportunity_output['records'])
				opportunity_tmp['AccountName'] = map((lambda item: item['Opportunity']['Account']['Name']),opportunity_output['records'])
				opportunity_tmp['CreatedDate'] = [datetime_SF_to_Mysql(x) for x in map((lambda item: item['CreatedDate']),opportunity_output['records'])]
				opportunity_tmp['CreatedById'] = map((lambda item: item['CreatedBy']['Id']),opportunity_output['records'])
				opportunity_tmp['CreatedByName'] = map((lambda item: item['CreatedBy']['Name']),opportunity_output['records'])
				opportunity_tmp['OpportunityType'] = map((lambda item: item['Opportunity']['Type']),opportunity_output['records'])
				opportunity_tmp['Field'] = map((lambda item: item['Field']),opportunity_output['records'])
				opportunity_tmp['OldValue'] = map((lambda item: item['OldValue']),opportunity_output['records'])
				opportunity_tmp['NewValue'] = map((lambda item: item['NewValue']),opportunity_output['records'])

			except Exception as e:
				LOG.info("No Data ... i = {:>4}: {}".format(i,e) )

			if (len(opportunity_history_df) == 0):
				opportunity_history_df = pd.DataFrame(opportunity_tmp)
				opportunity_history_OLD_df = pd.DataFrame(opportunity_tmp)
			else:	 
				opportunity_history_df = opportunity_history_df.append(pd.DataFrame(opportunity_tmp),ignore_index = True)
	
			try:
				LOG.info("(Actual,totalSize,delta) = ({:>5},{:>5},{:>5}) ... {:.2f} sec".format(len(opportunity_tmp['AccountId']),opportunity_output['totalSize'],\
																			len(opportunity_tmp['AccountId'])-opportunity_output['totalSize'],time.time() - start) )
			except:
				LOG.info("(Actual,totalSize,delta) = ({:>5},   NA,   NA) ... {:.2f} sec".format(len(account_df.ix[i]['AccountId_18']),time.time() - start) )

			try:	
				if (len(opportunity_tmp['AccountId']) == opportunity_output['totalSize']):
					all_records = True	
				else:
					cur_query = cur_query + 1		 
			except:
				all_records = True

	opportunity_history_df['AccountId_18'] = [x for x in opportunity_history_df.AccountId]
	opportunity_history_df['AccountId'] = [x[0:15] for x in opportunity_history_df.AccountId] ### Change AccountId to 15 characters
	opportunity_history_df['OpportunityId_18'] = [x for x in opportunity_history_df.OpportunityId]
	opportunity_history_df['OpportunityId'] = [x[0:15] for x in opportunity_history_df.OpportunityId] ### Change OpportunityId to 15 characters
	opportunity_history_df = opportunity_history_df.sort(['AccountId_18','CreatedDate','OpportunityId']).reset_index(drop=True)

	return(opportunity_history_df)

def sfdc_contact_history_query_account(sf,account_df):

	start = time.time()

	########################################
	# activity_df ... Activities History
	########################################
	contact_history_df = pd.DataFrame() 
	for i in range(0,1): ##len(account_df)):

		all_records = False
		contact_tmp = {}	
		cur_query = 1
		while(not all_records):

			#if (len(contact_tmp) == 0):
			CUR_DATETIME = '2016-06-01T00:00:00.000+0000'
			#else:
			#	CUR_DATETIME = activities_tmp['CreatedDate'][len(activities_tmp['CreatedDate'])-1]
	
			LOG.info("Contact History: {:>4} of {:>4} . Q{:>2} . {:>60} . ".format(i+1,len(account_df),cur_query,account_df.ix[i]['Name']) )
			query = "SELECT ContactId,CreatedDate,Field,OldValue,NewValue FROM ContactHistory WHERE CreatedDate > %s" % CUR_DATETIME 
			               ##FROM AccountHistory WHERE AccountId = '%s' AND CreatedDate > %s" % (contact_df.ix[i]['Id'],CUR_DATETIME) 
			#query = "SELECT ContactId,Contact.Account.Id,Contact.Account.Name,CreatedDate,CreatedBy.Id,CreatedBy.Name,Contact.Type,Field,OldValue,NewValue \
			 #              FROM ContactFieldHistory WHERE Contact.Account.Id = '%s'" % (account_df.ix[i]['AccountId_18']) 
			  #             ##FROM AccountHistory WHERE AccountId = '%s' AND CreatedDate > %s" % (contact_df.ix[i]['Id'],CUR_DATETIME) 

			try:
				contact_output = sf.query_all(query)
			except Exception as e:
				LOG.info("i = {:>4}: {}".format(i,e))
				contact_output = sf.query_all(query)
				
			contact_tmp = {}	
			try:
				contact_tmp['ContactId'] = map((lambda item: item['ContactId']),contact_output['records'])
				#contact_tmp['AccountId'] = map((lambda item: item['Contact']['Account']['Id']),contact_output['records'])
				#contact_tmp['AccountName'] = map((lambda item: item['Contact']['Account']['Name']),contact_output['records'])
				contact_tmp['CreatedDate'] = [datetime_SF_to_Mysql(x) for x in map((lambda item: item['CreatedDate']),contact_output['records'])]
				#contact_tmp['CreatedById'] = map((lambda item: item['CreatedBy']['Id']),contact_output['records'])
				#contact_tmp['CreatedByName'] = map((lambda item: item['CreatedBy']['Name']),contact_output['records'])
				#contact_tmp['ContactType'] = map((lambda item: item['Contact']['Type']),contact_output['records'])
				contact_tmp['Field'] = map((lambda item: item['Field']),contact_output['records'])
				contact_tmp['OldValue'] = map((lambda item: item['OldValue']),contact_output['records'])
				contact_tmp['NewValue'] = map((lambda item: item['NewValue']),contact_output['records'])

			except Exception as e:
				LOG.info("No Data ... i = {:>4}: {}".format(i,e))

			if (len(contact_history_df) == 0):
				contact_history_df = pd.DataFrame(contact_tmp)
				contact_history_OLD_df = pd.DataFrame(contact_tmp)
			else:	 
				contact_history_df = contact_history_df.append(pd.DataFrame(contact_tmp),ignore_index = True)
	
			try:
				LOG.info("(Actual,totalSize,delta) = ({:>5},{:>5},{:>5}) ... {:.2f} sec".format(len(contact_tmp['ContactId']),contact_output['totalSize'],\
																			len(contact_tmp['ContactId'])-contact_output['totalSize'],time.time() - start) )
			except:
				LOG.info("(Actual,totalSize,delta) = ({:>5},   NA,   NA) ... {:.2f} sec".format(len(account_df.ix[i]['ContactId_18']),time.time() - start) )

			try:	
				if (len(contact_tmp['ContactId']) == contact_output['totalSize']):
					all_records = True	
				else:
					cur_query = cur_query + 1		 
			except:
				all_records = True

	contact_history_df['AccountId_18'] = [x for x in contact_history_df.AccountId]
	contact_history_df['AccountId'] = [x[0:15] for x in contact_history_df.AccountId] ### Change AccountId to 15 characters
	contact_history_df['ContactId_18'] = [x for x in contact_history_df.ContactId]
	contact_history_df['ContactId'] = [x[0:15] for x in contact_history_df.ContactId] ### Change OpportunityId to 15 characters
	contact_history_df = contact_history_df.sort(['AccountId_18','CreatedDate','ContactId']).reset_index(drop=True)

	return(contact_history_df)

def read_sfdc_accounts(sf):

	###############################################
	# account_df ... Query sfdc Accounts 
	###############################################
	LOG.info("Salesforce ... Query Account object") 
	query = "SELECT Name,Id,Customer_Success_Manager1__c,NaicsCode,ALL_NAICS_Codes__c,Product_Line__c,Account_Status__c, \
				CS_Account_Status__c,Channel_Customer__c,Channel_Partner__c,MSA_Effective_Date__c,Customer_Lifespan__c,Total_Nmbr_Videos__c, \
				G2_Completed_Videos__c,of_Library_Completion__c,Total_Algo_Videos__c,Yearly_Client_ARR__c, \
				AnnualRevenue,Benefits_Eligible_Employees__c,NumberOfEmployees, \
				Health_Category__c,Health_Category_Reason__c,Health_Sub_Category__c, \
				Cancellation_Notification__c,Cancellation_Type__c, \
				Account_Health__c,HCR_Only__c,ARR_At_Risk__c, \
				Past_Client_Customer_Lifespan__c,Initial_Term_Length__c,X1095_C__c, \
				Current_Score_Value__c,Current_Score_Label__c,Cancellation_Notice_Received__c,M_A__c,Sales_Rep_Notes__c \
				FROM Account \
				WHERE Account_Status__c = 'Current Client' OR Account_Status__c = 'Channel Partner & Client' \
				OR Account_Status__c = 'Financial Channel Partner & Client' OR Account_Status__c = 'Broker Channel Partner & Client' \
				OR Account_Status__c = 'Past Client' \
				OR Past_Client_Customer_Lifespan__c > 0" #### NECESSARY FOR PAST CLIENTS THAT HAVE BEEN CHANGED TO PROSPECTS 

	account_output = sf.query_all(query)
	account_df = pd.DataFrame(account_output['records']).drop('attributes',1)

	## Remove trailing spaces
	for i in range(0,len(account_df)):
		try:
			account_df.loc[i,'Id'] = str.rstrip(str(account_df.ix[i]['Id']))
		except:
			account_df.loc[i,'Id'] = None

	account_df['AccountId_18'] = [x for x in account_df.Id] ### Change AccountId to 15 characters
	account_df['Id'] = [x[0:15] for x in account_df.Id] ### Change AccountId to 15 characters

	### Extra Edits ###
	## 1) Remove Guidespark
	account_df = account_df[(account_df.Id != '0015000000f9lLq')].reset_index(drop=True) ### Remove Guidespark
	## 2) Update MSA for the 'State of Colorado' ... non-evergreen re-paper
	account_df.loc[int(account_df[(account_df.AccountId_18 == '0015000000su9CMAAY')].index),'MSA_Effective_Date__c'] = '2015-04-01'

	#account_df = convert_object_to_date(account_df,'MSA_Effective_Date__c','%Y-%m-%d','MSA_Effective_Date')
	account_df['MSA_Effective_Date'] = pd.to_datetime(account_df['MSA_Effective_Date__c'])

	for i in range(0,len(account_df)):
		try:
			account_df.loc[i,'Health_Category_Reason__c'] = account_df.ix[i]['Health_Category_Reason__c'].replace('\n','').replace('\r','')
		except Exception as e:
			LOG.info("i = {:>4}: {}".format(i,e))

	for i in range(0,len(account_df)):
		if (account_df.ix[i]['Cancellation_Type__c'] == 'Attempted Cancellation'):
			account_df.loc[i,'churn'] = 1	
			account_df.loc[i,'churn_int'] = 0	
		elif (account_df.ix[i]['Cancellation_Type__c'] == 'Contraction'):
			account_df.loc[i,'churn'] = 2	
			account_df.loc[i,'churn_int'] = 0	
		elif (account_df.ix[i]['Cancellation_Type__c'] == 'Full Cancellation'):
			account_df.loc[i,'churn'] = 3	
			account_df.loc[i,'churn_int'] = 1	
		elif (account_df.ix[i]['Cancellation_Type__c'] == 'Lost Account'):
			account_df.loc[i,'churn'] = 4	
			account_df.loc[i,'churn_int'] = 1	
		elif (account_df.ix[i]['Past_Client_Customer_Lifespan__c'] > 0):   #### NECESSARY FOR PAST CLIENT THAT HAVE BEEN CHANGED TO PROSPECT
			account_df.loc[i,'churn'] = 4	
			account_df.loc[i,'churn_int'] = 1	
		elif (account_df.ix[i]['Account_Status__c'] == 'Past Client'):   #### NECESSARY FOR PAST CLIENT THAT HAVE BEEN CHANGED TO PROSPECT
			account_df.loc[i,'churn'] = 5	
			account_df.loc[i,'churn_int'] = 1	
		elif (pd.isnull(account_df.ix[i]['Cancellation_Type__c']) ):
			account_df.loc[i,'churn'] = 0	
			account_df.loc[i,'churn_int'] = 0	

	#############################################
	# Update Initial Term
	#############################################

	for i in range(0,len(account_df)):
		try:
			account_df.loc[i,'Initial_Term_Length__c'] = int(account_df.ix[i]['Initial_Term_Length__c'].replace(' Months',''))
		except Exception as e:
			LOG.info("i = {:>4}: {}".format(i,e))

		if (account_df.ix[i]['Initial_Term_Length__c'] == 'one (1)'):
			account_df.loc[i,'Initial_Term_Length__c'] = 12
		elif (account_df.ix[i]['Initial_Term_Length__c'] == 'two (2)'):
			account_df.loc[i,'Initial_Term_Length__c'] = 24
		elif (account_df.ix[i]['Initial_Term_Length__c'] == 'three (3)'):
			account_df.loc[i,'Initial_Term_Length__c'] = 36
		elif (account_df.ix[i]['Initial_Term_Length__c'] == 'four (4)'):
			account_df.loc[i,'Initial_Term_Length__c'] = 48 
		elif (account_df.ix[i]['Initial_Term_Length__c'] == 'five (5)'):
			account_df.loc[i,'Initial_Term_Length__c'] = 60 

	return(account_df)

def read_IN_sfdc_accounts(sf,act_string):

	###############################################
	# account_df ... Query sfdc Accounts 
	###############################################
	LOG.info("Salesforce ... Query Account object") 
	query = "SELECT Name,Id,Customer_Success_Manager1__c,NaicsCode,ALL_NAICS_Codes__c,Product_Line__c,Account_Status__c, \
				CS_Account_Status__c,Channel_Customer__c,Channel_Partner__c,MSA_Effective_Date__c,Customer_Lifespan__c,Total_Nmbr_Videos__c, \
				G2_Completed_Videos__c,of_Library_Completion__c,Total_Algo_Videos__c,Yearly_Client_ARR__c, \
				AnnualRevenue,Benefits_Eligible_Employees__c,NumberOfEmployees, \
				Health_Category__c,Health_Category_Reason__c,Health_Sub_Category__c, \
				Cancellation_Notification__c,Cancellation_Type__c, \
				Account_Health__c,HCR_Only__c,ARR_At_Risk__c, \
				Past_Client_Customer_Lifespan__c,Initial_Term_Length__c,X1095_C__c, \
				Current_Score_Value__c,Current_Score_Label__c,Cancellation_Notice_Received__c,M_A__c,Sales_Rep_Notes__c \
				FROM Account WHERE Id IN ('%s')" % (act_string)

	account_output = sf.query_all(query)
	account_df = pd.DataFrame(account_output['records']).drop('attributes',1)

	## Remove trailing spaces
	for i in range(0,len(account_df)):
		try:
			account_df.loc[i,'Id'] = str.rstrip(str(account_df.ix[i]['Id']))
		except:
			account_df.loc[i,'Id'] = None

	account_df['AccountId_18'] = [x for x in account_df.Id] ### Change AccountId to 15 characters
	account_df['Id'] = [x[0:15] for x in account_df.Id] ### Change AccountId to 15 characters

	### Extra Edits ###
	#account_df = convert_object_to_date(account_df,'MSA_Effective_Date__c','%Y-%m-%d','MSA_Effective_Date')
	account_df['MSA_Effective_Date'] = pd.to_datetime(account_df['MSA_Effective_Date__c'])

	#############################################
	# Update Initial Term
	#############################################

	for i in range(0,len(account_df)):
		try:
			account_df.loc[i,'Initial_Term_Length__c'] = int(account_df.ix[i]['Initial_Term_Length__c'].replace(' Months',''))
		except Exception as e:
			print(str(i) + '.',end="")

		if (account_df.ix[i]['Initial_Term_Length__c'] == 'one (1)'):
			account_df.loc[i,'Initial_Term_Length__c'] = 12
		elif (account_df.ix[i]['Initial_Term_Length__c'] == 'two (2)'):
			account_df.loc[i,'Initial_Term_Length__c'] = 24
		elif (account_df.ix[i]['Initial_Term_Length__c'] == 'three (3)'):
			account_df.loc[i,'Initial_Term_Length__c'] = 36
		elif (account_df.ix[i]['Initial_Term_Length__c'] == 'four (4)'):
			account_df.loc[i,'Initial_Term_Length__c'] = 48 
		elif (account_df.ix[i]['Initial_Term_Length__c'] == 'five (5)'):
			account_df.loc[i,'Initial_Term_Length__c'] = 60 

	return(account_df)

def sfdc_write_library_completion (sf,in_df):
	###########################
	# Write results to SFDC
	###########################
	
	sfdc_write_variables = ['Total_Algo_Videos__c','G2_Completed_Videos__c']
	
	if (len(sfdc_write_variables) != 2):
		LOG.info("Need 2 variables ... you specified {} variables".format(len(sfdc_write_variables)) )
		sys.exit()
	if (sfdc_write_variables[0] != 'Total_Algo_Videos__c'):
		LOG.info("Incorrect write variables")
		sys.exit()
	if (sfdc_write_variables[1] != 'G2_Completed_Videos__c'):
		LOG.info("Incorrect write variables")
		sys.exit()
	
	for i in range(0,len(in_df)):
		LOG.info("{:>5} of {:>5} ... {:>18} ... Account: {:>75} ".format(i,len(in_df)-1,in_df.ix[i]['AccountId_18'],in_df.ix[i]['Name']) )
		try:
			sf.Account.update(in_df.ix[i]['AccountId_18'],{sfdc_write_variables[0]:in_df.ix[i]['Devon_algo_Nvideo'], \
														sfdc_write_variables[1]:in_df.ix[i]['g2_Nvideo_cur'] } ) 
			LOG.info("SUCCESS")
		except Exception as e:
			LOG.info("FAILED ... i = {:>4}: {}".format(i,e))
	
