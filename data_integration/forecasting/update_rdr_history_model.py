#! /usr/bin/env python

################################################
#
# Written by DKJ ... 9/9/16
#
# Program will export sfdc activities for all
# accounts that have ever had an opportunity
#
################################################
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
from datetime import datetime,timedelta

sys.path.insert(0,'/home/analytics/analytics_sandbox/python_libs');
import common_libs as cm
import create_mysql as mys 
import sfdc_libs as sfdc
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

CREATE_NEW_SUMMARY_DATA = True
GET_ACTIVITY = False 
COMBINE_RECORDS = False
FIND_MTG = True
FIND_CALL = True
FIND_EMAIL = True
DEBUG = False  # True --> This is for a single / group of unique Opportunity Ids

start = time.time()

def find_percentage(df,X,Y,name):

	value_per = []
	for iii in range(0,len(df)):
		try:
			value_per.append(float(df.ix[iii][X]) / float(df.ix[iii][Y]) ) 
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

#########################################
## op_df ... Query Opportunity Database 
#########################################
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

#################################################################
# Find all accounts that either won or lost an opportunity
#################################################################
op_wonlost_df = op_df[['AccountId_18','AccountName','Type','StageName','Close_Date__c']][(op_df['StageName'].str.contains('Lost')) | (op_df['StageName'].str.contains('Won'))]
account_wonlost_df = op_wonlost_df[['AccountId_18','AccountName']].drop_duplicates().reset_index(drop=True)
op_won_df = op_df[['AccountId_18','AccountName','Type','StageName','Close_Date__c']][(op_df['StageName'].str.contains('Won'))]
account_won_df = op_won_df[['AccountId_18','AccountName']].drop_duplicates().reset_index(drop=True)

Nop_bin = len(account_wonlost_df)/500
for jjj in range(0,Nop_bin+1):
	LOG.info("{:>5} of {:>5} ... {},{}".format(jjj*500,len(account_wonlost_df),(jjj*500),((jjj+1)*500-1) ) )
	account_test_df = sfdc.read_IN_sfdc_accounts(sf,"','".join(account_wonlost_df.ix[(jjj*500):((jjj+1)*500-1)]['AccountId_18']))

	if (jjj == 0):
		account_df = account_test_df.copy(deep=True)
	else:
		account_df = account_df.append(account_test_df,ignore_index=True)

# Get unique accounts
unique_account = list(set(account_df.AccountId_18))

####################################
# Create random survival forest data
####################################
# Define columns
mtg_cols = ['is_mtg','Nmtg_total','Nmtg_cancel_total','Nmtg_noshow_total','Nmtg_completed_total']
email_cols = ['is_email','Nemail_total','Ncontact_customer','Ncontact_guidespark','Nfillform_total','Nfillform_good_total','Nfillform_bad_total']
call_cols = ['is_call','Ncall_total']
	
if (CREATE_NEW_SUMMARY_DATA == True):

	######################################
	# Get all activities for each account
	######################################
	if (GET_ACTIVITY == True):

		for iii in range(0,(len(account_df)/100+1)):
			account_tmp_df = account_df.ix[100*iii:(100*iii+99)].copy(deep=True).reset_index(drop=True)
			if (len(account_tmp_df) > 0):
				activity_df = sfdc.sfdc_activity_query_account(sf,account_tmp_df[['Id','Name']])
				idx = activity_df.index[(pd.isnull(activity_df['Description']) == False)]
	
				description = [x.replace('\n','&&') for x in activity_df['Description'][(pd.isnull(activity_df['Description']) == False)]  ]  # Remove \n from descriptions
				description_df = pd.merge(pd.DataFrame(idx),pd.DataFrame(description),how='left',left_index=True,right_index=True).rename(columns={'0_x':'index','0_y':'Description_NEW'}).set_index('index')
				activity_df = pd.merge(activity_df,description_df,how='left',left_index=True,right_index=True).drop('Description',1).rename(columns={'Description_NEW':'Description'})
	
				description = [x.replace('\r','') for x in activity_df['Description'][(pd.isnull(activity_df['Description']) == False)]  ]  # Remove \n from descriptions
				description_df = pd.merge(pd.DataFrame(idx),pd.DataFrame(description),how='left',left_index=True,right_index=True).rename(columns={'0_x':'index','0_y':'Description_NEW'}).set_index('index')
				activity_df = pd.merge(activity_df,description_df,how='left',left_index=True,right_index=True).drop('Description',1).rename(columns={'Description_NEW':'Description'})
	
				activity_df = pd.merge(activity_df,account_df[['AccountId_18','Name','MSA_Effective_Date__c']],how='left',left_on='AccountId',right_on='AccountId_18').drop('AccountId_18',1)
	
				#activity_df.to_csv('../SFDC_ACT/sfdc_activity.csv',index=False,encoding='utf-8') 
				activity_df.to_csv('../SFDC_ACT/sfdc_activity_' + str(iii).zfill(3) + '.csv',index=False,encoding='utf-8')
	
	if (COMBINE_RECORDS == True):
		Nrecords = 0
		for iii in range(0,(len(account_df)/100+1)):
	
			LOG.info('MERGE ACTIVITY FILES ... {:>3} of {:>3} ... '.format(iii+1,len(account_df)/100+1))
			activity_tmp_df = pd.read_csv('../SFDC_ACT/sfdc_activity_' + str(iii).zfill(3) + '.csv')

			Nrecords = Nrecords + len(activity_tmp_df)
			LOG.info('Records (Current File,Total) = ({:>6},{:>6})'.format(len(activity_tmp_df),Nrecords))
			if (iii == 0):
				activity_df = activity_tmp_df.copy(deep=True)
			else:
				activity_df = activity_df.append(pd.read_csv('../SFDC_ACT/sfdc_activity_' + str(iii).zfill(3) + '.csv'),ignore_index=True)
	
		### Output whether the initial op was won/lost
		account_woninitial_df = op_won_df[['AccountId_18','Type','StageName']][op_won_df['Type'].str.contains('Initial') == True].drop_duplicates().reset_index(drop=True)
		account_woninitial_df = account_woninitial_df[['AccountId_18','StageName']].drop_duplicates().reset_index(drop=True)
		account_woninitial_df['won'] = 1
		activity_df = pd.merge(activity_df,account_woninitial_df[['AccountId_18','won']],'left',left_on='AccountId',right_on='AccountId_18')   
		activity_df['won'] = activity_df['won'].fillna(0)
		activity_df = activity_df.drop('AccountId_18',1).rename(columns={'AccountId':'AccountId_18'}) 
	
		# Find list of emails on each 'Description'
		activity_df['total_email_list']  = [tuple(re.findall(r'[\w\.-]+@[\w\.-]+', x.upper()) ) if pd.notnull(x) == True else tuple() for x in activity_df['Description']]
		activity_df['unique_email_list'] = [tuple(set(x)) for x in activity_df['total_email_list']]
	
		# Output activity_df 
		activity_df.to_csv('../SFDC_ACT/sfdc_activity.csv',encoding='utf-8')
	
	else:
		LOG.info("Salesforce ... Load Activity Info ... {:.2f} sec".format(time.time()-start))
		activity_df = pd.read_csv('../SFDC_ACT/sfdc_activity.csv',index_col=[0])

	######### Add act_index for later access
	activity_df = activity_df.reset_index().rename(columns={'index':'act_index'}) 

	# Add event column for random survival forest
	activity_df['event'] = None 
	activity_df['Subject'] = [str(x).upper() for x in activity_df['Subject']]

	#########################################
	# Calculate lifetime day 
	#########################################
	activity_df['CreatedDate'] = pd.to_datetime(activity_df['CreatedDate'])
	account_first_df = activity_df[['AccountId_18','CreatedDate']].groupby('AccountId_18').first().reset_index().rename(columns={'CreatedDate':'Act_CreatedDate'})
	activity_df = pd.merge(activity_df,account_first_df[['AccountId_18','Act_CreatedDate']],'left',left_on='AccountId_18',right_on='AccountId_18')
	activity_df['lifetime_day'] = (activity_df['CreatedDate'] - activity_df['Act_CreatedDate']).astype('timedelta64[D]')

	###############################
	###############################
	# 1) Update Email info
	###############################
	###############################
	#unique_account = ['0015000000y8OFCAA2','0015000000stpTXAAY']
	if (FIND_MTG == True):
	
		LOG.info("FIND MEETING ... {:.2f} sec".format(time.time()-start))
	
		##########################################################
		### Meetings: Mark all Subjects that contain mtgs 
		##########################################################
		if ('is_mtg' not in activity_df.columns):
			activity_df['is_mtg'] = 0	
		if ('mtg_cancel' not in activity_df.columns):
			activity_df['mtg_cancel'] = 0	
		if ('mtg_noshow' not in activity_df.columns):
			activity_df['mtg_noshow'] = 0	
		if ('mtg_completed' not in activity_df.columns):
			activity_df['mtg_completed'] = 0	
		activity_df.loc[activity_df['MeetingOutcome'] == 'Cancelled','event'] = 'Meeting'	
		activity_df.loc[activity_df['MeetingOutcome'] == 'Completed','event'] = 'Meeting'	
		activity_df.loc[activity_df['MeetingOutcome'] == 'No Show','event'] = 'Meeting'	
		activity_df.loc[activity_df['MeetingOutcome'] == 'Cancelled','is_mtg'] = 1	
		activity_df.loc[activity_df['MeetingOutcome'] == 'Completed','is_mtg'] = 1	
		activity_df.loc[activity_df['MeetingOutcome'] == 'No Show','is_mtg'] = 1	
		activity_df.loc[activity_df['MeetingOutcome'] == 'Cancelled','mtg_cancel'] = 1	
		activity_df.loc[activity_df['MeetingOutcome'] == 'Completed','mtg_completed'] = 1	
		activity_df.loc[activity_df['MeetingOutcome'] == 'No Show','mtg_noshow'] = 1	
	
		#####################################
		## Meeting Stats: Get cumulative sum 
		#####################################
		test_df = activity_df[['AccountId_18','act_index','is_mtg']].set_index(["AccountId_18","act_index"])	
		mtg_cumsum_df = test_df.groupby(level=[0,1]).sum().groupby(level=[0]).cumsum().reset_index().rename(columns={'is_mtg':'Nmtg_total'}) 
		activity_df = pd.merge(activity_df,mtg_cumsum_df[['act_index','Nmtg_total']],'left',left_on='act_index',right_on='act_index')
	
		test_df = activity_df[['AccountId_18','act_index','mtg_cancel']].set_index(["AccountId_18","act_index"])	
		mtg_cumsum_df = test_df.groupby(level=[0,1]).sum().groupby(level=[0]).cumsum().reset_index().rename(columns={'mtg_cancel':'Nmtg_cancel_total'}) 
		activity_df = pd.merge(activity_df,mtg_cumsum_df[['act_index','Nmtg_cancel_total']],'left',left_on='act_index',right_on='act_index')
	
		test_df = activity_df[['AccountId_18','act_index','mtg_noshow']].set_index(["AccountId_18","act_index"])	
		mtg_cumsum_df = test_df.groupby(level=[0,1]).sum().groupby(level=[0]).cumsum().reset_index().rename(columns={'mtg_noshow':'Nmtg_noshow_total'}) 
		activity_df = pd.merge(activity_df,mtg_cumsum_df[['act_index','Nmtg_noshow_total']],'left',left_on='act_index',right_on='act_index')
	
		test_df = activity_df[['AccountId_18','act_index','mtg_completed']].set_index(["AccountId_18","act_index"])	
		mtg_cumsum_df = test_df.groupby(level=[0,1]).sum().groupby(level=[0]).cumsum().reset_index().rename(columns={'mtg_completed':'Nmtg_completed_total'}) 
		activity_df = pd.merge(activity_df,mtg_cumsum_df[['act_index','Nmtg_completed_total']],'left',left_on='act_index',right_on='act_index')
	
		#LOG.info("\n{}\n".format(timeline_df[['AccountId_18','ActivityType','CreatedDate','MeetingOutcome','Subject','Description','Nmtg_total'] + mtg_cols] \
		#							[(timeline_df.AccountId_18 == unique_account[0])]) )
	
	LOG.info("MEETING COMPLETE ... {:.2f} sec".format(time.time()-start))
	
	if (FIND_CALL == True):
	
		LOG.info("FIND CALL ... {:.2f} sec".format(time.time()-start))
	
		##########################################################
		### Calls Made: Mark all Subjects that contain calls 
		##########################################################
		if ('is_call' not in activity_df.columns):
			activity_df['is_call'] = 0	
		activity_df.loc[activity_df['Subject'].str.contains('VM') == True,'event'] = 'Call'	
		activity_df.loc[activity_df['Subject'].str.contains('NVM') == True,'event'] = 'Call'	
		activity_df.loc[activity_df['Subject'].str.contains('LVM') == True,'event'] = 'Call'	
		activity_df.loc[activity_df['Subject'].str.contains('NO VM') == True,'event'] = 'Call'	
		activity_df.loc[activity_df['Subject'].str.contains('MESSAGE LEFT:') == True,'event'] = 'Call'	
		activity_df.loc[activity_df['Subject'].str.contains('CALL -') == True,'event'] = 'Call'	
		activity_df.loc[activity_df['Subject'].str.contains('CALL') == True,'event'] = 'Call'	
		activity_df.loc[activity_df['Subject'].str.contains('NO ANSWER') == True,'event'] = 'Call'	
		activity_df.loc[activity_df['Subject'].str.contains('WRONG NUMBER') == True,'event'] = 'Call'	
		activity_df.loc[(activity_df['ActivityType'] == 'Call'),'event'] = 'Call'	
		activity_df.loc[pd.notnull(activity_df['CallDisposition']) == True,'event'] = 'Call'	
		activity_df.loc[activity_df['Subject'].str.contains('VM') == True,'is_call'] = 1	
		activity_df.loc[activity_df['Subject'].str.contains('NVM') == True,'is_call'] = 1	
		activity_df.loc[activity_df['Subject'].str.contains('LVM') == True,'is_call'] = 1	
		activity_df.loc[activity_df['Subject'].str.contains('NO VM') == True,'is_call'] = 1	
		activity_df.loc[activity_df['Subject'].str.contains('MESSAGE LEFT:') == True,'is_call'] = 1	
		activity_df.loc[activity_df['Subject'].str.contains('CALL -') == True,'is_call'] = 1	
		activity_df.loc[activity_df['Subject'].str.contains('CALL') == True,'is_call'] = 1	
		activity_df.loc[activity_df['Subject'].str.contains('NO ANSWER') == True,'is_call'] = 1 
		activity_df.loc[activity_df['Subject'].str.contains('WRONG NUMBER') == True,'is_call'] = 1
		activity_df.loc[(activity_df['ActivityType'] == 'Call'),'is_call'] = 1	
		activity_df.loc[pd.notnull(activity_df['CallDisposition']) == True,'is_call'] = 1
	
		#####################################
		## Calls Made: Get cumulative sum 
		#####################################
		test_df = activity_df[['AccountId_18','act_index','is_call']].set_index(["AccountId_18","act_index"])	
		# cumsum for all accounts
		call_cumsum_df = test_df.groupby(level=[0,1]).sum().groupby(level=[0]).cumsum().reset_index().rename(columns={'is_call':'Ncall_total'}) 
		# merge cumsum to activity_df 
		activity_df = pd.merge(activity_df,call_cumsum_df[['act_index','Ncall_total']],'left',left_on='act_index',right_on='act_index')
	
		#LOG.info("\n{}\n".format(timeline_df[['AccountId_18','ActivityType','CreatedDate','MeetingOutcome','Subject','Description','Ncall_total'] + call_cols] \
		#							[(timeline_df.AccountId_18 == unique_account[0])]) )
	
	LOG.info("CALL COMPLETE ... {:.2f} sec".format(time.time()-start))

	if (FIND_EMAIL == True):
	
		LOG.info("FIND EMAIL ... {:.2f} sec".format(time.time()-start))
	
		##########################################################
		### Emails Sent: Mark all Subjects that contain emails 
		##########################################################
		if ('is_email' not in activity_df.columns):
			activity_df['is_email'] = 0	
		if ('is_fillform' not in activity_df.columns):
			activity_df['is_fillform'] = 0	
			activity_df['is_fillform_good'] = 0	
			activity_df['is_fillform_bad'] = 0	
		activity_df.loc[activity_df['Subject'].str.contains('EMAIL') == True,'event'] = 'Email'	
		activity_df.loc[activity_df['Subject'].str.contains('E-MAIL') == True,'event'] = 'Email'	
		activity_df.loc[activity_df['Subject'].str.contains('EMAIL') == True,'is_email'] = 1	
		activity_df.loc[activity_df['Subject'].str.contains('E-MAIL') == True,'is_email'] = 1	

		activity_df.loc[activity_df['Subject'].str.contains('FILLED OUT FORM') == True,'event'] = 'fillform'
		activity_df.loc[activity_df['Subject'].str.contains('FILLED OUT FORM') == True,'is_fillform'] = 1
		activity_df.loc[activity_df['Subject'].str.contains('FILLED OUT FORM: UNSUBSCRIBEPAGE') == True,'is_fillform_bad'] = 1
		activity_df['is_fillform_good'] = activity_df['is_fillform'] - activity_df['is_fillform_bad'] 
	
		#####################################
		## Emails Sent: Get cumulative sum 
		#####################################
		test_df = activity_df[['AccountId_18','act_index','is_email']].set_index(["AccountId_18","act_index"])	
		# cumsum for all accounts
		email_cumsum_df = test_df.groupby(level=[0,1]).sum().groupby(level=[0]).cumsum().reset_index().rename(columns={'is_email':'Nemail_total'}) 
		# merge cumsum to activity_df 
		activity_df = pd.merge(activity_df,email_cumsum_df[['act_index','Nemail_total']],'left',left_on='act_index',right_on='act_index')
	
		#####################################
		## Fill Out Forms: Get cumulative sum 
		#####################################
		test_df = activity_df[['AccountId_18','act_index','is_fillform']].set_index(["AccountId_18","act_index"])	
		fillform_cumsum_df = test_df.groupby(level=[0,1]).sum().groupby(level=[0]).cumsum().reset_index().rename(columns={'is_fillform':'Nfillform_total'}) 
		activity_df = pd.merge(activity_df,fillform_cumsum_df[['act_index','Nfillform_total']],'left',left_on='act_index',right_on='act_index')
	
		test_df = activity_df[['AccountId_18','act_index','is_fillform_good']].set_index(["AccountId_18","act_index"])	
		fillform_cumsum_df = test_df.groupby(level=[0,1]).sum().groupby(level=[0]).cumsum().reset_index().rename(columns={'is_fillform_good':'Nfillform_good_total'}) 
		activity_df = pd.merge(activity_df,fillform_cumsum_df[['act_index','Nfillform_good_total']],'left',left_on='act_index',right_on='act_index')

		test_df = activity_df[['AccountId_18','act_index','is_fillform_bad']].set_index(["AccountId_18","act_index"])	
		fillform_cumsum_df = test_df.groupby(level=[0,1]).sum().groupby(level=[0]).cumsum().reset_index().rename(columns={'is_fillform_bad':'Nfillform_bad_total'}) 
		activity_df = pd.merge(activity_df,fillform_cumsum_df[['act_index','Nfillform_bad_total']],'left',left_on='act_index',right_on='act_index')

		###################################################
		## Email Addresses: Get cumulative sum
		###################################################
		for jjj in range(0,len(unique_account)/200+1):
			LOG.info("DETERMINE CONTACTS . {:>5} of {:>5} ... {:.2f} sec".format(jjj*200,len(unique_account),time.time()-start))
			test_unique_email_df = activity_df[['AccountId_18','act_index','total_email_list']][activity_df['AccountId_18'].isin(unique_account[(200*jjj):(200*(jjj+1))])].set_index(["AccountId_18","act_index"])	
	
			# cumsum for all accounts
			unique_cumsum_df = test_unique_email_df.groupby(level=[0,1]).sum().groupby(level=[0]).cumsum().reset_index().rename(columns={'total_email_list':'cum_total_email_list'})  
	
			# correct to put into list format 
			unique_cumsum_df['cum_total_email_list'] = [tuple(x.replace('(',',').replace(')',',').replace(' ','').replace('COM.','COM').split(',')) for x in unique_cumsum_df['cum_total_email_list']]  

			# remove null/dummy values from each list 
			unique_cumsum_df['cum_total_email_list'] = [filter(None,x) for x in unique_cumsum_df['cum_total_email_list']]  

			unique_cumsum_df['cum_total_email_list'] = [[y for y in x if y != 'NO_REPLY@GUIDESPARK.COM' ] for x in unique_cumsum_df['cum_total_email_list']] 
			unique_cumsum_df['cum_total_email_list'] = [[y for y in x if y != 'NOREPLY@GUIDESPARK.COM' ] for x in unique_cumsum_df['cum_total_email_list']] 
			unique_cumsum_df['cum_unique_email_list'] = [list(set(x)) if x is not None else x for x in unique_cumsum_df['cum_total_email_list']]  
	
			# Separate Guidespark / Customer Emails
			unique_cumsum_df['Ncontact_guidespark'] = [len(cm.all_substring('GUIDESPARK',x)) for x in unique_cumsum_df['cum_unique_email_list']]  
			unique_cumsum_df['Ncontact_customer'] = [(len(x) - len(cm.all_substring('GUIDESPARK',x))) for x in unique_cumsum_df['cum_unique_email_list']]  
	
			# Remove email lists 
			unique_cumsum_df = unique_cumsum_df.drop('cum_total_email_list',1).drop('cum_unique_email_list',1)
	
			if (jjj == 0):
				unique_cumsum_ALL_df = unique_cumsum_df.copy(deep=True)
			else:
				unique_cumsum_ALL_df = unique_cumsum_ALL_df.append(unique_cumsum_df,ignore_index=True).reset_index(drop=True)
			
	
		#	LOG.info("\n{0}".format(activity_df[['AccountId_18','Subject','is_email','Nemail_total']][activity_df['AccountId_18'] == unique_account[0]]) )

		activity_df = pd.merge(activity_df,unique_cumsum_ALL_df[['act_index','Ncontact_customer','Ncontact_guidespark']],'left',left_on='act_index',right_on='act_index')

		## Fill NAs with 0
		activity_df['Ncontact_guidespark'] = activity_df['Ncontact_guidespark'].fillna(0)
		activity_df['Ncontact_customer'] = activity_df['Ncontact_customer'].fillna(0)
		## Convert float to int 
		activity_df['Ncontact_guidespark'] = activity_df['Ncontact_guidespark'].astype(int)
		activity_df['Ncontact_customer'] = activity_df['Ncontact_customer'].astype(int)

	LOG.info("Output SFDC Activity Info ... {:.2f} sec".format(time.time()-start))
	activity_df.to_csv('../SFDC_ACT/sfdc_activity_TIMELIME.csv',encoding='utf-8')
	
else:	

	LOG.info("Load SFDC Activity Info ... {:.2f} sec".format(time.time()-start))
	activity_df = pd.read_csv('../SFDC_ACT/sfdc_activity_TIMELIME.csv',index_col=[0])
	activity_df['CreatedDate'] = pd.to_datetime(activity_df['CreatedDate'])
	activity_df['Act_CreatedDate'] = pd.to_datetime(activity_df['Act_CreatedDate'])
	
LOG.info("DATA INPUT COMPLETE ... {:.2f} sec".format(time.time()-start))

######################################################
######################################################
######################################################
################### Output Data ######################  
######################################################
######################################################
######################################################

#################
# Add final day
#################
account_first_df = activity_df[['AccountId_18','lifetime_day']].groupby('AccountId_18').max().reset_index().rename(columns={'lifetime_day':'final_day'})
activity_df = pd.merge(activity_df,account_first_df[['AccountId_18','final_day']],'left',left_on='AccountId_18',right_on='AccountId_18')

TDATA_VARIABLES = ['AccountId_18','CreatedDate','Act_CreatedDate','lifetime_day','final_day','event']
ACT_VARIABLES = ['NaicsCode','NumberOfEmployees']

MODEL_INDEPENDENT_VARIABLES = []
if (FIND_EMAIL  == True):
	for i in range(0,len(email_cols)):
		MODEL_INDEPENDENT_VARIABLES.append(email_cols[i])

if (FIND_CALL == True):
	for i in range(0,len(call_cols)):
		MODEL_INDEPENDENT_VARIABLES.append(call_cols[i])

if (FIND_MTG == True):
	for i in range(0,len(mtg_cols)):
		MODEL_INDEPENDENT_VARIABLES.append(mtg_cols[i])

timeline_df = activity_df[TDATA_VARIABLES + MODEL_INDEPENDENT_VARIABLES].copy(deep=True)
timeline_df = pd.merge(timeline_df,account_df[['AccountId_18'] + ACT_VARIABLES],'left',left_on='AccountId_18',right_on='AccountId_18')

###################
# Add all variables 
###################
tdata_timeline_df = timeline_df[TDATA_VARIABLES + ACT_VARIABLES + MODEL_INDEPENDENT_VARIABLES]

#### Do we need to set won library_size to PRIOR library_size
SDATA_VARIABLES = TDATA_VARIABLES + ACT_VARIABLES

###################################################
# Output for cox regression
###################################################
sdata_timeline_df = []
for i in range(0,len(unique_account)):

	if ((i % 500) == 499):
		LOG.info("Opportunity ... {:>5} of {:>5} ... sdata_timeline_df creation ... {:.2f} sec".format(i+1,len(unique_account),time.time()-start))

	#####################################
	# 1) Extract data from each account
	#####################################
	tmp_tdata_timeline_df = tdata_timeline_df.ix[cm.all_indices_CASE_SENSITIVE(unique_account[i],tdata_timeline_df['AccountId_18'])]

	################################################################################################################
	# 2) Group records with the SAME lifetime_day
	#	 Find the MAXIMUM INDEX (Nmax_idx) for the MAXIMUM RECORD for groups that have the same lifetime_day
	################################################################################################################
	tmp_sdata_timeline_MAXIDX_df = tmp_tdata_timeline_df[['AccountId_18','lifetime_day']].reset_index().groupby(['AccountId_18','lifetime_day'],as_index=False).agg({'index':max}).rename(columns={'index':'Nmax_idx'})
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
		#   printf_NEW(sys._getframe(),"[won_timeseries.py] %s\n",tmp_sdata_timeline_df)
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

sdata_timeline_COX_df = sdata_timeline_COX_df.rename(columns={'lifetime_day':'tstart'})
sdata_timeline_COX_df.to_csv('./output/sdata_rdr_history_COX.csv')
sdata_timeline_df = sdata_timeline_df.rename(columns={'lifetime_day':'tstart'})
sdata_timeline_df.to_csv('./output/sdata_rdr_history_RSF.csv')
sdata_timeline_df.to_csv('./output/sdata_rdr_history_RSF_' + cur_datetime.strftime('%Y%m%d') + '.csv',encoding='utf-8')
sdata_timeline_df.to_csv('./output/sdata_rdr_history_RSF_' + cur_datetime.strftime('%Y%m%d') + '_ORIG.csv',encoding='utf-8')

