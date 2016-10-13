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

####################################################################
#
# Created by DKJ ... creates static averages for a given timeframe
# 1) in_df ... dataframe to average
# 2) cur_days ... average duration
# 3) str_suffix ... suffix for how to name each column
# 4) filter_list ... what columns are used to filter the data vs cur_days
# 5) quantity_sum ... TRUE ... use np.sum ... FALSE ... use "count"
# 6) in_act_df .... contains ALL accounts .... used with RIGHT JOIN to make sure all accounts are used EVEN If 0
####################################################################
def calc_day_details(in_df,cur_days,str_suffix,filter_list,quantity_sum,in_act_df):

	for i in range(0,len(filter_list)):
		in_df = in_df.copy(deep=True)[ ((in_df.MSA_Effective_Date + timedelta(days=cur_days) ) > in_df[filter_list[i]])].reset_index(drop=True)
	
	if (quantity_sum):	
		out_df = in_df[['AccountId','OpportunityId','LineItemId','Quantity']].groupby(['AccountId']) \
							.agg({"OpportunityId":pd.Series.nunique,"LineItemId": pd.Series.nunique,"Quantity":np.sum  }) \
							.rename(columns = {'OpportunityId':'Nopportunity_' + str_suffix,'LineItemId':'NlineItem_' + str_suffix,'Quantity':'Nvideo_' + str_suffix}).reset_index()
	else:	
		out_df = in_df[['AccountId','OpportunityId','LineItemId','Quantity']].groupby(['AccountId']) \
							.agg({"OpportunityId":pd.Series.nunique,"LineItemId": pd.Series.nunique,"Quantity":"count" }) \
							.rename(columns = {'OpportunityId':'Nopportunity_' + str_suffix,'LineItemId':'NlineItem_' + str_suffix,'Quantity':'Nvideo_' + str_suffix}).reset_index()

	out_df = pd.merge(out_df,in_act_df[['AccountId']],'right',left_on='AccountId',right_on='AccountId').fillna(0)
	return out_df

def calc_day_details_G2(in_df,cur_days,str_suffix,filter_list,quantity_sum,in_act_df):

	for i in range(0,len(filter_list)):
		in_df = in_df.copy(deep=True)[ ((in_df.MSA_Effective_Date + timedelta(days=cur_days) ) > in_df[filter_list[i]])].reset_index(drop=True)

	out_df = in_df[['sfdc','account_id','trackable_id','user_id','parent_id']].groupby(['sfdc']) \
							.agg({"account_id":pd.Series.nunique,"trackable_id":pd.Series.nunique,"user_id":pd.Series.nunique,"parent_id": "count"  }) \
							.rename(columns = {'account_id':'g2_Nmicro_' + str_suffix,'trackable_id':'g2_Nvideo_' + str_suffix, \
												'user_id':'g2_Nuser_' + str_suffix,'parent_id':'g2_Nview_' + str_suffix}).reset_index()

	out_df = pd.merge(out_df,in_act_df[['sfdc']],'right',left_on='sfdc',right_on='sfdc').fillna(0)

	return out_df

def calc_day_details_G2edits(in_df,cur_days,str_suffix,filter_list,vd1,in_act_df):

	for i in range(0,len(filter_list)):
		in_df = in_df.copy(deep=True)[ ((in_df.MSA_Effective_Date + timedelta(days=cur_days) ) > in_df[filter_list[i]])].reset_index(drop=True)

	out_df = in_df[['sfdc','account_id','video_id','video_version_id','Ncnt_preview','Ncnt_qc']].groupby(['sfdc']) \
						.agg({"account_id":pd.Series.nunique,"video_id":pd.Series.nunique,"video_version_id":pd.Series.nunique, \
						"Ncnt_preview":"mean","Ncnt_qc": "mean"  }) \
						.rename(columns = {'account_id':'g2_act_' + str_suffix,'video_id':'g2_Nvideo_' + str_suffix,'video_version_id':'g2_Nversion_' + str_suffix, \
											'Ncnt_preview':'g2_avg_edits_preview_' + str_suffix,'Ncnt_qc':'g2_avg_edits_qc_' + str_suffix}).reset_index()

	out_df = pd.merge(out_df,in_act_df[['sfdc']],'right',left_on='sfdc',right_on='sfdc').fillna(0)

	return out_df

def calc_day_details_G2admin(in_df,cur_days,str_suffix,filter_list,vd1,in_act_df):

	for i in range(0,len(filter_list)):
		in_df = in_df.copy(deep=True)[ ((in_df.MSA_Effective_Date + timedelta(days=cur_days) ) > in_df[filter_list[i]])].reset_index(drop=True)

	out_df = in_df[['sfdc','account_id','user_id','action','created_at']].groupby(['sfdc']) \
						.agg({"account_id":pd.Series.nunique,"user_id":pd.Series.nunique,"action":pd.Series.nunique, \
						"created_at":"count"  }) \
						.rename(columns = {'account_id':'g2_admin_micro_' + str_suffix,'user_id':'g2_admin_user_' + str_suffix,'action':'g2_admin_action_' + str_suffix, \
											'created_at':'g2_admin_report_' + str_suffix}).reset_index()

	out_df = pd.merge(out_df,in_act_df[['sfdc']],'right',left_on='sfdc',right_on='sfdc').fillna(0)

	return out_df

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

account_df = read_sfdc_accounts(sf)
account_df = account_df[(pd.notnull(account_df.Initial_Term_Length__c)==True)].reset_index(drop=True)

printf("[update_churn_model_data.py]: Salesforce ... No of Accounts: %s\n",len(account_df))
printf("[update_churn_model_data.py]: Salesforce ... Account Query Time: %.2f sec\n",time.time() - start)

######################################
# Get all activities for each account
######################################
if (GET_ACTIVITY == True):

	account_history_df = sfdc_account_history_query_account(sf,account_df[['AccountId_18','Name']])
	account_history_df.to_csv('./output/sfdc_account_history_' + cur_datetime.strftime('%Y%m%d') + '.csv',encoding='utf-8')

	for i in range(0,(len(account_df)/100+1)):
		account_tmp_df = account_df.ix[100*i:(100*i+99)].copy(deep=True).reset_index(drop=True)
		if (len(account_tmp_df) > 0):
			activity_df = sfdc_activity_query_account(sf,account_tmp_df[['Id','Name']])
			idx = activity_df.index[(pd.isnull(activity_df['Description']) == False)]

			description = [x.replace('\n','&&&&&') for x in activity_df['Description'][(pd.isnull(activity_df['Description']) == False)]  ]  # Remove \n from descriptions
			description_df = pd.merge(pd.DataFrame(idx),pd.DataFrame(description),how='left',left_index=True,right_index=True).rename(columns={'0_x':'index','0_y':'Description_NEW'}).set_index('index')
			activity_df = pd.merge(activity_df,description_df,how='left',left_index=True,right_index=True).drop('Description',1).rename(columns={'Description_NEW':'Description'})

			description = [x.replace('\r','') for x in activity_df['Description'][(pd.isnull(activity_df['Description']) == False)]  ]  # Remove \n from descriptions
			description_df = pd.merge(pd.DataFrame(idx),pd.DataFrame(description),how='left',left_index=True,right_index=True).rename(columns={'0_x':'index','0_y':'Description_NEW'}).set_index('index')
			activity_df = pd.merge(activity_df,description_df,how='left',left_index=True,right_index=True).drop('Description',1).rename(columns={'Description_NEW':'Description'})

			activity_df = pd.merge(activity_df,account_df[['AccountId_18','Name','MSA_Effective_Date__c','churn','churn_int']],how='left',left_on='AccountId',right_on='AccountId_18').drop('AccountId_18',1)
	 
			#activity_df.to_csv('../SFDC_ACT/sfdc_activity.csv',index=False,encoding='utf-8') 
			activity_df.to_csv('../SFDC_ACT/sfdc_activity_' + str(i).zfill(3) + '.csv',index=False,encoding='utf-8') 

	Nrecords = 0
	for i in range(0,(len(account_df)/100+1)):

		printf('MERGE ACTIVITY FILES ... %3s of %3s ... ',i+1,len(account_df)/100+1)
		activity_tmp_df = pd.read_csv('../SFDC_ACT/sfdc_activity_' + str(i).zfill(3) + '.csv')

		Nrecords = Nrecords + len(activity_tmp_df)
		printf('Records (Current File,Total) = (%6s,%6s)\n',len(activity_tmp_df),Nrecords)
		if (i == 0):
			activity_df = activity_tmp_df.copy(deep=True)
		else:
			activity_df = activity_df.append(pd.read_csv('./output_ACT/sfdc_activity_' + str(i).zfill(3) + '.csv'),ignore_index=True)

	activity_df.to_csv('../SFDC_ACT/sfdc_activity.csv',encoding='utf-8')
	#activity_df.to_csv('../SFDC_ACT/sfdc_activity_' + cur_datetime.strftime('%Y%m%d') + '.csv',encoding='utf-8')

#activity_df = pd.read_csv('../SFDC_ACT/sfdc_activity_20160414.csv')

########################################
# op_df ... Query Opportunity Database 
########################################
printf("[update_churn_model_data.py]: Salesforce ... Query Opportunity object ... %.2f sec\n",time.time()-start)
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
op_df = op_df.join(pd.DataFrame(map((lambda item: item['Owner']['Name']),op_output['records']))).drop('Owner',1)
op_df = op_df.rename(columns={0:'Owner'})
op_df = op_df.drop('Account',1)
msa = []
close = []
for i in range(0,len(op_df)):
	try:
		msa.append(datetime.strptime(op_df.ix[i]['MSA_Effective_Date__c'],"%Y-%m-%d"))
	except:
		msa.append(None) 
	try:
		close.append(datetime.strptime(op_df.ix[i]['Close_Date__c'],"%Y-%m-%d"))
	except:
		close.append(None) 

op_df = op_df.join(pd.DataFrame(msa)).rename(columns={0:'MSA_Effective_Date'})
op_df = op_df.join(pd.DataFrame(close)).rename(columns={0:'Close_Date'})
op_df.MSA_Effective_Date = pd.to_datetime(op_df.MSA_Effective_Date)
op_df.Close_Date = pd.to_datetime(op_df.Close_Date)

#############################################
# Update Initial Term
#############################################
for i in range(0,len(op_df)):
	try:
		op_df.loc[i,'Initial_Term__c'] = int(op_df.ix[i]['Initial_Term__c'].replace(' Months',''))
	except:
		printf("%s.",i)

	if (op_df.ix[i]['Initial_Term__c'] == 'one (1)'):
		op_df.loc[i,'Initial_Term__c'] = 12
	elif (op_df.ix[i]['Initial_Term__c'] == 'two (2)'):
		op_df.loc[i,'Initial_Term__c'] = 24
	elif (op_df.ix[i]['Initial_Term__c'] == 'three (3)'):
		op_df.loc[i,'Initial_Term__c'] = 36
	elif (op_df.ix[i]['Initial_Term__c'] == 'four (4)'):
		op_df.loc[i,'Initial_Term__c'] = 48 
	elif (op_df.ix[i]['Initial_Term__c'] == 'five (5)'):
		op_df.loc[i,'Initial_Term__c'] = 60 
printf("\n")

#### Initial / Upsell sales only
op_initial_df = op_df[( (op_df.Type == 'Initial Sale') ) & \
                             (op_df.StageName == '6) Closed Won') & \
                             (op_df.Name.str.contains('Year 2') == False) & \
                             (op_df.Name.str.contains('Year 3') == False) & \
                             (op_df.Name.str.contains('Year 4') == False) & \
                             (op_df.Name.str.contains('Year 5') == False) & \
                             (op_df.Name.str.contains('Year 6') == False) & \
                             (op_df.MSA_Effective_Date + timedelta(days=365) > op_df.Close_Date) ] \
							 .reset_index(drop=True)


#for i in range(0,len(account_df)):
#	if (pd.notnull(account_df.ix[i]['Initial_Term_in_Months__c'])):
#		account_df.loc[i,'Initial_Term'] = account_df.ix[i]['Initial_Term_in_Months__c']
#	if (pd.notnull(account_df.ix[i]['Initial_Term_Length__c'])):
#		account_df.loc[i,'Initial_Term'] = account_df.ix[i]['Initial_Term_Length__c']
#	if (account_df.ix[i]['Initial_Term'] == 0):      # All blanks are 12 months
#		account_df.loc[i,'Initial_Term'] = 12
#
#initial_term_blank = {}
#initial_term_blank['0015000000k0P4TAAU'] = 12    # Reputation.com 
#initial_term_blank['0015000000kztDAAAY'] = 12    # Johnny Rockets Group 
#initial_term_blank['0015000000gSdGUAA0'] = 12    # Trulia 
#initial_term_blank['0015000000f9lMlAAI'] = 12    # IU Health 
#initial_term_blank['0015000000f9lKMAAY'] = 12    # IBEW Local 47 

################################
# Get opportunity history ...
################################
opportunity_history_df = sfdc_opportunity_history_query_account(sf,account_df[['AccountId_18','Name']])
opportunity_history_df = pd.merge(opportunity_history_df,op_df[['Id','ARR__c']],'left',left_on='OpportunityId',right_on='Id')
opportunity_history_df.to_csv('./output/sfdc_opportunity_history_' + cur_datetime.strftime('%Y%m%d') + '.csv',encoding='utf-8')

##################################################################################################
# Create an 'effective churn date' ... final date that they COULD churn with initial subscription
##################################################################################################
churn_deadline = []
past_churn_deadline = []
for i in range(0,len(account_df)):
	churn_deadline.append(account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3)))
	if(  (account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3))) > datetime.now()):
		past_churn_deadline.append(0)
	else:
		past_churn_deadline.append(1)
	
account_df['initial_churn_deadline'] = churn_deadline
account_df['past_churn_deadline'] = past_churn_deadline

churn_deadline_yr1 = []
churn_deadline_yr2 = []
churn_deadline_yr3 = []
churn_deadline_yr4 = []
churn_deadline_yr5 = []
past_churn_deadline_yr1 = []
past_churn_deadline_yr2 = []
past_churn_deadline_yr3 = []
past_churn_deadline_yr4 = []
past_churn_deadline_yr5 = []
for i in range(0,len(account_df)):
	if (account_df.ix[i]['Initial_Term_Length__c'] == 12):
		churn_deadline_yr1.append(account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3)))
		churn_deadline_yr2.append(account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3 + 12)))
		churn_deadline_yr3.append(account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3 + 24)))
		churn_deadline_yr4.append(account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3 + 36)))
		churn_deadline_yr5.append(account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3 + 48)))

		if(  (account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3))) > datetime.now()):
			past_churn_deadline_yr1.append(0)
		else:
			past_churn_deadline_yr1.append(1)

		if(  (account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3 + 12))) > datetime.now()):
			past_churn_deadline_yr2.append(0)
		else:
			past_churn_deadline_yr2.append(1)
	
		if(  (account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3 + 24))) > datetime.now()):
			past_churn_deadline_yr3.append(0)
		else:
			past_churn_deadline_yr3.append(1)

		if(  (account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3 + 36))) > datetime.now()):
			past_churn_deadline_yr4.append(0)
		else:
			past_churn_deadline_yr4.append(1)

		if(  (account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3 + 48))) > datetime.now()):
			past_churn_deadline_yr5.append(0)
		else:
			past_churn_deadline_yr5.append(1)


	if (account_df.ix[i]['Initial_Term_Length__c'] == 24):
		churn_deadline_yr1.append(None)
		churn_deadline_yr2.append(account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3)))
		churn_deadline_yr3.append(account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3 + 12)))
		churn_deadline_yr4.append(account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3 + 24)))
		churn_deadline_yr5.append(account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3 + 36)))
	
		past_churn_deadline_yr1.append('NA')

		if(  (account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3))) > datetime.now()):
			past_churn_deadline_yr2.append(0)
		else:
			past_churn_deadline_yr2.append(1)
	
		if(  (account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3 + 12))) > datetime.now()):
			past_churn_deadline_yr3.append(0)
		else:
			past_churn_deadline_yr3.append(1)

		if(  (account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3 + 24))) > datetime.now()):
			past_churn_deadline_yr4.append(0)
		else:
			past_churn_deadline_yr4.append(1)

		if(  (account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3 + 36))) > datetime.now()):
			past_churn_deadline_yr5.append(0)
		else:
			past_churn_deadline_yr5.append(1)


	if (account_df.ix[i]['Initial_Term_Length__c'] == 36):
		churn_deadline_yr1.append(None)
		churn_deadline_yr2.append(None)
		churn_deadline_yr3.append(account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3)))
		churn_deadline_yr4.append(account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3 + 12)))
		churn_deadline_yr5.append(account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3 + 24)))

		past_churn_deadline_yr1.append('NA')
		past_churn_deadline_yr2.append('NA')
	
		if(  (account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3))) > datetime.now()):
			past_churn_deadline_yr3.append(0)
		else:
			past_churn_deadline_yr3.append(1)

		if(  (account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3 + 12))) > datetime.now()):
			past_churn_deadline_yr4.append(0)
		else:
			past_churn_deadline_yr4.append(1)

		if(  (account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3 + 24))) > datetime.now()):
			past_churn_deadline_yr5.append(0)
		else:
			past_churn_deadline_yr5.append(1)

	if (account_df.ix[i]['Initial_Term_Length__c'] == 48):
		churn_deadline_yr1.append(None)
		churn_deadline_yr2.append(None)
		churn_deadline_yr3.append(None)
		churn_deadline_yr4.append(account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3)))
		churn_deadline_yr5.append(account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3 + 12)))

		past_churn_deadline_yr1.append('NA')
		past_churn_deadline_yr2.append('NA')
		past_churn_deadline_yr3.append('NA')
	
		if(  (account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3))) > datetime.now()):
			past_churn_deadline_yr4.append(0)
		else:
			past_churn_deadline_yr4.append(1)

		if(  (account_df.ix[i]['MSA_Effective_Date'] + relativedelta(months=(account_df.ix[i]['Initial_Term_Length__c']-3 + 12))) > datetime.now()):
			past_churn_deadline_yr5.append(0)
		else:
			past_churn_deadline_yr5.append(1)

account_df['churn_deadline_yr1'] = churn_deadline_yr1
account_df['churn_deadline_yr2'] = churn_deadline_yr2
account_df['churn_deadline_yr3'] = churn_deadline_yr3
account_df['churn_deadline_yr4'] = churn_deadline_yr4
account_df['churn_deadline_yr5'] = churn_deadline_yr5
account_df['past_churn_deadline_yr1'] = past_churn_deadline_yr1
account_df['past_churn_deadline_yr2'] = past_churn_deadline_yr2
account_df['past_churn_deadline_yr3'] = past_churn_deadline_yr3
account_df['past_churn_deadline_yr4'] = past_churn_deadline_yr4
account_df['past_churn_deadline_yr5'] = past_churn_deadline_yr5

##########################

op_12month_df = op_df[( (op_df.Type == 'Initial Sale') | \
                             (op_df.Type == 'Initial Sale - Channel') | \
                             (op_df.Type == 'Upsell/Cross-sell - AM') | \
                             (op_df.Type == 'Upsell/Cross-sell - Channel') | \
                             (op_df.Type == 'Upsell/Cross-sell') ) & \
                             (op_df.StageName == '6) Closed Won') & \
                             (op_df.Name.str.contains('Year 2') == False) & \
                             (op_df.Name.str.contains('Year 3') == False) & \
                             (op_df.Name.str.contains('Year 4') == False) & \
                             (op_df.Name.str.contains('Year 5') == False) & \
                             (op_df.Name.str.contains('Year 6') == False) & \
                             (op_df.MSA_Effective_Date + timedelta(days=365) > op_df.Close_Date) ] \
							 .reset_index(drop=True)

op_initial_upsell_df = op_df[( (op_df.Type == 'Initial Sale') | \
                             (op_df.Type == 'Initial Sale - Channel') | \
                             (op_df.Type == 'Upsell/Cross-sell - AM') | \
                             (op_df.Type == 'Upsell/Cross-sell - Channel') | \
                             (op_df.Type == 'Upsell/Cross-sell') ) & \
                             (op_df.StageName == '6) Closed Won') & \
                             (op_df.Name.str.contains('Year 2') == False) & \
                             (op_df.Name.str.contains('Year 3') == False) & \
                             (op_df.Name.str.contains('Year 4') == False) & \
                             (op_df.Name.str.contains('Year 5') == False) & \
                             (op_df.Name.str.contains('Year 6') == False) ] \
							 .reset_index(drop=True)

op_initial_df = op_df[( (op_df.Type == 'Initial Sale') | \
                             (op_df.Type == 'Initial Sale - Channel')) & \
                             (op_df.StageName == '6) Closed Won') & \
                             (op_df.Name.str.contains('Year 2') == False) & \
                             (op_df.Name.str.contains('Year 3') == False) & \
                             (op_df.Name.str.contains('Year 4') == False) & \
                             (op_df.Name.str.contains('Year 5') == False) & \
                             (op_df.Name.str.contains('Year 6') == False) ] \
							 .reset_index(drop=True)

op_upsell_df = op_df[( (op_df.Type == 'Upsell/Cross-sell - AM') | \
                             (op_df.Type == 'Upsell/Cross-sell - Channel') | \
                             (op_df.Type == 'Upsell/Cross-sell') ) & \
                             (op_df.StageName == '6) Closed Won') & \
                             (op_df.Name.str.contains('Year 2') == False) & \
                             (op_df.Name.str.contains('Year 3') == False) & \
                             (op_df.Name.str.contains('Year 4') == False) & \
                             (op_df.Name.str.contains('Year 5') == False) & \
                             (op_df.Name.str.contains('Year 6') == False) ] \
							 .reset_index(drop=True)
                             #(op_df.Close_Date__c > '2015-01-01') ]

###############################################
# sf_product_df ... Query OpportunityLineItem 
###############################################
printf("[update_churn_model_data.py]: Salesforce ... Query OpportunityLineItem (i.e. Product) object ... %.2f sec\n",time.time()-start)
query = "SELECT Id,OpportunityID,Library__c,Title_Number__c,ListPrice,Discount,TotalPrice,\
					UnitPrice,PricebookEntryId,PricebookEntry.Product2ID,PricebookEntry.Name, \
					Quantity,ServiceDate,Product_ID__c,Branding2__c,Video_Category__c, \
					Opportunity.Name,Opportunity.Type,Opportunity.StageName, \
					Opportunity.Close_Date__c,Opportunity.Churn_Date__c,ChargeType__c, \
					Opportunity.AccountId,Opportunity.Account.Name, \
					Opportunity.Account.MSA_Effective_Date__c,Opportunity.Upgrade__c \
					FROM OpportunityLineItem" #,Quantity, \
					#FROM OpportunityLineItem WHERE Library__c != 'O'" #,Quantity, \

start_product = time.time()
product_output = sf.query_all(query)
sf_product_df = pd.DataFrame(product_output['records']).drop('attributes',1)
sf_product_df = sf_product_df.rename(columns={'Id':'LineItemId'})
sf_product_df = sf_product_df.join(pd.DataFrame(map((lambda item: item['PricebookEntry']['Product2Id']),product_output['records'])))
sf_product_df = sf_product_df.rename(columns={0:'Product2Id'})
sf_product_df = sf_product_df.join(pd.DataFrame(map((lambda item: item['PricebookEntry']['Name']),product_output['records']))).drop('PricebookEntry',1) 
sf_product_df = sf_product_df.rename(columns={0:'PricebookEntry'})
sf_product_df = sf_product_df.join(pd.DataFrame(map((lambda item: item['Opportunity']['AccountId']),product_output['records'])))
sf_product_df = sf_product_df.rename(columns={0:'AccountId'})
sf_product_df = sf_product_df.join(pd.DataFrame(map((lambda item: item['Opportunity']['Account']['Name']),product_output['records'])))
sf_product_df = sf_product_df.rename(columns={0:'sf_account_name'})
sf_product_df = sf_product_df.join(pd.DataFrame(map((lambda item: item['Opportunity']['Account']['MSA_Effective_Date__c']),product_output['records'])))
sf_product_df = sf_product_df.rename(columns={0:'MSA_Effective_Date__c'})
sf_product_df = sf_product_df.join(pd.DataFrame(map((lambda item: item['Opportunity']['Name']),product_output['records'])))
sf_product_df = sf_product_df.rename(columns={0:'sf_op_name'})
sf_product_df = sf_product_df.join(pd.DataFrame(map((lambda item: item['Opportunity']['Type']),product_output['records'])))
sf_product_df = sf_product_df.rename(columns={0:'OpportunityType'})
sf_product_df = sf_product_df.join(pd.DataFrame(map((lambda item: item['Opportunity']['Close_Date__c']),product_output['records'])))
sf_product_df = sf_product_df.rename(columns={0:'Close_Date__c'})
sf_product_df = sf_product_df.join(pd.DataFrame(map((lambda item: item['Opportunity']['Churn_Date__c']),product_output['records'])))
sf_product_df = sf_product_df.rename(columns={0:'Churn_Date__c'})
sf_product_df = sf_product_df.join(pd.DataFrame(map((lambda item: item['Opportunity']['StageName']),product_output['records'])))
sf_product_df = sf_product_df.rename(columns={0:'StageName'})
sf_product_df = sf_product_df.join(pd.DataFrame(map((lambda item: item['Opportunity']['Upgrade__c']),product_output['records']))).drop('Opportunity',1)
sf_product_df = sf_product_df.rename(columns={0:'Upgrade__c'})

sf_product_df.to_csv('./output/sf_product_ALL_df.csv',encoding='utf-8')

### Correct Apttus issue ... 
## 1) Remove all one-time fees marked as products
sf_product_df = sf_product_df[(sf_product_df.ChargeType__c.str.contains('Subscription') == True) | (sf_product_df.ChargeType__c.str.contains('Tier') == True)].reset_index(drop=True)

## 2) Remove all 'O' products 
sf_product_noO_df = sf_product_df[(sf_product_df.Library__c != 'O')].reset_index(drop=True)

## 3) Remove all except for 'Open TBD' products 
sf_product_OpenTBD_df = sf_product_df[(sf_product_df.Product2Id == '01t38000002OFA1AAO')].reset_index(drop=True)

## 4) Merge 2) and 3)
sf_product_df = sf_product_noO_df.append(sf_product_OpenTBD_df).reset_index(drop=True)

## 5) Remove all Upgrades 
sf_product_df = sf_product_df[(sf_product_df.Upgrade__c == False)].reset_index(drop=True)

## Convert all ids to 15 characters
sf_product_df['AccountId_18'] = [x for x in sf_product_df.AccountId] ### Change AccountId to 15 characters
sf_product_df['AccountId'] = [x[0:15] for x in sf_product_df.AccountId] ### Change AccountId to 15 characters
sf_product_df['LineItemId'] = [x[0:15] for x in sf_product_df['LineItemId']]
sf_product_df['OpportunityId'] = [x[0:15] for x in sf_product_df['OpportunityId']]
#sf_product_df['Product2Id'] = [x[0:15] for x in sf_product_df['Product2Id']]
#sf_product_df['PricebookEntryId'] = [x[0:15] for x in sf_product_df['PricebookEntryId']]

### Write Churn Date to Close Date
for i in range(0,len(sf_product_df)):
	if (pd.notnull(sf_product_df.ix[i]['Churn_Date__c']) == True and pd.isnull(sf_product_df.ix[i]['Close_Date__c']) == True):
		sf_product_df.loc[i,'Close_Date__c'] = sf_product_df.ix[i]['Churn_Date__c']

##########################################
# MSA Lookup for past clients with no MSA
##########################################
msa_blank = {}
msa_blank['0015000000wlmZ2'] = '2012-09-19'   # The Lochton Companies
msa_blank['0015000000ldusg'] = '2013-09-12'   # Poms & Associates
msa_blank['0015000000gQADI'] = '2013-09-23'   # Spectrum Brands Holdings
msa_blank['0015000000gT3Ha'] = '2013-09-23'   # Power One
msa_blank['0015000000pzXqN'] = '2013-11-14'   # Britt Pogue 
msa_blank['0015000000f9lKT'] = '2013-09-13'   # Wells HCR Program 
		
msa = []
close = []
churn_date = []
for i in range(0,len(sf_product_df)):
	try:
		msa.append(datetime.strptime(sf_product_df.ix[i]['MSA_Effective_Date__c'],"%Y-%m-%d"))
	except:
		try:
			msa.append(datetime.strptime(msa_blank[sf_product_df.ix[i]['AccountId']],"%Y-%m-%d"))
		except:
			msa.append(None) 

	try:
		close.append(datetime.strptime(sf_product_df.ix[i]['Close_Date__c'],"%Y-%m-%d"))
	except:
		close.append(None) 

sf_product_df = sf_product_df.join(pd.DataFrame(msa)).rename(columns={0:'MSA_Effective_Date'})
sf_product_df = sf_product_df.join(pd.DataFrame(close)).rename(columns={0:'Close_Date'})
sf_product_df.MSA_Effective_Date = pd.to_datetime(sf_product_df.MSA_Effective_Date)
sf_product_df.Close_Date = pd.to_datetime(sf_product_df.Close_Date)

#######################################
# Update initial term of opportunities
#######################################
sf_product_df = pd.merge(sf_product_df,op_df[['Id','Initial_Term__c','Subscription_Start_Date__c','Renewal_Date__c']],'left',left_on='OpportunityId',right_on='Id').drop('Id',1)

###############################################
# sf_product_df .... i
# 1) Add Order Number and Year
# 2) Update Video Swap Quantity
###############################################
order_number = []
year = []
for i in range(0,len(sf_product_df)):
	try:
		year.append(sf_product_df['sf_op_name'][i].split(' - ')[2])
	except:
		year.append(None)
	try:
		order_number.append(sf_product_df['sf_op_name'][i].split(' - ')[1])
	except:
		order_number.append(None)
sf_product_df = sf_product_df.join(pd.DataFrame(year)).rename(columns = {0:'year'})  
sf_product_df = sf_product_df.join(pd.DataFrame(order_number)).rename(columns = {0:'order_number'}) 

for i in range(0,len(sf_product_df)):
	if (sf_product_df['Branding2__c'][i] == 'L'):
		sf_product_df['Branding2__c'][i] = 'Lite'	
	if (sf_product_df['Branding2__c'][i] == 'P'):
		sf_product_df['Branding2__c'][i] = 'Premium'	
	if (sf_product_df['Branding2__c'][i] == 'Plus'):
		sf_product_df['Branding2__c'][i] = 'PremiumPlus'	

##########################################
# Correct quantity for swaps
#
# Needed b/c Apttus documents swap in the PRICE instead of QUANTITY
#
##########################################
for i in range(0,len(sf_product_df)):
#	if (sf_product_df.ix[i]['OpportunityType'] == 'Video Swap'):
	if (sf_product_df.ix[i]['TotalPrice'] < 0):
		sf_product_df.loc[i,'Quantity'] = -abs(sf_product_df.ix[i]['Quantity'])
	elif (sf_product_df.ix[i]['TotalPrice'] > 0):
		sf_product_df.loc[i,'Quantity'] = abs(sf_product_df.ix[i]['Quantity'])

##########################
# Sum all opportunities
##########################
sf_product_complete_df = sf_product_df.copy(deep=True) \
							[ ( (sf_product_df.StageName == '6) Closed Won') \
							|   (sf_product_df.StageName == '8) Cancelled') ) &  \
                             	(sf_product_df.sf_op_name.str.contains('Year 2') == False) & \
                      			(sf_product_df.sf_op_name.str.contains('Year 3') == False) & \
                             	(sf_product_df.sf_op_name.str.contains('Year 4') == False) & \
                            	(sf_product_df.sf_op_name.str.contains('Year 5') == False) & \
                             	(sf_product_df.sf_op_name.str.contains('Year 6') == False) ] \
								.reset_index(drop=True)

sf_product_complete_DOWNTICK_df = sf_product_complete_df[(sf_product_complete_df.Quantity < 0) & (sf_product_complete_df.OpportunityType.str.contains('Renewal'))]
sf_product_complete_NoDOWNTICK_df = sf_product_complete_df.drop(sf_product_complete_df.index[sf_product_complete_DOWNTICK_df.index]).reset_index(drop=True)

sf_product_summary_PRODUCT_df = sf_product_complete_NoDOWNTICK_df.groupby(['AccountId','Close_Date','OpportunityId','sf_op_name','OpportunityType','StageName']) \
							.apply(lambda x: list(x.Product2Id) ) 
							#.apply(lambda x: ';'.join(x.Product2Id) ) 

sf_product_summary_PRICEBOOKENTRY_df = sf_product_complete_NoDOWNTICK_df.groupby(['AccountId','Close_Date','OpportunityId','sf_op_name','OpportunityType','StageName']) \
							.apply(lambda x: list(x.PricebookEntry) ) 
							#.apply(lambda x: ';'.join(x.Product2Id) ) 

sf_product_summary_QUANTITY_df = sf_product_complete_NoDOWNTICK_df.groupby(['AccountId','Close_Date','OpportunityId','sf_op_name','OpportunityType','StageName']) \
							.apply(lambda x: list(map(str,map(int,x.Quantity) )) ) 
							#.apply(lambda x: ';'.join(map(str,map(int,x.Quantity) )) ) 

sf_product_summary_SUMQUANTITY_df = sf_product_complete_NoDOWNTICK_df.groupby(['AccountId','Close_Date','OpportunityId','sf_op_name','OpportunityType','StageName']) \
							.agg({"LineItemId": "count","Quantity":np.sum }) \
							.rename(columns = {'LineItemId':'Nlineitem_total','Quantity':'Nquantity_total'})
							#.apply(lambda x: ';'.join(map(str,map(int,x.Quantity) )) ) 


sf_product_summary_df = pd.merge(pd.DataFrame(sf_product_summary_PRODUCT_df).rename(columns={0:'Product2Id_group'}), \
								 pd.DataFrame(sf_product_summary_PRICEBOOKENTRY_df).rename(columns={0:'PricebookEntry_group'}), \
								 'left',left_index=True,right_index=True)

sf_product_summary_df = pd.merge(sf_product_summary_df, \
								 pd.DataFrame(sf_product_summary_QUANTITY_df).rename(columns={0:'Quantity_group'}), \
								 'left',left_index=True,right_index=True)

sf_product_summary_df = pd.merge(sf_product_summary_df, \
								 pd.DataFrame(sf_product_summary_SUMQUANTITY_df), \
								 'left',left_index=True,right_index=True).reset_index().sort(['AccountId','Close_Date'])

delta = cur_datetime.date() - sf_product_summary_df.Close_Date
sf_product_summary_df['year'] = [-x.astype('timedelta64[Y]') for x in delta]

sf_product_summary_df = pd.merge(sf_product_summary_df, \
								 account_df[['Id','churn_int','Total_Nmbr_Videos__c']], \
								 'left',left_on='AccountId',right_on='Id').drop('Id',1)

sf_product_summary_df = pd.merge(sf_product_summary_df,op_df[['Id','AccountId_18','Initial_Term__c','Subscription_Start_Date__c','Renewal_Date__c','Upgrade__c']],'left',left_on='OpportunityId',right_on='Id').drop('Id',1)

#### Sort in the proper form
sf_product_summary_df = sf_product_summary_df.sort(['AccountId_18','Subscription_Start_Date__c','Close_Date']).reset_index(drop=True)

#################################################################
# Put video swaps after renewals IF same subscription start date
#################################################################

#swap_accounts = list(set(sf_product_df.ix[all_indices('Video Swap',sf_product_summary_df['OpportunityType'])]['AccountId_18'])) 
#for i in range(0,len(swap_accounts)):
#sys.exit()

###################################
# Find the end of each opportunity
###################################
op_end = []
op_current = []
for i in range(0,len(sf_product_summary_df)):
	try:
		op_end.append(sf_product_summary_df.ix[i]['Close_Date'] + relativedelta(months=(sf_product_summary_df.ix[i]['Initial_Term__c'])))
	except:
		op_end.append(None)

	try:
		if (op_end[i].date() > cur_datetime.date() ):
			op_current.append(1)
		else:
			op_current.append(0)
	except:
		op_current.append(None)

#dt = datetime.fromtimestamp(1346236702)
#print time.mktime(dt.timetuple())	

sf_product_summary_df['op_end'] = op_end 
sf_product_summary_df['op_current'] = op_current 

sf_product_summary_header = [
'AccountId',
'AccountId_18',
'OpportunityId',
'sf_op_name',
'Upgrade__c',   
'Close_Date',   
'op_end',   
'op_current',   
'Subscription_Start_Date__c',   
'Renewal_Date__c',   
'Initial_Term__c',
'year',
'churn_int',
'OpportunityType',
'StageName',
'Total_Nmbr_Videos__c',
'Nlineitem_total',
'Nquantity_total',
'Product2Id_group',
'Quantity_group', 
'PricebookEntry_group' ] 
sf_product_summary_df.to_csv('./output/sf_product_summary_df.csv',encoding='utf-8',columns=sf_product_summary_header)

### Group values by sfdc_id
sf_act_product_summary_df = sf_product_summary_df[['AccountId','AccountId_18','OpportunityId','Nlineitem_total','Nquantity_total']][(sf_product_summary_df.op_current == 1)] \
						.groupby(['AccountId']) \
						.agg({"OpportunityId":pd.Series.nunique,"Nlineitem_total": pd.Series.nunique,"Nquantity_total":np.sum  }) \
						.rename(columns = {'OpportunityId':'Nopportunity_cur','Nlineitem_total':'NlineItem_cur','Nquantity_total':'Nvideo_cur'}).reset_index()

account_df = pd.merge(account_df,sf_act_product_summary_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
printf("[update_churn_model_data.py]: Salesforce ... No of Accounts: %s ... %.2f sec\n",len(account_df),time.time()-start)

#######################################
# Get Nopportunity and Nvideo
#######################################

#### Initial sales only

sf_product_initial_df = sf_product_df.copy(deep=True) \
							[ ( (sf_product_df.OpportunityType == 'Initial Sale') \
							|   (sf_product_df.OpportunityType == 'Initial Sale - Channel') ) \
							&   (sf_product_df.StageName == '6) Closed Won') \
							&   (sf_product_df.sf_account_name != 'Test Apttus - ManofIT')] \
							.reset_index(drop=True)

sf_act_initial_count_df = sf_product_initial_df[['AccountId','OpportunityId','LineItemId','Quantity']].groupby(['AccountId']) \
						.agg({"OpportunityId":pd.Series.nunique,"LineItemId": pd.Series.nunique,"Quantity":np.sum  }) \
						.rename(columns = {'OpportunityId':'Nopportunity_Initial','LineItemId':'NlineItem_Initial','Quantity':'Nvideo_Initial'}).reset_index()

####################################
# Final join to sfdc account_df
####################################
account_df = pd.merge(account_df,sf_act_initial_count_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
printf("[update_churn_model_data.py]: Salesforce ... No of Accounts: %s\n",len(account_df))

#######################################
# Get Nopportunity and Nvideo
#######################################

# 1) All Years ... count opportunties for all accounts 
sf_product_all_won_df = sf_product_df.copy(deep=True) \
							[   (sf_product_df.StageName == '6) Closed Won') \
							&   (sf_product_df.sf_account_name != 'Test Apttus - ManofIT')] \
							.reset_index(drop=True)

sf_act_all_won_df = sf_product_all_won_df[['AccountId','OpportunityId']].groupby(['AccountId']) \
						.agg({"OpportunityId":pd.Series.nunique}) \
						.rename(columns = {'OpportunityId':'Nopportunity_All'}).reset_index()

account_df = pd.merge(account_df,sf_act_all_won_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)

#### Initial / Upsell sales only

# 1) All Years ... UPDATE HERE ... ADD SWAP COUNTS ... BC THEY DO NOT HAVE TO BE +1/-1 see 0063800000ZextF
sf_product_won_df = sf_product_df.copy(deep=True) \
							[ ( (sf_product_df.OpportunityType == 'Initial Sale') \
							|   (sf_product_df.OpportunityType == 'Initial Sale - Channel') \
							|   (sf_product_df.OpportunityType == 'Upsell/Cross-sell - AM') \
							|   (sf_product_df.OpportunityType == 'Upsell/Cross-sell - Channel') \
							|   (sf_product_df.OpportunityType == 'Upsell/Cross-sell') \
							|   (sf_product_df.OpportunityType == 'Video Swap') ) \
							&   (sf_product_df.StageName == '6) Closed Won') \
							&   (sf_product_df.sf_account_name != 'Test Apttus - ManofIT')] \
							.reset_index(drop=True)

sf_act_op_count_df = sf_product_won_df[['AccountId','OpportunityId','LineItemId','Quantity']].groupby(['AccountId']) \
						.agg({"OpportunityId":pd.Series.nunique,"LineItemId": pd.Series.nunique,"Quantity":np.sum  }) \
						.rename(columns = {'OpportunityId':'Nopportunity_Total','LineItemId':'NlineItem_Total','Quantity':'Nvideo_Total'}).reset_index()

sf_act_op_count_1mo_df = calc_day_details(sf_product_won_df,31,'1mo',['Close_Date'],True,sf_act_op_count_df)
sf_act_op_count_2mo_df = calc_day_details(sf_product_won_df,61,'2mo',['Close_Date'],True,sf_act_op_count_df)
sf_act_op_count_3mo_df = calc_day_details(sf_product_won_df,91,'3mo',['Close_Date'],True,sf_act_op_count_df)
sf_act_op_count_4mo_df = calc_day_details(sf_product_won_df,122,'4mo',['Close_Date'],True,sf_act_op_count_df)
sf_act_op_count_5mo_df = calc_day_details(sf_product_won_df,152,'5mo',['Close_Date'],True,sf_act_op_count_df)
sf_act_op_count_6mo_df = calc_day_details(sf_product_won_df,182,'6mo',['Close_Date'],True,sf_act_op_count_df)
sf_act_op_count_7mo_df = calc_day_details(sf_product_won_df,212,'7mo',['Close_Date'],True,sf_act_op_count_df)
sf_act_op_count_8mo_df = calc_day_details(sf_product_won_df,243,'8mo',['Close_Date'],True,sf_act_op_count_df)
sf_act_op_count_9mo_df = calc_day_details(sf_product_won_df,273,'9mo',['Close_Date'],True,sf_act_op_count_df)
sf_act_op_count_10mo_df = calc_day_details(sf_product_won_df,304,'10mo',['Close_Date'],True,sf_act_op_count_df)
sf_act_op_count_11mo_df = calc_day_details(sf_product_won_df,334,'11mo',['Close_Date'],True,sf_act_op_count_df)
sf_act_op_count_12mo_df = calc_day_details(sf_product_won_df,365,'12mo',['Close_Date'],True,sf_act_op_count_df)
sf_act_op_count_15mo_df = calc_day_details(sf_product_won_df,456,'15mo',['Close_Date'],True,sf_act_op_count_df)
sf_act_op_count_18mo_df = calc_day_details(sf_product_won_df,547,'18mo',['Close_Date'],True,sf_act_op_count_df)
sf_act_op_count_21mo_df = calc_day_details(sf_product_won_df,638,'21mo',['Close_Date'],True,sf_act_op_count_df)

####################################
# Final join to sfdc account_df
####################################
account_df = pd.merge(account_df,sf_act_op_count_1mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_op_count_2mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_op_count_3mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_op_count_4mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_op_count_5mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_op_count_6mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_op_count_7mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_op_count_8mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_op_count_9mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_op_count_10mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_op_count_11mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_op_count_12mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_op_count_15mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_op_count_18mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_op_count_21mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_op_count_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)

#######################################
# Get Video Swaps
#######################################

#### Initial / Upsell sales only

# 1) All Years ... all swaps ... only count >0 otherwise it will set to 0
sf_product_swap_df = sf_product_df.copy(deep=True) \
							[ 	(sf_product_df.OpportunityType == 'Video Swap') \
							&   (sf_product_df.StageName == '6) Closed Won') \
							&   (sf_product_df.sf_account_name != 'Test Apttus - ManofIT') \
							&   (sf_product_df.Quantity > 0)] \
							.reset_index(drop=True)

sf_act_swap_count_df = sf_product_swap_df[['AccountId','OpportunityId','LineItemId','Quantity']].groupby(['AccountId']) \
						.agg({"OpportunityId":pd.Series.nunique,"LineItemId": pd.Series.nunique,"Quantity":np.sum  }) \
						.rename(columns = {'OpportunityId':'Nopportunity_Total_swap','LineItemId':'NlineItem_Total_swap','Quantity':'Nvideo_Total_swap'}).reset_index()

sf_act_swap_count_df = pd.merge(sf_act_swap_count_df,sf_act_op_count_df[['AccountId']],'right',left_on='AccountId',right_on='AccountId').fillna(0)

sf_act_swap_count_1mo_df = calc_day_details(sf_product_swap_df,31,'1mo_swap',['Close_Date'],True,sf_act_op_count_df)
sf_act_swap_count_2mo_df = calc_day_details(sf_product_swap_df,61,'2mo_swap',['Close_Date'],True,sf_act_op_count_df)
sf_act_swap_count_3mo_df = calc_day_details(sf_product_swap_df,91,'3mo_swap',['Close_Date'],True,sf_act_op_count_df)
sf_act_swap_count_4mo_df = calc_day_details(sf_product_swap_df,122,'4mo_swap',['Close_Date'],True,sf_act_op_count_df)
sf_act_swap_count_5mo_df = calc_day_details(sf_product_swap_df,152,'5mo_swap',['Close_Date'],True,sf_act_op_count_df)
sf_act_swap_count_6mo_df = calc_day_details(sf_product_swap_df,182,'6mo_swap',['Close_Date'],True,sf_act_op_count_df)
sf_act_swap_count_7mo_df = calc_day_details(sf_product_swap_df,212,'7mo_swap',['Close_Date'],True,sf_act_op_count_df)
sf_act_swap_count_8mo_df = calc_day_details(sf_product_swap_df,243,'8mo_swap',['Close_Date'],True,sf_act_op_count_df)
sf_act_swap_count_9mo_df = calc_day_details(sf_product_swap_df,273,'9mo_swap',['Close_Date'],True,sf_act_op_count_df)
sf_act_swap_count_10mo_df = calc_day_details(sf_product_swap_df,304,'10mo_swap',['Close_Date'],True,sf_act_op_count_df)
sf_act_swap_count_11mo_df = calc_day_details(sf_product_swap_df,334,'11mo_swap',['Close_Date'],True,sf_act_op_count_df)
sf_act_swap_count_12mo_df = calc_day_details(sf_product_swap_df,365,'12mo_swap',['Close_Date'],True,sf_act_op_count_df)
sf_act_swap_count_15mo_df = calc_day_details(sf_product_swap_df,456,'15mo_swap',['Close_Date'],True,sf_act_op_count_df)
sf_act_swap_count_18mo_df = calc_day_details(sf_product_swap_df,547,'18mo_swap',['Close_Date'],True,sf_act_op_count_df)
sf_act_swap_count_21mo_df = calc_day_details(sf_product_swap_df,638,'21mo_swap',['Close_Date'],True,sf_act_op_count_df)

####################################
# Final join to sfdc account_df
####################################
account_df = pd.merge(account_df,sf_act_swap_count_1mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_swap_count_2mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_swap_count_3mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_swap_count_4mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_swap_count_5mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_swap_count_6mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_swap_count_7mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_swap_count_8mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_swap_count_9mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_swap_count_10mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_swap_count_11mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_swap_count_12mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_swap_count_15mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_swap_count_18mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_swap_count_21mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_swap_count_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
printf("[update_churn_model_data.py]: Salesforce ... No of Accounts: %s\n",len(account_df))

#######################################
# Get D-titles 
#######################################

#### Initial / Upsell sales only

# 1) All Years ... all D-titles ... only count >0 otherwise it will set to 0
sf_product_Dinitial_df = sf_product_df.copy(deep=True) \
							[ ( (sf_product_df.OpportunityType == 'Initial Sale') \
							|   (sf_product_df.OpportunityType == 'Initial Sale - Channel') ) \
							&   (sf_product_df.StageName == '6) Closed Won') \
							&   (sf_product_df.Library__c == 'D') \
							&   (sf_product_df.sf_account_name != 'Test Apttus - ManofIT')] \
							.reset_index(drop=True)

sf_act_Dinitial_count_df = sf_product_Dinitial_df[['AccountId','OpportunityId','LineItemId','Quantity']].groupby(['AccountId']) \
						.agg({"OpportunityId":pd.Series.nunique,"LineItemId": pd.Series.nunique,"Quantity":np.sum  }) \
						.rename(columns = {'OpportunityId':'Nopportunity_Dinitial','LineItemId':'NlineItem_Dinitial','Quantity':'Nvideo_Dinitial'}).reset_index()
	
sf_act_Dinitial_count_df = pd.merge(sf_act_Dinitial_count_df,sf_act_initial_count_df[['AccountId']],'right',left_on='AccountId',right_on='AccountId').fillna(0)

####################################
# Final join to sfdc account_df
####################################
account_df = pd.merge(account_df,sf_act_Dinitial_count_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
printf("[update_churn_model_data.py]: Salesforce ... No of Accounts: %s\n",len(account_df))

#######################################
# Get Initial New Title counts 
#######################################

#### Initial / Upsell sales only

# 1) All Years ... all New Title titles ... only count >0 otherwise it will set to 0
sf_product_NewTitle_initial_df = sf_product_df.copy(deep=True) \
							[ ( (sf_product_df.OpportunityType == 'Initial Sale') \
							|   (sf_product_df.OpportunityType == 'Initial Sale - Channel') ) \
							&   (sf_product_df.StageName == '6) Closed Won') \
							&   (sf_product_df.PricebookEntry.str.contains('New Title')) \
							&   (sf_product_df.sf_account_name != 'Test Apttus - ManofIT')] \
							.reset_index(drop=True)

sf_act_NewTitle_initial_count_df = sf_product_NewTitle_initial_df[['AccountId','OpportunityId','LineItemId','Quantity']].groupby(['AccountId']) \
						.agg({"OpportunityId":pd.Series.nunique,"LineItemId": pd.Series.nunique,"Quantity":np.sum  }) \
						.rename(columns = {'OpportunityId':'Nopportunity_NewTitle_initial','LineItemId':'NlineItem_NewTitle_initial','Quantity':'Nvideo_NewTitle_initial'}).reset_index()

sf_act_NewTitle_initial_count_df = pd.merge(sf_act_NewTitle_initial_count_df,sf_act_initial_count_df[['AccountId']],'right',left_on='AccountId',right_on='AccountId').fillna(0)

####################################
# Final join to sfdc account_df
####################################
account_df = pd.merge(account_df,sf_act_NewTitle_initial_count_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
printf("[update_churn_model_data.py]: Salesforce ... No of Accounts: %s\n",len(account_df))

#######################################
# Get Initial TBD counts 
#######################################

#### Initial / Upsell sales only

# 1) All Years ... all TBD-titles ... only count >0 otherwise it will set to 0
sf_product_TBDinitial_df = sf_product_df.copy(deep=True) \
							[ ( (sf_product_df.OpportunityType == 'Initial Sale') \
							|   (sf_product_df.OpportunityType == 'Initial Sale - Channel') ) \
							&   (sf_product_df.StageName == '6) Closed Won') \
							&   (sf_product_df.PricebookEntry.str.contains('TBD')) \
							&   (sf_product_df.sf_account_name != 'Test Apttus - ManofIT')] \
							.reset_index(drop=True)

sf_act_TBDinitial_count_df = sf_product_TBDinitial_df[['AccountId','OpportunityId','LineItemId','Quantity']].groupby(['AccountId']) \
						.agg({"OpportunityId":pd.Series.nunique,"LineItemId": pd.Series.nunique,"Quantity":np.sum  }) \
						.rename(columns = {'OpportunityId':'Nopportunity_TBDinitial','LineItemId':'NlineItem_TBDinitial','Quantity':'Nvideo_TBDinitial'}).reset_index()

sf_act_TBDinitial_count_df = pd.merge(sf_act_TBDinitial_count_df,sf_act_initial_count_df[['AccountId']],'right',left_on='AccountId',right_on='AccountId').fillna(0)

####################################
# Final join to sfdc account_df
####################################
account_df = pd.merge(account_df,sf_act_TBDinitial_count_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
printf("[update_churn_model_data.py]: Salesforce ... No of Accounts: %s\n",len(account_df))

########################################
# Find max churn date for each account
########################################
min_churn_date_df = op_df[['AccountId','Churn_Date__c']][pd.notnull(op_df.Churn_Date__c)] \
						.sort('Churn_Date__c',ascending=True).groupby(['AccountId']).first().reset_index()
max_churn_date_df = op_df[['AccountId','Churn_Date__c']][pd.notnull(op_df.Churn_Date__c)] \
						.sort('Churn_Date__c',ascending=False).groupby(['AccountId']).first().reset_index()

account_df = pd.merge(account_df,min_churn_date_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1).rename(columns={'Churn_Date__c':'min_Churn_Date'})
account_df = pd.merge(account_df,max_churn_date_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1).rename(columns={'Churn_Date__c':'max_Churn_Date'})
printf("[update_churn_model_data.py]: Salesforce ... No of Accounts: %s\n",len(account_df))

###################################
# Make some columns integers
###################################
for i in range(0,len(account_df)):
	if (account_df.ix[i]['Product_Line__c'] == 'Lite'):
		account_df.loc[i,'product_line_int'] = 0	
	elif (account_df.ix[i]['Product_Line__c'] == 'Premium'):
		account_df.loc[i,'product_line_int'] = 1	
	elif (account_df.ix[i]['Product_Line__c'] == 'PremiumPlus'):
		account_df.loc[i,'product_line_int'] = 2	

for i in range(0,len(account_df)):
	if (pd.isnull(account_df.ix[i]['Channel_Partner__c'])):
		account_df.loc[i,'channel_partner_int'] = 0	
	else: 
		account_df.loc[i,'channel_partner_int'] = 1	

customer_lifetime = []
churn_year = []
churn_yr1  = []
churn_yr2  = []
churn_yr3  = []
churn_yr4  = []
churn_yr5  = []
for i in range(0,len(account_df)):
	if (account_df.ix[i]['churn_int'] == 1):
		try:
			cur_lifespan = float((datetime.strptime(account_df.ix[i]['max_Churn_Date'],"%Y-%m-%d") - account_df.ix[i]['MSA_Effective_Date']).days-1) / 365
		except:
			cur_lifespan = account_df.ix[i]['Customer_Lifespan__c']
		
		try:
			customer_lifetime.append(cur_lifespan)
		except:
			customer_lifetime.append('NA')

		try:
			cur_churn_year = int(cur_lifespan - 0.01) + 1 
			churn_year.append(cur_churn_year) 
			try:
				if (cur_churn_year == 1):
					churn_yr1.append(1)
					churn_yr2.append('NA')
					churn_yr3.append('NA')
					churn_yr4.append('NA')
					churn_yr5.append('NA')
				elif (cur_churn_year == 2):
					if (account_df.ix[i]['Initial_Term_Length__c'] == 12):
						churn_yr1.append(0)
					else:
						churn_yr1.append('NA')
					churn_yr2.append(1)
					churn_yr3.append('NA')
					churn_yr4.append('NA')
					churn_yr5.append('NA')
				elif (cur_churn_year == 3):
					if (account_df.ix[i]['Initial_Term_Length__c'] == 12):
						churn_yr1.append(0)
					else:
						churn_yr1.append('NA')
					if (account_df.ix[i]['Initial_Term_Length__c'] == 24):
						churn_yr2.append(0)
					else:
						churn_yr2.append('NA')
					churn_yr3.append(1)
					churn_yr4.append('NA')
					churn_yr5.append('NA')
				elif (cur_churn_year == 4):
					if (account_df.ix[i]['Initial_Term_Length__c'] == 12):
						churn_yr1.append(0)
					else:
						churn_yr1.append('NA')
					if (account_df.ix[i]['Initial_Term_Length__c'] == 24):
						churn_yr2.append(0)
					else:
						churn_yr2.append('NA')
					if (account_df.ix[i]['Initial_Term_Length__c'] == 36):
						churn_yr3.append(0)
					else:
						churn_yr3.append('NA')
					churn_yr4.append(1)
					churn_yr5.append('NA')
				elif (cur_churn_year == 5):
					if (account_df.ix[i]['Initial_Term_Length__c'] == 12):
						churn_yr1.append(0)
					else:
						churn_yr1.append('NA')
					if (account_df.ix[i]['Initial_Term_Length__c'] == 24):
						churn_yr2.append(0)
					else:
						churn_yr2.append('NA')
					if (account_df.ix[i]['Initial_Term_Length__c'] == 36):
						churn_yr3.append(0)
					else:
						churn_yr3.append('NA')
					if (account_df.ix[i]['Initial_Term_Length__c'] == 48):
						churn_yr4.append(0)
					else:
						churn_yr4.append('NA')
					churn_yr5.append(1)
				elif (cur_churn_year > 5):
					if (account_df.ix[i]['Initial_Term_Length__c'] == 12):
						churn_yr1.append(0)
					else:
						churn_yr1.append('NA')
					if (account_df.ix[i]['Initial_Term_Length__c'] == 24):
						churn_yr2.append(0)
					else:
						churn_yr2.append('NA')
					if (account_df.ix[i]['Initial_Term_Length__c'] == 36):
						churn_yr3.append(0)
					else:
						churn_yr3.append('NA')
					if (account_df.ix[i]['Initial_Term_Length__c'] == 48):
						churn_yr4.append(0)
					else:
						churn_yr4.append('NA')
					if (account_df.ix[i]['Initial_Term_Length__c'] == 60):
						churn_yr5.append(0)
					else:
						churn_yr5.append('NA')
			except:
				churn_yr1.append('NA')
				churn_yr2.append('NA')
				churn_yr3.append('NA')
				churn_yr4.append('NA')
				churn_yr5.append('NA')
		except:
			churn_year.append('NA')
			churn_yr1.append('NA')
			churn_yr2.append('NA')
			churn_yr3.append('NA')
			churn_yr4.append('NA')
			churn_yr5.append('NA')

	else:
		customer_lifetime.append(account_df.ix[i]['Customer_Lifespan__c'])
		churn_year.append('NA') 
		if (account_df.ix[i]['past_churn_deadline_yr1'] == 1):
			churn_yr1.append(0)
		else:
			churn_yr1.append('NA')
		if (account_df.ix[i]['past_churn_deadline_yr2'] == 1):
			churn_yr2.append(0)
		else:
			churn_yr2.append('NA')
		if (account_df.ix[i]['past_churn_deadline_yr3'] == 1):
			churn_yr3.append(0)
		else:
			churn_yr3.append('NA')
		if (account_df.ix[i]['past_churn_deadline_yr4'] == 1):
			churn_yr4.append(0)
		else:
			churn_yr4.append('NA')
		if (account_df.ix[i]['past_churn_deadline_yr5'] == 1):
			churn_yr5.append(0)
		else:
			churn_yr5.append('NA')


account_df = account_df.join(pd.DataFrame(customer_lifetime)).rename(columns={0:'Calc_Customer_Lifespan'}) 
account_df = account_df.join(pd.DataFrame(churn_year)).rename(columns={0:'churn_year'}) 
account_df = account_df.join(pd.DataFrame(churn_yr1)).rename(columns={0:'churn_yr1'}) 
account_df = account_df.join(pd.DataFrame(churn_yr2)).rename(columns={0:'churn_yr2'}) 
account_df = account_df.join(pd.DataFrame(churn_yr3)).rename(columns={0:'churn_yr3'}) 
account_df = account_df.join(pd.DataFrame(churn_yr4)).rename(columns={0:'churn_yr4'}) 
account_df = account_df.join(pd.DataFrame(churn_yr5)).rename(columns={0:'churn_yr5'}) 
printf("[update_churn_model_data.py]: Salesforce ... No of Accounts: %s\n",len(account_df))

###################################
# Calculate customer lifetime bins
# Bins are >lower and <= higher
###################################			
customer_lifetime_00_05 = []
customer_lifetime_05_10 = []
customer_lifetime_10_15 = []
customer_lifetime_15_20 = []
customer_lifetime_20_25 = []
customer_lifetime_25_30 = []
customer_lifetime_30_ = []
for i in range(0,len(account_df)):
	cur_account_lifespan = int(float(account_df.ix[i]['Calc_Customer_Lifespan']-.000000001)/float(0.5))
	if (cur_account_lifespan > 0):
		customer_lifetime_00_05.append(0)
	else:
		customer_lifetime_00_05.append('NA')
	if (cur_account_lifespan > 1):
		customer_lifetime_05_10.append(0)
	else:
		customer_lifetime_05_10.append('NA')

	if (cur_account_lifespan > 2):
		customer_lifetime_10_15.append(0)
	else:
		customer_lifetime_10_15.append('NA')
	if (cur_account_lifespan > 3):
		customer_lifetime_15_20.append(0)
	else:
		customer_lifetime_15_20.append('NA')

	if (cur_account_lifespan > 4):
		customer_lifetime_20_25.append(0)
	else:
 		customer_lifetime_20_25.append('NA')
	if (cur_account_lifespan > 5):
		customer_lifetime_25_30.append(0)
	else:
		customer_lifetime_25_30.append('NA')

	if (cur_account_lifespan > 6):
		customer_lifetime_30_.append(0)
	else:
		customer_lifetime_30_.append('NA')

	##### Update churned accounts
	if (account_df.ix[i]['churn_int'] == 1):
		if (cur_account_lifespan == 0):
			customer_lifetime_00_05[i] = 1
		if (cur_account_lifespan == 1):
			customer_lifetime_05_10[i] = 1
		if (cur_account_lifespan == 2):
			customer_lifetime_10_15[i] = 1
		if (cur_account_lifespan == 3):
			customer_lifetime_15_20[i] = 1
		if (cur_account_lifespan == 4):
			customer_lifetime_20_25[i] = 1
		if (cur_account_lifespan == 5):
			customer_lifetime_25_30[i] = 1
		if (cur_account_lifespan >= 6):
			customer_lifetime_30_[i] = 1
			
   # else:
   #     customer_lifetime.append(account_df.ix[i]['Customer_Lifespan__c'])
account_df = account_df.join(pd.DataFrame(customer_lifetime_00_05))
account_df = account_df.rename(columns={0:'customer_lifespan_00_05'})
account_df = account_df.join(pd.DataFrame(customer_lifetime_05_10))
account_df = account_df.rename(columns={0:'customer_lifespan_05_10'})
account_df = account_df.join(pd.DataFrame(customer_lifetime_10_15))
account_df = account_df.rename(columns={0:'customer_lifespan_10_15'})
account_df = account_df.join(pd.DataFrame(customer_lifetime_15_20))
account_df = account_df.rename(columns={0:'customer_lifespan_15_20'})
account_df = account_df.join(pd.DataFrame(customer_lifetime_20_25))
account_df = account_df.rename(columns={0:'customer_lifespan_20_25'})
account_df = account_df.join(pd.DataFrame(customer_lifetime_25_30))
account_df = account_df.rename(columns={0:'customer_lifespan_25_30'})
account_df = account_df.join(pd.DataFrame(customer_lifetime_30_))
account_df = account_df.rename(columns={0:'customer_lifespan_30+'})
printf("[update_churn_model_data.py]: Salesforce ... No of Accounts: %s\n",len(account_df))
			
health_category = {}
health_category[None] = 0 
health_category['Broker Challenges'] = 1
health_category['Cancellation Notification'] = 2
health_category['Champion Turnover'] = 3
health_category['Customer Engagement'] = 4 
health_category['HCR Only'] = 5 
health_category['Incomplete Library'] = 6 
health_category['M&A'] = 7 
health_category['Major Cutbacks'] = 8
health_category['New Customer'] = 9 
health_category['Other'] = 10
health_category['Platform Issues'] = 11
health_category['Product Mismatch'] = 12
health_category['Production Issues'] = 13
health_category['ROI'] = 14
health_category['Usage/Adoption'] = 15
account_df['health_category_int'] = [health_category[x] for x in account_df.Health_Category__c]

industry = {}
industry[None] = ''
industry['21'] = 'Mining, Quarrying, and Oil and Gas Extraction'
industry['22'] = 'Utilities'
industry['23'] = 'Construction'
industry['31'] = 'Manufacturing'
industry['32'] = 'Manufacturing'
industry['33'] = 'Manufacturing'
industry['42'] = 'Wholesale Trade'
industry['44'] = 'Retail Trade'
industry['45'] = 'Retail Trade'
industry['48'] = 'Transportation and Warehousing'
industry['49'] = 'Transportation and Warehousing'
industry['51'] = 'Information'
industry['52'] = 'Finance and Insurance'
industry['53'] = 'Real Estate and Rental and Leasing'
industry['54'] = 'Professional, Scientific, and Technical Services'
industry['55'] = 'Management of Companies and Enterprises'
industry['56'] = 'Administrative and Support and Waste Management and Remediation Services'
industry['61'] = 'Educational Services'
industry['62'] = 'Health Care and Social Assistance'
industry['71'] = 'Arts, Entertainment, and Recreation'
industry['72'] = 'Accommodation and Food Services'
industry['81'] = 'Other Services'
industry['92'] = 'Public Administration'
for i in range(0,len(account_df)):
	try:
		account_df.loc[i,'industry'] = industry[account_df.ix[i]['NaicsCode'][0:2]]
	except:
		account_df.loc[i,'industry'] = ''

industry_int = {}
industry_int[''] = 0
industry_int['Mining, Quarrying, and Oil and Gas Extraction'] = 1
industry_int['Utilities'] = 2 
industry_int['Construction'] = 3
industry_int['Manufacturing'] = 4 
industry_int['Wholesale Trade'] = 5 
industry_int['Retail Trade'] = 6
industry_int['Transportation and Warehousing'] = 7
industry_int['Information'] = 8 
industry_int['Finance and Insurance'] = 9
industry_int['Real Estate and Rental and Leasing'] = 10
industry_int['Professional, Scientific, and Technical Services'] = 11
industry_int['Management of Companies and Enterprises'] = 12
industry_int['Administrative and Support and Waste Management and Remediation Services'] = 13
industry_int['Educational Services'] = 14
industry_int['Health Care and Social Assistance'] = 15
industry_int['Arts, Entertainment, and Recreation'] = 16
industry_int['Accommodation and Food Services'] = 17
industry_int['Other Services'] = 18
industry_int['Public Administration'] = 19
for i in range(0,len(account_df)):
	try:
		account_df.loc[i,'industry_int'] = industry_int[account_df.ix[i]['industry']]
	except:
		account_df.loc[i,'industry_int'] = 0

####################
# Output results
####################
printf("[update_churn_model_data.py]: Salesforce ... Output Results ... %.2f sec\n",time.time()-start)
execfile('churn_header.py')

op_df.to_csv('./output/op_df.csv',encoding='utf-8')
op_initial_upsell_df.to_csv('./output/op_initial_upsell_df.csv',encoding='utf-8')
op_12month_df.to_csv('./output/op_12month_df.csv',encoding='utf-8')

sf_product_header = ['Branding2__c',
'TotalPrice',
'UnitPrice',
'Product2Id',
'Video_Category__c',
'AccountId',
'sf_account_name',
'sf_op_name',
'OpportunityType',
'ChargeType__c',
'Product_ID__c',
'PricebookEntry',
'Quantity',
'OpportunityId',
'LineItemId',
'MSA_Effective_Date__c',
'Close_Date__c',
'Churn_Date__c',
'StageName',
'year',
'order_number']

sf_product_complete_df.to_csv('./output/sf_product_complete_df.csv',encoding='utf-8',columns=sf_product_header)
sf_product_df.to_csv('./output/sf_product_df.csv',encoding='utf-8',columns=sf_product_header)
sf_product_won_df.to_csv('./output/sf_product_won_df.csv',encoding='utf-8',columns=sf_product_header)
sf_product_swap_df.to_csv('./output/sf_product_swap_df.csv',encoding='utf-8',columns=sf_product_header)
sf_product_Dinitial_df.to_csv('./output/sf_product_Dinitial_df.csv',encoding='utf-8',columns=sf_product_header)
sf_product_TBDinitial_df.to_csv('./output/sf_product_TBDinitial_df.csv',encoding='utf-8',columns=sf_product_header)
sf_product_NewTitle_initial_df.to_csv('./output/sf_product_NewTitle_initial_df.csv',encoding='utf-8',columns=sf_product_header)

#sf_product_won_12month_df.to_csv('./output/sf_product_won_12month_df.csv',encoding='utf-8')

########################################################
# Read in attask ID / SFDC OpportunityLineItemID lookup
########################################################

if (UPDATE_ATTASK_LINEITEM):
	library_completion_df = pd.read_csv("./input_data/AttaskProjects_20160106_v8.csv")
	library_completion_SMALL_df = library_completion_df[['projectID','FINAL_OpportunityID','FINAL_LineItemID','DE_New_Customer_Upsell_Renewal','DE_Product_Title', 'status']]
	lineitem_attaskID_lookup = createGenericLookup_DF(library_completion_df,'projectID','FINAL_LineItemID')
	opportunity_attaskID_lookup = createGenericLookup_DF(library_completion_df,'projectID','FINAL_OpportunityID')

	########################
	# Grab data from attask
	########################
	printf("[update_churn_model_data.py]: Workfront  ... Update LineItemID ... %.2f sec\n",time.time()-start)
	Nlineitem_missing = 0
	for i in range(0,len(library_completion_df)):
		printf("%s of %s ... %.2f secs\n",i+1,len(library_completion_df),time.time()-start)
		try:
			query = 'UPDATE attask.projects SET DE_Opportunity_ID = "%s" WHERE projectID = "%s" AND input_date_id IN (SELECT max(id) from attask.input_date where day = "Sun")' \
								% (opportunity_attaskID_lookup[library_completion_df.ix[i]['projectID']][0:15],library_completion_df.ix[i]['projectID'])
			cur.execute(query)
		except:
			printf('No OpportunityID for ... %s\n',library_completion_df.ix[i]['projectID'])
	
		try:	
			query = 'UPDATE attask.projects SET DE_Line_Item_ID = "%s" WHERE projectID = "%s" AND input_date_id IN (SELECT max(id) from attask.input_date where day = "Sun")' \
								% (lineitem_attaskID_lookup[library_completion_df.ix[i]['projectID']][0:15],library_completion_df.ix[i]['projectID'])
			cur.execute(query)
		except:
			printf('No LineItemID for ... %s\n',library_completion_df.ix[i]['projectID'])
			Nlineitem_missing =	Nlineitem_missing + 1  	

		con.commit()
	printf('Total LineItems Missing = %4s of %4s\n',Nlineitem_missing,len(library_completion_df))

########################
# Grab data from attask
########################
printf("[update_churn_model_data.py]: Workfront ... Query WF Sandbox DB ... ")
query = 'SELECT * FROM attask.projects WHERE input_date_id IN (SELECT max(id) from attask.input_date where day = "Sun") \
										AND upper(name) NOT LIKE "%UPDATE%" \
										AND upper(name) NOT LIKE "%ACCOUNT MANAGEMENT%" \
										AND upper(name) NOT LIKE "%STYLE GUIDE%" \
										AND upper(name) NOT LIKE "%REFRESH%"'

wf_project_df = pd.read_sql(query,con)

printf("%.2f sec\n",time.time()-start)

#sf_product_MERGE_df = pd.merge(sf_product_df,wf_project_df,how='left',left_on='LineItemId',right_on='DE_Line_Item_ID')
#sf_product_MERGE_df.to_csv('./output/sf_product_MERGE_df.csv',encoding='utf-8')
sf_product_won_MERGE_df = pd.merge(sf_product_won_df,wf_project_df,how='left',left_on='LineItemId',right_on='DE_Line_Item_ID')

wf_project_df.to_csv('./output/wf_project_df.csv')
sf_product_won_MERGE_df.to_csv('./output/sf_product_won_MERGE_df.csv',encoding='utf-8')

############################################
# Look at the completed videos
############################################

#### Initial / Upsell sales only

sf_product_WF_total_df = sf_product_won_MERGE_df.copy(deep=True) \
								[  pd.notnull(sf_product_won_MERGE_df.DE_Line_Item_ID)] \
								.reset_index(drop=True)

sf_act_WF_total_df = sf_product_WF_total_df[['AccountId','OpportunityId','LineItemId','Quantity']].groupby(['AccountId']) \
                        .agg({"OpportunityId":pd.Series.nunique,"LineItemId": pd.Series.nunique,"Quantity":"count"  }) \
                        .rename(columns = {'OpportunityId':'Nop_WF_Total','LineItemId':'NlineItem_WF_Total','Quantity':'Nvideo_WF_Total'}).reset_index()

sf_product_WF_complete_df = sf_product_won_MERGE_df.copy(deep=True) \
								[  (sf_product_won_MERGE_df.status == 'CPL')] \
								.reset_index(drop=True)

sf_act_complete_df = sf_product_WF_complete_df[['AccountId','OpportunityId','LineItemId','Quantity']].groupby(['AccountId']) \
                        .agg({"OpportunityId":pd.Series.nunique,"LineItemId": pd.Series.nunique,"Quantity":"count"  }) \
                        .rename(columns = {'OpportunityId':'Nop_Total_complete','LineItemId':'NlineItem_Total_complete','Quantity':'Nvideo_Total_complete'}).reset_index()

sf_act_complete_df = pd.merge(sf_act_complete_df,sf_act_WF_total_df,'right',left_on='AccountId',right_on='AccountId').fillna(0)

sf_act_complete_1mo_df = calc_day_details(sf_product_WF_complete_df,31,'1mo_complete',['Close_Date','actualCompletionDate'],False,sf_act_WF_total_df)
sf_act_complete_2mo_df = calc_day_details(sf_product_WF_complete_df,61,'2mo_complete',['Close_Date','actualCompletionDate'],False,sf_act_WF_total_df)
sf_act_complete_3mo_df = calc_day_details(sf_product_WF_complete_df,91,'3mo_complete',['Close_Date','actualCompletionDate'],False,sf_act_WF_total_df)
sf_act_complete_4mo_df = calc_day_details(sf_product_WF_complete_df,122,'4mo_complete',['Close_Date','actualCompletionDate'],False,sf_act_WF_total_df)
sf_act_complete_5mo_df = calc_day_details(sf_product_WF_complete_df,152,'5mo_complete',['Close_Date','actualCompletionDate'],False,sf_act_WF_total_df)
sf_act_complete_6mo_df = calc_day_details(sf_product_WF_complete_df,182,'6mo_complete',['Close_Date','actualCompletionDate'],False,sf_act_WF_total_df)
sf_act_complete_7mo_df = calc_day_details(sf_product_WF_complete_df,212,'7mo_complete',['Close_Date','actualCompletionDate'],False,sf_act_WF_total_df)
sf_act_complete_8mo_df = calc_day_details(sf_product_WF_complete_df,243,'8mo_complete',['Close_Date','actualCompletionDate'],False,sf_act_WF_total_df)
sf_act_complete_9mo_df = calc_day_details(sf_product_WF_complete_df,273,'9mo_complete',['Close_Date','actualCompletionDate'],False,sf_act_WF_total_df)
sf_act_complete_10mo_df = calc_day_details(sf_product_WF_complete_df,304,'10mo_complete',['Close_Date','actualCompletionDate'],False,sf_act_WF_total_df)
sf_act_complete_11mo_df = calc_day_details(sf_product_WF_complete_df,334,'11mo_complete',['Close_Date','actualCompletionDate'],False,sf_act_WF_total_df)
sf_act_complete_12mo_df = calc_day_details(sf_product_WF_complete_df,365,'12mo_complete',['Close_Date','actualCompletionDate'],False,sf_act_WF_total_df)
sf_act_complete_15mo_df = calc_day_details(sf_product_WF_complete_df,456,'15mo_complete',['Close_Date','actualCompletionDate'],False,sf_act_WF_total_df)
sf_act_complete_18mo_df = calc_day_details(sf_product_WF_complete_df,547,'18mo_complete',['Close_Date','actualCompletionDate'],False,sf_act_WF_total_df)
sf_act_complete_21mo_df = calc_day_details(sf_product_WF_complete_df,638,'21mo_complete',['Close_Date','actualCompletionDate'],False,sf_act_WF_total_df)

####################################
# Final join to sfdc account_df
####################################
account_df = pd.merge(account_df,sf_act_complete_1mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_complete_2mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_complete_3mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_complete_4mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_complete_5mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_complete_6mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_complete_7mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_complete_8mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_complete_9mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_complete_10mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_complete_11mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_complete_12mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_complete_15mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_complete_18mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_complete_21mo_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df = pd.merge(account_df,sf_act_complete_df,'left',left_on='Id',right_on='AccountId').drop('AccountId',1)
account_df['Nvideo_Total_diff'] = [x for x in (account_df['Nvideo_Total'] - account_df['Nvideo_Total_complete'])]

account_df = find_percentage(account_df,'Nvideo_Total_complete','Nvideo_Total','Nvideo_Total_complete_%')
account_df = find_percentage(account_df,'Nvideo_1mo_complete','Nvideo_1mo','Nvideo_1mo_complete_%')
account_df = find_percentage(account_df,'Nvideo_2mo_complete','Nvideo_2mo','Nvideo_2mo_complete_%')
account_df = find_percentage(account_df,'Nvideo_3mo_complete','Nvideo_3mo','Nvideo_3mo_complete_%')
account_df = find_percentage(account_df,'Nvideo_4mo_complete','Nvideo_4mo','Nvideo_4mo_complete_%')
account_df = find_percentage(account_df,'Nvideo_5mo_complete','Nvideo_5mo','Nvideo_5mo_complete_%')
account_df = find_percentage(account_df,'Nvideo_6mo_complete','Nvideo_6mo','Nvideo_6mo_complete_%')
account_df = find_percentage(account_df,'Nvideo_7mo_complete','Nvideo_7mo','Nvideo_7mo_complete_%')
account_df = find_percentage(account_df,'Nvideo_8mo_complete','Nvideo_8mo','Nvideo_8mo_complete_%')
account_df = find_percentage(account_df,'Nvideo_9mo_complete','Nvideo_9mo','Nvideo_9mo_complete_%')
account_df = find_percentage(account_df,'Nvideo_10mo_complete','Nvideo_10mo','Nvideo_10mo_complete_%')
account_df = find_percentage(account_df,'Nvideo_11mo_complete','Nvideo_11mo','Nvideo_11mo_complete_%')
account_df = find_percentage(account_df,'Nvideo_12mo_complete','Nvideo_12mo','Nvideo_12mo_complete_%')
account_df = find_percentage(account_df,'Nvideo_15mo_complete','Nvideo_15mo','Nvideo_15mo_complete_%')
account_df = find_percentage(account_df,'Nvideo_18mo_complete','Nvideo_18mo','Nvideo_18mo_complete_%')
account_df = find_percentage(account_df,'Nvideo_21mo_complete','Nvideo_21mo','Nvideo_21mo_complete_%')
printf("[update_churn_model_data.py]: Salesforce ... No of Accounts: %s\n",len(account_df))

###################################
# Begin grabbing g2 data
###################################

######################################
# Find historical published videos
######################################
#############################################
#
# Min/max time for all videos
#
#############################################
#CREATE TABLE sandbox_prod.TMP_VIDEO_HISTORY
#SELECT A.account_id,A.trackable_id as video_id,A.video_version_id,COUNT(A.trackable_id) as Nview,min(A.min_time) as min_time,max(A.max_time) as max_time,B.created_at as video_created_at,B.updated_at as video_updated_at,
#	C.published_date as video_version_published_date,C.created_at as video_version_created_at,C.updated_at as video_version_updated_at
#from benchmark_prod.TMP_REACH_ALL A
#LEFT JOIN guidespark2_prod.videos B ON A.trackable_id=B.id
#LEFT JOIN guidespark2_prod.video_versions C ON A.video_version_id=C.id
#GROUP BY A.account_id,A.trackable_id,A.video_version_id;
#
#######################
## Algorithm
##
## TMP_VIDEO_HISTORY
## 1) video_created_at ... earliest record for video_id (may not be published version)
##
## 2) Grab published date of all video versions (video_version_published_date)
##
##	A) If video_version_published_date > min_time, use min_time (7%) for that case
##	   If video_version_published_date < min_time,use video_version_published_date for that case
##
##   B) Find min_date of the dates chosen in A) ... this is the START DATE FOR THE VIDEO
##
## 3) If no version of video_id is published ... video is closed
## Grab highest video_version_id that is closed grab the updated_at date ... This is the date that the video_id was closed
##
## TODO .... unclear if video_version_updated_at is the proper date to use for this 
##
#CREATE TABLE sandbox_prod.TMP_VIDEO_CLOSED
#	SELECT video_id,id as video_version_id
# 	FROM guidespark2_prod.video_versions WHERE video_id IN
#		(SELECT A.video_id from
#			(SELECT video_id FROM guidespark2_prod.video_versions WHERE state = 'closed' GROUP BY video_id) A
#			LEFT JOIN
#			(SELECT video_id FROM guidespark2_prod.video_versions WHERE state = 'published' GROUP BY video_id) B
#				ON A.video_id=B.video_id WHERE B.video_id IS NULL
#			) AND state IN ('closed') ORDER BY video_id,id asc;
#
#
#CREATE TABLE sandbox_prod.TMP_COMPLETED_VIDEO_EVENT
#SELECT min(start_date) as cur_date,'g2_publish_date' as `event`,A.sfdc,A.name as account_name,account_id,video_id,B.title as video_title FROM (
#SELECT account_id,video_id,video_version_id,
#	IF(video_version_published_date > min_time,min_time,IF (video_version_published_date != '1900-01-01 00:00:00',video_version_published_date,min_time)) as start_date 
#	FROM sandbox_prod.TMP_VIDEO_VIEW_HISTORY) T 
#	LEFT JOIN guidespark2_prod.accounts A ON T.account_id=A.id
#	LEFT JOIN guidespark2_prod.videos B ON T.video_id=B.id
#	WHERE B.title NOT LIKE ('%1095%') 
#	GROUP BY account_id,video_id
#UNION
#SELECT TTTT.end_date as cur_date,'g2_close_date' as `event`,A.sfdc,A.name as account_name,TTTT.account_id,TTTT.video_id,C.title as video_title FROM 
#(
#	SELECT account_id,video_id,video_version_id,IF(MAX(video_version_updated_at) > MAX(max_time),MAX(video_version_updated_at),MAX(max_time)) as end_date FROM
#	(
#		SELECT TT.account_id,T.video_id,T.video_version_id,TT.Nview,TT.video_version_published_date,TT.video_version_updated_at,TT.max_time 
#			FROM sandbox_prod.TMP_VIDEO_CLOSED T
#			LEFT JOIN
#			sandbox_prod.TMP_VIDEO_VIEW_HISTORY TT ON T.video_id=TT.video_id AND T.video_version_id=TT.video_version_id
#		UNION
#		SELECT TT.account_id,TT.video_id,TT.video_version_id,TT.Nview,TT.video_version_published_date,TT.video_version_updated_at,TT.max_time 
#			FROM sandbox_prod.TMP_VIDEO_CLOSED T
#			RIGHT JOIN
#			sandbox_prod.TMP_VIDEO_VIEW_HISTORY TT ON T.video_id=TT.video_id AND T.video_version_id=TT.video_version_id) TTT 
#	GROUP BY video_id) TTTT
#	LEFT JOIN guidespark2_prod.accounts A ON TTTT.account_id=A.id
#	LEFT JOIN TMP_G2_VIDEOS PPP ON TTTT.account_id=PPP.account_id AND TTTT.video_id=PPP.video_id ### REMOVE ALL videos in account_contents
#	LEFT JOIN guidespark2_prod.videos C ON TTTT.video_id=C.id
#	WHERE PPP.account_id IS NULL AND PPP.video_id IS NULL AND TTTT.end_date IS NOT NULL 
#	AND C.title NOT LIKE ('%1095%')
#	ORDER BY video_id,cur_date; 

######################################
# Get video count progression from G2 
######################################

#query = "SELECT * FROM sandbox_prod.TMP_COMPLETED_VIDEO_EVENT"
#printf("[update_churn_model_data.py]: G2 Videos ... Query Time ... %.2f\n",time.time()-start) 
#g2_completed_videos_df = pd.read_sql(query,con)

######################################
# Find current published videos in G2
######################################

query = "SELECT C.sfdc,GROUP_CONCAT(C.name) as account_name,GROUP_CONCAT(T.account_id) as all_account_id,COUNT(video_id) as Nmicro,T.* FROM \
		(SELECT B.id as video_id,B.title,A.account_id,C.id as video_version,C.state, \
			C.created_at as video_version_created_at,C.attask_id,C.attask_project_id,C.attask_task_id,C.line_item_id,A.contentable_id, \
			A.contentable_type,A.created_at,A.updated_at,A.public,A.position,A.auto_play from guidespark2_prod.account_contents A \
			LEFT JOIN guidespark2_prod.videos B ON A.contentable_id=B.id \
			RIGHT JOIN (SELECT video_id,max(id) as id,created_at,state,attask_id,attask_project_id,attask_task_id,line_item_id \
				FROM guidespark2_prod.video_versions WHERE state = 'published' GROUP BY video_id) C on A.contentable_id=C.video_id \
			WHERE A.contentable_type IN  ('Video') \
				AND A.id IN ( \
				select MAX(id) as id from guidespark2_prod.account_contents where contentable_type IN  ('Video') GROUP BY account_id,contentable_id) \
		UNION \
		SELECT B.displayable_id as video_id,D.title,A.account_id,E.id as video_version,E.state, \
			E.created_at as video_version_created_at,E.attask_id,E.attask_project_id,E.attask_task_id,E.line_item_id,A.contentable_id, \
			A.contentable_type,A.created_at,A.updated_at,A.public,A.position,A.auto_play \
			FROM guidespark2_prod.account_contents A \
			LEFT JOIN guidespark2_prod.learning_path_nodes B ON A.contentable_id=B.learning_path_id \
			LEFT JOIN guidespark2_prod.learning_paths C ON A.contentable_id=C.id \
			LEFT JOIN guidespark2_prod.videos D on B.displayable_id=D.id \
			RIGHT JOIN (SELECT video_id,max(id) as id,created_at,state,attask_id,attask_project_id,attask_task_id,line_item_id \
					FROM guidespark2_prod.video_versions WHERE state = 'published' GROUP BY video_id) E on B.displayable_id=E.video_id \
			WHERE A.contentable_type IN ('LearningPath') and B.displayable_type IN ('Video') \
				AND C.state IN ('published') ) T \
		LEFT JOIN guidespark2_prod.accounts C ON T.account_id=C.id \
			WHERE C.id NOT IN (select id from sandbox_prod.TMP_TEMP_ACCOUNTS) \
				AND T.title NOT LIKE ('%1095%') \
			GROUP BY sfdc,video_id"

#query = "select C.sfdc,C.id as account_id,C.name as account_name,B.id as video_id,B.title,D.id as video_version,D.state, \
#			D.created_at as video_version_created_at,D.attask_id,D.attask_project_id,D.attask_task_id,D.line_item_id,A.contentable_id, \
#			A.contentable_type,A.created_at,A.updated_at,A.public,A.position,A.auto_play from guidespark2_prod.account_contents A \
#				LEFT JOIN guidespark2_prod.videos B ON A.contentable_id=B.id \
#				LEFT JOIN guidespark2_prod.accounts C ON A.account_id=C.id \
#				RIGHT JOIN (SELECT video_id,max(id) as id,created_at,state,attask_id,attask_project_id,attask_task_id,line_item_id \
#					FROM guidespark2_prod.video_versions WHERE state = 'published' GROUP BY video_id) D on A.contentable_id=D.video_id \
#			WHERE A.contentable_type IN  ('Video') \
#			AND A.id IN ( \
#			select MAX(id) as id from guidespark2_prod.account_contents where contentable_type IN  ('Video') GROUP BY account_id,contentable_id)"

printf("[update_churn_model_data.py]: G2 ... Query Time ... ") 
g2_videos_df = pd.read_sql(query,con)
for i in range(0,len(g2_videos_df)):
	try:
		g2_videos_df.loc[i,'sfdc'] = str.rstrip(str(g2_videos_df.ix[i]['sfdc']))
	except:
		g2_videos_df.loc[i,'sfdc'] = None

g2_output_df = pd.read_sql('SELECT sfdc,account_id,min_time,trackable_id,user_id,parent_id FROM benchmark_prod.TMP_REACH_ALL',con)
g2_accounts_df = pd.read_sql('SELECT distinct LEFT(sfdc,15) as sfdc FROM guidespark2_prod.accounts',con)

g2_output_df = pd.merge(g2_output_df,account_df[['Id','MSA_Effective_Date']],'left',left_on='sfdc',right_on='Id').drop('Id',1)

printf("%.2f sec\n",time.time() - start)

#####################################################
# Calculate static library completion
# 1) Use g2_videos_df
# 2) Groupby sfdc
# 3) Get video counts
#####################################################
g2_videos_NOTNULL_df = g2_videos_df[(g2_videos_df.sfdc != 'test') & (g2_videos_df.sfdc != '') & (pd.notnull(g2_videos_df.sfdc))].copy(deep=True).reset_index()

g2_act_video_count_df = g2_videos_NOTNULL_df[['sfdc','account_id','video_id']].groupby(['sfdc']) \
                        .agg({"account_id":pd.Series.nunique,"video_id":pd.Series.nunique,"sfdc": "count"  }) \
                        .rename(columns = {'account_id':'g2_Nmicro_cur','video_id':'g2_Nvideo_cur_distinct','sfdc':'g2_Nvideo_cur'}).reset_index()

g2_act_video_count_df['sfdc_18'] = [x for x in g2_act_video_count_df.sfdc] ### Change AccountId to 15 characters
g2_act_video_count_df['sfdc'] = [x[0:15] for x in g2_act_video_count_df.sfdc] ### Change AccountId to 15 characters

#g2_act_micro_df = g2_videos_NOTNULL_df.groupby(['sfdc']) \
#					.apply(lambda x: list(set(map(str,map(int,x.account_id) ))) ).reset_index().rename(columns={0:'g2_micro_list'})  
#g2_act_micro_df['sfdc_18'] = [x for x in g2_act_micro_df.sfdc] ### Change AccountId to 15 characters
#g2_act_micro_df['sfdc'] = [x[0:15] for x in g2_act_micro_df.sfdc] ### Change AccountId to 15 characters

g2_act_video_count_df = pd.merge(account_df[['Id']],g2_act_video_count_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1).drop('sfdc_18',1).fillna(0)
account_df = pd.merge(account_df,g2_act_video_count_df,'left',left_on='Id',right_on='Id')
account_df['g2_library_completion_%_cur'] = [x for x in (account_df['g2_Nvideo_cur'] / account_df['Nvideo_cur']) ]
#account_df = pd.merge(account_df,g2_act_micro_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1).drop('sfdc_18',1)

printf("[update_churn_model_data.py]: Salesforce ... No of Accounts: %s\n",len(account_df))

#####################################################
#
# Calculate library completion from g2
#
# 1) Find all views in year 1 of each subscription
# 2) Find unique videos within those views
# 3) Unique Videos = Nvideo complete in year 1
#####################################################

printf("[update_churn_model_data.py]: G2 ... Merge Stats ... ")
###### All Time G2 stats ######
g2_act_complete_stats_df = g2_output_df[['sfdc','account_id','trackable_id','user_id','parent_id']].groupby(['sfdc']) \
						.agg({"account_id":pd.Series.nunique,"trackable_id":pd.Series.nunique,"user_id":pd.Series.nunique,"parent_id": "count"  }) \
						.rename(columns = {'account_id':'g2_Nmicro_Total','trackable_id':'g2_Nvideo_Total','user_id':'g2_Nuser_Total','parent_id':'g2_Nview_Total'}).reset_index()

# Add all accounts
g2_act_complete_stats_df = pd.merge(g2_act_complete_stats_df,g2_accounts_df[['sfdc']],'right',left_on='sfdc',right_on='sfdc').fillna(0)

g2_act_1mo_stats_df = calc_day_details_G2(g2_output_df,31,'1mo',['min_time'],True,g2_act_complete_stats_df)
g2_act_2mo_stats_df = calc_day_details_G2(g2_output_df,61,'2mo',['min_time'],True,g2_act_complete_stats_df)
g2_act_3mo_stats_df = calc_day_details_G2(g2_output_df,91,'3mo',['min_time'],True,g2_act_complete_stats_df)
g2_act_4mo_stats_df = calc_day_details_G2(g2_output_df,122,'4mo',['min_time'],True,g2_act_complete_stats_df)
g2_act_5mo_stats_df = calc_day_details_G2(g2_output_df,152,'5mo',['min_time'],True,g2_act_complete_stats_df)
g2_act_6mo_stats_df = calc_day_details_G2(g2_output_df,182,'6mo',['min_time'],True,g2_act_complete_stats_df)
g2_act_7mo_stats_df = calc_day_details_G2(g2_output_df,212,'7mo',['min_time'],True,g2_act_complete_stats_df)
g2_act_8mo_stats_df = calc_day_details_G2(g2_output_df,243,'8mo',['min_time'],True,g2_act_complete_stats_df)
g2_act_9mo_stats_df = calc_day_details_G2(g2_output_df,273,'9mo',['min_time'],True,g2_act_complete_stats_df)
g2_act_10mo_stats_df = calc_day_details_G2(g2_output_df,304,'10mo',['min_time'],True,g2_act_complete_stats_df)
g2_act_11mo_stats_df = calc_day_details_G2(g2_output_df,334,'11mo',['min_time'],True,g2_act_complete_stats_df)
g2_act_12mo_stats_df = calc_day_details_G2(g2_output_df,365,'12mo',['min_time'],True,g2_act_complete_stats_df)
g2_act_15mo_stats_df = calc_day_details_G2(g2_output_df,456,'15mo',['min_time'],True,g2_act_complete_stats_df)
g2_act_18mo_stats_df = calc_day_details_G2(g2_output_df,547,'18mo',['min_time'],True,g2_act_complete_stats_df)
g2_act_21mo_stats_df = calc_day_details_G2(g2_output_df,638,'21mo',['min_time'],True,g2_act_complete_stats_df)

##################
# Combine Results
##################
account_df = pd.merge(account_df,g2_act_1mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_act_2mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_act_3mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_act_4mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_act_5mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_act_6mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_act_7mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_act_8mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_act_9mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_act_10mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_act_11mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_act_12mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_act_15mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_act_18mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_act_21mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_act_complete_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df['Nvideo_g2_SFDC_12mo_diff'] = [x for x in (account_df['Nvideo_12mo'] - account_df['g2_Nvideo_12mo']) ]
account_df['Nvideo_g2_SFDC_12mo_%'] = [x for x in (account_df['g2_Nvideo_12mo'] / account_df['Nvideo_12mo']) ]

printf("%.2f sec\n",time.time() - start)

##################################
# Add Admin Usage since 1/1/2014
##################################
query = 'SELECT LEFT(A.sfdc,15) as sfdc,COUNT(distinct A.id) as g2_admin_micro_Total, \
		COUNT(distinct B.user_id) as g2_admin_user_Total,COUNT(distinct B.action) as g2_admin_action_Total, \
		COUNT(B.user_id) as g2_admin_report_Total \
		FROM guidespark2_prod.accounts A \
		LEFT JOIN sandbox_prod.TMP_ADMIN_ACCESS B ON A.id=B.account_id \
		GROUP BY LEFT(A.sfdc,15)'
g2_admin_usage_df = pd.read_sql(query,con)

query = 'SELECT * FROM sandbox_prod.TMP_ADMIN_ACCESS'
g2_admin_usage_all_df = pd.read_sql(query,con)
g2_admin_all_df = pd.merge(g2_admin_usage_all_df,account_df[['Id','MSA_Effective_Date']],'left',left_on='sfdc',right_on='Id').drop('Id',1)

g2_admin_1mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,31,'1mo',['created_at'],True,g2_act_complete_stats_df)
g2_admin_2mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,61,'2mo',['created_at'],True,g2_act_complete_stats_df)
g2_admin_3mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,91,'3mo',['created_at'],True,g2_act_complete_stats_df)
g2_admin_4mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,122,'4mo',['created_at'],True,g2_act_complete_stats_df)
g2_admin_5mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,152,'5mo',['created_at'],True,g2_act_complete_stats_df)
g2_admin_6mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,182,'6mo',['created_at'],True,g2_act_complete_stats_df)
g2_admin_7mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,212,'7mo',['created_at'],True,g2_act_complete_stats_df)
g2_admin_8mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,243,'8mo',['created_at'],True,g2_act_complete_stats_df)
g2_admin_9mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,273,'9mo',['created_at'],True,g2_act_complete_stats_df)
g2_admin_10mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,304,'10mo',['created_at'],True,g2_act_complete_stats_df)
g2_admin_11mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,334,'11mo',['created_at'],True,g2_act_complete_stats_df)
g2_admin_12mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,365,'12mo',['created_at'],True,g2_act_complete_stats_df)
g2_admin_15mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,456,'15mo',['created_at'],True,g2_act_complete_stats_df)
g2_admin_18mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,547,'18mo',['created_at'],True,g2_act_complete_stats_df)
g2_admin_21mo_stats_df = calc_day_details_G2admin(g2_admin_all_df,638,'21mo',['created_at'],True,g2_act_complete_stats_df)

##################
# Combine Results
##################
account_df = pd.merge(account_df,g2_admin_1mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_admin_2mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_admin_3mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_admin_4mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_admin_5mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_admin_6mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_admin_7mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_admin_8mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_admin_9mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_admin_10mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_admin_11mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_admin_12mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_admin_15mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_admin_18mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_admin_21mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_admin_usage_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)

##################################
# Edits info since 7/1/2014
##################################
query = "SELECT * FROM edits_prod.TMP_VD1_EDITS" 
g2_vd1_edits_df = pd.read_sql(query,con)
g2_vd1_edits_df = pd.merge(g2_vd1_edits_df,account_df[['Id','MSA_Effective_Date']],'left',left_on='sfdc',right_on='Id').drop('Id',1)

query = "SELECT * FROM edits_prod.TMP_ALL_CUSTOMER_TOUCH_EDITS" 
g2_all_edits_df = pd.read_sql(query,con)
g2_all_edits_df = pd.merge(g2_all_edits_df,account_df[['Id','MSA_Effective_Date']],'left',left_on='sfdc',right_on='Id').drop('Id',1)

###### Complete Stats #######
g2_vd1_edit_complete_df = g2_vd1_edits_df[['sfdc','account_id','video_id','video_version_id','Ncnt_preview','Ncnt_qc']].groupby(['sfdc']) \
						.agg({"account_id":pd.Series.nunique,"video_id":pd.Series.nunique,"video_version_id":pd.Series.nunique, \
						"Ncnt_preview":"mean","Ncnt_qc": "mean"  }) \
						.rename(columns = {'account_id':'g2_act_vd1','video_id':'g2_Nvideo_vd1','video_version_id':'g2_Nversion_vd1', \
											'Ncnt_preview':'g2_avg_edits_preview_vd1','Ncnt_qc':'g2_avg_edits_qc_vd1'}).reset_index()

g2_all_edit_complete_df = g2_all_edits_df[['sfdc','account_id','video_id','video_version_id','Ncnt_preview','Ncnt_qc']].groupby(['sfdc']) \
						.agg({"account_id":pd.Series.nunique,"video_id":pd.Series.nunique,"video_version_id":pd.Series.nunique, \
						"Ncnt_preview":"mean","Ncnt_qc": "mean"  }) \
						.rename(columns = {'account_id':'g2_act_all','video_id':'g2_Nvideo_all','video_version_id':'g2_Nversion_all', \
											'Ncnt_preview':'g2_avg_edits_preview_all','Ncnt_qc':'g2_avg_edits_qc_all'}).reset_index()

g2_vd1_edit_1mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,31,'1mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
g2_vd1_edit_2mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,61,'2mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
g2_vd1_edit_3mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,91,'3mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
g2_vd1_edit_4mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,122,'4mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
g2_vd1_edit_5mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,152,'5mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
g2_vd1_edit_6mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,182,'6mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
g2_vd1_edit_7mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,212,'7mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
g2_vd1_edit_8mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,243,'8mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
g2_vd1_edit_9mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,273,'9mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
g2_vd1_edit_10mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,304,'10mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
g2_vd1_edit_11mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,334,'11mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
g2_vd1_edit_12mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,365,'12mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
g2_vd1_edit_15mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,456,'15mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
g2_vd1_edit_18mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,547,'18mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)
g2_vd1_edit_21mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_df,638,'21mo_vd1',['video_created_at'],True,g2_vd1_edit_complete_df)

g2_all_edit_1mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,31,'1mo_all',['video_created_at'],True,g2_all_edit_complete_df)
g2_all_edit_2mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,61,'2mo_all',['video_created_at'],True,g2_all_edit_complete_df)
g2_all_edit_3mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,91,'3mo_all',['video_created_at'],True,g2_all_edit_complete_df)
g2_all_edit_4mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,122,'4mo_all',['video_created_at'],True,g2_all_edit_complete_df)
g2_all_edit_5mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,152,'5mo_all',['video_created_at'],True,g2_all_edit_complete_df)
g2_all_edit_6mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,182,'6mo_all',['video_created_at'],True,g2_all_edit_complete_df)
g2_all_edit_7mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,212,'7mo_all',['video_created_at'],True,g2_all_edit_complete_df)
g2_all_edit_8mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,243,'8mo_all',['video_created_at'],True,g2_all_edit_complete_df)
g2_all_edit_9mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,273,'9mo_all',['video_created_at'],True,g2_all_edit_complete_df)
g2_all_edit_10mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,304,'10mo_all',['video_created_at'],True,g2_all_edit_complete_df)
g2_all_edit_11mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,334,'11mo_all',['video_created_at'],True,g2_all_edit_complete_df)
g2_all_edit_12mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,365,'12mo_all',['video_created_at'],True,g2_all_edit_complete_df)
g2_all_edit_15mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,456,'15mo_all',['video_created_at'],True,g2_all_edit_complete_df)
g2_all_edit_18mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,547,'18mo_all',['video_created_at'],True,g2_all_edit_complete_df)
g2_all_edit_21mo_stats_df = calc_day_details_G2edits(g2_all_edits_df,638,'21mo_all',['video_created_at'],True,g2_all_edit_complete_df)

##################
# Combine Results
##################
account_df = pd.merge(account_df,g2_vd1_edit_complete_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_1mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_2mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_3mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_4mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_5mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_6mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_7mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_8mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_9mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_10mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_11mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_12mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_15mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_18mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_21mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_complete_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_1mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_2mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_3mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_4mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_5mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_6mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_7mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_8mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_9mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_10mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_11mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_12mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_15mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_18mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_21mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)


###############################################
# Edits 1st video only ... info since 7/1/2014
###############################################
query = "SELECT * FROM edits_prod.TMP_VD1_EDITS_1stVideo" 
g2_vd1_edits_1st_df = pd.read_sql(query,con)
g2_vd1_edits_1st_df = pd.merge(g2_vd1_edits_1st_df,account_df[['Id','MSA_Effective_Date']],'left',left_on='sfdc',right_on='Id').drop('Id',1)

query = "SELECT * FROM edits_prod.TMP_ALL_CUSTOMER_TOUCH_EDITS_1stVideo" 
g2_all_edits_1st_df = pd.read_sql(query,con)
g2_all_edits_1st_df = pd.merge(g2_all_edits_1st_df,account_df[['Id','MSA_Effective_Date']],'left',left_on='sfdc',right_on='Id').drop('Id',1)

###### Complete Stats #######
g2_vd1_edit_1st_complete_df = g2_vd1_edits_1st_df[['sfdc','account_id','video_id','video_version_id','Ncnt_preview','Ncnt_qc']].groupby(['sfdc']) \
						.agg({"account_id":pd.Series.nunique,"video_id":pd.Series.nunique,"video_version_id":pd.Series.nunique, \
						"Ncnt_preview":"mean","Ncnt_qc": "mean"  }) \
						.rename(columns = {'account_id':'g2_act_1st_vd1','video_id':'g2_Nvideo_1st_vd1','video_version_id':'g2_Nversion_1st_vd1', \
											'Ncnt_preview':'g2_avg_edits_preview_1st_vd1','Ncnt_qc':'g2_avg_edits_qc_1st_vd1'}).reset_index()

g2_all_edit_1st_complete_df = g2_all_edits_1st_df[['sfdc','account_id','video_id','video_version_id','Ncnt_preview','Ncnt_qc']].groupby(['sfdc']) \
						.agg({"account_id":pd.Series.nunique,"video_id":pd.Series.nunique,"video_version_id":pd.Series.nunique, \
						"Ncnt_preview":"mean","Ncnt_qc": "mean"  }) \
						.rename(columns = {'account_id':'g2_act_1st_all','video_id':'g2_Nvideo_1st_all','video_version_id':'g2_Nversion_1st_all', \
											'Ncnt_preview':'g2_avg_edits_preview_1st_all','Ncnt_qc':'g2_avg_edits_qc_1st_all'}).reset_index()

#g2_vd1_edit_1st_1mo_stats_df  = calc_day_details_G2edits(g2_vd1_edits_1st_df,31,'1st_1mo_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
#g2_vd1_edit_1st_3mo_stats_df  = calc_day_details_G2edits(g2_vd1_edits_1st_df,91,'1st_3mo_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
#g2_vd1_edit_1st_6mo_stats_df  = calc_day_details_G2edits(g2_vd1_edits_1st_df,182,'1st_6mo_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
#g2_vd1_edit_1st_9mo_stats_df  = calc_day_details_G2edits(g2_vd1_edits_1st_df,273,'1st_9mo_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
#g2_vd1_edit_1st_12mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,365,'1st_12mo_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
#g2_all_edit_1st_1mo_stats_df  = calc_day_details_G2edits(g2_all_edits_1st_df,31,'1st_1mo_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
#g2_all_edit_1st_3mo_stats_df  = calc_day_details_G2edits(g2_all_edits_1st_df,91,'1st_3mo_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
#g2_all_edit_1st_6mo_stats_df  = calc_day_details_G2edits(g2_all_edits_1st_df,182,'1st_6mo_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
#g2_all_edit_1st_9mo_stats_df  = calc_day_details_G2edits(g2_all_edits_1st_df,273,'1st_9mo_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
#g2_all_edit_1st_12mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,365,'1st_12mo_all',['video_created_at'],True,g2_all_edit_1st_complete_df)

g2_vd1_edit_1st_1mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,31,'1mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
g2_vd1_edit_1st_2mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,61,'2mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
g2_vd1_edit_1st_3mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,91,'3mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
g2_vd1_edit_1st_4mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,122,'4mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
g2_vd1_edit_1st_5mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,152,'5mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
g2_vd1_edit_1st_6mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,182,'6mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
g2_vd1_edit_1st_7mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,212,'7mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
g2_vd1_edit_1st_8mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,243,'8mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
g2_vd1_edit_1st_9mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,273,'9mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
g2_vd1_edit_1st_10mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,304,'10mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
g2_vd1_edit_1st_11mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,334,'11mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
g2_vd1_edit_1st_12mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,365,'12mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
g2_vd1_edit_1st_15mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,456,'15mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
g2_vd1_edit_1st_18mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,547,'18mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)
g2_vd1_edit_1st_21mo_stats_df = calc_day_details_G2edits(g2_vd1_edits_1st_df,638,'21mo_1st_vd1',['video_created_at'],True,g2_vd1_edit_1st_complete_df)

g2_all_edit_1st_1mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,31,'1mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
g2_all_edit_1st_2mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,61,'2mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
g2_all_edit_1st_3mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,91,'3mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
g2_all_edit_1st_4mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,122,'4mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
g2_all_edit_1st_5mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,152,'5mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
g2_all_edit_1st_6mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,182,'6mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
g2_all_edit_1st_7mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,212,'7mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
g2_all_edit_1st_8mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,243,'8mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
g2_all_edit_1st_9mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,273,'9mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
g2_all_edit_1st_10mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,304,'10mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
g2_all_edit_1st_11mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,334,'11mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
g2_all_edit_1st_12mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,365,'12mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
g2_all_edit_1st_15mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,456,'15mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
g2_all_edit_1st_18mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,547,'18mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)
g2_all_edit_1st_21mo_stats_df = calc_day_details_G2edits(g2_all_edits_1st_df,638,'21mo_1st_all',['video_created_at'],True,g2_all_edit_1st_complete_df)

#g2_vd1_edit_1st_1mo_stats_df_NEW.to_csv('g2_1moN_vd1.csv')
#g2_vd1_edit_1st_3mo_stats_df_NEW.to_csv('g2_3moN_vd1.csv')
#g2_vd1_edit_1st_6mo_stats_df_NEW.to_csv('g2_6moN_vd1.csv')
#g2_vd1_edit_1st_9mo_stats_df_NEW.to_csv('g2_9moN_vd1.csv')
#g2_vd1_edit_1st_12mo_stats_df_NEW.to_csv('g2_12moN_vd1.csv')
#g2_all_edit_1st_1mo_stats_df_NEW.to_csv('g2_1moN_all.csv')
#g2_all_edit_1st_3mo_stats_df_NEW.to_csv('g2_3moN_all.csv')
#g2_all_edit_1st_6mo_stats_df_NEW.to_csv('g2_6moN_all.csv')
#g2_all_edit_1st_9mo_stats_df_NEW.to_csv('g2_9moN_all.csv')
#g2_all_edit_1st_12mo_stats_df_NEW.to_csv('g2_12moN_all.csv')

##################
# Combine Results
##################
account_df = pd.merge(account_df,g2_vd1_edit_1st_complete_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_1st_1mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_1st_2mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_1st_3mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_1st_4mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_1st_5mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_1st_6mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_1st_7mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_1st_8mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_1st_9mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_1st_10mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_1st_11mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_1st_12mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_1st_15mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_1st_18mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_vd1_edit_1st_21mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_1st_complete_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_1st_1mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_1st_2mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_1st_3mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_1st_4mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_1st_5mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_1st_6mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_1st_7mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_1st_8mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_1st_9mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_1st_10mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_1st_11mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_1st_12mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_1st_15mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_1st_18mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)
account_df = pd.merge(account_df,g2_all_edit_1st_21mo_stats_df,'left',left_on='Id',right_on='sfdc').drop('sfdc',1)

account_df.to_csv('./output/account_df.csv',encoding='utf-8',columns=header)


tmp_header = [
'Id',
'AccountId_18',
'Name',
'MSA_Effective_Date',
'churn_int',
'Total_Nmbr_Videos__c',
'Nopportunity_All',
'Nopportunity_Total',
'Nvideo_Total',
'Nopportunity_cur',
'NlineItem_cur',
'NlineItem_Initial',
'Nvideo_cur',
'g2_Nvideo_cur',
'g2_library_completion_%_cur']
library_completion_df = account_df[tmp_header].copy(deep=True)
library_completion_df = library_completion_df.rename(columns={'Nopportunity_cur':'sfdc_Nopportunity_cur','NlineItem_Initial':'sfdc_NlineItem_Initial','NlineItem_cur':'sfdc_NlineItem_cur','Nvideo_cur':'sfdc_Nvideo_cur' })
library_completion_df = library_completion_df.rename(columns={'Nopportunity_All':'sfdc_Nopportunity_All'})
library_completion_df = library_completion_df.rename(columns={'Nopportunity_Total':'sfdc_Nopportunity_NewUpsell','Nvideo_Total':'sfdc_Nvideo_NewUpsell'})
library_completion_df = library_completion_df.rename(columns={'Total_Nmbr_Videos__c':'Cambria_sfdc_Nvideo'})

library_completion_header = [
'AccountId_18',
'Name',
'MSA_Effective_Date',
'churn_int',
'sfdc_NlineItem_Initial',
'sfdc_Nopportunity_All',
'sfdc_Nopportunity_NewUpsell',
'sfdc_Nopportunity_cur',
'Cambria_sfdc_Nvideo', 
'sfdc_Nvideo_NewUpsell', 
'sfdc_Nvideo_cur', 
'g2_Nvideo_cur',
'g2_library_completion_%_cur']
library_completion_df.to_csv('./output/library_completion_' + cur_datetime.strftime('%Y%m%d') +'.csv',encoding='utf-8',columns=library_completion_header)

