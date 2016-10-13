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

sys.path.insert(0,'/home/analytics/analytics_sandbox/python_libs');
sys.path.insert(0,'/home/analytics/analytics_sandbox/FY14/sales');
from common_libs import *
from create_mysql import *
from attask_libs import * 
from attask_api import StreamClient, ObjCode, AtTaskObject

# Logging
import log_libs as log
LOG = log.init_logging()

cur_datetime = datetime.now()
execfile('/home/analytics/analytics_sandbox/python_libs/stuff.py')

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',200)

con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

QUERY_WORKFRONT = True
DBNAME = "attask"
PROJECTtable = "projects"
TASKtable = "tasks"

start = time.time()

########## SIMPLE SALESFORCE ##############
sf = Salesforce(username='djohnson@guidespark.com', password=pwd,security_token=token)

############################################
# Query Opportunity/Contact Database database
############################################
LOG.info("Salesforce ... Query Opportunity object")
query = "SELECT Id,AccountId,OwnerId,Owner.Name,Name,Amount,Type,LeadSource,Close_Date__c,LastActivityDate,StageName,IsWon,No_of_Videos__c, \
				Account.Name,Account.Industry,Account.Industry_Text__c,Account.Sic,Account.All_US_SIC_Codes__c,Account.NaicsCode,Account.All_NAICS_Codes__c, \
				Account.Customer_Lifespan__c,Account.Yearly_Client_ARR__c,Account.Customer_Success_Manager1__c, \
				Primary_Contact__c,Product_Line__c,ARR__c, \
				Account.AnnualRevenue,Account.ARR_At_Risk__c,Account.Account_Health__c, \
				Account.Health_Category__c,Account.Health_Category_Reason__c, \
				Account.Account_Status__c,Account.NumberOfEmployees, \
				Account.Total_Employees__c,Benefits_Eligible_Employees__c FROM Opportunity WHERE IsWon = TRUE"
op_output = sf.query_all(query)
op_df = pd.DataFrame(op_output['records'])
sf_op_output = pd.DataFrame(op_output['records'])
sf_op_name = map((lambda item: item['Name']),op_output['records'])
sf_op_Nvideo = map((lambda item: item['No_of_Videos__c']),op_output['records'])
sf_op_arr = map((lambda item: item['ARR__c']),op_output['records'])
sf_op_is_won = map((lambda item: item['IsWon']),op_output['records'])
sf_op_close_date = map((lambda item: item['Close_Date__c']),op_output['records'])
sf_op_type = map((lambda item: item['Type']),op_output['records'])
sf_op_bookings = map((lambda item: item['Amount']),op_output['records'])
sf_op_product_line = map((lambda item: item['Product_Line__c']),op_output['records'])
sf_op_year_month = []

LOG.info("Salesforce ... Query Account object")
query = "SELECT Id,Name,Customer_Success_Manager1__c,Account_Status__c, \
				Industry,Industry_Text__c,Sic,All_US_SIC_Codes__c,NaicsCode,All_NAICS_Codes__c, \
				Customer_Lifespan__c,Yearly_Client_ARR__c, \
				Product_Line__c,Total_Nmbr_Videos__c, \
				AnnualRevenue,ARR_At_Risk__c,Account_Health__c, \
				Health_Category__c,Health_Category_Reason__c, \
				NumberOfEmployees, \
				Total_Employees__c,Benefits_Eligible_Employees__c FROM Account \
				WHERE Account_Status__c = 'Current Client' OR Account_Status__c = 'Channel Partner & Client'"

start_account = time.time()
account_output = sf.query_all(query)
account_df = pd.DataFrame(account_output['records'])
LOG.info("Account Query Time: {:.2f} seconds".format(time.time() - start_account) )

sf_act_id = map((lambda item: item['Id'][0:15]),account_output['records'])
sf_act_name = map((lambda item: item['Name']),account_output['records'])
sf_act_Nvideo = map((lambda item: item['Total_Nmbr_Videos__c']),account_output['records'])
sf_act_arr = map((lambda item: item['Yearly_Client_ARR__c']),account_output['records'])
sf_act_CSMid = map((lambda item: item['Customer_Success_Manager1__c']),account_output['records'])
sf_act_status = map((lambda item: item['Account_Status__c']),account_output['records'])
sf_act_product_line = map((lambda item: item['Product_Line__c']),account_output['records'])
sf_act_BEE = map((lambda item: item['Benefits_Eligible_Employees__c']),account_output['records'])
sf_act_NAICS = map((lambda item: item['NaicsCode']),account_output['records'])

LOG.info("Salesforce ... Query OpportunityLineItem (i.e. Product) object")
query = "SELECT Id,OpportunityID,Library__c,Title_Number__c,ListPrice,Discount,TotalPrice,\
					UnitPrice,PricebookEntryId,PricebookEntry.Product2ID,PricebookEntry.Name, \
					Quantity,ServiceDate,Product_ID__c,Branding2__c,Opportunity.Type \
					FROM OpportunityLineItem" ## WHERE Library__c != 'O'" #,Quantity, \

start_product = time.time()
product_output = sf.query_all(query)
#sf_product_df = pd.DataFrame(product_output['records'])
sf_product_sfdcid = map((lambda item: item['Id'][0:15]),product_output['records'])
sf_product_opid = map((lambda item: item['OpportunityId']),product_output['records'])
sf_product_id = map((lambda item: item['PricebookEntry']['Product2Id']),product_output['records'])
sf_product_id2 = map((lambda item: item['Product_ID__c']),product_output['records'])
sf_pricebookentry_id = map((lambda item: item['PricebookEntryId']),product_output['records'])
sf_product_name = map((lambda item: item['PricebookEntry']['Name']),product_output['records'])
sf_product_discount = map((lambda item: item['Discount']),product_output['records'])
sf_product_library = map((lambda item: item['Library__c']),product_output['records'])
sf_product_listprice = map((lambda item: item['ListPrice']),product_output['records'])
sf_product_totalprice = map((lambda item: item['TotalPrice']),product_output['records'])
sf_product_quantity = map((lambda item: item['Quantity']),product_output['records'])
sf_product_titlenumber = map((lambda item: item['Title_Number__c']),product_output['records'])
sf_product_branding = map((lambda item: item['Branding2__c']),product_output['records'])
sf_product_type = map((lambda item: item['Opportunity']['Type']),product_output['records'])

for iii in range(0,len(sf_product_titlenumber)):
	try:
		sf_product_titlenumber[iii] = sf_product_titlenumber[iii].lstrip('0') 
	except Exception as e:
		LOG.info("iii = {:>4} . :{:>20}: {}".format(iii,sf_product_titlenumber[iii],e))

product_dict = {}
#product_dict['Id'] = sf_product_sfdcid 
product_dict['LineItemId'] = sf_product_sfdcid
product_dict['sf_OpportunityId'] = sf_product_opid 
product_dict['sf_ProductId'] = sf_product_id 
product_dict['sf_ProductId2'] = sf_product_id2 
product_dict['sf_PricebookEntryId'] = sf_pricebookentry_id 
product_dict['sf_Name'] = sf_product_name
product_dict['sf_Discount'] = sf_product_discount 
product_dict['sf_Library'] = sf_product_library 
product_dict['sf_ListPrice'] = sf_product_listprice
product_dict['sf_TotalPrice'] = sf_product_totalprice 
product_dict['sf_Quantity'] = sf_product_quantity
product_dict['sf_TitleNumber'] = sf_product_titlenumber 
product_dict['sf_Branding'] = sf_product_branding 
product_dict['sf_Type'] = sf_product_type 
sf_product_df = pd.DataFrame(product_dict)

sf_product_df = sf_product_df.rename(columns = {'sf_Name':'sf_product_title'})
sf_product_df = pd.merge(sf_product_df,op_df[['Id','AccountId','Name','No_of_Videos__c']],'left', \
						right_on='Id',left_on='sf_OpportunityId')
sf_product_df = sf_product_df.rename(columns = {'Name':'sf_op_name'})
sf_product_df = pd.merge(sf_product_df,account_df[['Id','Name']],'left', \
						right_on='Id',left_on='AccountId')
sf_product_df = sf_product_df.rename(columns = {'Name':'sf_account_name'})
order_number = []
year = []
for i in range(0,len(sf_product_df)):
	try:
		year.append(sf_product_df['op_name'][i].split(' - ')[2])
	except:
		year.append(None)
	try:
		order_number.append(sf_product_df['op_name'][i].split(' - ')[1])
	except:
		order_number.append(None)
sf_product_df = sf_product_df.join(pd.DataFrame(year))
sf_product_df = sf_product_df.rename(columns = {0:'year'})
sf_product_df = sf_product_df.join(pd.DataFrame(order_number))
sf_product_df = sf_product_df.rename(columns = {0:'order_number'})

for i in range(0,len(sf_product_df)):
	if (sf_product_df['sf_Branding'][i] == 'L'):
		sf_product_df['sf_Branding'][i] = 'Lite'	
	if (sf_product_df['sf_Branding'][i] == 'P'):
		sf_product_df['sf_Branding'][i] = 'Premium'	
	if (sf_product_df['sf_Branding'][i] == 'Plus'):
		sf_product_df['sf_Branding'][i] = 'Premium Plus'	

LOG.info("Product Query Time: {:.2f} seconds".format(time.time() - start_account) )

op_product_df = pd.merge(op_df,sf_product_df,'left',left_on='Id',right_on='sf_OpportunityId')

#######################################
# Query Attask to get projects / tasks
#######################################
if (QUERY_WORKFRONT == True):

	#######################
	# Query Workfront
	#######################
	Nlimit = 2000
	#url = 'https://guidespark.attasksandbox.com/attask/api'
	url = 'https://guidespark.attask-ondemand.com/attask/api'
	
	if ('ondemand' in url):
	    LOG.info("NOTE: Running on ATTASK PRODUCTION DB")
	else:
	    LOG.info("NOTE: Running on ATTASK PRODUCTION DB")
	
	client = StreamClient(url)
	
	LOG.info('Logging into Workfront ...')
	client.login('djohnson@guidespark.com',pwd)
	LOG.info('Log in Complete')
	
	#######################
	# Query Projects 
	#######################
	LOG.info('Searching projects...')
	project_fld=[]
	project_fld.append('ID')
	project_fld.append('actualCompletionDate')
	project_fld.append('entryDate')
	project_fld.append('name')
	project_fld.append('DE:Line Item ID')
	project_fld.append('DE:Product Group')
	project_fld.append('status')
	
	projects=[]
	for i in range(0,20):
		query_success = False
		while query_success == False:
			try:
				projects = projects + client.search(ObjCode.PROJECT,{},project_fld,i*Nlimit,Nlimit)
				query_success = True
			except Exception as e:
				LOG.info('FAILURE ... {} ... {}'.format(sys.exc_traceback.tb_lineno,e))
				time.sleep(1)
	
	project_df = pd.DataFrame(projects)
	
	project_df = pd.merge(project_df,sf_product_df,how='left',left_on='DE:Line Item ID',right_on='LineItemId')
	
	#######################
	# Query Tasks 
	#######################
	task_fld = []
	task_fld.append('ID')
	task_fld.append('actualCompletionDate')
	task_fld.append('entryDate')
	task_fld.append('name')
	task_fld.append('taskNumber')
	task_fld.append('numberOfChildren')
	task_fld.append('projectID')
	task_fld.append('status')
	
	Nlimit = 200
	j=-1
	tasks = []
	for p in project_df.ID:
	
		j = j + 1
	
		if ((j % 100) == 99):
			LOG.info('TASK {:>7} of {:>7} Elements ... {:.3f} sec'.format(j+1,len(project_df),time.time()-start))
	
		for i in range(0,1):
			query_success = False
			Ntest = 0
			while query_success == False:
				try:
					tasks = tasks + client.search(ObjCode.TASK,{'projectID':p},task_fld,i*Nlimit,Nlimit)  ## USE SEARCH FOR MULTIPLE RECORDS
					query_success = True
				except:
					Ntest = Ntest + 1
					LOG.info('FAILURE ... Task: {:>33}'.format(p));
					if (Ntest >= 5):
						query_success = True
					else:
						time.sleep(1)
					
	
	task_df = pd.DataFrame(tasks)

	################################
	# Convert Project / Tasks dates
	################################
	project_df['name'] = [x.upper() for x in project_df["name"]]
	project_df['entryDate'] = pd.to_datetime(project_df["entryDate"])
	project_df['actualCompletionDate'] = [AttaskDate_to_datetime_NONE(x) for x in project_df["actualCompletionDate"]]
	project_df['actualCompletionDate'] = pd.to_datetime(project_df["actualCompletionDate"])
	project_df = project_df.rename(columns={'ID':'projectID','name':'PROJECT_name','actualCompletionDate':'PROJECT_actualCompletionDate','entryDate':'PROJECT_entryDate','status':'PROJECT_status'})
	project_df = project_df.rename(columns={'DE:Product Group':'DE_Product_Group','DE:Line Item ID':'DE_Line_Item_ID'})
	
	task_df['name'] = [x.upper() for x in task_df["name"]]
	task_df['entryDate'] = pd.to_datetime(task_df["entryDate"])
	task_df['actualCompletionDate'] = [AttaskDate_to_datetime_NONE(x) for x in task_df["actualCompletionDate"]]
	task_df['actualCompletionDate'] = pd.to_datetime(task_df["actualCompletionDate"])
	task_df = task_df.rename(columns={'ID':'taskID','name':'TASK_name','actualCompletionDate':'TASK_actualCompletionDate','entryDate':'TASK_entryDate','status':'TASK_status'})
	
else:

	project_df = pd.read_csv('./output/project.csv',index_col=[0])
	task_df = pd.read_csv('./output/task.csv',index_col=[0])
	project_df['PROJECT_actualCompletionDate'] = pd.to_datetime(project_df["PROJECT_actualCompletionDate"])
	task_df['TASK_actualCompletionDate'] = pd.to_datetime(task_df["TASK_actualCompletionDate"])

##########################################################################################################
# Summarize results 
#
# For debugging
#
# project_df.ix[int(project_df[project_df.projectID == '57c44fb200373461ad1d16f1f2e0b551'].index)]
# vd1_task_df.ix[int(vd1_task_df[vd1_task_df.projectID == '57c44fb200373461ad1d16f1f2e0b551'].index)] 
# completed_df.ix[int(completed_df[completed_df.projectID == '57c44fb200373461ad1d16f1f2e0b551'].index)] 
##########################################################################################################	
completed_project_df = project_df[(pd.notnull(project_df['PROJECT_actualCompletionDate']) == True) \
								& (pd.notnull(project_df['DE_Line_Item_ID']) == True) \
								& (project_df.PROJECT_name.str.contains('STYLE GUIDE') == False) \
								& (project_df.PROJECT_name.str.contains('TIMELINE') == False) \
								& (project_df['DE_Product_Group'] != 'Style Guide') \
								& (project_df['DE_Product_Group'] != 'SG') ].reset_index(drop=True)
	
## Only grab the LAST PROJECT_actualCompletionDate for any LineItemId's with multiple projects
completed_project_df = completed_project_df.sort(['PROJECT_actualCompletionDate','DE_Line_Item_ID']).reset_index(drop=True)
completed_project_df = completed_project_df.groupby('DE_Line_Item_ID').last().reset_index() 

vd1_task_df = task_df[(pd.notnull(task_df['TASK_actualCompletionDate']) == True) \
					& (task_df['TASK_status'] == 'CPL') \
					& ( (task_df['TASK_name'] == 'CUSTOMER VIDEO REVIEW #1') | (task_df['TASK_name'] == 'SEND VIDEO FOR CUSTOMER REVIEW') ) ].sort(['projectID','taskNumber']).reset_index(drop=True) 
	
vd1_task_df = pd.merge(vd1_task_df,project_df[['projectID','DE_Line_Item_ID']],'left',left_on='projectID',right_on='projectID')					
completed_vd1_df = vd1_task_df.groupby('projectID').first().reset_index()					
	
# Merge the PROJECT completion and the VD1 completion
# Remove since we are treating them separately
#completed_df = pd.merge(completed_project_df,completed_vd1_df,'left',left_on='projectID',right_on='projectID')

###########################
# Write results to SFDC
###########################
	
sfdc_write_variables = ['VD1_Completion_Date__c','Final_Completion_Date__c']
	
if (len(sfdc_write_variables) > 2):
	LOG.info("Too many write variables")
	sys.exit()
if (sfdc_write_variables[0] != 'VD1_Completion_Date__c'):
	LOG.info("Incorrect write variables")
	sys.exit()
if (sfdc_write_variables[1] != 'Final_Completion_Date__c'):
	LOG.info("Incorrect write variables")
	sys.exit()
	
for i in range(0,len(completed_project_df)):
	LOG.info("FINAL: {:>4} of {:>4} ... Line Item: {:>15} ... WF Project ID = {:>33} . {:.2f} sec".format(i,len(completed_project_df)-1,completed_project_df.ix[i]['DE_Line_Item_ID'],completed_project_df.ix[i]['projectID'],time.time()-start) )
	try:
		if (pd.notnull(completed_project_df.ix[i]['PROJECT_actualCompletionDate']) == True):
			sf.OpportunityLineItem.update(completed_project_df.ix[i]['DE_Line_Item_ID'],{sfdc_write_variables[1]:completed_project_df.ix[i]['PROJECT_actualCompletionDate'].strftime('%Y-%m-%d')})
		LOG.info("PROJECT SUCCESS")
	except Exception as e:
		LOG.info("****** PROJECT FAILED ****** . {:>4} . {}".format(i,e))

for i in range(0,len(completed_vd1_df)):
	LOG.info("VD1: {:>4} of {:>4} ... Line Item: {:>15} ... WF Project ID = {:>33} . {:.2f} sec".format(i,len(completed_vd1_df)-1,completed_vd1_df.ix[i]['DE_Line_Item_ID'],completed_vd1_df.ix[i]['projectID'],time.time()-start) )
	try:
		if (pd.notnull(completed_vd1_df.ix[i]['TASK_actualCompletionDate']) == True):
			sf.OpportunityLineItem.update(completed_vd1_df.ix[i]['DE_Line_Item_ID'],{sfdc_write_variables[0]:completed_vd1_df.ix[i]['TASK_actualCompletionDate'].strftime('%Y-%m-%d')})
		LOG.info("TASK SUCCESS")
	except Exception as e:
		LOG.info("****** TASK FAILED ****** . {:>4} . {}".format(i,e))

###########################
# Output raw data 
###########################
if (QUERY_WORKFRONT == True):
	project_df.to_csv('./output/project.csv',encoding='utf-8')
	task_df.to_csv('./output/task.csv',encoding='utf-8')


