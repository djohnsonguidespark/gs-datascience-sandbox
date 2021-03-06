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

cur_datetime = datetime.now()
execfile('/home/analytics/analytics_sandbox/python_libs/stuff.py')

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',400)

con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

OUTPUT_ALL_PROJECTS = False
CREATE_NOTES_TABLE = False

DBNAME = "attask"
PROJECTtable = "projects"
TASKtable = "tasks"

########## SIMPLE SALESFORCE ##############
sf = Salesforce(username='djohnson@guidespark.com', password=pwd,security_token=token)

############################################
# Query Opportunity/Contact Database database
############################################
printf("[vd1_final_completion_date.py]: Salesforce ... Query Opportunity object\n")
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

op_initial_customer_df = op_df[( (op_df.Type == 'Initial Sale') | \
		   		  			 (op_df.Type == 'Initial Sale - Channel')) & \
		   		  			 (op_df.Name.str.contains('Year 2') == False) & \
		   		  			 (op_df.Name.str.contains('Year 3') == False) & \
		   		  			 (op_df.Name.str.contains('Year 4') == False) & \
		   		  			 (op_df.Name.str.contains('Year 5') == False) & \
		   		  			 (op_df.Name.str.contains('Year 6') == False) & \
		   		  			 (op_df.Close_Date__c > '2015-01-01') ]

op_upsell_customer_df = op_df[( (op_df.Type == 'Upsell/Cross-sell - AM') | \
							 (op_df.Type == 'Upsell/Cross-sell - Channel') | \
							 (op_df.Type == 'Upsell/Cross-sell') ) & \
		   		  			 (op_df.Name.str.contains('Year 2') == False) & \
		   		  			 (op_df.Name.str.contains('Year 3') == False) & \
		   		  			 (op_df.Name.str.contains('Year 4') == False) & \
		   		  			 (op_df.Name.str.contains('Year 5') == False) & \
		   		  			 (op_df.Name.str.contains('Year 6') == False) & \
		   		  			 (op_df.Close_Date__c > '2015-01-01') ]

#op_new_df = op_df[(op_df.Type == 'Initial Sale') | \
#		   		  (op_df.Type == 'Initial Sale - Channel') | \
#				  (op_df.Type == 'Upsell/Cross-sell - AM') | \
#				  (op_df.Type == 'Upsell/Cross-sell - Channel') | \
#				  (op_df.Type == 'Upsell/Cross-sell')]

printf("[vd1_final_completion_date.py]: Salesforce ... Query Account object\n")
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
printf("[vd1_final_completion_date.py]: Account Query Time: %.3f seconds\n",time.time() - start_account)

sf_act_id = map((lambda item: item['Id'][0:15]),account_output['records'])
sf_act_name = map((lambda item: item['Name']),account_output['records'])
sf_act_Nvideo = map((lambda item: item['Total_Nmbr_Videos__c']),account_output['records'])
sf_act_arr = map((lambda item: item['Yearly_Client_ARR__c']),account_output['records'])
sf_act_CSMid = map((lambda item: item['Customer_Success_Manager1__c']),account_output['records'])
sf_act_status = map((lambda item: item['Account_Status__c']),account_output['records'])
sf_act_product_line = map((lambda item: item['Product_Line__c']),account_output['records'])
sf_act_BEE = map((lambda item: item['Benefits_Eligible_Employees__c']),account_output['records'])
sf_act_NAICS = map((lambda item: item['NaicsCode']),account_output['records'])

printf("[vd1_final_completion_date.py]: Salesforce ... Query OpportunityLineItem (i.e. Product) object\n")
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

for i in range(0,len(sf_product_titlenumber)):
	try:
		sf_product_titlenumber[i] = sf_product_titlenumber[i].lstrip('0') 
	except Exception as e:
		printf("i = %4d . :%20s: . Line %s: %s\n",i,sf_product_titlenumber[i],sys.exc_traceback.tb_lineno,e)

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
sf_product_df = pd.merge(sf_product_df,op_df[['Id','Close_Date__c','AccountId','Name','No_of_Videos__c']],'left', \
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

printf("[vd1_final_completion_date.py]: Product Query Time: %.2f seconds\n",time.time() - start_account)

op_product_df = pd.merge(op_df,sf_product_df,'left',left_on='Id',right_on='sf_OpportunityId')
op_initial_customer_product_df = pd.merge(op_initial_customer_df,sf_product_df,'left',left_on='Id',right_on='sf_OpportunityId')
op_upsell_customer_product_df = pd.merge(op_upsell_customer_df,sf_product_df,'left',left_on='Id',right_on='sf_OpportunityId')

op_df.to_csv('./output/all_new_customer_upsell.csv',encoding='utf-8')
op_initial_customer_product_df.to_csv('./output/2015_new_customer.csv',encoding='utf-8')
op_upsell_customer_product_df.to_csv('./output/2015_new_customer_upsell.csv',encoding='utf-8')

#######################################
# Combine sf_products
#######################################
sf_product_df = sf_product_df[((sf_product_df.sf_Type == 'Initial Sale') | \
		   		  (sf_product_df.sf_Type == 'Initial Sale - Channel') | \
				  (sf_product_df.sf_Type == 'Upsell/Cross-sell - AM') | \
				  (sf_product_df.sf_Type == 'Upsell/Cross-sell - Channel') | \
				  (sf_product_df.sf_Type == 'Upsell/Cross-sell')) & \
				  (sf_product_df.sf_Library != 'O') & \
				  (sf_product_df.Close_Date__c > '2015-01-01')]


op_initial_customer_product_df = op_initial_customer_product_df[(op_initial_customer_product_df.sf_Library != 'O') & \
				  (op_initial_customer_product_df.Close_Date__c_x > '2015-01-01')]
op_upsell_customer_product_df = op_upsell_customer_product_df[(op_upsell_customer_product_df.sf_Library != 'O') & \
				  (op_upsell_customer_product_df.Close_Date__c_x > '2015-01-01')]

#######################################
# Query Attask to get projects / tasks
#######################################
start_attask = time.time()
#query = "SELECT A.projectID,B.DE_Line_Item_ID,B.PROJECT_status,B.PROJECT_actualCompletionDate,A.taskNumber,A.name,A.numberOfChildren,A.status as TASK_status, \
#								CAST(A.actualCompletionDate as DATE) as VD1_actualCompletionDate \
#					FROM %s.%s A LEFT JOIN (SELECT projectID,status as project_status,CAST(actualCompletionDate as DATE) as PROJECT_actualCompletionDate, \
#											DE_Line_Item_ID FROM %s.%s WHERE input_date_id IN (SELECT max(id) FROM %s.input_date)) B ON A.projectID=B.projectID \
#					WHERE A.input_date_id IN (SELECT max(id) FROM %s.input_date) \
#					AND A.name = 'Customer Video Review #1' \
#					AND A.projectID IN ( \
#						SELECT projectID FROM %s.%s \
#							WHERE input_date_id IN (SELECT max(id) FROM %s.input_date) \
#							AND UPPER(name) NOT LIKE '%%UPDATE%%' \
#							AND UPPER(name) NOT LIKE '%%TIMELINE%%' \
#							AND UPPER(name) NOT LIKE '%%REFRESH%%' \
#							AND DE_Product_Group != 'Styl' \
#							AND DE_Line_Item_ID NOT IN ('nan','None') \
#							AND entryDate > '2015-01-01 00:00:00')" % (DBNAME,TASKtable,DBNAME,PROJECTtable,DBNAME,DBNAME,DBNAME,PROJECTtable,DBNAME)

query = "select projectID,name,status,DE_Line_Item_ID,entryDate,actualCompletionDate from %s.%s where input_date_id = 127 and entryDate > '2015-01-01 00:00:00'" % (DBNAME,PROJECTtable)

printf("[vd1_final_completion_date.py]: Attask Query Time: %.2f seconds\n",time.time() - start_attask)

WF_project_df = createDF_from_MYSQL_query(query)
WF_completed_df = WF_project_df[(WF_project_df.status == 'CPL')]

###############################
# Merge SFDC / WF results
###############################
op_initial_customer_product_df = pd.merge(op_initial_customer_product_df,WF_project_df,'left',left_on='LineItemId',right_on='DE_Line_Item_ID')
op_upsell_customer_product_df = pd.merge(op_upsell_customer_product_df,WF_project_df,'left',left_on='LineItemId',right_on='DE_Line_Item_ID')

op_initial_customer_product_df.to_csv('./output/op_initial_customer_product_df.csv',encoding='utf-8')
op_upsell_customer_product_df.to_csv('./output/op_upsell_customer_product_df.csv',encoding='utf-8')

###########################
# Write results to SFDC
###########################

#sfdc_write_variables = ['VD1_Completion_Date__c','Final_Completion_Date__c']
#
#if (len(sfdc_write_variables) > 2):
#	printf("Too many write variables\n")
#	sys.exit()
#if (sfdc_write_variables[0] != 'VD1_Completion_Date__c'):
#	printf("Incorrect write variables\n")
#	sys.exit()
#if (sfdc_write_variables[1] != 'Final_Completion_Date__c'):
#	printf("Incorrect write variables\n")
#	sys.exit()
#
#for i in range(0,len(completed_vd1_df)):
#	printf("%4i of %4i ... Line Item: %15s ... WF Project ID = %33s ... ",i,len(completed_vd1_df)-1,completed_vd1_df.ix[i]['DE_Line_Item_ID'],completed_vd1_df.ix[i]['projectID'])
#	try:
#		if (pd.isnull(completed_vd1_df.ix[i]['PROJECT_actualCompletionDate']) == True):
#			sf.OpportunityLineItem.update(completed_vd1_df.ix[i]['DE_Line_Item_ID'],{sfdc_write_variables[0]:completed_vd1_df.ix[i]['VD1_actualCompletionDate'].strftime('%Y-%m-%d')})
#		else:
#			sf.OpportunityLineItem.update(completed_vd1_df.ix[i]['DE_Line_Item_ID'],{sfdc_write_variables[0]:completed_vd1_df.ix[i]['VD1_actualCompletionDate'].strftime('%Y-%m-%d'), \
#																				 sfdc_write_variables[1]:completed_vd1_df.ix[i]['PROJECT_actualCompletionDate'].strftime('%Y-%m-%d')})
#		printf("SUCCESS\n")
#	except:
#		printf(" ******** FAILED ********\n")

	
