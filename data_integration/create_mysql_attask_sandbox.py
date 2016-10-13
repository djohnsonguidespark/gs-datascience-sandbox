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
INSERT_DELETED_PROJECTS_TASKS = False
CREATE_NOTES_TABLE = False

DBNAME = "attask"
NOTEStable = "notes"
PROJECTtable = "projects"
TASKtable = "tasks"
DATEtable = "input_date"
USERtable = "attask_users"
PTOtable = "attask_pto"
HOURtable = "attask_hours"
HOUR_TYPEtable = "attask_hour_types"
SUMMARYtable = "GM_SUMMARY"

###################################
# Output all projects every Sunday
###################################
if (datetime.today().weekday() == 6):
	OUTPUT_ALL_PROJECTS = True
	INSERT_DELETED_PROJECTS_TASKS = True
	CREATE_NOTES_TABLE = True

create_mysql_db(con,DBNAME)

cur_date = {}
cur_date['curDay'] = cur_datetime.strftime('%a')
cur_date['inputDate'] = cur_datetime.date()
cur_date['startTime'] = cur_datetime
create_attask_DATE_table(con,DBNAME,DATEtable)

cur_date['endTime'] = None 
insert_into_attask_DATE_table(con,DBNAME,DATEtable,cur_date,time.time())

cur.execute('SELECT max(id) from %s.%s' % (DBNAME,DATEtable))
try:
	max_id = int(cur.fetchall()[0][0]) 
except:
	max_id = 1

####################################
# Create the cost matrix
####################################
cost_matrix = {}
cost_matrix['WRITE']  = 43.40
cost_matrix['DESIGN'] = 38.75
cost_matrix['QC']     = 39.10
cost_matrix['CPS']    = 34.15
cost_matrix['CSM']    = 40.00 

########## SIMPLE SALESFORCE ##############
sf = Salesforce(username='djohnson@guidespark.com', password=pwd,security_token=token)

############################################
# Query Opportunity/Contact Database database
############################################
printf("[task_compute_GM.py]: Salesforce ... Query Opportunity object\n")
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

printf("[task_compute_GM.py]: Salesforce ... Query Account object\n")
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
printf("[task_compute_GM.py]: Account Query Time: %s seconds\n",time.time() - start_account)

sf_act_id = map((lambda item: item['Id'][0:15]),account_output['records'])
sf_act_name = map((lambda item: item['Name']),account_output['records'])
sf_act_Nvideo = map((lambda item: item['Total_Nmbr_Videos__c']),account_output['records'])
sf_act_arr = map((lambda item: item['Yearly_Client_ARR__c']),account_output['records'])
sf_act_CSMid = map((lambda item: item['Customer_Success_Manager1__c']),account_output['records'])
sf_act_status = map((lambda item: item['Account_Status__c']),account_output['records'])
sf_act_product_line = map((lambda item: item['Product_Line__c']),account_output['records'])
sf_act_BEE = map((lambda item: item['Benefits_Eligible_Employees__c']),account_output['records'])
sf_act_NAICS = map((lambda item: item['NaicsCode']),account_output['records'])

printf("[task_compute_GM.py]: Salesforce ... Query OpportunityLineItem (i.e. Product) object\n")
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

printf("[task_compute_GM.py]: Product Query Time: %s seconds\n",time.time() - start_account)

op_product_df = pd.merge(op_df,sf_product_df,'left',left_on='Id',right_on='sf_OpportunityId')

########################################################
# Read in attask ID / SFDC OpportunityLineItemID lookup
########################################################
product_lookup_file = '../lookup/attask_sfdc_integration_20150115_v14.xlsx'
lineitem_attaskID_lookup = createGenericLookup(product_lookup_file,'project','ID','LineItemId')

###############
# Query Attask
###############
Nlimit = 2000
#url = 'https://guidespark.attasksandbox.com/attask/api'
url = 'https://guidespark.attask-ondemand.com/attask/api'

if ('ondemand' in url):
	printf("\n[attask_project_query.py]: **************************************\n")
	printf("[attask_project_query.py]: NOTE: Running on ATTASK PRODUCTION DB")
	printf("\n[attask_project_query.py]: **************************************\n\n")
else:
	printf("\n[attask_project_query.py]: **************************************\n")
	printf("[attask_project_query.py]: NOTE: Running on ATTASK SANDBOX DB")
	printf("\n[attask_project_query.py]: **************************************\n\n")

client = StreamClient(url)

print 'Logging in...'
client.login('djohnson@guidespark.com',pwd)
print 'Done'

############################################
# Call Attask Project Query
############################################

print 'Searching Users...'
user_fld = user_fields()
user=[]
for i in range(0,5):
	user = user + client.search(ObjCode.USER,{},user_fld,i*Nlimit,Nlimit)
user_df = pd.DataFrame(user)
user_df=user_df.rename(columns = {'ID':'userID2'})
user_df=user_df.rename(columns = {'name':'user_name'})

print 'Searching Roles ...'
role_fld = role_fields()
role=[]
for i in range(0,5):
	role = role + client.search(ObjCode.ROLE,{},role_fld,i*Nlimit,Nlimit)
role_df = pd.DataFrame(role)
role_df=role_df.rename(columns = {'ID':'roleID2'})
role_df=role_df.rename(columns = {'name':'role_name'})

print 'Searching reserved times ...'
reserved_times_fld = reserved_times_fields()
reserved_times=[]
for i in range(0,5):
	reserved_times = reserved_times + client.search(ObjCode.RESERVED,{},reserved_times_fld,i*Nlimit,Nlimit)
reserved_times_df = pd.DataFrame(reserved_times)
reserved_times_df=reserved_times_df.rename(columns = {'ID':'reserved_timesID'})
reserved_times_df=reserved_times_df.rename(columns = {'name':'reserved_times_name'})

reserved_times_df = pd.merge(reserved_times_df,user_df[['userID2','user_name','roleID']],how='left',left_on='userID',right_on='userID2').reset_index(drop=True)
reserved_times_df = pd.merge(reserved_times_df,role_df[['roleID2','role_name']],how='left',left_on='roleID',right_on='roleID2').reset_index(drop=True)
reserved_times_df = AttaskDate_to_datetime(reserved_times_df,'startDate')
reserved_times_df = AttaskDate_to_datetime(reserved_times_df,'endDate')
reserved_times_df = reserved_times_df.drop('userID2', 1)
reserved_times_df = reserved_times_df.drop('roleID2', 1)

###########################################
# 1) Expand records that ar more than 1 day
# 2) Remove all Saturday and Sunday
###########################################
reservedID = [];
curDate = [];
startDate = [];
endDate = [];
objCode = [];
taskID = [];
userID = [];
user_name = [];
roleID = [];
role_name = [];
pto_hours = [];
day_of_week = [];

for i in range(0,len(reserved_times_df)):
	day_count = (reserved_times_df.ix[i]['endDate'] - reserved_times_df.ix[i]['startDate']).days+1
	for n in range(0,day_count):
		reservedID.append(reserved_times_df.ix[i]['reserved_timesID'])
		curDate.append(reserved_times_df.ix[i]['startDate'] + timedelta(n))
		day_of_week.append((reserved_times_df.ix[i]['startDate'] + timedelta(n)).strftime('%A') )
		startDate.append(reserved_times_df.ix[i]['startDate'])
		endDate.append(reserved_times_df.ix[i]['endDate'])
		objCode.append(reserved_times_df.ix[i]['objCode'])
		taskID.append(reserved_times_df.ix[i]['taskID'])
		userID.append(reserved_times_df.ix[i]['userID'])
		user_name.append(reserved_times_df.ix[i]['user_name'])
		roleID.append(reserved_times_df.ix[i]['roleID'])
		role_name.append(reserved_times_df.ix[i]['role_name'])
		if (day_count > 1):
			pto_hours.append(8)
		else:
			cur_hours = float((reserved_times_df.endDate[i] - reserved_times_df.startDate[i]).seconds)/3600 
			if (cur_hours > 8):
				cur_hours = 8
			pto_hours.append(cur_hours)

timeoff = {}
timeoff['reserved_timesID'] = reservedID
timeoff['curDate'] = curDate 
timeoff['startDate'] = startDate
timeoff['endDate'] = endDate
timeoff['objCode'] = objCode
timeoff['taskID'] = taskID
timeoff['userID'] = userID
timeoff['user_name'] = user_name
timeoff['roleID'] = roleID
timeoff['role_name'] = role_name
timeoff['pto_hours'] = pto_hours
timeoff['day_of_week'] = day_of_week

timeoff_df = pd.DataFrame(timeoff)
## Remove Saturday and Sunday
timeoff_df = timeoff_df[timeoff_df.day_of_week != 'Saturday']
timeoff_df = timeoff_df[timeoff_df.day_of_week != 'Sunday'].reset_index(drop=True)

drop_mysql_table(con,DBNAME,PTOtable)
create_attask_TIMEOFF_table(con,DBNAME,PTOtable)
insert_into_attask_TIMEOFF_table(con,DBNAME,PTOtable,timeoff_df,time.time())

print 'Searching Timesheet...'
timesheet_fld = timesheet_fields()
timesheet=[]
for i in range(0,5):
	timesheet = timesheet + client.search(ObjCode.TIMESHEET,{},timesheet_fld,i*Nlimit,Nlimit)
timesheet_df = pd.DataFrame(timesheet)
timesheet_df=timesheet_df.rename(columns = {'ID':'timesheetID'})

timesheet_df = pd.merge(timesheet_df,user_df[['userID2','user_name']],how='left',left_on='userID',right_on='userID2').sort(['timesheetID'],ascending=[1]).reset_index(drop=True)

print 'Searching Hour Types...'
Nlimit = 200
hour_type_fld = hour_type_fields()
hour_type=[]
for i in range(0,1):
	hour_type = hour_type + client.search(ObjCode.HOUR_TYPE,{},hour_type_fld,i*Nlimit,Nlimit)

hour_type_df = pd.DataFrame(hour_type)
hour_type_df = hour_type_df.rename(columns={'ID':'hour_typeID'})
hour_type_df = hour_type_df.rename(columns={'name':'hour_type_name'})
printf('%s hour(s) found\n',len(hour_type_df))

print 'Searching hours...'
Nlimit = 2000
hour_fld = hour_fields()
hours=[]
for i in range(0,100):
	query_success = False
	while query_success == False:
		try:
			hours = hours + client.search(ObjCode.HOUR,{},hour_fld,i*Nlimit,Nlimit)
			query_success = True
		except:
			printf('FAILURE ... Task: HOURS (%4d)\n',i);
			time.sleep(1)

hours_df = pd.DataFrame(hours)
hours_df = hours_df.rename(columns={'ID':'hourID'})

printf('%s hour(s) found\n',len(hours_df))

hours_df = pd.merge(hours_df,hour_type_df[['hour_typeID','hour_type_name']],how='left',left_on='hourTypeID',right_on='hour_typeID').sort(['hourID'],ascending=[1]).reset_index(drop=True)
hours_df = pd.merge(hours_df,user_df[['userID2','user_name']],how='left',left_on='ownerID',right_on='userID2').sort(['hourID'],ascending=[1]).reset_index(drop=True)
hours_df = pd.merge(hours_df,role_df[['roleID2','role_name']],how='left',left_on='roleID',right_on='roleID2').sort(['hourID'],ascending=[1]).reset_index(drop=True)

drop_mysql_table(con,DBNAME,USERtable)
create_attask_USER_table(con,DBNAME,USERtable)
start = time.time()
insert_into_attask_USER_table(con,DBNAME,USERtable,user_df,start)

drop_mysql_table(con,DBNAME,HOURtable)
create_attask_HOUR_table(con,DBNAME,HOURtable)
start = time.time()
insert_into_attask_HOUR_table(con,DBNAME,HOURtable,hours_df,start)

drop_mysql_table(con,DBNAME,HOUR_TYPEtable)
create_attask_HOUR_TYPE_table(con,DBNAME,HOUR_TYPEtable)
start = time.time()
insert_into_attask_HOUR_TYPE_table(con,DBNAME,HOUR_TYPEtable,hour_type_df,start)

print 'Searching projects...'
project_fld = project_fields()
projects=[]
for i in range(0,20):
	query_success = False
	while query_success == False:
		try:
			projects = projects + client.search(ObjCode.PROJECT,{},project_fld,i*Nlimit,Nlimit)
			query_success = True
		except Exception as e:
			printf('FAILURE ... %s ... %s \n',sys.exc_traceback.tb_lineno,e)
			time.sleep(1)

project_df = pd.DataFrame(projects)

### Add LineItemIDs from lookup table
#for i in range(0,len(project_df)):
#	try:
#		project_df["DE:Line Item ID"][i] = lineitem_attaskID_lookup[project_df.ID[i]][0:15]
#	except:
#		printf('No LineItemID for ... %s\n',project_df.ID[i])

for i in range(0,len(project_df)):
	if (project_df['DE:Product Group'][i] == 'A8' or project_df['DE:Product Group'][i] == 'A2'):  
		project_df['DE:Product Group'][i] = 'A'  
	elif (project_df['DE:Product Group'][i] == 'C6'):  
		project_df['DE:Product Group'][i] = 'C'  
	elif (project_df['DE:Product Group'][i] == 'U'):  
		project_df['DE:Product Group'][i] = 'C'  

project_df = pd.merge(project_df,sf_product_df,how='left',left_on='DE:Line Item ID',right_on='LineItemId')

if (OUTPUT_ALL_PROJECTS == False):
	project_mysql_df = project_df[(project_df.status == 'CUR') | (project_df.status == 'ONH') | (project_df.status == 'PLN') ].reset_index()
else:
	project_mysql_df = project_df

create_attask_PROJECT_table(con,DBNAME,PROJECTtable)
start = time.time()
insert_into_attask_PROJECT_table(con,DBNAME,PROJECTtable,max_id,project_mysql_df,start)

#############################################
# Call Attask Tasks Query
# update in parts
#
#############################################

create_attask_TASK_table(con,DBNAME,TASKtable)
start = time.time()

task_fld = task_fields()
Nlimit = 200
j=-1
start = time.time() 
tasks = []
for p in project_mysql_df.ID:

	j = j + 1

	if ((j % 100) == 99):
		printf('%7d of %7d Elements ... %.3f sec\n',j+1,len(project_mysql_df),time.time()-start)

	for i in range(0,1):
		query_success = False
		while query_success == False:
			try:
				tasks = tasks + client.search(ObjCode.TASK,{'projectID':p},task_fld,i*Nlimit,Nlimit)  ## USE SEARCH FOR MULTIPLE RECORDS
				query_success = True
			except:
				printf('FAILURE ... Task: %33s\n',p);
				time.sleep(1)


	### update for this set of tasks
	if ((j % 100) == 99):
		if (len(tasks) > 0):
			task_df = pd.DataFrame(tasks)

			task_df = pd.merge(task_df,role_df[['roleID2','role_name']],how='left',left_on='roleID',right_on='roleID2')
			task_df = pd.merge(task_df,user_df[['userID2','user_name']],how='left',left_on='assignedToID',right_on='userID2')

			task_df = task_df.sort(['projectID','taskNumber'],ascending=[0,1]).reset_index()

			gm_work = []
			for i in range(0,len(task_df)):
				if (task_df.status[i].upper() == 'CNN'):	
					gm_work.append(0)
				else:
					if (task_df.actualWork[i] > 0.00):
						gm_work.append(task_df.actualWork[i]) # actualWork is LOGGED Work
					else:
						gm_work.append(task_df.work[i])  # work is PLANNED Work
			task_df = task_df.join(pd.DataFrame(gm_work))
			task_df = task_df.rename(columns = {0:'gm_work'})

			###################################
			# Compute the cost of each task
			###################################
			task_wage = [0] * len(task_df)
	
			for i in range(0,len(cost_matrix.keys() )):
				cur_wage = cost_matrix[cost_matrix.keys()[i]]
				Itmp = [k for k, x in enumerate(task_df.role_name) if cost_matrix.keys()[i].upper() in str(x).upper()]
		
				for k in range(0,len(Itmp)):
					task_wage[Itmp[k]] = cur_wage 	
					#print(tmp)

			task_df = task_df.join(pd.DataFrame(task_wage))
			task_df = task_df.rename(columns = {0:'task_wage'})
		
			task_cost = []
			for i in range(0,len(task_df)): 
				task_cost.append(gm_work[i]*task_wage[i])
			task_df = task_df.join(pd.DataFrame(task_cost))
			task_df = task_df.rename(columns = {0:'task_cost'})

			insert_into_attask_TASK_table(con,DBNAME,TASKtable,max_id,task_df,start)

			# Reset tasks
			tasks = []

##########################
# Update endTime
##########################
Nlimit=2000
query = "UPDATE %s.%s SET endTime = '%s' WHERE id = '%s'" % (DBNAME,DATEtable,datetime.now(),max_id)
printf("%s\n",query)
cur.execute(query)
con.commit() # necessary to finish statement

#################################
# INSERT DELETED PROJECTS / TASKS 
#################################

if (INSERT_DELETED_PROJECTS_TASKS):

	execfile('/home/analytics/analytics_sandbox/python_libs/deleted_query.py')

	deleted_projects_df = createDF_from_MYSQL('attaskDB_NEW','projects',query_projects)
	deleted_tasks_df = createDF_from_MYSQL('attaskDB_NEW','tasks',query_tasks)

	upload_deleted_into_attask_PROJECT_table(con,DBNAME,'projects',max_id,deleted_projects_df,start)
	upload_deleted_into_attask_TASK_table(con,DBNAME,'tasks',max_id,deleted_tasks_df,start)

##########################
# CREATE GM SUMMARY TABLE 
##########################

if (CREATE_NOTES_TABLE == True):
#	printf("[create_mysql_attask_sandbox.py] Create GM_SUMMARY table")
#	con = None
#	con = mdb.connect('localhost','root','','');
#	cur = con.cursor()
#	
#	drop_mysql_table(DBNAME,SUMMARYtable)
#	query = "CREATE TABLE %s.%s (SELECT T.*,(T.sf_TotalPrice/CAST(T.sf_Quantity as DECIMAL(10,2)) - T.task_cost) / (T.sf_TotalPrice/CAST(T.sf_Quantity as DECIMAL(10,2))) as gm \
#				FROM ( \
#					select A.projectID,B.project_name,B.status,B.actualCompletionDate, \
#					B.NewCustomer_Upsell_Renewal,B.LineItemID,B.sf_library,B.sf_titlenumber, \
#					B.sf_product_line,B.sf_product_title,B.sf_product_name, \
#					SUM(A.work) as work,SUM(A.actualWork) as actualWork,SUM(A.gm_work) as gm_work,SUM(task_cost) as task_cost, \
#					B.sf_ListPrice,B.sf_TotalPrice,B.sf_Quantity \
#					FROM %s.tasks A \
#					LEFT JOIN \
#					%s.projects B \
#					ON A.projectID=B.projectID \
#					WHERE A.numberOfChildren = 0 GROUP BY A.projectID,B.project_name) T)" % (DBNAME,SUMMARYtable,DBNAME,DBNAME) 
#	query = "CREATE TABLE attask.GM_SUMMARY 
#		SELECT T.*,(T.sf_TotalPrice/CAST(T.sf_Quantity as DECIMAL(10,2)) - T.task_cost) / (T.sf_TotalPrice/CAST(T.sf_Quantity as DECIMAL(10,2))) as gm 
#               FROM ( 
#                   select A.projectID,B.name,B.entryDate,B.status,B.actualCompletionDate, 
#                   B.DE_New_Customer_Upsell_Renewal,B.DE_Line_Item_ID,
#                   B.DE_Product_Title,B.DE_Product_Line, B.DE_Product_Code,B.DE_Product_Group, 
#                   SUM(A.work) as work,SUM(A.actualWork) as actualWork,SUM(A.gm_work) as gm_work,SUM(task_cost) as task_cost, 
#                   B.sf_ListPrice,B.sf_TotalPrice,B.sf_Quantity 
#                   FROM (select * from attask.tasks where input_date_id = 87) A 
#                   LEFT JOIN (select * from attask.projects where input_date_id = 87) B 
#                   ON A.projectID=B.projectID 
#					WHERE A.numberOfChildren = 0 GROUP BY A.projectID,B.project_name) T)" % (DBNAME,SUMMARYtable,DBNAME,DBNAME) 
#	
#	query = query.replace('\t','');
#	cur.execute(query)
#	con.commit()
	
	###########################################
	# Call Attask Note Query
	###########################################
    printf('Searching Notes ...\n')
    Nlimit = 2000
    note_fld = note_fields()
    drop_mysql_table(con,DBNAME,NOTEStable)
    create_attask_NOTE_table(con,DBNAME,NOTEStable)
    start = time.time()
    for ppp in range(0,16):
        notes = []
        for i in range(ppp*50,(ppp+1)*50):
            query_success = False
            while query_success == False:
                try:
                    notes = notes + client.search(ObjCode.NOTE,{},note_fld,i*Nlimit,Nlimit)
                    query_success = True
                except:
                    printf('FAILURE ... Task: %10s : %10s\n',i*Nlimit,Nlimit);
                    time.sleep(1)

        pd.DataFrame(notes).to_csv('./backup/notes_' + str(ppp).zfill(2) + '_' + cur_datetime.strftime('%Y%m%d') + '.csv',encoding='utf-8')

        if (len(notes) > 0):
            insert_into_attask_NOTE_table(con,DBNAME,NOTEStable,pd.DataFrame(notes),start)

        #all_notes_df = pd.merge(pd.DataFrame(notes),pd.DataFrame(project_list),how='left',left_on='topObjID',right_on='project_id')
	
