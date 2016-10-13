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

cur_datetime = datetime.now()
start = time.time()

churn_df = pd.read_csv('./output/final_pred_' + cur_datetime.strftime('%Y%m%d') + '.csv')

########## SIMPLE SALESFORCE ##############
sf = Salesforce(username='djohnson@guidespark.com', password=pwd,security_token=token)

###########################
# Write results to SFDC
###########################

sfdc_write_variables = ['Churn_Model_Probability__c','Churn_Model_Health_Score__c']

if (len(sfdc_write_variables) > 2):
	printf("Too many write variables\n")
	sys.exit()
if (sfdc_write_variables[0] != 'Churn_Model_Probability__c'):
	printf("Incorrect write variables\n")
	sys.exit()
if (sfdc_write_variables[1] != 'Churn_Model_Health_Score__c'):
	printf("Incorrect write variables\n")
	sys.exit()

####################
# Reset variables
####################
for i in range(0,len(churn_df)):
	printf("%4i of %4i ... SFDC Account = %15s ... ",i,len(churn_df)-1,churn_df.ix[i]['sfdc'])
	try:
		sf.Account.update(churn_df.ix[i]['sfdc'],{sfdc_write_variables[0]:None, \
												  sfdc_write_variables[1]:None})
		printf("RESET SUCCESS ... %.2f sec\n",time.time()-start)
	except Exception as e:
		printf("\n RESET FAILED ... Line %s: i = %4d: %s\n",sys.exc_traceback.tb_lineno,i,e)

####################
# Update variables
####################
for i in range(0,len(churn_df)):
	printf("%4i of %4i ... SFDC Account = %15s ... (Health Score,Survival) = (%7.2f%%,%1s) ... ",i,len(churn_df)-1,churn_df.ix[i]['sfdc'],churn_df.ix[i]['surv']*100,churn_df.ix[i]['pred_status'] )
	try:
		sf.Account.update(churn_df.ix[i]['sfdc'],{sfdc_write_variables[0]:churn_df.ix[i]['surv'], \
												  sfdc_write_variables[1]:churn_df.ix[i]['pred_status']})
		printf("UPDATE SUCCESS ... %.2f sec\n",time.time()-start)
	except Exception as e:
		printf("\n UPDATE FAILED ... Line %s: i = %4d: %s\n",sys.exc_traceback.tb_lineno,i,e)


	
	
