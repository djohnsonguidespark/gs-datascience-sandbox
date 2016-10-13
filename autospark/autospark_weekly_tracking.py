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

##############################################
# Update the day for the query
##############################################
query_day = 'Sun'

DBNAME = "attask"

con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

query_group = []
query_group.append('AutoSpark')
query_group.append('AE')
query_group.append('AE (Templa')
query_group.append('PPT')

for i in range(0,len(query_group)):
	query = "SELECT A.projectID,B.name as project_name,B.entryDate,B.DE_New_Customer_Upsell_Renewal,B.status as project_status,B.DE_Product_Title as product_title, \
				B.DE_Product_Line as product_line,B.DE_Product_Video_Category as Product_Video_Category,B.DE_Product_Video_Subcategory as Product_Video_Subcategory, \
				B.company_name,GROUP_CONCAT(distinct A.name ORDER BY A.name desc) as task_name, \
				GROUP_CONCAT(distinct A.user_name) as user_name,GROUP_CONCAT(distinct A.role_name) as role_name, \
				A.status as task_status,B.actualCompletionDate as project_completion_date,C.actualCompletionDate as Initial_Development_completion_date, \
				A.work as planned_hours,A.actualWork \
				FROM %s.tasks A \
				LEFT JOIN (select * from %s.projects where input_date_id IN (select max(id) from %s.input_date where day='%s')) B ON A.projectID=B.projectID \
				LEFT JOIN (SELECT projectID,actualCompletionDate from %s.tasks where UPPER(name) = 'INITIAL DEVELOPMENT' and \
				input_date_id IN (select max(id) from %s.input_date where day='%s')) C ON A.projectID=C.projectID \
				WHERE A.projectID IN ( \
				select projectID from %s.projects where DE_Production_Tool = '%s' and status NOT IN ('DED') and input_date_id IN (select max(id) from %s.input_date where day='%s')) \
				AND A.name IN ('Video Draft #1','Write Script') and A.status = 'CPL' and \
				A.input_date_id IN (select max(id) from %s.input_date where day='%s') GROUP BY A.projectID" \
				% (DBNAME,DBNAME,DBNAME,query_day,DBNAME,DBNAME,query_day,DBNAME,query_group[i],DBNAME,query_day,DBNAME,query_day)  	

	query = query.replace('\t','');
	if (query_group[i] == 'AutoSpark'):
		autospark_df = createDF_from_MYSQL_query(query)
	elif (query_group[i] == 'AE'):
		AE_df = createDF_from_MYSQL_query(query)
	elif (query_group[i] == 'AE (Templa'):
		AEtemplate_df = createDF_from_MYSQL_query(query)
	elif (query_group[i] == 'PPT'):
		PPT_df = createDF_from_MYSQL_query(query)

columns =  ['project_name','projectID','company_name','entryDate','DE_New_Customer_Upsell_Renewal', \
			'project_status','product_title','Product_Video_Category','Product_Video_Subcategory',   \
			'product_line','task_name','user_name','role_name','task_status', \
			'project_completion_date','Initial_Development_completion_date', \
			'planned_hours','actualWork'] 
createXLSX('./output/autospark_weekly','Autospark',columns,[],autospark_df,True)
createXLSX('./output/autospark_weekly','AE',columns,[],AE_df,False)
createXLSX('./output/autospark_weekly','AE_template',columns,[],AEtemplate_df,False)
createXLSX('./output/autospark_weekly','PPT',columns,[],PPT_df,False)

execfile('email_attachment_autospark.py')
