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

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',400)

con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

start = time.time()

######################################
# Find current published videos in G2
######################################
query = "SELECT A.id,A.title,B.name as category_name,A.category_id,A.created_at,A.updated_at,REPLACE(REPLACE(REPLACE(A.description,',',''),'\n',''),'\r',' ') as description,A.duration,A.short_name \
			FROM guidespark2_prod.videos A LEFT JOIN guidespark2_prod.categories B ON A.category_id=B.id"

g2_videos_df = pd.read_sql(query,con)

solution_lookup = []
for i in range(0,len(g2_videos_df)):

	if ((i % 1000) == 999):
		printf('[solution_lookup.py] %7d of %7d Elements ... %.3f sec\n',i+1,len(g2_videos_df),time.time()-start)

	try:
		if (bool(re.search('TEST',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Demo')
		elif (bool(re.search('DEMO',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Demo')
		elif (bool(re.search('BENEFIT',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('ENROLLMENT',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('OE',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('AE',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('HDHP',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('HIGH.*DEDUCTIBLE',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('CDHP',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('HRA',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('HSA',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('HEALTH.*SAVINGS',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('HEALTH.*SAVINGS.*ACCOUNT',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('HEALTH.*CARE.*ACCOUNT',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('HEALTH.*CARE.*SPEND',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('HEALTH.*RETIREMENT.*ACCOUNT',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('HEALTH.*REIMBURSEMENT.*ACCOUNT',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('HEALTH',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('MEDICAL',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('VISION',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('DENTAL',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('HCR',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('HEALTH.*CARE.*REFORM',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('HEALTHCARE',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('401',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('403',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('FSA',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('FLEXIBLE.*SPEND',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('EDUCA',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('DISABI',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('LIFE',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('WELLNESS',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('WALK',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('NEW.*HIRE',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('ONBOARD',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('WELCOME',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('LTC',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('LONG.*TERM.*CARE',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('INSURANCE',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('TIME.*OFF',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('VACATION',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('BABY',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('MATERNITY',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('DOCTOR',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('CONSUM',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Benefits')
		elif (bool(re.search('TAXES',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compensation')
		elif (bool(re.search('ESPP',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compensation')
		elif (bool(re.search('EMPLOYEE.*STOCK.*PURCHASE',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compensation')
		elif (bool(re.search('COMPENSATION',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compensation')
		elif (bool(re.search('RSU',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compensation')
		elif (bool(re.search('STOCK',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compensation')
		elif (bool(re.search('RESTRICTED.*STOCK.*UNITS',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compensation')
		elif (bool(re.search('UNITS ',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compensation')
		elif (bool(re.search('UNIT ',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compensation')
		elif (bool(re.search('SHARE ',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compensation')
		elif (bool(re.search('PAYROLL',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compensation')
		elif (bool(re.search('BASE.*PAY',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compensation')
		elif (bool(re.search('SALARY',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compensation')
		elif (bool(re.search('LTI',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compensation')
		elif (bool(re.search('INCENTIVE',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compensation')
		elif (bool(re.search('PERFORMANCE',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Performance Management')
		elif (bool(re.search('SERVICE',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Performance Management')
		elif (bool(re.search('RECOGNI',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Performance Management')
		elif (bool(re.search('PROGRESS',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Performance Management')
		elif (bool(re.search('ENGAGEMENT',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Performance Management')
		elif (bool(re.search('GOAL',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Performance Management')
		elif (bool(re.search('DEVELOP',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Performance Management')
		elif (bool(re.search('COMPLIANCE',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compliance')
		elif (bool(re.search('HIPAA',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compliance')
		elif (bool(re.search('CODE.*CONDUCT',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compliance')
		elif (bool(re.search('INSIDE.*TRAD',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compliance')
		elif (bool(re.search('SOCIAL.*MEDIA',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compliance')
		elif (bool(re.search('SECURITY',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compliance')
		elif (bool(re.search('PRIVACY',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compliance')
		elif (bool(re.search('SAFETY',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compliance')
		elif (bool(re.search('ICD-10',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Compliance')
		elif (bool(re.search('WORKDAY',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Systems')
		elif (bool(re.search('SUCCESS.*FACTORS',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Systems')
		elif (bool(re.search('SYSTEMS',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Systems')
		elif (bool(re.search('RETIRE',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Financial Wellness')
		elif (bool(re.search('INVESTM',g2_videos_df.ix[i]['title'].upper())) ):
			solution_lookup.append('Financial Wellness')

		else:
			solution_lookup.append(None)
	except:	
		solution_lookup.append(None)

g2_videos_df['lookup'] = solution_lookup

header = ['id','title','lookup','category_name','category_id','created_at','updated_at','duration','short_name']
g2_videos_df.to_csv('solution_lookup.csv',columns = header)
createXLSX('solution_lookup','lookup',header,[],g2_videos_df,True)

