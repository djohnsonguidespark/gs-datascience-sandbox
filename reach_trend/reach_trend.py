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

execfile('/home/analytics/analytics_sandbox/python_libs/stuff.py')

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',400)

DBNAME = "benchmark_prod"
INTERVAL = 300

con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

query_group = []
query_group.append('AutoSpark')
query_group.append('AE')
query_group.append('AE (Templa')
query_group.append('PPT')

cur_datetime = datetime.now()
cur_year= cur_datetime.year
cur_month= cur_datetime.month
cur_day= cur_datetime.day

start = time.time()
cur_date = []
Naccount = []
Naccount_BM = []
mktg_effectiveness = []
mktg_effectiveness_BM = []
for i in range(0,18):

	printf("%3s of 12",i+1)
	#### MKTG effectiveness query
	query = "select C.*,IF(IFNULL(D.account_id,0)=0,0,IF(USER_reach < 0.01,0,1)) as in_benchmark FROM \
			(select '1-Year' as cur_year,A.account_id,A.account_name,A.industry_name,COUNT(distinct A.trackable_id) as Nvideo, \
			COUNT(distinct A.user_id) as Nuser,COUNT(distinct A.user_id)/B.AVG_target_audience as USER_reach, \
			COUNT(distinct A.user_id)/A.BEEs as USER_reach_BEE, \
			COUNT(distinct A.parent_id) as Nparent,COUNT(distinct A.parent_id)/B.AVG_target_audience as PARENT_reach, \
			COUNT(distinct A.parent_id)/A.BEEs as PARENT_reach_BEEs, \
			A.BEEs,B.AVG_target_audience  \
			FROM %s.TMP_REACH_ALL A \
			LEFT JOIN \
			(SELECT account_id,AVG(BEEs) as AVG_BEEs,AVG(target_audience) as AVG_target_audience FROM ( \
						SELECT account_id,trackable_id,BEEs,target_audience  \
							FROM %s.TMP_REACH_ALL  \
							WHERE min_time BETWEEN DATE_SUB('%s-%s-%s 00:00:00',INTERVAL %s DAY) AND '%s-%s-%s 00:00:00'  \
							GROUP BY account_id,trackable_id) T \
			GROUP BY account_id) B \
			ON A.account_id=B.account_id \
			WHERE A.min_time BETWEEN DATE_SUB('%s-%s-%s 00:00:00',INTERVAL %s DAY) AND '%s-%s-%s 00:00:00' \
			GROUP BY A.account_id) C \
			LEFT JOIN \
			(SELECT account_id FROM ( \
				SELECT account_id,MIN(min_time) as min_time,MAX(min_time) as max_time FROM %s.TMP_REACH_ALL GROUP BY account_id) T \
				WHERE min_time <= DATE_SUB('%s-%s-%s 00:00:00', INTERVAL %s DAY) AND max_time >= DATE_SUB('%s-%s-%s 00:00:00', INTERVAL %s DAY) ) D \
			ON C.account_id=D.account_id" % (DBNAME,DBNAME, \
											cur_year,str(cur_month).zfill(2),str(cur_day).zfill(2), \
											INTERVAL, \
											cur_year,str(cur_month).zfill(2),str(cur_day).zfill(2), \
											cur_year,str(cur_month).zfill(2),str(cur_day).zfill(2), \
											INTERVAL, \
											cur_year,str(cur_month).zfill(2),str(cur_day).zfill(2), \
											DBNAME, \
											cur_year,str(cur_month).zfill(2),str(cur_day).zfill(2), \
											INTERVAL/2, \
											cur_year,str(cur_month).zfill(2),str(cur_day).zfill(2), \
											INTERVAL/4) 

	query = query.replace('\t',''); 
	mktg_effectiveness_df = createDF_from_MYSQL_query(query)

	printf(" ... %.2f sec\n",time.time()-start)
	printf("%s\n\n",query)

	cur_date.append("%4s-%2s-%2s" % (cur_year,str(cur_month).zfill(2),str(cur_day).zfill(2) ) )
	Naccount.append( len(mktg_effectiveness_df))
	mktg_effectiveness.append( np.median(mktg_effectiveness_df.USER_reach))
	Naccount_BM.append( len(mktg_effectiveness_df[(mktg_effectiveness_df.USER_reach > 0.01) & (mktg_effectiveness_df.in_benchmark == 1)].USER_reach))
	mktg_effectiveness_BM.append( np.median(mktg_effectiveness_df[(mktg_effectiveness_df.USER_reach > 0.01) & (mktg_effectiveness_df.in_benchmark == 1)].USER_reach))

	if (cur_month == 1):
		cur_month = 12
		cur_year = cur_year - 1
	else:
		cur_month = cur_month - 1

	#top10_mktg_effectiveness_df = top10_mktg_effectiveness_df.rename(columns={'USER_reach':'MKTG Effectiveness'})

output = {}
output['cur_date'] = cur_date
output['Naccount'] = Naccount
output['Naccount_BM'] = Naccount_BM
output['reach'] = mktg_effectiveness
output['reach_BM'] = mktg_effectiveness_BM

output_df = pd.DataFrame(output)

print(output_df) 

sys.exit()

columns =  ['account_id','account_name','yearweek','industry_name', \
			'Nvideo','Nview','Nuser','MKTG Effectiveness','BEEs','AVG_target_audience']
special_format= {}
special_format['MKTG Effectiveness'] = "0.0%" 
createXLSX('./output/top10_weekly','Top 10 MKTG Effectiveness',columns,special_format,top10_mktg_effectiveness_df[0:11],True)
createXLSX('./output/top10_weekly','Top 10 Nuser',columns,special_format,top10_Nuser_df[0:11],False)

