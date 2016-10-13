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

DBNAME = "benchmark_prod"

con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

query_group = []
query_group.append('AutoSpark')
query_group.append('AE')
query_group.append('AE (Templa')
query_group.append('PPT')

cur_week = (cur_datetime - timedelta(days=1)).date()

#### Top-10 by MKTG effectiveness
query = "SELECT account_id,account_name,yearweek,industry_name,Nvideo,Nparent as Nview,Nuser,USER_reach,BEEs,AVG_target_audience \
			FROM %s.AER_REACH_SUMMARY_DATEINTERVAL_WEEKLY_account \
			WHERE yearweek = '%s' and BEEs > 1 ORDER BY USER_reach desc" % (DBNAME,cur_week)

query = query.replace('\t','');
top10_mktg_effectiveness_df = createDF_from_MYSQL_query(query)
top10_mktg_effectiveness_df = top10_mktg_effectiveness_df.rename(columns={'USER_reach':'MKTG Effectiveness'})
 
#### Top-10 by Nuser
query = "SELECT account_id,account_name,yearweek,industry_name,Nvideo,Nparent as Nview,Nuser,USER_reach,BEEs,AVG_target_audience \
			FROM %s.AER_REACH_SUMMARY_DATEINTERVAL_WEEKLY_account \
			WHERE yearweek = '%s' and BEEs > 1 ORDER BY Nuser desc" % (DBNAME,cur_week)

query = query.replace('\t','');
top10_Nuser_df = createDF_from_MYSQL_query(query)
top10_Nuser_df = top10_Nuser_df.rename(columns={'USER_reach':'MKTG Effectiveness'})
 
columns =  ['account_id','account_name','yearweek','industry_name', \
			'Nvideo','Nview','Nuser','MKTG Effectiveness','BEEs','AVG_target_audience']
special_format= {}
special_format['MKTG Effectiveness'] = "0.0%" 
createXLSX('./output/top10_weekly','Top 10 MKTG Effectiveness',columns,special_format,top10_mktg_effectiveness_df[0:11],True)
createXLSX('./output/top10_weekly','Top 10 Nuser',columns,special_format,top10_Nuser_df[0:11],False)

execfile('email_attachment_top10.py')
