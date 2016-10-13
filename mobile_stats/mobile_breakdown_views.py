#! /usr/bin/env python

import sys
import re 
import MySQLdb as mdb
import datetime 
import pandas as pd
from pandas import pivot_table 
from user_agents import parse
import numpy as np
import time
from datetime import datetime, timedelta
from openpyxl.reader.excel import Workbook
from openpyxl.style import Color, Fill
from openpyxl.cell import Cell

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',400)

sys.path.insert(0,'/home/analytics/analytics_sandbox/python_libs');
from common_libs import *

DB_NAME = 'guidespark2_prod';
TABLE_NAME = 'accounts'
cur_datetime = datetime.now()

##########################
## Query DB 
##########################
con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

############################################
# 0) GET ALL sessions ... set is_mobile 
############################################
query = "SELECT A.*,B.is_mobile,B.user_agent FROM ( \
				SELECT * FROM benchmark_prod.TMP_REACH_ALL WHERE session_id IN ( \
					SELECT id from guidespark2_prod.sessions where account_id = 1065 and created_at > '2014-12-10 00:00:00')) A \
						LEFT JOIN \
						guidespark2_prod.sessions B \
						ON A.session_id=B.id" 

views_df = createDF_from_MYSQL_query(query) 

##########################
# Update Desktop Views
##########################
count = []
os = []
browser = []
device = []
for i in range(0,len(views_df)):
	count.append(1)
	try:
		ua = parse(views_df.ix[i]['user_agent'])
		try:
			browser.append(ua.browser.family + '-' + ua.browser.version_string)
		except:
			browser.append('ERROR')
		try:
			os.append(ua.os.family + '-' + ua.os.version_string)
		except:
			os.append('ERROR')
		try:
			device.append(ua.device.family)
		except:
			device.append('ERROR')
	except:
		browser.append('ERROR')
		os.append('ERROR')
		device.append('ERROR')

views_df = views_df.join(pd.DataFrame(browser)).rename(columns={0:'browser'})
views_df = views_df.join(pd.DataFrame(os)).rename(columns={0:'os'})
views_df = views_df.join(pd.DataFrame(device)).rename(columns={0:'device'})

views_df.to_csv('BofA_view_breakdown.csv')

##device_table = (pivot_table(df,values='count',index='device',aggfunc=np.sum)).order(ascending=False)
##os_table = (pivot_table(df,values='count',index='os',aggfunc=np.sum)).order(ascending=False)  
##browser_table = (pivot_table(df,values='count',index='browser',aggfunc=np.sum)).order(ascending=False)  
##mobile_table = (pivot_table(df,values='count',index='mobile',aggfunc=np.sum)).order(ascending=False)  
#

