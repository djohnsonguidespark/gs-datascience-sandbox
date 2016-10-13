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
#query = "select user_agent,COUNT(user_agent) as Ncnt from sessions where account_id = 1065 and created_at > '2014-12-09 00:00:00' and is_mobile = 1 GROUP BY user_agent" % (DB_NAME)
query = "select id,user_agent,created_at,account_id,g1_id from %s.sessions where account_id = 1065 and created_at > '2014-12-09 00:00:00' and is_mobile = 1" % (DB_NAME)
printf(':%s:\n',query.replace('\t',''))
cur.execute(query)
session_input = cur.fetchall()
session_id = []
user_agent = []
created_at = []
account_id = []
g1_id = []
browser = []
os = []
device = []
count = []
start = time.time()
for i in range(0,len(session_input)):
	if ( (i % 100000) == 0 ):
		printf("Session = %6d of %6d ... %.3f sec\n",i,len(session_input),time.time()-start)

	session_id.append(int(session_input[i][0]))
	user_agent.append(session_input[i][1])
	created_at.append(session_input[i][2])
	account_id.append(session_input[i][3])
	g1_id.append(session_input[i][4])

	count.append(1)
	try:
		ua = parse(session_input[i][1])
		browser.append(ua.browser.family + '-' + ua.browser.version_string)
		os.append(ua.os.family + '-' + ua.os.version_string)
		device.append(ua.device.family)
	except:
		browser.append('ERROR')
		os.append('ERROR')
		device.append('ERROR')

		#printf(":%s-%s:%s:\n",user_agent.browser.family, \

dict_output = {}
dict_output['session_id'] = session_id 
dict_output['user_agent'] = user_agent
dict_output['created_at'] = created_at
dict_output['account_id'] = account_id
dict_output['g1_id'] = g1_id
dict_output['browser'] = browser 
dict_output['os'] = os 
dict_output['device'] = device 
dict_output['count'] = count 

python_mobile_session = []
prod_mobile_session = []
Nsession = []

#####################################
# Create mobile index for python lib
#####################################
mobile = []
a = all_substring('Amazon',browser)
b = all_substring('Android',browser)
c = all_substring('Mobile',browser)
d = all_substring('MOBILE',user_agent)

#####################################
# Calculate the # of PYTHON mobile sessions
#####################################
mobile_index = union(union(union(a,b),c),d)
for i in range(0,len(browser)):
	mobile.append(0)

for i in range(0,len(mobile_index)):
	mobile[mobile_index[i]] = 1

##################################
# Create pivot table with results
##################################
dict_output['mobile'] = mobile 

### mobile browser contains 'Mobile','Amazon' or 'Android'
df = pd.DataFrame(dict_output)
device_table = (pivot_table(df,values='count',index='device',aggfunc=np.sum)).order(ascending=False)
os_table = (pivot_table(df,values='count',index='os',aggfunc=np.sum)).order(ascending=False)  
browser_table = (pivot_table(df,values='count',index='browser',aggfunc=np.sum)).order(ascending=False)  
mobile_table = (pivot_table(df,values='count',index='mobile',aggfunc=np.sum)).order(ascending=False)  

prod_browser = []
prod_count = []
for x in user_agent:
	##############
	# g1 and g2
	##############
	prod_count.append(1)
	new_user_agent = 'Other'

	if ('OPR' in x):
		new_user_agent = 'Opera'
	elif ('iPad' in x or 'iPhone OS' in x):
		new_user_agent = 'Mobile Safari'
	elif ('Android' in x):
		new_user_agent = 'Android'
	elif ('Chrome' in x):
		new_user_agent = 'Chrome'
	elif ('Firefox' in x):
		new_user_agent = 'Firefox'
	elif ('Safari' in x):
		new_user_agent = 'Safari'
	elif ('MSIE' in x):
		new_user_agent = 'Internet Explorer'
	elif ('Trident' in x and 'rv:11.0' in x):
		new_user_agent = 'Internet Explorer 11'
	elif ('Site24x7' in x):
		new_user_agent = 'Blacklisted UA'
	elif ('GUIDESPARK1' in x):
		if ('MOBILE' in x):
			new_user_agent = 'G1 Mobile'
		else:
			new_user_agent = 'G1 Desktop'
	prod_browser.append(new_user_agent)


prod_output = {}

prod_output = {}
prod_output['count'] = prod_count 
prod_output['prod_browser'] = prod_browser
prod_df = pd.DataFrame(prod_output)
prod_browser_table = (pivot_table(prod_df,values='count',index='prod_browser',aggfunc=np.sum)).order(ascending=False)

###########################################
# Create mobile index for production query 
###########################################
prod_mobile = []
a = all_substring('Amazon',prod_browser)
b = all_substring('Android',prod_browser)
c = all_substring('Mobile',prod_browser)

###############################################
# Calculate the # of PRODUCTION mobile sessions
###############################################
prod_mobile_index = union(union(union(a,b),c),d)
for i in range(0,len(browser)):
	prod_mobile.append(0)

for i in range(0,len(prod_mobile_index)):
	prod_mobile[prod_mobile_index[i]] = 1

python_mobile_session.append(len(mobile_index))
prod_mobile_session.append(len(prod_mobile_index)) 
printf(":MOBILE INDEX,PROD_MOBILE_INDEX  ... %s,%s ELEMENTS:\n",len(mobile_index),len(prod_mobile_index) )
if (len(list(set(mobile_index)-set(prod_mobile_index))) > 0 or len(list(set(prod_mobile_index)-set(mobile_index))) > 0):
	printf("QUERIES ARE NOT THE SAME ... REVIEW\n")
	printf(":DIFF LIST MOBILE_INDEX (python_lib)   ... %s ...:\n",list(set(mobile_index)-set(prod_mobile_index)))
	printf(":DIFF LIST PROD_MOBILE_INDEX (g2_prod) ... %s ...:\n\n",list(set(prod_mobile_index)-set(mobile_index)))
	#sys.exit()

printf("\n************************************************************\n")
printf("(MOBILE_INDEX python,PROD_MOBILE_INDEX g2) = (%6d,%6d)\n",sum(python_mobile_session),sum(prod_mobile_session) )
printf("************************************************************\n")

df.to_csv('mobile_breakdown.csv')

