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
START_DATETIME = datetime(2014,6,1,0,0,0)
END_DATETIME = datetime(2014,12,1,0,0,0)

cur_datetime = datetime.now()

#####################################################
# Browser Query END_DATETIME
# Add 1 to day since we push 7 hours to UTC time
#####################################################
#QUERY_START_DATETIME = datetime.datetime.strftime(START_DATETIME,'%Y-%m-%d %H:%M:%S')
#QUERY_END_DATETIME = datetime.datetime.strftime(END_DATETIME+datetime.timedelta(days=1),'%Y-%m-%d %H:%M:%S')
QUERY_START_DATETIME = datetime.strftime(START_DATETIME,'%Y-%m-%d')
QUERY_END_DATETIME = datetime.strftime(END_DATETIME+timedelta(days=1),'%Y-%m-%d')

##########################
## Query DB 
##########################
con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

############################################
# 0) GET ALL sessions ... set is_mobile 
############################################
query = "SELECT id,user_agent,created_at,account_id,g1_id FROM %s.sessions where user_agent IS NOT NULL" % (DB_NAME)
printf('[create_mobile_field.py] :%s:\n',query.replace('\t',''))
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
		printf("[create_mobile_field.py] Session = %6d of %6d ... %.3f sec\n",i,len(session_input),time.time()-start)

	session_id.append(int(session_input[i][0]))
	user_agent.append(session_input[i][1])
	created_at.append(session_input[i][2])
	account_id.append(session_input[i][3])
	g1_id.append(session_input[i][4])

	count.append(1)
	try:
		ua = parse(session_input[i][1])
		browser.append(ua.browser.family + '-' + ua.browser.version_string)
	except:
		browser.append('ERROR')

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

prod_browser = []
prod_count = []
for x in user_agent:
	###########
	# g2 only
	###########
#		if ('GUIDESPARK1' not in x):
#			prod_count.append(1)
#			new_user_agent = 'Other'
#
#			if ('OPR' in x):
#				new_user_agent = 'Opera'
#			elif ('iPad' in x or 'iPhone OS' in x):
#				new_user_agent = 'Mobile Safari'
#			elif ('Android' in x):
#				new_user_agent = 'Android'
#			elif ('Chrome' in x):
#				new_user_agent = 'Chrome'
#			elif ('Firefox' in x):
#				new_user_agent = 'Firefox'
#			elif ('Safari' in x):
#				new_user_agent = 'Safari'
#			elif ('MSIE' in x):
#				new_user_agent = 'Internet Explorer'
#			elif ('Trident' in x and 'rv:11.0' in x):
#				new_user_agent = 'Internet Explorer 11'
#			elif ('Site24x7' in x):
#				new_user_agent = 'Blacklisted UA'
#			#elif ('GUIDESPARK1' in x):
#			#	if ('MOBILE' in x):
#			#		new_user_agent = 'G1 Mobile'
#			#	else:
#			#		new_user_agent = 'G1 Desktop'
#			prod_browser.append(new_user_agent)

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

python_mobile_session = []
python_mobile_session.append(len(mobile_index))
prod_mobile_session = []
prod_mobile_session.append(len(prod_mobile_index)) 
printf("[create_mobile_field.py] :MOBILE INDEX,PROD_MOBILE_INDEX  ... %s,%s ELEMENTS:\n",len(mobile_index),len(prod_mobile_index) )
if (len(list(set(mobile_index)-set(prod_mobile_index))) > 0 or len(list(set(prod_mobile_index)-set(mobile_index))) > 0):
	printf("[create_mobile_field.py] QUERIES ARE NOT THE SAME ... REVIEW\n")
	printf("[create_mobile_field.py] :DIFF LIST MOBILE_INDEX (python_lib)   ... %s ...:\n",list(set(mobile_index)-set(prod_mobile_index)))
	printf("[create_mobile_field.py] :DIFF LIST PROD_MOBILE_INDEX (g2_prod) ... %s ...:\n\n",list(set(prod_mobile_index)-set(mobile_index)))
	#sys.exit()

printf("\n[create_mobile_field.py] ************************************************************\n")
printf("[create_mobile_field.py] (MOBILE_INDEX python,PROD_MOBILE_INDEX g2) = (%6d,%6d)\n",sum(python_mobile_session),sum(prod_mobile_session) )
printf("[create_mobile_field.py] ************************************************************\n")

### update db and add is_mobile column

t0 = time.time()
query = "ALTER TABLE %s.sessions ADD is_mobile TINYINT AFTER g1_type" % (DB_NAME) 
printf("[create_mobile_field.py]: :%s:\n",query)
cur.execute(query)
printf("[create_mobile_field.py]: Module Timing: %s seconds\n",time.time() - t0)

t0 = time.time()
query = "UPDATE %s.sessions SET is_mobile = 0" % (DB_NAME) 
printf("[create_mobile_field.py]: :%s:\n",query)
cur.execute(query)
printf("[create_mobile_field.py]: Module Timing: %s seconds\n",time.time() - t0)

t0 = time.time()
for i in range(0,len(prod_mobile)):
	if ( ((i+1) % 100000) == 0 ):
		printf("Session = %6d of %6d\n",i+1,len(prod_mobile))

	if (prod_mobile[i] == 1):
		query = "UPDATE %s.sessions SET is_mobile = 1 WHERE id = %s" % (DB_NAME,session_id[i]) 
		try:
			cur.execute(query)
		except:
			printf('QUERY FAILED ... :%s:\n',query)
	
printf("[create_mobile_field.py]: Module Timing: %s seconds\n",time.time() - t0)

con.commit()

