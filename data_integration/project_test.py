#! /usr/bin/env python

import sys
import re 
import csv
import MySQLdb as mdb
import pandas as pd
import time

sys.path.insert(0,'/home/analytics/analytics_sandbox/python_libs');
sys.path.insert(0,'/home/analytics/analytics_sandbox/FY14/sales');
from project_test_libs import * 
from attask_api import StreamClient, ObjCode, AtTaskObject

execfile('/home/analytics/analytics_sandbox/python_libs/stuff.py')

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',400)

con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

###############
# Query Attask
###############
Nlimit = 2000
#url = 'https://guidespark.attasksandbox.com/attask/api'
url = 'https://guidespark.attask-ondemand.com/attask/api'

client = StreamClient(url)

print 'Logging in...'
client.login('djohnson@guidespark.com',pwd)
print 'Done'

############################################
# Call Attask Project Query
############################################

print 'Searching projects...'
project_fld = project_fields()
projects=[]
for i in range(0,10):
	query_success = False
	while query_success == False:
		try:
			projects = projects + client.search(ObjCode.PROJECT,{},project_fld,i*Nlimit,Nlimit)
			query_success = True
		except Exception as e:
			printf('Line %s ... FAILURE ... %s \n',sys.exc_traceback.tb_lineno,e)
			time.sleep(1)

project_df = pd.DataFrame(projects)

