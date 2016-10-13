#! /usr/bin/env python

import sys
import re
import json
import csv
import collections
from openpyxl import load_workbook
from openpyxl.reader.excel import Workbook
from openpyxl.style import Color, Fill
from openpyxl.cell import Cell
from datetime import datetime, timedelta
from subprocess import call
import MySQLdb as mdb
import pandas as pd
import numpy as np 
import math 
import time

sys.path.insert(0,'/home/analytics/analytics_sandbox/python_libs');
from common_libs import *

start = time.time()

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',400)

cur_datetime = datetime.now()
DBNAME = 'guidespark2_prod'
TABLENAME = 'reviews'

con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

edits_df = pd.read_csv('/home/analytics/analytics_sandbox/data_integration/Edits_Update_2015_37.csv') 

category_int = []
for i in range(0,len(edits_df)):
	try:
		if (edits_df.ix[i]['objective/subjective'].upper() == 'SUBJECTIVE'):
			category_int.append(1)
		elif (edits_df.ix[i]['objective/subjective'].upper() == 'OBJECTIVE'):
			category_int.append(2)
		elif (edits_df.ix[i]['objective/subjective'].upper() == 'BOTH'):
			category_int.append(3)
		else:
			category_int.append(0)
	except:
		category_int.append(0)

edits_df = edits_df.join(pd.DataFrame(category_int)).rename(columns = {0:'edit_category'}) 


for i in range(0,len(edits_df)):

	if ((i % 1000) == 999):
		printf('[upload_edits_data.py] %7d of %7d Elements ... %.3f sec\n',i+1,len(edits_df),time.time()-start)

	try:
		query = "UPDATE %s.%s SET opinion = %s WHERE id = '%s'" % (DBNAME,TABLENAME,edits_df.ix[i].edit_category,edits_df.ix[i].id)
		cur.execute(query)
	except Exception as e:
		printf('\n%s\n', query.replace('\t','') )
		printf("Line %s: . %s\n",sys.exc_traceback.tb_lineno,e)

con.commit()


