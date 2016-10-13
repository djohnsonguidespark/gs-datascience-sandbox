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

start = time.time()

DBNAME_IN = 'attaskDB_NEW'
DBNAME_OUT = 'attask'

execfile('/home/analytics/analytics_sandbox/python_libs/deleted_query.py')

deleted_projects_df = createDF_from_MYSQL(DBNAME_IN,'projects',query_projects)
deleted_tasks_df = createDF_from_MYSQL(DBNAME_IN,'tasks',query_tasks)

query = 'SELECT max(id) FROM %s.input_date WHERE day = "Sun"' % (DBNAME_OUT)
input_date_id = int(pd.read_sql(query,con).ix[0]['max(id)'])

printf('input_date_id = %s\n',input_date_id)

upload_deleted_into_attask_PROJECT_table(con,DBNAME_OUT,'projects',input_date_id,deleted_projects_df,start)
upload_deleted_into_attask_TASK_table(con,DBNAME_OUT,'tasks',input_date_id,deleted_tasks_df,start)

