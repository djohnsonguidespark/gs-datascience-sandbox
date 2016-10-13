#! /usr/bin/env python

import sys
import re 
import csv
import MySQLdb as mdb
import pandas as pd
import psycopg2

sys.path.insert(0,'/home/analytics/analytics_sandbox/python_libs');
from common_libs import *
from create_mysql import *
from attask_libs import * 
from attask_api import StreamClient, ObjCode, AtTaskObject
from sfdc_libs import *

cur_datetime = datetime.now()
execfile('/home/analytics/analytics_sandbox/python_libs/stuff.py')

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',400)

def create_conn(*args,**kwargs):
	config = kwargs['config']
	try:
		con=psycopg2.connect(dbname=config['dbname'], host=config['host'], 
							port=config['port'], user=config['user'], 
							password=config['pwd'])
		return con
	except Exception as err:
		print(err)

config = { 'dbname': 'guidespark2prod', 
		   'user':user_red,
		   'pwd':pwd_red,
		   'host':host_red,
		   'port':'5439'
		 }


con_mysql = None
con_mysql = mdb.connect('localhost','root','','');
cur_mysql = con_mysql.cursor()

query = 'SELECT * FROM guidespark2_prod.activities LIMIT 100000'
start = time.time()
sandbox_df = pd.read_sql(query,con_mysql)
printf("MYSQL %s secs\n",time.time()-start)

DBNAME = 'guidespark2prod'
conn_string = "dbname="+ DBNAME + " port='5439' user=" + user_red + " password=" + pwd_red + " host=" + host_red

# print("Connecting to database\n        ->" + conn_string)
conn = psycopg2.connect(conn_string);
cursor = conn.cursor();

start = time.time()
out_df = pd.read_sql("select * from g2.playbackactivity LIMIT 100000",conn)
printf("AWS %s secs\n",time.time()-start)


