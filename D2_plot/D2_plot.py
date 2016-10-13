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
#from geopy import geocoders
#from pygeocoder import Geocoder
from tzwhere import tzwhere
import pytz

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',400)

sys.path.insert(0,'/home/analytics/analytics_sandbox/python_libs');
from common_libs import *

DBNAME = 'sandbox_prod';
TABLENAME1 = 'TMP_D2_PLOT_TOTAL'

cur_datetime = datetime.now()
start=time.time()

#########################
# Query DB 
#########################
con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

##########################
# 1) Read in the D2 data 
##########################
query = "SELECT * FROM %s.%s" % (DBNAME,TABLENAME1)

#printf(':%s:\n',query.replace('\t',''))
d2_plot_df = createDF_from_MYSQL_query(query)

printf("[D2_plot.py] Session Location Query Complete ... %.2f min\n",(time.time()-start)/60)

##g = geocoders.GoogleV3()
#tz=tzwhere.tzwhere()
#tz_name = []
#for i in range(0,len(session_location_df)):
#	
#	if (i % 1000 == 999):
#		printf("[time_zone.py] Parent_ids %6d of %6d ... %.2f min\n",i+1,len(session_location_df),(time.time()-start)/60)
#	try:
#		tz_name.append(tz.tzNameAt(session_location_df['latitude'][i],session_location_df['longitude'][i]) )
#	except:
#		tz_name.append(None)
#
#session_location_df = session_location_df.join(pd.DataFrame(tz_name))
#session_location_df = session_location_df.rename(columns={0:'tz_name'})
#session_location_df.to_csv('./output/session_locations_NoTimeDelta_' + cur_datetime.strftime('%Y%m%d') +'.csv',encoding='utf-8')
#
################
## Update table
################
#query = "ALTER TABLE %s.%s ADD tz_name VARCHAR(50)" % (DBNAME,TABLENAME1);
#cur.execute(query)
#con.commit() # necessary to finish statement
#
#query = "ALTER TABLE %s.%s ADD utc_time_delta TINYINT" % (DBNAME,TABLENAME1);
#cur.execute(query)
#con.commit() # necessary to finish statement
#
##########################################
## Update session_locations table
##########################################
#printf("[time_zone.py] Update session_locations table\n")
#for i in range(0,len(session_location_df)):
#
#	if (i % 10000 == 9999):
#		printf("[time_zone.py] Sessions %7d of %7d ... %.2f min\n",i+1,len(session_location_df),(time.time()-start)/60)
#
#	try:
#		query = "UPDATE %s.%s SET tz_name = '%s' WHERE id = %d" % (DBNAME,TABLENAME1,tz_name[i],session_location_df['id'][i]);
#		cur.execute(query)
#		con.commit() # necessary to finish statement
#	except Exception as e:
#		printf("tz_name ... i = %4d . %8d :%25s: . :Line %5s: %70s\n", \
#			i,session_location_df['id'][i],tz_name[i],sys.exc_traceback.tb_lineno,e)
#
#	try:
#		query = "UPDATE %s.%s SET utc_time_delta = %s WHERE id = %d" % (DBNAME,TABLENAME1,int(pytz.timezone(tz_name[i]).localize(session_location_df['created_at'][i]).strftime('%z'))/100,session_location_df['id'][i] );
#		cur.execute(query)
#		con.commit() # necessary to finish statement
#	except Exception as e:
#		printf("utc_time_delta ... i = %4d . %8d :%25s: . :Line %5s: %70s\n", \
#			i,session_location_df['id'][i],tz_name[i],sys.exc_traceback.tb_lineno,e)
#
#query = "SELECT id,session_id,created_at,latitude,longitude,tz_name,utc_time_delta FROM %s.%s" % (DBNAME,TABLENAME1)
#createDF_from_MYSQL_query(query).to_csv('./output/session_locations_' + cur_datetime.strftime('%Y%m%d') +'.csv',encoding='utf-8')
#
#########################################
## Update sessions table
#########################################
#printf("[time_zone.py] Alter sessions table ... %.2f\n",time.time()-start)
#query = "ALTER TABLE %s.%s ADD tz_name VARCHAR(50)" % (DBNAME,TABLENAME2);
#cur.execute(query)
#con.commit()  #necessary to finish statement
#query = "ALTER TABLE %s.%s ADD tz_local_time DATETIME" % (DBNAME,TABLENAME2);
#cur.execute(query)
#con.commit()  #necessary to finish statement
#
#query = "ALTER TABLE %s.%s ADD utc_time_delta TINYINT" % (DBNAME,TABLENAME2);
#cur.execute(query)
#con.commit()  #necessary to finish statement
#
#printf("[time_zone.py] Update tz_name in sessions table ... %.2f\n",time.time()-start)
#query = "UPDATE %s.%s A LEFT JOIN %s.session_locations B ON A.id = B.session_id SET A.tz_name = B.tz_name WHERE A.id = B.session_id" % (DBNAME,TABLENAME2,DBNAME);
#cur.execute(query)
#con.commit()  #necessary to finish statement
#
#printf("[time_zone.py] Update sessions table\n")
#query = "SELECT * FROM %s.%s" % (DBNAME,TABLENAME2);
#session_df = createDF_from_MYSQL_query(query)
#start = time.time()
#for i in range(0,len(session_df)):
#
#	if (i % 100000 == 99999):
#		printf("Sessions %7d of %7d ... %.3f min\n",i+1,len(session_df),(time.time()-start)/60)
#
#	##/ TODO should be correct
#	try:
#		query = "UPDATE %s.%s SET utc_time_delta = %s WHERE id = %d" % (DBNAME,TABLENAME2,int(pytz.timezone(session_df['tz_name'][i]).localize(session_df['created_at'][i]).strftime('%z'))/100,session_df['id'][i] );
#		cur.execute(query)
#		con.commit()  #necessary to finish statement
#
#		query = "UPDATE %s.%s SET tz_local_time = '%s' WHERE id  = %d" % (DBNAME,TABLENAME2,session_df['created_at'][i] + timedelta(hours = int(pytz.timezone(session_df['tz_name'][i]).localize(session_df['created_at'][i]).strftime('%z'))/100),session_df['id'][i] );
#		cur.execute(query)
#		con.commit()  #necessary to finish statement
#	except Exception as e:
#		printf("i = %4d . :%25s: . :Line %5s: %70s\n", \
#			i,session_df['tz_name'][i],sys.exc_traceback.tb_lineno,e)
#			
