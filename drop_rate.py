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

DBNAME = 'sandbox_prod';
TABLENAME = 'TMP_DROP_SLIDE_DETAIL'

cur_datetime = datetime.now()

##########################
## Query DB 
##########################
con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

###################
# 0) GET DROP RATE 
###################
query = "SELECT A.*,CONCAT(A.account_id,'-',A.video_version_id) as unique_id,B.Nview_total FROM \
		(SELECT account_id,account_name,video_title,video_category,video_id,video_version_id,slide_title,Nslide_total,slide_number, \
		COUNT(slide_number) as Nview FROM %s.%s \
		GROUP BY account_id,video_id,video_version_id,slide_number) A \
		LEFT JOIN \
		(SELECT account_id,account_name,video_title,video_category,video_id,video_version_id,COUNT(video_version_id) as Nview_total \
		FROM %s.%s GROUP BY account_id,video_id,video_version_id) B \
		ON A.account_id=B.account_id AND A.video_id=B.video_id AND A.video_version_id=B.video_version_id \
		" % (DBNAME,TABLENAME,DBNAME,TABLENAME) 

printf(':%s:\n',query.replace('\t',''))
drop_rate = createDF_from_MYSQL_query(query)
drop_rate['Nslide_total'] = drop_rate['Nslide_total'].astype(int)   

unique_video_version = list(set(drop_rate['unique_id']))
start = time.time()
Nview_remaining_START = [0] * len(drop_rate)
Nview_remaining_END = [0] * len(drop_rate)
Nview_remaining_START_percentage = [0] * len(drop_rate)
Nview_remaining_END_percentage = [0] * len(drop_rate)

for i in range(0,len(unique_video_version)):
	
	if (i % 1000 == 999):
		printf("video versions %5d of %5d ... %.3f sec\n",i+1,len(unique_video_version),time.time()-start)

	cur_data = drop_rate.ix[all_indices(unique_video_version[i],drop_rate['unique_id'])].sort('slide_number')

	Nview_remaining_END[cur_data.index[0]] = drop_rate['Nview_total'][cur_data.index[0]] - drop_rate['Nview'][cur_data.index[0]]
	Nview_remaining_END_percentage[cur_data.index[0]] = float(drop_rate['Nview_total'][cur_data.index[0]] - drop_rate['Nview'][cur_data.index[0]]) / float(drop_rate['Nview_total'][cur_data.index[0]])     
	for j in range(1,len(cur_data)):
		Nview_remaining_END[cur_data.index[j]] = Nview_remaining_END[cur_data.index[j-1]] - drop_rate['Nview'][cur_data.index[j]] 
		Nview_remaining_END_percentage[cur_data.index[j]] = float(Nview_remaining_END[cur_data.index[j-1]] - drop_rate['Nview'][cur_data.index[j]]) / float(drop_rate['Nview_total'][cur_data.index[0]])

	Nview_remaining_START[cur_data.index[0]] = drop_rate['Nview_total'][cur_data.index[0]]
	Nview_remaining_START_percentage[cur_data.index[0]] = float(drop_rate['Nview_total'][cur_data.index[0]]) / float(drop_rate['Nview_total'][cur_data.index[0]]) 
	for j in range(1,len(cur_data)):
		Nview_remaining_START[cur_data.index[j]] = Nview_remaining_START[cur_data.index[j-1]] - drop_rate['Nview'][cur_data.index[j-1]] 
		Nview_remaining_START_percentage[cur_data.index[j]] = float(Nview_remaining_START[cur_data.index[j-1]] - drop_rate['Nview'][cur_data.index[j-1]]) / float(drop_rate['Nview_total'][cur_data.index[0]]) 

drop_rate = drop_rate.join(pd.DataFrame(Nview_remaining_START))  
drop_rate = drop_rate.rename(columns = {0:'Nview_remaining_START'})  
drop_rate = drop_rate.join(pd.DataFrame(Nview_remaining_START_percentage))  
drop_rate = drop_rate.rename(columns = {0:'Nview_remaining_START_percentage'})  
drop_rate = drop_rate.join(pd.DataFrame(Nview_remaining_END))  
drop_rate = drop_rate.rename(columns = {0:'Nview_remaining_END'})  
drop_rate = drop_rate.join(pd.DataFrame(Nview_remaining_END_percentage))  
drop_rate = drop_rate.rename(columns = {0:'Nview_remaining_END_percentage'})  
drop_rate['percentage_viewed'] = (drop_rate['slide_number']-1)/(drop_rate['Nslide_total']-1)

cur_datetime = datetime.now()
drop_rate.to_csv('drop_rate_' + cur_datetime.strftime('%Y%m%d') +'.csv') #,encoding='utf-8')
#drop_rate.to_csv('/media/sf_transfer/drop_rate_' + cur_datetime.strftime('%Y%m%d') +'.csv') #,encoding='utf-8')

