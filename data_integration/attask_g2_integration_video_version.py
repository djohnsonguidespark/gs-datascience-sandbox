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

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',400)

cur_datetime = datetime.now()
DBNAME = 'attask'
TABLENAME = 'notes'

con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

query_df = createDF_from_MYSQL(DBNAME,TABLENAME,"SELECT * FROM %s.%s WHERE noteText LIKE '%%http%%a.guidespark.com%%'")

split_var = []
project_id = []
task_id = []
entry_date = []
video_id = []
url_string = []
test_out = []
video_version_id = []
v1r1 = []
video_version = []
video_revision = []
for i in range(0,len(query_df)):
	project_id.append(query_df['top_obj_id'][i])
	try:
		task_id.append(query_df['obj_id'][i])
	except:
		task_id.append(None)

	############################################
	# Split string to extract video_id and V1R1
	############################################
	tmp_split = query_df['noteText'][i].split('/')
	tmp_version = tmp_split[len(tmp_split)-1].split("\n",1)[0]  
	tmp_version = tmp_version.split("\r",1)[0]  
	tmp_version = tmp_version.split(" ",1)[0]  
	tmp_version = tmp_version.split("#",1)[0]  
	tmp_version = tmp_version.split(")",1)[0]  
	tmp_version = tmp_version.split(".",1)[0]  
	tmp_version = tmp_version.split(",",1)[0]  
	tmp_version = tmp_version.split("?",1)[0]  
	tmp_version = tmp_version.split("-",1)[0]  
	tmp_version = tmp_version.split("!",1)[0]  
	tmp_version = tmp_version.split("[",1)[0]  
	tmp_version = tmp_version.split(";",1)[0]  
	tmp_version = tmp_version.split("Thanks",1)[0]  
	tmp_version = tmp_version.split("replaced",1)[0]  
	tmp_version = tmp_version.split("Tagging",1)[0]  

	tmp_split_test = query_df['noteText'][i].split('http')
	#tmp_version_test = tmp_split_test[all_substring("://",tmp_split_test)[0]]
	try:
		tmp_version_test = ''
		try:
			tmp_version_test = tmp_split_test[all_substring(".a.guidespark.com/videos/",tmp_split_test)[0]]
		except:
			try:
				tmp_version_test = tmp_split_test[all_substring(".a.guidespark.com//videos/",tmp_split_test)[0]]
			except:
				tmp_version_test = tmp_split_test[all_substring(".a.guidespark.com/admin/",tmp_split_test)[0]]
		tmp_version_test = tmp_version_test.replace('s://','')  
		tmp_version_test = tmp_version_test.replace('://','')  
		tmp_version_test = tmp_version_test.replace('\r',' ')  
		tmp_version_test = tmp_version_test.replace('\n',' ')  
		tmp_version_test = (tmp_version_test.split(' '))[0]  
		try:
			tmp_version_test = (tmp_version_test.split('a.guidespark.com/videos/'))[1]  
		except:
			try:
				tmp_version_test = (tmp_version_test.split('a.guidespark.com//videos/'))[1]  
			except:
				tmp_version_test = (tmp_version_test.split('a.guidespark.com/admin/'))[1]  
				tmp_version_test = tmp_version_test.replace('dashboard/','')  
				tmp_version_test = tmp_version_test.replace('accounts/','')  
				tmp_version_test = tmp_version_test.replace('assessments/','')  
				tmp_version_test = tmp_version_test.replace('revisions/','')  
				tmp_version_test = tmp_version_test.replace('/feedback_history','')  
	
		tmp_version_test = (tmp_version_test.split('#'))[0]		
		tmp_version_test = (tmp_version_test.split('.'))[0]		
		tmp_version_test = (tmp_version_test.split(','))[0]		
		tmp_version_test = (tmp_version_test.split(')'))[0]		
		tmp_version_test = (tmp_version_test.split('('))[0]		
		tmp_version_test = (tmp_version_test.split('?'))[0]		
		tmp_version_test = (tmp_version_test.split('!'))[0]		
		tmp_version_test = (tmp_version_test.split('-'))[0]		
		tmp_version_test = (tmp_version_test.split(':'))[0]		
		tmp_version_test = (tmp_version_test.split(';'))[0]		
		tmp_version_test = (tmp_version_test.split('['))[0]		
		tmp_version_test = (tmp_version_test.split('>'))[0]		
		tmp_version_test = (tmp_version_test.split('<'))[0]		
		tmp_version_test = tmp_version_test.replace('Thanks','')		
		tmp_version_test = tmp_version_test.replace('replaced','')		
		tmp_version_test = tmp_version_test.replace('Tagging','')		
		tmp_version_test = tmp_version_test.replace('ead','')		
		tmp_version_test = tmp_version_test.replace('but','')		
		tmp_version_test = tmp_version_test.replace('a','')		
		tmp_version_test = tmp_version_test.replace('videos/','')		
		tmp_version_test = tmp_version_test.replace('/build','')		
		tmp_version_test = tmp_version_test.replace('/preview','')		
		tmp_version_test = tmp_version_test.replace('a','')		
		tmp_version_test = tmp_version_test.replace(' ','')		

	except Exception as e:
		tmp_version_test = None
		printf("i = %4d . :%s: . Line %s: %s\n",i,tmp_version_test,sys.exc_traceback.tb_lineno,e)

	test_out.append(tmp_version_test)
	entry_date.append(query_df['entry_date'][i])
	url_string.append(query_df['noteText'][i])

	try:
		if ('/' in tmp_version_test.upper() and 'V' in tmp_version_test.upper() and 'R' in tmp_version_test.upper() and 'VIDEO_VERSION' not in tmp_version_test.upper() ):
			tmp_split_out = tmp_version_test.upper().split('/')
			tmp_video_id = tmp_split_out[0]
			tmp_video_version_id = None
			tmp_v1r1 = tmp_split_out[1]
			try:
				tmp_video_version = (tmp_split_out[1]).split('R')[0].split('V')[1]
			except:
				tmp_video_version = None
			try:
				tmp_video_revision = (tmp_split_out[1]).split('R')[1]
			except:
				tmp_video_revision = None
		else:
			try:
				if ('VIDEO_VERSIONS' in tmp_version_test.upper()):
					tmp_video_id = None
					tmp_video_version_id = tmp_version_test.replace('video_versions/','')
				else:
					tmp_video_id = tmp_version_test.split('/')[0]
					tmp_video_version_id = None
			except:
				tmp_video_id = tmp_version_test.split('/')[0]
				tmp_video_version_id = None
				
			tmp_v1r1 = None
			tmp_video_version = None
			tmp_video_revision = None
	except:
		try:
			tmp_video_id = tmp_version_test.split('/')[0]
		except:
			tmp_video_id = None 
		tmp_video_version_id = None
		tmp_v1r1 = None
		tmp_video_version = None
		tmp_video_revision = None

	video_id.append(tmp_video_id)
	video_version_id.append(tmp_video_version_id)
	v1r1.append(tmp_v1r1)
	video_version.append(tmp_video_version)
	video_revision.append(tmp_video_revision)

	####
	#if (len(tmp_split[len(tmp_split)-1]) != 4):
	#if (i == 34):
	#	printf("%5s ... %s ... %s\n",i,tmp_split[len(tmp_split)-1],tmp_version)
	#	sys.exit()

video_lookup = {}
video_lookup['projectID'] = project_id 
video_lookup['taskID'] = task_id 
video_lookup['noteText'] = url_string
video_lookup['entry_date'] = entry_date
video_lookup['test_split'] = test_out 
video_lookup['video_id'] = video_id 
video_lookup['video_version_id'] = video_version_id 
video_lookup['v1r1'] = v1r1
video_lookup['video_version'] = video_version 
video_lookup['video_revision'] = video_revision 
video_lookup_df = pd.DataFrame(video_lookup)
 
header = ['projectID','taskID','entry_date','video_id','video_version_id','v1r1','video_version','video_revision','test_split','noteText']
#createXLSX('attask_g2_lookup','video_creation_cycle',header,[],video_lookup_df,True)
video_lookup_df.to_csv('attask_g2_lookup.csv',columns=header)

########################
# Remove None taskIDs
########################
idx = []
for i in range(0,len(video_lookup_df)):
	if (video_lookup_df.ix[i]['taskID'] == 'None'):
		idx.append(i)


video_lookup_df = video_lookup_df.drop(video_lookup_df.index[idx]).reset_index(drop=True)

########################################
# Update video versions table
######################################## 

con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

start = time.time()
Nerror = 0

try:
	query = "ALTER TABLE guidespark2_prod.video_versions ADD attask_project_id VARCHAR(33) AFTER attask_id"
	cur.execute(query)
except Exception as e:
	printf('\n%s\n', query.replace('\t','') )
	printf("Line %s: . %s\n",sys.exc_traceback.tb_lineno,e)

try:
	query = "ALTER TABLE guidespark2_prod.video_versions ADD attask_task_id VARCHAR(33) AFTER attask_project_id"
	cur.execute(query)
except Exception as e:
	printf('\n%s\n', query.replace('\t','') )
	printf("Line %s: . %s\n",sys.exc_traceback.tb_lineno,e)

for i in range(0,len(video_lookup_df)):

	if ((i % 1000) == 999):
		printf('[attask_g2_integration_video_version.py] %7d of %7d Elements ... %.3f sec\n',i+1,len(video_lookup_df),time.time()-start)

	try:
		query = "UPDATE guidespark2_prod.video_versions SET attask_project_id = '%s' WHERE video_id = '%s' AND major = '%s' and minor = '%s'" % \
					(video_lookup_df['projectID'][i],video_lookup_df['video_id'][i],video_lookup_df['video_version'][i],video_lookup_df['video_revision'][i] )  
		cur.execute(query)
	except Exception as e:
		printf("%s\n",query)
		printf("UPDATE ATTASK ID ... i = %5d . Line %s: %s\n",i,sys.exc_traceback.tb_lineno,e)
		Nerror = Nerror + 1

	try:
		query = "UPDATE guidespark2_prod.video_versions SET attask_task_id = '%s' WHERE video_id = '%s' AND major = '%s' and minor = '%s'" % \
					(video_lookup_df['taskID'][i],video_lookup_df['video_id'][i],video_lookup_df['video_version'][i],video_lookup_df['video_revision'][i] )  
		cur.execute(query)
	except Exception as e:
		printf("%s\n",query)
		printf("UPDATE ATTASK TASK ID ... i = %5d . Line %s: %s\n",i,sys.exc_traceback.tb_lineno,e)
		Nerror = Nerror + 1

con.commit()


