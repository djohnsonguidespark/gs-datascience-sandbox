#! /usr/bin/env python

import sys
import re 
import MySQLdb as mdb
import csv
import subprocess 
import time
from openpyxl import load_workbook

sys.path.insert(0,'/home/analytics/analytics_sandbox/python_libs');
from common_libs import *

DB_NAME = 'guidespark2_prod';
TABLE_NAME = 'accounts'

######################################################
# if sys.argv[1] = 1 ... only update the video_lookup
######################################################

video_lookup_update_only = 0 
if (len(sys.argv) >= 2):
	video_lookup_update_only = sys.argv[1]
	printf("[g2_update.py] :UPDATE VIDEO LOOKUP ONLY:\n")

##########################
## Query DB 
##########################
con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

#################################################
## 1) ADD video_category lookup 
#################################################

t0_total = time.time()
t0 = t0_total
 
##############################################################
# 0a) Update account_ids that are missing (Kluge on 12/4/2014) 
##############################################################
t0 = time.time()

query = "UPDATE %s.activities a INNER JOIN %s.activities b \
		ON a.parent_id = b.id \
		SET a.account_id = b.account_id \
		WHERE a.account_id IS NULL \
		AND a.type = 'PlaybackActivity' \
		AND a.parent_id IS NOT NULL \
		AND b.account_id IS NOT NULL" % (DB_NAME,DB_NAME)
cur.execute(query)
printf("[g2_update.py]: Module Timing: %s seconds\n",time.time() - t0)

##############################################################
# 0b) Update slide_ids that are missing (Kluge on 4/14/2015) 
##############################################################
#t0 = time.time()
#
#query = "UPDATE %s.activities A LEFT JOIN %s.video_slides B \
#		ON A.video_version_id=B.video_version_id \
#		AND A.absolute_time >= B.starts_at \
#		AND A.absolute_time < (B.starts_at + B.duration) \
#		SET A.slide_id = B.id WHERE A.slide_id IS NULL AND A.video_version_id IS NOT NULL"  % (DB_NAME,DB_NAME)
#
#cur.execute(query)
#printf("[g2_update.py]: Module Timing: %s seconds\n",time.time() - t0)

##############################################################
# 1) Remove unnecessary TEMP / TEST / DEMO etc ... accounts 
#############################################################
t0 = time.time()

query = "select id,name from %s.accounts WHERE \
		UPPER(name) LIKE '%%TEST%%' \
		OR UPPER(name) LIKE '%%TEZT%%' \
		OR UPPER(name) = 'MONITOR' \
		OR UPPER(name) LIKE '%%TEMP%%' \
		OR UPPER(name) = 'ACME' \
		OR UPPER(name) = 'YXU' \
		OR UPPER(name) LIKE '%%DEMO%%'" % (DB_NAME) 
		#OR UPPER(name) = 'ADMIN' \
printf("%s\n",query.replace('\t','') )
cur.execute(query)
output = cur.fetchall();

printf("[g2_update.py]: DELETE accounts FROM %s.activities\n",DB_NAME)
for row in output:
	printf("[g2_update.py]: ACCOUNT REMOVAL: %5s : %40s ... %.2f sec\n",row[0],row[1],time.time() - t0)
	try:
		query = "DELETE FROM %s.accounts WHERE id = %s" % (DB_NAME,row[0])
		cur.execute(query)
	except:
		printf('Delete account %s Failed\n',row[0])

	try:
		query = "DELETE FROM %s.activities WHERE account_id = %s" % (DB_NAME,row[0])
		printf("%s\n",query.replace('\t','') )
		cur.execute(query)
	except Exception as e:
		printf("[g2_update.py]: Delete activities from account  %s Failed . Line %s: %s\n",row[0],sys.exc_traceback.tb_lineno,e)

con.commit()

### Delete remaining accounts
del_account = [1,233,348,389,455,461,466,534,694,783,784,828,842,843,914,940,989,998,1013,1021,1022,1060,1070,1074,1078,1085,1102,1241,1251,1261,1271, \
			   663,882,2331,839,1115,1491,1511,1641,2221,3261,	]
for row in del_account:
	printf("[g2_update.py]: ACCOUNT REMOVAL: %5s ... %.2f sec\n",row,time.time()-t0)
	try:
		query = "DELETE FROM %s.accounts WHERE id = %s" % (DB_NAME,row)
		cur.execute(query)
	except:
		printf('Delete account %s Failed\n',row)

	try:
		query = "DELETE FROM %s.activities WHERE account_id = %s" % (DB_NAME,row)
		cur.execute(query)
	except:
		printf('Delete activities from account %s Failed\n',row)

printf("[g2_update.py]: Module Timing: %s seconds\n",time.time() - t0)
con.commit()

################################################################
# 3) ADD new column to video_versions (attask_project_id) 
################################################################
execfile('/home/analytics/analytics_sandbox/data_integration/attask_g2_integration_video_version.py')
execfile('/home/analytics/analytics_sandbox/data_integration/upload_edits_data.py')

####################################################################
# 3) Run deal_percentage_complete_PROD.py 
####################################################################
#t0 = time.time()
#execfile('/home/djohnson/analytics/FY14/content_team/production/load_balance/deal_percentage_complete_PROD.py')
#printf("[deal_percentage_complete_PROD.py]: Module Timing: %s seconds\n",time.time() - t0)

t0 = time.time()
query = "CREATE INDEX Iclient_action ON %s.activities (client_action)" % (DB_NAME) 
printf("[g2_update.py]: :%s:\n",query)
try:
	cur.execute(query)
except Exception as e:
	printf("[g2_update.py]: %s Failed . Line %s: %s\n",query,sys.exc_traceback.tb_lineno,e)
	
printf("[g2_update.py]: Module Timing: %s seconds\n",time.time() - t0)


################################################################
# 4a) ADD new column to activities (client_action -> user_click) 
################################################################

#t0 = time.time()
#query = "ALTER TABLE %s.activities ADD user_click TINYINT AFTER client_action" % (DB_NAME) 
#printf("[g2_update.py]: :%s:\n",query)
#try:
#	cur.execute(query)
#except Exception as e:
#	printf("[g2_update.py]: %s Failed . Line %s: %s\n",query,sys.exc_traceback.tb_lineno,e)
#
#printf("[g2_update.py]: Module Timing: %s seconds\n",time.time() - t0)
#
#t0 = time.time()
#query = "UPDATE %s.activities SET user_click = 0" % (DB_NAME) 
#printf("[g2_update.py]: :%s:\n",query)
#cur.execute(query)
#printf("[g2_update.py]: Module Timing: %s seconds\n",time.time() - t0)
#
#query = "UPDATE %s.activities SET user_click = 1 \
#		WHERE client_action LIKE '%%FULLSCREEN%%' \
#		OR client_action = '_PAUSE' \
#		OR client_action = '_PLAY' \
#		OR client_action = '_PROGRESS_BAR_CHANGE' \
#		OR client_action = '_RESIZE' \
#		OR client_action LIKE '%%SHOW%%' \
#		OR client_action = '_SLIDE_CHANGE' \
#		OR client_action = '_START' \
#		OR client_action = '_VOLUME_CHANGE' " % (DB_NAME) 
#
#printf("[g2_update.py]: :%s:\n",query.replace('\t',''))
#cur.execute(query)
#con.commit()
#
#t0 = time.time()
#query = "CREATE INDEX Iuser_click ON %s.activities (user_click)" % (DB_NAME) 
#printf("[g2_update.py]: :%s:\n",query)
#try:
#	cur.execute(query)
#except Exception as e:
#	printf("[g2_update.py]: %s Failed . Line %s: %s\n",query,sys.exc_traceback.tb_lineno,e)
#
#printf("[g2_update.py]: Module Timing: %s seconds\n",time.time() - t0)

###############################################################
# 4b) ADD new column to activities (client_action -> buffering) 
###############################################################
t0 = time.time()
#query = "ALTER TABLE %s.activities ADD buffer TINYINT AFTER user_click" % (DB_NAME) 
#printf("[g2_update.py]: :%s:\n",query)
#cur.execute(query)
#printf("[g2_update.py]: Module Timing: %s seconds\n",time.time() - t0)

t0 = time.time()
#	query = "UPDATE %s.activities SET buffer = 0" % (DB_NAME) 
#	printf("[g2_update.py]: :%s:\n",query)
#	cur.execute(query)
#	printf("[g2_update.py]: Module Timing: %s seconds\n",time.time() - t0)
#
#	t0 = time.time()
#	query = "UPDATE %s.activities SET buffer = 1 \
#			WHERE client_action LIKE '%%VIDEO_BUFFERING%%' \
#			OR client_action = '_PLAYBACK_CONTINUE' " % (DB_NAME) 
#
#	printf("[g2_update.py]: :%s:\n",query.replace('\t',''))
#	cur.execute(query)
#	printf("[g2_update.py]: Module Timing: %s seconds\n",time.time() - t0)
#
#	t0 = time.time()
#	query = "CREATE INDEX Ibuffer ON %s.activities (buffer)" % (DB_NAME) 
#	printf("[g2_update.py]: :%s:\n",query)
#	cur.execute(query)
#	printf("[g2_update.py]: Module Timing: %s seconds\n",time.time() - t0)
#	con.commit()

######################################################################
# 4c) ADD is_mobile flag within sessions 
######################################################################
execfile('create_mobile_field.py')

con.commit()
printf("[g2_update.py]: Total Time %s\n",time.time() - t0_total)
