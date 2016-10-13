# -*- coding: utf-8 -*-

################################
#
# 9/11/2014
# Program created by Devon K Johnson
#
# Program reads attask to determine the status of each project
# Outputs a ranking to say if project is on-time or needs to be escalated
#
# Variables
# 1) project_list -> all projects to be analyzed ... subject to filters
# 2) buffer_df -> project_list mapped to a dict 
# 3) ranking_df ->  the ranking for each project ... split by Rough Cut 1, Rough Cut 2 and Final Cut 
#		NOTE: idx of attask_input_df matches the index in buffer_df
# 4) attask_input_df -> merge of buffer_df and ranking_df 
#
# Ranking
# 1 -> date < planningComplete
# 2 -> date >= planningComplete & date < planningComplete + rc1_threshold_delta
# 3 -> date >= planningComplete + rc1_threshold_delta & date < planningComplete + buffer_rc1_total
# 4 -> date >= buffer_rc1_total
#
# NOTE: Http Error 500: Internal Server Error
# This generally means that one of the fields in
# project_fields is incorrect ... most likely a 
# custom field that begins with DE:
#
#
################################

from __future__ import division

import sys
import json
from simple_salesforce import Salesforce

from operator import itemgetter
import time
from datetime import datetime, timedelta
from openpyxl.reader.excel import Workbook
from openpyxl.style import Border,Style,Color, Fill
from openpyxl.cell import Cell
from workdays import * 

import numpy as np
import scipy.special
import pandas as pd
from pandas import pivot_table 

sys.path.insert(0,'/home/analytics/analytics_sandbox/python_libs');

from common_libs import * 

from attask_libs import * 
from attask_api import StreamClient, ObjCode, AtTaskObject

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',400)

execfile('/home/analytics/analytics_sandbox/python_libs/stuff.py')

###############
# Query Attask
###############
url = 'https://guidespark.attask-ondemand.com/attask/api'
#url = 'https://guidespark.attasksandbox.com/attask/api'

if ('ondemand' in url):
	printf("\n**********************************\n")
	printf("NOTE: Running PRODUCTION update")
	printf("\n**********************************\n\n")
else:
	printf("\n**********************************\n")
	printf("NOTE: Running SANDBOX update")
	printf("\n**********************************\n\n")

#tmp = raw_input('Do you want to UPDATE RANKINGS? (y/n): ')

update_rankings = True

###############
# Current Time
###############
cur_datetime = datetime.now()
printf('\n\n*************************************\ndatetime = %s\n*************************************\n\n',cur_datetime)

		
client = StreamClient(url)

print 'Logging in...'
client.login('djohnson@guidespark.com',pwd)
print 'Done'

print 'Retrieving user...'
user = AtTaskObject(client.get(ObjCode.USER,client.user_id,['ID','homeGroupID','emailAddr']))
print 'Done'
print user

print 'Get field lookups ...'
project_fld = project_fields()
task_fld = task_fields()

print 'Searching projects...'
Nlimit = 2000
projects=[]
for i in range(0,40):
	projects = projects + client.search(ObjCode.PROJECT,{},project_fld,i*Nlimit,Nlimit)

printf('%s project(s) found\n',len(projects))

##############################
# Cycle through all PROJECTS 
##############################
Nproject = 0 
for p in projects:

	#cur_project = AtTaskObject(projects[4]) 
	cur_project = AtTaskObject(p) 

	Nproject = Nproject + 1	
	try:
		printf('Project %5s of %5s ... %4s ... %100s',Nproject,len(projects),cur_project.status,cur_project.name)
	except:
		printf('Project %5s of %5s ... NAME ERROR',Nproject,len(projects))

	#############################################
	# Reset 'Project Ranking' and 'Ranking Flag'
	#############################################
	if update_rankings:
		try:
			tmp = AtTaskObject(client.put(ObjCode.PUT_PROJECT,cur_project.ID,{'DE:Project Ranking':'','DE:Ranking Flag':''}),client)
			printf(' ... Project Ranking/Ranking Flag RESET\n')
		except:
			printf(' ... Project Ranking/Ranking Flag RESET Failed\n')
	else:
		printf(' ... No ranking updates\n')
