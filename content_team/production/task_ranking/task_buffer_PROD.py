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
# 2 -> date >= planningComplete & date < planningComplete + initdev_threshold_delta
# 3 -> date >= planningComplete + initdev_threshold_delta & date < planningComplete + buffer_initdev_total
# 4 -> date >= buffer_initdev_total
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

holidays = [datetime(2014,5,26,0,0,0),datetime(2014,7,4,0,0,0),datetime(2014,9,1,0,0,0), \
			datetime(2014,11,27,0,0,0),datetime(2014,11,28,0,0,0),datetime(2014,12,25,0,0,0), \
			datetime(2014,12,31,0,0,0),datetime(2015,1,1,0,0,0)]
#holidays = ['2014-07-04','2014-09-01','2014-11-27','2014-11-28']

###########################################
# User Inputs
#
# timing_bin -> algo starts by binning timing relative to hard/soft deadline ... all
#				other ranking components follow these bins ... this is the timing range
#				of each bin
###########################################
timing_bin = 5 

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

update_rankings = True
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
template_fld = template_fields()
template_task_fld = template_task_fields()

template = [] 
template = template + client.search(ObjCode.TEMPLATE,{},template_fld)
for t in template:
	cur_template = AtTaskObject(template[1]) 
	
template_task = [] 
template_task = template_task + client.search(ObjCode.TEMPLATE_TASK,{},template_task_fld)
for t in template_task:
	cur_template_task = AtTaskObject(template_task[1]) 
	
template_map = map((lambda item: item['ID']),template)
template_name = map((lambda item: item['name']),template)
template_task_map = map((lambda item: item['templateID']),template_task)
template_task_name = map((lambda item: item['name']),template_task)
#template_task_map = sorted(tasks, key=itemgetter('taskNumber')) ### Sort by taskNumber

print 'Searching projects...'
Nlimit = 2000
projects=[]
for i in range(0,40):
	query_success = False
	while query_success == False:
		try:
			projects = projects + client.search(ObjCode.PROJECT,{},project_fld,i*Nlimit,Nlimit)
			query_success = True
		except:
			printf('FAILURE ... Project: (%4d)\n',i);
			time.sleep(1)

global_project_ID = map((lambda item: item['ID']),projects)
global_project_name = map((lambda item: item['name']),projects)

printf('%s project(s) found\n',len(projects))

##############
# Current Time
##############
cur_datetime = datetime.now()

#############################
# Cycle through all PROJECTS 
#############################
project_list = []
TOTAL_project_list = []
parent_task_idx = []
parent_task_CSM = []
parent_task_name = []
parent_task_status = []
parent_task_product_line = []
parent_task_planned_completion_date = []

dict_project = {}
dict_task = {}
tmp = {} 
tmp['ID'] = 'None' 	
tmp['lastSurveyedOn'] = 'None'	
tmp['name'] = 'None' 	
tmp['objCode'] = 'None' 	
tmp['showWhatsNew'] = 'None' 	

Nproject = 0 

for p in projects:

	#cur_project = AtTaskObject(projects[4]) 
	cur_project = AtTaskObject(p) 

	Nproject = Nproject + 1	

	#if (cur_project.ID != '55fa2b2000052a6bc201c2f55a051f9b'): #Nproject != 13):
	#	continue

	#if (Nproject > 3300):
	#	continue

	try:
		printf('Project %5s of %5s ... %4s ... %105s',Nproject,len(projects),cur_project.status,cur_project.name)
	except:
		printf('Project %5s of %5s ... %4s ... NAME ERROR',Nproject,len(projects),cur_project.status)

	###################################################
	# Add all projects to final list ... project_list
	# 1) All projects that DO NOT CONTAIN 'Style Guide'
	# 2) All with a non-NONE value for entry_date
	# 3) Current Projects only (CUR)  
	# 4) Portfolio = Client Projects  
	###################################################	

	Iportfolio = 0
	try:
		if (   p["portfolio"]["ID"] == '55c2357e00df512370137330c967bdc3' or p["portfolio"]["ID"] == '54f8a618000bc2eba05e82a51eb937a8' \
			or p["portfolio"]["ID"] == '54f8b0be0011a1cd5451df3c25f3d95b' or p["portfolio"]["ID"] == '54f8b0a3001195abd33e1a733700ceb7' 
			or p["portfolio"]["ID"] == 'D3fc9af7a9367bc2e0433465290a4286' \
			or p["portfolio"]["ID"] == '54f8b07f00118a0aae0325cedfae4ab5'):
			Iportfolio = 1
	except:
		Iportfolio = 0

	#############################################
	#
	# Collect ALL current and Planning projects
	#
	#############################################
	dict_date = {}
	min_parent_task = []
	if (cur_project.data.get('DE:Product Title') != 'Style Guide' and cur_project.categoryID != None \
			and cur_project.data.get('entryDate') != None \
			and	Iportfolio == 1 \
			and cur_project.status == 'CUR' or cur_project.status == 'PLN' or cur_project.status == 'ONH'): 

		dict_date['ID'] = cur_project.ID
		try:
			dict_date['company'] = p['company']['name'].lstrip().rstrip() 
		except:
			dict_date['company'] = 'NA'
		try:
			dict_date['CSM'] = p['owner']['name'] 
		except:
			dict_date['CSM'] = 'NA' 
			
		dict_date['project_name'] = cur_project.name.replace("'","").strip(' \t\n\r').replace(u'\x92','').replace(u'\xa0','').replace(u'\u2013','').replace(u'\u2019','').encode('latin1','replace')
		dict_date['status'] = cur_project.status
		dict_date['unit'] = 1 

		TOTAL_project_list.append(dict_date)

	dict_date = {}
	#if ( (cur_project.data.get('DE:New Customer/Upsell/Renewal') == 'New Customer' or cur_project.data.get('DE:New Customer/Upsell/Renewal') == 'Upsell') \
	if (cur_project.data.get('DE:Product Title') != 'Style Guide' and cur_project.categoryID != None \
			and cur_project.data.get('entryDate') != None \
			and	Iportfolio == 1 \
			and cur_project.status == 'CUR'): 

		printf(' ... INCLUDED')

		#################
		# Grab all tasks 
		#################
		task = []
		for i in range(0,1):
			query_success = False
			while query_success == False:
				try:
					task = task + client.search(ObjCode.TASK,{'projectID':cur_project.ID},task_fld,i*Nlimit,Nlimit)  ## USE SEARCH FOR MULTIPLE RECORDS
					query_success = True
				except:
					printf('FAILURE ... Task: (%4d)\n',i);
					time.sleep(1)

		try:
			task_df = pd.DataFrame(task).sort(['taskNumber']).reset_index(drop=True)
		except:
			task_df = []
			printf(' ... No TASKS ... NOT INCLUDED\n');

		###############################
		# Only continue if it has tasks
		###############################
		if (len(task_df) > 0 and cur_project.name != '7-Eleven - FSA Update 2015'):			

			############################################
			# Find critical parent tasks for each CSM
			# eliminate project name (i.e. start at 1) 
			############################################
			cur_parent_task_name = []
			for i in range(0,len(task_df)): 
				try:
					idx = task_df[(task_df['taskNumber'] == (i+1))].index[0]
					if (task_df.ix[idx]['numberOfChildren'] > 0 and task_df.ix[idx]['name'].upper() != 'PLANNING' \
						and task_df.ix[idx]['status'] != 'CPL' 
						and ('INITIAL DEVELOPMENT' in task_df.ix[idx]['name'].upper() 
						or 'VIDEO REVIEW CYCLE' in task_df.ix[idx]['name'].upper()
						or 'FINAL APPROVAL' in task_df.ix[idx]['name'].upper()
						or 'ADDITIONAL VIDEO EDIT' in task_df.ix[idx]['name'].upper() ) ):
	
							#printf("\n%2s\n",idx)	
							try:
								parent_task_CSM.append(p['owner']['name'])
							except:
								parent_task_CSM.append(None)
							cur_parent_task_name.append(task_df['name'][idx])
							parent_task_name.append(task_df['name'][idx])
							parent_task_status.append(task_df['status'][idx])
							parent_task_planned_completion_date.append(task_df['plannedCompletionDate'][idx][0:10])
							parent_task_product_line.append(p['DE:Product Line'])
				except Exception as e:
					printf("[task_buffer_TEST.py]: %3s ... Line %s: %s\n",i,sys.exc_traceback.tb_lineno,e)
	
			#####################
			# Find current task
			#####################
			i = 0
			cur_task = 'NA'
			while True:
				try:
					idx = task_df[(task_df['taskNumber'] == (i+1))].index[0]
					#printf("%2s:%3s:%3s\n",idx,task_df.ix[idx]['taskNumber'],task_df.ix[idx]['status'])	
					if ((task_df.ix[idx]['status'] == 'INP' or task_df.ix[idx]['status'] == 'NEW') and task_df.ix[idx]['numberOfChildren'] == 0 ):
						cur_task = task_df.ix[idx]['name']
						break
					else:
						i = i + 1
						if (i >= (len(task_df))):
							break		
				except Exception as e:
					printf("[task_buffer_TEST.py]: %3s ... Line %s: %s\n",i,sys.exc_traceback.tb_lineno,e)
					i = i + 1
					if (i >= 1000):
						break		
	
			dict_date = {}
	
			keep_project = 1 
			try:
				dict_date['ID'] = cur_project.ID
				dict_date['current_task'] = cur_task 
				try:
					dict_date['company'] = p['company']['name'].lstrip().rstrip() 
				except:
					dict_date['company'] = 'NA'
				try:
					dict_date['CSM'] = p['owner']['name'] 
				except:
					dict_date['CSM'] = 'NA' 
				
				dict_date['project_name'] = cur_project.name.replace("'","").strip(' \t\n\r').replace(u'\x92','').replace(u'\xa0','').replace(u'\u2013','').replace(u'\u2019','').encode('latin1','replace')
				dict_date['status'] = cur_project.status
			
				try: 
					if (p['DE:Program Start Date'] != None and p['DE:Program Start Date'] != 'NaT'):
						dict_date['program_start_date'] = datetime.strptime(p['DE:Program Start Date'],'%Y-%m-%d') 
					else:
						dict_date['program_start_date'] = None 
				except:
					dict_date['program_start_date'] = None 
	
				try:
					if (p['DE:Customer Hard Deadline'] != None and p['DE:Customer Hard Deadline'] != 'NaT'):
						dict_date['hard_deadline'] = datetime.strptime(p['DE:Customer Hard Deadline'],'%Y-%m-%d') 
					else:
						dict_date['hard_deadline'] = None 
				except:
					dict_date['hard_deadline'] = None 
	
				try:
					if (p['DE:Soft Deadline'] != None and p['DE:Soft Deadline'] != 'NaT'):
						dict_date['soft_deadline'] = datetime.strptime(p['DE:Soft Deadline'],'%Y-%m-%d') 
					else:
						dict_date['soft_deadline'] = None 
				except:
					dict_date['soft_deadline'] = None 
	
			except:
				keep_project = 0
				
			####################
			# Find Parent Tasks
			####################
			all_parent_task_df = task_df[(task_df.numberOfChildren > 0) & (task_df.taskNumber > 1)]
	
			test_df = task_df[(task_df['name'] == parent_task_name[0])]
			all_parent_task_df = test_df
			for i in range(0,len(cur_parent_task_name)):
				test_df = task_df[(task_df['name'] == cur_parent_task_name[i])]
				all_parent_task_df = pd.concat([all_parent_task_df,test_df]) 
				
			#################################
			# Cycle through the parent tasks
			#################################
			cur_parent_task_df = all_parent_task_df[(all_parent_task_df.status == 'INP') | (all_parent_task_df.status == 'NEW')]
		
			###################################################
			# Find the first parent task that is not complete 
			###################################################
			new_inp_index = list(cur_parent_task_df.index)
	
			if (len(new_inp_index) > 0): 
				try:
					min_new_inp_index = min(list(cur_parent_task_df.index))
					cur_parent_task = cur_parent_task_df.ix[min_new_inp_index]['name']
					cur_parent_df = task_df.ix[min_new_inp_index][['name','plannedStartDate','taskNumber','plannedCompletionDate','actualCompletionDate','status','numberOfChildren','plannedDuration']]
	
					try:
						dict_date['PARENT_task'] = cur_parent_df['name']
					except:
						dict_date['PARENT_task'] = None 
	
					try:
						dict_date['PARENT_status'] = cur_parent_df['status']
					except:
						dict_date['PARENT_status'] = None 
	
					try:	
						dict_date['PARENT_start_date'] = datetime.strptime(cur_parent_df['plannedStartDate'][:-5],'%Y-%m-%dT%H:%M:%S:%f') 
					except:
						dict_date['PARENT_start_date'] = None 
				
					try:			
						dict_date['PARENT_completion_date_planned'] = workday(datetime.strptime(cur_parent_df['plannedCompletionDate'][:-5],'%Y-%m-%dT%H:%M:%S:%f'),0,holidays)   
					except:
						dict_date['PARENT_completion_date_planned'] = None   
	
					try:
						dict_date['PARENT_completion_date_actual'] = datetime.strptime(cur_parent_df['actualCompletionDate'][:-5],'%Y-%m-%dT%H:%M:%S:%f') 
					except:
						dict_date['PARENT_completion_date_actual'] = None 
	
				except:
					keep_project = 0 
					dict_date['PARENT_task'] = None 
					dict_date['PARENT_status'] = None 
					dict_date['PARENT_start_date'] = None 
					dict_date['PARENT_completion_date_actual'] = None 
					dict_date['PARENT_completion_date_planned'] = None
		
			if (keep_project != 0):
				printf(' ... ADDED TO PROJECT LIST\n')
				project_list.append(dict_date)
			else:
				printf(' ... REMOVE PROJECT\n')
	else:
		printf('\n')

if (len(project_list) > 0):
	unit_df = []
	for i in range(0,len(project_list)):
		unit_df.append(1)
	buffer_df = pd.DataFrame(project_list)
	buffer_df = buffer_df.join(pd.DataFrame(unit_df,columns=['unit']))

	#########################################################################################################
	# Set ranking
	#	
	# Timeline
	# 	1	|	        2				|						3						|				4						
	# 		planned_completion_date		planned_completion_date + buffer_threshold		planned_completion_date + buffer
	#
	#
	#
	# 0 -> project complete 
	# a -> initdev 
	# b -> vu1
	# c -> final_cut 
	# 1 -> date < planned completion date
	# 2 -> date >= planned completion date & date < (planned_completion_date + buffer_threshold)
	# 3 -> date >= (planned_completion_date + buffer_threshold) & (date < planned_completion_date + buffer)
	# 4 -> date >= (planned_completion_date + buffer)
	#########################################################################################################

	ranking_list = []
	parent_idx = []
	parent_cur_task_length = []
	parent_planned_task_length = []
	parent_planned_task_length_w_buffer = []
	parent_day_delta = []
	parent_day_delta_w_buffer_threshold = []
	parent_day_delta_w_buffer = []
	parent_day_delta_w_remaining_buffer = []
	parent_day_delta_program = []
	parent_day_delta_hard_deadline = []
	parent_day_delta_soft_deadline = []
	parent_buffer_df = buffer_df[(buffer_df['PARENT_status'] != "CPL")] 
	for i in range(0,len(parent_buffer_df)):
		dict_parent = {}
		idx = parent_buffer_df.index[i]	
		#dict_parent['stage'] = ['final']*len(idx) 
		try:
			dict_parent['ID'] = parent_buffer_df['ID'][idx] 
		except:
			dict_parent['ID'] = None 
		try:
			dict_parent['parent_idx'] = idx
		except:
			dict_parent['parent_idx'] = None 
		try:
			dict_parent['cur_task_length'] = (cur_datetime - parent_buffer_df['PARENT_start_date'][idx]).days
		except:
			dict_parent['cur_task_length'] = None 
		try:
			dict_parent['planned_task_length'] = (parent_buffer_df['PARENT_completion_date_planned'][idx] - parent_buffer_df['PARENT_start_date'][idx]).days
		except:
			dict_parent['planned_task_length'] = None
		#try:
		#	dict_parent['planned_task_length_w_buffer'] = parent_buffer_df['PARENT_buffer_date_planned'][idx] - parent_buffer_df['PARENT_start_date'][idx] 
		#except:
		#	dict_parent['planned_task_length_w_buffer'] = None 
		try:
			dict_parent['day_delta'] = networkdays(parent_buffer_df['PARENT_completion_date_planned'][idx],cur_datetime,holidays)
		except:
			dict_parent['day_delta'] = None
		try:
			dict_parent['day_delta_program'] = networkdays(parent_buffer_df['program_start_date'][idx],cur_datetime,holidays)
		except:
			dict_parent['day_delta_program'] = None
		try:
			dict_parent['day_delta_hard_deadline'] = networkdays(parent_buffer_df['hard_deadline'][idx],cur_datetime,holidays)
		except:
			dict_parent['day_delta_hard_deadline'] = None
		try:
			dict_parent['day_delta_soft_deadline'] = networkdays(parent_buffer_df['soft_deadline'][idx],cur_datetime,holidays)
		except:
			dict_parent['day_delta_soft_deadline'] = None

	#dict_final['stage'] = ['final']*len(final_idx) 
	#dict_final['idx'] = final_idx

		ranking_list.append(dict_parent)			

	ranking_df = pd.DataFrame(ranking_list)

attask_input_df = pd.merge(buffer_df,ranking_df,'left',left_on='ID',right_on='ID')

TOTAL_project_status = map((lambda item: item['status']),TOTAL_project_list)
TOTAL_project_company = map((lambda item: item['company']),TOTAL_project_list)
TOTAL_project_unit = map((lambda item: item['unit']),TOTAL_project_list)

dict_TOTAL = {}
dict_TOTAL['company'] = TOTAL_project_company
dict_TOTAL['Nlibrary'] = TOTAL_project_unit
company_df = pd.DataFrame(dict_TOTAL)

#############################
# Perform Rankings
#############################
task_ranking(client,cur_datetime,update_rankings,timing_bin,company_df,attask_input_df)

execfile('email_attachment.py')



