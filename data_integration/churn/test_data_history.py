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
from dateutil.relativedelta import relativedelta

sys.path.insert(0,'/home/analytics/analytics_sandbox/python_libs');
from common_libs import *
from create_mysql import *
from attask_libs import * 
from attask_api import StreamClient, ObjCode, AtTaskObject
from sfdc_libs import *
from inspect import currentframe, getframeinfo

# Logging
import log_libs as log
LOG = log.init_logging()

cur_datetime = datetime.now() - timedelta(hours = 8)

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',400)

con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

UPDATE_ATTASK_LINEITEM = False 
GET_ACTIVITY = True
OP_HISTORY = True 

STAGE_DURATION = True
CLOSE_DATE = True
DEAL_SIZE = True

start = time.time()

##############################################
##############################################
##############################################
############### Main Program #################
##############################################
##############################################
##############################################

old_data_df = pd.read_csv('/home/analytics/analytics_sandbox/data_integration/churn_BAK/outputBAK/sdata_op_history_RSF_20160830_ORIG.csv',index_col = [0])
#old_data_df = pd.read_csv('/home/analytics/analytics_sandbox/data_integration/churn/output/sdata_op_history_RSF_20160902_ORIG.csv',index_col = [0])
allday_data_df = pd.read_csv('/home/analytics/analytics_sandbox/data_integration/churn/output/sdata_allday_RNN.csv',index_col = [0])

old_data_MAX_df = old_data_df[(old_data_df['tstart'] == old_data_df['final_day'])].reset_index()

#compare_var = ['OpportunityType','Op_CreatedDate','final_day','won','lost','s1','s2','s3','s4','s5','stageback','close_change','close_push','close_pullin','amount_change','amount_up','amount_down','amount_per']  
compare_var = ['OpportunityType','won','lost','stageback','close_change','close_push','close_pullin','amount_change','amount_up','amount_down','amount_per']  

unique_op = list(set(old_data_MAX_df.OpportunityId))
Nop = 0
for i in range(0,len(unique_op)):

	if ((i % 500) == 499):
		LOG.info("DATA TEST: Unique Op ... {:>5} of {:>5}".format(i+1,len(unique_op)) )
	
	old_idx  = max(all_indices_CASE_SENSITIVE(unique_op[i],old_data_MAX_df['OpportunityId']))	
	cur_old_data_MAX_df = old_data_MAX_df[((old_data_MAX_df.index == old_idx) == True)].reset_index(drop=True)

	if ('Initial' in cur_old_data_MAX_df.ix[0]['OpportunityType'] or 'Upsell' in cur_old_data_MAX_df.ix[0]['OpportunityType']): 
		Nop = Nop + 1
		test_idx = all_indices_CASE_SENSITIVE(unique_op[i],allday_data_df['OpportunityId'])	
		test_allday_data_df = allday_data_df.ix[test_idx].reset_index(drop=True)

		cur_allday_data_df = test_allday_data_df[(test_allday_data_df['time'] == int(cur_old_data_MAX_df['tstop']))].reset_index(drop=True)

		for j in range(0,len(compare_var)):
			if (cur_allday_data_df.ix[0][compare_var[j]] != cur_old_data_MAX_df.ix[0][compare_var[j]]):
				LOG.info("(i,j) = ({:>5},{:>5}) . Nop = {} . {} . (won={},lost={}) . {} . {} != {}".format(i,j,Nop,unique_op[i],str(cur_allday_data_df.ix[0]['won']),
																							str(cur_allday_data_df.ix[0]['lost']),	
																							compare_var[j],str(cur_allday_data_df.ix[0][compare_var[j]]),
																							str(cur_old_data_MAX_df.ix[0][compare_var[j]]))) 	



#tmp_sdata_timeline_df = tmp_tdata_timeline_df.ix[tmp_sdata_timeline_MAXIDX_df['Nmax_idx']][SDATA_VARIABLES + MODEL_INDEPENDENT_VARIABLES].reset_index(drop=True)
#			for j in range(1,len(idx)):
#				cur_event = op_output_df.ix[idx[j]]['event']
#				#printf_NEW(sys._getframe(),"%5s ... ",idx[j])
#				cur_idx = op_output_df.ix[idx[j]]['op_index']
#				prev_idx = op_output_df.ix[idx[j-1]]['op_index']
#				if ((cur_event == 'created') | (cur_event == 'opportunityCreatedFromLead') | (cur_event == 'StageName')):
#					NEW_stagenum = str(op_output_df.ix[idx[j]]['NewValue'])[0]
#					OLD_stagenum = str(op_output_df.ix[idx[j]]['OldValue'])[0]
#					#printf_NEW(sys._getframe(),"In-Loop ... OLD,NEW = %1s,%1s\n",OLD_stagenum,NEW_stagenum)
#		
#					##################################################
#					# Update changing element / reset stage base date
#					##################################################
#					stage_time_df.loc[cur_idx,OLD_stagenum] = stage_time_df.ix[prev_idx][OLD_stagenum] \
#																				+ (op_output_df.ix[cur_idx]['CreatedDate'] - op_output_df.ix[prev_idx]['CreatedDate']).days
#					cur_stage_date = op_output_df.ix[cur_idx]['CreatedDate']
#					#stage_time_df.loc[cur_idx,op_output_df.ix[idx[j]]['OLD_stagenum']] = stage_time_df.ix[prev_idx][op_output_df.ix[idx[j]]['OLD_stagenum']] \
#					#															+ (op_output_df.ix[idx[j]]['CreatedDate'] - op_output_df.ix[idx[j-1]]['CreatedDate']).days
#					###############################
#					# Update other elements
#					###############################
#					cur_stage_cols = extra_val(stage_cols,OLD_stagenum)
#					stage_time_df.loc[op_output_df.ix[cur_idx]['op_index'],cur_stage_cols] = stage_time_df.ix[prev_idx][cur_stage_cols]
#	
#					###############################
#					# Update 'stageback' 
#					###############################
#					if (NEW_stagenum < OLD_stagenum):
#						Nstageback = Nstageback + 1
#						#stage_time_df.loc[cur_idx,NEW_stagenum] = stage_time_df.ix[prev_idx][NEW_stagenum]
#					stage_time_df.loc[op_output_df.ix[cur_idx]['op_index'],'stageback'] = Nstageback
#	
#					###############################
#					# Update 'won' & 'lost' 
#					###############################
#					if ('Closed Won' in str(op_output_df.ix[cur_idx]['NewValue'])):
#						stage_time_df.loc[op_output_df.ix[cur_idx]['op_index'],'won'] = 1
#					if ('Closed Lost' in str(op_output_df.ix[cur_idx]['NewValue'])):
#						stage_time_df.loc[op_output_df.ix[cur_idx]['op_index'],'lost'] = 1
##					if (op_output_df.ix[idx[j]]['NEW_stagenum'] == '6'):
##						stage_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'won'] = 1
##					if (op_output_df.ix[idx[j]]['NEW_stagenum'] == '7'):
##						stage_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'lost'] = 1
#
#				else:	
#					#printf_NEW(sys._getframe(),"No-Loop ... OLD,NEW = %1s,%1s\n",OLD_stagenum,NEW_stagenum)
#					if (NEW_stagenum >= OLD_stagenum):
#						if (stage_time_df.ix[prev_idx][NEW_stagenum] > 0):
#							stage_time_df.loc[cur_idx,NEW_stagenum] = stage_time_df.ix[prev_idx][NEW_stagenum] \
#																				+ (op_output_df.ix[idx[j]]['CreatedDate'] - cur_stage_date).days
#							#cur_stage_date = op_output_df.ix[cur_idx]['CreatedDate']
#						else:
#							stage_time_df.loc[cur_idx,NEW_stagenum] = (op_output_df.ix[cur_idx]['CreatedDate'] - cur_stage_date).days  
#					else:
#						stage_time_df.loc[cur_idx,NEW_stagenum] = stage_time_df.ix[prev_idx][NEW_stagenum] \
#																				+ (op_output_df.ix[idx[j]]['CreatedDate'] - cur_stage_date).days
#					cur_stage_date = op_output_df.ix[cur_idx]['CreatedDate']
#
#					###############################
#					# Update other elements
#					###############################
#					cur_stage_cols = extra_val(stage_cols,NEW_stagenum)
#					stage_time_df.loc[op_output_df.ix[cur_idx]['op_index'],cur_stage_cols] = stage_time_df.ix[prev_idx][cur_stage_cols]
#
#			final_stage.loc[all_indices(unique_op[i],final_stage['OpportunityId']),'final_stage'] = 's' + str(NEW_stagenum)
#	
#			#	if (cur_idx == 18583):
#			#		printf_NEW(sys._getframe(),"\n%s\n",stage_time_df.ix[idx][['1','2','3','4','5','6','7','stageback']])
#			#		sys.exit()
#	
#			
#	timeline_df = pd.merge(opportunity_history_df,stage_time_df,'left',left_index=True,right_index=True)
#
#	printf_NEW(sys._getframe(),"STAGE DURATION END   ... %.2f sec\n",time.time()-start)
#	
#	printf_NEW(sys._getframe(),"\n%s\n",timeline_df[['OpportunityId','CreatedDate','Op_CreatedDate','lifetime_day','event','NewValue','OldValue'] + stage_cols + ex_cols] \
#	                            [(timeline_df.OpportunityId == unique_op[0])])
#
################################
################################
## 2) Close Date 
################################
################################
#
#if (CLOSE_DATE == True):
#	op_output_df = opportunity_history_df[['OpportunityId','CreatedDate','Op_CreatedDate','lifetime_day','event','NewValue','OldValue']] \
#						[(opportunity_history_df.event == 'CloseDate')] \
#						.reset_index().rename(columns={'index':'op_index'})
#
#	op_output_df['NewValue'] = pd.to_datetime(op_output_df['NewValue'])
#	op_output_df['OldValue'] = pd.to_datetime(op_output_df['OldValue'])
#
#	close_cols = ['close_change','close_push','close_pullin']
#	close_time_df = pd.DataFrame(0,index=list(range(0,len(opportunity_history_df))),columns=(close_cols))
#
#	#unique_op = list(set(op_output_df.OpportunityId))
#	#unique_op = ['0065000000YpYTb']
#	#unique_op = ['0063800000anDZp']
#
#	for i in range(0,len(unique_op)):
#		if ((i % 500) == 499):
#			printf_NEW(sys._getframe(),"CLOSE DATE: Unique Op ... %5s of %5s ... %.2f sec\n",i+1,len(unique_op),time.time()-start)
#	
#		idx = all_indices_CASE_SENSITIVE(unique_op[i],op_output_df['OpportunityId'])
#
#		if (len(idx) > 0):
#			Nclose_change = 0
#			Nclose_push = 0
#			Nclose_pullin = 0
#			for j in range(0,len(idx)):
#				######################################
#				# Check if date is > or < than current
#				######################################
#				if (op_output_df.ix[idx[j]]['NewValue'] > op_output_df.ix[idx[j]]['OldValue']):
#					Nclose_change = Nclose_change + 1
#					Nclose_push = Nclose_push + 1
#				elif (op_output_df.ix[idx[j]]['NewValue'] < op_output_df.ix[idx[j]]['OldValue']):
#					Nclose_change = Nclose_change + 1
#					Nclose_pullin = Nclose_pullin + 1
#
#				close_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'close_change'] = Nclose_change
#				close_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'close_push']   = Nclose_push
#				close_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'close_pullin'] = Nclose_pullin
#	
#	timeline_df = pd.merge(timeline_df,close_time_df,'left',left_index=True,right_index=True)
#	
#	printf_NEW(sys._getframe(),"\n%s\n",timeline_df[['OpportunityId','CreatedDate','Op_CreatedDate','lifetime_day','event','NewValue','OldValue'] + close_cols] \
#								[(timeline_df.OpportunityId == unique_op[0]) & (timeline_df.event == 'CloseDate') ])
#
####################
####################
## 3) Amount 
####################
####################
#
#if (DEAL_SIZE == True):
#	op_output_df = opportunity_history_df[['OpportunityId','CreatedDate','Op_CreatedDate','lifetime_day','event','NewValue','OldValue']] \
#						[(opportunity_history_df.event == 'Amount')] \
#						.reset_index().rename(columns={'index':'op_index'})
#
#	amount_cols = ['amount_change','amount_up','amount_down','amount_per']
#	amount_time_df = pd.DataFrame(0,index=list(range(0,len(opportunity_history_df))),columns=(amount_cols))
#
#	#unique_op = list(set(op_output_df.OpportunityId))
#	#unique_op = ['0065000000YpYTb']
#	#unique_op = ['0063800000anDZp']
#
#	for i in range(0,len(unique_op)):
#		if ((i % 500) == 499):
#			printf_NEW(sys._getframe(),"DEAL SIZE: Unique Op ... %5s of %5s ... %.2f sec\n",i+1,len(unique_op),time.time()-start)
#	
#		idx = all_indices_CASE_SENSITIVE(unique_op[i],op_output_df['OpportunityId'])
#		if (len(idx) > 0):
#			Namount_change = 0
#			Namount_up = 0
#			Namount_down = 0
#			cur_lifetime_day = 0
#			cur_amount = 0
#			for j in range(0,len(idx)):
#				######################################
#				# Check if date is > or < than current
#				######################################
#				if (op_output_df.ix[idx[j]]['NewValue'] > op_output_df.ix[idx[j]]['OldValue']):
#					Namount_change = Namount_change + 1
#					Namount_up = Namount_up + 1
#				elif (op_output_df.ix[idx[j]]['NewValue'] < op_output_df.ix[idx[j]]['OldValue']):
#					Namount_change = Namount_change + 1
#					Namount_down = Namount_down + 1
#
#				amount_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'amount_change'] = Namount_change
#				amount_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'amount_up']   = Namount_up
#				amount_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'amount_down'] = Namount_down
#				if (op_output_df.ix[idx[j]]['lifetime_day'] != cur_lifetime_day): 
#					cur_lifetime_day = op_output_df.ix[idx[j]]['lifetime_day']
#					cur_amount = int(float(op_output_df.ix[idx[j]]['OldValue']))
#					if (int(float(op_output_df.ix[idx[j]]['OldValue'])) != 0): 
#						amount_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'amount_per'] = float(op_output_df.ix[idx[j]]['NewValue']) / float(op_output_df.ix[idx[j]]['OldValue']) 
#					else:
#						amount_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'amount_per'] = 0  
#				else:
#					if (cur_amount != 0): 
#						amount_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'amount_per'] = float(op_output_df.ix[idx[j]]['NewValue']) / cur_amount 
#					else:
#						amount_time_df.loc[op_output_df.ix[idx[j]]['op_index'],'amount_per'] = 0  
#	
#	timeline_df = pd.merge(timeline_df,amount_time_df,'left',left_index=True,right_index=True)
#	
#	printf_NEW(sys._getframe(),"\n%s\n",timeline_df[['OpportunityId','CreatedDate','Op_CreatedDate','lifetime_day','event','NewValue','OldValue'] + amount_cols] \
#								[(timeline_df.OpportunityId == unique_op[0]) & (timeline_df.event == 'Amount') ])
#
#######################################################
#######################################################
#######################################################
##################### Clean Data ######################  
#######################################################
#######################################################
#######################################################
#op_idx = min(all_indices_CASE_SENSITIVE('0065000000YpYTb',timeline_df['OpportunityId']))
#	
### STAGE DURATION 
#if (STAGE_DURATION == True):
#	#out_cols = stage_cols + ex_cols
#	out_cols = ex_cols
#	for i in range(0,len(unique_op)):
#
#		if ((i % 500) == 499):
#			printf_NEW(sys._getframe(),"STAGE_DURATION: Clean Data ... %5s of %5s ... %.2f sec\n",i+1,len(unique_op),time.time()-start)
#
#		Ncol_val = [0] * len(timeline_df)
#
#		idx = all_indices_CASE_SENSITIVE(unique_op[i],timeline_df['OpportunityId'])
#	
#		for j in range(0,len(idx)):
#			if (timeline_df.ix[idx[j]]['event'] == 'StageName'):
#				for k in range(0,len(out_cols)):
#					Ncol_val[k] = timeline_df.ix[idx[j]][out_cols[k]]
#
#			for k in range(0,len(out_cols)):
#				timeline_df.loc[idx[j],out_cols[k]] = Ncol_val[k]
#
#	printf_NEW(sys._getframe(),"\n%s\n",timeline_df[['OpportunityId','CreatedDate','Op_CreatedDate','lifetime_day','event','NewValue','OldValue'] + stage_cols + ex_cols] \
#								[(timeline_df.OpportunityId == unique_op[0])])
#
#printf_NEW(sys._getframe(),"Data Cleaned: SFDC Activities ... %.2f sec\n",time.time()-start)
#
#
### CLOSE_DATE
#if (CLOSE_DATE == True):
#	out_cols = close_cols
#	for i in range(0,len(unique_op)):
#
#		if ((i % 500) == 499):
#			printf_NEW(sys._getframe(),"CLOSE_DATE: Clean Data ... %5s of %5s ... %.2f sec\n",i+1,len(unique_op),time.time()-start)
#
#		Ncol_val = [0] * len(timeline_df)
#
#		idx = all_indices_CASE_SENSITIVE(unique_op[i],timeline_df['OpportunityId'])
#	
#		for j in range(0,len(idx)):
#			if (timeline_df.ix[idx[j]]['event'] == 'CloseDate'):
#				for k in range(0,len(out_cols)):
#					Ncol_val[k] = timeline_df.ix[idx[j]][out_cols[k]]
#
#			for k in range(0,len(out_cols)):
#				timeline_df.loc[idx[j],out_cols[k]] = Ncol_val[k]
#
#	printf_NEW(sys._getframe(),"\n%s\n",timeline_df[['OpportunityId','CreatedDate','Op_CreatedDate','lifetime_day','event','NewValue','OldValue'] + close_cols + ex_cols] \
#								[(timeline_df.OpportunityId == unique_op[0])])
#
#printf_NEW(sys._getframe(),"Data Cleaned: SFDC Activities ... %.2f sec\n",time.time()-start)
#
### DEAL_SIZE 
#if (DEAL_SIZE == True):
#	out_cols = amount_cols
#	for i in range(0,len(unique_op)):
#
#		if ((i % 500) == 499):
#			printf_NEW(sys._getframe(),"DEAL_SIZE: Clean Data ... %5s of %5s ... %.2f sec\n",i+1,len(unique_op),time.time()-start)
#
#		Ncol_val = [0] * len(timeline_df)
#
#		idx = all_indices_CASE_SENSITIVE(unique_op[i],timeline_df['OpportunityId'])
#	
#		for j in range(0,len(idx)):
#			if (timeline_df.ix[idx[j]]['event'] == 'Amount'):
#				for k in range(0,len(out_cols)):
#					Ncol_val[k] = timeline_df.ix[idx[j]][out_cols[k]]
#
#			for k in range(0,len(out_cols)):
#				timeline_df.loc[idx[j],out_cols[k]] = Ncol_val[k]
#
#	printf_NEW(sys._getframe(),"\n%s\n",timeline_df[['OpportunityId','CreatedDate','Op_CreatedDate','lifetime_day','event','NewValue','OldValue'] + amount_cols + ex_cols] \
#								[(timeline_df.OpportunityId == unique_op[0])])
#
#printf_NEW(sys._getframe(),"Data Cleaned: SFDC Activities ... %.2f sec\n",time.time()-start)
#
##################
## Add final day
##################
#final_day = []
#timeline_df['final_day'] = [None] * len(timeline_df)
#for i in range(0,len(unique_op)):
#
#	if ((i % 500) == 499):
#		printf_NEW(sys._getframe(),"Add Final Day ... %5s of %5s ... %.2f sec\n",i+1,len(unique_op),time.time()-start)
#
#	idx = all_indices_CASE_SENSITIVE(unique_op[i],timeline_df['OpportunityId'])
#	
#	base_day = timeline_df.ix[idx[0]]['Op_CreatedDate']
#	if (timeline_df.ix[idx[len(idx)-1]]['won'] != 0):
#		final_idx = max(all_indices_CASE_SENSITIVE(unique_op[i],timeline_df['OpportunityId']))
#		try:
#			timeline_df.loc[idx,'final_day'] = timeline_df.ix[final_idx]['lifetime_day']
#		except:
#			timeline_df.loc[idx,'final_day'] = None
#	elif ('Lost' in str(timeline_df.ix[idx[len(idx)-1]]['NewValue']) or 'Omitted' in str(timeline_df.ix[idx[len(idx)-1]]['NewValue']) ):
#		final_idx = max(all_indices_CASE_SENSITIVE(unique_op[i],timeline_df['OpportunityId']))
#		try:
#			timeline_df.loc[idx,'final_day'] = timeline_df.ix[final_idx]['lifetime_day']
#		except:
#			timeline_df.loc[idx,'final_day'] = None
#	else:   #### Make final day = the current day since they have not churned
#		try:
#			timeline_df.loc[idx,'final_day'] = (cur_datetime - base_day).days
#		except:
#			timeline_df.loc[idx,'final_day'] = None
#
##printf_NEW(sys._getframe(),"\n%s\n",timeline_df[['OpportunityId','CreatedDate','Op_CreatedDate','lifetime_day','final_day','event','NewValue','OldValue'] + stage_cols + close_cols + amount_cols + ex_cols] \
##						    [(timeline_df.OpportunityId == '0065000000YpYTb')])
#
#timeline_df.to_csv('./output/sfdc_opportunity_history_' + cur_datetime.strftime('%Y%m%d') + '.csv',encoding='utf-8')
#
#won_df = timeline_df[['OpportunityId','final_day','won']].groupby(['OpportunityId','final_day']).sum().reset_index()
#won_df['won'][won_df.won > 0] = 1
#lost_df = timeline_df[['OpportunityId','final_day','lost']].groupby(['OpportunityId','final_day']).sum().reset_index()
#lost_df['lost'][lost_df.lost > 0] = 1
#
#######################################################
#######################################################
#######################################################
#################### Output Data ######################  
#######################################################
#######################################################
#######################################################
#
#timeline_df = timeline_df.rename(columns={'1':'s1'})
#timeline_df = timeline_df.rename(columns={'2':'s2'})
#timeline_df = timeline_df.rename(columns={'3':'s3'})
#timeline_df = timeline_df.rename(columns={'4':'s4'})
#timeline_df = timeline_df.rename(columns={'5':'s5'})
#timeline_df = timeline_df.rename(columns={'6':'s6'})
#timeline_df = timeline_df.rename(columns={'7':'s7'})
#timeline_df = timeline_df.rename(columns={'8':'s8'})
#
#MODEL_INDEPENDENT_VARIABLES = []
#if (STAGE_DURATION  == True):
#	MODEL_INDEPENDENT_VARIABLES.append('s1')
#	MODEL_INDEPENDENT_VARIABLES.append('s2')
#	MODEL_INDEPENDENT_VARIABLES.append('s3')
#	MODEL_INDEPENDENT_VARIABLES.append('s4')
#	MODEL_INDEPENDENT_VARIABLES.append('s5')
#	MODEL_INDEPENDENT_VARIABLES.append('s6')
#	MODEL_INDEPENDENT_VARIABLES.append('s7')
#	MODEL_INDEPENDENT_VARIABLES.append('stageback')
#	MODEL_INDEPENDENT_VARIABLES.append('lost')
#
#if (CLOSE_DATE == True):
#	for i in range(0,len(close_cols)):
#		MODEL_INDEPENDENT_VARIABLES.append(close_cols[i])
#if (DEAL_SIZE == True):
#	for i in range(0,len(amount_cols)):
#		MODEL_INDEPENDENT_VARIABLES.append(amount_cols[i])
#
##ALL_VARIABLES = ['sfdc','won_int','won','lifetime_day','final_day','Cancellation_Notice_Received','event','op_type','video_id','video_title']
##TDATA_VARIABLES = ['OpportunityId','won','lifetime_day','final_day','event','OpportunityType','Product_Line__c','NaicsCode']
#TDATA_VARIABLES = ['OpportunityId','won','CreatedDate','Op_CreatedDate','lifetime_day','final_day','event','OpportunityType','Product_Line__c','NaicsCode','NumberOfEmployees','LeadSource','month_created']
#tdata_timeline_df = timeline_df[TDATA_VARIABLES + MODEL_INDEPENDENT_VARIABLES]
#
#### Do we need to set won library_size to PRIOR library_size
#SDATA_VARIABLES = TDATA_VARIABLES
##SDATA_VARIABLES.remove('won_int')
##SDATA_VARIABLES.remove('Cancellation_Notice_Received')
##SDATA_VARIABLES.remove('op_type')
##SDATA_VARIABLES.remove('video_id')
##SDATA_VARIABLES.remove('video_title')
#
####################################################
## Output for cox regression
####################################################
#sdata_timeline_df = []
#for i in range(0,len(unique_op)):
#
#	if ((i % 500) == 499):
#		printf_NEW(sys._getframe(),"Opportunity ... %5s of %5s ... sdata_timeline_df creation ... %.2f sec\n",i+1,len(unique_op),time.time()-start)
#
#	#####################################
#	# 1) Extract data from each account
#	#####################################
#	tmp_tdata_timeline_df = tdata_timeline_df.ix[all_indices_CASE_SENSITIVE(unique_op[i],tdata_timeline_df.OpportunityId)]
#	#tmp_tdata_timeline_df = tmp_tdata_timeline_df.drop(['video_id','video_title'],1)
#	#printf('%s\n\n',tmp_tdata_timeline_df)
#
#	################################################################################################################
#	# 2) Group records with the SAME lifetime_day
#	#	  Find the MAXIMUM INDEX (Nmax_idx) for the MAXIMUM RECORD for groups that have the same lifetime_day
#	################################################################################################################
#	tmp_sdata_timeline_MAXIDX_df = tmp_tdata_timeline_df[['OpportunityId','lifetime_day']].reset_index().groupby(['OpportunityId','lifetime_day'],as_index=False).agg({'index':max}).rename(columns={'index':'Nmax_idx'})
#	tmp_sdata_timeline_df = tmp_tdata_timeline_df.ix[tmp_sdata_timeline_MAXIDX_df['Nmax_idx']][SDATA_VARIABLES + MODEL_INDEPENDENT_VARIABLES].reset_index(drop=True)
#
#	########################################
#	# 3) Create tstart / tstop R framework 
#	########################################
#	if (len(tmp_sdata_timeline_df) > 0):
#		tmp_sdata_timeline_df.insert(3,'tstop',[None] * len(tmp_sdata_timeline_df))
#		for j in range(0,len(tmp_sdata_timeline_df)-1):
#			Nidx = tmp_sdata_timeline_df.index[j]
#			N1idx = tmp_sdata_timeline_df.index[j+1]
#			if (tmp_sdata_timeline_df.ix[N1idx]['lifetime_day'] < tmp_sdata_timeline_df.ix[N1idx]['final_day']):
#				tmp_sdata_timeline_df.loc[Nidx,'tstop'] = tmp_sdata_timeline_df.ix[N1idx]['lifetime_day']
#			else:
#				tmp_sdata_timeline_df.loc[Nidx,'tstop'] = tmp_sdata_timeline_df.ix[N1idx]['final_day']
#		cur_idx = tmp_sdata_timeline_df.index[len(tmp_sdata_timeline_df)-1]
#		tmp_sdata_timeline_df.loc[cur_idx,'tstop'] = tmp_sdata_timeline_df.ix[cur_idx]['final_day']
#
#		#########################################################
#		# 4) Grab value for all variables where index = Nmax_idx 
#		#########################################################
#		#tmp_sdata_timeline_df['Nvideo'] = [tmp_tdata_timeline_df.ix[x]['Nvideo'] for x in tmp_sdata_timeline_df['Nmax_idx']]
#		#tmp_sdata_timeline_df['Nview_total'] = [tmp_tdata_timeline_df.ix[x]['Nview_total'] for x in tmp_sdata_timeline_df['Nmax_idx']]
#		#tmp_sdata_timeline_df['library_completion_per'] = tmp_sdata_timeline_df['Nvideo'] / tmp_sdata_timeline_df['library_size']
#
#		#######################################################
#		# 1) Remove any records AFTER 'won' date (i.e. full cancellation)
#		# 2) Remove cancellation RECORD and add 'won=1' to previous record 
#		# 3) Correct where tstart = tstop
#		#######################################################
#		# 1) 
#		tmp_sdata_timeline_df = tmp_sdata_timeline_df[(tmp_sdata_timeline_df.lifetime_day <= tmp_sdata_timeline_df.tstop)].reset_index(drop=True)
#		# 2) 
#		#if (len(tmp_sdata_timeline_df[(tmp_sdata_timeline_df['won'] == 1)].index) == 1):
#		#   won_idx = tmp_sdata_timeline_df[(tmp_sdata_timeline_df['won'] == 1)].index[0]
#		#   tmp_sdata_timeline_df.loc[won_idx-1,'won'] = 1
#		#   tmp_sdata_timeline_df = tmp_sdata_timeline_df[(tmp_sdata_timeline_df.index != won_idx)]
#		#elif (len(tmp_sdata_timeline_df[(tmp_sdata_timeline_df['won'] == 1)].index) > 1):
#		#   printf_NEW(sys._getframe(),"[won_timeseries.py] %s\n",tmp_sdata_timeline_df)
#		# 3)
#
#		### output for cox regression (no lifetime_day = tstop)
#		tmp_sdata_timeline_COX_df = tmp_sdata_timeline_df.copy(deep=True)
#		tmp_sdata_timeline_COX_df = tmp_sdata_timeline_COX_df[(tmp_sdata_timeline_COX_df['lifetime_day'] != tmp_sdata_timeline_COX_df['tstop'])]
#
#		if (len(tmp_sdata_timeline_COX_df) > 0):
#			if (i==0):
#				sdata_timeline_COX_df = tmp_sdata_timeline_COX_df
#			else:
#				sdata_timeline_COX_df = sdata_timeline_COX_df.append(tmp_sdata_timeline_COX_df,ignore_index=True)
#
#		### output for random survival forest (lifetime_day = tstop ... OK ... gives the final values for stage lengths)
#		if (len(tmp_sdata_timeline_df) > 0):
#			if (i==0):
#				sdata_timeline_df = tmp_sdata_timeline_df
#			else:
#				sdata_timeline_df = sdata_timeline_df.append(tmp_sdata_timeline_df,ignore_index=True)
#
#### output for cox regression (no lifetime_day = tstop)
#sdata_timeline_COX_df = pd.merge(sdata_timeline_COX_df,won_df,'left',left_on=['OpportunityId','tstop'],right_on=['OpportunityId','final_day'])
#sdata_timeline_COX_df['won_x'][(sdata_timeline_COX_df.won_y == 1)] = 1
#sdata_timeline_COX_df = sdata_timeline_COX_df.drop({'won_y','final_day_y'},1).rename(columns={'won_x':'won','final_day_x':'final_day','lifetime_day':'tstart'})
#sdata_timeline_COX_df = pd.merge(sdata_timeline_COX_df,lost_df,'left',left_on=['OpportunityId','tstop'],right_on=['OpportunityId','final_day'])
#sdata_timeline_COX_df['lost_x'][(sdata_timeline_COX_df.lost_y == 1)] = 1
#sdata_timeline_COX_df = sdata_timeline_COX_df.drop({'lost_y','final_day_y'},1).rename(columns={'lost_x':'lost','final_day_x':'final_day','lifetime_day':'tstart'})
#sdata_timeline_COX_df.to_csv('./output/sdata_op_history_df_COX.csv')
#
##################################
## RSF ... add final information
##################################
#### 1) output for random survival forest (lifetime_day = tstop ... OK ... gives the final values for stage lengths)
#sdata_timeline_df = pd.merge(sdata_timeline_df,won_df,'left',left_on=['OpportunityId','tstop','lifetime_day'],right_on=['OpportunityId','final_day','final_day'])
#sdata_timeline_df['won_x'][(sdata_timeline_df.won_y == 1)] = 1
#sdata_timeline_df = sdata_timeline_df.drop({'won_y','final_day_y'},1).rename(columns={'won_x':'won','final_day_x':'final_day','lifetime_day':'tstart'})
#sdata_timeline_df = pd.merge(sdata_timeline_df,lost_df,'left',left_on=['OpportunityId','tstop','tstart'],right_on=['OpportunityId','final_day','final_day'])
#sdata_timeline_df['lost_x'][(sdata_timeline_df.lost_y == 1)] = 1
#sdata_timeline_df = sdata_timeline_df.drop({'lost_y','final_day_y'},1).rename(columns={'lost_x':'lost','final_day_x':'final_day','lifetime_day':'tstart'})
#
#### 2) Add final stage time for those that did not WIN or LOSE
#op_out = list(set(sdata_timeline_df.OpportunityId))
#for i in range(0,len(op_out)):
#
#	if ((i % 500) == 499):
#		printf_NEW(sys._getframe(),"Opportunity ... %5s of %5s ... RSF final record update ... %.2f sec\n",i+1,len(unique_op),time.time()-start)
#
#	sidx = all_indices_CASE_SENSITIVE(op_out[i],final_stage['OpportunityId'])
#	cur_stage = final_stage.ix[sidx[0]]['final_stage']  
#	idx = all_indices_CASE_SENSITIVE(op_out[i],sdata_timeline_df['OpportunityId'])
#	max_idx = max(idx)
#
#	if (cur_stage != 's8'):
#		if ((sdata_timeline_df.ix[max_idx]['won'] == 0) & (sdata_timeline_df.ix[max_idx]['lost'] == 0)):   
#			## Add new row
#			sdata_timeline_df = sdata_timeline_df.append(sdata_timeline_df.ix[max_idx],ignore_index=True)
#			## Get index
#			cur_idx = len(sdata_timeline_df)-1  
#	
#			## Update data 
#			sdata_timeline_df.loc[cur_idx,'tstart'] = int(sdata_timeline_df.ix[cur_idx]['final_day'])
#			sdata_timeline_df.loc[cur_idx,cur_stage] = int(sdata_timeline_df.ix[cur_idx][cur_stage] + sdata_timeline_df.ix[max_idx]['final_day'] - sdata_timeline_df.ix[max_idx]['tstart'])
#			sdata_timeline_df.loc[cur_idx,'CreatedDate'] = sdata_timeline_df.ix[cur_idx]['Op_CreatedDate'] + timedelta(days = int(sdata_timeline_df.ix[cur_idx]['final_day']))
#
#sdata_timeline_df = sdata_timeline_df.sort(['OpportunityId','CreatedDate']).reset_index(drop=True) 
#sdata_timeline_df.to_csv('./output/sdata_op_history_RSF_' + cur_datetime.strftime('%Y%m%d') + '_ORIG.csv',encoding='utf-8')
#sdata_timeline_df.to_csv('./output/sdata_op_history_RSF_' + cur_datetime.strftime('%Y%m%d') + '.csv',encoding='utf-8')
#sdata_timeline_df.to_csv('./output/sdata_op_history_RSF.csv',encoding='utf-8')
#
###############################################
###############################################
###############################################
#final_cols = ['s'+x for x in stage_cols] 
#final_cols = ['s'+x for x in stage_cols] 
#final_cols.remove('s0') 
#final_cols.remove('s8') 
#printf_NEW(sys._getframe(),"\n%s\n",sdata_timeline_df[['OpportunityId','CreatedDate','Op_CreatedDate','tstart','tstop','final_day','event'] + final_cols + ex_cols] \
#						        [(sdata_timeline_df.OpportunityId == unique_op[0])])
#
