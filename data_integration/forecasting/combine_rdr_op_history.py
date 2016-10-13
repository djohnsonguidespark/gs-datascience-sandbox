#! /usr/bin/env python

################################################
#
# Written by DKJ ... 9/9/16
#
# Program will export sfdc activities for all
# accounts that have ever had an opportunity
#
################################################
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
from datetime import datetime,timedelta

sys.path.insert(0,'/home/analytics/analytics_sandbox/python_libs');
import common_libs as cm
import create_mysql as mys 
import sfdc_libs as sfdc
from inspect import currentframe, getframeinfo

# Logging
import log_libs as log
LOG = log.init_logging()

cur_datetime = datetime.now() - timedelta(hours = 8) 
execfile('/home/analytics/analytics_sandbox/python_libs/stuff.py')

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',400)

con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

CREATE_NEW_SUMMARY_DATA = False
GET_ACTIVITY = False 
COMBINE_RECORDS = False
FIND_MTG = True
FIND_CALL = True
FIND_EMAIL = True
FIND_STAGE = True
DEBUG = False  # True --> This is for a single / group of unique Opportunity Ids

start = time.time()

def find_percentage(df,X,Y,name):

	value_per = []
	for iii in range(0,len(df)):
		try:
			value_per.append(float(df.ix[iii][X]) / float(df.ix[iii][Y]) ) 
		except:
			value_per.append(None) 

	df = df.join(pd.DataFrame(value_per))
	df = df.rename(columns={0:name})
	return df


##############################################
##############################################
##############################################
############### Main Program #################
##############################################
##############################################
##############################################

LOG.info("Load SFDC RDR Activity Info ... {:.2f} sec".format(time.time()-start))
rdr_timeline_df = pd.read_csv('./output/sdata_rdr_history_RSF.csv',index_col=[0])
rdr_timeline_df['Act_CreatedDate'] = pd.to_datetime(rdr_timeline_df['Act_CreatedDate'])
rdr_timeline_df['CreatedDate'] = pd.to_datetime(rdr_timeline_df['CreatedDate'])
rdr_timeline_COX_df = pd.read_csv('./output/sdata_rdr_history_COX.csv',index_col=[0])
rdr_timeline_COX_df['Act_CreatedDate'] = pd.to_datetime(rdr_timeline_COX_df['Act_CreatedDate'])
rdr_timeline_COX_df['CreatedDate'] = pd.to_datetime(rdr_timeline_COX_df['CreatedDate'])

LOG.info("Load SFDC Opportunity Activity Info ... {:.2f} sec".format(time.time()-start))
op_timeline_df = pd.read_csv('./output/sdata_op_history_RSF.csv',index_col=[0])
op_timeline_df['Op_CreatedDate'] = pd.to_datetime(op_timeline_df['Op_CreatedDate'])
op_timeline_df['CreatedDate'] = pd.to_datetime(op_timeline_df['CreatedDate'])
op_timeline_COX_df = pd.read_csv('./output/sdata_op_history_COX.csv',index_col=[0])
op_timeline_COX_df['Op_CreatedDate'] = pd.to_datetime(op_timeline_COX_df['Op_CreatedDate'])
op_timeline_COX_df['CreatedDate'] = pd.to_datetime(op_timeline_COX_df['CreatedDate'])

## Keep Initial Sale only
op_timeline_df = op_timeline_df[op_timeline_df['OpportunityType'].str.contains('Initial') == True].reset_index(drop=True)	
op_timeline_COX_df = op_timeline_COX_df[op_timeline_COX_df['OpportunityType'].str.contains('Initial') == True].reset_index(drop=True)	

LOG.info("DATA INPUT COMPLETE ... {:.2f} sec".format(time.time()-start))

######################################################
######################################################
######################################################
############## Combine / Output Data #################  
######################################################
######################################################
######################################################

#########################################
# Combine rdr and op info 
#########################################
all_timeline_df = rdr_timeline_df.append(op_timeline_df,ignore_index=True).drop('Act_CreatedDate',1).drop('NaicsCode',1).reset_index(drop=True)

#########################################
# Calculate lifetime day 
#########################################
account_first_df = rdr_timeline_df[['AccountId_18','CreatedDate']].groupby('AccountId_18').first().reset_index().rename(columns={'CreatedDate':'Act_CreatedDate'})
all_timeline_df = pd.merge(all_timeline_df,account_first_df[['AccountId_18','Act_CreatedDate']],'left',left_on='AccountId_18',right_on='AccountId_18')
all_timeline_df['lifetime_day'] = (all_timeline_df['CreatedDate'] - all_timeline_df['Act_CreatedDate']).astype('timedelta64[D]')

all_timeline_df = all_timeline_df.sort(['AccountId_18','CreatedDate']).reset_index(drop=True)

all_timeline_df = all_timeline_df[pd.notnull(all_timeline_df['lifetime_day']) == True].reset_index(drop=True)

####################################
# Remove accounts where data was
# clearly deleted from activities 
# 
# Laeeq .. prior to 2015
####################################
unique_account = cm.extra_val(list(set(all_timeline_df['AccountId_18'])),list(set(all_timeline_df[all_timeline_df.lifetime_day<0]['AccountId_18'])))
all_timeline_df = all_timeline_df[all_timeline_df.AccountId_18.isin(unique_account)].reset_index(drop=True)

#########################################
# Update tstart and tstop 
#########################################
all_timeline_df['tstop'] = (all_timeline_df['lifetime_day'] + all_timeline_df['tstop'] - all_timeline_df['tstart']).astype(int)
all_timeline_df['tstart'] = all_timeline_df['lifetime_day']

#########################################
# Update NAICS code 
#########################################
naics_df = op_timeline_df[['AccountId_18','NaicsCode']].groupby('AccountId_18').first().reset_index()
all_timeline_df = pd.merge(all_timeline_df,naics_df[['AccountId_18','NaicsCode']],'left',left_on='AccountId_18',right_on='AccountId_18')

######################################################
######################################################
######################################################
#################### Clean Data ######################  
######################################################
######################################################
######################################################

#unique_account = unique_account[0:200]
#unique_account = ['0013800001696CZAAY']
#unique_account = ['0015000000fAn7OAAS']
#unique_account = ['0015000000y8ZAUAA2']
#unique_account = ['0015000000styjuAAA']
#unique_account = ['0015000000qt5aTAAQ']
#unique_account = ['0015000000rQgFUAA0']
#unique_account = ['00138000015y2EXAAY','0015000000qt5aTAAQ']
#unique_account = ['00138000015y2EXAAY']
#unique_account = ['0015000000gU7XzAAK','00138000015y2EXAAY','0015000000qt5aTAAQ']
#unique_account = ['0015000000rwJS9AAM']

##################################################################
# Filter all days after won
# NOTE: keep lost opportunity accounts b/c they could come back
##################################################################
act_won_df = all_timeline_df[['AccountId_18','CreatedDate']][(all_timeline_df['won'] == 1)]

for iii in range(0,len(unique_account)):

	if ((iii % 500) == 499):
		LOG.info("FILTER AFTER WON/LOSS: Clean Data ... {:>5} of {:>5} ... {:.2f} sec".format(iii+1,len(unique_account),time.time()-start) )

	# Find all indices for given account
	idx = cm.all_indices_CASE_SENSITIVE(unique_account[iii],all_timeline_df['AccountId_18'])

	# Remove all those after won/lost 
	won_idx = act_won_df[act_won_df.AccountId_18 == unique_account[iii]].index
	if (len(won_idx) > 0):
		remove_idx = [x for x in idx if x > min(won_idx)]
		all_timeline_df = all_timeline_df.drop(remove_idx)

all_timeline_df = all_timeline_df.reset_index(drop=True)

LOG.info("FILTER AFTER WON/LOSS completed ... {:.2f} sec".format(time.time()-start) )

#####################################
# Fill in the blanks 
#####################################

ex_cols = ['won','lost']
op_created_cols = ['Nop_created','Nop_lost']
stage_cols = ['s1','s2','s3','s4','s5','s6','s7']
close_cols = ['close_change','close_push','close_pullin','stageback']
amount_cols = ['amount_change','amount_up','amount_down','amount_per']
mtg_cols = ['Nmtg_total','Nmtg_cancel_total','Nmtg_noshow_total','Nmtg_completed_total']
email_cols = ['Nemail_total','Ncontact_customer','Ncontact_guidespark','Nfillform_total','Nfillform_good_total','Nfillform_bad_total']
call_cols = ['Ncall_total']

#########################################
# Filter out data after 'won' == 1 
#########################################
all_timeline_df['won'] = all_timeline_df['won'].fillna(0)
all_timeline_df['lost'] = all_timeline_df['lost'].fillna(0)

#act_won_date_df = all_timeline_df[['AccountId_18','CreatedDate','won']][all_timeline_df['won'] == 1].groupby(['AccountId_18']).first()
#act_won_date_df = all_timeline_df[['AccountId_18','CreatedDate','won']][all_timeline_df['won'] == 1]

#########################################
# Op Creation / Lost 
# Check if opportunity was created / lost 
#########################################

LOG.info("OP CREATION / LOSS Update ... {:.2f} sec".format(time.time()-start) )

all_timeline_PRE_df = all_timeline_df.copy(deep=True)

# op created
merge_cols = ['AccountId_18','OpportunityId','CreatedDate']

## OLD WAY
#op_created_date_df = all_timeline_df[['OpportunityId','CreatedDate']][(all_timeline_df['event'] == 'created') | (all_timeline_df['event'] == 'opportunityCreatedFromLead')]
#op_created_date_df = pd.merge(op_created_date_df,pd.DataFrame([1]*len(all_timeline_df)).rename(columns={0:'is_op_created'}),'left',left_index=True,right_index=True)
#all_timeline_OLD_df = pd.merge(all_timeline_df,op_created_date_df[['is_op_created']],'left',left_index=True,right_index=True)

op_created_date_df = op_timeline_df[merge_cols].drop_duplicates().reset_index(drop=True)
op_created_date_df = pd.merge(op_created_date_df,pd.DataFrame([1]*len(op_created_date_df)).rename(columns={0:'is_op_created'}),'left',left_index=True,right_index=True)
all_timeline_df = pd.merge(all_timeline_df,op_created_date_df[merge_cols + ['is_op_created']],'left',left_on=merge_cols,right_on=merge_cols)

# op lost
## OLD WAY
#act_lost_df = all_timeline_df[['AccountId_18','OpportunityId','CreatedDate']][(all_timeline_df['lost'] == 1)]
#act_lost_df = pd.merge(act_lost_df,pd.DataFrame([1]*len(all_timeline_df)).rename(columns={0:'is_op_lost'}),'left',left_index=True,right_index=True)
#all_timeline_df = pd.merge(all_timeline_df,act_lost_df[['is_op_lost']],'left',left_index=True,right_index=True)

act_lost_df = op_timeline_df[['AccountId_18','OpportunityId','CreatedDate']][(op_timeline_df['lost'] == 1)]
act_lost_df = pd.merge(act_lost_df,pd.DataFrame([1]*len(act_lost_df)).rename(columns={0:'is_op_lost'}),'left',left_index=True,right_index=True)
all_timeline_df = pd.merge(all_timeline_df,act_lost_df[merge_cols + ['is_op_lost']],'left',left_on=merge_cols,right_on=merge_cols)

## fillna
all_timeline_df['is_op_created'] = all_timeline_df['is_op_created'].fillna(0)
all_timeline_df['is_op_lost'] = all_timeline_df['is_op_lost'].fillna(0)

## Fill in the blanks
out_cols = op_created_cols

## Calculate Nop_created and Nop_lost
all_timeline_df = all_timeline_df.reset_index().rename(columns={"index":"act_index"})
test_df = all_timeline_df[['AccountId_18','act_index','is_op_lost']].set_index(["AccountId_18","act_index"])
test_cumsum_df = test_df.groupby(level=[0,1]).sum().groupby(level=[0]).cumsum().reset_index().rename(columns={'is_op_lost':'Nop_lost'})
all_timeline_df = pd.merge(all_timeline_df,test_cumsum_df[['act_index','Nop_lost']],'left',left_on='act_index',right_on='act_index')

test_df = all_timeline_df[['AccountId_18','act_index','is_op_created']].set_index(["AccountId_18","act_index"])
test_cumsum_df = test_df.groupby(level=[0,1]).sum().groupby(level=[0]).cumsum().reset_index().rename(columns={'is_op_created':'Nop_created'})
all_timeline_df = pd.merge(all_timeline_df,test_cumsum_df[['act_index','Nop_created']],'left',left_on='act_index',right_on='act_index')

#all_timeline_df = all_timeline_df.drop('act_index',1)

LOG.debug("\n{}".format(all_timeline_df[['AccountId_18','OpportunityId','CreatedDate','Act_CreatedDate','lifetime_day','won','lost','event'] + out_cols] \
							[(all_timeline_df['AccountId_18'] == unique_account[0])]) )

LOG.info("OP CREATION / LOSS Update completed... {:.2f} sec".format(time.time()-start) )

#########################################
# Stage Duration 
# 1) Set all initial fields to 0
# 2) Find all missing data in the middle (Use dates to input the proper time)
#########################################

if (FIND_STAGE == True):
	## STAGE DURATION 
	out_cols = stage_cols
	#out_cols = ['s2']

	all_timeline_df = all_timeline_df[all_timeline_df['AccountId_18'].isin(unique_account)]	
	for kkk in range(0,len(out_cols)):
		LOG.info('STAGE DURATION: {:>3} of {:>3} ... {} ... {:.2f} sec'.format(kkk+1,len(out_cols),out_cols[kkk],time.time()-start) )
		not_idx  = all_timeline_df[pd.notnull(all_timeline_df[out_cols[kkk]]) == True].index
		null_idx = all_timeline_df[pd.isnull(all_timeline_df[out_cols[kkk]]) == True].index

		tmp_df = all_timeline_df.ix[not_idx][['AccountId_18','won','lost',out_cols[kkk]]].reset_index().rename(columns={'index':'Findex'})

		################################################################################
		## Check which AccountId_18 have NO values (i.e they do not exist in tmp_df)
		## Use fillna(0) to set all values
		################################################################################
		null_act = cm.extra_val(list(set(all_timeline_df.AccountId_18)),list(set(tmp_df.AccountId_18)) )
		all_timeline_df.loc[all_timeline_df[all_timeline_df['AccountId_18'].isin(null_act) == True].index,out_cols[kkk]] = 0

		###############################################################
		## Reset to account for AccountId_18 that have been set to zero 
		###############################################################
		not_idx  = all_timeline_df[pd.notnull(all_timeline_df[out_cols[kkk]]) == True].index
		null_idx = all_timeline_df[pd.isnull(all_timeline_df[out_cols[kkk]]) == True].index

		tmp_df = all_timeline_df.ix[not_idx][['AccountId_18','won','lost',out_cols[kkk]]].reset_index().rename(columns={'index':'Findex'})

		###########################################
		## Fill is all other AccountId_18 values 
		###########################################
		all_timeline_df = pd.merge(all_timeline_df.reset_index(),tmp_df[['AccountId_18','Findex']],'left',left_on=['AccountId_18','act_index'],right_on=['AccountId_18','Findex']).set_index('index')

		all_timeline_df['Bindex'] = all_timeline_df['Findex']
		all_timeline_df['Bindex'] = all_timeline_df.groupby(['AccountId_18'])['Bindex'].transform(lambda grp: grp.fillna(method='ffill'))
		all_timeline_df['Bindex'] = all_timeline_df.groupby(['AccountId_18'])['Bindex'].transform(lambda grp: grp.fillna(method='bfill'))
		all_timeline_df['Findex'] = all_timeline_df.groupby(['AccountId_18'])['Findex'].transform(lambda grp: grp.fillna(method='bfill'))
		all_timeline_df['Findex'] = all_timeline_df.groupby(['AccountId_18'])['Findex'].transform(lambda grp: grp.fillna(method='ffill'))
		all_timeline_df['Bindex'] = all_timeline_df['Bindex'].astype(int)
		all_timeline_df['Findex'] = all_timeline_df['Findex'].astype(int)

		all_timeline_df[out_cols[kkk] + '_Bval'] = [int(all_timeline_df.ix[x][out_cols[kkk]]) for x in all_timeline_df['Bindex']]
		all_timeline_df[out_cols[kkk] + '_Fval'] = [int(all_timeline_df.ix[x][out_cols[kkk]]) for x in all_timeline_df['Findex']]

		## Update all values
		all_timeline_df[out_cols[kkk]] = [all_timeline_df.ix[x][out_cols[kkk] + '_Bval'] if all_timeline_df.ix[x][out_cols[kkk] + '_Bval'] >= all_timeline_df.ix[x][out_cols[kkk] + '_Fval'] \
													else (all_timeline_df.ix[x][out_cols[kkk] + '_Bval'] + int((all_timeline_df.ix[x]['CreatedDate'] - all_timeline_df.ix[all_timeline_df.ix[x]['Bindex']]['CreatedDate']).days)) \
														for x in all_timeline_df.index]

		#all_timeline_df[out_cols[kkk] + '_test'] = [all_timeline_df.ix[x][out_cols[kkk] + '_Bval'] if all_timeline_df.ix[x][out_cols[kkk] + '_Bval'] >= all_timeline_df.ix[x][out_cols[kkk] + '_Fval'] \
		#												else (all_timeline_df.ix[x][out_cols[kkk] + '_Bval'] + int((all_timeline_df.ix[x]['CreatedDate'] - all_timeline_df.ix[all_timeline_df.ix[x]['Bindex']]['CreatedDate']).days)) \
		#													for x in all_timeline_df.index]

#		for iii in range(0,len(all_timeline_df)):
#			x = all_timeline_df.index[iii]
#			if (all_timeline_df.ix[x][out_cols[kkk] + '_Bval'] == all_timeline_df.ix[x][out_cols[kkk] + '_Fval']):	
#				LOG.info('1: {:>3} {}'.format(x,all_timeline_df.ix[x][out_cols[kkk] + '_Bval'])) 
#			else:	
#				LOG.info('2: {:>3} {}'.format(x,all_timeline_df.ix[x][out_cols[kkk] + '_Bval'] + int((all_timeline_df.ix[x]['CreatedDate'] - all_timeline_df.ix[all_timeline_df.ix[x]['Bindex']]['CreatedDate']).days)) ) 

		#LOG.info('3\n{}'.format(all_timeline_df[['act_index','Bindex','Findex','AccountId_18','won','lost','lifetime_day',out_cols[kkk],out_cols[kkk] + '_Bval',out_cols[kkk] + '_Fval']]) )

		### Remove dummy columns for the next script
		all_timeline_df = all_timeline_df.drop(['Findex','Bindex',out_cols[kkk] + '_Bval',out_cols[kkk] + '_Fval' ],1)

all_timeline_df = all_timeline_df.reset_index(drop=True)

LOG.info('{:.2f} sec'.format(time.time()-start) )
	
#if (FIND_STAGE == True):
#
#	## STAGE DURATION 
#	out_cols = stage_cols
#
#	LOG.debug("\n{}".format(all_timeline_df[['AccountId_18','OpportunityId','CreatedDate','Act_CreatedDate','lifetime_day','won','lost','event'] + out_cols] \
#							[(all_timeline_df['AccountId_18'] == unique_account[0])]) )
#
#	for iii in range(0,len(unique_account)):
#
#		#if ((iii % 100) == 99):
#		LOG.info("STAGE_DURATION: Clean Data ... {:>5} of {:>5} ... {:.2f} sec".format(iii+1,len(unique_account),time.time()-start) )
#
#		# Find all indices for given account
#		idx = cm.all_indices_CASE_SENSITIVE(unique_account[iii],all_timeline_df['AccountId_18'])
#
#		# Find all NULL indices for given account
#		null_idx =  list(all_timeline_df[(all_timeline_df['AccountId_18'] == unique_account[iii]) & (pd.isnull(all_timeline_df[out_cols[0]]) == True)].index)
#
#		# Find first NON-NULL idx
#		extra_idx = sorted(cm.extra_val(idx,null_idx)) 
#
#		LOG.debug("STEP 1: ... {:.2f} sec".format(time.time()-start) )
#		if (len(extra_idx) > 0):
#			null_idx1 = [x for x in null_idx if x <= extra_idx[0]]
#			null_idx2 = [x for x in null_idx if x > extra_idx[0]]
#
#			LOG.debug("STEP 1a: ... {:.2f} sec".format(time.time()-start) )
#			###############################################
#			# 1) Set all initial fields to 0
#			###############################################
#			for jjj in range(0,len(null_idx1)):
#				for kkk in range(0,len(out_cols)):
#					all_timeline_df.loc[null_idx1[jjj],out_cols[kkk]] = 0
#
#			##########################################################################
#			# 2) Find all missing data in the middle / end
#			# a) check if the value is different between upstream / downstream index
#			# b) if same, equal upstream
#			# c) if different, need to calculate the difference
#			##########################################################################
#			LOG.debug("STEP 2: ... {:.2f} sec".format(time.time()-start) )
#			for jjj in range(0,len(null_idx2)):
#
#				tmp_idx = [x for x in idx if x >= null_idx2[jjj]]	
#				remaining_idx = len(null_idx2) - jjj
#
#				if (len(cm.extra_val(tmp_idx,null_idx2[jjj:len(null_idx2)])) > 0):
#
#					# Find index before and after that is NOT NULL
#					start_idx = max([x for x in idx if x < null_idx2[jjj] and pd.isnull(all_timeline_df.ix[x][out_cols[0]]) == False ])
#					end_idx = [x for x in idx if x > null_idx2[jjj] and pd.isnull(all_timeline_df.ix[x][out_cols[0]]) == False ]
#					if (len(end_idx) > 0):
#						end_idx   = min(end_idx)
#	
#						for kkk in range(0,len(out_cols)):
#
#							# if equal, no change for that stage
#							if (all_timeline_df.ix[start_idx][out_cols[kkk]] == all_timeline_df.ix[end_idx][out_cols[kkk]]): 
#								LOG.debug("STEP 3_1: {} ... {:.2f} sec".format(null_idx2[jjj],time.time()-start) )
#								all_timeline_df.loc[null_idx2[jjj],out_cols[kkk]] = all_timeline_df.ix[start_idx][out_cols[kkk]] 
#								LOG.debug("STEP 3_2: {} ... {:.2f} sec".format(null_idx2[jjj],time.time()-start) )
#							else:
#								LOG.debug("STEP 4_1: {} ... {:.2f} sec".format(null_idx2[jjj],time.time()-start) )
#								all_timeline_df.loc[null_idx2[jjj],out_cols[kkk]] = int(all_timeline_df.ix[start_idx][out_cols[kkk]]) \
#															+ int((all_timeline_df.ix[null_idx2[jjj]]['CreatedDate'] - all_timeline_df.ix[start_idx]['CreatedDate']).days)
#								LOG.debug("STEP 4_2: {} ... {:.2f} sec".format(null_idx2[jjj],time.time()-start) )
#		else:			
#
#			LOG.debug("STEP 1b: ... {:.2f} sec".format(time.time()-start) )
#			###############################################
#			# 1) Set all initial fields to 0
#			###############################################
#			for jjj in range(0,len(idx)):
#				for kkk in range(0,len(out_cols)):
#					all_timeline_df.loc[idx[jjj],out_cols[kkk]] = 0
#
LOG.debug("\n{}".format(all_timeline_df[['AccountId_18','OpportunityId','CreatedDate','Act_CreatedDate','lifetime_day','won','lost','event'] + out_cols] \
							[(all_timeline_df['AccountId_18'] == unique_account[0])]) )

########################################
# Op History Fields
# 1) Find all missing data in the middle
# 2) Set all initial fields to 0
#########################################
out_cols = amount_cols + close_cols

#out_cols.remove('close_push')

for iii in range(0,len(out_cols)):
	tmp_df = all_timeline_df.groupby(['AccountId_18'])[out_cols[iii]].transform(lambda grp: grp.fillna(method='ffill'))
	all_timeline_df[out_cols[iii]] = tmp_df.tolist()
	#all_timeline_df[out_cols[iii]] = all_timeline_df[out_cols[iii]].fillna(0)
	tmp_df = all_timeline_df.groupby(['AccountId_18'])[out_cols[iii]].transform(lambda grp: grp.fillna(method='bfill'))
	all_timeline_df[out_cols[iii]] = tmp_df.tolist()
	#all_timeline_df[out_cols[iii]] = all_timeline_df[out_cols[iii]].fillna(0)

out_cols = amount_cols + close_cols
LOG.debug("\n{}".format(all_timeline_df[['AccountId_18','OpportunityId','CreatedDate','Act_CreatedDate','lifetime_day','won','lost','event'] + out_cols] \
							[(all_timeline_df['AccountId_18'] == unique_account[0])]) )

#########################
## MTG / EMAIL / CALL 
#########################
out_cols = mtg_cols + email_cols + call_cols
#out_cols.remove('Nfillform_total')
for iii in range(0,len(out_cols)):
	tmp_df = all_timeline_df.groupby(['AccountId_18'])[out_cols[iii]].transform(lambda grp: grp.fillna(method='ffill'))
	all_timeline_df[out_cols[iii]] = tmp_df.tolist()
	tmp_df = all_timeline_df.groupby(['AccountId_18'])[out_cols[iii]].transform(lambda grp: grp.fillna(method='bfill'))
	all_timeline_df[out_cols[iii]] = tmp_df.tolist()

LOG.debug("\n{}".format(all_timeline_df[['AccountId_18','OpportunityId','CreatedDate','Act_CreatedDate','lifetime_day','won','lost','event'] + out_cols] \
								[(all_timeline_df['AccountId_18'] == unique_account[0])]) )


LOG.info("Data Cleaned: MTG / EMAIL / CALL Activities ... {:.2f} sec".format(time.time()-start))

#################
# Add final day
#################
final_day = []
all_timeline_df['final_day'] = [None] * len(all_timeline_df)
for i in range(0,len(unique_account)):

	if ((i % 500) == 499):
		LOG.info("Add Final Day ... {:>5} of {:>5} ... {:.2f} sec".format(i+1,len(unique_account),time.time()-start) )

	idx = cm.all_indices_CASE_SENSITIVE(unique_account[i],all_timeline_df['AccountId_18'])

	base_day = all_timeline_df.ix[idx[0]]['Act_CreatedDate']
	if (all_timeline_df.ix[idx[len(idx)-1]]['won'] != 0):
		final_idx = max(cm.all_indices_CASE_SENSITIVE(unique_account[i],all_timeline_df['AccountId_18']))
		try:
			all_timeline_df.loc[idx,'final_day'] = all_timeline_df.ix[final_idx]['lifetime_day']
		except:
			all_timeline_df.loc[idx,'final_day'] = None
	elif (all_timeline_df.ix[idx[len(idx)-1]]['lost'] != 0):
		final_idx = max(cm.all_indices_CASE_SENSITIVE(unique_account[i],all_timeline_df['AccountId_18']))
		try:
			all_timeline_df.loc[idx,'final_day'] = all_timeline_df.ix[final_idx]['lifetime_day']
		except:
			all_timeline_df.loc[idx,'final_day'] = None
	else:   #### Make final day = the current day since they have not churned
		try:
			all_timeline_df.loc[idx,'final_day'] = (cur_datetime - base_day).days
		except:
			all_timeline_df.loc[idx,'final_day'] = None

##########################
# Find won/lost accounts
#
# Critical for bookkeeping
##########################
won_df  = all_timeline_df[['AccountId_18','lifetime_day','won']][all_timeline_df['won'] == 1]
lost_df = all_timeline_df[['AccountId_18','lifetime_day','lost']][all_timeline_df['lost'] == 1]

#################
# Add final day
#################
LOG.debug("\n{}".format(all_timeline_df[['AccountId_18','OpportunityId','CreatedDate','Act_CreatedDate','lifetime_day','final_day','won','lost','event']] \
							[(all_timeline_df['AccountId_18'] == unique_account[0])]) )

TDATA_VARIABLES = ['AccountId_18','CreatedDate','Act_CreatedDate','lifetime_day','final_day','event'] + ex_cols
ACT_VARIABLES = ['NaicsCode','NumberOfEmployees']

MODEL_INDEPENDENT_VARIABLES = []
if (FIND_EMAIL  == True):
	MODEL_INDEPENDENT_VARIABLES = MODEL_INDEPENDENT_VARIABLES + email_cols

if (FIND_CALL == True):
	MODEL_INDEPENDENT_VARIABLES = MODEL_INDEPENDENT_VARIABLES + call_cols

if (FIND_MTG == True):
	MODEL_INDEPENDENT_VARIABLES = MODEL_INDEPENDENT_VARIABLES + mtg_cols

if (FIND_STAGE == True):
	MODEL_INDEPENDENT_VARIABLES = MODEL_INDEPENDENT_VARIABLES + stage_cols

MODEL_INDEPENDENT_VARIABLES = MODEL_INDEPENDENT_VARIABLES + op_created_cols
MODEL_INDEPENDENT_VARIABLES = MODEL_INDEPENDENT_VARIABLES + close_cols
MODEL_INDEPENDENT_VARIABLES = MODEL_INDEPENDENT_VARIABLES + amount_cols

timeline_df = all_timeline_df[TDATA_VARIABLES + ACT_VARIABLES + MODEL_INDEPENDENT_VARIABLES].copy(deep=True)

###################
# Add all variables 
###################
tdata_timeline_df = timeline_df[TDATA_VARIABLES + ACT_VARIABLES + MODEL_INDEPENDENT_VARIABLES]

#### Do we need to set won library_size to PRIOR library_size
SDATA_VARIABLES = TDATA_VARIABLES + ACT_VARIABLES

###################################################
# Output for cox regression
###################################################
sdata_timeline_df = []
for i in range(0,len(unique_account)):

	if ((i % 500) == 499):
		LOG.info("Account ... {:>5} of {:>5} ... sdata_timeline_df creation ... {:.2f} sec".format(i+1,len(unique_account),time.time()-start))

	#####################################
	# 1) Extract data from each account
	#####################################
	tmp_tdata_timeline_df = tdata_timeline_df.ix[cm.all_indices_CASE_SENSITIVE(unique_account[i],tdata_timeline_df['AccountId_18'])]

	################################################################################################################
	# 2) Group records with the SAME lifetime_day
	#	 Find the MAXIMUM INDEX (Nmax_idx) for the MAXIMUM RECORD for groups that have the same lifetime_day
	################################################################################################################
	tmp_sdata_timeline_MAXIDX_df = tmp_tdata_timeline_df[['AccountId_18','lifetime_day']].reset_index().groupby(['AccountId_18','lifetime_day'],as_index=False).agg({'index':max}).rename(columns={'index':'Nmax_idx'})
	tmp_sdata_timeline_df = tmp_tdata_timeline_df.ix[tmp_sdata_timeline_MAXIDX_df['Nmax_idx']][SDATA_VARIABLES + MODEL_INDEPENDENT_VARIABLES].reset_index(drop=True)

	########################################
	# 3) Create tstart / tstop R framework 
	########################################
	if (len(tmp_sdata_timeline_df) > 0):
		tmp_sdata_timeline_df.insert(3,'tstop',[None] * len(tmp_sdata_timeline_df))
		for j in range(0,len(tmp_sdata_timeline_df)-1):
			Nidx = tmp_sdata_timeline_df.index[j]
			N1idx = tmp_sdata_timeline_df.index[j+1]
			if (tmp_sdata_timeline_df.ix[N1idx]['lifetime_day'] < tmp_sdata_timeline_df.ix[N1idx]['final_day']):
				tmp_sdata_timeline_df.loc[Nidx,'tstop'] = tmp_sdata_timeline_df.ix[N1idx]['lifetime_day']
			else:
				tmp_sdata_timeline_df.loc[Nidx,'tstop'] = tmp_sdata_timeline_df.ix[N1idx]['final_day']
		cur_idx = tmp_sdata_timeline_df.index[len(tmp_sdata_timeline_df)-1]
		tmp_sdata_timeline_df.loc[cur_idx,'tstop'] = tmp_sdata_timeline_df.ix[cur_idx]['final_day']

		#########################################################
		# 4) Grab value for all variables where index = Nmax_idx 
		#########################################################
		#tmp_sdata_timeline_df['Nvideo'] = [tmp_tdata_timeline_df.ix[x]['Nvideo'] for x in tmp_sdata_timeline_df['Nmax_idx']]
		#tmp_sdata_timeline_df['Nview_total'] = [tmp_tdata_timeline_df.ix[x]['Nview_total'] for x in tmp_sdata_timeline_df['Nmax_idx']]
		#tmp_sdata_timeline_df['library_completion_per'] = tmp_sdata_timeline_df['Nvideo'] / tmp_sdata_timeline_df['library_size']

		#######################################################
		# 1) Remove any records AFTER 'won' date (i.e. full cancellation)
		# 2) Remove cancellation RECORD and add 'won=1' to previous record 
		# 3) Correct where tstart = tstop
		#######################################################
		# 1) 
		tmp_sdata_timeline_df = tmp_sdata_timeline_df[(tmp_sdata_timeline_df.lifetime_day <= tmp_sdata_timeline_df.tstop)].reset_index(drop=True)
		# 2) 
		#if (len(tmp_sdata_timeline_df[(tmp_sdata_timeline_df['won'] == 1)].index) == 1):
		#   won_idx = tmp_sdata_timeline_df[(tmp_sdata_timeline_df['won'] == 1)].index[0]
		#   tmp_sdata_timeline_df.loc[won_idx-1,'won'] = 1
		#   tmp_sdata_timeline_df = tmp_sdata_timeline_df[(tmp_sdata_timeline_df.index != won_idx)]
		#elif (len(tmp_sdata_timeline_df[(tmp_sdata_timeline_df['won'] == 1)].index) > 1):
		#   printf_NEW(sys._getframe(),"[won_timeseries.py] %s\n",tmp_sdata_timeline_df)
		# 3)

		### output for cox regression (no lifetime_day = tstop)
		tmp_sdata_timeline_COX_df = tmp_sdata_timeline_df.copy(deep=True)
		tmp_sdata_timeline_COX_df = tmp_sdata_timeline_COX_df[(tmp_sdata_timeline_COX_df['lifetime_day'] != tmp_sdata_timeline_COX_df['tstop'])]

		if (len(tmp_sdata_timeline_COX_df) > 0):
			if (i==0):
				sdata_timeline_COX_df = tmp_sdata_timeline_COX_df
			else:
				sdata_timeline_COX_df = sdata_timeline_COX_df.append(tmp_sdata_timeline_COX_df,ignore_index=True)

		### output for random survival forest (lifetime_day = tstop ... OK ... gives the final values for stage lengths)
		if (len(tmp_sdata_timeline_df) > 0):
			if (i==0):
				sdata_timeline_df = tmp_sdata_timeline_df
			else:
				sdata_timeline_df = sdata_timeline_df.append(tmp_sdata_timeline_df,ignore_index=True)

### output for cox regression (no lifetime_day = tstop)
#sdata_timeline_COX_df = pd.merge(sdata_timeline_COX_df,won_df,'left',left_on=['AccountId_18','tstop'],right_on=['AccountId_18','final_day'])
#sdata_timeline_COX_df['won_x'][(sdata_timeline_COX_df.won_y == 1)] = 1
#sdata_timeline_COX_df = sdata_timeline_COX_df.drop({'won_y','final_day_y'},1).rename(columns={'won_x':'won','final_day_x':'final_day','lifetime_day':'tstart'})
#sdata_timeline_COX_df = pd.merge(sdata_timeline_COX_df,lost_df,'left',left_on=['AccountId_18','tstop'],right_on=['AccountId_18','final_day'])
#sdata_timeline_COX_df['lost_x'][(sdata_timeline_COX_df.lost_y == 1)] = 1
#sdata_timeline_COX_df = sdata_timeline_COX_df.drop({'lost_y','final_day_y'},1).rename(columns={'lost_x':'lost','final_day_x':'final_day','lifetime_day':'tstart'})
#sdata_timeline_COX_df.to_csv('./output/sdata_op_history_df_COX.csv')
#

##################################
## RSF ... add final information
##################################
### 1) output for random survival forest (lifetime_day = tstop ... OK ... gives the final values for stage lengths)
if (len(won_df) > 0):
	sdata_timeline_df = pd.merge(sdata_timeline_df,won_df,'left',left_on=['AccountId_18','lifetime_day'],right_on=['AccountId_18','lifetime_day'])
	sdata_timeline_df['won_x'][(sdata_timeline_df.won_y == 1)] = 1
	sdata_timeline_df = sdata_timeline_df.drop({'won_y'},1).rename(columns={'won_x':'won'})
if (len(lost_df) > 0):
	sdata_timeline_df = pd.merge(sdata_timeline_df,lost_df,'left',left_on=['AccountId_18','lifetime_day'],right_on=['AccountId_18','lifetime_day'])
	sdata_timeline_df['lost_x'][(sdata_timeline_df.lost_y == 1)] = 1
	sdata_timeline_df = sdata_timeline_df.drop({'lost_y'},1).rename(columns={'lost_x':'lost'})

##############################
## Remove any duplicate rows
##############################
sdata_timeline_df = sdata_timeline_df.drop_duplicates()

sdata_timeline_COX_df = sdata_timeline_COX_df.rename(columns={'lifetime_day':'tstart'})
sdata_timeline_COX_df.to_csv('./output/sdata_all_history_COX_df.csv')
sdata_timeline_df = sdata_timeline_df.rename(columns={'lifetime_day':'tstart'})
sdata_timeline_df.to_csv('./output/sdata_all_history_RSF.csv')
sdata_timeline_df.to_csv('./output/sdata_all_history_RSF_' + cur_datetime.strftime('%Y%m%d') + '.csv',encoding='utf-8')
sdata_timeline_df.to_csv('./output/sdata_all_history_RSF_' + cur_datetime.strftime('%Y%m%d') + '_ORIG.csv',encoding='utf-8')

#### 2) Add final stage time for those that did not WIN or LOSE
#op_out = list(set(sdata_timeline_df.OpportunityId))
#for i in range(0,len(op_out)):
#
#	if ((i % 500) == 499):
#		printf_NEW(sys._getframe(),"Opportunity ... %5s of %5s ... RSF final record update ... %.2f sec\n",i+1,len(unique_account),time.time()-start)
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
#if (DEBUG == False):
#	sdata_timeline_df.to_csv('./output/sdata_op_history_RSF_' + cur_datetime.strftime('%Y%m%d') + '_ORIG.csv',encoding='utf-8')
#	sdata_timeline_df.to_csv('./output/sdata_op_history_RSF_' + cur_datetime.strftime('%Y%m%d') + '.csv',encoding='utf-8')
#	sdata_timeline_df.to_csv('./output/sdata_op_history_RSF.csv',encoding='utf-8')
#else:
#	sdata_timeline_df.to_csv('./output/sdata_op_history_RSF_' + cur_datetime.strftime('%Y%m%d') + '_TEST.csv',encoding='utf-8')
#
###############################################
###############################################
###############################################
#final_cols = ['s'+x for x in stage_cols]
#final_cols = ['s'+x for x in stage_cols]
#final_cols.remove('s0')
#final_cols.remove('s8')
#printf_NEW(sys._getframe(),"\n%s\n",sdata_timeline_df[['OpportunityId','CreatedDate','Op_CreatedDate','tstart','tstop','final_day','event'] + final_cols + ex_cols] \
#								[(sdata_timeline_df.OpportunityId == unique_account[0])])
#
#
