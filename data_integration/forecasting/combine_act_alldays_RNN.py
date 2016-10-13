#! /usr/bin/env python

import sys
import pandas as pd 

sys.path.insert(0,'/home/analytics/analytics_sandbox/python_libs')

# Logging
import log_libs as log
LOG = log.init_logging()

##############################################
##############################################
##############################################
############### Main Program #################
##############################################
##############################################
##############################################

Nrecords = 0
for i in range(0,4):
	LOG.info('MERGE ALLDAY RNN FILES ... %3s of 4 ... ',i)
	sdata_tmp_df = pd.read_csv('./output/sdata_act_allday_RNN_' + str(i+1).zfill(2) + '.csv',index_col=[0])

	Nrecords = Nrecords + len(sdata_tmp_df)
	LOG.info('Records (Current File,Total) = ({:>6},{:>6})'.format(len(sdata_tmp_df),Nrecords))
	if (i == 0):
		sdata_df = sdata_tmp_df.copy(deep=True)
	else:
		sdata_df = sdata_df.append(pd.read_csv('./output/sdata_act_allday_RNN_' + str(i+1).zfill(2) + '.csv',index_col=[0]),ignore_index=True)

sdata_df = sdata_df.reset_index(drop=True)
sdata_df.to_csv('./output/sdata_act_allday_RNN.csv',encoding='utf-8')

