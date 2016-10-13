#! /usr/bin/env python

import sys
import re 
import csv
from decimal import Decimal
import MySQLdb as mdb
import pandas as pd
from openpyxl import load_workbook
import time
import collections
from matplotlib.pyplot import *
import itertools

from benchmark_common_libs import *
from benchmark_create_mysql import *
from aer_libs import *

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',50)

cur_datetime = datetime.now()

DBNAME = "benchmark_prod"
READ_BENCHMARK_TABLE = "AER_GLOBAL_BENCHMARK"
READ_TABLE_LAUNCH = "TMP_PROGRAM_LAUNCH" 
READ_TABLE = "TMP_AER_DAILY_account"
WRITE_TABLE = "AER_PROGRAM_BENCHMARK_GS"
WRITE_TABLE_ROW = "AER_PROGRAM_BENCHMARK_GS_ROW"
program_length = 90 
min_percentage = '0.025'
max_percentage = '0.975'
BENCHMARK_TARGET = 0.20 

########################################
# Algorithm
# 1) Start at -21 days from program start date
# 2) Calculate cumsum of reach
# 3) Find the 2.5% and 97.5% points of the cumsum reach ... 2 sigma from normal distribution
# 4) If 2.5% is -1, remove ... date is incorrect
# 5) If 2.5% is > min_day + program_length, remove ... date is incorrect (>2 months AFTER inputted date)
# 6) Use 2.5% points to start PROGRAM curve for each account
# 7) Find MEDIAN and AVERAGE for all accounts for the given PROGRAM
# 8) This will give our daily benchmark
########################################

cmap = ['Blues', 'BuGn', 'BuPu',
                             'GnBu', 'Greens', 'Greys', 'Oranges', 'OrRd',
                             'PuBu', 'PuBuGn', 'PuRd', 'Purples', 'RdPu',
                             'Reds', 'YlGn', 'YlGnBu', 'YlOrBr', 'YlOrRd']

con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

############################
# Grab notes from Mysql DB  
############################
query = "SELECT * FROM %s.%s" % (DBNAME,READ_BENCHMARK_TABLE) 
benchmark_df = createDF_from_MYSQL_query(con,query)

query = "SELECT * FROM %s.%s" % (DBNAME,READ_TABLE) 
aer_df = createDF_from_MYSQL_query(con,query)

aer_df = create_BEE_bin(aer_df)
aer_df = create_Library_bin(aer_df)

corr_days_since_OE= range(0,program_length)
data_out = pd.DataFrame(corr_days_since_OE)
data_out = data_out.rename(columns={0:'days_since_OE'})

unique_accounts = list(set(aer_df['account_id']))
t0 = time.time()
calculated_program_start_date = []
i025 = []
i975 = []
for i in range(0,len(unique_accounts)):
	tmp_aer = aer_df[(aer_df['account_id'] == unique_accounts[i]) & (aer_df['days_since_OE'] >= 0) & (aer_df['days_since_OE'] < program_length)].reset_index()
	tmp_aer = tmp_aer.rename(columns={'index':'old_index'})

	####################################################################
	# Only keep accounts with valid program launch date
	####################################################################
	if (len(tmp_aer) > 0):
		date_list = [tmp_aer['program_launch_date'][0] + timedelta(days=x) for x in range(0,program_length)]

		## Find missing dates
		missing_dates = sorted(list(set(date_list).difference(tmp_aer['cur_date'])))
		df2 = pd.DataFrame() 
		for j in range(0,len(missing_dates)):
			df2 = df2.append(pd.DataFrame([[tmp_aer.ix[0]['old_index'],
											tmp_aer.ix[0]['BEEs'],
											tmp_aer.ix[0]['Nvideo'],
											tmp_aer.ix[0]['account_id'],
											tmp_aer.ix[0]['account_name'],
											missing_dates[j],
											str(missing_dates[j].month).zfill(2),
											missing_dates[j].year,
											(missing_dates[j] - tmp_aer.ix[0]['program_launch_date']).days,
											tmp_aer.ix[0]['industry_name'],
											tmp_aer.ix[0]['program_launch_date'],
											Decimal(0.0),
											tmp_aer.ix[0]['sfdc'],
											Decimal(0.0),
											tmp_aer.ix[0]['BEE_bin'],
											tmp_aer.ix[0]['Nvideo_bin'] ]] ))

		for j in range(0,len(tmp_aer.columns)):
			df2 = df2.rename(columns={j:tmp_aer.columns[j]})

		tmp_aer = tmp_aer.append(df2).sort_index(by=['cur_date'],ascending=True).reset_index(drop=True)			

		corr_reach_cumsum = [] 
		total = Decimal('0')
		for j in range(0,len(tmp_aer)):
			#printf("%3d ... %s\n",j,tmp_aer['reach'][j])
			try:
				total = total + tmp_aer['reach'][j]
				corr_reach_cumsum.append(total)
			except:
				corr_reach_cumsum.append(total)

			####################################################
			# Create reach profile for this customer 
			####################################################
		data_out[unique_accounts[i]] = corr_reach_cumsum

		printf("[aer_daily.py]: Account %4d of %4d ... G2 Account Id  = %5d .. Module Timing: %.3f sec\n", \
							i+1,len(unique_accounts),unique_accounts[i],time.time() - t0)		


data_out = data_out.drop('days_since_OE',1)

##########################################################################
# Output data ... AER webpage ... separate Account, GS, Industry by rows
##########################################################################
df_out = pd.DataFrame(list(data_out.columns))
df_out = df_out.rename(columns={0:'account_id'})
df_out['final_reach'] = pd.DataFrame(list(data_out.ix[program_length-1]))
df_out.to_csv('./output/OE_reach_final_day_ALL.csv')

##########################################################################
# 2) Add to mysql ... Gainsight ... New columns for Account, GS, Industry
##########################################################################
printf("[aer_daily.py] Output AER Account Curve Benchmarks\n")

account_id = []
account_name = []
days_since_OE_out = []
sfdc = []
program_date = []
category = []
industry = []
library = []
library_bin = []
company_size = []
company_size_bin = []
cum_reach = []
for i in range(0,len(data_out.columns)):
	cur_industry = aer_df['industry_name'][list(aer_df['account_id']).index(data_out.columns[i])]
	cur_Nvideo = aer_df['Nvideo'][list(aer_df['account_id']).index(data_out.columns[i])]
	cur_Nvideo_bin = aer_df['Nvideo_bin'][list(aer_df['account_id']).index(data_out.columns[i])]
	cur_BEE = aer_df['BEEs'][list(aer_df['account_id']).index(data_out.columns[i])]
	cur_BEE_bin = aer_df['BEE_bin'][list(aer_df['account_id']).index(data_out.columns[i])]
	cur_BEE_bin = aer_df['BEE_bin'][list(aer_df['account_id']).index(data_out.columns[i])]
	cur_sfdc = aer_df['sfdc'][list(aer_df['account_id']).index(data_out.columns[i])]
	cur_program_launch_date = aer_df['program_launch_date'][list(aer_df['account_id']).index(data_out.columns[i])]
	cur_account_name = aer_df['account_name'][list(aer_df['account_id']).index(data_out.columns[i])]
	for j in range(0,program_length):
		days_since_OE_out.append(j)
		account_id.append(data_out.columns[i])
		account_name.append(cur_account_name)
		sfdc.append(cur_sfdc)
		program_date.append(cur_program_launch_date + timedelta(days = j) )
		category.append('account')
		industry.append(cur_industry)
		library.append(cur_Nvideo)
		library_bin.append(cur_Nvideo_bin)
		company_size.append(cur_BEE)
		company_size_bin.append(cur_BEE_bin)
		if ( (cur_program_launch_date + timedelta(days = j)) <= datetime.now().date() ):
			cum_reach.append(data_out[data_out.columns[i]][j])
		else:
			cum_reach.append(0)

reach_output_rows = {}
reach_output_rows['days_since_OE'] = days_since_OE_out 
reach_output_rows['category'] = 'account' 
reach_output_rows['account_id'] = account_id 
reach_output_rows['account_name'] = account_name   
reach_output_rows['sfdc'] = sfdc 
reach_output_rows['date'] = program_date 
reach_output_rows['industry'] = industry 
reach_output_rows['Nvideo'] = library 
reach_output_rows['Nvideo_bin'] = library_bin 
reach_output_rows['BEEs'] = company_size 
reach_output_rows['BEE_bin'] = company_size_bin 
reach_output_rows['reach'] = cum_reach 

reach_output_rows_df = pd.DataFrame(reach_output_rows)

###############
# Benchmarks 
###############

## 1) Guidespark
printf("[aer_daily.py] Output Guidespark Reach Curve\n")
gs_reach_df = benchmark_df[(benchmark_df.benchmark == 'bm_guidespark')]
reach_output_rows_df = pd.merge(reach_output_rows_df,gs_reach_df[['program_day','value']],how='left',left_on='days_since_OE',right_on='program_day')
reach_output_rows_df = reach_output_rows_df.rename(columns={'value':'bm_guidespark'})
reach_output_rows_df = reach_output_rows_df.drop('program_day',1)

## 2) Industry Benchmark
printf("[aer_daily.py] Output Industry Reach Curves\n")
industry_reach_df = benchmark_df[(benchmark_df.benchmark == 'bm_industry')]
reach_output_rows_df = pd.merge(reach_output_rows_df,industry_reach_df[['program_day','benchmark_bin','value']],how='left',left_on=['industry','days_since_OE'],right_on=['benchmark_bin','program_day']).sort(['account_id','days_since_OE']).reset_index(drop=True)
reach_output_rows_df = reach_output_rows_df.rename(columns={'value':'bm_industry'})
reach_output_rows_df = reach_output_rows_df.drop('benchmark_bin',1)
reach_output_rows_df = reach_output_rows_df.drop('program_day',1)

## 3) BEE Benchmark
printf("[aer_daily.py] Output BEE Reach Curves\n")
bee_reach_df = benchmark_df[(benchmark_df.benchmark == 'bm_company_size')]
reach_output_rows_df = pd.merge(reach_output_rows_df,bee_reach_df[['program_day','benchmark_bin','value']],how='left',left_on=['BEE_bin','days_since_OE'],right_on=['benchmark_bin','program_day']).sort(['account_id','days_since_OE']).reset_index(drop=True)
reach_output_rows_df = reach_output_rows_df.rename(columns={'value':'bm_bee'})
reach_output_rows_df = reach_output_rows_df.drop('benchmark_bin',1)
reach_output_rows_df = reach_output_rows_df.drop('program_day',1)

## 4) Library Benchmark
printf("[aer_daily.py] Output Library Reach Curves\n")
Nvideo_reach_df = benchmark_df[(benchmark_df.benchmark == 'bm_library_size')]
reach_output_rows_df = pd.merge(reach_output_rows_df,Nvideo_reach_df[['program_day','benchmark_bin','value']],how='left',left_on=['Nvideo_bin','days_since_OE'],right_on=['benchmark_bin','program_day']).sort(['account_id','days_since_OE']).reset_index(drop=True)
reach_output_rows_df = reach_output_rows_df.rename(columns={'value':'bm_library_size'})
reach_output_rows_df = reach_output_rows_df.drop('benchmark_bin',1)
reach_output_rows_df = reach_output_rows_df.drop('program_day',1)

## Create trigger benchmark ... peg to 20%
bm_trigger = []
max_gs_benchmark = max(reach_output_rows_df['bm_guidespark'])
uplift_ratio = BENCHMARK_TARGET/max_gs_benchmark
for i in range(0,len(reach_output_rows_df)):
	bm_trigger.append(reach_output_rows_df['bm_guidespark'][i]*uplift_ratio)

reach_output_rows_df = pd.merge(reach_output_rows_df,pd.DataFrame(bm_trigger),how='left',left_index=True,right_index=True)
reach_output_rows_df = reach_output_rows_df.rename(columns={0:'bm_trigger'})

###########################
# Add percentiles for all
###########################
#gs_reach_df = create_percentile_bin(reach_output_rows_df,'days_since_OE','reach')
#reach_output_rows_df = reach_output_rows_df.join(pd.DataFrame(gs_reach_df))
#reach_output_rows_df = reach_output_rows_df.rename(columns={0:'gs_percentile'})
#
#industry_reach_df = create_percentile_bin_2D(reach_output_rows_df,'days_since_OE','industry','reach')
#reach_output_rows_df = reach_output_rows_df.join(pd.DataFrame(industry_reach_df))
#reach_output_rows_df = reach_output_rows_df.rename(columns={0:'industry_percentile'})
#
#bee_reach_df = create_percentile_bin_2D(reach_output_rows_df,'days_since_OE','BEE_bin','reach')
#reach_output_rows_df = reach_output_rows_df.join(pd.DataFrame(bee_reach_df))
#reach_output_rows_df = reach_output_rows_df.rename(columns={0:'bee_percentile'})

drop_mysql_table(con,DBNAME,WRITE_TABLE_ROW)
create_BENCHMARK_ROW_mysql_table(con,DBNAME,WRITE_TABLE_ROW)
insert_into_BENCHMARK_ROW_mysql_DB(con,DBNAME,WRITE_TABLE_ROW,reach_output_rows_df)

###########################
# Output to gainsight
###########################
reach_output_rows_GAINSIGHT_df = reach_output_rows_df
reach_output_rows_GAINSIGHT_df = reach_output_rows_GAINSIGHT_df.rename(columns={'days_since_OE':'program_day'})
reach_output_rows_GAINSIGHT_df = reach_output_rows_GAINSIGHT_df.rename(columns={'Nvideo':'library_size'})
reach_output_rows_GAINSIGHT_df = reach_output_rows_GAINSIGHT_df.rename(columns={'Nvideo_bin':'library_size_bin'})
reach_output_rows_GAINSIGHT_df = reach_output_rows_GAINSIGHT_df.rename(columns={'BEEs':'BEE'})
reach_output_rows_GAINSIGHT_df = reach_output_rows_GAINSIGHT_df.rename(columns={'reach':'account_reach'})
reach_output_rows_GAINSIGHT_df = reach_output_rows_GAINSIGHT_df.rename(columns={'bm_GS':'bm_guidespark'})
#reach_output_rows_GAINSIGHT_df = reach_output_rows_GAINSIGHT_df.rename(columns={'date':'cur_date'})
#reach_output_rows_GAINSIGHT_df = reach_output_rows_GAINSIGHT_df.rename(columns={'date':'cur_date'})
reach_output_rows_GAINSIGHT_df = reach_output_rows_GAINSIGHT_df.rename(columns={'account_name':'account'})
reach_output_rows_GAINSIGHT_df = reach_output_rows_GAINSIGHT_df.rename(columns={'bm_company_size':'bm_bee'})
reach_output_rows_GAINSIGHT_df['cur_date'] = [datetime.now().date() for x in reach_output_rows_GAINSIGHT_df.date]
reach_output_rows_GAINSIGHT_df['id'] = range(1,len(reach_output_rows_GAINSIGHT_df)+1)

header = ['id','cur_date','account', 'sfdc','library_size', 'library_size_bin', 'BEE', 'BEE_bin', \
           'program_day', 'date','industry','account_reach','bm_trigger','bm_guidespark', 'bm_industry', \
           'bm_library_size','bm_bee']

reach_output_rows_GAINSIGHT_df.to_csv('gainsight.csv',columns=header,index=False)


