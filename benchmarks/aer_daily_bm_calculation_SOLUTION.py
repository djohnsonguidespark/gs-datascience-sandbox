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

try:
	if (sys.argv[1] < 2014):
		printf("[aer_daily_bm_calculation_SOLUTION.py]: Wrong input year ... must be >2014 ... your input = %s\n",sys.argv[1])
		sys.exit()
	else:
		printf("[aer_daily_bm_calculation_SOLUTION.py]: Input year ... %s\n",sys.argv[1])
		CUR_YEAR = sys.argv[1]
except:
	printf("[aer_daily_bm_calculation_SOLUTION.py]: No input year ... set to 2015\n")
	CUR_YEAR = "2015"

DBNAME = "benchmark_prod"
READ_TABLE_LAUNCH = "TMP_PROGRAM_LAUNCH" 
READ_TABLE_DATE_RANGE = 'TMP_TARGET_AUDIENCE_date_range'
READ_TABLE = "TMP_AER_DAILY_account_solution"
WRITE_BENCHMARK_TABLE = "AER_GLOBAL_BENCHMARK_" + CUR_YEAR
WRITE_TABLE_ROW = "AER_PROGRAM_BENCHMARK_GS_SOLUTION_ROW_BM" + CUR_YEAR

ACCOUNT_REACH_DAY = 30

min_day = -21
program_length = 90 
min_percentage = '0.025'
max_percentage = '0.975'
reach_threshold = 0.01   ### What the min cumsum reach needs to be to include in the calculation

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

search_days = abs(2*min_day)
max_day = 182 # 6 months 

t0 = time.time()

##########################
# CMAPS
##########################
#cmaps = [('Sequential',     ['Blues', 'BuGn', 'BuPu',
#                             'GnBu', 'Greens', 'Greys', 'Oranges', 'OrRd',
#                             'PuBu', 'PuBuGn', 'PuRd', 'Purples', 'RdPu',
#                             'Reds', 'YlGn', 'YlGnBu', 'YlOrBr', 'YlOrRd']),
#         ('Sequential (2)', ['afmhot', 'autumn', 'bone', 'cool', 'copper',
#                             'gist_heat', 'gray', 'hot', 'pink',
#                             'spring', 'summer', 'winter']),
#         ('Diverging',      ['BrBG', 'bwr', 'coolwarm', 'PiYG', 'PRGn', 'PuOr',
#                             'RdBu', 'RdGy', 'RdYlBu', 'RdYlGn', 'Spectral',
#                             'seismic']),
#         ('Qualitative',    ['Accent', 'Dark2', 'Paired', 'Pastel1',
#                             'Pastel2', 'Set1', 'Set2', 'Set3']),
#         ('Miscellaneous',  ['gist_earth', 'terrain', 'ocean', 'gist_stern',
#                             'brg', 'CMRmap', 'cubehelix',
#                             'gnuplot', 'gnuplot2', 'gist_ncar',
#                             'nipy_spectral', 'jet', 'rainbow',
#                             'gist_rainbow', 'hsv', 'flag', 'prism'])]

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
READ_DATA = True

if (READ_DATA == True):
	if (CUR_YEAR == '2014'):
		query = "SELECT * FROM %s.%s WHERE program_launch_year = '%s'" % (DBNAME,READ_TABLE_LAUNCH,CUR_YEAR) 
	else:
		query = "SELECT * FROM %s.%s WHERE program_launch_year = '%s' AND YEAR(program_launch_date) >= %s" % (DBNAME,READ_TABLE_LAUNCH,CUR_YEAR,int(CUR_YEAR)) 
	program_launch_df = pd.read_sql(query,con)
	program_launch_df = program_launch_df.rename(columns = {'program_launch_date':'program_launch_date_' + CUR_YEAR})

	query = "SELECT * FROM %s.%s" % (DBNAME,READ_TABLE_DATE_RANGE) 
	video_date_range_df = pd.read_sql(query,con)

	query = "SELECT * FROM %s.%s WHERE solution_name IS NOT NULL" % (DBNAME,READ_TABLE) 
	aer_df = pd.read_sql(query,con)

	printf("[aer_daily_bm_calculation_SOLUTION.py]: Query Complete . %.2f sec\n",time.time()-t0)

	aer_df = create_BEE_bin(aer_df)
	aer_df = create_Library_bin(aer_df)

	aer_df = pd.merge(aer_df,program_launch_df[['g2_account_id','program_launch_date_' + CUR_YEAR]],how='right',left_on='account_id',right_on=['g2_account_id']).sort(['account_id','solution_name','cur_date'])
	aer_df = aer_df[(aer_df['account_id'].notnull() == True)]	
	aer_df['account_id'] = aer_df['account_id'].astype('int')

	days_since_OE_2014 = []
	for i in range(0,len(aer_df)):
		try:
			days_since_OE_2014.append((aer_df.ix[i]['cur_date'] - aer_df.ix[i]['program_launch_date_' + CUR_YEAR]).days)
		except Exception as e:
			#printf("[aer_daily_bm_calculation_SOLUTION.py]: %4s ... Line %s: %s\n",i,sys.exc_traceback.tb_lineno,e)
			days_since_OE_2014.append(None)

	aer_df['days_since_OE_' + CUR_YEAR] = days_since_OE_2014

	aer_df = aer_df.rename(columns={'days_since_OE':'cur_days_since_OE'})
	aer_df = aer_df.rename(columns={'days_since_OE_' + CUR_YEAR:'days_since_OE'})
	aer_df = aer_df.rename(columns={'program_launch_date':'cur_program_launch_date'})
	aer_df = aer_df.rename(columns={'program_launch_date_' + CUR_YEAR:'program_launch_date'})

	printf("[aer_daily_bm_calculation_SOLUTION.py]: Data Merge Complete . %.2f sec\n",time.time()-t0)

corr_days_since_OE= range(0,program_length)
data_out = pd.DataFrame(corr_days_since_OE).rename(columns={0:'days_since_OE'})

solution_out = [] 
industry_out = [] 
BEE_bin_out = [] 
Nvideo_bin_out = []
TA_out = []

unique_accounts = list(set(aer_df['account_id']))
unique_solutions = list(set(aer_df['solution_name']))
calculated_program_start_date = []
i025 = []
i975 = []
for i in range(0,len(unique_accounts)):
	for k in range(0,len(unique_solutions)):
		tmp_aer = aer_df[(aer_df['account_id'] == unique_accounts[i]) & (aer_df['solution_name'] == unique_solutions[k]) & (aer_df['days_since_OE'] >= min_day) & (aer_df['days_since_OE'] < max_day)].reset_index()
		tmp_aer = tmp_aer.rename(columns={'index':'old_index'})
		tmp_aer['reach'] = tmp_aer['reach'].fillna(0)		

		####################################################################
		# Only keep accounts with valid program launch date
		####################################################################
		if (len(tmp_aer) > 0):
			date_list = [tmp_aer['program_launch_date'][0] + timedelta(days=min_day) + timedelta(days=x) for x in range(0,max_day - min_day)]
	
			## Find missing dates
			missing_dates = sorted(list(set(date_list).difference(tmp_aer['cur_date'])))
			df2 = pd.DataFrame() 
			for j in range(0,len(missing_dates)):
				df2 = df2.append(pd.DataFrame([[tmp_aer.ix[0]['old_index'],
												tmp_aer.ix[0]['account_id'],
												tmp_aer.ix[0]['account_name'],
												tmp_aer.ix[0]['solution_name'],
												tmp_aer.ix[0]['subdomain'],
												tmp_aer.ix[0]['sfdc'],
												tmp_aer.ix[0]['industry_name'],
												tmp_aer.ix[0]['program_launch_date'],
												(missing_dates[j] - tmp_aer.ix[0]['program_launch_date']).days,
												missing_dates[j],
												str(missing_dates[j].month).zfill(2),
												missing_dates[j].year,
												tmp_aer.ix[0]['Nvideo'],
												tmp_aer.ix[0]['BEEs'],
												Decimal(0.0),
												Decimal(0.0),
												tmp_aer.ix[0]['BEE_bin'],
												tmp_aer.ix[0]['Nvideo_bin'] ]] ))
	
			for j in range(0,len(tmp_aer.columns)):
				df2 = df2.rename(columns={j:tmp_aer.columns[j]})
	
			tmp_aer = tmp_aer.append(df2).sort(['cur_date'],ascending=True).reset_index(drop=True)			
			tmp_aer_new = tmp_aer[['old_index','cur_date','industry_name','days_since_OE','cur_days_since_OE','reach','unique_viewers','BEEs']]
		
			reach_cumsum = [] 
			total = Decimal('0')
			for j in range(0,len(tmp_aer['days_since_OE'])):
				#printf("%3d ... %s\n",j,tmp_aer_new['reach'][j])
				try:
					total = total + Decimal(tmp_aer_new['reach'][j])
				except Exception as e:
					printf("[aer_daily_bm_calculation_SOLUTION.py]: %4s ... Line %s: %s\n",j,sys.exc_traceback.tb_lineno,e)
				reach_cumsum.append(total)
	
			####################################################
			# Find bottom 2.5% and top 2.5% from reach_cumsum
			####################################################
			day_min_cumsum = max(reach_cumsum)*Decimal(min_percentage)	
			day_max_cumsum = max(reach_cumsum)*Decimal(max_percentage)	
	
			cur_i025 = len([x for x in reach_cumsum if x <= day_min_cumsum])-1
			i025.append(cur_i025)
				
			i975.append(len([x for x in reach_cumsum if x <= day_max_cumsum])-1)
	
			#########################################################
			# Calculate cumsum from 2.5% ... to program_length
			#########################################################
			if (cur_i025 >= 0 & cur_i025 <= (min_day + search_days)):
				corr_reach_cumsum = []
				corr_total = 0
				#####################################
				## Find computed program start date
				#####################################
				try:
					calculated_program_start_date.append(tmp_aer['cur_date'][cur_i025])
				except:
					calculated_program_start_date.append("")

				######################################################################
				# Find target audience	
				# 1) All videos viewed during cur_i025 and cur_i025+program_length
				######################################################################	
				cur_video_TA_df = video_date_range_df[(video_date_range_df['account_id'] == unique_accounts[i]) & (video_date_range_df['solution_name'] == unique_solutions[k]) \
								& ( ((video_date_range_df['min_time'] >= min(tmp_aer_new['cur_date'])) & (video_date_range_df['min_time'] <= max(tmp_aer_new['cur_date'])) ) \
								  | ((video_date_range_df['max_time'] >= min(tmp_aer_new['cur_date'])) & (video_date_range_df['max_time'] <= max(tmp_aer_new['cur_date'])) ) \
								  | ((video_date_range_df['min_time'] <= min(tmp_aer_new['cur_date'])) & (video_date_range_df['max_time'] >= max(tmp_aer_new['cur_date'])) ) )]

				cur_TA = float(mean(cur_video_TA_df['target_audience']))
				TA_out.append(cur_TA)

				#TODO .... add TA_out to output

				############################
				# Find cumulative reach	
				############################	
				for j in range(cur_i025,cur_i025 + program_length):
					try:
						corr_total = corr_total + float(tmp_aer_new.ix[j]['unique_viewers']) / cur_TA
						corr_reach_cumsum.append(corr_total)
					except:
						corr_reach_cumsum.append(corr_total)
					#printf("%3d ... %.4f,%.4f\n",j,tmp_aer_new['reach'][j],corr_reach_cumsum[j-cur_i025])
	
				####################################################
				# Create reach profile for this customer 
				####################################################
				data_out[str(unique_accounts[i]) + '_' + tmp_aer['solution_name'][0]] = corr_reach_cumsum
				industry_out.append(tmp_aer['industry_name'][0])
				BEE_bin_out.append(tmp_aer['BEE_bin'][0])
				Nvideo_bin_out.append(tmp_aer['Nvideo_bin'][0])
	
			printf("[aer_daily_bm_calculation_SOLUTION.py]: Account %4d of %4d ... G2 Account Id = %5d . Solution = %25s .  Module Timing: %.2f sec\n", \
															i+1,len(unique_accounts),unique_accounts[i],unique_solutions[k],time.time() - t0)		
	
data_out = data_out.drop('days_since_OE',1)
i025_group = [(g[0], len(list(g[1]))) for g in itertools.groupby(sorted(i025))]
i975_group = [(g[0], len(list(g[1]))) for g in itertools.groupby(sorted(i975))]

#####################################
# Remove data where reach < threshold
#####################################
remove_column_idx = []
for i in range(0,len(data_out.columns)):
	if (data_out.ix[program_length-1][data_out.columns[i]] < reach_threshold):
		remove_column_idx.append(i)	

#remove_column_idx.sort(reverse=True)
data_out_filter = data_out
data_out_filter = data_out_filter.drop(data_out_filter.columns[remove_column_idx],axis=1)
TA_out_filter = list(industry_out)
industry_out_filter = list(industry_out)
BEE_bin_out_filter = list(BEE_bin_out)
Nvideo_bin_out_filter = list(Nvideo_bin_out)
calculated_program_start_date_filter = list(calculated_program_start_date)

for i in range(len(remove_column_idx)-1,-1,-1):
	del TA_out_filter[remove_column_idx[i]]
	del industry_out_filter[remove_column_idx[i]]
	del BEE_bin_out_filter[remove_column_idx[i]]
	del Nvideo_bin_out_filter[remove_column_idx[i]]
	del calculated_program_start_date_filter[remove_column_idx[i]]

mean_data_out = []
median_data_out = []
for i in range(0,len(data_out_filter)):
	mean_data_out.append(np.mean(data_out_filter.ix[i]))	
	median_data_out.append(np.median(data_out_filter.ix[i]))	

##########################################################################
# Output data ... AER webpage ... separate Account, GS, Industry by rows
##########################################################################
df_out = pd.DataFrame(list(data_out.columns))
df_out = df_out.rename(columns={0:'account_id'})
df_out['final_reach'] = pd.DataFrame(list(data_out.ix[program_length-1]))
#df_out.to_csv('./output/OE_reach_final_day_ALL.csv')

df_out_filter = pd.DataFrame(list(data_out_filter.columns))
df_out_filter = df_out_filter.rename(columns={0:'account_id'})
df_out_filter['final_reach'] = pd.DataFrame(list(data_out_filter.ix[program_length-1]))
#df_out_filter.to_csv('./output/OE_reach_final_day_FILTER.csv')

##########################################################################
# 2) Add to mysql ... Gainsight ... New columns for Account, GS, Industry
##########################################################################
printf("[aer_daily_bm_calculation_SOLUTION.py]: Output Account Curve Benchmarks\n")

account_id = []
account_name = []
days_since_OE_out = []
sfdc = []
program_date = []
program_launch_date = []
calculated_program_launch_date = []
category = []
industry = []
solution = []
library = []
library_bin = []
TA_size = []
company_size = []
company_size_bin = []
cum_reach = []
in_benchmark = []
reach_ranking = []

account_reach_ranking = list(pd.DataFrame(data_out.ix[ACCOUNT_REACH_DAY-1]).sort([ACCOUNT_REACH_DAY-1],ascending=False).reset_index()['index'])
for i in range(0,len(data_out.columns)):

	if ((i % 100) == 99):
		printf("[aer_daily_bm_calculation_SOLUTION.py]: Account/Solution ... %5s of %5s ... %.2f sec\n",i+1,len(data_out.columns),time.time()-t0)
	
	cur_account  = int(data_out.columns[i].split('_')[0])  
	cur_solution = data_out.columns[i].split('_')[1]  
	cur_industry = aer_df['industry_name'][list(aer_df['account_id']).index(cur_account)]
	cur_Nvideo = aer_df['Nvideo'][list(aer_df['account_id']).index(cur_account)]
	cur_Nvideo_bin = aer_df['Nvideo_bin'][list(aer_df['account_id']).index(cur_account)]
	cur_TA = TA_out[i]
	cur_BEE = aer_df['BEEs'][list(aer_df['account_id']).index(cur_account)]
	cur_BEE_bin = aer_df['BEE_bin'][list(aer_df['account_id']).index(cur_account)]
	cur_BEE_bin = aer_df['BEE_bin'][list(aer_df['account_id']).index(cur_account)]
	cur_sfdc = aer_df['sfdc'][list(aer_df['account_id']).index(cur_account)]
	cur_date = aer_df['program_launch_date'][list(aer_df['account_id']).index(cur_account)]
	cur_account_name = aer_df['account_name'][list(aer_df['account_id']).index(cur_account)]
	for j in range(0,program_length):
		days_since_OE_out.append(j)
		account_id.append(cur_account)
		account_name.append(cur_account_name)
		solution.append(cur_solution)
		sfdc.append(cur_sfdc)
		program_launch_date.append(cur_date)
		calculated_program_launch_date.append(calculated_program_start_date[i])
		program_date.append(calculated_program_start_date[i] + timedelta(days = j) )
		category.append('account')
		industry.append(cur_industry)
		library.append(cur_Nvideo)
		library_bin.append(cur_Nvideo_bin)
		TA_size.append(cur_TA)
		company_size.append(cur_BEE)
		company_size_bin.append(cur_BEE_bin)
		cum_reach.append(data_out[data_out.columns[i]][j])
		if (len(all_indices_CASE_SENSITIVE(data_out.columns[i],data_out_filter.columns)) > 0):
			in_benchmark.append(1)
		else:
			in_benchmark.append(0)
		reach_ranking.append(all_indices_CASE_SENSITIVE(data_out.columns[i],account_reach_ranking)[0])

reach_output_rows = {}
reach_output_rows['days_since_OE'] = days_since_OE_out 
reach_output_rows['category'] = 'account' 
reach_output_rows['account_id'] = account_id 
reach_output_rows['account_name'] = account_name   
reach_output_rows['solution'] = solution   
reach_output_rows['sfdc'] = sfdc 
reach_output_rows['date'] = program_date 
reach_output_rows['program_launch_date'] = program_launch_date 
reach_output_rows['calculated_program_launch_date'] = calculated_program_launch_date 
reach_output_rows['industry'] = industry 
reach_output_rows['Nvideo'] = library 
reach_output_rows['Nvideo_bin'] = library_bin 
#reach_output_rows['target_audience'] = TA_size 
#reach_output_rows['BEEs'] = company_size 
reach_output_rows['BEEs'] = TA_size 
reach_output_rows['BEE_bin'] = company_size_bin 
reach_output_rows['reach'] = cum_reach 
reach_output_rows['in_benchmark'] = in_benchmark 
reach_output_rows['account_reach_ranking']   = [-1] * len(days_since_OE_out) 
reach_output_rows['account_reach_ranking_%'] = [-1] * len(days_since_OE_out) 
reach_output_rows_df = pd.DataFrame(reach_output_rows)

unique_solutions = list(set(solution))
for i in range(0,len(unique_solutions)):
	ALL_reach_output_rows_df = reach_output_rows_df[(reach_output_rows_df['solution'] == unique_solutions[i])].reset_index()
	SINGLE_reach_output_rows_df = reach_output_rows_df[(reach_output_rows_df['solution'] == unique_solutions[i]) & (reach_output_rows_df['days_since_OE'] == (ACCOUNT_REACH_DAY-1))]
	account_reach_ranking = SINGLE_reach_output_rows_df.reset_index()[['index','account_id','reach']].sort(['reach'],ascending=False).reset_index(drop=True)
	max_reach_ranking = len(account_reach_ranking)
	for j in range(0,len(account_reach_ranking)):
		account_idx = all_indices_CASE_SENSITIVE(account_reach_ranking.ix[j]['account_id'],ALL_reach_output_rows_df['account_id'])
		output_idx = list(ALL_reach_output_rows_df.ix[account_idx]['index']) 
		reach_output_rows_df.loc[output_idx,'account_reach_ranking'] = j + 1
		reach_output_rows_df.loc[output_idx,'account_reach_ranking_%'] = float(j + 1) / float(max_reach_ranking)

unique_size = list(set(company_size_bin))
for i in range(0,len(unique_solutions)):
	for k in range(0,len(unique_size)):
		ALL_reach_output_rows_df = reach_output_rows_df[(reach_output_rows_df['solution'] == unique_solutions[i]) & (reach_output_rows_df['BEE_bin'] == unique_size[k])].reset_index()
		SINGLE_reach_output_rows_df = reach_output_rows_df[(reach_output_rows_df['solution'] == unique_solutions[i]) & (reach_output_rows_df['BEE_bin'] == unique_size[k]) & (reach_output_rows_df['days_since_OE'] == (ACCOUNT_REACH_DAY-1))]
		account_reach_ranking = SINGLE_reach_output_rows_df.reset_index()[['index','account_id','reach']].sort(['reach'],ascending=False).reset_index(drop=True)
		max_reach_ranking = len(account_reach_ranking)
		for j in range(0,len(account_reach_ranking)):
			account_idx = all_indices_CASE_SENSITIVE(account_reach_ranking.ix[j]['account_id'],ALL_reach_output_rows_df['account_id'])
			output_idx = list(ALL_reach_output_rows_df.ix[account_idx]['index']) 
			reach_output_rows_df.loc[output_idx,'account_reach_ranking_BEE'] = j + 1
			reach_output_rows_df.loc[output_idx,'account_reach_ranking_BEE_%'] = float(j + 1) / float(max_reach_ranking)

#reach_output_rows_df.to_csv('/home/analytics/analytics_sandbox/data_integration/churn/output/reach_output_rows_df.csv')

###############
# Benchmarks 
###############

## 1) Guidespark
printf("[aer_daily_bm_calculation_SOLUTION.py]: Output Guidespark Reach Curve\n")
reach_output_rows_df = pd.merge(reach_output_rows_df,pd.DataFrame(median_data_out),how='left',left_on='days_since_OE',right_index=True)
reach_output_rows_df = reach_output_rows_df.rename(columns={0:'bm_guidespark'})

## 2) Industry Benchmark
printf("[aer_daily_bm_calculation_SOLUTION.py]: Output Industry Reach Curves\n")
industry_reach_df = calculate_BENCHMARK('industry',industry_out_filter,data_out_filter)
reach_output_rows_df = pd.merge(reach_output_rows_df,industry_reach_df,how='left',left_on=['industry','days_since_OE'],right_on=['industry','days_since_OE']).sort(['account_id','solution','days_since_OE']).reset_index(drop=True)
reach_output_rows_df = reach_output_rows_df.rename(columns={'median_reach':'bm_industry'})

## 3) BEE Benchmark
printf("[aer_daily_bm_calculation_SOLUTION.py]: Output BEE Reach Curves\n")
bee_reach_df = calculate_BENCHMARK('BEE_bin',BEE_bin_out_filter,data_out_filter)
reach_output_rows_df = pd.merge(reach_output_rows_df,bee_reach_df,how='left',left_on=['BEE_bin','days_since_OE'],right_on=['BEE_bin','days_since_OE']).sort(['account_id','solution','days_since_OE']).reset_index(drop=True)
reach_output_rows_df = reach_output_rows_df.rename(columns={'median_reach':'bm_bee'})

## 4) Library Benchmark ... no meaning yet since we are not calculating the library size properly
printf("[aer_daily_bm_calculation_SOLUTION.py]: Output Library Reach Curves\n")
Nvideo_reach_df = calculate_BENCHMARK('Nvideo_bin',Nvideo_bin_out_filter,data_out_filter)
reach_output_rows_df = pd.merge(reach_output_rows_df,Nvideo_reach_df,how='left',left_on=['Nvideo_bin','days_since_OE'],right_on=['Nvideo_bin','days_since_OE']).sort(['account_id','solution','days_since_OE']).reset_index(drop=True)
reach_output_rows_df = reach_output_rows_df.rename(columns={'median_reach':'bm_library_size'})

## Create trigger benchmark
bm_trigger = []
for i in range(0,len(reach_output_rows_df)):
	if (str(reach_output_rows_df['bm_industry'][i]) != 'nan' and str(reach_output_rows_df['bm_bee'][i]) != 'nan'):  
		bm_trigger.append((reach_output_rows_df['bm_industry'][i] + reach_output_rows_df['bm_bee'][i]) / 2)
	else:
		bm_trigger.append(0)

reach_output_rows_df = pd.merge(reach_output_rows_df,pd.DataFrame(bm_trigger),how='left',left_index=True,right_index=True)
reach_output_rows_df = reach_output_rows_df.rename(columns={0:'bm_trigger'})

reach_output_rows_df = reach_output_rows_df.reset_index(drop=True)

drop_mysql_table(con,DBNAME,WRITE_BENCHMARK_TABLE)
create_g2_GLOBAL_BENCHMARK_table(con,DBNAME,WRITE_BENCHMARK_TABLE)
insert_into_g2_GLOBAL_BENCHMARK_table(con,DBNAME,WRITE_BENCHMARK_TABLE,median_data_out,industry_reach_df,bee_reach_df,Nvideo_reach_df,time.time())

drop_mysql_table(con,DBNAME,WRITE_TABLE_ROW)
create_BENCHMARK_SOLUTION_ROW_mysql_table(con,DBNAME,WRITE_TABLE_ROW)
insert_into_BENCHMARK_SOLUTION_ROW_mysql_DB(con,DBNAME,WRITE_TABLE_ROW,reach_output_rows_df)

drop_mysql_table(con,DBNAME,'AER_PROGRAM_BENCHMARK_GS_SOLUTION_ROW_BM_ALL')
if (CUR_YEAR == '2015'):
	query = "CREATE TABLE %s.AER_PROGRAM_BENCHMARK_GS_SOLUTION_ROW_BM_ALL  \
					(SELECT * FROM ( \
					SELECT *,'2014' as cur_year,IF(YEAR(program_launch_date) = 2014,1,0) as updated_launch_date FROM %s.AER_PROGRAM_BENCHMARK_GS_SOLUTION_ROW_BM2014 \
					UNION \
					SELECT *,'2015' as cur_year,IF(YEAR(program_launch_date) = 2015,1,0) as updated_launch_date FROM %s.AER_PROGRAM_BENCHMARK_GS_SOLUTION_ROW_BM2015 ) T \
					ORDER BY cur_year,account_id,solution,program_day)" % (DBNAME,DBNAME,DBNAME)

	cur.execute(query)
	con.commit() # necessary to finish statement

	
  
#reach_output_rows_df.to_csv('aer_daily_bm_calculation.csv',encoding='utf-8')


