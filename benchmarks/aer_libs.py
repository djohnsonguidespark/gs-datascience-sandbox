#! /usr/bin/env python

import sys
import re 
import csv
from decimal import Decimal
import MySQLdb as mdb
import pandas as pd
from scipy import stats 
from numpy import *
from openpyxl import load_workbook
import time
import collections
from matplotlib.pyplot import *
import matplotlib.pyplot as plt
import matplotlib.pylab as plab 
import matplotlib.colors as colors 
import matplotlib.cm as cmx 
import itertools
import math

from benchmark_common_libs import *
from benchmark_create_mysql import *

def create_percentile_bin(in_df,bin_column,percentage_column):

	###################################################
	# Creates percentile of 
	# 1) percentage_column
	# for
    # 2) All bins in bin_column
	###################################################
	var_percentile = [0] * len(in_df) 

	unique_var = list(set(in_df[bin_column]))
	for i in range(0,len(unique_var)):
		cur_data = in_df[(in_df[bin_column] == unique_var[i])]
		tmp_percentile = [stats.percentileofscore(cur_data[percentage_column],a,'weak') for a in cur_data[percentage_column]] 
		#tmp_percentile = [stats.percentileofscore(cur_data[percentage_column],a,'strict') for a in cur_data[percentage_column]] 
		for j in range(0,len(cur_data)):
			var_percentile[cur_data.index[j]] = tmp_percentile[j]

	return(var_percentile)

def create_percentile_bin_2D(in_df,bin_column1,bin_column2,percentage_column):

	###################################################
	# Creates percentile of 
	# 1) percentage_column
	# for
    # 2) All bins in bin_column1 / bin_column2
	###################################################
	unique_var1 = sorted(list(set(in_df[bin_column1])))
	var_percentile = [0] * len(in_df) 

	for k in range(0,len(unique_var1)):

		in2_df = in_df[(in_df[bin_column1] == unique_var1[k])] 

		unique_var2 = [x for x in sorted(list(set(in2_df[bin_column2]))) if x is not None]
		for i in range(0,len(unique_var2)):
			cur_data = in2_df[(in2_df[bin_column2] == unique_var2[i])]
			tmp_percentile = [stats.percentileofscore(cur_data[percentage_column],a,'weak') for a in cur_data[percentage_column]] 
			for j in range(0,len(cur_data)):
				var_percentile[cur_data.index[j]] = tmp_percentile[j]

	return(var_percentile)

def create_median_benchmark(in_df,bin_column,cur_timeframe,df_var):

	################################
	# Gives median of bin_column 
	################################

	var_benchmark = in_df.groupby(bin_column).median()
	for i in range(0,len(var_benchmark.columns)):
		var_benchmark = var_benchmark.rename(columns={var_benchmark.columns[i]:'MEDIAN_' + var_benchmark.columns[i]})

	list_timeframe = [cur_timeframe] * len(var_benchmark)
	var_benchmark[df_var] = list_timeframe	

	var_benchmark_cnt = in_df.groupby(bin_column).count()
	var_benchmark_cnt = var_benchmark_cnt.rename(columns={var_benchmark_cnt.columns[0]:'Naccount'})
	var_benchmark_sum = in_df.groupby(bin_column).sum()
	var_benchmark_sum = var_benchmark_sum.rename(columns={var_benchmark_cnt.columns[0]:'Nvideo'})

	var_benchmark = pd.merge(var_benchmark,pd.DataFrame(var_benchmark_cnt['Naccount']),how='left',right_index=True,left_index=True)
	var_benchmark = pd.merge(var_benchmark,pd.DataFrame(var_benchmark_sum['Nvideo']),how='left',right_index=True,left_index=True)
#	try:
#		var_benchmark = var_benchmark.drop('MEDIAN_gs_percentile',1)
#	except:
#		printf('No gs_percentile\n')
#	try:
#		var_benchmark = var_benchmark.drop('MEDIAN_industry_percentile',1)
#	except:
#		printf('No industry_percentile\n')
#	try:
#		var_benchmark = var_benchmark.drop('MEDIAN_bee_percentile',1)
#	except:
#		printf('No bee_percentile\n')
#	try:
#		var_benchmark = var_benchmark.drop('MEDIAN_Nvideo_percentile',1)
#	except:
#		printf('No Nvideo_percentile\n')
#	try:
#		var_benchmark = var_benchmark.drop('MEDIAN_account_id',1)
#	except:
#		printf('No account_id\n')
#
	var_benchmark.reset_index(inplace=True) # Create numeric index ... helps with joining on multiple columns

	return(var_benchmark)

def add_non_benchmark_values(cur_timeframe,in_df,var_of_interest,bin_column,bin_percentile):

	unique_bin = [x for x in sorted(list(set(in_df[bin_column]))) if x is not None]
	for k in range(0,len(unique_bin)):
		cur_bin = unique_bin[k]

		if (len(in_df[var_of_interest][(in_df['in_benchmark'] == 1) & (in_df['cur_year'] == cur_timeframe) & (in_df[bin_column] == cur_bin) ]) > 0):
			## Add Max component
			min_USER_reach = min(in_df[var_of_interest][(in_df['in_benchmark'] == 1) \
										& (in_df['cur_year'] == cur_timeframe) \
										& (in_df[bin_column] == cur_bin) ])
			min_bin_reach_percentile = min(in_df[bin_percentile][(in_df['in_benchmark'] == 1) \
										& (in_df['cur_year'] == cur_timeframe) \
										& (in_df[bin_column] == cur_bin) ])/100
			max_USER_reach = max(in_df[var_of_interest][(in_df['in_benchmark'] == 1) \
										& (in_df['cur_year'] == cur_timeframe) \
										& (in_df[bin_column] == cur_bin) ])
			max_bin_reach_percentile = max(in_df[bin_percentile][(in_df['in_benchmark'] == 1) \
										& (in_df['cur_year'] == cur_timeframe) \
					 					& (in_df[bin_column] == cur_bin) ])/100

			############################
			## Average values below min
			############################
			bin_reach_out_df = in_df[(in_df['in_benchmark'] == 0) \
												    & (in_df[var_of_interest] <= min_USER_reach) \
													& (in_df['cur_year'] == cur_timeframe) \
													& (in_df[bin_column] == cur_bin)	]
			#bin_reach_percentile_out = [stats.percentileofscore(bin_reach_out_df['USER_reach'],a,'mean')*min_bin_reach_percentile for a in bin_reach_out_df['USER_reach']]
			#for j in range(0,len(bin_reach_percentile_out)):
			#	in_df[bin_percentile][bin_reach_out_df.index[j]] = bin_reach_percentile_out[j]

			for j in range(0,len(bin_reach_out_df)):
				percent_value = 100*(bin_reach_out_df[var_of_interest][bin_reach_out_df.index[j]] ) * min_bin_reach_percentile / min_USER_reach
				in_df.loc[bin_reach_out_df.index[j],bin_percentile] = percent_value 

			############################
			## Average values above max 
			############################
			bin_reach_out_df = in_df[(in_df['in_benchmark'] == 0) \
												    & (in_df[var_of_interest] >= max_USER_reach) \
													& (in_df['cur_year'] == cur_timeframe) \
													& (in_df[bin_column] == cur_bin)	]
			#bin_reach_percentile_out = [stats.percentileofscore(bin_reach_out_df['USER_reach'],a,'mean')*(1-max_bin_reach_percentile) for a in bin_reach_out_df['USER_reach']]
			#for j in range(0,len(bin_reach_percentile_out)):
			#	in_df[bin_percentile][bin_reach_out_df.index[j]] = bin_reach_percentile_out[j] + max_bin_reach_percentile*100

			for j in range(0,len(bin_reach_out_df)):
				in_df.loc[bin_reach_out_df.index[j],bin_percentile] = 100*max_bin_reach_percentile 

			################################
			## Average values in the center 
			################################
			account_reach_sort_df =	in_df[(in_df[bin_column] == cur_bin)].sort(var_of_interest).reset_index()
			account_reach_sort_NA_df = account_reach_sort_df[(account_reach_sort_df[bin_percentile] == 'NA')]
			account_reach_sort_NoNA = account_reach_sort_df[(account_reach_sort_df[bin_percentile] != 'NA')].index
			bin_reach_percentile_out_NA = []	
			for j in range(0,len(account_reach_sort_NA_df)):
				Nidx = account_reach_sort_NA_df.index[j]
				Nidx_low = max([x for x in account_reach_sort_NoNA if x < Nidx])
				Nidx_high = min([x for x in account_reach_sort_NoNA if x > Nidx])
				percent_value = account_reach_sort_df.ix[Nidx_low][bin_percentile] \
								+ (account_reach_sort_df.ix[Nidx][var_of_interest] - account_reach_sort_df.ix[Nidx_low][var_of_interest]) \
								* (account_reach_sort_df.ix[Nidx_high][bin_percentile] - account_reach_sort_df.ix[Nidx_low][bin_percentile]) \
								/ (account_reach_sort_df.ix[Nidx_high][var_of_interest] - account_reach_sort_df.ix[Nidx_low][var_of_interest])	
				account_reach_sort_df[bin_percentile][Nidx] = percent_value
				in_df.loc[account_reach_sort_NA_df['index'][Nidx],bin_percentile] = percent_value
		else:
			Nin = len(in_df[var_of_interest][(in_df['in_benchmark'] == 1) & (in_df['cur_year'] == cur_timeframe) & (in_df[bin_column] == cur_bin) ]) 
			Nout = len(in_df[var_of_interest][(in_df['in_benchmark'] == 0) & (in_df['cur_year'] == cur_timeframe) & (in_df[bin_column] == cur_bin) ])
			printf('[aer_libs.py][add_non_benchmark_values] Missed: k=%3s . Benchmark (in,out) = (%4s,%4s) ... %80s\n',k,Nin,Nout,unique_bin[k])

			#gs_reach_percentile = [stats.percentileofscore(cur_reach_NoNA_df['USER_reach_corrected'],a,'weak') for a in cur_reach_NoNA_df['USER_reach_corrected']] 
			cur_bin_reach_NoNA_df = in_df[(in_df['in_benchmark'] == 0) & (in_df['cur_year'] == cur_timeframe) & (in_df[bin_column] == cur_bin) ]
			if (Nout == 1):
				cur_reach_percentile = [stats.percentileofscore(cur_bin_reach_NoNA_df[var_of_interest],a,'mean') for a in cur_bin_reach_NoNA_df[var_of_interest]]
			else:
				cur_reach_percentile = [stats.percentileofscore(cur_bin_reach_NoNA_df[var_of_interest],a,'weak') for a in cur_bin_reach_NoNA_df[var_of_interest]]
	
			for j in range(0,len(cur_bin_reach_NoNA_df)):
				in_df.loc[cur_bin_reach_NoNA_df.index[j],bin_percentile] = cur_reach_percentile[j]
	
	return(in_df)



