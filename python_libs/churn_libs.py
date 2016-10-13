#! /usr/bin/env python

import sys
import re
import json
import csv
from datetime import datetime, timedelta
import MySQLdb as mdb
import pandas as pd
import numpy as np 
import math 
from common_libs import *

def calc_lifetime_day(in_df,cur_date,base_date):
	lifetime_day = []

	#if type(in_df.ix[0][cur_date]) is not pd.tslib.Timestamp:
	#	raise TypeError('%s ... Arg must be a Timestamp, not a %s' % (cur_date,type(in_df.ix[0][cur_date])) )
	#if type(in_df.ix[0][base_date]) is not pd.tslib.Timestamp:
	#	raise TypeError('%s ... Arg must be a Timestamp, not a %s' % (base_date,type(in_df.ix[0][base_date])) )

	Nissue = 0
	#error = {}
	for i in range(0,len(in_df)):
		try:
			lifetime_day.append( (in_df.ix[i][cur_date] - in_df.ix[i][base_date]).days )
		except Exception as e:
			#printf("[churn_libs.py][calc_lifetime_day]: Line %s: i = %4d: %s\n",sys.exc_traceback.tb_lineno,i,e)
			Nissue = Nissue + 1
			lifetime_day.append(None)
			#try:
			#	error[e] = error[e] + 1 
			#except Exception as e:
			#	printf("[churn_libs.py][calc_lifetime_day]: Line %s: i = %4d: %s\n",sys.exc_traceback.tb_lineno,i,e)
			#	error[e] = 1 
			#	printf("%s,%s\n",e,error[e])

	#for i in range(0,len(error)):
	#	count = list(error.values())[i]
	#	count_per = float(count) / float(len(in_df)) * 100
	#	printf("[churn_libs.py][calc_lifetime_day]: Error Count = %7d of %7d (%6.2f%%)... Error: %s\n",count,len(in_df),count_per,error.keys()[i])
	#printf("\n")
	printf("[churn_libs.py][calc_lifetime_day]: Error Count = %7d of %7d (%6.2f%%)\n",Nissue,len(in_df),float(Nissue)/float(len(in_df))*100)

	return lifetime_day

