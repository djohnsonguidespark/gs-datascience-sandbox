############################################################
#
# Created by DKJ ... 8/29/16
#
# Program summarizes all content hours and outputs to an xlsx file
#
############################################################
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import sys
import time 
import os
import MySQLdb as mdb

sys.path.insert(0,'/home/analytics/analytics_sandbox/python_libs/')
import common_libs as cm 

cur_datetime = datetime.now()

if (cur_datetime.month >= 8 & cur_datetime.month <= 11):
	TARGET_CUSTOMER_HOURS = 50
	TARGET_NONCUSTOMER_HOURS = 2.5
else:
	TARGET_CUSTOMER_HOURS = 32 
	TARGET_NONCUSTOMER_HOURS = 8 

# Logging
import logging
from logging.config import fileConfig

DBNAME = 'content_prod'
TABLENAME = 'TMP_ATTASK_WEEKLY_HOURS'

fileConfig('../python_libs/logging_config.ini')
logger = logging.getLogger()
LOG = logging.getLogger()

# create file logging format
handler = logging.FileHandler('file.log')
formatter = logging.Formatter('[%(asctime)s][%(filename)s][%(module)s][Line %(lineno)5d] %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
LOG.addHandler(handler) ## All LOG entries go to the log file

pd.set_option('display.width',500)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',200)

#############################################
# Main Function
#############################################
def main():
	con = None
	con = mdb.connect('localhost','root','','');
	cur = con.cursor()
	
	###############################
	# Find Current YearWeek combo
	###############################	
	LOG.info("Get current yearweek")
	cur_week  = str(cur_datetime.strftime('%Y-%m-%d'))	
	last_week = str((cur_datetime - timedelta(days = 7)).strftime('%Y-%m-%d'))	
	query = ("SELECT YEARWEEK('%s') as cur_yearweek,YEARWEEK('%s') as last_yearweek" % (cur_week,last_week ))	
	yearweek_df = pd.read_sql(query,con)

	###############
	# Query DB
	###############
	LOG.info("Query Data")
	query = ("SELECT yearweek,userID,user_name,role_name_summary,customer_hours,non_customer_hours,pto_hours FROM %s.%s" % (DBNAME,TABLENAME) )
	input_data_df = pd.read_sql(query,con)
	LOG.info("Query Complete")

	##############################################
	# Filter current last yearweek stats
	##############################################
	LOG.info("Filter Data to yearweek = %s",yearweek_df.ix[0]['last_yearweek'])
	last_yearweek_df = input_data_df[(input_data_df['yearweek'] == yearweek_df.ix[0]['last_yearweek'])]
	last_yearweek_df['Target Customer Hours'] = TARGET_CUSTOMER_HOURS
	last_yearweek_df['Target NonCustomer Hours'] = TARGET_NONCUSTOMER_HOURS
	last_yearweek_df['Customer Utilization'] = last_yearweek_df['customer_hours'] / float(TARGET_CUSTOMER_HOURS)
	last_yearweek_df['NonCustomer Utilization'] = last_yearweek_df['non_customer_hours'] / float(TARGET_NONCUSTOMER_HOURS)
	#last_yearweek_df['Days In Office'] = None 
	#last_yearweek_df['Target Customer Hrs / Day'] = None 

	##############################################
	# Output Data to XLSX file 
	##############################################
	columns = ['user_name','role_name_summary','customer_hours','Target Customer Hours','Customer Utilization','non_customer_hours','Target NonCustomer Hours','NonCustomer Utilization']
	special_format= {}
	special_format['customer_hours'] = "0.00"	
	special_format['Customer Utilization'] = "0.0%"	
	special_format['non_customer_hours'] = "0.00"	
	special_format['NonCustomer Utilization'] = "0.0%"	
	cm.createXLSX('Utilization_Capacity_Tracking','All',columns,special_format,last_yearweek_df[columns],True)

	return columns,last_yearweek_df,input_data_df

if __name__ == '__main__':
	columns,last_yearweek_df,input_data_df = main()

