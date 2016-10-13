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

cur_datetime = datetime.now()
execfile('/home/analytics/analytics_sandbox/python_libs/stuff.py')

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',400)

con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

renewal_match_delta = 15 
GLOBAL_product_progression_df = []	

start = time.time()

def find_percentage(df,X,Y,name):

	value_per = []
	for i in range(0,len(df)):
		try:
			value_per.append(float(df.ix[i][X]) / float(df.ix[i][Y]) ) 
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

########## SIMPLE SALESFORCE ##############
sf = Salesforce(username='djohnson@guidespark.com', password=pwd,security_token=token)

########################
# Read in product data 
########################
sf_product_df = pd.read_csv('./output/sf_product_df.csv')

### Added Subscription_Start_Date__c sort filter (using close date can cause issues ... 0015000000nLa1rAAC) 
#sf_product_summary_df = pd.read_csv('./output/sf_product_summary_df.csv').sort(['AccountId_18','Subscription_Start_Date__c','Close_Date']).reset_index(drop=True)

### Issue is cancellations are after Renewals and video swaps are after renewals
#sf_product_summary_df = pd.read_csv('./output/sf_product_summary_df.csv').sort(['AccountId_18','Subscription_Start_Date__c','OpportunityType']).reset_index(drop=True)
sf_product_summary_df = pd.read_csv('./output/sf_product_summary_df.csv')

sf_product_summary_df.Product2Id_group = [x.replace('[','').replace(']','').replace(', ',',') for x in sf_product_summary_df.Product2Id_group]
sf_product_summary_df.PricebookEntry_group = [x.replace('[','').replace(']','').replace(', ',',') for x in sf_product_summary_df.PricebookEntry_group]
sf_product_summary_df.Quantity_group = [x.replace('[','').replace(']','').replace(', ',',') for x in sf_product_summary_df.Quantity_group]

account = list(set(sf_product_summary_df.AccountId_18))

execfile('omit_accounts.py')
execfile('update_accounts.py')
account = extra_val(account,omit_accounts)
account = extra_val(account,update_accounts)

#account = update_accounts

#############################################################
# Print all accounts that do NOT being with 'Initial Sale'
#############################################################
printf("\n***********************************************\nAll Accounts THAT DO NOT HAVE 'Initial Sale'\n***********************************************\n")
for i in range(0,len(account)):
	sf_cur_df = sf_product_summary_df[(sf_product_summary_df.AccountId_18 == account[i])].reset_index()

	if ('Initial Sale' not in sf_cur_df.ix[0]['OpportunityType']):
		printf("%s ... %25s ... %s\n",sf_cur_df.ix[0]['AccountId_18'],sf_cur_df.ix[0]['OpportunityType'],sf_cur_df.ix[0]['sf_op_name'])
	
###########################
# Create initial dataframe 
###########################
columns=['AccountId_18','Initial_Close_Date','OpportunityType_init','OpportunityId_init','OpportunityId_cur','Product2Id','PricebookEntry','op_Product2Id_match','Product2Id_match',\
		'op_000','op_001','op_002','op_003','op_004','op_005','op_006','op_007','op_008','op_009',\
		'op_010','op_011','op_012','op_013','op_014','op_015','op_016','op_017','op_018','op_019',\
		'op_020','op_021','op_022','op_023','op_024','op_025','op_026','op_027','op_028','op_029',\
		'op_030','op_031','op_032','op_033','op_034','op_035','op_036','op_037','op_038','op_039',\
		'op_040','op_041','op_042','op_043','op_044','op_045','op_046','op_047','op_048','op_049',\
		'op_050','op_051','op_052','op_053','op_054','op_055','op_056','op_057','op_058','op_059',\
		'op_060','op_061','op_062','op_063','op_064','op_065','op_066','op_067','op_068','op_069',\
		'op_070','op_071','op_072','op_073','op_074','op_075','op_076','op_077','op_078','op_079',\
		'op_080','op_081','op_082','op_083','op_084','op_085','op_086','op_087','op_088','op_089',\
		'op_090','op_091','op_092','op_093','op_094','op_095','op_096','op_097','op_098','op_099'] 
GLOBAL_product_progression_df = pd.DataFrame(columns=columns)
 
###########################
# Find product progression 
###########################
#account = ['0015000000f9lMAAAY']   ## Allergan ... perfect
#account = ['0015000000fBXFqAAO']   ## Erie Indemnity ... perfect (Video Swaps and all)
#account = ['00138000016BfOrAAK']   ## Navstar Video Swap example
#account = ['0015000000fBWwGAAW']   ## Recreational Equipment ... issue with 003 ... self corrects but is down 1 video for a short time
									## Video swap works ... BUT this case swaps two of the same titles ... may make the algo fail
#account = ['0015000000f9lLyAAI']   ## Synopsis ... matches 25 videos at the end ... alot of dups ... not sure if being handled correctly
#account = ['0015000000skUwoAAE']   ## 
#account = ['0015000000fBXZXAA4']   ## Tribune Company ... works with renewal_match_delta = 15 .... need to update 0065000000OIknw ... renewal flat (0065000000SHlIe) causing an issue .. but ends correctly 
#account = ['0015000000fBVQoAAO']   ## Verizon ... Need a video swap between final 2 opportunities 
#account = ['0015000000oXcj2AAC']   ## Duplicate Initial Sales
#account = ['0015000000gPz9sAAC']   ## Chevron ... all working with cancellations
#account = ['0015000000o5xkZAAQ'] 
#account = ['0015000000o5UZYAA2'] 
#account = ['0015000000rFPbOAAW'] 
#account = ['0015000000p8ZeZAAU']
#account = ['0015000000pzvfTAAQ']
#account = ['0015000000rQgFUAA0']   ## Cancellation after current date ... check so that final_no_of_videos is correct
#account = ['0015000000fBVRpAAO']
#account = ['0015000000o5UZYAA2']   ## Gibralter ... renewal-flat issue ... op_004
#account = ['0015000000nLa1rAAC']   ## Uno ... cancellation and THEN new initial sale
#account = ['0015000000ryBOUAA2']
#account = ['0015000000fBWslAAG']   ## Bayhealth ... systems module
#account = ['0015000000f9lLVAAY']   ## Yahoo .. no products in first 3 opportunties
#account = ['0015000000f9lMTAAY']   ## Amgen ... initial op is renewal
#account = ['0015000000wmDqjAAE']   ## Innovative interfaces
#account = ['0015000000rw8SZAAY']   ## BakerHughes ... swaps/cancellation not matching ... check 0065000000QibBa ... Open Enrollment already gone 
#account = ['0015000000fBWsUAAW']   ## Macys ... now matches ... upsell with -1
#account = ['0015000000vxgy6AAA']   ## Landmark account
#account = ['0015000000p8mVaAAI'] 

fileout = open('./output/product_flow_out.csv','w') 
fileout.write('Account,Total Accounts,Account Id, Op Name,Op Number,Op Type,Op Id,Product Id,output\n')

cur_account = []
final_no_of_videos = [] 
no_of_videos_final_op = [] 
#for i in range(0,3):
for i in range(0,len(account)):

	sf_cur_df = sf_product_summary_df[(sf_product_summary_df.AccountId_18 == account[i])].reset_index()

	#printf("\nAccount = %4s of %4s ... %18s ... %s\n",i+1,len(account),sf_cur_df.ix[0]['AccountId_18'],sf_cur_df.ix[0]['sf_op_name'])

	account_id = []
	close_date = []
	renewal_account_id = []
	renewal_close_date = []
	account_id_list = [sf_cur_df.ix[0]['AccountId_18']] * 5 
	close_date_list = ['op_id','op_type','close_date','subscription_start_date','renewal_date']
	subscription_start_date_list = ['op_id','op_type','close_date','subscription_start_date','renewal_date']
	renewal_date_list = ['op_id','op_type','close_date','subscription_start_date','renewal_date']
	op_id_list = ['op_id','op_type','close_date','subscription_start_date','renewal_date']
	op_type_list = ['op_id','op_type','close_date','subscription_start_date','renewal_date']
	product_list = ['op_id','op_type','close_date','subscription_start_date','renewal_date']
	pricebook_list = ['op_id','op_type','close_date','subscription_start_date','renewal_date']
	product_list_match = ['op_id','op_type','close_date','subscription_start_date','renewal_date']
	pricebook_list_match = ['op_id','op_type','close_date','subscription_start_date','renewal_date']
	next_op = 0

	product_progression_df = []
	#if ('Initial Sale' in sf_cur_df.ix[0]['OpportunityType']):
	if (True):

		########################################
		# A) Find the total number of projects
		########################################
		for j in range(0,len(sf_cur_df)):

			if ('Initial Sale' in sf_cur_df.ix[j]['OpportunityType'] or 'Upsell' in sf_cur_df.ix[j]['OpportunityType'] or 'Video Swap' in sf_cur_df.ix[j]['OpportunityType'] ):
				quantity = sf_cur_df.ix[j]['Quantity_group'].split(',') 
				productId = sf_cur_df.ix[j]['Product2Id_group'].split(',') 
				pricebook = sf_cur_df.ix[j]['PricebookEntry_group'].split(',') 

				## Document current account and close date
				account_id.append(sf_cur_df.ix[j]['AccountId_18'])
				close_date.append(sf_cur_df.ix[j]['Close_Date'])

				## Tabulate current products 
				tmp_account_id_list = [] 
				tmp_close_date_list = [] 
				tmp_op_type_list = [] 
				tmp_op_id_list = [] 
				tmp_subscription_start_date_list = [] 
				tmp_renewal_date_list = [] 
				tmp_product_list = [] 
				tmp_pricebook_list = [] 
				for k in range(0,len(productId)):
					for m in range(0,int(quantity[k])):
						tmp_account_id_list.append(sf_cur_df.ix[j]['AccountId_18'])
						tmp_op_type_list.append(sf_cur_df.ix[j]['OpportunityType'])
						tmp_op_id_list.append(sf_cur_df.ix[j]['OpportunityId'])
						tmp_close_date_list.append(sf_cur_df.ix[j]['Close_Date'])
						tmp_subscription_start_date_list.append(sf_cur_df.ix[j]['Subscription_Start_Date__c'])
						tmp_renewal_date_list.append(sf_cur_df.ix[j]['Renewal_Date__c'])
						tmp_product_list.append(productId[k]) 
						tmp_pricebook_list.append(pricebook[k]) 

				account_id_list = account_id_list + tmp_account_id_list
				op_type_list = op_type_list + tmp_op_type_list
				op_id_list = op_id_list + tmp_op_id_list
				close_date_list = close_date_list + tmp_close_date_list
				renewal_date_list = renewal_date_list + tmp_renewal_date_list
				subscription_start_date_list = subscription_start_date_list + tmp_subscription_start_date_list
				product_list = product_list + tmp_product_list
				pricebook_list = pricebook_list + tmp_pricebook_list

				#printf("%10s ... %3s ... %s\n",close_date[len(close_date)-1],len(product_list),product_list)

		#############################################################
		# Add Renewal products that never show up in Initial/Upsell
		#############################################################
		for j in range(0,len(sf_cur_df)):

			if ('Renewal' in sf_cur_df.ix[j]['OpportunityType']):
				quantity = sf_cur_df.ix[j]['Quantity_group'].split(',') 
				productId = sf_cur_df.ix[j]['Product2Id_group'].split(',') 
				pricebook = sf_cur_df.ix[j]['PricebookEntry_group'].split(',') 

				## Document current account and close date
				renewal_account_id.append(sf_cur_df.ix[j]['AccountId_18'])
				renewal_close_date.append(sf_cur_df.ix[j]['Close_Date'])

				## Tabulate current products 
				tmp_account_id_list = [] 
				tmp_close_date_list = [] 
				tmp_op_type_list = [] 
				tmp_op_id_list = [] 
				tmp_subscription_start_date_list = [] 
				tmp_renewal_date_list = [] 
				tmp_product_list = [] 
				tmp_pricebook_list = [] 
				for k in range(0,len(productId)):
					for m in range(0,int(quantity[k])):
						Ridx = all_indices_CASE_SENSITIVE(productId[k],product_list)  ### Only grab products that ARE NOT in the original list
						if (len(Ridx) == 0):
							tmp_account_id_list.append(sf_cur_df.ix[j]['AccountId_18'])
							tmp_op_type_list.append(sf_cur_df.ix[j]['OpportunityType'])
							tmp_op_id_list.append(sf_cur_df.ix[j]['OpportunityId'])
							tmp_close_date_list.append(sf_cur_df.ix[j]['Close_Date'])
							tmp_subscription_start_date_list.append(sf_cur_df.ix[j]['Subscription_Start_Date__c'])
							tmp_renewal_date_list.append(sf_cur_df.ix[j]['Renewal_Date__c'])
							tmp_product_list.append(productId[k]) 
							tmp_pricebook_list.append(pricebook[k]) 

				account_id_list = account_id_list + tmp_account_id_list
				op_type_list = op_type_list + tmp_op_type_list
				op_id_list = op_id_list + tmp_op_id_list
				close_date_list = close_date_list + tmp_close_date_list
				renewal_date_list = renewal_date_list + tmp_renewal_date_list
				subscription_start_date_list = subscription_start_date_list + tmp_subscription_start_date_list
				product_list = product_list + tmp_product_list
				pricebook_list = pricebook_list + tmp_pricebook_list

				#printf("%10s ... %3s ... %s\n",close_date[len(close_date)-1],len(product_list),product_list)

		#######################################################################
		# Set the current product ops / subscription start date / renewal date 
		#######################################################################
		cur_subscription_df = pd.DataFrame(op_id_list).rename(columns = {0:'OpportunityId'})
		cur_subscription_df = pd.merge(cur_subscription_df,pd.DataFrame(subscription_start_date_list).rename(columns = {0:'Subscription_Start_Date__c'}),'left',left_index=True,right_index=True)
		cur_subscription_df = pd.merge(cur_subscription_df,pd.DataFrame(renewal_date_list).rename(columns = {0:'Renewal_Date__c'}),'left',left_index=True,right_index=True)

		########################################
		# Document the opportunities 
		########################################
		product_progression_df = pd.DataFrame(account_id_list).rename(columns = {0:'AccountId_18'})
		product_progression_df = pd.merge(product_progression_df,pd.DataFrame(close_date_list).rename(columns = {0:'Initial_Close_Date'}),'left',left_index=True,right_index=True)
		product_progression_df = pd.merge(product_progression_df,pd.DataFrame(op_type_list).rename(columns = {0:'OpportunityType_init'}),'left',left_index=True,right_index=True)
		product_progression_df = pd.merge(product_progression_df,pd.DataFrame(op_id_list).rename(columns = {0:'OpportunityId_init'}),'left',left_index=True,right_index=True)
		product_progression_df = pd.merge(product_progression_df,pd.DataFrame(op_id_list).rename(columns = {0:'OpportunityId_cur'}),'left',left_index=True,right_index=True)
		product_progression_df = pd.merge(product_progression_df,pd.DataFrame(product_list).rename(columns = {0:'Product2Id'}),'left',left_index=True,right_index=True)
		product_progression_df = pd.merge(product_progression_df,pd.DataFrame(pricebook_list).rename(columns = {0:'PricebookEntry'}),'left',left_index=True,right_index=True)

		###############################################################
		# Add integers to deal with multiple products with the same id 
		###############################################################
		op_product_index_df = pd.DataFrame([str(x).zfill(3) for x in product_progression_df.groupby(['OpportunityId_init','Product2Id']).cumcount()]).rename(columns = {0:'op_Product2Id_match'})
		op_product_index_df.ix[0]['op_Product2Id_match'] = 'op_id'
		op_product_index_df.ix[1]['op_Product2Id_match'] = 'op_type'
		op_product_index_df.ix[2]['op_Product2Id_match'] = 'close_date'
		op_product_index_df.ix[3]['op_Product2Id_match'] = 'subscription_start_date'
		op_product_index_df.ix[4]['op_Product2Id_match'] = 'renewal_date'
		for j in range(5,len(op_product_index_df)):
			op_product_index_df.ix[j]['op_Product2Id_match'] = product_progression_df.ix[j]['Product2Id'] + '_' + op_product_index_df.ix[j]['op_Product2Id_match']
		product_progression_df = pd.merge(product_progression_df,op_product_index_df,'left',left_index=True,right_index=True)

		product_index_df = pd.DataFrame([str(x).zfill(3) for x in product_progression_df.groupby('Product2Id').cumcount()]).rename(columns = {0:'Product2Id_match'})
		product_index_df.ix[0]['Product2Id_match'] = 'op_id'
		product_index_df.ix[1]['Product2Id_match'] = 'op_type'
		product_index_df.ix[2]['Product2Id_match'] = 'close_date'
		product_index_df.ix[3]['Product2Id_match'] = 'subscription_start_date'
		product_index_df.ix[4]['Product2Id_match'] = 'renewal_date'
		for j in range(5,len(product_index_df)):
			product_index_df.ix[j]['Product2Id_match'] = product_progression_df.ix[j]['Product2Id'] + '_' + product_index_df.ix[j]['Product2Id_match']
		product_progression_df = pd.merge(product_progression_df,product_index_df,'left',left_index=True,right_index=True)

		##################################
		## B) Document product progression 
		##################################
		for j in range(0,len(sf_cur_df)):

			cur_op = 'op_' + str(j).zfill(3) 
			next_op = j + 1
			quantity = sf_cur_df.ix[j]['Quantity_group'].split(',') 
			productId = sf_cur_df.ix[j]['Product2Id_group'].split(',') 
			pricebook = sf_cur_df.ix[j]['PricebookEntry_group'].split(',') 

			##########################################################
			## Tabulate current products ... from current opportunity
			##########################################################
			tmp_op_id_list = ['op_id','op_type','close_date','subscription_start_date','renewal_date'] 
			tmp_product_list = ['op_id','op_type','close_date','subscription_start_date','renewal_date'] 
			tmp_pricebook_list = ['op_id','op_type','close_date','subscription_start_date','renewal_date'] 
			for k in range(0,len(productId)):
				for m in range(0,int(quantity[k])):
					tmp_op_id_list.append(sf_cur_df.ix[j]['OpportunityId'])
					tmp_product_list.append(productId[k]) 
					tmp_pricebook_list.append(pricebook[k]) 

			tmp_product_index_df = pd.DataFrame([str(x).zfill(3) for x in pd.DataFrame(tmp_product_list).rename(columns={0:'Product2Id'}).groupby('Product2Id').cumcount()]).rename(columns = {0:'tmp_Product2Id_match'})
			tmp_product_index_df.ix[0]['tmp_Product2Id_match'] = 'op_id' 
			tmp_product_index_df.ix[1]['tmp_Product2Id_match'] = 'op_type' 
			tmp_product_index_df.ix[2]['tmp_Product2Id_match'] = 'close_date' 
			tmp_product_index_df.ix[3]['tmp_Product2Id_match'] = 'subscription_start_date'
			tmp_product_index_df.ix[4]['tmp_Product2Id_match'] = 'renewal_date'
			for k in range(5,len(tmp_product_index_df)):
				tmp_product_index_df.ix[k]['tmp_Product2Id_match'] = tmp_product_list[k] + '_' + tmp_product_index_df.ix[k]['tmp_Product2Id_match']

			cur_product_df = pd.merge(pd.DataFrame(tmp_op_id_list).rename(columns={0:'OpportunityId'}),tmp_product_index_df,'left',left_index=True,right_index=True) 
			cur_product_df = pd.merge(cur_product_df,pd.DataFrame([1] * len(tmp_product_index_df)).rename(columns={0:'count'}),'left',left_index=True,right_index=True) 

			##############################				
			## Tabulate swapped products 
			##############################
			tmp_swap_account_id_list = [] 
			tmp_swap_op_id_list = [] 
			tmp_swap_product_list = [] 
			tmp_swap_pricebook_list = [] 
			for k in range(0,len(productId)):
				Nquantity = int(quantity[k])
				if (Nquantity < 0):
					for m in range(0,abs(Nquantity) ):
						tmp_swap_account_id_list.append(sf_cur_df.ix[j]['AccountId_18'])
						tmp_swap_op_id_list.append(sf_cur_df.ix[j]['OpportunityId'])
						tmp_swap_product_list.append(productId[k]) 
						tmp_swap_pricebook_list.append(pricebook[k]) 

			cur_swap_product_df = []
			tmp_swap_product_index_df = []
			if (len(tmp_swap_product_list) > 0):
				tmp_swap_product_index_df = pd.DataFrame([str(x).zfill(3) for x in pd.DataFrame(tmp_swap_product_list).rename(columns={0:'Product2Id'}).groupby('Product2Id').cumcount()]).rename(columns = {0:'tmp_Product2Id_match'})
				tmp_swap_pricebook_df = pd.DataFrame(tmp_swap_pricebook_list).rename(columns={0:'swap_PricebookId'})
				for k in range(0,len(tmp_swap_product_index_df)):
					tmp_swap_product_index_df.ix[k]['tmp_Product2Id_match'] = tmp_swap_product_list[k] + '_' + tmp_swap_product_index_df.ix[k]['tmp_Product2Id_match']

				cur_swap_product_df = pd.merge(pd.DataFrame(tmp_swap_op_id_list).rename(columns={0:'OpportunityId'}),tmp_product_index_df,'left',left_index=True,right_index=True) 
				cur_swap_product_df = pd.merge(cur_swap_product_df,pd.DataFrame([1] * len(tmp_swap_product_index_df)).rename(columns={0:'count'}),'left',left_index=True,right_index=True) 
				
			if ('Initial Sale' in sf_cur_df.ix[j]['OpportunityType'] or 'Upsell' in sf_cur_df.ix[j]['OpportunityType'] or 'Video Swap' in sf_cur_df.ix[j]['OpportunityType']):

				###########################################
				## Initial Sale or Upsell
				## 1) Match OpportunityId to OpportunityId
				###########################################
				################################################################################################################################
				## Video Swaps
				## 1) Match swapped products 
				## 2) Set swapped product to 'NaN' ... set new product to 1 
				## 3) Reset all other existing products (ie 0 or 1) to 0 or 1 
				################################################################################################################################
				product_progression_df = pd.merge(product_progression_df,cur_product_df,'left',left_on=['OpportunityId_init','op_Product2Id_match'],right_on=['OpportunityId','tmp_Product2Id_match']).drop('OpportunityId',1).drop('tmp_Product2Id_match',1)
				product_progression_df.loc[0,'count'] = sf_cur_df.ix[j]['OpportunityId'] 
				product_progression_df.loc[1,'count'] = sf_cur_df.ix[j]['OpportunityType'] 
				product_progression_df.loc[2,'count'] = sf_cur_df.ix[j]['Close_Date'] 
				product_progression_df.loc[3,'count'] = sf_cur_df.ix[j]['Subscription_Start_Date__c'] 
				product_progression_df.loc[4,'count'] = sf_cur_df.ix[j]['Renewal_Date__c'] 
				product_progression_df = product_progression_df.rename(columns = {'count':cur_op}) 

				### Set all other products to 0 ... that are still active but NOT in this opportunity
				if ('Upsell' in sf_cur_df.ix[j]['OpportunityType'] or 'Video Swap' in sf_cur_df.ix[j]['OpportunityType']):
					old_column = product_progression_df.columns[len(product_progression_df.columns)-2]
					match1 = pd.notnull(product_progression_df[old_column])
					#if ('Video Swap' in sf_cur_df.ix[j]['OpportunityType']):
						#swap_matched_product = len(tmp_swap_product_index_df) * [False]
					if (len(tmp_swap_product_index_df) > 0):
						swap_matched_product = len(tmp_swap_product_index_df) * [False]

					for k in range(5,len(match1)):
						if (match1[k] == True):
							product_progression_df.loc[k,cur_op] = 0

						### Removed swapped videos in video swap and upsell
						#if ('Video Swap' in sf_cur_df.ix[j]['OpportunityType'] ):
						if (len(tmp_swap_product_index_df) > 0):
							### 1) Check global product ids
							for m in range(0,len(tmp_swap_product_index_df)):
								if (swap_matched_product[m] == False and pd.notnull(product_progression_df.loc[k,cur_op]) ):
									if (product_progression_df.ix[k]['Product2Id_match'] == tmp_swap_product_index_df.ix[m]['tmp_Product2Id_match']):
										product_progression_df.loc[k,cur_op] = float('Nan') 
										swap_matched_product[m] = True

							### 2) Check local product ids 
							for m in range(0,len(tmp_swap_product_index_df)):
								if (swap_matched_product[m] == False and pd.notnull(product_progression_df.loc[k,cur_op]) ):
										if (product_progression_df.ix[k]['op_Product2Id_match'] == tmp_swap_product_index_df.ix[m]['tmp_Product2Id_match']):
											product_progression_df.loc[k,cur_op] = float('Nan') 
											swap_matched_product[m] = True

					# No match ... output results for audit
					#if ('Video Swap' in sf_cur_df.ix[j]['OpportunityType'] ):
					for m in range(0,len(tmp_swap_product_index_df)):
						if (swap_matched_product[m] == False):
							fileout.write("%s,%s,%s,%s," % (i+1,len(account),sf_cur_df.ix[0]['AccountId_18'],sf_cur_df.ix[0]['sf_op_name']))
							fileout.write("%s,Video Swap ... No product match,%s,%s,%s,%s,%s \n" % (cur_op,sf_cur_df.ix[j]['AccountId_18'],sf_cur_df.ix[j]['OpportunityType'],sf_cur_df.ix[j]['OpportunityId'],tmp_swap_product_index_df.ix[m]['tmp_Product2Id_match'],tmp_swap_pricebook_df.ix[m]['swap_PricebookId']) )
							printf("\nAccount = %4s of %4s ... %18s ... %s\n",i+1,len(account),sf_cur_df.ix[0]['AccountId_18'],sf_cur_df.ix[0]['sf_op_name'])
							printf("%3s . Video Swap ... No product match       ... %s . %20s . %s . %s . %s\n",cur_op,sf_cur_df.ix[j]['AccountId_18'],sf_cur_df.ix[j]['OpportunityType'],sf_cur_df.ix[j]['OpportunityId'],tmp_swap_product_index_df.ix[m]['tmp_Product2Id_match'],tmp_swap_pricebook_df.ix[m]['swap_PricebookId'])

			elif ('Renewal' in sf_cur_df.ix[j]['OpportunityType']): ## or 'Video Swap' in sf_cur_df.ix[j]['OpportunityType'] ):

				product_progression_df[cur_op] = np.nan
				product_progression_df.loc[0,cur_op] = sf_cur_df.ix[j]['OpportunityId'] 
				product_progression_df.loc[1,cur_op] = sf_cur_df.ix[j]['OpportunityType'] 
				product_progression_df.loc[2,cur_op] = sf_cur_df.ix[j]['Close_Date'] 
				product_progression_df.loc[3,cur_op] = sf_cur_df.ix[j]['Subscription_Start_Date__c'] 
				product_progression_df.loc[4,cur_op] = sf_cur_df.ix[j]['Renewal_Date__c'] 

				################################################################################################################################
				## 1) Match Renewal Date of current products to New Subscription Date
				## 2) Set to 'NaN' ... any product that is gone from that opportunity
				## 3) Update 'Subscription_Start_Date' and 'Renewal Date' for all products in product_progression_df (ie cur_subscription_df)
				################################################################################################################################
				new_subscription_day = datetime.strptime(sf_cur_df.ix[j]['Subscription_Start_Date__c'],"%Y-%m-%d")
				try:
					new_renewal_day = datetime.strptime(sf_cur_df.ix[j]['Renewal_Date__c'],"%Y-%m-%d")
				except Exception as e:
					printf("[product_flow.py]: NO RENEWAL DATE . %15s . Line %s: i = %4d: %s\n",sf_cur_df.ix[j]['OpportunityId'],sys.exc_traceback.tb_lineno,i,e)
					new_renewal_day = datetime.strptime(sf_cur_df.ix[j]['op_end'],"%Y-%m-%d %H:%M:%S")

				match_idx = []
				op_match_idx = []
				for k in range(5,len(cur_subscription_df)):
					if (pd.notnull(cur_subscription_df.ix[k]['Renewal_Date__c']) == True): 
						try:
							cur_renewal_day = datetime.strptime(cur_subscription_df.ix[k]['Renewal_Date__c'],"%Y-%m-%d")
						except:
							cur_renewal_day = cur_subscription_df.ix[k]['Renewal_Date__c']
				
						### Check that renewal date +-2 days of current subscription start date	
						if ((cur_renewal_day - timedelta(days=renewal_match_delta)) <= new_subscription_day and (cur_renewal_day + timedelta(days=renewal_match_delta)) >= new_subscription_day):
							#printf("k=%s\n",k)
							match_idx.append(k)
							op_match_idx.append(cur_subscription_df.ix[k]['OpportunityId'])

				op_match_idx = list(set(op_match_idx))

				#if (j == 3):
				#	sys.exit()

				###########################
				# Update subscription info
				###########################
				if (len(match_idx) > 0):
					cur_op_product_progression_df = product_progression_df.ix[match_idx].reset_index()

					## Renumber the op_Product2Id_match column
					idx_cur_op_product_index_df = pd.DataFrame([str(x).zfill(3) for x in cur_op_product_progression_df.groupby(['Product2Id']).cumcount()]).rename(columns = {0:'tmp_Product2Id_match'})
					for k in range(0,len(idx_cur_op_product_index_df)):
						idx_cur_op_product_index_df.ix[k]['tmp_Product2Id_match'] = cur_op_product_progression_df.ix[k]['Product2Id'] + '_' + idx_cur_op_product_index_df.ix[k]['tmp_Product2Id_match']
					cur_op_product_progression_df['op_Product2Id_match'] = [x for x in idx_cur_op_product_index_df['tmp_Product2Id_match']]

					#if (j == 9):
					#	sys.exit()

					for k in range(5,len(cur_product_df)):
						cur_match_idx = all_indices_CASE_SENSITIVE(cur_product_df.ix[k]['tmp_Product2Id_match'],cur_op_product_progression_df['op_Product2Id_match'])

						#### 1) Check if there is a match
						if (len(cur_match_idx) >= 1):
							idx = cur_op_product_progression_df.ix[cur_match_idx[0]]['index']	
							product_progression_df.loc[idx,cur_op] = 1	

							###################################
							# Update current subscription info
							###################################
							product_progression_df.loc[idx,'OpportunityId_cur'] = sf_cur_df.ix[j]['OpportunityId']
							cur_subscription_df.loc[idx,'OpportunityId'] = sf_cur_df.ix[j]['OpportunityId']
							cur_subscription_df.loc[idx,'Subscription_Start_Date__c'] = new_subscription_day
							cur_subscription_df.loc[idx,'Renewal_Date__c'] = new_renewal_day

							### Added	
							#op_match_idx.append(cur_subscription_df.ix[idx]['OpportunityId'])
						else:

							fileout.write("%s,%s,%s,%s," % (i+1,len(account),sf_cur_df.ix[0]['AccountId_18'],sf_cur_df.ix[0]['sf_op_name']))
							fileout.write("%s,Phase 1 ... No product match,%s,%s,%s,%s,%s,Search other opportunities etc \n" % (cur_op,sf_cur_df.ix[j]['AccountId_18'],sf_cur_df.ix[j]['OpportunityType'], \
																															sf_cur_df.ix[j]['OpportunityId'],cur_product_df.ix[k]['tmp_Product2Id_match'], \
																					sf_product_df.ix[all_indices_CASE_SENSITIVE(cur_product_df.ix[k]['tmp_Product2Id_match'][:-4],sf_product_df['Product2Id'])[0]]['PricebookEntry'] ) )	
							printf("\nAccount = %4s of %4s ... %18s ... %s\n",i+1,len(account),sf_cur_df.ix[0]['AccountId_18'],sf_cur_df.ix[0]['sf_op_name'])
							printf("%3s . Phase 1 ... No product match       ... %s . %20s . %s . %s . %s . Search other opportunities etc \n",cur_op,sf_cur_df.ix[j]['AccountId_18'],sf_cur_df.ix[j]['OpportunityType'], \
																																		sf_cur_df.ix[j]['OpportunityId'],cur_product_df.ix[k]['tmp_Product2Id_match'], \
																					sf_product_df.ix[all_indices_CASE_SENSITIVE(cur_product_df.ix[k]['tmp_Product2Id_match'][:-4],sf_product_df['Product2Id'])[0]]['PricebookEntry'] )	
							### 2) Search all values outside of match_idx
							no_match_idx = extra_val(list(product_progression_df.index)[3:],match_idx)
							no_cur_op_product_progression_df = product_progression_df.ix[no_match_idx].reset_index()
							no_cur_match_idx = all_indices_CASE_SENSITIVE(cur_product_df.ix[k]['tmp_Product2Id_match'],no_cur_op_product_progression_df['op_Product2Id_match'])

							if (len(no_cur_match_idx) >= 1):
								idx = no_cur_op_product_progression_df.ix[no_cur_match_idx[0]]['index']	
								product_progression_df.loc[idx,cur_op] = 1	

								op_match_idx.append(cur_subscription_df.ix[idx]['OpportunityId'])

								###################################
								# Update current subscription info
								###################################
								product_progression_df.loc[idx,'OpportunityId_cur'] = sf_cur_df.ix[j]['OpportunityId']
								cur_subscription_df.loc[idx,'OpportunityId'] = sf_cur_df.ix[j]['OpportunityId']
								cur_subscription_df.loc[idx,'Subscription_Start_Date__c'] = new_subscription_day
								cur_subscription_df.loc[idx,'Renewal_Date__c'] = new_renewal_day

							else:
								fileout.write("%s,%s,%s,%s," % (i+1,len(account),sf_cur_df.ix[0]['AccountId_18'],sf_cur_df.ix[0]['sf_op_name']))
								fileout.write("%s,Phase 2 ... STILL No product match,%s,%s,%s,%s,%s,REMOVE 000,001 etc \n" % (cur_op,sf_cur_df.ix[j]['AccountId_18'],sf_cur_df.ix[j]['OpportunityType'],sf_cur_df.ix[j]['OpportunityId'],\
																																				cur_product_df.ix[k]['tmp_Product2Id_match'], \
																		sf_product_df.ix[all_indices_CASE_SENSITIVE(cur_product_df.ix[k]['tmp_Product2Id_match'][:-4],sf_product_df['Product2Id'])[0]]['PricebookEntry']	) )
								printf("%3s . Phase 2 ... STILL No product match ... %s . %20s . %s . %s . %s . REMOVE 000,001 etc \n",cur_op,sf_cur_df.ix[j]['AccountId_18'],sf_cur_df.ix[j]['OpportunityType'], \
																																				sf_cur_df.ix[j]['OpportunityId'],cur_product_df.ix[k]['tmp_Product2Id_match'], \
																		sf_product_df.ix[all_indices_CASE_SENSITIVE(cur_product_df.ix[k]['tmp_Product2Id_match'][:-4],sf_product_df['Product2Id'])[0]]['PricebookEntry']	 )
		
								### 3) Search all values that do NOT have the current opportunity ID AND are not in match_idx 
								final_match_idx = extra_val(no_match_idx,all_indices_CASE_SENSITIVE(sf_cur_df.ix[j]['OpportunityId'],cur_subscription_df.OpportunityId))  
								final_cur_op_product_progression_df = product_progression_df.ix[final_match_idx].reset_index()
								final_cur_match_idx = all_indices_CASE_SENSITIVE(cur_product_df.ix[k]['tmp_Product2Id_match'][:-4],final_cur_op_product_progression_df['Product2Id'])

								if (len(final_cur_match_idx) == 1):
									idx = final_cur_op_product_progression_df.ix[final_cur_match_idx[0]]['index']	
									product_progression_df.loc[idx,cur_op] = 1	

									op_match_idx.append(cur_subscription_df.ix[idx]['OpportunityId'])

									###################################
									# Update current subscription info
									###################################
									product_progression_df.loc[idx,'OpportunityId_cur'] = sf_cur_df.ix[j]['OpportunityId']
									cur_subscription_df.loc[idx,'OpportunityId'] = sf_cur_df.ix[j]['OpportunityId']
									cur_subscription_df.loc[idx,'Subscription_Start_Date__c'] = new_subscription_day
									cur_subscription_df.loc[idx,'Renewal_Date__c'] = new_renewal_day

								else:
									fileout.write("%s,%s,%18s,%s," % (i+1,len(account),sf_cur_df.ix[0]['AccountId_18'],sf_cur_df.ix[0]['sf_op_name']))
									fileout.write("%s,Phase 3 ... WOW No product match,%s,%s,%s,%s,%s,Add ProductId\n" % (cur_op,sf_cur_df.ix[j]['AccountId_18'],sf_cur_df.ix[j]['OpportunityType'], \
																														sf_cur_df.ix[j]['OpportunityId'],cur_product_df.ix[k]['tmp_Product2Id_match'], \
																								sf_product_df.ix[all_indices_CASE_SENSITIVE(cur_product_df.ix[k]['tmp_Product2Id_match'][:-4],sf_product_df['Product2Id'])[0]]['PricebookEntry'] ) ) 	
									printf("%3s . Phase 3 ... WOW No product match   ... %s . %20s . %s . %s . %s . Add ProductId\n",cur_op,sf_cur_df.ix[j]['AccountId_18'],sf_cur_df.ix[j]['OpportunityType'], \
																																	sf_cur_df.ix[j]['OpportunityId'],cur_product_df.ix[k]['tmp_Product2Id_match'], \
																								sf_product_df.ix[all_indices_CASE_SENSITIVE(cur_product_df.ix[k]['tmp_Product2Id_match'][:-4],sf_product_df['Product2Id'])[0]]['PricebookEntry'] ) 	

									update_accounts.append(sf_cur_df.ix[j]['AccountId_18'])
									with open('update_accounts.py','w') as f:
										f.write("update_accounts = [\n")
										for s in update_accounts:
											f.write("'" + s + "',\n")
										f.write("]")

									sys.exit()

					### Set all other products to 0 ... that are still active but NOT in this opportunity
					op_match_idx = list(set(op_match_idx))
					old_column = product_progression_df.columns[len(product_progression_df.columns)-2]
					notnull = pd.notnull(product_progression_df[old_column])[5:]	

					###################################################################
					# if 'Renewal - Downtick'
					# 1) Find opportunities that were NOT touched
					# 2) OR opportunities in op_match_idx that had NO product transfers 
					# 2) Transfer all 0/1's from those opportunities to the current
					####################################################################
					if (sf_cur_df.ix[j]['OpportunityType'] == 'Renewal - Flat'):

						## match non-null values and set to 0
						notnull_df = pd.notnull(product_progression_df.ix[5:][old_column]) 
						notnull_idx = list(notnull_df[(notnull_df == True)].index)
						for k in range(0,len(notnull_idx)):
							if (product_progression_df.ix[notnull_idx[k]][cur_op] != 1): 
								product_progression_df.loc[notnull_idx[k],cur_op] = 0

					else:
						op_match_unique_df = pd.DataFrame(product_progression_df[['OpportunityId_cur',cur_op]][5:].groupby(['OpportunityId_cur'])[cur_op].nunique())
						op_list_NOVAL = list(set(op_match_unique_df[(op_match_unique_df[cur_op] > 0)].index))
						#untouched_op = extra_val(op_match_idx,op_list)

						op_list = list(set(product_progression_df['OpportunityId_cur'][5:]))
						untouched_op = extra_val(op_list,op_match_idx)
	
						match1 = []
						for k in range(0,len(untouched_op)): 
							match1 = match1 + all_indices_CASE_SENSITIVE(untouched_op[k],product_progression_df.OpportunityId_cur)
						match1.sort()

						## remove null values
						notnull_df = pd.notnull(product_progression_df.ix[match1][old_column])	
						notnull_idx = list(notnull_df[(notnull_df == True)].index)
						for k in range(0,len(notnull_idx)):
							if (product_progression_df.ix[notnull_idx[k]][cur_op] != 1): 
								product_progression_df.loc[notnull_idx[k],cur_op] = 0

				else:
					fileout.write("%s,%s,%s,%s," % (i+1,len(account),sf_cur_df.ix[0]['AccountId_18'],sf_cur_df.ix[0]['sf_op_name']) )
					fileout.write("No opportunity match (check if this is a RENEWAL) ,%s,%s\n" % (sf_cur_df.ix[j]['OpportunityId'],sf_cur_df.ix[j]['OpportunityType']) )
					printf("Account = %4s of %4s ... %18s ... %s\n",i+1,len(account),sf_cur_df.ix[0]['AccountId_18'],sf_cur_df.ix[0]['sf_op_name'])
					printf("No opportunity match (check if this is a RENEWAL) ... %s ... %s\n",sf_cur_df.ix[j]['OpportunityId'],sf_cur_df.ix[j]['OpportunityType'])

					#################################################################
					# Missed the opportunity ... update values from prior opporunity
					#################################################################
					old_column = product_progression_df.columns[len(product_progression_df.columns)-2]
					for k in range(5,len(product_progression_df)):
						if (pd.notnull(product_progression_df.ix[k][old_column]) == True): 
							product_progression_df.loc[k,cur_op] = -1 

			elif ('Cancellation' in sf_cur_df.ix[j]['OpportunityType']): ## or 'Video Swap' in sf_cur_df.ix[j]['OpportunityType'] ):

				product_progression_df[cur_op] = np.nan
				product_progression_df.loc[0,cur_op] = sf_cur_df.ix[j]['OpportunityId'] 
				product_progression_df.loc[1,cur_op] = sf_cur_df.ix[j]['OpportunityType'] 
				product_progression_df.loc[2,cur_op] = sf_cur_df.ix[j]['Close_Date'] 
				product_progression_df.loc[3,cur_op] = sf_cur_df.ix[j]['Subscription_Start_Date__c'] 
				product_progression_df.loc[4,cur_op] = sf_cur_df.ix[j]['Renewal_Date__c'] 

				################################################################################################################################
				## 1) Match Renewal Date of current products to New Subscription Date
				## 2) Set to 'NaN' ... any product that is gone from that opportunity
				## 3) Update 'Subscription_Start_Date' and 'Renewal Date' for all products in product_progression_df (ie cur_subscription_df)
				################################################################################################################################
				new_subscription_day = datetime.strptime(sf_cur_df.ix[j]['Subscription_Start_Date__c'],"%Y-%m-%d")
				try:
					new_renewal_day = datetime.strptime(sf_cur_df.ix[j]['Renewal_Date__c'],"%Y-%m-%d")
					fileout.write("%s,%s,%s,%s," % (i+1,len(account),sf_cur_df.ix[0]['AccountId_18'],sf_cur_df.ix[0]['sf_op_name']))
					fileout.write("%s,SHOULD NOT HAVE A RENEWAL DATE FOR CANCELLATION,%s,%s,%s,%s,REMOVE 000,001 etc \n" % (cur_op,sf_cur_df.ix[j]['AccountId_18'],sf_cur_df.ix[j]['OpportunityType'],sf_cur_df.ix[j]['OpportunityId'],cur_product_df.ix[k]['tmp_Product2Id_match']) )
					printf("%3s . SHOULD NOT HAVE A RENEWAL DATE FOR CANCELLATION ... %s . %20s . %s . %s . REMOVE 000,001 etc \n",cur_op,sf_cur_df.ix[j]['AccountId_18'],sf_cur_df.ix[j]['OpportunityType'],sf_cur_df.ix[j]['OpportunityId'],cur_product_df.ix[k]['tmp_Product2Id_match'])
				except:
					new_renewal_day = None
				new_renewal_day = None
	
				match_idx = []
				op_match_idx = []
				for k in range(5,len(cur_subscription_df)):
					if (pd.notnull(cur_subscription_df.ix[k]['Renewal_Date__c']) == True): 
						try:
							cur_renewal_day = datetime.strptime(cur_subscription_df.ix[k]['Renewal_Date__c'],"%Y-%m-%d")
						except:
							cur_renewal_day = cur_subscription_df.ix[k]['Renewal_Date__c']
					
						### Check that renewal date +-days of current subscription start date	
						if ((cur_renewal_day - timedelta(days=renewal_match_delta)) <= new_subscription_day and (cur_renewal_day + timedelta(days=renewal_match_delta)) >= new_subscription_day):
							#printf("k=%s\n",k)
							match_idx.append(k)
							op_match_idx.append(cur_subscription_df.ix[k]['OpportunityId'])

				op_match_idx = list(set(op_match_idx))

				###########################
				# Update subscription info
				###########################
				if (len(match_idx) > 0):
					cur_op_product_progression_df = product_progression_df.ix[match_idx].reset_index()

					## Renumber the op_Product2Id_match column
					idx_cur_op_product_index_df = pd.DataFrame([str(x).zfill(3) for x in cur_op_product_progression_df.groupby(['Product2Id']).cumcount()]).rename(columns = {0:'tmp_Product2Id_match'})
					for k in range(0,len(idx_cur_op_product_index_df)):
						idx_cur_op_product_index_df.ix[k]['tmp_Product2Id_match'] = cur_op_product_progression_df.ix[k]['Product2Id'] + '_' + idx_cur_op_product_index_df.ix[k]['tmp_Product2Id_match']
					cur_op_product_progression_df['op_Product2Id_match'] = [x for x in idx_cur_op_product_index_df['tmp_Product2Id_match']]

					#if (j == 9):
					#	sys.exit()

					for k in range(5,len(cur_product_df)):
						cur_match_idx = all_indices_CASE_SENSITIVE(cur_product_df.ix[k]['tmp_Product2Id_match'],cur_op_product_progression_df['op_Product2Id_match'])

						#### 1) Check if there is a match
						if (len(cur_match_idx) >= 1):
							idx = cur_op_product_progression_df.ix[cur_match_idx[0]]['index']	
							product_progression_df.loc[idx,cur_op] = -1 

							###################################
							# Update current subscription info
							###################################
							product_progression_df.loc[idx,'OpportunityId_cur'] = sf_cur_df.ix[j]['OpportunityId']
							cur_subscription_df.loc[idx,'OpportunityId'] = sf_cur_df.ix[j]['OpportunityId']
							cur_subscription_df.loc[idx,'Subscription_Start_Date__c'] = new_subscription_day
							cur_subscription_df.loc[idx,'Renewal_Date__c'] = new_renewal_day
						else:

							fileout.write("%s,%s,%s,%s," % (i+1,len(account),sf_cur_df.ix[0]['AccountId_18'],sf_cur_df.ix[0]['sf_op_name']))
							fileout.write("%s,Phase 1 . CANCEL . No product match,%s,%s,%s,%s,%s,Search other opportunities etc \n" % (cur_op,sf_cur_df.ix[j]['AccountId_18'],sf_cur_df.ix[j]['OpportunityType'], \
																												sf_cur_df.ix[j]['OpportunityId'],cur_product_df.ix[k]['tmp_Product2Id_match'], \
																								sf_product_df.ix[all_indices_CASE_SENSITIVE(cur_product_df.ix[k]['tmp_Product2Id_match'][:-4],sf_product_df['Product2Id'])[0]]['PricebookEntry'] ) )	
							printf("\nAccount = %4s of %4s ... %18s ... %s\n",i+1,len(account),sf_cur_df.ix[0]['AccountId_18'],sf_cur_df.ix[0]['sf_op_name'])
							printf("%3s . Phase 1 . CANCEL . No product match       ... %s . %20s . %s . %s . %s . Search other opportunities etc \n",cur_op,sf_cur_df.ix[j]['AccountId_18'],sf_cur_df.ix[j]['OpportunityType'], \
																														sf_cur_df.ix[j]['OpportunityId'],cur_product_df.ix[k]['tmp_Product2Id_match'], \
																								sf_product_df.ix[all_indices_CASE_SENSITIVE(cur_product_df.ix[k]['tmp_Product2Id_match'][:-4],sf_product_df['Product2Id'])[0]]['PricebookEntry'] )	

							### 2) Search all values outside of match_idx
							no_match_idx = extra_val(list(product_progression_df.index)[3:],match_idx)
							no_cur_op_product_progression_df = product_progression_df.ix[no_match_idx].reset_index()
							no_cur_match_idx = all_indices_CASE_SENSITIVE(cur_product_df.ix[k]['tmp_Product2Id_match'],no_cur_op_product_progression_df['op_Product2Id_match'])

							if (len(no_cur_match_idx) >= 1):
								idx = no_cur_op_product_progression_df.ix[no_cur_match_idx[0]]['index']	
								product_progression_df.loc[idx,cur_op] = -1

								op_match_idx.append(cur_subscription_df.ix[idx]['OpportunityId'])

								###################################
								# Update current subscription info
								###################################
								product_progression_df.loc[idx,'OpportunityId_cur'] = sf_cur_df.ix[j]['OpportunityId']
								cur_subscription_df.loc[idx,'OpportunityId'] = sf_cur_df.ix[j]['OpportunityId']
								cur_subscription_df.loc[idx,'Subscription_Start_Date__c'] = new_subscription_day
								cur_subscription_df.loc[idx,'Renewal_Date__c'] = new_renewal_day

							else:
								fileout.write("%s,%s,%s,%s," % (i+1,len(account),sf_cur_df.ix[0]['AccountId_18'],sf_cur_df.ix[0]['sf_op_name']))
								fileout.write("%s,Phase 2 . CANCEL . STILL No product match,%s,%s,%s,%s,%s,REMOVE 000,001 etc \n" % (cur_op,sf_cur_df.ix[j]['AccountId_18'],sf_cur_df.ix[j]['OpportunityType'], \
																																sf_cur_df.ix[j]['OpportunityId'],cur_product_df.ix[k]['tmp_Product2Id_match'], \
																								sf_product_df.ix[all_indices_CASE_SENSITIVE(cur_product_df.ix[k]['tmp_Product2Id_match'][:-4],sf_product_df['Product2Id'])[0]]['PricebookEntry'] ) )	
								printf("%3s . Phase 2 . CANCEL . STILL No product match ... %s . %20s . %s . %s . %s . REMOVE 000,001 etc \n",cur_op,sf_cur_df.ix[j]['AccountId_18'],sf_cur_df.ix[j]['OpportunityType'], \
																																			sf_cur_df.ix[j]['OpportunityId'],cur_product_df.ix[k]['tmp_Product2Id_match'], \
																								sf_product_df.ix[all_indices_CASE_SENSITIVE(cur_product_df.ix[k]['tmp_Product2Id_match'][:-4],sf_product_df['Product2Id'])[0]]['PricebookEntry'] ) 
		
								### 3) Search all values that do NOT have the current opportunity ID AND are not in match_idx 
								final_match_idx = extra_val(no_match_idx,all_indices_CASE_SENSITIVE(sf_cur_df.ix[j]['OpportunityId'],cur_subscription_df.OpportunityId))  
								final_cur_op_product_progression_df = product_progression_df.ix[final_match_idx].reset_index()
								final_cur_match_idx = all_indices_CASE_SENSITIVE(cur_product_df.ix[k]['tmp_Product2Id_match'][:-4],final_cur_op_product_progression_df['Product2Id'])

								if (len(final_cur_match_idx) == 1):
									idx = final_cur_op_product_progression_df.ix[final_cur_match_idx[0]]['index']	
									product_progression_df.loc[idx,cur_op] = -1

									op_match_idx.append(cur_subscription_df.ix[idx]['OpportunityId'])

									###################################
									# Update current subscription info
									###################################
									product_progression_df.loc[idx,'OpportunityId_cur'] = sf_cur_df.ix[j]['OpportunityId']
									cur_subscription_df.loc[idx,'OpportunityId'] = sf_cur_df.ix[j]['OpportunityId']
									cur_subscription_df.loc[idx,'Subscription_Start_Date__c'] = new_subscription_day
									cur_subscription_df.loc[idx,'Renewal_Date__c'] = new_renewal_day

								else:
									fileout.write("%s,%s,%18s,%s," % (i+1,len(account),sf_cur_df.ix[0]['AccountId_18'],sf_cur_df.ix[0]['sf_op_name']))
									fileout.write("%s,Phase 3 . CANCEL . WOW No product match,%s,%s,%s,%s,%s,Add ProductId\n" % (cur_op,sf_cur_df.ix[j]['AccountId_18'],sf_cur_df.ix[j]['OpportunityType'], \
																															sf_cur_df.ix[j]['OpportunityId'],cur_product_df.ix[k]['tmp_Product2Id_match'], \
																								sf_product_df.ix[all_indices_CASE_SENSITIVE(cur_product_df.ix[k]['tmp_Product2Id_match'][:-4],sf_product_df['Product2Id'])[0]]['PricebookEntry'] ) )	
									printf("%3s . Phase 3 . CANCEL . WOW No product match   ... %s . %20s . %s . %s . %s . Add ProductId\n",cur_op,sf_cur_df.ix[j]['AccountId_18'],sf_cur_df.ix[j]['OpportunityType'], \
																																		sf_cur_df.ix[j]['OpportunityId'],cur_product_df.ix[k]['tmp_Product2Id_match'], \
																								sf_product_df.ix[all_indices_CASE_SENSITIVE(cur_product_df.ix[k]['tmp_Product2Id_match'][:-4],sf_product_df['Product2Id'])[0]]['PricebookEntry'] )	

									update_accounts.append(sf_cur_df.ix[j]['AccountId_18'])
									with open('update_accounts.py','w') as f:
										f.write("update_accounts = [\n")
										for s in update_accounts:
											f.write("'" + s + "',\n")
										f.write("]")

									sys.exit()

					### Set all other products to 0
					old_column = product_progression_df.columns[len(product_progression_df.columns)-2]
					match1_new = all_indices_CASE_SENSITIVE(-1,product_progression_df[cur_op])
					match1_old = pd.notnull(product_progression_df[old_column])
					for k in range(5,len(match1_old)):
						if (match1_old[k] == True and len(all_indices_CASE_SENSITIVE(k,match1_new)) == 0 ):
							product_progression_df.loc[k,cur_op] = 0
					for k in range(0,len(match1_new)):
						product_progression_df.loc[match1_new[k],cur_op] = float('Nan') 

				else:
					fileout.write("%s,%s,%s,%s," % (i+1,len(account),sf_cur_df.ix[0]['AccountId_18'],sf_cur_df.ix[0]['sf_op_name']) )
					fileout.write("No opportunity match (check if this is a RENEWAL) ,%s,%s\n" % (sf_cur_df.ix[j]['OpportunityId'],sf_cur_df.ix[j]['OpportunityType']) )
					printf("Account = %4s of %4s ... %18s ... %s\n",i+1,len(account),sf_cur_df.ix[0]['AccountId_18'],sf_cur_df.ix[0]['sf_op_name'])
					printf("No opportunity match (check if this is a RENEWAL) ... %s ... %s\n",sf_cur_df.ix[j]['OpportunityId'],sf_cur_df.ix[j]['OpportunityType'])

			### Update op_Product2Id_match with the new OpportunityIds from cur_subscription_df
			new_op_product_index_df = pd.DataFrame([str(x).zfill(3) for x in product_progression_df.groupby(['OpportunityId_cur','Product2Id']).cumcount()]).rename(columns = {0:'op_Product2Id_match'})
			for k in range(5,len(new_op_product_index_df)):
				product_progression_df.loc[k,'op_Product2Id_match'] = product_progression_df.ix[k]['Product2Id'] + '_' + new_op_product_index_df.ix[k]['op_Product2Id_match']
	
			if (j == 30):
				sys.exit()

			#printf("%10s ... %3s ... %s\n",close_date[len(close_date)-1],len(product_list),product_list)

		### Give the sum of the videos

		# 1) Find final op where today > op_close_date
		first_op_idx = list(product_progression_df.columns).index('op_000')
		for k in range(first_op_idx,len(product_progression_df.columns)):
			cur_column = product_progression_df.columns[k]
			#if (datetime.today() >= datetime.strptime(product_progression_df.ix[2][cur_column],"%Y-%m-%d %H:%M:%S")):
			if ( (datetime.today() + timedelta(days=2000)) >= datetime.strptime(product_progression_df.ix[2][cur_column],"%Y-%m-%d %H:%M:%S")):
				max_date_op = product_progression_df.columns[k]

		cur_account.append(sf_cur_df.ix[0]['AccountId_18'])
		final_no_of_videos.append(product_progression_df[max_date_op][5:].count())
		no_of_videos_final_op.append(product_progression_df[max_date_op][5:].sum())

		### Backfill columns so all will append to GLOBAL_product_progression_df
		max_op = product_progression_df.columns[len(product_progression_df.columns)-1]
		next_op = int(max_op.replace('op_','')) + 1
		for k in range(next_op,100):
			cur_op = 'op_' + str(k).zfill(3)
			product_progression_df[cur_op] = np.nan
			product_progression_df.loc[0,cur_op] = 'op_id' 
			product_progression_df.loc[1,cur_op] = 'op_type' 
			product_progression_df.loc[2,cur_op] = 'close_date' 
			product_progression_df.loc[3,cur_op] = 'subscription_start_date' 
			product_progression_df.loc[4,cur_op] = 'renewal_date' 
	
		GLOBAL_product_progression_df = GLOBAL_product_progression_df.append(product_progression_df,ignore_index=True)	
	
	else:	
		printf("No Initial Sale ... %s ... %25s ... %s\n",sf_cur_df.ix[0]['AccountId_18'],sf_cur_df.ix[0]['OpportunityType'],sf_cur_df.ix[0]['sf_op_name'])

	#print product_progression_df

GLOBAL_product_progression_df.to_csv('./output/GLOBAL_product_progression_' + cur_datetime.strftime('%Y%m%d') + '.csv')

video_count_df = pd.DataFrame(cur_account).rename(columns = {0:'AccountId_18'})
video_count_df = pd.merge(video_count_df,pd.DataFrame(final_no_of_videos).rename(columns = {0:'Devon_algo_Nvideo'}),'left',left_index=True,right_index=True)
video_count_df = pd.merge(video_count_df,pd.DataFrame(no_of_videos_final_op).rename(columns = {0:'No_Of_Videos_final_op'}),'left',left_index=True,right_index=True)
video_count_df.to_csv('./output/product_flow_count.csv')

#############################################
# Add algo results to library_completion_df
#############################################
library_completion_df = pd.read_csv('./output/library_completion_' + cur_datetime.strftime('%Y%m%d') + '.csv')
try: 
	library_completion_df = library_completion_df.drop('Devon_algo_Nvideo',1)
except:
	printf('algo_Nvideo ... does not exist\n')
try: 
	library_completion_df = library_completion_df.drop('Cambria_algo_delta',1)
except:
	printf('Cambria_algo_delta ... does not exist\n')
try: 
	library_completion_df = library_completion_df.drop('g2_algo_completion_%',1)
except:
	printf('g2_algo_completion ... does not exist\n')
header = list(library_completion_df.columns[1:])

library_completion_df = pd.merge(library_completion_df,video_count_df,'left',left_on=['AccountId_18'],right_on=['AccountId_18'])
library_completion_df['Cambria_algo_delta'] = library_completion_df['Cambria_sfdc_Nvideo'] - library_completion_df['Devon_algo_Nvideo']
library_completion_df['g2_algo_completion_%'] = library_completion_df['g2_Nvideo_cur']/library_completion_df['Devon_algo_Nvideo']

Naccount = len(library_completion_df)
Nmatch2 = len(library_completion_df[(library_completion_df['Cambria_algo_delta'] <= 2) & (library_completion_df['Cambria_algo_delta'] >= -2)])
Nmatch1 = len(library_completion_df[(library_completion_df['Cambria_algo_delta'] <= 1) & (library_completion_df['Cambria_algo_delta'] >= -1)])
Nmatch0 = len(library_completion_df[(library_completion_df['Cambria_algo_delta'] == 0)])
Nnull = len(library_completion_df[pd.isnull(library_completion_df['Cambria_algo_delta'])])

printf("\nMATCHING ACCOUNT STATS\n")
printf("Total         = %4s\n+-2           = %4s (%6.2f%%)\n+-1           = %4s (%6.2f%%)\nMatching      = %4s (%6.2f%%)\nNo algo value = %4s (%6.2f%%)\n\n\n", \
																												Naccount,Nmatch2,float(Nmatch2)/float(Naccount)*100, \
																												Nmatch1,float(Nmatch1)/float(Naccount)*100, \
																												Nmatch0,float(Nmatch0)/float(Naccount)*100, \
																												Nnull,float(Nnull)/float(Naccount)*100   )
header = header[:header.index('sfdc_Nvideo_NewUpsell')] + ['Devon_algo_Nvideo'] + header[header.index('sfdc_Nvideo_NewUpsell'):] + ['g2_algo_completion_%','Cambria_algo_delta']
library_completion_df.to_csv('./output/library_completion_' + cur_datetime.strftime('%Y%m%d') + '.csv',encoding='utf-8',columns=header)

###########################
# Write results to SFDC
###########################
sfdc_write_library_completion(sf,library_completion_df)

printf("\nMATCHING ACCOUNT STATS\n")
printf("Total         = %4s\n+-2           = %4s (%6.2f%%)\n+-1           = %4s (%6.2f%%)\nMatching      = %4s (%6.2f%%)\nNo algo value = %4s (%6.2f%%)\n\n\n", \
																												Naccount,Nmatch2,float(Nmatch2)/float(Naccount)*100, \
																												Nmatch1,float(Nmatch1)/float(Naccount)*100, \
																												Nmatch0,float(Nmatch0)/float(Naccount)*100, \
																												Nnull,float(Nnull)/float(Naccount)*100   )
