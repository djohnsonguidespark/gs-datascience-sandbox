#############################################
# Created by DKJ 10/10/2014
#
# Must run after 'task_buffer_PROD.py
# Will not run by itself
#############################################

if 'update_rankings' in locals():
	printf("\n\n'update_rankings' variable exists:\n\n")
else:
	printf("\n\nTERMINATE PROGRAM ... 'update_rankings' variable DOES NOT exist\n\n")
	sys.exit()

project_week_split = 2

##############################
# Merge data frame for output
##############################
attask_input_df = pd.merge(buffer_df,ranking_df,how='left',right_on='idx',left_index=True).reset_index(drop=True)

#####################################################################################
# Create date field which is hard_deadline .. if None, it becomes soft_deadline
#####################################################################################
best_close_date_type = []
best_close_date = []
day_delta_best = []
for i in range(0,len(attask_input_df)):
	if (attask_input_df['hard_deadline'][i] != None and pd.notnull(attask_input_df['hard_deadline'][i])):
		best_close_date.append(attask_input_df['hard_deadline'][i])
		best_close_date_type.append('hard')
		day_delta_best.append(attask_input_df['day_delta_hard_deadline'][i])
		#if ('FTI' in attask_input_df.project_name[i]):
		#	printf(':%4s:%s\n',i,attask_input_df['hard_deadline'][i])
		#	#sys.exit()
	else: 
		if (attask_input_df['soft_deadline'][i] != None and pd.notnull(attask_input_df['soft_deadline'][i])):
			best_close_date.append(attask_input_df['soft_deadline'][i])
			best_close_date_type.append('soft')
			day_delta_best.append(attask_input_df['day_delta_soft_deadline'][i])
		else:
			best_close_date.append(None)
			best_close_date_type.append(None)
			day_delta_best.append(None)

attask_input_df = attask_input_df.join(pd.DataFrame(best_close_date,columns=['best_close_date']))
attask_input_df = attask_input_df.join(pd.DataFrame(best_close_date_type,columns=['best_close_date_type']))
attask_input_df = attask_input_df.join(pd.DataFrame(day_delta_best,columns=['day_delta_best']))
	
################################################
# Add total number of open projects per company
################################################
company_group = company_df.groupby(['company']).aggregate(np.sum).reset_index()
attask_input_df = pd.merge(attask_input_df,company_group,how='left',right_on='company',left_on='company').reset_index(drop=True)
	
##################################################
# Bin sub-groups for each customer with >4 videos
# KLUGE: best to filter attask_input_df ... but giving 
# issues 
##################################################
dict_completion = {}
bin_correction = [0] * len(attask_input_df)
for i in range(0,len(company_group)):
	Pidx = []
	TMP_completion_date = []
	Cidx = all_indices(company_group['company'][i],attask_input_df['company'])	
	for j in range(0,len(Cidx)):
		TMP_completion_date.append(attask_input_df['final_completion_date_planned'][Cidx[j]])			

	dict_completion['Cidx'] = Cidx
	dict_completion['completion_date'] = TMP_completion_date 
	completion_df = pd.DataFrame(dict_completion).sort('completion_date',ascending=1)	

	for j in range(0,len(Cidx)):
		if (attask_input_df['Nlibrary'][Cidx[j]] != 'nan'):
			bin_correction[Cidx[j]] = attask_input_df['Nlibrary'][Cidx[j]] - int(j/float(project_week_split))*project_week_split 
	
	#if ('VISA' in attask_input_df['company']):
	#	sys.exit()
	
attask_input_df = attask_input_df.join(pd.DataFrame(bin_correction,columns=['bin_correction']))
	
#############################
# Find indices for each bin 
#############################
data = np.array(attask_input_df['day_delta_best'])
bins = np.linspace(-500,500,1000/timing_bin+1)
bin_idx = np.digitize(data,bins)
	
##### Set null values to -1
isnull = list(attask_input_df[pd.isnull(attask_input_df['day_delta_best'])].index)
bin_idx[isnull] = -1
	
bin_idx_BAK = bin_idx
	
########################################################################################################################
# Add correction to bin_idx
# a) 1 week per # of videos open
# b) 1st video does not count
#
# How to split large libraries
# c) Split entire library into groups of 4 based on current completion date ... groups defined by 'project_week_split'
#	1st set is 1 video 	... 1 video with the earliest completion dates (1)
#	2nd set of 5 videos ... 5 videos with the next earliest completion dates (1-6)
#	3rd set of 5 videos ... 5 videos with the next earliest completion dates (7-11)
#	etc ....
#
# d) Distribute groups across timeframe
#
#	Date Configuration for 16 video library	
#	--------------------------------------------------------------------------------------------------------------------
#	|1st set 				|2nd set		 		|3rd set				|4th set				| Hard Deadline			
#	|Hard Deadline - 16 wks	|Hard Deadline - 12 wks |Hard Deadline - 8 wks	|Hard Deadline - 4 wks
#
########################################################################################################################
for i in range(0,len(bin_idx)):
	if (bin_idx[i] != -1):
		#bin_idx[i] = bin_idx[i] + attask_input_df['Nlibrary'][i] - 1
		bin_idx[i] = bin_idx[i] + attask_input_df['bin_correction'][i] - 1
	
attask_input_df = attask_input_df.join(pd.DataFrame(bin_idx,columns=['BIN_day_delta_best']))
	
###################################################################################################
# Rank output
# 1) BIN_day_delta_best (determined by timing_bins
# 2) best_close_date_type (hard vs soft) 
# 3) ranking_flag (c4_fc,c3_fc,c2_fc ....
# 4) day_delta_w_remaining_buffer ... (>0 to <0 ... <0 means that you are upstream of the deadline)
#
# If timing_bin = 10, may want to add 'day_delta_best' 
###################################################################################################
#attask_input_df = attask_input_df.sort(['BIN_day_delta_best','best_close_date_type','day_delta_best','ranking_flag','day_delta_w_remaining_buffer'],ascending=[0,1,0,0,0]).reset_index(drop=True)
attask_input_df = attask_input_df.sort(['BIN_day_delta_best','best_close_date_type','ranking_flag','day_delta_w_remaining_buffer'],ascending=[0,1,0,0]).reset_index(drop=True)
	
#attask_input_filter = attask_input_df[(attask_input_df['ranking_flag'] != '1a_rc1') \
#									& (attask_input_df['ranking_flag'] != '1b_rc2') \
#									& (attask_input_df['ranking_flag'] != '1c_fc')]
	
#if (timing_bin == 5):
print(attask_input_df[['project_status','BIN_day_delta_best','best_close_date_type','ranking_flag','day_delta_w_remaining_buffer','day_delta_w_buffer']])
	
if update_rankings:
	attask_update_success = []
	for i in range(0,len(attask_input_df)):	
		#############################################
		# Update 'Project Ranking' & 'Ranking Flag'
		#############################################
		Nrank = i + 1
		printf('%5s of %5s ... %4s ... %4s . %4s . %6s . %6s . %6s . %3s ... %100s',i+1,len(attask_input_df),Nrank, \
											attask_input_df.BIN_day_delta_best[i], \
											attask_input_df.best_close_date_type[i], \
											attask_input_df.day_delta_best[i], \
											attask_input_df.ranking_flag[i], \
											attask_input_df.day_delta_w_remaining_buffer[i], \
											attask_input_df.project_status[i], \
											attask_input_df.project_name[i].replace('\x92','').replace('\xe7','').replace('\xe8','').replace('\xe9','') )
		cur_project = attask_input_df.project_name[i]
		try:
			cur_project = AtTaskObject(client.put(ObjCode.PUT_PROJECT,attask_input_df.ID[i],{'DE:Project Ranking':Nrank,'DE:Ranking Flag':attask_input_df.ranking_flag[i]}),client)
			printf(' ... Project Ranking/Ranking Flag UPDATED\n')
			attask_update_success.append(1)
		except:
			printf(' ... Project Ranking/Ranking Flag Update Failed\n********************\n')
			attask_update_success.append(0)
		#else:
		#	printf("\n")
	
	####################################
	# Add attask_update_success column
	####################################			
	attask_input_df = attask_input_df.join(pd.DataFrame(attask_update_success,columns=['attask_update_success']))
	
	###############
	# Output File
	###############
	attask_input_df.to_csv('/media/sf_transfer/output/attask_input_df_' + cur_datetime.strftime('%Y%m%d') +'.csv')
	
