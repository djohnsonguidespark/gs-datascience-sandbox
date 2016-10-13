#! /usr/bin/env python

#def task_ranking(client,cur_datetime,update_rankings,timing_bin,company_df,attask_input_df,parent_task_df):

#############################################
# Created by DKJ 10/10/2014
#
# Must run after 'task_buffer_PROD.py
# Will not run by itself
#############################################

bin_correction_multiplier = 1

##############################
# Should we update rankings
##############################
if 'update_rankings' in locals():
	printf("\n\n'update_rankings' variable exists:\n\n")
else:
	printf("\n\nTERMINATE PROGRAM ... 'update_rankings' variable DOES NOT exist\n\n")
	sys.exit()

attask_input_df = pd.merge(buffer_df,ranking_df,'left',left_on='ID',right_on='ID')

#######################
# PARENT TASK LOOKUP
#######################
parent_task_lookup = []
parent_task_lookup.append('INITIAL DEVELOPMENT')
parent_task_lookup.append('VIDEO REVIEW CYCLE #1')
parent_task_lookup.append('VIDEO REVIEW CYCLE #2')
parent_task_lookup.append('VIDEO REVIEW CYCLE #3')
parent_task_lookup.append('VIDEO REVIEW CYCLE #4')
parent_task_lookup.append('VIDEO REVIEW CYCLE #5')
parent_task_lookup.append('VIDEO REVIEW CYCLE #6')
parent_task_lookup.append('VIDEO REVIEW CYCLE #7')
parent_task_lookup.append('VIDEO REVIEW CYCLE #8')
parent_task_lookup.append('VIDEO REVIEW CYCLE #9')
parent_task_lookup.append('VIDEO REVIEW CYCLE #10')
parent_task_lookup.append('VIDEO REVIEW CYCLE #11')
parent_task_lookup.append('VIDEO REVIEW CYCLE #12')
parent_task_lookup.append('VIDEO REVIEW CYCLE #13')
parent_task_lookup.append('VIDEO REVIEW CYCLE #14')
parent_task_lookup.append('VIDEO REVIEW CYCLE #15')
parent_task_lookup.append('VIDEO REVIEW CYCLE #16')
parent_task_lookup.append('VIDEO REVIEW CYCLE #17')
parent_task_lookup.append('VIDEO REVIEW CYCLE #18')
parent_task_lookup.append('VIDEO REVIEW CYCLE #19')
parent_task_lookup.append('VIDEO REVIEW CYCLE #20')
parent_task_lookup.append('ADDITIONAL VIDEO EDITS')
parent_task_lookup.append('FINAL APPROVAL & PUBLISH')

#######################################################################################################
# Rank the parent tasks by priority
#######################################################################################################
parent_task_rank = [-1] * len(attask_input_df) 
for i in range(0,len(attask_input_df)):
	try:
		parent_task_rank[i] = parent_task_lookup.index(attask_input_df.PARENT_task[i]) + 1
	except:
		parent_task_rank[i] = len(parent_task_lookup) + 1
		
#for i in range(0,len(parent_task_lookup)):
#	Nidx = all_indices(parent_task_lookup[i],attask_input_df.PARENT_task)
#
#	for j in range(0,len(Nidx)):
#		parent_task_rank[Nidx[j]] = i

attask_input_df = attask_input_df.join(pd.DataFrame(parent_task_rank,columns=['PARENT_task_rank']))

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
		if ('NVIDIA' in attask_input_df.project_name[i]):
			printf(':%4s:%s:%s\n',i,attask_input_df.project_name[i],attask_input_df['hard_deadline'][i])
			#sys.exit()
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
#company_group = company_df.groupby(['company']).aggregate(np.sum).reset_index()
company_group = pd.DataFrame(attask_input_df[['ID','company']]).groupby(['company']).count().reset_index().rename(columns={"ID":"Nlibrary"})
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
	TMP_best_close_date = []
	Cidx = all_indices(company_group['company'][i],attask_input_df['company'])	
	for j in range(0,len(Cidx)):
		TMP_completion_date.append(attask_input_df['PARENT_completion_date_planned'][Cidx[j]])			
		TMP_best_close_date.append(attask_input_df['best_close_date'][Cidx[j]])			

	dict_completion['Cidx'] = Cidx
	dict_completion['PARENT_completion_date'] = TMP_completion_date 
	dict_completion['best_close_date'] = TMP_best_close_date 
	completion_df = pd.DataFrame(dict_completion).sort(['best_close_date','PARENT_completion_date'],ascending=[1,1]).reset_index(drop=True)	

	for j in range(0,len(completion_df)):
		idx = completion_df.ix[j]['Cidx']
		#printf("%2s : %2s : ",j,idx)
		if (attask_input_df['Nlibrary'][idx] != 'nan'):
			#bin_correction[idx] = attask_input_df['Nlibrary'][idx] - int(j/float(bin_correction_multiplier))*bin_correction_multiplier 
			bin_correction[idx] = attask_input_df['Nlibrary'][idx] - int(j/float(bin_correction_multiplier))*bin_correction_multiplier 
			#printf(" %2s\n",attask_input_df['Nlibrary'][idx] - int(j/float(bin_correction_multiplier))*bin_correction_multiplier) 

	#if ('AOL' in company_group['company'][i]):
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

bin_idx_BAK = list(bin_idx)

########################################################################################################################
# Add correction to bin_idx
# a) 1 week per # of videos open
# b) 1st video does not count
#
# How to split large libraries
# c) Split entire library into groups of 4 based on current completion date ... groups defined by 'bin_correction_multiplier'
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
# 3) PARENT_task 
# 4) day_delta ... (>0 to <0 ... <0 means that you are upstream of the deadline)
#
# If timing_bin = 10, may want to add 'day_delta_best' 
###################################################################################################
#attask_input_df = attask_input_df.sort(['BIN_day_delta_best','best_close_date_type','day_delta_best','ranking_flag','day_delta_w_remaining_buffer'],ascending=[0,1,0,0,0]).reset_index(drop=True)
attask_input_df = attask_input_df.sort(['BIN_day_delta_best','best_close_date_type','PARENT_task_rank','day_delta_best'],ascending=[0,1,1,0]).reset_index(drop=True)

attask_input_df.to_csv('/media/sf_transfer/attask_input_df.csv',encoding='utf-8')

#if (timing_bin == 5):
#print(attask_input_df[['project_status','BIN_day_delta_best','best_close_date_type','ranking_flag','day_delta_w_remaining_buffer','day_delta_w_buffer']])

if update_rankings:
	attask_update_success = []
	for i in range(0,len(attask_input_df)):	
		#############################################
		# Update 'Project Ranking' & 'Ranking Flag'
		#############################################
		Nrank = i + 1
		printf('%5s of %5s ... %4s ... %4s . %4s . %6s . %6s ... %100s',i+1,len(attask_input_df),Nrank, \
										attask_input_df.BIN_day_delta_best[i], \
										attask_input_df.best_close_date_type[i], \
										attask_input_df.day_delta_best[i], \
										attask_input_df.status[i], \
										attask_input_df.project_name[i].encode('ascii','ignore') )
		cur_project = attask_input_df.project_name[i]

		try:
			#cur_project = AtTaskObject(client.put(ObjCode.PUT_PROJECT,attask_input_df.ID[i],{'DE:Project Ranking':Nrank,'DE:Ranking Flag':attask_input_df.ranking_flag[i]}),client)
			#cur_project = AtTaskObject(client.put(ObjCode.PUT_PROJECT,attask_input_df.ID[i],{'DE:Project Ranking':Nrank}),client)
			printf(' ... Project Ranking/Ranking Flag UPDATED\n')
			attask_update_success.append(1)
		except:
			printf(' ... Project Ranking/Ranking Flag Update Failed\n********************\n')
			attask_update_success.append(0)

	####################################
	# Add attask_update_success column
	####################################			
	attask_input_df = attask_input_df.join(pd.DataFrame(attask_update_success,columns=['attask_update_success']))

	###############
	# Output File
	###############
	attask_input_df.to_csv('/media/sf_transfer/output/attask_input_df_' + cur_datetime.strftime('%Y%m%d') +'.csv',encoding='utf-8')

###############################
# Create xlsx file
###############################
wb = Workbook()
ws = wb.create_sheet(0)
wb.remove_sheet(wb.get_sheet_by_name('Sheet'))
ws.title = 'project_ranking'

####################
# Format xlsx file
####################
ws.cell(row = 0, column = 0).style.alignment.wrap_text = True
ws.merge_cells('A1:C1')
ws.merge_cells('D1:E1')
for i in range(0,14):
	ws.cell(row = 1, column = i).style.fill.fill_type = Fill.FILL_SOLID 
	ws.cell(row = 1, column = i).style.fill.start_color.index = Color.DARKBLUE 
	ws.cell(row = 1, column = i).style.font.color = Color(Color.WHITE) 
for i in range(0,14):
	ws.cell(row = 0, column = 0).style.alignment.horizontal = "center" 
	ws.cell(row = 0, column = 0).style.alignment.vertical = "center" 

#for j in range(3,30):
#	ws.cell(row = j, column = 5).style.number_format.format_code = "$#,##0" 
	
ws.cell(row = 0,column = 0).value = 'Report Creation Time'
ws.cell(row = 0,column = 3).value = cur_datetime
ws.cell(row = 0,column = 7).value = '=SUBTOTAL(3,H3:H1000000)'
ws.cell(row = 1,column = 0).value = 'Rank'
ws.cell(row = 1,column = 1).value = 'Company'
ws.cell(row = 1,column = 2).value = 'Library Size'
ws.cell(row = 1,column = 3).value = 'Completion Date'
ws.cell(row = 1,column = 4).value = 'Project Name'
ws.cell(row = 1,column = 5).value = 'Project Status'
ws.cell(row = 1,column = 6).value = 'CSM'
ws.cell(row = 1,column = 7).value = 'Current Task'
ws.cell(row = 1,column = 8).value = 'Parent Task'
ws.cell(row = 1,column = 9).value = 'Hard Deadline'
ws.cell(row = 1,column = 10).value = 'Soft Deadline'
ws.cell(row = 1,column = 11).value = 'Day_delta_best'
ws.cell(row = 1,column = 12).value = 'BIN_day_delta_best'
ws.cell(row = 1,column = 13).value = 'Best_close_date_type'

########################
# Output attask_input_df
########################
Icsm = 0
for i in range(0,len(attask_input_df)):
	ws.cell(row = (Icsm+2), column = 0).value = i+1
	ws.cell(row = (Icsm+2), column = 1).value = attask_input_df.company[i] 
	ws.cell(row = (Icsm+2), column = 2).value = attask_input_df.Nlibrary[i] 
	ws.cell(row = (Icsm+2), column = 3).value = str(attask_input_df.PARENT_completion_date_planned[i])[0:10]
	try:
		ws.cell(row = (Icsm+2), column = 4).value = attask_input_df.project_name[i].encode('ascii','ignore') 
	except:
		printf("Skip\n");
	ws.cell(row = (Icsm+2), column = 5).value = attask_input_df.status[i] 
	ws.cell(row = (Icsm+2), column = 6).value = attask_input_df.CSM[i] 
	ws.cell(row = (Icsm+2), column = 7).value = attask_input_df.current_task[i] 
	
	idx = 7   
	idx = idx + 1	
	try:
		if (pd.notnull(attask_input_df.PARENT_task[i])):
			ws.cell(row = (Icsm+2), column = idx).value = attask_input_df.PARENT_task[i] 
		else:
			ws.cell(row = (Icsm+2), column = idx).value = 'NA' 
	except:
		ws.cell(row = (Icsm+2), column = idx).value = 'NA' 

	idx = idx + 1	
	try:
		if (pd.notnull(attask_input_df.hard_deadline[i])):
			ws.cell(row = (Icsm+2), column = idx).value = attask_input_df.hard_deadline[i] 
		else:
			ws.cell(row = (Icsm+2), column = idx).value = 'NA' 
	except:
		ws.cell(row = (Icsm+2), column = idx).value = 'NA' 

	idx = idx + 1
	try:
		if (pd.notnull(attask_input_df.soft_deadline[i])):
			ws.cell(row = (Icsm+2), column = idx).value = attask_input_df.soft_deadline[i] 
		else:
			ws.cell(row = (Icsm+2), column = idx).value = 'NA' 
	except:
		ws.cell(row = (Icsm+2), column = idx).value = 'NA' 

	idx = idx + 1
	try:
		if (pd.notnull(attask_input_df.day_delta_best[i])):
			ws.cell(row = (Icsm+2), column = idx).value = attask_input_df.day_delta_best[i] 
		else:
			ws.cell(row = (Icsm+2), column = idx).value = 'NA' 
	except:
		ws.cell(row = (Icsm+2), column = idx).value = 'NA' 

	idx = idx + 1
	ws.cell(row = (Icsm+2), column = idx).value = attask_input_df.BIN_day_delta_best[i] 

	idx = idx + 1
	try:
		if (pd.notnull(attask_input_df.best_close_date_type[i])):
			ws.cell(row = (Icsm+2), column = idx).value = attask_input_df.best_close_date_type[i] 
		else:
			ws.cell(row = (Icsm+2), column = idx).value = 'NA' 
	except:
		ws.cell(row = (Icsm+2), column = idx).value = 'NA' 

	Icsm = Icsm + 1

Nrow = ws.get_highest_row();
printf('Nrow = %s\n',Nrow)
ws.auto_filter = "A2:N" + str(Nrow) # Turn on Autofilter 
wb.save('/media/sf_transfer/output/project_ranking_' + cur_datetime.strftime('%Y%m%d') +'.xlsx')
	
