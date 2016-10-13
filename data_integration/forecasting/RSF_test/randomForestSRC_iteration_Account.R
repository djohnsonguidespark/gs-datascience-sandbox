
rm(list=ls(all=TRUE))
graphics.off()
options(width=220)

PLOT_PREDICTION = FALSE
MODEL_TIME_LIMIT = 730 
VERIFY_TRAINING_SET = FALSE
OP_COUNT_SPLIT = 10 

Xsize = 7 
Ysize = 3

source("/home/djohnson/analytics/Rlibs/common_libs.R")
source("/home/djohnson/analytics/Rlibs/churn_libs.R")

################## Load packages ##################
suppressMessages(library("ggplot2"))         # Graphics engine
suppressMessages(library("RColorBrewer"))    # Nice color palettes
suppressMessages(library("plot3D"))          # for 3d surfaces. 
suppressMessages(library("plyr"))           # Better data manipulations
suppressMessages(library("dplyr"))           # Better data manipulations
suppressMessages(library("parallel"))        # mclapply for multicore processing
suppressMessages(library("knitr"))
suppressMessages(library("stringr"))

# Analysis packages.
suppressMessages(library("randomForestSRC")) # random forest for survival, regression and 
                           # classification
suppressMessages(library("ggRandomForests")) # ggplot2 random forest figures (This!)

################## Functions ##################
filter_df <- function (data_in) {

	data_in <- data_in[,names(data_in) != 'X']
	data_in <- data_in[data_in$tstart >= 0,]  # activity prior to MSA Effective Date
	
	names(data_in)[names(data_in) == 'AccountId_18'] <- "opid"
	data_in['NaicsCode'] <- substr(data_in$NaicsCode,0,2) 
	
	data_in['stagebackBIN'] <- rep(0,nrow(data_in)) 
	data_in[which(data_in['stageback'] == 0),'stagebackBIN'] <- 0 
	data_in[which(data_in['stageback'] >= 1),'stagebackBIN'] <- 1 
	
	data_in['opid'] <- as.factor(data_in$opid)
	data_in['Product_Line__c'] <- as.factor(data_in$Product_Line__c)
	data_in['event'] <- as.factor(data_in$event)
	data_in['NaicsCode'] <- as.factor(data_in$NaicsCode)
	data_in['LeadSource'] <- as.factor(data_in$LeadSource)
	
	max_tstop <- ddply(data_in[,c('opid','tstop')],~opid,summarise,max_tstop=max(tstop))

	data_in <- merge(x = data_in,y = max_tstop, by = "opid", all.x = TRUE)

	data_in <- data_in[data_in$tstop==data_in$max_tstop,] # remove all except for the final input 

	data_in['max_tstop'] <- NULL
	data_in['NaicsCode'] <- NULL
	data_in['NumberOfEmployees'] <- NULL
	data_in['Product_Line__c'] <- NULL
	data_in['LeadSource'] <- NULL

	rownames(data_in) <- seq(length = nrow(data_in))

	return(data_in)
}

### Get current date
TOTAL_CASES = 70 
cur_datetime = as.Date(substr(Sys.time(),1,10))-1
#cur_datetime = as.Date(substr(Sys.time(),1,10))
input_files = vector("character",len=0)
for (i in 1:TOTAL_CASES) {
	tmp_datetime = gsub("-","",toString(cur_datetime-14*(i-1)))
	input_files = append(input_files,sprintf('./input/sdata_all_history_RSFcorrected_%8s.csv',tmp_datetime))
}
input_files = rev(input_files)

################ Default Settings ##################
theme_set(theme_bw())     # A ggplot2 theme with white background

## Set open circle for censored, and x for events 
event.marks <- c(1, 4)
event.labels <- c(FALSE, TRUE)

## We want red for death events, so reorder this set.
strCol <- brewer.pal(3, "Set1")[c(2,1,3)]

library("tidyr")        # Transforming wide data into long data (gather)

##################
# Start the clock!
##################
ptm <- proc.time()

##############################
# Read in all input files 
##############################
all_file_dates <- vector("character",length=0)
input_rfsrc_all <- read.csv(file='./output/rfsrc_RSF_act_all.csv', header=TRUE, sep=",",stringsAsFactors=FALSE)
 
for(ppp in 1:TOTAL_CASES) {

	cat(sprintf("%4s of %4s ... Reading File = %s .... %7.2f sec\n",ppp,TOTAL_CASES,input_files[ppp],as.numeric((proc.time()-ptm)[3]) ))

	input_sdata <- read.csv(file=input_files[ppp], header=TRUE, sep=",",stringsAsFactors=FALSE)
	input_sdata <- filter_df(input_sdata)
	#input_sdata <- input_sdata[input_sdata$Act_CreatedDate > '2015-04-05 00:00:00',]

	if (ppp > 1) {
		prev_date_out = as.Date(sprintf("%s-%s-%s",substr(date_out,1,4),substr(date_out,5,6),substr(date_out,7,8)))	
	} 

	if (ppp == TOTAL_CASES) {
		cur_datetime = as.Date(max(input_sdata$CreatedDate)) 
		date_out <- gsub("-","",as.character(cur_datetime))  
	} else {
		date_out <- str_sub(str_sub(input_files[ppp],start = -12),1,8)  
	}
	cur_date_out = as.Date(sprintf("%s-%s-%s",substr(date_out,1,4),substr(date_out,5,6),substr(date_out,7,8)))	
	all_file_dates <- append(all_file_dates,cur_date_out)

	if (ppp > 1) {
		TIME_BIN_DELTA <- as.integer(difftime(cur_date_out,prev_date_out,c("days")))  
		TIME_BIN_DELTA <- 14
		#cat(sprintf("%s ... %s ... %s days\n",cur_date_out,prev_date_out,TIME_BIN_DELTA))
	} else {
		orig_datetime = cur_date_out
	} 

	## Not displayed ##
	## Set modes correctly. For binary variables: transform to logical
	## Check for range of 0, 1
	## There is probably a better way to do this.
	input_sdata[,names(input_sdata)=='won'] <- as.logical(input_sdata[,names(input_sdata)=='won'])
	input_sdata[,names(input_sdata)=='lost'] <- as.logical(input_sdata[,names(input_sdata)=='lost'])
	for(ind in 1:dim(input_sdata)[2]){
		if(is.character(input_sdata[, ind])) {   
			input_sdata[, ind] <- factor(input_sdata[, ind])
		}
	}

	input_sdata <- cbind(input_sdata,rep(date_out,nrow(input_sdata)) ) 
	names(input_sdata)[length(input_sdata)] <- 'curdate'

	if (ppp == 1) {
		input_sdata_all <- input_sdata
	} else {
		input_sdata_all <- rbind(input_sdata_all,input_sdata)
	}
	#assign(sprintf("input_sdata_%8s", date_out),input_sdata)
}

input_sdata_all$time_bin = as.integer(input_sdata_all$tstop / TIME_BIN_DELTA) 
rownames(input_sdata_all) <- seq(length = nrow(input_sdata_all))

########################################
# Find unique opportunities 
########################################
unique_op <- as.character(unique(input_sdata_all[input_sdata_all$curdate == gsub("-","",as.character(cur_datetime)),'opid']))

########################################
# Variables of Interest
########################################
output_var = vector(mode="list")
	
output_var$n1 = c('opid','Act_CreatedDate','CreatedDate','time_bin','tstop','curdate','won','lost','Nemail_total','Ncontact_customer','Ncontact_guidespark',
						'Nfillform_total','Nfillform_good_total','Nfillform_bad_total','Ncall_total','Nmtg_total','Nmtg_cancel_total','Nmtg_noshow_total',
						'Nmtg_completed_total','s1','s2','s3','Nop_created','Nop_lost','close_change','close_push','close_pullin',
						'stageback','amount_change','amount_up','amount_down','amount_per','stagebackBIN')
	
#######################################
# Get pred_status for all time_bins
#######################################
max_time_bin = as.integer(as.integer(difftime(cur_datetime,orig_datetime,c("days")))/TIME_BIN_DELTA) + 1
DELTA <- as.integer(length(unique_op) / OP_COUNT_SPLIT)    
unique_date = unique(input_sdata_all$curdate)
for(ppp in 1:(OP_COUNT_SPLIT+1)) {

	cat(sprintf("\n########################################\n"))
	cat(sprintf("# Case %2s .... %7.2f min\n",ppp,as.numeric((proc.time()-ptm)[3])/60 ))
	cat(sprintf("# Opportunities %5s - %5s of %5s\n",(ppp-1)*DELTA+1,ppp*DELTA,length(unique_op)) )
	cat(sprintf("########################################\n"))

	#cur_op_data <- as.data.frame(input_sdata_all[input_sdata_all$opid==unique_op[ppp],output_var$n1])
	cur_op_data <- as.data.frame(input_sdata_all[,output_var$n1])
	cur_op_data <- cur_op_data[!duplicated(cur_op_data[c('opid','tstop')]),]

	if ( (ppp*DELTA) <= length(unique_op)) {
		cur_op_data <- cur_op_data[which(cur_op_data$opid %in% unique_op[(((ppp-1)*DELTA)+1):(ppp*DELTA)]),] 
	} else {
		cur_op_data <- cur_op_data[which(cur_op_data$opid %in% unique_op[(((ppp-1)*DELTA)+1):length(unique_op)]),] 
	}

	cat(sprintf("ALL ... %s\n",match('0063800000by1yz',cur_op_data$opid)))

	## TODO
	## Loop to use all models to avoid any causality issues
	## 1) For all time bins 'j' (time_bin), find pred_status for unique_op[ppp]
	## 2) Use the model that is closest to the date where t_op < t_model
	## 3) Output results for all ops
	for (tn in 1:length(unique_date)) {

		cat(sprintf("Unique_date = %10s .... ",unique_date[tn]))

		###################################
		###################################
		###################################
		## Random Survival Forest Model 
		###################################
		###################################
		###################################

		####################################################
		# 1) Find date 'cur_date' for current time bin 'tn'
		####################################################
		cur_op_time_data <- cur_op_data[which(cur_op_data$curdate == as.integer(toString(unique_date[tn]))),] 
		cat(sprintf("TIME only ... %4s ... ",match('0063800000by1yz',cur_op_time_data$opid)))

		# if cur_op_time_data is not NULL, run through the model
		if (nrow(cur_op_time_data) > 0) {

			cat(sprintf("Data Exists .... %7.2f min\n",as.numeric((proc.time()-ptm)[3])/60 ))

			###################################################
			# 2) Load the proper model 
			# TODO: only allowing final model ... other not working
			###################################################
			rfsrc_model <- get(load(sprintf("./models/rfsrc_act_data_%8s.rda",gsub("-","",as.character(all_file_dates[TOTAL_CASES])))) )	

			#########################
			# 3) Predict survival % 
			#########################
			rfsrc_predict <- predict(rfsrc_model, newdata = cur_op_time_data[,!(names(cur_op_time_data) %in% c('opid','Act_CreatedDate','CreatedDate','time_bin','lost','Nop_lost','curdate'))],
		                          	na.action = "na.impute")
			#rfsrc_predict <- predict(rfsrc_model, newdata = cur_op_time_data[cur_op_time_data$time_bin == (tn-1),!(names(cur_op_time_data) %in% c('opid','Act_CreatedDate','CreatedDate','time_bin','lost'))],
			#                          	na.action = "na.impute")

			#########################
			# 4) Find Pred Status 
			#########################
			final_pred <- cur_op_time_data[,c("opid","Act_CreatedDate","CreatedDate","tstop","won","lost","Nop_lost","time_bin")]
			#final_pred <- cur_op_time_data[cur_op_time_data$time_bin == (tn-1) ,c("opid","Act_CreatedDate","tstop",'won','lost','time_bin')]
	
			#target <- which(names(final_pred) == 'lost')[1]
			#final_pred <- cbind(final_pred[,1:target,drop=F], data.frame(rep(0) * nrow(final_pred)), final_pred[,(target+1):length(final_pred),drop=F]) 
			#names(final_pred[target+1]) <- 'outcome'
			final_pred['outcome'] <- 'Prospect'
			final_pred['outcome'][(final_pred$won == TRUE),] <- 'Won'
			final_pred['outcome'][(final_pred$lost == TRUE),] <- 'Lost'
	
			final_pred['outcomeINT'] <- 2 
			final_pred['outcomeINT'][(final_pred$won == TRUE),] <- 3 
			final_pred['outcomeINT'][(final_pred$lost == TRUE),] <- 1
			#final_pred['outcomeINT'] <- as.numeric(as.factor(final_pred$outcome))  
	
			################################################################
			# Match current probability to current date for all test cases
			###############################################################
		
			#cat(sprintf('nrow(final_pred) = %s\n',nrow(final_pred)))
				
			#row.has.na <- apply(final_pred, 1, function(x){any(is.na(x))})
			#final_pred <- final_pred[!row.has.na,]
		
			final_pred['idx'] <- rep(0,nrow(final_pred)) 
			final_pred['time_interest'] <- rep(0,nrow(final_pred)) 
			final_pred['survival'] <- rep(0,nrow(final_pred)) 
		
			max_idx <- length(rfsrc_predict$time.interest)
			for (i in 1:nrow(final_pred) ) {
				idx <- max(which(rfsrc_predict$time.interest <= final_pred[i,'tstop']))
				if (idx == Inf) {
					idx <- max_idx
				}
				final_pred[i,'idx'] <- idx
				final_pred[i,'time_interest'] <- rfsrc_predict$time.interest[idx]
				final_pred[i,'survival'] <- rfsrc_predict$survival[i,idx]
			}
	
			rownames(final_pred) <- seq(length = nrow(final_pred))

			if (exists("output_pred") && is.data.frame(get("output_pred"))) {
				output_pred <- rbind(output_pred,final_pred)
			} else {
				output_pred = final_pred
			}	

		} else {
			cat(sprintf("No Data .... %7.2f min\n",as.numeric((proc.time()-ptm)[3])/60 ))
		}
	}
}

output_pred <- output_pred[with(output_pred,order(opid,CreatedDate)),]
rownames(output_pred) <- seq(length = nrow(output_pred))

#########################################################
# Match global mean / global sd for each time.interest
#########################################################
# 1) Filter for cur_datetime only
cur_rfsrc_all <- input_rfsrc_all[input_rfsrc_all$cur_time == gsub("-","",as.character(cur_datetime)),c('time_interest','tstop','survival','global_mean','global_sd','LOWER_SD_PER','UPPER_SD_PER','cur_time')]
cur_rfsrc_all <- cur_rfsrc_all[!duplicated(cur_rfsrc_all[c('time_interest')]),]
rownames(cur_rfsrc_all) <- seq(length = nrow(cur_rfsrc_all))

#########################################################################
# 2) Match input_rfsrc_all$time.interest with output_pred$time.interest 
#		NOTE -> may be some missing due to training/test set
#########################################################################
cur_idx <- sort(unique(output_pred$time_interest))
model_idx <- sort(unique(cur_rfsrc_all$time_interest))

#########################################################
# 3) Fill in missing time.interests 
#    Use weighted-average
#########################################################
missing_idx <- setdiff(cur_idx,model_idx)

for (i in 1:length(missing_idx)) {
	pre_idx  <- max(which(cur_rfsrc_all$time_interest < missing_idx[i]))  
	post_idx <- min(which(cur_rfsrc_all$time_interest > missing_idx[i]))  
	if (post_idx != Inf) {
		xn <- missing_idx[i] 
		y1 <- cur_rfsrc_all$global_mean[post_idx]  
		y0 <- cur_rfsrc_all$global_mean[pre_idx]  
		x1 <- cur_rfsrc_all$time_interest[post_idx]
		x0 <- cur_rfsrc_all$time_interest[pre_idx] 
		inp_mean <- (xn-x0) * ((y1-y0) / (x1-x0) + y0 / (xn-x0))  
		y1 <- cur_rfsrc_all$global_sd[post_idx]  
		y0 <- cur_rfsrc_all$global_sd[pre_idx]  
		inp_sd <- (xn-x0) * ((y1-y0) / (x1-x0) + y0 / (xn-x0))  
	} else {
		inp_mean <- cur_rfsrc_all$global_mean[pre_idx]  
		inp_sd <- cur_rfsrc_all$global_sd[pre_idx]
	}

	tmp <- as.data.frame(cbind(missing_idx[i],missing_idx[i],0,inp_mean,inp_sd,cur_rfsrc_all$LOWER_SD_PER[1],cur_rfsrc_all$UPPER_SD_PER[1],cur_rfsrc_all$cur_time[1]))
	names(tmp) <- names(cur_rfsrc_all) 

	if (i == 1) {
		out_tmp <- tmp
	} else {
		out_tmp <- rbind(out_tmp,tmp)
	}	
}

cur_rfsrc_all <- rbind(cur_rfsrc_all,out_tmp)
cur_rfsrc_all <- cur_rfsrc_all[with(cur_rfsrc_all,order(time_interest)),]
rownames(cur_rfsrc_all) <- seq(length = nrow(cur_rfsrc_all))

###################################
# 4) Join to existing predictions
###################################
output_pred <- merge(output_pred,cur_rfsrc_all[,c('time_interest','global_mean','global_sd','LOWER_SD_PER','UPPER_SD_PER')],by.x='time_interest',by.y='time_interest')
#output_pred <- merge(output_pred,cur_rfsrc_all[,c('global_mean','global_sd','LOWER_SD_PER','UPPER_SD_PER')],by.x='time_interest',by.y='time_interest')

##########################
# 5) Get new pred_status 
##########################
output_pred['gainsight_account_health'] <- rep(1,nrow(output_pred)) 
output_pred['pred_status'] <- rep(0,nrow(output_pred)) 
output_pred[which(output_pred['survival'] < (output_pred['global_mean']-output_pred['global_sd']*output_pred['LOWER_SD_PER'])),'pred_status'] <- 3 
output_pred[which(output_pred['survival'] >= (output_pred['global_mean']-output_pred['global_sd']*output_pred['LOWER_SD_PER'])),'pred_status'] <- 2 
output_pred[which(output_pred['survival'] >= (output_pred['global_mean']+output_pred['global_sd']*output_pred['UPPER_SD_PER'])),'pred_status'] <- 1 

output_pred <- output_pred[with(output_pred,order(opid,CreatedDate)),]
rownames(output_pred) <- seq(length = nrow(output_pred))	

write.csv(output_pred,file="./output/rfsrc_act_raw_forecast_data.csv")
write.csv(output_pred,file="/media/sf_transfer/rfsrc_act_raw_forecast_data.csv")


