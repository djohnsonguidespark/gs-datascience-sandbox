
rm(list=ls(all=TRUE))
graphics.off()
options(width=220)

PLOT_PREDICTION = TRUE
MODEL_TIME_LIMIT = 730 
VERIFY_TRAINING_SET = FALSE

Xsize = 7 
Ysize = 3

source("/home/djohnson/analytics/Rlibs/common_libs.R")
source("/home/djohnson/analytics/Rlibs/churn_libs.R")

################## Load packages ##################
suppressMessages(library("ggplot2"))         # Graphics engine
suppressMessages(library("RColorBrewer"))    # Nice color palettes
suppressMessages(library("plot3D"))          # for 3d surfaces. 
suppressMessages(library("plyr"))            # Better data manipulations
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

input_files = vector("character",len=0)
input_files = append(input_files,'./input/sdata_all_history_RSFcorrected_20161012.csv')

library("tidyr")        # Transforming wide data into long data (gather)

## We want red for death events, so reorder this set.
strCol <- brewer.pal(3, "Set1")[c(2,1,3)]

##################
# Start the clock!
##################
ptm <- proc.time()

##############################
# Cycle through all months
##############################
for(ppp in 1:1) {

	input_sdata <- read.csv(file=input_files[ppp], header=TRUE, sep=",",stringsAsFactors=FALSE)
	input_sdata <- filter_df(input_sdata)
	#input_sdata <- input_sdata[input_sdata$Act_CreatedDate > '2015-04-05 00:00:00',]
	cur_datetime = as.Date(max(input_sdata$CreatedDate)) 

	cat(sprintf("cur_datetime = %s\n",cur_datetime))

	date_out <- gsub("-","",as.character(cur_datetime))  

	rfsrc_model <- get(load(sprintf("./models/rfsrc_act_data_%8s.rda",gsub("-","",as.character(cur_datetime)))) )	

	### Need to match types from old model to new data
	input_sdata$close_pullin <- as.integer(input_sdata$close_pullin)

	## Not displayed ##
	## Set modes correctly. For binary variables: transform to logical
	## Check for range of 0, 1
	## There is probably a better way to do this.
	input_sdata[,names(input_sdata)=='won'] <- as.logical(input_sdata[,names(input_sdata)=='won'])
	input_sdata[,names(input_sdata)=='lost'] <- as.logical(input_sdata[,names(input_sdata)=='lost'])
	for(ind in 1:dim(input_sdata)[2]){
#		if(!is.factor(input_sdata[, ind])){
#			if(length(unique(input_sdata[which(!is.na(input_sdata[, ind])), ind]))<= 2) {
#				if(sum(range(input_sdata[, ind], na.rm = TRUE) ==  c(0, 1)) ==  2){
#					input_sdata[, ind] <- as.logical(input_sdata[, ind])
#				}
#			}
#		} else {
#			if (length(unique(input_sdata[which(!is.na(input_sdata[, ind])), ind]))<= 2) {
#				if(sum(sort(unique(input_sdata[, ind])) ==  c(0, 1)) ==  2){
#					input_sdata[, ind] <- as.logical(input_sdata[, ind])
#				}
#				if(sum(sort(unique(input_sdata[, ind])) ==  c(FALSE, TRUE)) ==  2){
#					input_sdata[, ind] <- as.logical(input_sdata[, ind])
#				}
#			}
#		 }
#		#if(!is.logical(input_sdata[, ind]) & length(unique(input_sdata[which(!is.na(input_sdata[, ind])), ind]))<= 5) {   
#		#	cat(ind,"\n")
#		#	input_sdata[, ind] <- factor(input_sdata[, ind])
#		#}
		if(is.character(input_sdata[, ind])) {   
			input_sdata[, ind] <- factor(input_sdata[, ind])
		}
	}

	###################################
	###################################
	###################################
	## Random Survival Forest Model 
	###################################
	###################################
	###################################

	
	########################################
	# Variables of Interest
	########################################
	output_var = vector(mode="list")

	# Remove Nop_lost / s4 ... these were the highest as expected
	output_var$n1 = c('opid','Act_CreatedDate','tstop','won','lost','Nemail_total','Ncontact_customer','Ncontact_guidespark',
						'Nfillform_total','Nfillform_good_total','Nfillform_bad_total','Ncall_total','Nmtg_total','Nmtg_cancel_total','Nmtg_noshow_total',
						'Nmtg_completed_total','s1','s2','s3','Nop_created','Nop_lost','close_change','close_push','close_pullin',
						'stageback','amount_change','amount_up','amount_down','amount_per','stagebackBIN')
	
	## 1) Random (time irrelevant)
	cur_input_sdata <- input_sdata[output_var$n1]

	if (VERIFY_TRAINING_SET == FALSE) {
		rfsrc_pred <- predict(rfsrc_model, newdata = cur_input_sdata[,!(names(cur_input_sdata) %in% c('opid','Act_CreatedDate','CreatedDate','time_bin','Nop_lost','lost'))],
		                          	na.action = "na.impute")
		final_pred <- cur_input_sdata[,c("opid","Act_CreatedDate","tstop","won",'Nop_lost',"lost")]
	}
	
	if (PLOT_PREDICTION) {
		dev.new(xpos=700,ypos=500,width=Xsize,height=2*Ysize)
		p7 <- plot(gg_error(rfsrc_model)) + coord_cartesian(ylim = c(0.0, 0.31))
		print(p7)


	}
	
	final_pred['outcome'] <- 'Prospect'
	final_pred['outcome'][(final_pred$won == TRUE),] <- 'Won'
	final_pred['outcome'][(final_pred$lost == TRUE),] <- 'Lost'
	final_pred['outcome'][(final_pred$Nop_lost == TRUE),] <- 'Lost'
	
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
	
	max_idx <- length(rfsrc_pred$time.interest)
	for (i in 1:nrow(final_pred) ) {
		idx <- max(which(rfsrc_pred$time.interest <= final_pred[i,'tstop']))
		if (idx == Inf) {
			idx <- max_idx
		}
		final_pred[i,'idx'] <- idx
		final_pred[i,'time_interest'] <- rfsrc_pred$time.interest[idx]
		final_pred[i,'survival'] <- rfsrc_pred$survival[i,idx]
	}
	
	global_surv_mean = colMeans(rfsrc_pred$survival)
	global_surv_sd = vector("numeric",length=0)
	for (i in 1:ncol(rfsrc_pred$survival)) {
		global_surv_sd = append(global_surv_sd,sd(rfsrc_pred$survival[,i])) 
	}
	global_surv_stats <- data.frame(rfsrc_pred$time.interest)
	global_surv_stats <- cbind(global_surv_stats,data.frame(global_surv_mean))
	global_surv_stats <- cbind(global_surv_stats,data.frame(global_surv_sd))
	colnames(global_surv_stats) <- c('time','global_mean','global_sd')
	
	final_pred <- final_pred[with(final_pred,order(tstop)),]
	row.names(final_pred) <- 1:nrow(final_pred)
	final_pred <- merge(final_pred,global_surv_stats,by.x='time_interest',by.y='time')
	
	###################################################
	# Err on cautious side (after cross-validation)
	# 1) Equal will move to bottom bin
	# Good b/c lower bins will be monitored
	###################################################
	final_pred['gainsight_account_health'] <- rep(1,nrow(final_pred)) 
	final_pred['pred_status'] <- rep(0,nrow(final_pred)) 
	final_pred[which(final_pred['survival'] < (final_pred['global_mean']-final_pred['global_sd']*0.5)),'pred_status'] <- 3 
	final_pred[which(final_pred['survival'] >= (final_pred['global_mean']-final_pred['global_sd']*0.5)),'pred_status'] <- 2 
	final_pred[which(final_pred['survival'] >= (final_pred['global_mean']+final_pred['global_sd']*0.5)),'pred_status'] <- 1 
	
	cur_var <- output_var$n1
	Xmin = 0	
	col = c('red','blue','forestgreen')
	if (PLOT_PREDICTION) {
		dev.new(xpos=10,ypos=10,width=2*Xsize,height=2.25*Ysize)
		#bins = as.integer(final_pred$outcome)
		bins = final_pred$outcomeINT
		cur_title = sprintf("Case %3s ... Var = %s",ppp,paste(paste(cur_var[0:9],collapse = ", "),paste(cur_var[10:length(cur_var)],collapse = ", "),sep = '\n'))
		plot(final_pred$tstop,final_pred$survival, pch=bins, xlim=c(Xmin,365*1.01),ylim=c(0.0,1.0),ylab="1 - Closed Won Probability",
						main=cur_title,col=ifelse(final_pred$outcomeINT==2,col[2],ifelse(final_pred$outcomeINT==1,col[1],col[3])),xaxt='n') 
		#lines(basehaz_fit$time,basehaz_fit$baseline_survival,col='black')
		lines(global_surv_stats$time,global_surv_stats$global_mean,col='black')
		lines(global_surv_stats$time,(global_surv_stats$global_mean + global_surv_stats$global_sd*0.5),col='black',lty=2)
		lines(global_surv_stats$time,(global_surv_stats$global_mean - global_surv_stats$global_sd*0.5),col='black',lty=2)
		#points(final_pred$tstop,fitted(poly_fit),col='green')
		#lines(sort(final_pred$time),fitted(poly_fit)[order(final_pred$time)],col='red',type='b')
		axis(1,c(0,182,365,547,730,912,1095,1277),las=1)
		#axis(1,c(0,182,365,547,730,912,1095,1277,1460,1642,1825,2008,2190,2373),las=1)
		legend(325,1,pch=sort(unique(bins)),col=col,sort(unique(final_pred$outcome)),bty='o',bg = 'light gray')
	}
	
	input_var <- 'won' 
	#nochurn_pred <- final_pred[which(final_pred[input_var] == 0),]
	#nochurn_tab <- table(factor(nochurn_pred$pred_status),factor(nochurn_pred$gainsight_account_health))	
	
	#final_pred <- final_pred[(final_pred$time_interest > 30),]
	pred <- factor(final_pred$pred_status)
	actual <- factor(final_pred[,input_var])
	xtab <- table(pred,actual)	
	
	print(xtab)
	
	cox_churn_fit = c() 
	#new_row <- churn_stats('won','All-Time','',final_pred,cox_churn_fit)
	#year_row1 <- data.frame(churn_stats('won','Yr','1',final_pred,cox_churn_fit),stringsAsFactors=FALSE )
	#year_row2 <- data.frame(churn_stats('won','Yr','2',final_pred,cox_churn_fit),stringsAsFactors=FALSE )
	#year_row3 <- data.frame(churn_stats('won','Yr','3',final_pred,cox_churn_fit),stringsAsFactors=FALSE )
	#year_row4 <- data.frame(churn_stats('won','Yr','4',final_pred,cox_churn_fit),stringsAsFactors=FALSE )
	#year_row5 <- data.frame(churn_stats('won','Yr','5',final_pred,cox_churn_fit),stringsAsFactors=FALSE )

	assign(sprintf("final_pred_%02dmo", ppp-1),final_pred)

	final_pred$cur_time <- rep(date_out,nrow(final_pred))	
	output_pred = final_pred
}

write.csv(output_pred,file="./output/rfsrc_pred.csv")
write.csv(output_pred,file="/media/sf_transfer/rfsrc_pred.csv")

#for(ppp in 1:(TOTAL_CASES+1)) {
#
#	if (ppp == 1) {
#		final_pred_00mo$month <- rep(-(ppp-1),nrow(final_pred_00mo))
#		output_pred <- final_pred_00mo
#	} else {
#		assign(sprintf("input_sdata_%02dmo$month", ppp-1),rep(-(ppp-1),nrow(get(sprintf("input_sdata_%02dmo", ppp-1))) ))
#	}
#
#}

