
rm(list=ls(all=TRUE))
graphics.off()
options(width=220)

PLOT_PREDICTION = FALSE
MODEL_TIME_LIMIT = 730
VERIFY_TRAINING_SET = FALSE  # Set to TRUE for standard CV ... Run model on training set / test on training set to verify perfect results 

UPPER_SD_PER = 0.5
LOWER_SD_PER = 0.5

Xsize = 7 
Ysize = 3

TOTAL_FILES = 70 

args = commandArgs(trailingOnly=TRUE)
if (length(args) > 0) {
	CASE = as.integer(args[1])
	ACT_START = (CASE-1) * 15 + 1
	ACT_END = (CASE) * 15
} else {
	CASE = -1
	ACT_START = 1
	ACT_END = TOTAL_FILES
}

cat(sprintf("(ACT_START,ACT_END) = (%3d,%3d)\n",ACT_START,ACT_END))

source("/home/djohnson/analytics/Rlibs/common_libs.R")
source("/home/djohnson/analytics/Rlibs/churn_libs.R")

################## Load packages ##################
suppressMessages(library("ggplot2",quietly = TRUE))         # Graphics engine
suppressMessages(library("RColorBrewer",quietly = TRUE))    # Nice color palettes
suppressMessages(library("plot3D",quietly = TRUE))          # for 3d surfaces. 
suppressMessages(library("plyr",quietly = TRUE))           # Better data manipulations
suppressMessages(library("dplyr",quietly = TRUE))           # Better data manipulations
suppressMessages(library("parallel",quietly = TRUE))        # mclapply for multicore processing
suppressMessages(library("knitr",quietly = TRUE))
suppressMessages(library("stringr",quietly = TRUE))

# Analysis packages.
suppressMessages(library("randomForestSRC",quietly = TRUE)) # random forest for survival, regression and 
                           # classification
suppressMessages(library("ggRandomForests",quietly = TRUE)) # ggplot2 random forest figures (This!)
suppressMessages(library("rms", quietly = TRUE))

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
cur_datetime = as.Date(substr(Sys.time(),1,10))-1
input_files = vector("character",len=0)
#for (i in 1:TOTAL_CASES) {
for (i in ACT_START:ACT_END) {
	tmp_datetime = gsub("-","",toString(cur_datetime-14*(i-1)))
	input_files = append(input_files,sprintf('./input/sdata_all_history_RSFcorrected_%8s.csv',tmp_datetime))
}

TOTAL_CASES = length(input_files)

################ Default Settings ##################
theme_set(theme_bw())     # A ggplot2 theme with white background

## Set open circle for censored, and x for events 
event.marks <- c(1, 4)
event.labels <- c(FALSE, TRUE)

## We want red for death events, so reorder this set.
strCol <- brewer.pal(3, "Set1")[c(2,1,3)]

library("tidyr",quietly=TRUE)        # Transforming wide data into long data (gather)

##################
# Start the clock!
##################
ptm <- proc.time()

##############################
# Cycle through all months
##############################
for(ppp in 1:TOTAL_CASES) {

	#ppp = (ppp-1)*17 + 1	
	input_sdata <- read.csv(file=input_files[ppp], header=TRUE, sep=",",stringsAsFactors=FALSE)
	input_sdata <- filter_df(input_sdata)
	#input_sdata <- input_sdata[input_sdata$Act_CreatedDate > '2015-04-05 00:00:00',]

	if (ppp == 1) {
		cur_datetime = as.Date(max(input_sdata$CreatedDate)) 
		date_out <- gsub("-","",as.character(cur_datetime))  
	} else {
		date_out <- str_sub(str_sub(input_files[ppp],start = -12),1,8)  
	}

	cat(sprintf("\n########################################\n"))
	cat(sprintf("# Case %2s .... %7.2f min\n",ppp+ACT_START-1,as.numeric((proc.time()-ptm)[3])/60 ))
	cat(sprintf("# File %s ... %s\n",input_files[ppp],date_out))
	cat(sprintf("########################################\n"))

	### Need to match types from old model to new data
	input_sdata$close_pullin <- as.integer(input_sdata$close_pullin)

	cur_data <- input_sdata

	## Not displayed ##
	## Set modes correctly. For binary variables: transform to logical
	## Check for range of 0, 1
	## There is probably a better way to do this.
	cur_data[,names(cur_data)=='won'] <- as.logical(cur_data[,names(cur_data)=='won'])
	cur_data[,names(cur_data)=='lost'] <- as.logical(cur_data[,names(cur_data)=='lost'])
	for(ind in 1:dim(cur_data)[2]){
		if(is.character(cur_data[, ind])) {   
			cur_data[, ind] <- factor(cur_data[, ind])
		}
	}
	# Convert age to years
	
	labels <- names(cur_data) 
	
	#### Get class of each column ####
	cls <- sapply(cur_data, class) 
	
	dta.labs <- data.frame(cbind(names = colnames(cur_data), label = labels, type = cls))

	st.labs <- as.character(names(cur_data))
	names(st.labs) <- rownames(dta.labs)

	## Not displayed ##
	# create a data dictionary table
	tmp <- dta.labs
	colnames(tmp) <- c("Variable name", "Description", "Type")
	kable(tmp, 
	      #format="latex",
	      caption = "\\label{T:dataLabs}\\code{cur_data} data set variable dictionary.",
	      row.names = FALSE,
	      booktabs=TRUE)
	
	# Use tidyr::gather to transform the data into long format.
	cnt <- c(which(cls == "numeric" ), which(cls == "integer"))
	fct <- setdiff(1:ncol(cur_data), cnt) # The complement of numeric/integers.
	fct <- c(fct, which(colnames(cur_data) == "tstop"))
	dta <- suppressWarnings(gather(cur_data[,fct], variable, value, -tstop))
	
	# Use tidyr::gather to transform the data into long format.
	cnt <- c(cnt, which(colnames(cur_data) == "won"))
	dta <- gather(cur_data[,cnt], variable, value, -tstop, -won)

	# plot panels for each covariate colored by the logical chas variable.
	if (PLOT_PREDICTION) {
		dev.new(xpos=800,ypos=100,width=Xsize,height=2*Ysize)
		p3 <- ggplot(dta %>% filter(!is.na(value)), 
		       aes(x = tstop, y = value, color = won, shape = won)) +
		geom_point(alpha = 0.4) +
		geom_rug(data = dta[which(is.na(dta$value)),], color = "grey50") +
		labs(y = "", x = st.labs["tstop"], color = "Won", shape = "Won") +
		scale_color_manual(values = strCol) +
		scale_shape_manual(values = event.marks) +
		facet_wrap(~variable, scales = "free_y", ncol = 4) +
		theme(legend.position = c(0.8, 0.2))
		print(p3)
	}
	
	set.seed(1000)
	unique_opid <- sort(unique(cur_data$opid)) 
	
	########################################
	# Variables of Interest
	########################################
	output_var = vector(mode="list")

	# Remove Nop_lost / s4 ... these were the highest as expected
	output_var$n1 = c('opid','Act_CreatedDate','tstop','won','lost','Nemail_total','Ncontact_customer','Ncontact_guidespark',
						'Nfillform_total','Nfillform_good_total','Nfillform_bad_total','Ncall_total','Nmtg_total','Nmtg_cancel_total','Nmtg_noshow_total',
						'Nmtg_completed_total','s1','s2','s3','Nop_created','close_change','close_push','close_pullin',
						'stageback','amount_change','amount_up','amount_down','amount_per','stagebackBIN')
	
	## 1) Random (time irrelevant)
	if (ppp == 1) {
		train_opid <- sample(unique_opid,0.80*length(unique_opid)) 
		opid.train <- unique_opid[which((unique_opid %in% train_opid) == TRUE)] 
		opid.test  <- unique_opid[which(!(unique_opid %in% train_opid) == TRUE)] 

		#######################################################################
		# Convert all to factor to make sure all factors are taken into account
		# 	If not, model will crash
		#######################################################################
		#cur_data.names <- names(cur_data[,!(names(cur_data) %in% c('opid','Act_CreatedDate','tstop','won','lost'))]) 
		#for (i in 1:length(cur_data.names)) {
		#	cur_data[,cur_data.names[i]] <- as.factor(cur_data[,cur_data.names[i]])
		#}

		#cur_data.train <- cur_data[output_var$n1][cur_data$opid %in% opid.train,]
		#cur_data.train$close_pullin <- as.factor(cur_data.train$close_pullin)   ## Why does this arbitrarily stop being a factor??

	}	

	############################################
	# New Train / Test set for each date range
	############################################
	cur_data.train <- cur_data[output_var$n1][cur_data$opid %in% opid.train,]
	cur_data.test <- cur_data[output_var$n1][cur_data$opid %in% opid.test,]

	#######################################################
	# Test that Test Set has same factors as Training Set
	#######################################################

	Ntrain_op = length(unique(cur_data.train$opid))
	Ntest_op  = length(unique(cur_data.test$opid)) 
	cat(sprintf("Opportunity Count (Train,Test) = (%5s,%5s) ... (Train %%, Test %%) = (%6.2f%%,%6.2f%%)\n",Ntrain_op,Ntest_op,
												as.numeric(Ntrain_op) / (as.numeric(Ntrain_op) + as.numeric(Ntest_op)) * 100,
												as.numeric(Ntest_op) / (as.numeric(Ntrain_op) + as.numeric(Ntest_op)) * 100 ) )

	###################################
	###################################
	###################################
	## Random Survival Forest Model 
	###################################
	###################################
	###################################
	rrr = tryCatch({
		rfsrc_cur_data <- rfsrc(Surv(tstop, won) ~ ., data = cur_data.train[,!(names(cur_data.train) %in% c('opid','Act_CreatedDate','lost'))] , 
						forest = T,   # required to use vimp functions
						tree.err = T, # required for plotting tree error
						nsplit = 10, na.action = "na.impute")
		## save this model
		save(rfsrc_cur_data, file = sprintf("./models/rfsrc_act_data_%8s.rda",date_out))

		## This is the same as OOB error ... 1-Cindex
		cat(sprintf("%s%%\n",(1-rcorr.cens(-rfsrc_cur_data$predicted.oob,Surv(cur_data.train$tstop, cur_data.train$won)  )["C Index"]) * 100)) 

		if (PLOT_PREDICTION && ppp == 1) {
			dev.new(xpos=700,ypos=500,width=Xsize,height=2*Ysize)
			p7 <- plot(gg_error(rfsrc_cur_data)) + coord_cartesian(ylim = c(0.09, 0.31))
			print(p7)

			#dev.new(xpos=900,ypos=500,width=2*Xsize,height=2.25*Ysize)
			#p9 <- plot(gg_rfsrc(rfsrc_cur_data), alpha=.2) + 
			#scale_color_manual(values = strCol) + 
			#theme(legend.position = "none") + 
			#labs(y = "1 - Closed Won Probability", x = "Days") +
			#coord_cartesian(ylim = c(-0.01, 1.01),xlim = c(0, 365) )
			#print(p9)
	
			dev.new(xpos=1000,ypos=500,width=Xsize,height=2*Ysize)
			p10 <- plot(gg_vimp(rfsrc_cur_data), lbls = st.labs) + 
			theme(legend.position = c(0.8, 0.2)) + 
			labs(fill = "VIMP > 0")
			print(p10)
		}
	
		#######################################################
		# Plot all OOB predicted survival curves for each input
		#######################################################
		#dev.new(xpos=800,ypos=500,width=Xsize,height=2*Ysize)
		#ggRFsrc <- plot(gg_rfsrc(rfsrc_cur_data), alpha = 0.2) + 
		#  scale_color_manual(values = strCol) + 
		#  theme(legend.position = "none") + 
		#  labs(y = "Survival Probability", x = "Days") +
		#  coord_cartesian(ylim = c(-0.01, 1.01))
		#show(ggRFsrc)
		
		######################################################
		# Test Set Data
		######################################################
		
		if (VERIFY_TRAINING_SET == FALSE) {
			rfsrc_cur_data_test <- predict(rfsrc_cur_data, newdata = cur_data.test[,!(names(cur_data.test) %in% c('opid','Act_CreatedDate','lost'))],
			                          	na.action = "na.impute")
			final_pred <- cur_data.test[,c("opid","Act_CreatedDate","tstop",'won','lost')]
		} else {
			rfsrc_cur_data_test <- predict(rfsrc_cur_data, newdata = cur_data.train[,!(names(cur_data.train) %in% c('opid','Act_CreatedDate','lost'))],
		   		                       	na.action = "na.impute")
			final_pred <- cur_data.train[,c("opid","Act_CreatedDate","tstop",'won','lost')] 
		}
		
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
		
		final_pred['idx'] <- rep(0,nrow(final_pred)) 
		final_pred['time_interest'] <- rep(0,nrow(final_pred)) 
		final_pred['survival'] <- rep(0,nrow(final_pred)) 
		
		max_idx <- length(rfsrc_cur_data_test$time.interest)
		for (i in 1:nrow(final_pred) ) {
			idx <- max(which(rfsrc_cur_data_test$time.interest <= final_pred[i,'tstop']))
			if (idx == Inf) {
				idx <- max_idx
			}
			final_pred[i,'idx'] <- idx
			final_pred[i,'time_interest'] <- rfsrc_cur_data_test$time.interest[idx]
			final_pred[i,'survival'] <- rfsrc_cur_data_test$survival[i,idx]
		}
		
		global_surv_mean = colMeans(rfsrc_cur_data_test$survival)
		global_surv_sd = vector("numeric",length=0)
		for (i in 1:ncol(rfsrc_cur_data_test$survival)) {
			global_surv_sd = append(global_surv_sd,sd(rfsrc_cur_data_test$survival[,i])) 
		}
		global_surv_stats <- data.frame(rfsrc_cur_data_test$time.interest)
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
		final_pred['LOWER_SD_PER'] <- rep(LOWER_SD_PER,nrow(final_pred))
		final_pred['UPPER_SD_PER'] <- rep(UPPER_SD_PER,nrow(final_pred))
		final_pred[which(final_pred['survival'] < (final_pred['global_mean']-final_pred['global_sd']*LOWER_SD_PER)),'pred_status'] <- 3 
		final_pred[which(final_pred['survival'] >= (final_pred['global_mean']-final_pred['global_sd']*LOWER_SD_PER)),'pred_status'] <- 2 
		final_pred[which(final_pred['survival'] >= (final_pred['global_mean']+final_pred['global_sd']*UPPER_SD_PER)),'pred_status'] <- 1 
	
		cur_var <- output_var$n1
		Xmin = 0	
		col = c('red','blue','forestgreen')
		if (PLOT_PREDICTION && ppp == 1) {
			dev.new(xpos=10,ypos=10,width=2*Xsize,height=2.25*Ysize)
			bins = final_pred$outcomeINT
			cur_title = sprintf("Case %3s ... Var = %s",ppp,paste(paste(cur_var[0:9],collapse = ", "),paste(cur_var[10:length(cur_var)],collapse = ", "),sep = '\n'))
			plot(final_pred$tstop,final_pred$survival, pch=bins, xlim=c(Xmin,MODEL_TIME_LIMIT*1.01),ylim=c(0.0,1.0),ylab="1 - Closed Won Probability",
						main=cur_title,col=ifelse(final_pred$outcomeINT==2,col[2],ifelse(final_pred$outcomeINT==1,col[1],col[3])),xaxt='n') 
			lines(global_surv_stats$time,global_surv_stats$global_mean,col='black')
			lines(global_surv_stats$time,(global_surv_stats$global_mean + global_surv_stats$global_sd*0.5),col='black',lty=2)
			lines(global_surv_stats$time,(global_surv_stats$global_mean - global_surv_stats$global_sd*0.5),col='black',lty=2)
			axis(1,c(0,182,365,547,730,912,1095,1277),las=1)
			legend(1,300,pch=sort(unique(bins)),col=col,sort(unique(final_pred$outcome)),bty='o',bg = 'light gray')
		}
	
		input_var <- 'won' 
	
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

		# Print prediction summary  
		print(rfsrc_cur_data_test)
		cat("\n\n")
	
		assign(sprintf("final_pred_%02dmo", ppp-1),final_pred)

		final_pred$cur_time <- rep(date_out,nrow(final_pred))	
		if (exists("output_pred") && is.data.frame(get("output_pred"))) {
			output_pred <- rbind(output_pred,final_pred)
		} else {
			output_pred = final_pred
		}	

	}, error = function(e) {
		cat(sprintf('Error in code for %s\n',date_out))	
	})

}

write.csv(output_pred,file=sprintf("./output/rfsrc_RSF_act_all_%02d.csv",CASE))
write.csv(output_pred,file=sprintf("/media/sf_transfer/rfsrc_RSF_act_all_%02d.csv",CASE))

