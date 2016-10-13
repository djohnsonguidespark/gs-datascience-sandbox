
rm(list=ls(all=TRUE))
graphics.off()
options(width=220)

PLOT_PREDICTION = TRUE
MODEL_TIME_LIMIT = 1095
VERIFY_TRAINING_SET = FALSE  # Set to TRUE for standard CV ... Run model on training set / test on training set to verify perfect results 

UPPER_SD_PER = 0.5
LOWER_SD_PER = 0.5

Xsize = 7 
Ysize = 3

source("/home/djohnson/analytics/Rlibs/common_libs.R")
source("/home/djohnson/analytics/Rlibs/churn_libs.R")

################## Load packages ##################
library("ggplot2",quietly = TRUE)         # Graphics engine
library("RColorBrewer",quietly = TRUE)    # Nice color palettes
library("plot3D",quietly = TRUE)          # for 3d surfaces. 
library("plyr",quietly = TRUE)           # Better data manipulations
library("dplyr",quietly = TRUE)           # Better data manipulations
library("parallel",quietly = TRUE)        # mclapply for multicore processing
library("knitr",quietly = TRUE)
library("survival",quietly = TRUE)
library("risksetROC",quietly = TRUE)

# Analysis packages.
library("randomForestSRC",quietly = TRUE) # random forest for survival, regression and 
                           # classification
library("ggRandomForests",quietly = TRUE) # ggplot2 random forest figures (This!)
library("rms", quietly = TRUE)

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
	
	max_tstop <- ddply(data_in[c('opid','tstop')],~opid,summarise,max_tstop=max(tstop))

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

################ Default Settings ##################
theme_set(theme_bw())     # A ggplot2 theme with white background

## Set open circle for censored, and x for events 
event.marks <- c(1, 4)
event.labels <- c(FALSE, TRUE)

## We want red for death events, so reorder this set.
strCol <- brewer.pal(3, "Set1")[c(2,1,3)]

library("tidyr",quietly=TRUE)        # Transforming wide data into long data (gather)

##################
# Load data
##################
input_sdata <- read.csv(file="sdata_all_history_RSFcorrected.csv", header=TRUE, sep=",",stringsAsFactors=FALSE)
account_df <- read.csv(file="../../output/account_df.csv",header=TRUE,sep=",",stringsAsFactors=FALSE)

cat(sprintf('After Initial Read         ... No of Accounts: %4s\n',length(unique(input_sdata$AccountId_18))) )

input_sdata_raw <- input_sdata

###############################
# Remove meaningless data
###############################
input_sdata <- filter_df(input_sdata)
#input_sdata <- input_sdata[input_sdata$Act_CreatedDate > '2015-04-05 00:00:00',]  # Day that CloseDate / Amount changes were logged in op history

pbc <- input_sdata

## Not displayed ##
## Set modes correctly. For binary variables: transform to logical
## Check for range of 0, 1
## There is probably a better way to do this.
for(ind in 1:dim(pbc)[2]){
	if(!is.factor(pbc[, ind])){
		if(length(unique(pbc[which(!is.na(pbc[, ind])), ind]))<= 2) {
			if(sum(range(pbc[, ind], na.rm = TRUE) ==  c(0, 1)) ==  2){
				pbc[, ind] <- as.logical(pbc[, ind])
			}
		}
	} else {
		if (length(unique(pbc[which(!is.na(pbc[, ind])), ind]))<= 2) {
			if(sum(sort(unique(pbc[, ind])) ==  c(0, 1)) ==  2){
				pbc[, ind] <- as.logical(pbc[, ind])
			}
			if(sum(sort(unique(pbc[, ind])) ==  c(FALSE, TRUE)) ==  2){
				pbc[, ind] <- as.logical(pbc[, ind])
			}
		}
	 }
	#if(!is.logical(pbc[, ind]) & length(unique(pbc[which(!is.na(pbc[, ind])), ind]))<= 5) {   
	#	cat(ind,"\n")
	#	pbc[, ind] <- factor(pbc[, ind])
	#}
	if(is.character(pbc[, ind])) {   
		pbc[, ind] <- factor(pbc[, ind])
	}
}

labels <- names(pbc) 

#### Get class of each column ####
cls <- sapply(pbc, class) 

dta.labs <- data.frame(cbind(names = colnames(pbc), label = labels, type = cls))
# Put the "years" variable on tops
#dta.labs <- rbind(dta.labs[nrow(dta.labs),], dta.labs[-nrow(dta.labs),])

st.labs <- as.character(names(pbc))
names(st.labs) <- rownames(dta.labs)

## Not displayed ##
# create a data dictionary table
tmp <- dta.labs
colnames(tmp) <- c("Variable name", "Description", "Type")
kable(tmp, 
      #format="latex",
      caption = "\\label{T:dataLabs}\\code{pbc} data set variable dictionary.",
      row.names = FALSE,
      booktabs=TRUE)

#print(tmp)
#cat("\n\n")

# Use tidyr::gather to transform the data into long format.
cnt <- c(which(cls == "numeric" ), which(cls == "integer"))
fct <- setdiff(1:ncol(pbc), cnt) # The complement of numeric/integers.
fct <- c(fct, which(colnames(pbc) == "tstop"))
dta <- suppressWarnings(gather(pbc[,fct], variable, value, -tstop))

# plot panels for each covariate colored by the logical chas variable.
#dev.new(xpos=0,ypos=100,width=Xsize,height=2*Ysize)
#p2 <- ggplot(dta, aes(x = tstop, fill = value)) +
#  geom_histogram(color = "black", binwidth = 1) +
#  labs(y = "", x = st.labs["tstop"]) +
#  scale_fill_brewer(palette="RdBu",na.value = "white" ) +   ## palette automatic ... if na, color = white
#  facet_wrap(~variable, scales = "free_y", nrow = 2) +
#  theme(legend.position = "none")
#print(p2)


# Use tidyr::gather to transform the data into long format.
cnt <- c(cnt, which(colnames(pbc) == "won"))
dta <- gather(pbc[,cnt], variable, value, -tstop, -won)

# plot panels for each covariate colored by the logical chas variable.
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

# create a missing data table
#pbc.train <- pbc %>% filter(!is.na(treatment))
#st <- apply(pbc,2, function(rw){sum(is.na(rw))})
#st.t <- apply(pbc.train,2, function(rw){sum(is.na(rw))})
#st <- data.frame(cbind(full = st, train = st.t))
#st <- st[which(st$full>0),]
#colnames(st) <- c("pbc", "pbc.train")
#
#kable(st, 
#   	       format="latex",
#           caption = "\\label{T:missing}Missing value counts in \\code{pbc} data set and pbc clinical train observations (\\code{pbc.train}).",
#           digits = 3,
#           booktabs=TRUE)
#print(st)
#cat("\n\n")
#
## Create the train and test data sets.
#pbc.train <- pbc %>% filter(!is.na(treatment))
#pbc.test <- pbc %>% filter(is.na(treatment))
#

set.seed(1000)
#set.seed(5000)
#churn_sfdc <- sort(unique(input_sdata[which(input_sdata$churn==1),]$sfdc)) 
unique_opid <- sort(unique(pbc$opid)) 

########################################
# Variables of Interest
########################################
output_var = vector(mode="list")
output_var$n1 = c('opid','Act_CreatedDate','tstop','won','lost','Nemail_total','Ncontact_customer','Ncontact_guidespark',
					'Nfillform_total','Nfillform_good_total','Nfillform_bad_total','Ncall_total','Nmtg_total','Nmtg_cancel_total','Nmtg_noshow_total',
					'Nmtg_completed_total','s1','s2','s3','s4','Nop_created','Nop_lost','close_change','close_push','close_pullin',
					'stageback','amount_change','amount_up','amount_down','amount_per','stagebackBIN')

# Remove Nop_lost / s4 ... these were the highest as expected
output_var$n1 = c('opid','Act_CreatedDate','tstop','won','lost','Nemail_total','Ncontact_customer','Ncontact_guidespark',
					'Nfillform_total','Nfillform_good_total','Nfillform_bad_total','Ncall_total','Nmtg_total','Nmtg_cancel_total','Nmtg_noshow_total',
					'Nmtg_completed_total','s1','s2','s3','Nop_created','Nop_lost','close_change','close_push','close_pullin',
					'stageback','amount_change','amount_up','amount_down','amount_per','stagebackBIN')

## 1) Random (time irrelevant)
train_opid <- sample(unique_opid,0.80*length(unique_opid)) 
opid.train <- unique_opid[which((unique_opid %in% train_opid) == TRUE)] 
opid.test  <- unique_opid[which(!(unique_opid %in% train_opid) == TRUE)] 

pbc.train <- pbc[output_var$n1][pbc$opid %in% opid.train,]
pbc.test <- pbc[output_var$n1][pbc$opid %in% opid.test,]

# Create the gg_survival object ... USE SINGLE VARIABLE FOR KAPLAN-MEIER
gg_dta <- gg_survival(interval = "tstop",
                      censor = "won", 
                      by = "stageback", 
                      data = pbc.train, 
                      conf.int = 0.95)

#########################################
## Kaplan-Meier for discrete variable
#########################################
#dev.new(xpos=0,ypos=500,width=Xsize,height=2*Ysize)
#p4 <- plot(gg_dta) +
#  labs(y = "Survival Probability", x = "Observation Time (years)", 
#       color = "stageback", fill = "stageback") +
#  theme(legend.position = c(0.2, 0.2)) +
#  coord_cartesian(y = c(0, 1.01))
#print(p4)

#######################################################
# Kaplan-Meier cumulative hazard for discrete variable
#######################################################
#dev.new(xpos=500,ypos=500,width=Xsize,height=2*Ysize)
#p5 <- plot(gg_dta, type = "cum_haz") +
#  labs(y = "Cumulative Hazard", x = "Observation Time (years)", 
#       color = "Treatment", fill = "Treatment") +
#  theme(legend.position = c(0.2, 0.8)) +
#  coord_cartesian(ylim = c(-0.02, 1.22))
#print(p5)
#
########################################
# Kaplan-Meier for continuous variable
########################################
#pbc.s3 <- pbc.train
#pbc.s3$s3_grp <- cut(pbc.s3$s3, breaks = c(-1,0, 15, 30, 50, 150))
#
#dev.new(xpos=600,ypos=500,width=Xsize,height=2*Ysize)
#p6 <- plot(gg_survival(interval = "tstop", censor = "won", by = "s3_grp", 
#                 data = pbc.s3), error = "none") +
#				labs(y = "Survival Probability", x = "Observation Time (years)",color = "stageback")
#print(p6)
#
### Not displayed ##
## Create a table summarizing the ph model from fleming and harrington 1991
#fleming.table <- data.frame(matrix(ncol = 3, nrow = 5))
#rownames(fleming.table) <- 
#  c("Age", "log(Albumin)", "log(Bilirubin)", "Edema", "log(Prothrombin Time)")
#colnames(fleming.table) <- c("Coef.", "Std. Err.", "Z stat.")
#fleming.table[,1] <- c(0.0333, -3.0553,0.8792, 0.7847, 3.0157) 
#fleming.table[,2] <- c(0.00866, 0.72408,0.09873,0.29913,1.02380) 
#fleming.table[,3] <- c(3.84,-4.22,8.9,2.62,2.95) 
#
#kable(fleming.table, 
#      format="latex",
#      caption = "\\label{T:FHmodel}\\code{pbc} proportional hazards model summary of 312 randomized cases in \\code{pbc.train} data set. ~\\citep[Table 4.4.3c]{fleming:1991} ", 
#      digits = 3,
#      booktabs=TRUE)
#
#print(fleming.table)
#cat("\n\n")
#
###################################
###################################
###################################
## Random Survival Forest Model 
###################################
###################################
###################################
rfsrc_pbc <- rfsrc(Surv(tstop, won) ~ ., data = pbc.train[,!(names(pbc.train) %in% c('opid','Act_CreatedDate','lost','Nop_lost'))] , 
					forest = T,   # required to use vimp functions
					tree.err = T, # required for plotting tree error
					nsplit = 10, na.action = "na.impute")

timeROC = c(10,30,90,150,180,270,365,550,730)
for (i in 1:length(timeROC)) {
	dev.new(xpos=700+50*(i-1),ypos=500,width=Xsize,height=2*Ysize)
	w.ROC = risksetROC(Stime = pbc.train$tstop,  
			status = pbc.train$won, 
			marker = rfsrc_pbc$predicted.oob, 
			predict.time = timeROC[i], 
			method = "Cox", 
			main = paste("OOB Survival ROC Curve at t=", 
								timeROC[i]),
			plot=TRUE, 
			lwd = 3, 
			col = "red" )

	cat(sprintf("time = %3s ... AUC = %s\n",timeROC[i],w.ROC$AUC))
}


## in reality, we use data caching to make vignette 
## compilation quicker. The rfsrc_pbc forest is stored
## as a ggRandomForests data sets
##
## This code block produces the R output from the 
## rfsrc grow block above. We set the chunk argument 
## "echo=FALSE" above so this code does not show up 
## in the manuscript.
#data("rfsrc_pbc", package = "ggRandomForests")
#
#print(rfsrc_pbc)
#cat("\n\n")
#
dev.new(xpos=700,ypos=500,width=Xsize,height=2*Ysize)
p7 <- plot(gg_error(rfsrc_pbc)) + coord_cartesian(ylim = c(0.0, 0.2))
print(p7)

#######################################################
# Plot all OOB predicted survival curves for each input
#######################################################
#dev.new(xpos=800,ypos=500,width=Xsize,height=2*Ysize)
#ggRFsrc <- plot(gg_rfsrc(rfsrc_pbc), alpha = 0.2) + 
#  scale_color_manual(values = strCol) + 
#  theme(legend.position = "none") + 
#  labs(y = "Survival Probability", x = "Days") +
#  coord_cartesian(ylim = c(-0.01, 1.01))
#show(ggRFsrc)

############################################################
# Median predicted survival curves ... 95% confidence bands 
############################################################
#dev.new(xpos=900,ypos=500,width=Xsize,height=2*Ysize)
#p8 <- plot(gg_rfsrc(rfsrc_pbc, by = "stageback")) +  
#  theme(legend.position = c(0.2, 0.2)) + 
#  labs(y = "Survival Probability", x = "Days") +
#  coord_cartesian(ylim = c(-0.01, 1.01))
#print(p8)

######################################################
# Test Set Data
######################################################

if (VERIFY_TRAINING_SET == FALSE) {
	rfsrc_pbc_test <- predict(rfsrc_pbc, newdata = pbc.test[,!(names(pbc.test) %in% c('opid','Act_CreatedDate','Nop_lost','lost'))],
	                          	na.action = "na.impute")
	final_pred <- pbc.test[,c("opid","Act_CreatedDate","tstop",'won','lost','Nop_lost')]
} else {
	rfsrc_pbc_test <- predict(rfsrc_pbc, newdata = pbc.train[,!(names(pbc.train) %in% c('opid','Act_CreatedDate','Nop_lost','lost'))],
   		                       	na.action = "na.impute")
	final_pred <- pbc.train[,c("opid","Act_CreatedDate","tstop",'won','lost','Nop_lost')] 
}

#target <- which(names(final_pred) == 'lost')[1]
#final_pred <- cbind(final_pred[,1:target,drop=F], data.frame(rep(0) * nrow(final_pred)), final_pred[,(target+1):length(final_pred),drop=F]) 
#names(final_pred[target+1]) <- 'outcome'
final_pred['outcome'] <- 'Prospect'
final_pred['outcome'][(final_pred$won == TRUE),] <- 'Won'
final_pred['outcome'][(final_pred$lost == TRUE),] <- 'Lost'
final_pred['outcome'][(final_pred$Nop_lost == TRUE),] <- 'Lost'

final_pred['outcomeINT'] <- as.numeric(as.factor(final_pred$outcome))  

################################################################
# Match current probability to current date for all test cases
###############################################################

#cat(sprintf('nrow(final_pred) = %s\n',nrow(final_pred)))
	
#row.has.na <- apply(final_pred, 1, function(x){any(is.na(x))})
#final_pred <- final_pred[!row.has.na,]

final_pred['idx'] <- rep(0,nrow(final_pred)) 
final_pred['time_interest'] <- rep(0,nrow(final_pred)) 
final_pred['survival'] <- rep(0,nrow(final_pred)) 

max_idx <- length(rfsrc_pbc_test$time.interest)
for (i in 1:nrow(final_pred) ) {
	idx <- max(which(rfsrc_pbc_test$time.interest <= final_pred[i,'tstop']))
	if (idx == Inf) {
		idx <- max_idx
	}
	final_pred[i,'idx'] <- idx
	final_pred[i,'time_interest'] <- rfsrc_pbc_test$time.interest[idx]
	final_pred[i,'survival'] <- rfsrc_pbc_test$survival[i,idx]
}

global_surv_mean = colMeans(rfsrc_pbc_test$survival)
global_surv_sd = vector("numeric",length=0)
for (i in 1:ncol(rfsrc_pbc_test$survival)) {
	global_surv_sd = append(global_surv_sd,sd(rfsrc_pbc_test$survival[,i])) 
}
global_surv_stats <- data.frame(rfsrc_pbc_test$time.interest)
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

ppp = 1	
cur_var <- output_var$n1
Xmin = 0	
col = c('red','blue','forestgreen')
if (PLOT_PREDICTION) {
	dev.new(xpos=10,ypos=10,width=2*Xsize,height=2.25*Ysize)
	#bins = as.integer(final_pred$outcome)
	bins = final_pred$outcomeINT
	cur_title = sprintf("Case %3s ... Var = %s",ppp,paste(paste(cur_var[0:9],collapse = ", "),paste(cur_var[10:length(cur_var)],collapse = ", "),sep = '\n'))
	plot(final_pred$tstop,final_pred$survival, pch=bins, xlim=c(Xmin,MODEL_TIME_LIMIT*1.01),ylim=c(0.0,1.0),ylab="1 - Closed Won Probability",
					main=cur_title,col=ifelse(final_pred$outcomeINT==2,col[2],ifelse(final_pred$outcomeINT==1,col[1],col[3])),xaxt='n') 
	#lines(basehaz_fit$time,basehaz_fit$baseline_survival,col='black')
	lines(global_surv_stats$time,global_surv_stats$global_mean,col='black')
	lines(global_surv_stats$time,(global_surv_stats$global_mean + global_surv_stats$global_sd*0.5),col='black',lty=2)
	lines(global_surv_stats$time,(global_surv_stats$global_mean - global_surv_stats$global_sd*0.5),col='black',lty=2)
	#points(final_pred$tstop,fitted(poly_fit),col='green')
	#lines(sort(final_pred$time),fitted(poly_fit)[order(final_pred$time)],col='red',type='b')
	axis(1,c(0,182,365,547,730,912,1095,1277),las=1)
	#axis(1,c(0,182,365,547,730,912,1095,1277,1460,1642,1825,2008,2190,2373),las=1)
	legend(x='bottomleft',NULL,pch=sort(unique(bins)),col=col,sort(unique(final_pred$outcome)),bty='o',bg = 'light gray')
}

input_var <- 'won' 
#nochurn_pred <- final_pred[which(final_pred[input_var] == 0),]
#nochurn_tab <- table(factor(nochurn_pred$pred_status),factor(nochurn_pred$gainsight_account_health))	

#final_pred <- final_pred[(final_pred$time_interest > 30),]
pred <- factor(final_pred$pred_status)
actual <- factor(final_pred[,input_var])
xtab <- table(pred,actual)	

print(xtab)

#cox_churn_fit = c() 
#new_row <- churn_stats('won','All-Time','',final_pred,cox_churn_fit)
#year_row1 <- data.frame(churn_stats('won','Yr','1',final_pred,cox_churn_fit),stringsAsFactors=FALSE )
#year_row2 <- data.frame(churn_stats('won','Yr','2',final_pred,cox_churn_fit),stringsAsFactors=FALSE )
#year_row3 <- data.frame(churn_stats('won','Yr','3',final_pred,cox_churn_fit),stringsAsFactors=FALSE )
#year_row4 <- data.frame(churn_stats('won','Yr','4',final_pred,cox_churn_fit),stringsAsFactors=FALSE )
#year_row5 <- data.frame(churn_stats('won','Yr','5',final_pred,cox_churn_fit),stringsAsFactors=FALSE )

# Predict survival for 106 patients not in randomized trial
#data("rfsrc_pbc_test", package="ggRandomForests")
# Print prediction summary  
print(rfsrc_pbc_test)
cat("\n\n")

# The forest summary indicates there are 106 test set 
# observations with 36 deaths and the predicted error rate 
#is $19.1\%$. We plot the predicted survival just as we did the training set estimates.

# RED --> drug group
# BLUE --> placebo group

dev.new(xpos=900,ypos=500,width=2*Xsize,height=2.25*Ysize)
p9 <- plot(gg_rfsrc(rfsrc_pbc_test), alpha=.2) + 
  scale_color_manual(values = strCol) + 
  theme(legend.position = "none") + 
  labs(y = "1 - Closed Won Probability", x = "Days") +
  coord_cartesian(ylim = c(-0.01, 1.01),xlim = c(0, 365) )
print(p9)

dev.new(xpos=1000,ypos=500,width=Xsize,height=2*Ysize)
p10 <- plot(gg_vimp(rfsrc_pbc), lbls = st.labs) + 
  theme(legend.position = c(0.8, 0.2)) + 
  labs(fill = "VIMP > 0")
print(p10)

## calculate for document
#ggda <- gg_vimp(rfsrc_pbc)

###################
# Minimum Depth 
###################

varsel_pbc <- var.select(rfsrc_pbc)
gg_md <- gg_minimal_depth(varsel_pbc, lbls = st.labs)
print(gg_md)

#data("varsel_pbc", package = "ggRandomForests")
#gg_md <- gg_minimal_depth(varsel_pbc)
#print(gg_md)
cat("\n\n")

dev.new(xpos=1100,ypos=500,width=Xsize,height=2*Ysize)
p11 <- plot(gg_md, lbls = st.labs)
print(p11)

###################################
# Compare VIMP vs Minimum Depth
###################################

dev.new(xpos=1200,ypos=500,width=Xsize,height=2*Ysize)
p12 <- plot(gg_minimal_vimp(gg_md), lbls = st.labs) +
  theme(legend.position=c(0.8, 0.2))
print(p12)

