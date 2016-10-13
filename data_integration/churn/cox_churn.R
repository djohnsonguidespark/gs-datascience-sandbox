## Load the survival library

library(survival)
library(car)
library(plyr)
library(zoo)

#############################
## Delete all variables
#############################
rm(list=ls(all=TRUE))
graphics.off()
options(width=220)

create_df <- function (data_in) {

	### Convert cox output list to dataframe 
	data_df <- cbind(data.frame(data_in$time),data.frame(data_in$n.risk))
	data_df <- cbind(data_df,data.frame(data_in$n.event))
	colnames(data_df) <- c('time','n.risk','n.event')

	surv_df <- data.frame(data_in$surv)
	for (i in 1:ncol(surv_df)) {
		names(surv_df)[i] <- gsub("X","surv_",names(surv_df)[i])
	}
	data_df <- cbind(data_df,data.frame(surv_df))
	stderr_df <- data.frame(data_in$std.err)
	for (i in 1:ncol(stderr_df)) {
		names(stderr_df)[i] <- gsub("X","stderr_",names(stderr_df)[i])
	}
	data_df <- cbind(data_df,data.frame(stderr_df))
	lower_df <- data.frame(data_in$lower)
	for (i in 1:ncol(lower_df)) {
		names(lower_df)[i] <- gsub("X","lower_",names(lower_df)[i])
	}
	data_df <- cbind(data_df,data.frame(lower_df))
	upper_df <- data.frame(data_in$upper)
	for (i in 1:ncol(upper_df)) {
		names(upper_df)[i] <- gsub("X","upper_",names(upper_df)[i])
	}
	data_df <- cbind(data_df,data.frame(upper_df))

	return(data_df)

}


source("/home/analytics/analytics_sandbox/Rlibs/common_libs.R")
source("/home/analytics/analytics_sandbox/Rlibs/churn_libs.R")

output_date = gsub("-","",substr(as.character(Sys.time()),1,10)) 

Xsize=4
Ysize=3
completion_delta = .5
NVIEW = 100
KM_UNIQUE_THRESHOLD = 300

#MODEL_TIME_LIMIT = 730 
MODEL_TIME_LIMIT = 5000 
OUTPUT_BINS = 3 

CONSERVE_MEMORY = TRUE
NSFDC_BIN = 100

PLOT_ON = FALSE
PLOT_SURVIVAL_CURVES = FALSE
PLOT_PREDICTION = FALSE

#df=data.frame(y=rnorm(500,0,20),x1=rnorm(500,50,100),x2=rnorm(500,10,40))
#df$x3=df$x1+runif(500,-50,50); df$x4=df$x2+runif(500,-5,5)
## Fit a linear model to the data
#mod=lm(y~.,data=df); plot(mod,which=1:2)

ptm <- proc.time()

year_row_names = c('MODEL_TIME_LIMIT','Ntotal','Nevent','Nremoved','var_independent','Ncoef',
						'bin1_OK','bin1_churn','bin2_OK','bin2_churn','bin3_OK','bin3_churn',   
						'bin1_churn_per','bin2_churn_per','bin3_churn_per',
						'bin1_customer_per','bin2_customer_per','bin3_customer_per',
						'bin1_total_churn','bin2_total_churn','bin3_total_churn', 
						'Likelihood_value','Likelihood_df','Likelihood_p',
						'Wald_value','Wald_df','Wald_p',
						'Logrank_value','Logrank_df','Logrank_p','year')

##################
# Load data
##################
churn_sdata <- read.csv(file="./output/sdata_timeline_df.csv", header=TRUE, sep=",",stringsAsFactors=FALSE)
account_df <- read.csv(file="./output/account_df.csv",header=TRUE,sep=",",stringsAsFactors=FALSE)

cat(sprintf('After Initial Read         ... No of Accounts: %4s\n',length(unique(churn_sdata$sfdc))) )

cancel_lookup <- churn_sdata[which(churn_sdata$Cancellation_Notice_Received != ''),c('sfdc','Total_Lost_ARR','Cancellation_Notice_Received','ChurnDate')]
cancel_lookup <- cancel_lookup[!duplicated(cancel_lookup),]
row.names(cancel_lookup) <- 1:nrow(cancel_lookup)

#churn_sdata <- merge(churn_sdata,account_df[,c('Id','Yearly_Client_ARR__c')],by.x='sfdc',by.y='Id',all.x=TRUE)	

###############################
# Remove meaningless data
###############################
churn_sdata <- churn_sdata[,names(churn_sdata) != 'X']
churn_sdata <- churn_sdata[churn_sdata$tstart >= 0,]  # activity prior to MSA Effective Date
churn_sdata <- churn_sdata[!is.na(churn_sdata$library_size),]  ## activity before initial sale
churn_sdata[is.na(churn_sdata$library_completion_per),'library_completion_per'] <- 0  ## divide by 0 set to 0 
churn_sdata[which(churn_sdata$library_completion_per > 1.0),'library_completion_per'] <- 1  ## >1 set to 1 
#churn_sdata <- churn_sdata[which(abs(churn_sdata$Cambria_algo_delta) <= 2 | churn_sdata$non_evergreen == 1),] ## Remove those where library does not match 

#industry_int <- as.factor(churn_sdata$industry)
#levels(industry_int) <- 1:length(levels(industry_int))
#churn_sdata['industry_int'] <- as.numeric(industry_int) 

#churn_sdata <- churn_sdata[which(abs(churn_sdata$Cambria_algo_delta) <= 2 | churn_sdata$non_evergreen == 1),] ## Remove those where library does not match 

cat(sprintf('After Initial Data Removal ... No of Accounts: %4s\n',length(unique(churn_sdata$sfdc))) )

#########################
# Create combined fields
#########################
churn_sdata['C_NlineItem_Initial_library_completion_per'] <- as.numeric(churn_sdata[,'NlineItem_Initial']) * churn_sdata[,'library_completion_per']
churn_sdata['C_library_size_library_completion_per'] <- as.numeric(churn_sdata[,'library_size']) * churn_sdata[,'library_completion_per']
churn_sdata['C_Nvideo_library_completion_per'] <- as.numeric(churn_sdata[,'Nvideo']) * churn_sdata[,'library_completion_per']
churn_sdata['C_Nactivity_weekly_library_size'] <- as.numeric(churn_sdata[,'Nactivity_weekly']) * churn_sdata[,'library_size']
churn_sdata['C_Nactivity_weekly_Nvideo'] <- as.numeric(churn_sdata[,'Nactivity_weekly']) * churn_sdata[,'Nvideo']

#############################################################################################
# Remove data where churn=1 and library_completion_per = 0
# Why? Idea is that we may have TURNED OFF videos prior to the subscription end
#		Want to make sure we capture the library completion AT THE EXACT TIME of the notice
# UPDATE --- No longer an issue since Cancellation Notice Date is being used? ---
#############################################################################################
#sfdc_0lib = as.vector(churn_sdata$sfdc[which(churn_sdata$churn == 1 & churn_sdata$library_completion_per == 0)])
#if (length(sfdc_0lib) > 0) {
#	for (i in 1:length(sfdc_0lib) ) {
#		churn_sdata = churn_sdata[which(churn_sdata$sfdc != sfdc_0lib[i]),]
#	}
#	rownames(churn_sdata) <- 1:nrow(churn_sdata)
#}

breaks<-append(seq(0,1-completion_delta,completion_delta),100000)
churn_sdata['library_completion_bin'] <- rep(0,nrow(churn_sdata))
bin_num <- rep(1,nrow(churn_sdata))
for (i in 2:length(breaks) ) {
	N <- which(churn_sdata$library_completion_per > breaks[i-1] & churn_sdata$library_completion_per <= breaks[i])
	bin_num[N] <- i-1 
}
library_bin_names <- vector("numeric",length=0)
for (i in 1:(length(breaks)-2)) {
	library_bin_names <- append(library_bin_names,sprintf("%.1f%%-%.1f%%",breaks[i]*100,breaks[i+1]*100))
}
library_bin_names <- append(library_bin_names,sprintf(">%.1f%%",breaks[length(breaks)-1]*100) )

churn_sdata['library_completion_bin'] <- bin_num
churn_sdata['C_NlineItem_Initial_library_completion_bin'] <- as.numeric(churn_sdata[,'NlineItem_Initial']) * churn_sdata[,'library_completion_bin']
churn_sdata['C_library_size_library_completion_bin'] <- as.numeric(churn_sdata[,'library_size']) * churn_sdata[,'library_completion_bin']
churn_sdata['C_Nvideo_library_completion_bin'] <- as.numeric(churn_sdata[,'Nvideo']) * churn_sdata[,'library_completion_bin']


###########################
# 1) Create bins
###########################
churn_sdata['Ndowntick_per'] <- churn_sdata['Ndowntick_count'] / churn_sdata['library_size']
churn_sdata['Nview_per'] <- churn_sdata['Nview_total'] / churn_sdata['Benefits_Eligible_Employees__c']
churn_sdata['Nuser_per'] <- churn_sdata['Nunique_user'] / churn_sdata['Benefits_Eligible_Employees__c']

churn_sdata['MSA_year'] <- as.integer(substring(churn_sdata[,'actual_MSA_Effective_Date'],1,4))
churn_sdata[grep('True',churn_sdata[,'HCR_Only__c']),'HCR_Only__c'] <- 1 
churn_sdata[grep('False',churn_sdata[,'HCR_Only__c']),'HCR_Only__c'] <- 0 

churn_sdata['Branding_bin'] <- rep(0,nrow(churn_sdata)) 
churn_sdata[grep('Lite',churn_sdata[,'Branding2__c']),'Branding_bin'] <- 1 
churn_sdata[grep('Premium',churn_sdata[,'Branding2__c']),'Branding_bin'] <- 2 
churn_sdata[grep('PremiumPlus',churn_sdata[,'Branding2__c']),'Branding_bin'] <- 3 

churn_sdata['Lite'] <- rep(0,nrow(churn_sdata)) 
churn_sdata['Premium'] <- rep(0,nrow(churn_sdata)) 
churn_sdata['PremiumPlus'] <- rep(0,nrow(churn_sdata)) 
churn_sdata[grep('Lite',churn_sdata[,'Branding2__c']),'Lite'] <- 1 
churn_sdata[grep('Premium',churn_sdata[,'Branding2__c']),'Premium'] <- 2 
churn_sdata[grep('PremiumPlus',churn_sdata[,'Branding2__c']),'PremiumPlus'] <- 3 

if("Nview_total" %in% colnames(churn_sdata))
{
	churn_sdata['Nview_total'] <- churn_sdata[,'Nview_total'] / NVIEW
	churn_sdata['Nview_total_bin'] <- churn_sdata[,'Nview_total'] / NVIEW

	churn_sdata['Nview_total_bin'] <- rep(0,nrow(churn_sdata)) 
	churn_sdata[which(churn_sdata['Nview_total'] > 0 & churn_sdata['Nview_total'] < (1000/NVIEW)),'Nview_total_bin'] <- 1 
	churn_sdata[which(churn_sdata['Nview_total'] >= (1000/NVIEW) & churn_sdata['Nview_total'] < (2500/NVIEW) ),'Nview_total_bin'] <- 2 
	churn_sdata[which(churn_sdata['Nview_total'] >= (2500/NVIEW) & churn_sdata['Nview_total'] < (10000/NVIEW) ),'Nview_total_bin'] <- 3 
	churn_sdata[which(churn_sdata['Nview_total'] >= (10000/NVIEW) ),'Nview_total_bin'] <- 4

	churn_sdata['Nview_total_G'] <- rep(1,nrow(churn_sdata))
	churn_sdata[which(churn_sdata['tstart'] > 730),'Nview_total_G'] <- 2

}

if("view_qtr" %in% colnames(churn_sdata))
{
	churn_sdata['view_qtr'] <- rep(0,nrow(churn_sdata)) 
	churn_sdata[which(churn_sdata['first_view_qtr'] == 1),'view_qtr'] <- 1 
	churn_sdata[which(churn_sdata['first_view_qtr'] > 1),'view_qtr'] <- 2 
}

if("Ncsm_change" %in% colnames(churn_sdata))
{
	churn_sdata['Ncsm_change_bin'] <- rep(0,nrow(churn_sdata)) 
	churn_sdata[which(churn_sdata['Ncsm_change'] == 1),'Ncsm_change_bin'] <- 1 
	churn_sdata[which(churn_sdata['Ncsm_change'] > 1),'Ncsm_change_bin'] <- 2 
}

if("NPS" %in% colnames(churn_sdata))
{
	churn_sdata['NPS_bin'] <- rep(0,nrow(churn_sdata)) 
	churn_sdata[which(churn_sdata['NPS'] > 0 & churn_sdata['NPS'] < 3.33333),'NPS_bin'] <- 1 
	churn_sdata[which(churn_sdata['NPS'] >= 3.33333 & churn_sdata['NPS'] < 6.66666),'NPS_bin'] <- 2 
	churn_sdata[which(churn_sdata['NPS'] >= 6.66666),'NPS_bin'] <- 3 
}

if("CSAT" %in% colnames(churn_sdata))
{
	churn_sdata['CSAT_bin'] <- rep(0,nrow(churn_sdata)) 
	churn_sdata[which(churn_sdata['CSAT'] > 0 & churn_sdata['CSAT'] < 3),'CSAT_bin'] <- 1 
	churn_sdata[which(churn_sdata['CSAT'] == 3),'CSAT_bin'] <- 2 
	churn_sdata[which(churn_sdata['CSAT'] > 3),'CSAT_bin'] <- 3 
}

if("Nadmin_usage_day" %in% colnames(churn_sdata))
{
	churn_sdata['Nadmin_usage_day_bin'] <- rep(0,nrow(churn_sdata)) 
	churn_sdata[which(churn_sdata['Nadmin_usage_day'] > 0 & churn_sdata['Nadmin_usage_day'] < 10),'Nadmin_usage_day_bin'] <- 1 
	churn_sdata[which(churn_sdata['Nadmin_usage_day'] >= 5 & churn_sdata['Nadmin_usage_day'] < 10),'Nadmin_usage_day_bin'] <- 2 
	churn_sdata[which(churn_sdata['Nadmin_usage_day'] >= 10 & churn_sdata['Nadmin_usage_day'] < 25),'Nadmin_usage_day_bin'] <- 3 
	churn_sdata[which(churn_sdata['Nadmin_usage_day'] >= 25),'Nadmin_usage_day_bin'] <- 4 
	#churn_sdata[which(churn_sdata['Nadmin_usage'] >= 25 & churn_sdata['Nadmin_usage'] < 50),'admin_bin'] <- 4 
	#churn_sdata[which(churn_sdata['Nadmin_usage'] >= 50 & churn_sdata['Nadmin_usage'] < 75),'admin_bin'] <- 5 
	#churn_sdata[which(churn_sdata['Nadmin_usage'] >= 75 & churn_sdata['Nadmin_usage'] < 100),'admin_bin'] <- 6 
	#churn_sdata[which(churn_sdata['Nadmin_usage'] >= 100),'admin_bin'] <- 7 
}

if("Nvideo_delivery_time" %in% colnames(churn_sdata))
{
	churn_sdata['Nvideo_delivery_time_bin'] <- rep(0,nrow(churn_sdata)) 
	churn_sdata[which(churn_sdata['Nvideo_delivery_time'] > 0 & churn_sdata['Nvideo_delivery_time'] < 30),'Nvideo_delivery_time_bin'] <- 1 
	churn_sdata[which(churn_sdata['Nvideo_delivery_time'] >= 30 & churn_sdata['Nvideo_delivery_time'] < 90),'Nvideo_delivery_time_bin'] <- 2 
	churn_sdata[which(churn_sdata['Nvideo_delivery_time'] >= 90 & churn_sdata['Nvideo_delivery_time'] < 182),'Nvideo_delivery_time_bin'] <- 3 
	churn_sdata[which(churn_sdata['Nvideo_delivery_time'] >= 182),'Nvideo_delivery_time_bin'] <- 4 

	churn_sdata['Nvideo_delivered_WF_G'] <- rep(1,nrow(churn_sdata))
	churn_sdata[which(churn_sdata['Nvideo_delivered_WF'] > 0),'Nvideo_delivered_WF_G'] <- 2

	churn_sdata['Nvideo_delivery_time_G'] <- rep(1,nrow(churn_sdata))
	churn_sdata[which(churn_sdata['Nvideo_delivered_WF'] > 0),'Nvideo_delivery_time_G'] <- 2

}

####################################################
# 2) Create timeframe strata for variables 
#	In case variable is NOT a proportional hazard
####################################################
churn_sdata['Nupsell_G'] <- rep(1,nrow(churn_sdata))
churn_sdata[which(churn_sdata['tstart'] > 730),'Nupsell_G'] <- 2

churn_sdata['Nvideo_G'] <- rep(1,nrow(churn_sdata))
churn_sdata[which(churn_sdata['tstart'] > 730),'Nvideo_G'] <- 2

churn_sdata['Nnps_survey_G'] <- rep(1,nrow(churn_sdata))
churn_sdata[which(churn_sdata['tstart'] > 730),'Nnps_survey_G'] <- 2

churn_sdata['Ncsm_change_G'] <- rep(1,nrow(churn_sdata))
churn_sdata[which(churn_sdata['tstart'] > 730),'Ncsm_change_G'] <- 2

churn_sdata['NlineItem_Initial_G'] <- rep(1,nrow(churn_sdata))
churn_sdata[which(churn_sdata['tstart'] > 730),'NlineItem_Initial_G'] <- 2

churn_sdata['Nbenefits_G'] <- rep(1,nrow(churn_sdata))
churn_sdata[which(churn_sdata['tstart'] > 730),'Nbenefits_G'] <- 2

churn_sdata['Nnps_G'] <- rep(1,nrow(churn_sdata))
churn_sdata[which(churn_sdata['tstart'] > 600),'Nnps_G'] <- 2

churn_sdata['Nview_weekly_G'] <- rep(1,nrow(churn_sdata))
churn_sdata[which(churn_sdata['tstart'] > 730),'Nview_weekly_G'] <- 2

if("Nedits_vd1" %in% colnames(churn_sdata))
{
	churn_sdata['Nedits_vd1_G'] <- rep(1,nrow(churn_sdata))
	churn_sdata[which(churn_sdata['Nvideo_delivered_WF'] > 0),'Nedits_vd1_G'] <- 2
}

if("Nedits_all" %in% colnames(churn_sdata))
{
	churn_sdata['Nedits_all_G'] <- rep(1,nrow(churn_sdata))
	churn_sdata[which(churn_sdata['Nvideo_delivered_WF'] > 0),'Nedits_all_G'] <- 2
}

##################################
# 3) Data Manipulation (Filtering)
##################################

churn_sdata <- churn_sdata[which(churn_sdata['tstart'] < MODEL_TIME_LIMIT),]
#churn_sdata <- churn_sdata[churn_sdata$MSA_year >= 2013,]
#churn_sdata <- churn_sdata[churn_sdata$first_view_month >= 0,]
churn_sdata <- churn_sdata[churn_sdata$HCR_Only__c == 0,]
#churn_sdata <- churn_sdata[churn_sdata$library_completion_per >= 0,]
#churn_sdata <- churn_sdata[churn_sdata$view_qtr != 0,]
#churn_sdata <- churn_sdata[churn_sdata$Nvideo != 0,]
#churn_sdata <- churn_sdata[churn_sdata$Branding2__c == 'Lite',]
#churn_sdata <- churn_sdata[churn_sdata$Initial_Term_Length__c == 12,]
#churn_sdata <- churn_sdata[churn_sdata$NlineItem_Initial <= 5,]

cat(sprintf('Post Filter                ... No of Accounts: %4s\n',length(unique(churn_sdata$sfdc))) )

################################
# Choose covariates of interest 
################################
all_var <- list(names(churn_sdata))

#cur_var = c('Nnps_survey','Nupsell','Nview_total','C_NlineItem_Initial_library_completion_bin','Ncsm_change') ## All significant ... results aren't as good
cur_var = c('Nnps_survey','Nupsell','Nview_total','NlineItem_Initial','Ncsm_change','Nbenefits','Ncompensation') ## All significant ... results aren't as good
cur_var = c('Nnps_survey','Nupsell','Nview_total','Ncsm_change','Nsolution','Nbenefits') ## All significant ... results aren't as good
cur_var = c('Nnps_survey','Nupsell','Nview_total','NlineItem_Initial','Ncsm_change') ## All significant ... results aren't as good
cur_var = c('Nnps_survey','Nupsell','Nview_total','C_NlineItem_Initial_library_completion_bin','Ncsm_change') ## All significant ... results aren't as good
#cur_var = c('Nnps_survey:strata(Nnps_survey_G)','Nupsell:strata(Nupsell_G)','Nview_total:strata(Nview_total_G)','NlineItem_Initial','Ncsm_change:strata(Ncsm_change_G)') ## All significant ... results aren't as good
#cur_var = c('Nnps_survey:strata(Nnps_survey_G)') ## All significant ... results aren't as good

########################################################################
# Cross-Validation works here ... Ideal for 0-730 days
cur_var = c('Nnps_survey','Nupsell','Nview_total','NlineItem_Initial','Ncsm_change') ## All significant ... BEST RESULTS
########################################################################

##### NOTE: This is identical to non-strata with MODEL_TIME_LIMIT = 730
#cur_var = c('Nnps_survey:strata(Nnps_survey_G)','Nupsell:strata(Nupsell_G)','Nview_total:strata(Nview_total_G)','NlineItem_Initial:strata(NlineItem_Initial_G)','Ncsm_change:strata(Ncsm_change_G)') ## All significant ... results aren't as good

#cur_var = c('C_Nvideo_library_completion_bin') ## churn spike at 600 
#cur_var = c('Nvideo','library_completion_bin','Ncsm_change') ## churn spike at 600 
#cur_var = c('C_Nactivity_weekly_library_size')
#cur_var = c('C_Nactivity_weekly_Nvideo')
#cur_var = c('C_library_size_library_completion_bin')
#cur_var = c('Nvideo')  ## significant
#cur_var = c('library_size')  ## significant
#cur_var = c('Ncsm_change') ## significant ... higher is bad
#cur_var = c('NlineItem_Initial') ## significant ... higher is better
#cur_var = c('C_NlineItem_Initial_library_completion_per') ## significant ... higher is better (<NlineItem_Initial by itself)
#cur_var = c('NPS') ## All significant ... NON-Proportional hazard 
#cur_var = c('Nsolution') ## All significant ...  
#cur_var = c('Nbenefits:strata(Nbenefits_G)') ## All significant ... NON-Proportional hazard ... both significant 
#cur_var = c('Nbenefits','Ncompensation') ## All significant ... NON-Proportional hazard ... both significant 
#cur_var = c('Nsolution','Nbenefits','Ncompensation') ## Benefits still good ... Nsolution/Ncompensation go away 
#cur_var = c('Nbenefits') ## All significant ... Churn goes DOWN with larger library (<1)
#cur_var = c('Ncompensation') ## All significant ... Churn goes DOWN with larger library (<1)
#cur_var = c('Ncompliance') ## Not significant 
#cur_var = c('Nperf_mgmt') ## Not significant 
#cur_var = c('Ncsm_change') ## Not significant 
#cur_var = c('Nupsell') ## Significant ... NON-Proportional Hazard
#cur_var = c('Nview_total') ## Not significant 
#cur_var = c('Nview_weekly:strata(Nview_weekly_G)') ## Not significant 

output_var = vector(mode="list")
output_var$n1  = c('Nnps_survey','Nupsell','Nview_total','NlineItem_Initial','Ncsm_change') 
#output_var$n2  = c('Nnps_survey')
#output_var$n3  = c('Nupsell')
#output_var$n4  = c('Nview_total')
#output_var$n5  = c('Ncsm_change')
#output_var$n6  = c('Nadmin_usage_day')
#output_var$n7  = c('Nreport_total')
#output_var$n8  = c('Nview_weekly')
#output_var$n9  = c('Nactivity_total')
#output_var$n10 = c('Nactivity_weekly')
#output_var$n11 = c('Nsolution')
#output_var$n12 = c('Nbenefits')
#output_var$n13 = c('Ncompensation')
#output_var$n14 = c('Ncompliance')
#output_var$n15 = c('Nperf_mgmt')
#output_var$n16 = c('Nvideo')
#output_var$n17 = c('Nvideo_delivered_WF')
#output_var$n18 = c('library_size')
#output_var$n19 = c('Nvideo_delivery_time')
#output_var$n20 = c('library_completion_per')
#output_var$n21 = c('library_completion_bin')
#output_var$n22 = c('C_Nvideo_library_completion_bin')
#output_var$n23 = c('C_library_size_library_completion_bin')
#output_var$n24 = c('C_NlineItem_Initial_library_completion_bin')
#output_var$n25 = c('first_video_day')
#output_var$n26 = c('Nedits_vd1')
#output_var$n27 = c('Nedits_all')
#output_var$n28 = c('NlineItem_Initial')
#output_var$n29 = c('Ndowntick')
#output_var$n30 = c('Ndowntick_per')
#output_var$n31 = c('Nswap')
#output_var$n32 = c('Nnps_survey','Nupsell','Nview_total','C_NlineItem_Initial_library_completion_bin','Ncsm_change') 
#output_var$n33 = c('Nnps_survey','Nupsell','Nview_total','C_library_size_library_completion_bin','Ncsm_change') 
#output_var$n34 = c('Nnps_survey','Nupsell','Nview_total','C_Nvideo_library_completion_bin','Ncsm_change') 
#output_var$n35 = c('Nnps_survey','Nupsell','Nview_total','C_NlineItem_Initial_library_completion_per','Ncsm_change') 
#output_var$n37 = c('Nnps_survey','Nupsell','Nview_total','C_Nvideo_library_completion_per','Ncsm_change') 
#output_var$n38 = c('Nnps_survey','Nupsell','Nview_total','C_NlineItem_Initial_library_completion_bin') 
#output_var$n39 = c('Nnps_survey','Nupsell','Nview_total','C_library_size_library_completion_bin') 
#output_var$n40 = c('Nnps_survey','Nupsell','Nview_total','C_Nvideo_library_completion_bin') 
#output_var$n41 = c('Nnps_survey','Nupsell','Nview_total','C_NlineItem_Initial_library_completion_per') 
#output_var$n42 = c('Nnps_survey','Nupsell','Nview_total','C_library_size_library_completion_per') 
#output_var$n43 = c('Nnps_survey','Nupsell','Nview_total','C_Nvideo_library_completion_per') 
#output_var$n44 = c('Nnps_survey','Nupsell','Nview_total') 
#output_var$n45 = c('Nvideo','library_size') 
#output_var$n46 = c('Nvideo_delivered_WF','library_size') 
#output_var$n47 = c('Nvideo_delivered_WF','library_size','Nvideo_delivery_time') 
#output_var$n48 = c('Nvideo_delivered_WF','Nactivity_weekly','Nvideo_delivery_time') 
#
### Gives best results, but likely over-fitting
#output_var$n1  = c('Nupsell','library_size','C_NlineItem_Initial_library_completion_bin','Nvideo','Nview_total','NlineItem_Initial','Nvideo_delivered_WF','Nsolution','Nnps_survey','Ncsm_change') 
#output_var$n2  = c('Nupsell','C_NlineItem_Initial_library_completion_bin','Nview_total','NlineItem_Initial','Nvideo_delivered_WF','Nsolution','Nnps_survey','Ncsm_change','first_video_day','Nvideo_delivery_time') 
#output_var$n3  = c('Nupsell','C_NlineItem_Initial_library_completion_bin','Nview_total','NlineItem_Initial','Nsolution','Nnps_survey','Ncsm_change','first_video_day','Nvideo_delivery_time') 
#output_var$n4  = c('Nupsell','C_NlineItem_Initial_library_completion_per','Nview_total','NlineItem_Initial','Nsolution','Nnps_survey','Ncsm_change','first_video_day','Nvideo_delivery_time') 
#output_var$n5  = c('Nupsell','C_NlineItem_Initial_library_completion_per','Nview_total','NlineItem_Initial','Nsolution','Nnps_survey','Ncsm_change','first_video_day') 
#output_var$n6  = c('Nupsell','C_NlineItem_Initial_library_completion_per','Nview_total','NlineItem_Initial','Nsolution','Nnps_survey','Ncsm_change','first_video_day','Nvideo_delivery_time','Nactivity_weekly') 
#output_var$n7  = c('Nupsell','C_NlineItem_Initial_library_completion_per','Nview_total','NlineItem_Initial','Nsolution','Nnps_survey','Ncsm_change','first_video_day','Nactivity_weekly','Nactivity_total') 
#output_var$n8  = c('Nupsell','C_NlineItem_Initial_library_completion_per','Nview_total','NlineItem_Initial','Nsolution','Nnps_survey','Ncsm_change','first_video_day','Ndowntick','Nswap') 
#output_var$n9  = c('Nupsell','C_NlineItem_Initial_library_completion_per','Nview_total','NlineItem_Initial','Nsolution','Nnps_survey','Ncsm_change','first_video_day','Nadmin_usage_day') 
#output_var$n10  = c('Nupsell','C_NlineItem_Initial_library_completion_per','Nview_total','Nsolution','Nnps_survey','Ncsm_change','first_video_day','Ndowntick') 
#output_var$n11  = c('Nupsell','C_NlineItem_Initial_library_completion_bin','Nview_total','Nsolution','Nnps_survey','Ncsm_change','first_video_day','Ndowntick') 
#output_var$n12  = c('Nupsell','C_NlineItem_Initial_library_completion_bin','Nview_total','Nsolution','Nnps_survey','Ncsm_change','Ndowntick') 
#output_var$n13  = c('Nupsell','C_NlineItem_Initial_library_completion_bin','Nview_total','Nnps_survey','Ncsm_change','Ndowntick') 
#output_var$n14  = c('Nupsell','NlineItem_Initial','Nview_total','Nsolution','Nnps_survey','Ncsm_change','Ndowntick') 
#output_var$n15  = c('Nupsell','NlineItem_Initial','Nview_total','Nnps_survey','Ncsm_change','Ndowntick') 
#output_var$n16  = c('Nupsell','C_NlineItem_Initial_library_completion_bin','Nsolution','Nview_total','Nnps_survey','Ncsm_change') 
#output_var$n17  = c('Nupsell','C_NlineItem_Initial_library_completion_bin','Nview_total','Nnps_survey','Ncsm_change') 
#output_var$n18  = c('Nupsell','NlineItem_Initial','Nview_total','Nsolution','Nnps_survey','Ncsm_change') 
#output_var$n19  = c('Nupsell','NlineItem_Initial','Nview_total','Nnps_survey','Ncsm_change') 
#output_var$n20  = c('Nnps_survey','Nupsell:strata(Nupsell_G)','Nview_total:strata(Nview_total_G)','C_library_size_library_completion_per','Ncsm_change') 
#output_var$n21  = c('Nnps_survey','Nupsell:strata(Nupsell_G)','Nview_total:strata(Nview_total_G)','C_library_size_library_completion_per','Ncsm_change:strata(Ncsm_change_G)') 

#################################
# Plot Churn time histogram
#################################
if (PLOT_ON) {

	sfdc_max_tstop <- ddply(churn_sdata[c('sfdc','tstop')],~sfdc,summarize,max_tstop=max(tstop))
	sfdc_max_tstop <- merge(sfdc_max_tstop,churn_sdata[,c('sfdc','tstop','churn')],by.x=c('sfdc','max_tstop'),by.y=c('sfdc','tstop')) 

	#breaks = c(0,182,365,547,730,912,1095,1277,1460,1642,1825)
	breaks = c(0,365,730,1095,1460,1825,5000)
	Nbin = rep(0,length(breaks)-1)
	break_location <- breaks[1:(length(breaks)-1)] + (breaks[2:length(breaks)] - breaks[1:length(breaks)-1])/2
	for (i in 1:nrow(sfdc_max_tstop)) {
		for (j in 2:length(breaks)) {
			if (sfdc_max_tstop$max_tstop[i] > breaks[j-1]) {
				Nbin[j-1] = Nbin[j-1] + 1
			}
		}
	}

	dev.new(xpos=0,ypos=0,width=2*Xsize,height=2*Ysize)
	hist(sfdc_max_tstop[which(sfdc_max_tstop$churn==1),]$max_tstop, breaks=breaks,xlab = 'Days since Subscribing',col='blue',xaxt='n',freq=TRUE,labels=TRUE,main='Churn Date Histogram')
	axis(1,breaks,las=1)
	text(break_location,30,Nbin)
}

################################
# Look at variable correlation
################################

all_correlation_var = c("library_size","Nvideo",
"Nupsell","NlineItem_Initial","Ndowntick","Ndowntick_count","Ndowntick_per",
"Nswap","Ncsm_change","Nnps_survey",
"Nadmin_usage_day","Nunique_user","Nview_total","Nview_weekly","Nedits_vd1",
"Nedits_all","Nvideo_delivery_time","library_completion_per",
"first_view_month",'Nactivity_weekly','Nactivity_total','C_Nvideo_library_completion_per',
'Nsolution','Nbenefits','Ncompensation','Nfwa','Nperf_mgmt','Ncompliance')

plot_sdata <- churn_sdata[,which(names(churn_sdata) %in% all_correlation_var)]

for (i in 1:length(names(plot_sdata))) {
	for (j in i:length(names(plot_sdata))) {
		if (i != j) {
			pearson_cor <- cor(plot_sdata[,i],plot_sdata[,j],method='pearson')
			pearson_ptest <- cor.test(plot_sdata[,i],plot_sdata[,j],method='pearson')$p.value
			spearman_cor <- cor(plot_sdata[,i],plot_sdata[,j],method='spearman') 
			#spearman_ptest <- cor.test(plot_sdata[,i],plot_sdata[,j],method='spearman')$p.value 
			if (is.na(pearson_cor) == FALSE) {
				if (abs(pearson_cor) > 0.3) {
					cor_output <- sprintf("(%2d,%2d) ... %35s vs %35s ... (Pearson,Spearman) = (%.3f,%.3f) ... (Pearson Sig, Spearman Sig) = (%.4e,)\n",
								i,j,names(plot_sdata)[i],names(plot_sdata)[j],pearson_cor,spearman_cor,pearson_ptest )  
					cat(cor_output)
				}
			}
		}
	}
}

############################
# Add prediction
############################

##########################################################################
# After Cox proportional hazards model fit 
# Plot the predicted survival for each input (based on variable values)
#
# Can use to set up cross-validation ... split in 2 ways
# 1) Random (time is irrelevant)
# 2) Time-based (based on dates)

#set.seed(1000)
churn_sfdc <- sort(unique(churn_sdata[which(churn_sdata$churn==1),]$sfdc)) 
unique_sfdc <- sort(unique(churn_sdata$sfdc)) 
## 1) Random (time irrelevant)
train_sfdc <- sample(unique_sfdc,0.85*length(unique_sfdc)) 
test_sfdc  <- unique_sfdc[which(!(unique_sfdc %in% train_sfdc) == TRUE)] 

## 2) Time-based (based on dates) 
#train_sfdc <- unique(churn_sdata[which(churn_sdata['actual_MSA_Effective_Date'] < '2014-07-01 00:00:00'),]$sfdc)
#test_sfdc  <- unique_sfdc[which((unique_sfdc %in% train_sfdc) == FALSE)] 


train_sdata <- churn_sdata[churn_sdata$sfdc %in% train_sfdc,]
test_sdata <- churn_sdata[churn_sdata$sfdc %in% test_sfdc,]

## 3) Use all sfdc ... no cross-validation
train_sdata <- churn_sdata
test_sdata <- churn_sdata

### Compute a few things prior to the loop
sfdc_max_tstop <- ddply(test_sdata[c('sfdc','tstop')],~sfdc,summarize,max_tstop=max(tstop))
#sfdc_max_tstop <- ddply(test_sdata[c('sfdc','tstop')],.(sfdc),function(x) x[which.max(x$tstop),])  

index = vector('numeric',length=0)
for (i in 1:nrow(sfdc_max_tstop)) {
	index <- append(index,which(test_sdata$sfdc == sfdc_max_tstop$sfdc[i] & test_sdata$tstop == sfdc_max_tstop$max_tstop[i]))
}
final_record_test_sdata <- test_sdata[index,]  
	
sfdc_id = data.frame(sort(unique(final_record_test_sdata$sfdc)),1:length(sort(unique(final_record_test_sdata$sfdc)))) 
names(sfdc_id) <- c('sfdc','id')

### Start multi-variable loop
output_df = data.frame()
for (ppp in 1:length(output_var)) {

	cat('\n\n*****************************************************\n')
	cat(sprintf('********************** Case %3s *********************\n',ppp))
	cat('*****************************************************\n')
	
	cur_var = output_var[[ppp]]
	cat(sprintf('\n\n**** Current Variable = %s\n',paste(cur_var,collapse=", ")) )
	
	####################################################
	# Fit survival curves based on binary variables
	####################################################
	
#	cat('\n*****************************************************\n')
#	cat('******* Kaplan-Meyer ... Plot Survival Curves *******\n')
#	cat('*****************************************************\n')
#	
#	### Only run if less than 300 unique values
#	train_sdata$survival <- Surv(train_sdata$tstop,train_sdata$churn==1)
#	col = c('red','blue','green','brown','black')
#	for (i in 1:length(cur_var) ) {
#	
#		input_var = unlist(strsplit(cur_var[i],":")) 
#		plot_var = input_var[1]
#		strata_var_value = -1
#		if (length(input_var) > 1) {
#			strata_var = gsub("\\)","",gsub("strata\\(","",input_var[2]))  
#			strata_var_value = sort(unique(train_sdata[[strata_var]]))
#		}
#		if (nrow(unique(train_sdata[plot_var])) < KM_UNIQUE_THRESHOLD) { 
#			#for (j in 1:length(strata_var_value) ) {
#			cat(sprintf("\n%s\n",plot_var)) 
#	
#			#if (strata_var_value != -1) {
#			#	km_sdata <- train_sdata[which(train_sdata[plot_var] == strata_var_value[j]),]
#			#} else {
#			#	km_sdata <- train_sdata
#			#}
#			km_sdata = train_sdata
#			km_formula <- as.formula(paste("survival ~ ",plot_var,sep ="")) 
#	
#			fit_lib <- survfit(km_formula, data = km_sdata)
#	
#			bins = sort(unique(km_sdata[,plot_var]))
#			#bins[length(bins)] = plot_var 
#			survival_title = sprintf("Churn Survival Curves by %s",plot_var)
#	
#			if (PLOT_ON) {
#				dev.new(xpos=800,ypos=0,width=2*Xsize,height=2*Ysize)
#				plot(fit_lib, lty = 1:length(bins), mark.time = FALSE, ylim=c(.6,1), xlab = 'Days since Subscribing', ylab = 'Percent Surviving', col = col)
#				legend(20, .9, bins, lty=1:length(bins), bty = 'n', ncol = 1, col = col)
#				title(main = survival_title)
#			}
#	
#			# And run a log-rank test
#			print(survdiff(km_formula, data = km_sdata))
#			#}
#		} else {
#			cat(sprintf("Skipping KM ... %5s Unique Curves  ... Too many unique values\n",nrow(unique(train_sdata[plot_var])) ) ) 
#		}
#	}
#	
	
	###################
	# Cox Regression
	###################
	cox_formula <- as.formula(paste("Surv(tstart, tstop, churn) ~ ",paste(cur_var,collapse=" + "),sep ="")) 
	cox_churn_fit <- coxph(cox_formula , train_sdata)
	
	cat('\n**************************************************\n')
	cat('********** COX Regression: Result Churn **********\n')
	cat('**************************************************\n')
	print(summary(cox_churn_fit))
	
	cat('\n*********************************************\n')
	cat('** Test: Proportional Hazards (p > .05 OK) **\n')
	cat('*********************************************\n')
	zp <- cox.zph(cox_churn_fit)
	print(zp)
	for (i in 1:length(cur_var) ) {
		if (PLOT_ON) {
			dev.new(xpos=0,ypos=0,width=2*Xsize,height=2*Ysize)
			beta_title = sprintf("Beta Value for %s",cur_var[i])
			plot(zp[i]) # Choose degrees of freedom 2 to see the plot (default will show crazy splines)
			title(main=beta_title)
		}
	}
	
	#cat('\n*********************************************\n')
	#cat('** Test: Variance Inflation Factor **\n')
	#cat('*********************************************\n')
	#zp <- vcov(cox_churn_fit)
	#print(zp)
	#for (i in 1:length(cur_var) ) {
	#	dev.new(xpos=0,ypos=0,width=2*Xsize,height=2*Ysize)
	#	beta_title = sprintf("Beta Value for %s",cur_var[i])
	#	plot(zp[i]) # Choose degrees of freedom 2 to see the plot (default will show crazy splines)
	#	title(main=beta_title)
	#}
	
	#################################################################################
	# Use SURVFIT.COXPH to get the predicted survival function
	# https://stat.ethz.ch/R-manual/R-devel/library/survival/html/survfit.coxph.html 
	#################################################################################
	
	cat('\n*********************************************\n')
	cat('************** Cross Validation *************\n')
	cat('*********************************************\n')
	
	##########################################################################
	# After Cox proportional hazards model fit 
	# Plot the predicted survival for each input (based on variable values)
	#
	# Can use to set up cross-validation ... split in 2 ways
	# 1) Random (time is irrelevant)
	# 2) Time-based (based on dates)
	
	###################################################
	# 1) Find final record for each sfdc in test_sdata
	# 2) Grab all variable values from each sfdc
	# 3) Input into survfit with newdata
	# 4) Check results
	###################################################
	
	## 1) 
	out_sfdc <- data.frame()
	for (i in 1:nrow(final_record_test_sdata)) {
	
		#if ((i %% 50) == 0) {
		#	cat(sprintf("Test Account ... %4s of %4s ... %.3f sec\n",i,nrow(final_record_test_sdata),as.numeric(proc.time()['elapsed']-ptm['elapsed']) ) )
		#}
	
		#test_sfdc <- test_sdata[which(test_sdata$sfdc == final_record_test_sdata[i,'sfdc']),][c('tstart','tstop',cur_var)]
		if (nrow(out_sfdc) == 0) {
			out_sfdc <- test_sdata[which(test_sdata$sfdc == final_record_test_sdata[i,'sfdc']),][c('sfdc','tstart','tstop',cur_var)]
		} else {
			out_sfdc <- rbind(out_sfdc,test_sdata[which(test_sdata$sfdc == final_record_test_sdata[i,'sfdc']),][c('sfdc','tstart','tstop',cur_var)])
		}
	
		#test_sfdc['id'] <- rep(1,nrow(test_sfdc))
	
	}
	cat(sprintf("Accounts Updated ... %4s of %4s ... %.3f sec\n",i,nrow(final_record_test_sdata),as.numeric(proc.time()['elapsed']-ptm['elapsed']) ) )
	out_sfdc <- merge(out_sfdc,sfdc_id,by.x='sfdc',by.y='sfdc')
	out_sfdc <- out_sfdc[,!(names(out_sfdc) %in% 'sfdc')]

	if (CONSERVE_MEMORY) {
		for (i in 1:ceiling(nrow(final_record_test_sdata)/NSFDC_BIN)) {
			cur_data = out_sfdc[which(out_sfdc$id > (i-1)*NSFDC_BIN & out_sfdc$id <= i*NSFDC_BIN),]
			if (i == 1) {
				cox_pred_curve <- summary(survfit(cox_churn_fit, newdata=cur_data))  ## TAKING ALL THE TIME
			} else if (i >= 1) {
				cox_pred_curve_NEW <- summary(survfit(cox_churn_fit, newdata=cur_data))  ## TAKING ALL THE TIME
				cox_pred_curve$surv = cbind(cox_pred_curve$surv,cox_pred_curve_NEW$surv)			
				cox_pred_curve$lower = cbind(cox_pred_curve$lower,cox_pred_curve_NEW$lower)			
				cox_pred_curve$upper = cbind(cox_pred_curve$upper,cox_pred_curve_NEW$upper)			
				cox_pred_curve$std.err = cbind(cox_pred_curve$std.err,cox_pred_curve_NEW$std.err)			
			}

		cat(sprintf("Survfit Account ... %4s of %4s ... %.3f sec\n",i*NSFDC_BIN,nrow(final_record_test_sdata),as.numeric(proc.time()['elapsed']-ptm['elapsed']) ) )

		}
	} else {
		cox_pred_curve <- summary(survfit(cox_churn_fit, newdata=out_sfdc))  ## TAKING ALL THE TIME
	}

	cat(sprintf("Survfit complete ... %.3f sec\n",as.numeric(proc.time()['elapsed']-ptm['elapsed']) ) )
	#cox_pred_curve_df <- create_df(cox_pred_curve
	#cat(sprintf("Survfit DF complete ... %.3f sec\n",as.numeric(proc.time()['elapsed']-ptm['elapsed']) ) )
	
	final_pred = data.frame()
	global_surv = data.frame(cox_pred_curve$time)
	colnames(global_surv) <- c('time')
	for (i in 1:nrow(final_record_test_sdata)) {

		final_sfdc <- final_record_test_sdata[i,c('sfdc','churn','tstop')]
		#max_col <- c('time','n.risk','n.event',paste('surv_',max_row,sep=""),paste('stderr_',max_row,sep=""),paste('lower_',max_row,sep=""),paste('upper_',max_row,sep=""))
		test_sfdc <- out_sfdc[which(out_sfdc['id']==i),] 
		cur_rows <- as.numeric(row.names(test_sfdc))
		max_col <- max(cur_rows) 
		cox_idx <- which(cox_pred_curve$time <= final_sfdc$tstop)   
		if (length(cox_idx) > 0) {
			max_row = max(cox_idx)
		} else {
			max_row = 0
		}
	
		surv_curve = vector("numeric",length=0)
		if (max_row > 0) {
			max_cox_time <- cox_pred_curve$time[max_row]
			max_cox_risk <- cox_pred_curve$n.risk[max_row]
			max_cox_event <- cox_pred_curve$n.event[max_row]
			max_cox_surv <- as.numeric(cox_pred_curve$surv[max_row,max_col])
			max_cox_stderr <- as.numeric(cox_pred_curve$std.err[max_row,max_col])
			max_cox_lower <- as.numeric(cox_pred_curve$lower[max_row,max_col])
			max_cox_upper <- as.numeric(cox_pred_curve$upper[max_row,max_col])
			max_cox_pred_curve_df <- data.frame(max_cox_time,max_cox_risk,max_cox_event,max_cox_surv,max_cox_stderr,max_cox_lower,max_cox_upper)
			colnames(max_cox_pred_curve_df) <- c('time','n.risk','n.event','surv','std.err','lower','upper')
	
			idx0 = 1
			for (j in 1:length(cur_rows)) {
				tmp_idx = which(cox_pred_curve$time <= test_sfdc[j,'tstop'])
				if (length(tmp_idx) > 0) {
					idx = max(tmp_idx)
				#	cat(sprintf("i,j=(%4s,%4s) ...%s\n",i,j,idx))
					if (idx > idx0) {
						surv_curve = append(surv_curve,cox_pred_curve$surv[idx0:idx,cur_rows[j]])
						#cat(sprintf("(time,tstop,time+,idx0,idx,surv) = (%s,%s,%s,%s,%s,%s,%s)\n",cox_pred_curve_df[idx,'time'],test_sfdc[j,'tstop'],
						#																	cox_pred_curve_df[idx+1,'time'],idx0,idx,
						#																	length(idx0:idx),j+3))
						idx0 = idx + 1
					}
				}
			}
		} else {
			max_cox_pred_curve_df <- data.frame(time=0,n.risk=0,n.event=0,surv=1,std.err=0,lower=1,upper=1)
		}
	
		global_surv = cbind(global_surv,data.frame( append(surv_curve,rep(NA,length(cox_pred_curve$time)-length(surv_curve))) ) ) 
		names(global_surv)[length(names(global_surv))] <- i  

		#############################################
		# Algo to determine breakpoints
		# 1) Stitch together all survival curves
		# 2) Take mean and std.dev at each time
		# 3) Poly fit to +1sigma / -1sigma 
		# 4) Determine where predicted curves fall
		# 5) Categorize ALL accounts 
		#############################################
	
		if (PLOT_SURVIVAL_CURVES) {
			cur_data = data.frame(A=final_record_test_sdata[i,cur_var[1]])
			names(cur_data)[names(cur_data) == 'A'] <- cur_var[1]
	
			plot_title = sprintf("%4i . Cox Pred . %s=%s",i,cur_var[1],final_record_test_sdata[i,cur_var[1]]) 
			if (length(cur_var) > 1) {
				for (j in 2:length(cur_var)) {
					cur_data = cbind(cur_data,data.frame(A=final_record_test_sdata[i,cur_var[j]]) )
					names(cur_data)[names(cur_data) == 'A'] <- cur_var[j]
					plot_title = sprintf("%s,%s=%s",plot_title,cur_var[j],final_record_test_sdata[i,cur_var[j]]) 
				}
			}
			plot_title = sprintf("%s\nChurn=%s,Time=%s",plot_title,final_record_test_sdata[i,'churn'],final_record_test_sdata[i,'tstop']) 
	
			if (length(dev.list()) < 59) {
				if (PLOT_ON) {
					dev.new(xpos=0,ypos=0,width=2*Xsize,height=2*Ysize)
					#cur_data[cur_var[3]] <- cur_data[cur_var[1]] - 20 
					plot(survfit(cox_churn_fit, newdata=cur_data), xlab = "Days", ylab="Survival %",main=plot_title) 
					plot(survfit(cox_churn_fit, newdata=test_sfdc), xlab = "Days", ylab="Survival %",main=plot_title) 
					abline(a=NULL,b=NULL,v=final_record_test_sdata[i,'tstop'],col=c('blue')) 
				}
			}
		}
	
		if (nrow(final_pred) == 0) {
			final_pred <- cbind(final_sfdc,max_cox_pred_curve_df)
		} else {
			final_pred <- rbind(final_pred,cbind(final_sfdc,max_cox_pred_curve_df))
		}
	}
	
	#############################################
	# Algo to determine breakpoints
	# 1) Stitch together all survival curves
	# 2) Take mean and std.dev at each time
	# 3) Poly fit to +1sigma / -1sigma 
	# 4) Determine where predicted curves fall
	# 5) Categorize ALL accounts 
	#############################################
	
	global_surv_mean = rowMeans(global_surv[,which(names(global_surv) != 'time')],na.rm=TRUE) 
	global_surv_sd = vector("numeric",length=0)
	for (i in 1:nrow(global_surv)) {
		global_surv_sd = append(global_surv_sd,sd(global_surv[i,which(!is.na(global_surv[i,]) & names(global_surv) != 'time')]) ) 
	}
	
	global_surv_stats <- data.frame(global_surv$time)
	global_surv_stats <- cbind(global_surv_stats,data.frame(global_surv_mean))
	global_surv_stats <- cbind(global_surv_stats,data.frame(global_surv_sd))
	colnames(global_surv_stats) <- c('time','global_mean','global_sd')
	global_surv_stats <- rbind(data.frame(time=0,global_mean=1,global_sd=0),global_surv_stats)
	if (max(global_surv_stats$time) < (MODEL_TIME_LIMIT+1)) {
		Nrow = nrow(global_surv_stats)
		global_surv_stats <- rbind(global_surv_stats,data.frame(time=MODEL_TIME_LIMIT+1,global_mean=global_surv_stats$global_mean[Nrow],global_sd=global_surv_stats$global_sd[Nrow]))
	}

	####################################################################################
	# Fill in the missing times within global_surv_stats
	# 1) No curve fit ... delta function at event locations within global_surv_stats
	####################################################################################
	
	global_surv_curve <- data.frame(time=1:max(global_surv_stats$time),global_mean=1,global_sd=0)
	cur_mean = 1
	cur_sd = 0
	for (i in 2:nrow(global_surv_stats)) {
		idx <- which(global_surv_curve$time < global_surv_stats$time[i] & global_surv_curve$time >= global_surv_stats$time[i-1])
		global_surv_curve[idx,'global_mean'] = cur_mean	
		global_surv_curve[idx,'global_sd'] = cur_sd	
		cur_mean = global_surv_stats$global_mean[i]	
		cur_sd = global_surv_stats$global_sd[i]	
	}
	global_surv_curve[max(global_surv_stats$time),'global_mean'] = cur_mean	
	global_surv_curve[max(global_surv_stats$time),'global_sd'] = cur_sd	
	
	#basehaz_fit <- basehaz(cox_churn_fit)
	#names(basehaz_fit)[names(basehaz_fit) == 'hazard'] <- 'baseline_hazard'
	#basehaz_fit['baseline_survival'] <- 1-basehaz_fit['baseline_hazard'] 
	
	#poly_fit <- lm(surv ~ poly(tstop,3),data=final_pred)
	#print(summary(poly_fit))
	
	cat(sprintf('nrow(final_pred) = %s\n',nrow(final_pred)))
	
	#########################################################################################################################
	# Who is at risk?
	# 1) Above mean + 1sig ... OK      ... i.e final_pred$survival >= basehaz_fit where final_pred$tstop = basehaz_fit$time
	# 2) Between 1) and 3) ... i.e final_pred$survival < basehaz_fit where final_pred$tstop = basehaz_fit$time
	# 3) Below mean - 1sig ... at risk ... i.e final_pred$survival < basehaz_fit where final_pred$tstop = basehaz_fit$time
	#########################################################################################################################
	cat('\n*********************************************\n')
	cat('************* At Risk Statistics ************\n')
	cat('*********************************************\n')
	
	final_predOLD <- final_pred
	final_pred <- final_pred[with(final_pred,order(time)),]
	#final_pred[which(final_pred$tstop > MODEL_TIME_LIMIT),'tstop'] <- MODEL_TIME_LIMIT
	row.names(final_pred) <- 1:nrow(final_pred)
	
	#final_pred <- merge(final_pred,basehaz_fit,by.x='tstop',by.y='time')
	final_pred <- merge(final_pred,global_surv_curve,by.x='tstop',by.y='time')
	
	cat(sprintf('nrow(final_pred) = %s\n',nrow(final_pred)))
	
	row.has.na <- apply(final_pred, 1, function(x){any(is.na(x))})
	final_pred <- final_pred[!row.has.na,]
	
	###################################################
	# Err on cautious side (after cross-validation)
	# 1) Equal will move to bottom bin
	# Good b/c lower bins will be monitored
	###################################################
	final_pred['pred_status'] <- rep(0,nrow(final_pred)) 
	if (OUTPUT_BINS != 4) {
		final_pred[which(final_pred['surv'] < (final_pred['global_mean']-final_pred['global_sd']*0.5)),'pred_status'] <- 3 
		final_pred[which(final_pred['surv'] >= (final_pred['global_mean']-final_pred['global_sd']*0.5)),'pred_status'] <- 2 
		final_pred[which(final_pred['surv'] >= (final_pred['global_mean']+final_pred['global_sd']*0.5)),'pred_status'] <- 1 
	} else {
		final_pred[which(final_pred['surv'] < (final_pred['global_mean']-final_pred['global_sd']*1.5)),'pred_status'] <- 4 
		final_pred[which(final_pred['surv'] >= (final_pred['global_mean']-final_pred['global_sd']*1.5)),'pred_status'] <- 3 
		final_pred[which(final_pred['surv'] >= (final_pred['global_mean']-final_pred['global_sd']*0.5)),'pred_status'] <- 2 
		final_pred[which(final_pred['surv'] >= (final_pred['global_mean']+final_pred['global_sd']*0.5)),'pred_status'] <- 1 
	}
	
	#############################################################################################
	# Write output file
	# 1) time --> latest event time (ie the latest churn time relative to the current account
	# 2) tstop --> current time of the given account
	#
	# Example
	# event 1 (time) ................... event 2 (time)
	#                       | tstop
	#
	#
	# In this case ... time will be = (event 1 time) ... until tstop == event 2 (time)
	#############################################################################################

	final_pred <- merge(final_pred,account_df[,c('Id','Name','MSA_Effective_Date__c','Yearly_Client_ARR__c','Current_Score_Value__c','Health_Category__c','Health_Category_Reason__c','Initial_Term_Length__c')],by.x='sfdc',by.y='Id',all.x=TRUE)	
	final_pred <- merge(final_pred,cancel_lookup[,c('sfdc','Total_Lost_ARR','Cancellation_Notice_Received','ChurnDate')],by.x='sfdc',by.y='sfdc',all.x=TRUE)	

	### Add the Days to Renewal ###
	days_to_renewal = rep(-1,nrow(final_pred))
	renewal_date = rep(-1,nrow(final_pred))
	renewal_qtr = rep(-1,nrow(final_pred))
	notice_qtr = rep(-1,nrow(final_pred))
	current_qtr = rep(-1,nrow(final_pred))
	gainsight_account_health = rep(-1,nrow(final_pred))
	for (j in 1:nrow(final_pred)) {
		current_qtr[j] = as.character(as.yearqtr(Sys.time()))
		days_to_renewal[j] = ceiling((final_pred$tstop[j] + 90)/365) * 365 - 90 - final_pred$tstop[j]
		if (final_pred$Current_Score_Value__c[j] >= 0) {
			gainsight_account_health[j] = '2_Red'
		}
		if (final_pred$Current_Score_Value__c[j] >= 51) {
			gainsight_account_health[j] = '2_Orange'
		}
		if (final_pred$Current_Score_Value__c[j] >= 66) {
			gainsight_account_health[j] = '1_Yellow'
		}
		if (final_pred$Current_Score_Value__c[j] >= 81) {
			gainsight_account_health[j] = '1_Green'
		}

		#########################################################################
		# Find churn date based on renewal dates
		# 1) If churn = 0, use the renewal date based on the MSA_Effective_Date
		# 3) If churn = 1, use the churn date
		#########################################################################
		past_date = FALSE
		first_year = TRUE
		if (final_pred$churn[j] == 0) {
			cur_date <- as.POSIXlt(final_pred$MSA_Effective_Date__c[j])  
			while(!past_date) {
				if (first_year) {
					cur_date$year = cur_date$year + final_pred$Initial_Term_Length__c[j]/12 
				} else {
					cur_date$year = cur_date$year + 1 
				}
				if (Sys.time() <= cur_date) {  
					renewal_date[j] = as.character(cur_date) 
					renewal_qtr[j] = as.character(as.yearqtr(cur_date)) 
					past_date = TRUE
				} 
				first_year = FALSE
			}

		} else {
			notice_date <- as.POSIXlt(final_pred$Cancellation_Notice_Received[j])  
			notice_qtr[j] = as.character(as.yearqtr(notice_date) )
			churn_date <- as.POSIXlt(final_pred$ChurnDate[j])  
			renewal_date[j] = as.character(churn_date) 
			renewal_qtr[j] = as.character(as.yearqtr(churn_date)) 
			final_pred$Yearly_Client_ARR__c[j] <- -final_pred$Total_Lost_ARR[j] 
		}
	}

	final_pred['gainsight_account_health'] <- gainsight_account_health
	final_pred['renewal_date'] <- renewal_date
	final_pred['renewal_qtr'] <- renewal_qtr
	final_pred['notice_qtr'] <- notice_qtr
	final_pred['current_qtr'] <- current_qtr
	final_pred['current_year'] <- ceiling(final_pred['tstop'] / 365) + 1
	target <- which(names(final_pred) == 'pred_status')[1]
	final_pred <- cbind(final_pred[,1:target,drop=F], data.frame(days_to_renewal), final_pred[,(target+1):length(final_pred),drop=F])

	forecast_summary <- ddply(final_pred,c('current_qtr','notice_qtr','renewal_qtr','current_year','pred_status','churn'),summarize,Naccount=length(Yearly_Client_ARR__c),sum_ARR=sum(Yearly_Client_ARR__c)) 
	forecast_predstatus_summary <- ddply(final_pred,c('renewal_qtr','pred_status'),summarize,Naccount_predstatus=length(Yearly_Client_ARR__c),sum_ARR_predstatus=sum(Yearly_Client_ARR__c)) 
	forecast_year_predstatus_summary <- ddply(final_pred,c('current_year','renewal_qtr','pred_status'),summarize,Naccount_year_predstatus=length(Yearly_Client_ARR__c),sum_ARR_year_predstatus=sum(Yearly_Client_ARR__c)) 
	forecast_global_summary <- ddply(final_pred,c('renewal_qtr'),summarize,Naccount_All=length(Yearly_Client_ARR__c),sum_ARR_All=sum(Yearly_Client_ARR__c)) 
	forecast_summary <- merge(forecast_summary,forecast_year_predstatus_summary,by.x=c('current_year','renewal_qtr','pred_status'),by.y=c('current_year','renewal_qtr','pred_status'),all.x=TRUE)
	forecast_summary <- merge(forecast_summary,forecast_predstatus_summary,by.x=c('renewal_qtr','pred_status'),by.y=c('renewal_qtr','pred_status'),all.x=TRUE)
	forecast_summary <- merge(forecast_summary,forecast_global_summary,by.x='renewal_qtr',by.y='renewal_qtr',all.x=TRUE)
	forecast_summary['ARR_%'] <- forecast_summary['sum_ARR']/forecast_summary['sum_ARR_All']
	forecast_summary['Account_%'] <- forecast_summary['Naccount']/forecast_summary['Naccount_All']

	write.csv(final_pred, file = sprintf("./output/final_pred_%s_MODELTIME%s_VAR_%s.csv",output_date,MODEL_TIME_LIMIT,paste(cur_var,collapse="_")), row.names=FALSE)
	
	col = c('blue','red')
	if (PLOT_PREDICTION) {
		dev.new(xpos=10,ypos=10,width=3*Xsize,height=2*Ysize)
		bins = as.integer(final_pred$churn)
		cur_title = sprintf("Case %3s ... Var = %s",ppp,paste(cur_var,collapse = ", ") )
		plot(final_pred$tstop,final_pred$surv, pch=bins, xlim=c(0,max(sfdc_max_tstop$max_tstop)*1.01),ylim=c(0.3,1.0),ylab="Survival %",main=cur_title,col=ifelse(final_pred$churn==1,'red','blue'),xaxt='n') 
		#lines(basehaz_fit$time,basehaz_fit$baseline_survival,col='black')
		lines(global_surv_curve$time,global_surv_curve$global_mean,col='black')
		lines(global_surv_curve$time,(global_surv_curve$global_mean + global_surv_curve$global_sd*0.5),col='black',lty=2)
		lines(global_surv_curve$time,(global_surv_curve$global_mean - global_surv_curve$global_sd*0.5),col='black',lty=2)
		if (OUTPUT_BINS == 4) {
			lines(global_surv_curve$time,(global_surv_curve$global_mean - global_surv_curve$global_sd*1.5),col='red',lty=2)
		}
		#points(final_pred$tstop,fitted(poly_fit),col='green')
		#lines(sort(final_pred$time),fitted(poly_fit)[order(final_pred$time)],col='red',type='b')
		axis(1,c(0,182,365,547,730,912,1095,1277,1460,1642,1825),las=1)
		legend(20,.9,pch=unique(bins),col=col,c('Current Customer','Notice to Churn'),bty='n')
	}
	
	col = c('blue','red')
	if (PLOT_PREDICTION) {
		dev.new(xpos=10,ypos=10,width=3*Xsize,height=2*Ysize)
		bins = as.integer(final_pred$churn)
		cur_title = sprintf("Case %3s ... Var = %s",ppp,paste(cur_var,collapse = ", ") )
		plot(final_pred$tstop,final_pred$surv,pch=bins,cex=final_pred$Yearly_Client_ARR__c/100000,xlim=c(0,max(sfdc_max_tstop$max_tstop)*1.01),
											ylim=c(0.3,1.0),ylab="Survival %",main=cur_title,col=ifelse(final_pred$churn==1,'red','blue'),xaxt='n') 
		#lines(basehaz_fit$time,basehaz_fit$baseline_survival,col='black')
		lines(global_surv_curve$time,global_surv_curve$global_mean,col='black')
		lines(global_surv_curve$time,(global_surv_curve$global_mean + global_surv_curve$global_sd*0.5),col='black',lty=2)
		lines(global_surv_curve$time,(global_surv_curve$global_mean - global_surv_curve$global_sd*0.5),col='black',lty=2)
		if (OUTPUT_BINS == 4) {
			lines(global_surv_curve$time,(global_surv_curve$global_mean - global_surv_curve$global_sd*1.5),col='red',lty=2)
		}
		#points(final_pred$tstop,fitted(poly_fit),col='green')
		#lines(sort(final_pred$time),fitted(poly_fit)[order(final_pred$time)],col='red',type='b')
		axis(1,c(0,182,365,547,730,912,1095,1277,1460,1642,1825),las=1)
		legend(20,.9,pch=unique(bins),col=col,c('Current Customer','Notice to Churn'),bty='n')
	}

	predict_expected <-data.frame(predict(cox_churn_fit,type='expected'))
	names(predict_expected)[names(predict_expected) == names(predict_expected[1])] <- 'predict_expected'
	#####################
	# Confusion Matrix
	#####################
	
	#############################
	# All-Time Stats 
	#############################

	cat('\n*********************************************\n')
	cat('*********** Add To Output Results ***********\n')
	cat('*********************************************\n')
	
	new_row <- churn_stats('All-Time',final_pred,cox_churn_fit)
	year_row1 <- data.frame(churn_stats('Yr 1',final_pred[which(final_pred$time < 365),],cox_churn_fit),stringsAsFactors=FALSE )
	year_row2 <- data.frame(churn_stats('Yr 2',final_pred[which(final_pred$time >= 365 & final_pred$time < 730),],cox_churn_fit),stringsAsFactors=FALSE )
	year_row3 <- data.frame(churn_stats('Yr 3',final_pred[which(final_pred$time >= 730),],cox_churn_fit),stringsAsFactors=FALSE )
	year_row4 <- data.frame(churn_stats('Yr 3',final_pred[which(final_pred$time >= 730),],cox_churn_fit),stringsAsFactors=FALSE )
	year_row5 <- data.frame(churn_stats('Yr 3',final_pred[which(final_pred$time >= 730),],cox_churn_fit),stringsAsFactors=FALSE )

	year_row1['year'] <- 1
	year_row2['year'] <- 2
	year_row3['year'] <- 3
	year_row4['year'] <- 4
	year_row5['year'] <- 5 

	test1_df <- data.frame(rbind(year_row1,year_row1,year_row1),stringsAsFactors=FALSE)		
	test2_df <- data.frame(rbind(year_row2,year_row2,year_row2),stringsAsFactors=FALSE)		
	test3_df <- data.frame(rbind(year_row3,year_row3,year_row3),stringsAsFactors=FALSE)		
	test4_df <- data.frame(rbind(year_row4,year_row4,year_row4),stringsAsFactors=FALSE)		
	test5_df <- data.frame(rbind(year_row5,year_row5,year_row5),stringsAsFactors=FALSE)		
	names(test1_df) <- year_row_names
	names(test2_df) <- year_row_names
	names(test3_df) <- year_row_names
	names(test4_df) <- year_row_names
	names(test5_df) <- year_row_names
	test1_df[2,'bin1_churn_per'] <- test1_df[1,'bin2_churn_per']
	test1_df[2,'bin1_customer_per'] <- test1_df[1,'bin2_customer_per']
	test1_df[3,'bin1_churn_per'] <- test1_df[1,'bin3_churn_per']
	test1_df[3,'bin1_customer_per'] <- test1_df[1,'bin3_customer_per']
	test1_df['pred_status'] <- 1:3
	test2_df[2,'bin1_churn_per'] <- test2_df[1,'bin2_churn_per']
	test2_df[2,'bin1_customer_per'] <- test2_df[1,'bin2_customer_per']
	test2_df[3,'bin1_churn_per'] <- test2_df[1,'bin3_churn_per']
	test2_df[3,'bin1_customer_per'] <- test2_df[1,'bin3_customer_per']
	test2_df['pred_status'] <- 1:3
	test3_df[2,'bin1_churn_per'] <- test3_df[1,'bin2_churn_per']
	test3_df[2,'bin1_customer_per'] <- test3_df[1,'bin2_customer_per']
	test3_df[3,'bin1_churn_per'] <- test3_df[1,'bin3_churn_per']
	test3_df[3,'bin1_customer_per'] <- test3_df[1,'bin3_customer_per']
	test3_df['pred_status'] <- 1:3
	test4_df[2,'bin1_churn_per'] <- test4_df[1,'bin2_churn_per']
	test4_df[2,'bin1_customer_per'] <- test4_df[1,'bin2_customer_per']
	test4_df[3,'bin1_churn_per'] <- test4_df[1,'bin3_churn_per']
	test4_df[3,'bin1_customer_per'] <- test4_df[1,'bin3_customer_per']
	test4_df['pred_status'] <- 1:3
	test5_df[2,'bin1_churn_per'] <- test5_df[1,'bin2_churn_per']
	test5_df[2,'bin1_customer_per'] <- test5_df[1,'bin2_customer_per']
	test5_df[3,'bin1_churn_per'] <- test5_df[1,'bin3_churn_per']
	test5_df[3,'bin1_customer_per'] <- test5_df[1,'bin3_customer_per']
	test5_df['pred_status'] <- 1:3

	year_row_df <- rbind(test1_df,test2_df,test3_df,test4_df,test5_df)

	### Max out at 10 variables	
	for (i in 1:length(cox_churn_fit$coefficients)) {
		new_row = cbind(new_row,names(cox_churn_fit$coefficients[i]))
		new_row = cbind(new_row,zp$table[i,3],summary(cox_churn_fit)$coefficients[i,5],summary(cox_churn_fit)$coefficients[i,2],summary(cox_churn_fit)$coefficients[i,3]  )
	}
	if (length(cox_churn_fit$coefficients) < 10) {
		for (i in (length(cox_churn_fit$coefficients)+1):10) {
			new_row = cbind(new_row,"","","","","")
		}
	}
	output_df = rbind(output_df,new_row)

	cat(sprintf("Case %3s complete ... %.3f sec\n",ppp,as.numeric(proc.time()['elapsed']-ptm['elapsed']) ) )
	
}

names(output_df) = c('MODEL_TIME_LIMIT','Ntotal','Nevent','Nremoved','var_independent','Ncoef',
						'bin1_OK','bin1_churn','bin2_OK','bin2_churn','bin3_OK','bin3_churn',   
						'bin1_churn_per','bin2_churn_per','bin3_churn_per',
						'bin1_customer','bin2_customer','bin3_customer',
						'bin1_total_churn','bin2_total_churn','bin3_total_churn', 
						'Likelihood_value','Likelihood_df','Likelihood_p',
						'Wald_value','Wald_df','Wald_p',
						'Logrank_value','Logrank_df','Logrank_p',
						'V1','V1_PH','V1_p','V1_exp_coef','V1_se_coef',
						'V2','V2_PH','V2_p','V2_exp_coef','V2_se_coef',
						'V3','V3_PH','V3_p','V3_exp_coef','V3_se_coef',
						'V4','V4_PH','V4_p','V4_exp_coef','V4_se_coef',
						'V5','V5_PH','V5_p','V5_exp_coef','V5_se_coef',
						'V6','V6_PH','V6_p','V6_exp_coef','V6_se_coef',
						'V7','V7_PH','V7_p','V7_exp_coef','V7_se_coef',
						'V8','V8_PH','V8_p','V8_exp_coef','V8_se_coef',
						'V9','V9_PH','V9_p','V9_exp_coef','V9_se_coef',
						'V10','V10_PH','V10_p','V10_exp_coef','V10_se_coef')
 
forecast_summary <- merge(forecast_summary,year_row_df[,c('year','pred_status','MODEL_TIME_LIMIT','bin1_churn_per','bin1_customer_per')],by.x=c('current_year','pred_status'),by.y=c('year','pred_status'),all.x=TRUE)
churn_forecast = vector('numeric',length=0)
churn_verified = vector('numeric',length=0)
churn_target = vector('numeric',length=0)
for (i in 1:nrow(forecast_summary)) {
	churn_target <- append(churn_target,as.numeric(forecast_summary[i,'bin1_churn_per'])*as.numeric(forecast_summary[i,'sum_ARR_year_predstatus']) )
	if (forecast_summary$churn[i] == 0) {
		churn_verified <- append(churn_verified,0)
		if (!is.na(forecast_summary[i,'sum_ARR'])) {
			churn_forecast <- append(churn_forecast,as.numeric(forecast_summary[i,'bin1_churn_per'])*as.numeric(forecast_summary[i,'sum_ARR']) )
		} else {
			churn_forecast <- append(churn_forecast,0)
		}
	} else {
		churn_forecast <- append(churn_forecast,0)
		if (!is.na(forecast_summary[i,'sum_ARR'])) {
			churn_verified <- append(churn_verified,as.numeric(forecast_summary[i,'sum_ARR']) )
		} else {
			churn_verified <- append(churn_verified,0)
		}
	}
}
forecast_summary['churn_target'] <- churn_target
forecast_summary['churn_forecast'] <- churn_forecast
forecast_summary['churn_verified'] <- churn_verified

write.csv(forecast_summary, file = sprintf("./output/forecast_summary.csv"), row.names=FALSE)

fileout = sprintf("./output/model_performance_%s_MODELTIME%s.csv",output_date,MODEL_TIME_LIMIT)
if (file.exists(fileout)) {
	cat('File Exists ... Append\n')
	existing_df <- read.csv(fileout,header=TRUE,sep=",",stringsAsFactors=FALSE,encoding='UTF-8')
	output_df = rbind(existing_df,output_df)
}

write.csv(output_df,file = fileout, row.names=FALSE)
write.csv(final_pred, file = sprintf("./output/final_pred_%s.csv",output_date), row.names=FALSE)
#write.csv(final_pred, file = sprintf("./output/final_pred_%s.csv",gsub("-","",format(strptime(date(),"%a %b %d %H:%M:%S %Y")[1],"%Y-%m-%d")[1])), row.names=FALSE)

