
churn_stats <- function(output_str,input_pred,cox_fit)
{

	nochurn_pred <- input_pred[which(input_pred$churn == 0),]
	nochurn_tab <- table(factor(nochurn_pred$pred_status),factor(nochurn_pred$gainsight_account_health))	

	pred <- factor(input_pred$pred_status)
	actual <- factor(input_pred$churn)
	xtab <- table(pred,actual)

	total_churn  <- sum(xtab[,2])
	if (OUTPUT_BINS == 4) {
		total_churn  <- sum(xtab[,2])
	}
	idx_tab1 = which(row.names(xtab) == 1)
	if (length(idx_tab1) > 0) {
		bin1_OK  <- xtab[idx_tab1,1]
		bin1_Churn  <- xtab[idx_tab1,2]
		bin1_customer_per  <- as.numeric(xtab[idx_tab1,1] + xtab[idx_tab1,2]) / as.numeric(length(unique(input_pred$sfdc)))
		bin1_churn_per  <- as.numeric(xtab[idx_tab1,2]) / as.numeric(xtab[idx_tab1,1] + xtab[idx_tab1,2])
		bin1_total_churn_per  <- as.numeric(xtab[idx_tab1,2]) / total_churn
	} else {
		bin1_OK  <- 0
		bin1_Churn  <- 0
		bin1_customer_per  <- 0 
		bin1_churn_per  <- 0
		bin1_total_churn_per  <- 0 
	}	
	idx_tab2 = which(row.names(xtab) == 2)
	if (length(idx_tab2) > 0) {
		bin2_OK  <- xtab[idx_tab2,1]
		bin2_Churn  <- xtab[idx_tab2,2]
		bin2_customer_per  <- as.numeric(xtab[idx_tab2,1] + xtab[idx_tab2,2]) / as.numeric(length(unique(input_pred$sfdc)))
		bin2_churn_per  <- as.numeric(xtab[idx_tab2,2]) / as.numeric(xtab[idx_tab2,1] + xtab[idx_tab2,2])
		bin2_total_churn_per  <- as.numeric(xtab[idx_tab2,2]) / total_churn
	} else {
		bin2_OK  <- 0
		bin2_Churn  <- 0
		bin2_customer_per  <- 0 
		bin2_churn_per  <- 0
		bin2_total_churn_per  <- 0 
	}	
	idx_tab3 = which(row.names(xtab) == 3)
	if (length(idx_tab3) > 0) {
		bin3_OK  <- xtab[idx_tab3,1]
		bin3_Churn  <- xtab[idx_tab3,2]
		bin3_customer_per  <- as.numeric(xtab[idx_tab3,1] + xtab[idx_tab3,2]) / as.numeric(length(unique(input_pred$sfdc)))
		bin3_churn_per  <- as.numeric(xtab[idx_tab3,2]) / as.numeric(xtab[idx_tab3,1] + xtab[idx_tab3,2])
		bin3_total_churn_per  <- as.numeric(xtab[idx_tab3,2]) / total_churn
	} else {
		bin3_OK  <- 0
		bin3_Churn  <- 0
		bin3_customer_per  <- 0 
		bin3_churn_per  <- 0
		bin3_total_churn_per  <- 0 
	}	

#	accuracy  <- as.numeric(xtab[1,1] + xtab[2,2]) / as.numeric(xtab[1,1] + xtab[1,2] + xtab[2,1] + xtab[2,2])
#	accuracy  <- as.numeric(xtab[1,1] + xtab[2,2]) / as.numeric(xtab[1,1] + xtab[1,2] + xtab[2,1] + xtab[2,2])
#	precision <- as.numeric(xtab[2,2]) / as.numeric(xtab[2,2] + xtab[1,2])
#	recall    <- as.numeric(xtab[2,2]) / as.numeric(xtab[2,2] + xtab[2,1])
#	Fscore    <- as.numeric(2*precision*recall) / (precision + recall) 
	
	cat(sprintf("\n\n*********************\n***** %s *****\n",output_str))
	cat(sprintf("*********************\n"))
	print(xtab)
	cat(sprintf("Bin 1 Churn = %7.2f%% ... (%% customers, total churn) = (%7.2f%%,%7.2f%%)\n",bin1_churn_per*100,bin1_customer_per*100,bin1_total_churn_per*100) )
	cat(sprintf("Bin 2 Churn = %7.2f%% ... (%% customers, total churn) = (%7.2f%%,%7.2f%%)\n",bin2_churn_per*100,bin2_customer_per*100,bin2_total_churn_per*100) )
	cat(sprintf("Bin 3 Churn = %7.2f%% ... (%% customers, total churn) = (%7.2f%%,%7.2f%%)\n",bin3_churn_per*100,bin3_customer_per*100,bin3_total_churn_per*100) )
	if (OUTPUT_BINS == 4) {
		cat(sprintf("Bin 4 Churn = %7.2f%% ... (%% customers, total churn) = (%7.2f%%,%7.2f%%)\n",bin4_churn_per*100,bin4_customer_per*100,as.numeric(xtab[4,2])/total_churn*100) )
	}
	#cat(sprintf("Fscore    = %.2f%%\n",Fscore*100))
	#cat(sprintf("Precision = %.2f%%\n",precision*100))
	#cat(sprintf("Recall    = %.2f%%\n",recall*100))

	print(nochurn_tab)
	
	new_row = cbind(MODEL_TIME_LIMIT,cox_fit$n,cox_fit$nevent,length(cox_fit$na.action),as.character(cox_fit$formula)[3],length(cox_fit$coefficients), 
					bin1_OK,bin1_Churn,bin2_OK,bin2_Churn,bin3_OK,bin3_Churn,
					bin1_churn_per,bin2_churn_per,bin3_churn_per,
					bin1_customer_per,bin2_customer_per,bin3_customer_per,
					bin1_total_churn_per,bin2_total_churn_per,bin3_total_churn_per,
					as.numeric(summary(cox_fit)$logtest['test']),as.integer(as.numeric(summary(cox_fit)$logtest['df'])),as.numeric(summary(cox_fit)$logtest['pvalue']),  
					as.numeric(summary(cox_fit)$waldtest['test']),as.integer(as.numeric(summary(cox_fit)$waldtest['df'])),as.numeric(summary(cox_fit)$waldtest['pvalue']),  
					as.numeric(summary(cox_fit)$sctest['test']),as.integer(as.numeric(summary(cox_fit)$sctest['df'])),as.numeric(summary(cox_fit)$sctest['pvalue']) )

	return(new_row)

}


