update_par <- function(num)
{
  #pch = symbol_type
  #cex = symbol size
  #lwd = line type
  #cex.axis = axis font scaled by cex
  #cex.lab = axis lable scaled by cex
  #mai = (botton,left,top,right) margin in inches
  #mgp = (title,label,line) margin ... c(3,1,0) default ... make 3 decrease to make title and axis be closer 
  par(pch=num,las=2,lwd=2,cex=1,cex.main=1.0,cex.axis=0.4,cex.lab=0.8,cex=2,mai=c(2.0,1.0,0.7,0.3),mgp=c(2.5,0.5,0) ) #,o
  #par(pch=num,lwd=2,cex=2,cex.axis=1,cex.lab=1,cex=2,omd=c(0.1,0.9,0.1,0.9),mgp=c(1.5,0.5,0) )#,omi=c(0.2,0.2,0.2,
  
}

mysql_query_noreturn <- function(con,query) 
{
  D <- dbSendQuery(con,query)
  dbHasCompleted(D);
  dbClearResult(D);
}

mysql_query <- function(con,query) 
{
  D <- dbSendQuery(con,query)
  output <- fetch(D,-1) ## Put all values into output
  dbHasCompleted(D);
  dbClearResult(D);
  return(output);
}

##################################################
# Function computes model error for linear model
##################################################
model_cv_error_lm <- function(fit,y,x) 
{
	cv_error = 0
	N = length(x)
	for (i in 1:N) {
		model = 0 
		for (j in 1:4) {
			if (is.na(fit$coefficients[j]) == FALSE) {
				model = model + as.numeric(fit$coefficients[j]) * x[i]^j 
				#cat(sprintf('%s . %s. %s . %s . %s \n',model,as.numeric(fit$coefficients[j]),input$Ntotal_library[i],input$Ntotal_library[i]^2,input$Ntotal_library[i]^3) )
			}
		}
		#cat('model = ',model,'\n')
		#cat('value = ',0.5 / N * (input$Nvideo_distinct[i] - model) * (input$Nvideo_distinct[i] - model),'\n')  
		cv_error = cv_error + 0.5 / N * (y[i] - model) * (y[i] - model)  
	}
	return(cv_error)
}

model_cv_error_log10 <- function(fit,y,x) 
{
	cv_error = 0
	N = length(x)
	for (i in 1:N) {
		#model = fit$coefficients[1] + fit$coefficients[2] * log10(x[i]) 
		model = fit$coefficients[1] * log10(x[i]) 
		cv_error = cv_error + 0.5 / N * (y[i] - model) * (y[i] - model)  
	}
	return(cv_error)
}
read_xls <- function(filename,Nsheet) 
{
	return(read.xls (filename, sheet = Nsheet, header = TRUE,stringsAsFactors=FALSE))
}

find_month <- function(input_date) 
{
  return(read.xls (filename, sheet = 1, header = TRUE))
}

closeDBconnections <- function()
{
	#################################
	## Close all existing connections 
	#################################
	all_cons <- dbListConnections(MySQL())
	for (con in all_cons) dbDisconnect(con)
}

#####################
# Create hist names 
#####################
hist_names_sec <- function(breaks) {

	names <- vector("numeric",length=0)
	for (i in 1:(length(breaks)-2)) {
		names <- append(names,sprintf("%d-%d min",breaks[i]/60,breaks[i+1]/60))
	}
	names <- append(names,sprintf(">%d min",breaks[length(breaks)-1]/60) )

	return(names)
}

#####################
# Create hist names 
#####################
hist_names <- function(breaks) {

	names <- vector("numeric",length=0)
	for (i in 1:(length(breaks)-2)) {
		names <- append(names,sprintf("%d-%d",breaks[i],breaks[i+1]))
	}
	names <- append(names,sprintf(">%d",breaks[length(breaks)-1]) )

	return(names)
}

#####################################################
# Create hist of input_var using bins from input_hist
#####################################################
createhist <- function(summary_type,input_hist,breaks,input_var) {

	output <- vector("numeric",length=0)
	for (i in 2:length(breaks) ) {
		idx <- which(input_hist > breaks[i-1] & input_hist <= breaks[i])

		#print(input_var[idx])
		if (length(idx) > 0) {
			if (summary_type == "MEAN") {
				#cat(length(idx)," . ",mean(input_var[idx]),"\n")
				output <- append(output,mean(input_var[idx]))
			} else if (summary_type == "MEDIAN") {
				output <- append(output,median(input_var[idx] ))
			}
		} else {
			output <- append(output,0.0)
		}
	}
	cat("\n")
	return(output)
}

#####################################################
# Create contact_lookup 
#####################################################
create_contact_lookup <- function() {

	contact_num = vector("character",length=0)
	contact_name = vector("character",length=0)

	for (i in 1:15) {
		contact_num = append(contact_num,i)
	}
	contact_name = append(contact_name,"Pres,Prin,C-level,Exec Dir")
	contact_name = append(contact_name,"VP")
	contact_name = append(contact_name,"Director")
	contact_name = append(contact_name,"Manager")
	contact_name = append(contact_name,"Head Of")
	contact_name = append(contact_name,"Supervisor, Administrator")
	contact_name = append(contact_name,"Specialist,Analyst,Coordinator,Generalist")
	contact_name = append(contact_name,"Advisor,Consultant")
	contact_name = append(contact_name,"HR")
	contact_name = append(contact_name,"Partner")
	contact_name = append(contact_name,"")
	contact_name = append(contact_name,"")
	contact_name = append(contact_name,"")
	contact_name = append(contact_name,"None")
	contact_name = append(contact_name,"Unknown")
	contact_lookup <- data.frame(contact_num,contact_name,stringsAsFactors=FALSE)

	return(contact_lookup)
}

#####################################################
# Create hierarchical clustering dendogram
#####################################################
createHCplot <- function(cluster_table)
{
	######################
	# Create row names 
	######################
	for (i in 1:nrow(cluster_table)) {
		row.names(cluster_table)[i] <- sprintf('%s - %4d , %5d',cluster_table[i,1],cluster_table[i,3],cluster_table[i,2] )   
	}

	######################
	# Remove 1st column
	# Set = to the row.name
	######################
	cluster_table <- cluster_table[,4:ncol(cluster_table)]

	scaled.cluster_table <- scale(cluster_table)
	print(colMeans(cluster_table) )
	print(apply(cluster_table,2,sd) )

	d <- dist(cluster_table)
	hc <- hclust(d)
	dev.new(xpos=0,ypos=0,width=16,height=9.5)
	plot(hc,cex=0.75)
  
}

