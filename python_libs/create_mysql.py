#! /usr/bin/env python
from common_libs import *
from datetime import datetime, timedelta
import MySQLdb as mdb
import time

sys.path.insert(0,'/home/djohnson/analytics/python_libs');
from common_libs import *

def drop_mysql_db(con,DBNAME):

	cur = con.cursor()
	query = 'DROP DATABASE IF EXISTS %s' % DBNAME
	cur.execute(query);

def create_mysql_db(con,DBNAME):

	cur = con.cursor()
	query = 'CREATE DATABASE IF NOT EXISTS %s' % DBNAME
	cur.execute(query);

def drop_mysql_table(con,DBNAME,TABLENAME):

	cur = con.cursor()
	query = 'DROP TABLE IF EXISTS %s.%s' % (DBNAME,TABLENAME)
	cur.execute(query);

def create_attask_DATE_table(con,DBNAME, TABLENAME):

	cur = con.cursor()
	query = 'USE %s' % DBNAME;
	cur.execute(query);

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		day			  	 	VARCHAR(4), \
		inputDate	  	 	DATE, \
		startTime  	 		DATETIME, \
		endTime			  	DATETIME)" % TABLENAME

	cur.execute(query)
	con.commit()

def insert_into_attask_DATE_table(con,DBNAME,TABLENAME,out,start):

	con = None
	con = mdb.connect('localhost','root','','');
	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################
	query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				out['curDay'], \
				out['inputDate'], \
				out['startTime'], \
				out['endTime'])

	print(query)
	cur.execute(query)
	con.commit() # necessary to finish statement

def create_attask_NOTE_table(con,DBNAME, TABLENAME):

	cur = con.cursor()
	query = 'USE %s' % DBNAME;
	cur.execute(query);

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		note_id		  	  	 	VARCHAR(33), \
		customer_id	  	  	 	VARCHAR(33), \
		owner_id	  	  	 	VARCHAR(33), \
		entry_date 		  	 	DATETIME, \
		subject		  	 		VARCHAR(21), \
		thread_id	 	     	VARCHAR(33), \
		thread_date  	 		DATETIME, \
		top_note_obj_code     	VARCHAR(10), \
		top_obj_id		     	VARCHAR(33), \
		obj_id			     	VARCHAR(33), \
		parent_note_id	     	VARCHAR(33), \
		auditType		     	VARCHAR(3), \
		auditText			  	VARCHAR(1000), \
		ext_ref_id			  	VARCHAR(33), \
		is_message			  	VARCHAR(6), \
		num_replies			  	SMALLINT, \
		noteText			  	VARCHAR(2000))" % TABLENAME

	cur.execute(query)
	con.commit()

def insert_into_attask_NOTE_table(con,DBNAME,TABLENAME,out,start):

	cur = con.cursor()

	out = AttaskDate_to_datetime(out,'entryDate')
	out = AttaskDate_to_datetime(out,'threadDate')

	#########################
	## INSERT DATA INTO TABLE
	#########################
	for j in range(0,len(out)):
		try:
			tmp_auditText = out.auditText[j].encode('ascii','ignore').replace("'","")
		except:
			tmp_auditText = None

		try:
			tmp_noteText = out.noteText[j].encode('ascii','ignore').replace("'"," ").replace('"',' ').replace("  "," ").replace('\\','')
		except:
			tmp_noteText = None

		#printf("%s\n",tmp_noteText)
		if ((j % 1000) == 999):
			printf('[create_mysql.py][insert_into_attask_NOTE_table] %7d of %7d Elements ... %.3f sec\n',j+1,len(out),time.time()-start)

		query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				out.ID[j], \
				out.customerID[j], \
				out.ownerID[j], \
				out.entryDate[j], \
				out.subject[j], \
				out.threadID[j], \
				out.threadDate[j], \
				out.topNoteObjCode[j], \
				out.topObjID[j], \
				out.objID[j], \
				out.parentNoteID[j], \
				out.auditType[j], \
				tmp_auditText, \
				out.extRefID[j], \
				out.isMessage[j], \
				out.numReplies[j], \
				tmp_noteText)

		#print(query)
		cur.execute(query)
		con.commit() # necessary to finish statement

def create_attask_PROJECT_table(con,DBNAME, TABLENAME):

	cur = con.cursor()

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s.%s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		input_date_id									SMALLINT UNSIGNED, \
		projectID										VARCHAR(33), \
		name											VARCHAR(100), \
		DE_Additional_Client_Notes						VARCHAR(100), \
		DE_Backup_Voice_Talent							VARCHAR(50), \
		DE_Benefits_Eligible_Employees					MEDIUMINT UNSIGNED, \
		DE_Brand_Management_Designer					VARCHAR(50), \
		DE_Brand_Management_Writer						VARCHAR(50), \
		DE_Calculated_Completion_Date					DATETIME, \
		DE_Current_Velocity								MEDIUMINT UNSIGNED, \
		DE_Customer_Goals_Objectives					VARCHAR(200), \
		DE_Customer_Hard_Deadline						DATE, \
		DE_Date_Logged									DATE, \
		DE_Days_In_Queue								SMALLINT UNSIGNED, \
		DE_Days_In_Queue_Color							VARCHAR(20), \
		DE_Days_with_Voice_Actor_1						FLOAT, \
		DE_Days_with_Voice_Actor_2						FLOAT, \
		DE_Days_with_Voice_Actor_3						FLOAT, \
		DE_Days_with_Voice_Actor_4						FLOAT, \
		DE_Days_with_Voice_Actor_5						FLOAT, \
		DE_Days_with_Voice_Actor_6						FLOAT, \
		DE_Days_with_Voice_Actor_7						FLOAT, \
		DE_Days_with_Voice_Actor_Final_Cut				FLOAT, \
		DE_Days_with_Voice_Actor_Initial_Development	FLOAT, \
		DE_Driver_for_Hard_Deadline						VARCHAR(50), \
		DE_Driver_for_Soft_Deadline						VARCHAR(50), \
		DE_Effective_Date								DATE, \
		DE_G1_G2										VARCHAR(3), \
		DE_Line_Item_ID									VARCHAR(16), \
		DE_List_demos_shown_and_customer_feedback		VARCHAR(100), \
		DE_New_Customer_Upsell_Renewal					VARCHAR(13), \
		DE_New_Titles_Requested							VARCHAR(50), \
		DE_OPERATIONAL_Product_Line						VARCHAR(30), \
		DE_Opportunity_ID								VARCHAR(16), \
		DE_Order_Number									VARCHAR(4), \
		DE_Persona										VARCHAR(12), \
		DE_Product_Code									VARCHAR(5), \
		DE_Product_Group								VARCHAR(4), \
		DE_Product_Line									VARCHAR(13), \
		DE_Product_Title								VARCHAR(60), \
		DE_Product_Video_Category						VARCHAR(20), \
		DE_Product_Video_Subcategory					VARCHAR(20), \
		DE_Production_Tool								VARCHAR(10), \
		DE_Program_Start_Date							DATE, \
		DE_Project_Total_Velocity						INT, \
		DE_Renewal_Date									DATE, \
		DE_Requested_Kickoff_Call_Timing				DATE, \
		DE_Role_Suffix									VARCHAR(50), \
		DE_Script_Word_Count							MEDIUMINT UNSIGNED, \
		DE_Soft_Deadline								DATE, \
		DE_Substatus									VARCHAR(50), \
		DE_Target_Deadline_and_Drivers					VARCHAR(100), \
		DE_Target_Language								VARCHAR(50), \
		DE_Template_Type								VARCHAR(40), \
		DE_Template_Version								VARCHAR(5), \
		DE_To_Translation_Services						VARCHAR(4), \
		DE_Video_Points									DATE, \
		DE_Video_Type									DATE, \
		DE_Voice_In_1									DATE, \
		DE_Voice_In_2									DATE, \
		DE_Voice_In_3									DATE, \
		DE_Voice_In_4									DATE, \
		DE_Voice_In_5									DATE, \
		DE_Voice_In_6									DATE, \
		DE_Voice_In_7									DATE, \
		DE_Voice_In_Initial_Development					DATE, \
		DE_Voice_Out_1									DATE, \
		DE_Voice_Out_2									DATE, \
		DE_Voice_Out_3									DATE, \
		DE_Voice_Out_4									DATE, \
		DE_Voice_Out_5									DATE, \
		DE_Voice_Out_6									DATE, \
		DE_Voice_Out_7									DATE, \
		DE_Voice_Out_Final_Cut							DATE, \
		DE_Voice_Out_Initial_Development				DATE, \
		voice_talent									VARCHAR(20), \
		DE_Voice_in_Final_Cut							DATE, \
		actualCompletionDate							DATETIME, \
		actualStartDate									DATETIME, \
		actualDurationMinutes							MEDIUMINT UNSIGNED, \
		actualLaborCost									FLOAT, \
		actualWorkRequired								MEDIUMINT UNSIGNED, \
		categoryID										VARCHAR(33), \
		company_name									VARCHAR(100), \
		companyID										VARCHAR(33), \
		conditionID										VARCHAR(3), \
		conditionType									VARCHAR(4), \
		cpi												FLOAT(5,2), \
		csi												FLOAT(5,2), \
		description										VARCHAR(20), \
		durationMinutes									MEDIUMINT UNSIGNED, \
		eac												FLOAT, \
		enteredByID										VARCHAR(33), \
		entryDate										DATETIME, \
		estCompletionDate								DATETIME, \
		estStartDate									DATETIME, \
		lastUpdateDate									DATETIME, \
		groupID											VARCHAR(33), \
		owner											VARCHAR(50), \
		ownerID											VARCHAR(33), \
		percentComplete									FLOAT(5,2), \
		plannedLaborCost								FLOAT(5,2), \
		plannedCompletionDate							DATETIME, \
		plannedStartDate								DATETIME, \
		portfolioID										VARCHAR(33), \
		portfolio										VARCHAR(50), \
		priority										SMALLINT UNSIGNED, \
		projectedCompletionDate							DATETIME, \
		projectedStartDate								DATETIME, \
		referenceNumber									VARCHAR(8), \
		risk											SMALLINT UNSIGNED, \
		spi												FLOAT(5,2), \
		sponsorID										VARCHAR(33), \
		status											VARCHAR(4), \
		statusUpdate									VARCHAR(100), \
		template										VARCHAR(30), \
		templateID										VARCHAR(33), \
		workRequired									MEDIUMINT UNSIGNED, \
		sf_LineItemId									VARCHAR(16), \
		sf_Branding										VARCHAR(15), \
		sf_Discount										FLOAT(5,2), \
		sf_library										VARCHAR(2), \
		sf_ListPrice									MEDIUMINT UNSIGNED, \
		sf_product_title								VARCHAR(60), \
		sf_product_name									VARCHAR(80), \
		sf_opportunity_id								VARCHAR(33), \
		sf_opportunity_name								VARCHAR(16), \
		sf_PricebookEntryId								VARCHAR(16), \
		sf_ProductId									VARCHAR(16), \
		sf_ProductId2									VARCHAR(16), \
		sf_Quantity										SMALLINT UNSIGNED, \
		sf_titlenumber									VARCHAR(4), \
		sf_TotalPrice									FLOAT, \
		sf_Type											VARCHAR(33), \
		sf_AccountId									VARCHAR(16), \
		sf_op_name										VARCHAR(50), \
		sf_No_of_Videos									SMALLINT UNSIGNED, \
		sf_account_name									VARCHAR(50), \
		sf_order_number									VARCHAR(4) )" % (DBNAME,TABLENAME)

	query = query.replace('\t',' ').replace('     ',' ').replace('    ',' ').replace('   ',' ').replace('  ',' ') 
	#printf('%s\n',query)	
	cur.execute(query)
	con.commit()

def insert_into_attask_PROJECT_table(con,DBNAME,TABLENAME,Idatetime,out,start):

	cur_datetime = datetime.now()
	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################
	for j in range(0,len(out)):

		if ((j % 1000) == 999):
			printf('[create_mysql.py][insert_into_attask_PROJECT_table] %7d of %7d Elements ... %.3f sec\n',j+1,len(out),time.time()-start)

		try:
			opportunity_id = out["sf_OpportunityID"][j][0:15]
		except:
			opportunity_id = None

		try:
			sf_opportunity_id = out["sf_OpportunityID"][j][0:15]
		except:
			sf_opportunity_id = None

		try:
			sf_product_name = out['sf_Library'][j] + out['sf_TitleNumber'][j] + ' - ' + sf_product_title
		except:
			sf_product_name = None

		try:
			company_name = check_NONE(out["company"][j],"name").encode('ascii','unicode').replace("'","")
		except:
			company_name = None		

		try:
			template_id = check_NONE(out["template"][j],"ID").encode('ascii','unicode').replace("'","")
		except:
			template_id = None		

		try:
			template_name = check_NONE(out["template"][j],"name").encode('ascii','unicode').replace("'","")
		except:
			template_name = None		

		query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				Idatetime, \
				out['ID'][j], \
				replace_unicode(out['name'][j]), \
				replace_unicode(out['DE:Additional Client Notes'][j]), \
				ListElement0(out['DE:Backup Voice Talent'][j]), \
				out['DE:Benefits Eligible Employees'][j], \
				out['DE:Brand Management Designer/Art Director'][j], \
				out['DE:Brand Management Writer/Client Content Strategist'][j], \
				out['DE:Calculated Completion Date'][j], \
				out['DE:Current Velocity'][j], \
				replace_unicode(out['DE:Customer Goals/Objectives'][j]), \
				ListElement0(out['DE:Customer Hard Deadline'][j]), \
				out['DE:Date Logged'][j], \
				out['DE:Days In Queue'][j], \
				out['DE:Days In Queue Color'][j], \
				out['DE:Days with Voice Actor 1'][j], \
				out['DE:Days with Voice Actor 2'][j], \
				out['DE:Days with Voice Actor 3'][j], \
				out['DE:Days with Voice Actor 4'][j], \
				out['DE:Days with Voice Actor 5'][j], \
				out['DE:Days with Voice Actor 6'][j], \
				out['DE:Days with Voice Actor 7'][j], \
				out['DE:Days with Voice Actor Final Cut'][j], \
				out['DE:Days with Voice Actor Initial Development'][j], \
				ListElement0(out['DE:Driver for Hard Deadline'][j]), \
				ListElement0(out['DE:Driver for Soft Deadline'][j]), \
				ListElement0(out['DE:Effective Date'][j]), \
				out['DE:G1/G2'][j], \
				out['DE:Line Item ID'][j], \
				replace_unicode(out['DE:List demos shown and customer feedback'][j]), \
				out['DE:New Customer/Upsell/Renewal'][j], \
				replace_unicode(out['DE:New Titles Requested'][j]), \
				ListElement0(out['DE:OPERATIONAL | Product Line'][j]), \
				out['DE:Opportunity ID'][j], \
				ListElement0(out['DE:Order Number'][j]), \
				ListElement0(out['DE:Persona'][j]), \
				ListElement0(out['DE:Product Code'][j]), \
				ListElement0(out['DE:Product Group'][j]), \
				ListElement0(out['DE:Product Line'][j]), \
				replace_unicode(ListElement0(out['DE:Product Title'][j])), \
				out['DE:Product Video Category'][j], \
				out['DE:Product Video Subcategory'][j], \
				out['DE:Production Tool'][j], \
				out['DE:Program Start Date'][j], \
				out['DE:Project Total Velocity'][j], \
				out['DE:Renewal Date'][j], \
				replace_unicode(out['DE:Requested Kickoff Call Timing'][j]), \
				out['DE:Role Suffix'][j], \
				out['DE:Script Word Count'][j], \
				ListElement0(out['DE:Soft Deadline'][j]), \
				out['DE:Substatus'][j], \
				replace_unicode(out['DE:Target Deadline and Drivers'][j]), \
				ListElement0(out['DE:Target Language'][j]), \
				ListElement0(out['DE:Template Type'][j]), \
				out['DE:Template Version'][j], \
				out['DE:To Translation Services?'][j], \
				out['DE:Video Points'][j], \
				ListElement0(out['DE:Video Type'][j]), \
				ListElement0(out['DE:Voice In 1'][j]), \
				ListElement0(out['DE:Voice In 2'][j]), \
				ListElement0(out['DE:Voice In 3'][j]), \
				ListElement0(out['DE:Voice In 4'][j]), \
				ListElement0(out['DE:Voice In 5'][j]), \
				ListElement0(out['DE:Voice In 6'][j]), \
				ListElement0(out['DE:Voice In 7'][j]), \
				ListElement0(out['DE:Voice In Initial Development'][j]), \
				ListElement0(out['DE:Voice Out 1'][j]), \
				ListElement0(out['DE:Voice Out 2'][j]), \
				ListElement0(out['DE:Voice Out 3'][j]), \
				ListElement0(out['DE:Voice Out 4'][j]), \
				ListElement0(out['DE:Voice Out 5'][j]), \
				ListElement0(out['DE:Voice Out 6'][j]), \
				ListElement0(out['DE:Voice Out 7'][j]), \
				ListElement0(out["DE:Voice Out Final Cut"][j]), \
				ListElement0(out["DE:Voice Out Initial Development"][j]), \
				ListElement0(out["DE:Voice Talent"][j]), \
				ListElement0(out["DE:Voice in Final Cut"][j]), \
				AttaskDate_to_datetime_NONE(out["actualCompletionDate"][j]), \
				AttaskDate_to_datetime_NONE(out["actualStartDate"][j]), \
				out['actualDurationMinutes'][j], \
				out['actualLaborCost'][j], \
				out['actualWorkRequired'][j], \
				out['categoryID'][j], \
				company_name, \
				out['companyID'][j], \
				out['condition'][j], \
				out['conditionType'][j], \
				out['cpi'][j], \
				out['csi'][j], \
				replace_unicode(out['description'][j]), \
				out['durationMinutes'][j], \
				out['eac'][j], \
				out['enteredByID'][j], \
				AttaskDate_to_datetime_NONE(out["entryDate"][j]), \
				AttaskDate_to_datetime_NONE(out["estCompletionDate"][j]), \
				AttaskDate_to_datetime_NONE(out["estStartDate"][j]), \
				AttaskDate_to_datetime_NONE(out["lastUpdateDate"][j]), \
				out['groupID'][j], \
				check_NONE(out["owner"][j],"name"), \
				out['ownerID'][j], \
				out['percentComplete'][j], \
				out['plannedLaborCost'][j], \
				AttaskDate_to_datetime_NONE(out["plannedCompletionDate"][j]), \
				AttaskDate_to_datetime_NONE(out["plannedStartDate"][j]), \
				check_NONE(out["portfolio"][j],"ID"), \
				check_NONE(out["portfolio"][j],"name"), \
				out['priority'][j], \
				AttaskDate_to_datetime_NONE(out["projectedCompletionDate"][j]), \
				AttaskDate_to_datetime_NONE(out["projectedStartDate"][j]), \
				out['referenceNumber'][j], \
				out['risk'][j], \
				out['spi'][j], \
				out['sponsorID'][j], \
				out['status'][j], \
				replace_unicode(out["statusUpdate"][j]), \
				template_name, \
				template_id, \
				out['workRequired'][j], \
				out['LineItemId'][j], \
				out['sf_Branding'][j], \
				out['sf_Discount'][j], \
				out['sf_Library'][j], \
				out['sf_ListPrice'][j], \
				replace_unicode(out['sf_product_title'][j]), \
				sf_product_name, \
				opportunity_id, \
				replace_unicode(out['sf_op_name'][j]), \
				out['sf_PricebookEntryId'][j], \
				out['sf_ProductId'][j], \
				out['sf_ProductId2'][j], \
				out['sf_Quantity'][j], \
				out['sf_TitleNumber'][j], \
				out['sf_TotalPrice'][j], \
				out['sf_Type'][j], \
				out['AccountId'][j], \
				replace_unicode(out['sf_op_name'][j]), \
				out['No_of_Videos__c'][j], \
				replace_unicode(out['sf_account_name'][j]), \
				out['order_number'][j] )


		try:
			cur.execute(query)
			con.commit()
		except Exception as e:
			printf('\n%s\n', query.replace('\t','') )	
			printf("j = %5d . projectID = %s . Line %s: . %s\n",j,out['ID'][j],sys.exc_traceback.tb_lineno,e)
	
	
	###################
	#CREATE INDICES
	###################
	try:
		query= "CREATE INDEX IownerID ON  %s.%s (ownerID)" % (DBNAME,TABLENAME)	
		cur.execute(query)
		con.commit()
	except Exception as e:
		printf("[insert_into_attask_PROJECT_table] Line %s: %s\n",sys.exc_traceback.tb_lineno,e)

	try:
		query= "CREATE INDEX IprojectID ON  %s.%s (projectID)" % (DBNAME,TABLENAME)	
		cur.execute(query)
		con.commit()
	except Exception as e:
		printf("[insert_into_attask_PROJECT_table] Line %s: %s\n",sys.exc_traceback.tb_lineno,e)

def upload_deleted_into_attask_PROJECT_table(con,DBNAME,TABLENAME,Idatetime,out,start):

	cur_datetime = datetime.now()
	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################
	for j in range(0,len(out)):

		if ((j % 1000) == 999):
			printf('[create_mysql.py][insert_into_attask_PROJECT_table] %7d of %7d Elements ... %.3f sec\n',j+1,len(out),time.time()-start)

		try:
			company_name = check_NONE(out["company"][j],"name").encode('ascii','unicode').replace("'","")
		except:
			company_name = None		

		query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				Idatetime, \
				out['projectID'][j], \
				replace_unicode(out['project_name'][j]), \
				'', \
				'', \
				out['Benefits_Eligible_Employees'][j], \
				'', \
				'', \
				'', \
				out['Current_Velocity'][j], \
				'', \
				ListElement0(out['Customer_Hard_Deadline'][j]), \
				out['Date_Logged'][j], \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				ListElement0(out['Effective_Date'][j]), \
				out['G1_G2'][j], \
				out['LineItemId'][j], \
				'', \
				out['NewCustomer_Upsell_Renewal'][j], \
				'', \
				'', \
				out['OpportunityId'][j], \
				ListElement0(out['Order_Number'][j]), \
				ListElement0(out['Persona'][j]), \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				out['Program_Start_Date'][j], \
				out['Project_Total_Velocity'][j], \
				out['Renewal_Date'][j], \
				'', \
				'', \
				'', \
				ListElement0(out['Soft_Deadline'][j]), \
				'', \
				'', \
				'', \
				ListElement0(out['Template_Type'][j]), \
				out['Template_Version'][j], \
				'', \
				out['Video_Points'][j], \
				ListElement0(out['Video_Type'][j]), \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				ListElement0(out['Voice_Talent'][j]), \
				'', \
				out['actualCompletionDate'][j], \
				out['actualStartDate'][j], \
				out['actualDurationMinutes'][j], \
				out['actualLaborCost'][j], \
				out['actualWorkRequired'][j], \
				out['categoryID'][j], \
				company_name, \
				out['companyID'][j], \
				'', \
				'', \
				'', \
				'', \
				'', \
				out['durationMinutes'][j], \
				'', \
				'', \
				out['entryDate'][j], \
				out['estCompletionDate'][j], \
				out['estStartDate'][j], \
				out['lastUpdateDate'][j], \
				'', \
				out['owner_name'][j], \
				out['ownerID'][j], \
				out['percentComplete'][j], \
				out['plannedLaborCost'][j], \
				out['plannedCompletionDate'][j], \
				out['plannedStartDate'][j], \
				out['portfolioID'][j], \
				out['portfolio_name'][j], \
				out['priority'][j], \
				out['projectedCompletionDate'][j], \
				out['projectedStartDate'][j], \
				'', \
				out['risk'][j], \
				'', \
				out['sponsorID'][j], \
				out['status'][j], \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'', \
				'')

		query = query.replace('\t','')
		try:
			cur.execute(query)
			con.commit()
		except Exception as e:
			printf('\n%s\n', query.replace('\t','') )	
			printf("j = %5d . projectID = %s . Line %s: . %s\n",j,out['projectID'][j],sys.exc_traceback.tb_lineno,e)

def create_attask_TASK_table(con,DBNAME, TABLENAME):

	cur = con.cursor()
	query = 'USE %s' % DBNAME;
	cur.execute(query);

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		input_date_id					SMALLINT UNSIGNED, \
		taskID							VARCHAR(33), \
		projectID						VARCHAR(33), \
		pre_sort_index					INT, \
		taskNumber						INT, \
		numberOfChildren				SMALLINT, \
		status							VARCHAR(4), \
		work							FLOAT(7,2), \
		actualWork						FLOAT(8,2), \
		gm_work							FLOAT(8,2), \
		task_cost						FLOAT(12,4), \
		role_name						VARCHAR(25), \
		user_name						VARCHAR(100), \
		task_wage						FLOAT(8,2), \
		actualCompletionDate			DATETIME, \
		actualCost						FLOAT(8,2), \
		actualDuration					FLOAT(8,2), \
		actualDurationMinutes			INT, \
		actualExpenseCost				INT, \
		actualLaborCost					INT, \
		actualStartDate					DATETIME, \
		actualWorkRequired				INT, \
		approvalEstStartDate			DATETIME, \
		approvalPlannedStartDate		DATETIME, \
		approvalProjectedStartDate		DATETIME, \
		assignedToID					VARCHAR(33), \
		commitDate						DATETIME, \
		completionPendingDate			DATETIME, \
		costAmount						INT, \
		costType						VARCHAR(3), \
		cpi								SMALLINT, \
		csi								SMALLINT, \
		duration						INT, \
		durationMinutes					INT, \
		durationType					VARCHAR(2), \
		durationUnit					VARCHAR(2), \
		entryDate						DATETIME, \
		estCompletionDate				DATETIME, \
		estStartDate					DATETIME, \
		handoffDate						DATETIME, \
		lastUpdateDate					DATETIME, \
		name							VARCHAR(50), \
		objCode							VARCHAR(5), \
		originalDuration				INT, \
		percentComplete					FLOAT(6,2), \
		plannedCompletionDate			DATETIME, \
		plannedCost						INT, \
		plannedDuration					INT, \
		plannedDurationMinutes			INT, \
		plannedExpenseCost				INT, \
		plannedLaborCost				INT, \
		plannedStartDate				DATETIME, \
		priority						SMALLINT, \
		progressStatus					VARCHAR(3), \
		projectedCompletionDate			DATETIME, \
		projectedDurationMinutes		INT, \
		projectedStartDate				DATETIME, \
		remainingDurationMinutes		INT, \
		roleID							VARCHAR(33), \
		spi								FLOAT(8,2), \
		teamID							VARCHAR(33), \
		workRequired					INT)" % TABLENAME 
	
	#printf('%s\n',query.replace('\t','') )	
	cur.execute(query)
	con.commit()

def insert_into_attask_TASK_table(con,DBNAME,TABLENAME,Idatetime,out,start):

	cur_datetime = datetime.now()
	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################
	for j in range(0,len(out)):

		if ((j % 1000) == 999):
			printf('[create_mysql.py][insert_into_attask_TASK_table] %7d of %7d Elements ... %.3f sec\n',j+1,len(out),time.time()-start)

		try:
			cur_user_name = out["user_name"][j].replace("'","")
		except:
			cur_user_name = None

		query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s')" % \
				(DBNAME,TABLENAME, \
				Idatetime, \
				out["ID"][j], \
				out["projectID"][j], \
				out["index"][j], \
				out["taskNumber"][j], \
				out["numberOfChildren"][j], \
				out["status"][j], \
				out["work"][j], \
				out["actualWork"][j], \
				out["gm_work"][j], \
				out["task_cost"][j], \
				out["role_name"][j], \
				cur_user_name, \
				out["task_wage"][j], \
				AttaskDate_to_datetime_NONE(out["actualCompletionDate"][j]), \
				out["actualCost"][j], \
				out["actualDuration"][j], \
				out["actualDurationMinutes"][j], \
				out["actualExpenseCost"][j], \
				out["actualLaborCost"][j], \
				AttaskDate_to_datetime_NONE(out["actualStartDate"][j]), \
				out["actualWorkRequired"][j], \
				AttaskDate_to_datetime_NONE(out["approvalEstStartDate"][j]), \
				AttaskDate_to_datetime_NONE(out["approvalPlannedStartDate"][j]), \
				AttaskDate_to_datetime_NONE(out["approvalProjectedStartDate"][j]), \
				out["assignedToID"][j], \
				AttaskDate_to_datetime_NONE(out["commitDate"][j]), \
				AttaskDate_to_datetime_NONE(out["completionPendingDate"][j]), \
				out["costAmount"][j], \
				out["costType"][j], \
				out["cpi"][j], \
				out["csi"][j], \
				out["duration"][j], \
				out["durationMinutes"][j], \
				out["durationType"][j], \
				out["durationUnit"][j], \
				AttaskDate_to_datetime_NONE(out["entryDate"][j]), \
				AttaskDate_to_datetime_NONE(out["estCompletionDate"][j]), \
				AttaskDate_to_datetime_NONE(out["estStartDate"][j]), \
				AttaskDate_to_datetime_NONE(out["handoffDate"][j]), \
				AttaskDate_to_datetime_NONE(out["lastUpdateDate"][j]), \
				out["name"][j].encode('ascii','ignore').replace("'",""), \
				out["objCode"][j], \
				out["originalDuration"][j], \
				out["percentComplete"][j], \
				AttaskDate_to_datetime_NONE(out["plannedCompletionDate"][j]), \
				out["plannedCost"][j], \
				out["plannedDuration"][j], \
				out["plannedDurationMinutes"][j], \
				out["plannedExpenseCost"][j], \
				out["plannedLaborCost"][j], \
				AttaskDate_to_datetime_NONE(out["plannedStartDate"][j]), \
				out["priority"][j], \
				out["progressStatus"][j], \
				AttaskDate_to_datetime_NONE(out["projectedCompletionDate"][j]), \
				out["projectedDurationMinutes"][j], \
				AttaskDate_to_datetime_NONE(out["projectedStartDate"][j]), \
				out["remainingDurationMinutes"][j], \
				out["roleID"][j], \
				out["spi"][j], \
				out["teamID"][j], \
				out["workRequired"][j])

		#print(query)
		try:
			cur.execute(query)
			con.commit()
		except Exception as e:
			printf("j = %5d . projectID = %s . Line %s: . %s\n",j,out['projectID'][j],sys.exc_traceback.tb_lineno,e)
		

	###################
	# CREATE INDICES
	###################
#	try:
#		query = "CREATE INDEX IassignedToID ON  %s.%s (assignedToID)" % (DBNAME,TABLENAME)	
#		cur.execute(query)
#		con.commit()
#	except Exception as e:
#		printf("[insert_into_attask_TASK_table] Line %s: %s\n",sys.exc_traceback.tb_lineno,e)
#
#	try:
#		query = "CREATE INDEX IprojectID ON  %s.%s (projectID)" % (DBNAME,TABLENAME)	
#		cur.execute(query)
#		con.commit()
#	except Exception as e:
#		printf("[insert_into_attask_TASK_table] Line %s: %s\n",sys.exc_traceback.tb_lineno,e)
#
#	try:
#		query = "CREATE INDEX ItaskID ON  %s.%s (taskID)" % (DBNAME,TABLENAME)	
#		cur.execute(query)
#		con.commit()
#	except Exception as e:
#		printf("[insert_into_attask_TASK_table] Line %s: %s\n",sys.exc_traceback.tb_lineno,e)
#
#	try:
#		query = "CREATE INDEX IroleID ON  %s.%s (roleID)" % (DBNAME,TABLENAME)
#		cur.execute(query)
#		con.commit()
#	except Exception as e:
#		printf("[insert_into_attask_TASK_table] Line %s: %s\n",sys.exc_traceback.tb_lineno,e)

def upload_deleted_into_attask_TASK_table(con,DBNAME,TABLENAME,Idatetime,out,start):

	cur_datetime = datetime.now()
	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################
	for j in range(0,len(out)):

		if ((j % 1000) == 999):
			printf('[create_mysql.py][insert_into_attask_TASK_table] %7d of %7d Elements ... %.3f sec\n',j+1,len(out),time.time()-start)

		try:
			cur_user_name = out["user_name"][j].replace("'","")
		except:
			cur_user_name = None

		query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s')" % \
				(DBNAME,TABLENAME, \
				Idatetime, \
				out["taskID"][j], \
				out["projectID"][j], \
				'', \
				out["taskNumber"][j], \
				out["numberOfChildren"][j], \
				out["status"][j], \
				out["work"][j], \
				out["actualWork"][j], \
				out["gm_work"][j], \
				out["task_cost"][j], \
				out["role_name"][j], \
				cur_user_name, \
				out["task_wage"][j], \
				out["actualCompletionDate"][j], \
				out["actualCost"][j], \
				out["actualDuration"][j], \
				out["actualDurationMinutes"][j], \
				out["actualExpenseCost"][j], \
				out["actualLaborCost"][j], \
				out["actualStartDate"][j], \
				out["actualWorkRequired"][j], \
				out["approvalEstStartDate"][j], \
				out["approvalPlannedStartDate"][j], \
				out["approvalProjectedStartDate"][j], \
				out["assignedToID"][j], \
				out["commitDate"][j], \
				out["completionPendingDate"][j], \
				out["costAmount"][j], \
				out["costType"][j], \
				out["cpi"][j], \
				out["csi"][j], \
				out["duration"][j], \
				out["durationMinutes"][j], \
				out["durationType"][j], \
				out["durationUnit"][j], \
				out["entryDate"][j], \
				out["estCompletionDate"][j], \
				out["estStartDate"][j], \
				out["handoffDate"][j], \
				out["lastUpdateDate"][j], \
				out["name"][j], \
				out["objCode"][j], \
				out["originalDuration"][j], \
				out["percentComplete"][j], \
				out["plannedCompletionDate"][j], \
				out["plannedCost"][j], \
				out["plannedDuration"][j], \
				out["plannedDurationMinutes"][j], \
				out["plannedExpenseCost"][j], \
				out["plannedLaborCost"][j], \
				out["plannedStartDate"][j], \
				out["priority"][j], \
				out["progressStatus"][j], \
				out["projectedCompletionDate"][j], \
				out["projectedDurationMinutes"][j], \
				out["projectedStartDate"][j], \
				out["remainingDurationMinutes"][j], \
				out["roleID"][j], \
				out["spi"][j], \
				out["teamID"][j], \
				out["workRequired"][j])

		#print(query)
		try:
			cur.execute(query)
			con.commit()
		except Exception as e:
			printf("j = %5d . projectID = %s . Line %s: . %s\n",j,out['projectID'][j],sys.exc_traceback.tb_lineno,e)
		

def create_attask_USER_table(con,DBNAME, TABLENAME):

	cur = con.cursor()
	query = 'USE %s' % DBNAME;
	cur.execute(query);

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		userID							VARCHAR(33), \
		name							VARCHAR(100))" % TABLENAME 
	
	#printf('%s\n',query.replace('\t','') )	
	cur.execute(query)
	con.commit()

def insert_into_attask_USER_table(con,DBNAME,TABLENAME,out,start):

	cur_datetime = datetime.now()

	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################
	for j in range(0,len(out)):

		if ((j % 1000) == 999):
			printf('[create_mysql.py][insert_into_attask_TASK_table] %7d of %7d Elements ... %.3f sec\n',j+1,len(out),time.time()-start)

		query = "INSERT INTO %s.%s VALUES ('','%s','%s')" % \
				(DBNAME,TABLENAME, \
				out["userID2"][j], \
				out["user_name"][j].replace("'","") )

		#print(query)
		cur.execute(query)
		con.commit()

	###################
	# CREATE INDICES
	###################
	query = "CREATE INDEX IuserID ON  %s.%s (userID)" % (DBNAME,TABLENAME)	
	cur.execute(query)
	con.commit()

def create_attask_HOUR_table(con,DBNAME, TABLENAME):

	cur = con.cursor()
	query = 'USE %s' % DBNAME;
	cur.execute(query);

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		hourID							VARCHAR(33), \
		entryDate						DATE, \
		hours							FLOAT, \
		actualCost						FLOAT, \
		hourTypeID						VARCHAR(33), \
		hour_type_name					VARCHAR(100), \
		projectID						VARCHAR(33), \
		taskID							VARCHAR(33), \
		ownerID							VARCHAR(33), \
		user_name						VARCHAR(100), \
		roleID							VARCHAR(33), \
		role_name						VARCHAR(100), \
		opTaskID						VARCHAR(33), \
		objCode							VARCHAR(33), \
		status							VARCHAR(5))" % TABLENAME 
	
	#printf('%s\n',query.replace('\t','') )	
	cur.execute(query)
	con.commit()

def insert_into_attask_HOUR_table(con,DBNAME,TABLENAME,out,start):

	cur_datetime = datetime.now()

	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################
	for j in range(0,len(out)):

		if ((j % 1000) == 999):
			printf('[create_mysql.py][insert_into_attask_HOUR_table] %7d of %7d Elements ... %.3f sec\n',j+1,len(out),time.time()-start)

		query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				out["hourID"][j], \
				out["entryDate"][j], \
				out["hours"][j], \
				out["actualCost"][j], \
				out["hourTypeID"][j], \
				out["hour_type_name"][j], \
				out["projectID"][j], \
				out["taskID"][j], \
				out["ownerID"][j], \
				out["user_name"][j].replace("'",""), \
				out["roleID"][j], \
				out["role_name"][j], \
				out["opTaskID"][j], \
				out["ownerID"][j], \
				out["status"][j] )

		#print(query)
		cur.execute(query)
		con.commit()

	###################
	# CREATE INDICES
	###################
	query = "CREATE INDEX IhourID ON  %s.%s (hourID)" % (DBNAME,TABLENAME)	
	cur.execute(query)
	con.commit()
	query = "CREATE INDEX IownerID ON  %s.%s (ownerID)" % (DBNAME,TABLENAME)	
	cur.execute(query)
	con.commit()
	query = "CREATE INDEX IprojectID ON  %s.%s (projectID)" % (DBNAME,TABLENAME)	
	cur.execute(query)
	con.commit()
	query = "CREATE INDEX ItaskID ON  %s.%s (taskID)" % (DBNAME,TABLENAME)	
	cur.execute(query)
	con.commit()
	query = "CREATE INDEX IroleID ON  %s.%s (roleID)" % (DBNAME,TABLENAME)
	cur.execute(query)
	con.commit()
	query = "CREATE INDEX IhourTypeID ON  %s.%s (hourTypeID)" % (DBNAME,TABLENAME)	
	cur.execute(query)
	con.commit()

	query = "ALTER TABLE %s.%s ADD role_name_summary VARCHAR(25) AFTER role_name" % (DBNAME,TABLENAME)	
	cur.execute(query)
	con.commit()
	query = "UPDATE %s.%s SET role_name_summary = 'Writer' WHERE UPPER(role_name) LIKE '%%WRITER%%'" % (DBNAME,TABLENAME)	
	cur.execute(query)
	con.commit()
	query = "UPDATE %s.%s SET role_name_summary = 'QC' WHERE UPPER(role_name) LIKE '%%QC%%'" % (DBNAME,TABLENAME)	
	cur.execute(query)
	con.commit()
	query = "UPDATE %s.%s SET role_name_summary = 'Designer' WHERE UPPER(role_name) LIKE '%%DESIGN%%'" % (DBNAME,TABLENAME)	
	cur.execute(query)
	con.commit()
	#query = "UPDATE %s.%s SET role_name_summary = 'Brand Mgmt' WHERE UPPER(role_name) LIKE '%%BRAND%%'" % (DBNAME,TABLENAME)	
	#cur.execute(query)
	#con.commit()

def create_attask_HOUR_TYPE_table(con,DBNAME, TABLENAME):

	cur = con.cursor()
	query = 'USE %s' % DBNAME;
	cur.execute(query);

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		hour_typeID						VARCHAR(33), \
		hour_type_name					VARCHAR(100), \
		objID							VARCHAR(33), \
		objCode							VARCHAR(33))" % TABLENAME 
	
	#printf('%s\n',query.replace('\t','') )	
	cur.execute(query)
	con.commit()

def insert_into_attask_HOUR_TYPE_table(con,DBNAME,TABLENAME,out,start):

	cur_datetime = datetime.now()

	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################
	for j in range(0,len(out)):

		if ((j % 1000) == 999):
			printf('[create_mysql.py][insert_into_attask_HOURTYPE_table] %7d of %7d Elements ... %.3f sec\n',j+1,len(out),time.time()-start)

		query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				out["hour_typeID"][j], \
				out["hour_type_name"][j], \
				out["objID"][j], \
				out["objCode"][j] )

		#print(query)
		cur.execute(query)
		con.commit()

	###################
	# CREATE INDICES
	###################
	query = "CREATE INDEX IhourTypeID ON  %s.%s (hour_typeID)" % (DBNAME,TABLENAME)	
	cur.execute(query)
	con.commit()

def create_attask_TIMEOFF_table(con,DBNAME, TABLENAME):

	cur = con.cursor()
	query = 'USE %s' % DBNAME;
	cur.execute(query);

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		reserved_timesID				VARCHAR(33), \
		day_of_week						VARCHAR(10), \
		userID							VARCHAR(33), \
		user_name						VARCHAR(100), \
		roleID							VARCHAR(33), \
		role_name						VARCHAR(100), \
		curDate							DATETIME, \
		startDate						DATETIME, \
		endDate							DATETIME, \
		pto_hours						FLOAT, \
		taskID							VARCHAR(33), \
		objCode							VARCHAR(33) )" % TABLENAME
	
	#printf('%s\n',query.replace('\t','') )	
	cur.execute(query)
	con.commit()

def insert_into_attask_TIMEOFF_table(con,DBNAME,TABLENAME,out,start):

	cur_datetime = datetime.now()

	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################
	for j in range(0,len(out)):

		if ((j % 1000) == 999):
			printf('[create_mysql.py][insert_into_attask_HOUR_table] %7d of %7d Elements ... %.3f sec\n',j+1,len(out),time.time()-start)

		query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s')" % \
				(DBNAME,TABLENAME, \
				out["reserved_timesID"][j], \
				out["day_of_week"][j], \
				out["userID"][j], \
				out["user_name"][j].replace("'",""), \
				out["roleID"][j], \
				out["role_name"][j], \
				out["curDate"][j], \
				out["startDate"][j], \
				out["endDate"][j], \
				out["pto_hours"][j], \
				out["taskID"][j], \
				out["objCode"][j] )

		#print(query)
		cur.execute(query)
		con.commit()

	###################
	# CREATE INDICES
	###################
	query = "CREATE INDEX IuserID ON  %s.%s (userID)" % (DBNAME,TABLENAME)	
	cur.execute(query)
	con.commit()
	query = "CREATE INDEX IroleID ON  %s.%s (roleID)" % (DBNAME,TABLENAME)	
	cur.execute(query)
	con.commit()

def create_SF_mysql_table(con,DBNAME, TABLENAME):

	cur = con.cursor()
	query = 'USE %s' % DBNAME;
	cur.execute(query);

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		owner_id	  	  	 	VARCHAR(21), \
		owner_name	  	  	 	VARCHAR(100), \
		op_id	  		  	 	VARCHAR(21), \
		created_date  	 		DATETIME, \
		created_by_id  	 		VARCHAR(21), \
		account_id	 	     	VARCHAR(21), \
		account_health 	     	VARCHAR(10), \
		health_category	     	VARCHAR(50), \
		health_category_reason  VARCHAR(500), \
		account_status 	     	VARCHAR(15), \
		name	 	       		VARCHAR(100), \
		contact_id       		VARCHAR(21), \
		contact_name       		VARCHAR(100), \
		contact_title      		VARCHAR(100), \
		final_amount			VARCHAR(21), \
		op_type	 	     	   	VARCHAR(30), \
		lead_source 	   	   	VARCHAR(30), \
		final_close_date		DATETIME, \
		last_activity_date 	 	DATETIME, \
		stage_name	  	 		VARCHAR(30), \
		nvideo		  	 		INT, \
		product_line  	 		VARCHAR(30), \
		won			  	 		INT, \
		industry_sf	  	 		VARCHAR(100), \
		industry_text_sf		VARCHAR(100), \
		industry	  	 		VARCHAR(100), \
		Nindustry	  	 		TINYINT UNSIGNED, \
		lifespan_cust  	 		FLOAT(6,2), \
		revenue_cust  	 		BIGINT, \
		arr			  	 		INT, \
		arr_risk	  	 		INT, \
		bee			  	 		BIGINT, \
		total_employee 	 		INT, \
		total_employee_bin 		VARCHAR(14), \
		old_value	  	 		VARCHAR(30), \
		new_value	  	 		VARCHAR(30), \
		prob		  	 		FLOAT(5,1), \
		amount					VARCHAR(21), \
		close_date				DATETIME, \
		sic						VARCHAR(20), \
		sic_sector				VARCHAR(100), \
		sic_extra				VARCHAR(100), \
		sic_all					VARCHAR(100), \
		naics					VARCHAR(10), \
		naics_sector			VARCHAR(100), \
		naics_extra				VARCHAR(100), \
		naics_all				VARCHAR(100))" % TABLENAME

	cur.execute(query)
	con.commit()

def insert_into_SF_mysql_DB(con,DBNAME,TABLENAME,out,start):

	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################
	for j in range(0,len(out)):

		if ((j % 1000) == 999):
			printf('%5d of %5d Elements ... %.3f sec\n',j+1,len(out),time.time()-start)

		query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				out[j].owner_id, \
				out[j].owner_name, \
				out[j].op_id, \
				out[j].created_date, \
				out[j].created_by_id, \
				out[j].account_id, \
				out[j].account_health, \
				out[j].health_category, \
				out[j].health_category_reason, \
				out[j].account_status, \
				out[j].name, \
				out[j].contact_id, \
				out[j].contact_name, \
				out[j].contact_title, \
				out[j].final_amount, \
				out[j].op_type, \
				out[j].lead_source, \
				out[j].final_close_date, \
				out[j].last_activity_date, \
				out[j].stage_name, \
				out[j].nvideos, \
				out[j].product_line, \
				out[j].won, \
				out[j].industry_sf, \
				out[j].industry_text_sf, \
				out[j].industry, \
				out[j].Nindustry, \
				out[j].lifespan, \
				out[j].annual_revenue, \
				out[j].ARR, \
				out[j].ARR_risk, \
				out[j].BEE, \
				out[j].total_employees, \
				out[j].total_employee_bin, \
				out[j].old_value, \
				out[j].new_value, \
				out[j].prob, \
				out[j].amount, \
				out[j].close_date, \
				out[j].sic, \
				out[j].sic_sector, \
				out[j].sic_extra, \
				out[j].sic_all, \
				out[j].naics, \
				out[j].naics_sector, \
				out[j].naics_extra, \
				out[j].naics_all)

		#print(query)
		cur.execute(query)
		con.commit() # necessary to finish statement

	####################################
	# Update ... convert 'None' to NULL
	####################################
	query = "UPDATE %s.%s SET last_activity_date=NULL WHERE last_activity_date = '0000-00-00'" % (DBNAME,TABLENAME)
	cur.execute(query)
	con.commit() # necessary to finish statement
	query = "UPDATE %s.%s SET close_date=NULL WHERE close_date = '0000-00-00'" % (DBNAME,TABLENAME)
	cur.execute(query)
	con.commit() # necessary to finish statement
	query = "UPDATE %s.%s SET contact_name=NULL WHERE contact_name = 'None'" % (DBNAME,TABLENAME)
	cur.execute(query)
	con.commit() # necessary to finish statement
	query = "UPDATE %s.%s SET contact_name=NULL WHERE contact_title = 'None'" % (DBNAME,TABLENAME)
	cur.execute(query)
	con.commit() # necessary to finish statement
	
	delete_records=False
	if (delete_records):
		query = "DELETE from %s.%s WHERE new_value = '2) Buying Signals' OR old_value = '2) Buying Signals'" % (DBNAME,TABLENAME)
		cur.execute(query)
		con.commit() # necessary to finish statement

		query = "DELETE from %s.%s WHERE new_value LIKE '%%WALKING%%THE%%' OR old_value LIKE '%%WALKING%%THE%%'" % (DBNAME,TABLENAME)
		cur.execute(query)
		con.commit() # necessary to finish statement

		query = "DELETE from %s.%s WHERE new_value = '2) Proposal' OR old_value = '2) Proposal'" % (DBNAME,TABLENAME)
		cur.execute(query)
		con.commit() # necessary to finish statement

		query = "DELETE from %s.%s WHERE new_value = '3) Proposal' OR old_value = '3) Proposal'" % (DBNAME,TABLENAME)
		cur.execute(query)
		con.commit() # necessary to finish statement

		query = "DELETE from %s.%s WHERE new_value LIKE '%%NEGOTIATION%%' OR old_value LIKE '%%NEGOTIATION%%'" % (DBNAME,TABLENAME)
		cur.execute(query)
		con.commit() # necessary to finish statement

		query = "DELETE from %s.%s WHERE new_value = '6) Committed' OR old_value = '6) Committed'" % (DBNAME,TABLENAME)
		cur.execute(query)
		con.commit() # necessary to finish statement

		query = "DELETE from %s.%s WHERE new_value = '7) Closed Won' OR old_value = '7) Closed Won'" % (DBNAME,TABLENAME)
		cur.execute(query)
		con.commit() # necessary to finish statement

		query = "DELETE from %s.%s WHERE new_value = '8) Closed Lost' OR old_value = '8) Closed Lost'" % (DBNAME,TABLENAME)
		cur.execute(query)
		con.commit() # necessary to finish statement


#	query = "CREATE INDEX Itest ON %s.%s (<column_name>)" % (DBNAME,TABLENAME)
#	cur.execute(query)
#	con.commit() # necessary to finish statement
#
#	#################################################
#	## ALTER / UPDATE EXAMPLE
#	#################################################
#	query = "ALTER TABLE %s.%s ADD <column_name> FLOAT(7,4)" % (DBNAME,TABLENAME) 
#	cur.execute(query)
#	con.commit() # necessary to finish statement
#
#	query = "UPDATE %s.%s a SET a.<column_name>=0" \
#			% (DBNAME,TABLENAME1) 
#	cur.execute(query)
#	con.commit() # necessary to finish statement
#
#	query = "UPDATE %s.%s a, %s.%s b SET a.base_pay = b.base_pay WHERE b.<column_name1>=a.g<column_name1> and b.<column_name2>=a.<column_name2>" \
#			% (DBNAME,TABLENAME1,DBNAME,TABLENAME)
#	cur.execute(query)
#	con.commit() # necessary to finish statement

def alterMYSQLtable(cur,DBNAME,TABLENAME):

	try:
		query = 'ALTER TABLE %s.%s ADD nemail TINYINT UNSIGNED AFTER prob' % (DBNAME,TABLENAME)
		cur.execute(query);
	except mdb.Error, e:
		printf("FAILED QUERY :%s:\n",query)
		printf(":%s:\n",e)

	try:
		query = 'ALTER TABLE %s.%s ADD ncall TINYINT UNSIGNED AFTER prob' % (DBNAME,TABLENAME)
		cur.execute(query);
	except mdb.Error, e:
		printf("FAILED QUERY :%s:\n",query)
		printf(":%s:\n",e)

	try:
		query = 'ALTER TABLE %s.%s ADD nactivity TINYINT UNSIGNED AFTER prob' % (DBNAME,TABLENAME)
		cur.execute(query);
	except mdb.Error, e:
		printf("FAILED QUERY :%s:\n",query)
		printf(":%s:\n",e)

	try:
		query = 'ALTER TABLE %s.%s ADD mtg3_wait_time INT AFTER prob' % (DBNAME,TABLENAME)
		cur.execute(query);
	except mdb.Error, e:
		printf("FAILED QUERY :%s:\n",query)
		printf(":%s:\n",e)

	try:
		query = 'ALTER TABLE %s.%s ADD mtg2_wait_time INT AFTER prob' % (DBNAME,TABLENAME)
		cur.execute(query);
	except mdb.Error, e:
		printf("FAILED QUERY :%s:\n",query)
		printf(":%s:\n",e)

	try:
		query = 'ALTER TABLE %s.%s ADD mtg1_wait_time INT AFTER prob' % (DBNAME,TABLENAME)
		cur.execute(query);
	except mdb.Error, e:
		printf("FAILED QUERY :%s:\n",query)
		printf(":%s:\n",e)

	try:
		query = 'ALTER TABLE %s.%s ADD mtg_num TINYINT UNSIGNED AFTER prob' % (DBNAME,TABLENAME)
		cur.execute(query);
	except mdb.Error, e:
		printf("FAILED QUERY :%s:\n",query)
		printf(":%s:\n",e)

	try:
		query = 'ALTER TABLE %s.%s ADD upsell_created_date DATETIME AFTER prob' % (DBNAME,TABLENAME)
		cur.execute(query);
	except mdb.Error, e:
		printf("FAILED QUERY :%s:\n",query)
		printf(":%s:\n",e)

	try:
		query = 'ALTER TABLE %s.%s ADD click_num TINYINT UNSIGNED AFTER prob' % (DBNAME,TABLENAME)
		cur.execute(query);
	except mdb.Error, e:
		printf("FAILED QUERY :%s:\n",query)
		printf(":%s:\n",e)

	try:
		query = 'ALTER TABLE %s.%s ADD view_num TINYINT UNSIGNED AFTER prob' % (DBNAME,TABLENAME)
		cur.execute(query);
	except mdb.Error, e:
		printf("FAILED QUERY :%s:\n",query)
		printf(":%s:\n",e)

	try:
		query = 'ALTER TABLE %s.%s ADD reply_num TINYINT UNSIGNED AFTER prob' % (DBNAME,TABLENAME)
		cur.execute(query);
	except mdb.Error, e:
		printf("FAILED QUERY :%s:\n",query)
		printf(":%s:\n",e)

	try:
		query = 'ALTER TABLE %s.%s ADD re_num TINYINT UNSIGNED AFTER prob' % (DBNAME,TABLENAME)
		cur.execute(query);
	except mdb.Error, e:
		printf("FAILED QUERY :%s:\n",query)
		printf(":%s:\n",e)

	try:
		query = 'ALTER TABLE %s.%s ADD wonlossopen TINYINT UNSIGNED AFTER prob' % (DBNAME,TABLENAME)
		cur.execute(query);
	except mdb.Error, e:
		printf("FAILED QUERY :%s:\n",query)
		printf(":%s:\n",e)

	try:
		query = 'ALTER TABLE %s.%s ADD contact_num TINYINT UNSIGNED AFTER prob' % (DBNAME,TABLENAME)
		cur.execute(query);
	except mdb.Error, e:
		printf("FAILED QUERY :%s:\n",query)
		printf(":%s:\n",e)

def create_marketo_ACTIVITY_table(con,DBNAME,ACTIVITY_TABLE,LEAD_TABLE):

	cur = con.cursor()
	query = 'USE %s' % DBNAME;
	cur.execute(query);

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		datetime	  	  	 	DATETIME, \
		lead_id			   	 	INT, \
		activity_id		   	 	BIGINT, \
		type	 		  	 	VARCHAR(100), \
		detail		  	 		VARCHAR(300), \
		old_value	  	 		VARCHAR(50), \
		new_value	  	 		VARCHAR(50), \
		error		  	 		TINYINT)" % ACTIVITY_TABLE

	cur.execute(query)
	con.commit()

	query = "CREATE TABLE IF NOT EXISTS %s ( \
		lead_id INT unsigned NOT NULL PRIMARY KEY, \
		email		  	  	 		VARCHAR(100), \
		FirstName	  	  	 		VARCHAR(15), \
		LastName	  	  	 		VARCHAR(30), \
		Title		  	  	 		VARCHAR(60), \
		sfdc_lead_id	  	 		VARCHAR(16), \
		sfdc_account_id	  	 		VARCHAR(16), \
		sfdc_account_name  	 		VARCHAR(100), \
		LeadSource		  	 		VARCHAR(50), \
		LeadStatus		  	 		VARCHAR(30), \
		TGEs			  	 		INT, \
		Initial_Sale_Upsell_Opptys	SMALLINT, \
		Upsell_Opptys				SMALLINT)" % LEAD_TABLE

	cur.execute(query)
	con.commit()

def insert_into_marketo_LEAD_table(con,DBNAME,LEAD_TABLE,lead,idnum,email):

	cur = con.cursor()

	#########################
	# Perform Error checking
	#########################

	#########################
	## INSERT DATA INTO TABLE
	#########################
	query = "INSERT INTO %s.%s VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
									   '%s','%s','%s')" % \
			(DBNAME,LEAD_TABLE, \
			lead.id, \
			lead.email, \
			check_value(lead.attributes,'FirstName'), \
			check_value(lead.attributes,'LastName').replace("'",""), \
			check_value(lead.attributes,'Title'), \
			check_value(lead.attributes,'mkto_si__Sales_Insight__c'), \
			check_value(lead.attributes,'Account_ID__c'), \
			check_value(lead.attributes,'Account_Name__c'), \
			check_value(lead.attributes,'LeadSource'), \
			check_value(lead.attributes,'LeadStatus'), \
			check_value(lead.attributes,'Total_Global_Employees__c'), \
			check_value(lead.attributes,'Initial_Sale_Upsell_Opptys__c'), \
			check_value(lead.attributes,'Upsell_Opptys__c') )
#			lead.attributes['FirstName'], \
#			lead.attributes['LastName'], \
#			lead.attributes['Title'], \
#			lead.attributes['mkto_si__Sales_Insight__c'].split('?id=')[1].split('"')[0], \
#			lead.attributes['Account_ID__c'], \
#			lead.attributes['Account_Name__c'], \
#			lead.attributes['LeadSource'], \
#			lead.attributes['LeadStatus'], \
#			lead.attributes['Total_Global_Employees__c'], \
#			lead.attributes['Initial_Sale_Upsell_Opptys__c'], \
#			lead.attributes['Upsell_Opptys__c'] )

	#print(query)
	cur.execute(query)
	con.commit() # necessary to finish statement

def insert_into_marketo_ACTIVITY_table(con,DBNAME,ACTIVITY_TABLE,lead,activity,error):

	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################
	for j in range(0,len(activity)):

		if ((j % 100) == 99):
			printf('[create_mysql.py][insert_into_marketo_ACTIVITY_table] %7d of %7d Elements\n',j+1,len(activity))

		#printf("%d . %s\n",j,out[j].detail)

		try:
			detail = activity[j].detail.encode('ascii','ignore').replace("'","")
		except:
			detail = "" 

		old_value = "" 
		new_value = "" 
		if (activity[j].type == 'Change Data Value'):
			try:
				old_value = activity[j].attributes['Old Value'].encode('ascii','ignore').replace("'","")
			except:
				old_value = "" 
			try:
				new_value = activity[j].attributes['New Value'].encode('ascii','ignore').replace("'","")
			except:
				new_value = "" 
		#printf(":%s:",old_value)
		#printf("%s:\n",new_value)
 
		query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s','%s','%s','%s','%s')" % \
				(DBNAME,ACTIVITY_TABLE, \
				activity[j].datetime, \
				lead.id, \
				activity[j].id, \
				activity[j].type, \
				detail, \
				old_value, \
				new_value, \
				error )

		#print(query)
		cur.execute(query)
		con.commit() # necessary to finish statement

def create_g2_USER_LOOKUP_table(con,DBNAME, TABLENAME):

	cur = con.cursor()

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s.%s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		user_id						INT, \
		fullname					VARCHAR(100), \
		firstname					VARCHAR(50), \
		lastname					VARCHAR(50), \
		email						VARCHAR(100), \
		role						VARCHAR(100))" % (DBNAME,TABLENAME) 
	
	#printf('%s\n',query.replace('\t','') )	
	cur.execute(query)
	con.commit()

def insert_into_g2_USER_LOOKUP_table(con,DBNAME,TABLENAME,out,start):

	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################
	for j in range(0,len(out)):

		printf('[create_mysql.py][insert_into_g2_USER_LOOKUP_table] %7d of %7d Elements ... %.3f sec\n',j+1,len(out),time.time()-start)

		try:
			firstname = out['firstname'][j].replace("'","")
		except:
			firstname = None 

		try:
			lastname = out['lastname'][j].replace("'","")
		except:
			lastname = None 

		try:
			fullname = out['firstname'][j].replace("'","") + ' ' + out['lastname'][j].replace("'","")
		except:
			fullname = None 

		query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s','%s','%s')" % \
			(DBNAME,TABLENAME, \
			out["user_id"][j], \
			fullname, \
			firstname, \
			lastname, \
			out["email"][j], \
			out["role"][j] )

		#print(query)
		cur.execute(query)
		con.commit()


