#! /usr/bin/env python
from benchmark_common_libs import *
from datetime import datetime, timedelta
import MySQLdb as mdb
import time

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

def create_BENCHMARK_mysql_table(con,DBNAME, TABLENAME):

	cur = con.cursor()
	query = 'USE %s' % DBNAME;
	cur.execute(query);

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		category	  	  	 	VARCHAR(10), \
		match_field	  	  	 	VARCHAR(100), \
		program_day	  	  	 	SMALLINT, \
		reach			  	  	FLOAT(12,7) )" % TABLENAME 

	cur.execute(query)
	con.commit()

def insert_into_BENCHMARK_mysql_DB(con,DBNAME,TABLENAME,out):

	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################
		
	### Add zero data for plotting purposes
	query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s')" % \
			(DBNAME,TABLENAME, \
			out['category'][0], \
			out['match_field'][0].replace("'",""), \
			0, \
			0.0 )
	cur.execute(query)
	con.commit() # necessary to finish statement

	for j in range(0,len(out)):

		if ((j % 1000) == 999):
			printf('%5d of %5d Elements ... %.3f sec\n',j+1,len(out),time.time()-start)

		query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				out['category'][j], \
				out['match_field'][j].replace("'",""), \
				j+1, \
				float(out['reach'][j]) )

		#print(query)
		cur.execute(query)
		con.commit() # necessary to finish statement

def create_BENCHMARK_SOLUTION_mysql_table(con,DBNAME, TABLENAME):

	cur = con.cursor()
	query = 'USE %s' % DBNAME;
	cur.execute(query);

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		solution	  	  	 	VARCHAR(30), \
		category	  	  	 	VARCHAR(10), \
		match_field	  	  	 	VARCHAR(100), \
		program_day	  	  	 	SMALLINT, \
		reach			  	  	FLOAT(12,7) )" % TABLENAME 

	cur.execute(query)
	con.commit()

def insert_into_BENCHMARK_SOLUTION_mysql_DB(con,DBNAME,TABLENAME,out):

	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################
		
	### Add zero data for plotting purposes
	query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s','%s')" % \
			(DBNAME,TABLENAME, \
			out['solution'][0], \
			out['category'][0], \
			out['match_field'][0].replace("'",""), \
			0, \
			0.0 )
	cur.execute(query)
	con.commit() # necessary to finish statement

	for j in range(0,len(out)):

		if ((j % 1000) == 999):
			printf('%5d of %5d Elements ... %.3f sec\n',j+1,len(out),time.time()-start)

		query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				out['solution'][j], \
				out['category'][j], \
				out['match_field'][j].replace("'",""), \
				j+1, \
				float(out['reach'][j]) )

		#print(query)
		cur.execute(query)
		con.commit() # necessary to finish statement

def create_BENCHMARK_ROW_mysql_table(con,DBNAME, TABLENAME):

	cur = con.cursor()
	query = 'USE %s' % DBNAME;
	cur.execute(query);

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		account_id	  	  			 		INT, \
		account		  	  	 				VARCHAR(100), \
		sfdc		  	  				 	VARCHAR(19), \
		industry			  	  		 	VARCHAR(60), \
		in_benchmark						TINYINT UNSIGNED, \
		account_reach_ranking				MEDIUMINT UNSIGNED, \
		account_reach_ranking_percentile	FLOAT(12,7), \
		library_size	  	 	 			SMALLINT UNSIGNED, \
		library_size_bin			  	  	VARCHAR(12), \
		BEE							  	  	INT UNSIGNED, \
		BEE_bin						  	  	VARCHAR(20), \
		program_day	  				  	 	SMALLINT, \
		cur_date	  	 			 	 	DATETIME, \
		program_launch_date				 	DATETIME, \
		calculated_program_launch_date		DATETIME, \
		account_reach				 	 	FLOAT(12,7), \
		bm_trigger					  		FLOAT(12,7), \
		bm_guidespark				  		FLOAT(12,7), \
		bm_industry			 		 		FLOAT(12,7), \
		bm_library_size_reach				FLOAT(12,7), \
		bm_company_reach					FLOAT(12,7) )" % TABLENAME 

	cur.execute(query)
	con.commit()

def insert_into_BENCHMARK_ROW_mysql_DB(con,DBNAME,TABLENAME,out):

	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################

	start = time.time()		
	for j in range(0,len(out)):

		if ((j % 1000) == 999):
			printf('%5d of %5d Elements ... %.3f sec\n',j+1,len(out),time.time()-start)

		try:
			cur_industry = out['industry'][j].replace("'","")
		except:
			cur_industry = "" 
		try:
			cur_sfdc = out['sfdc'][j]
		except:
			cur_sfdc = "" 

		## Eventually change the 2nd 'bm_trigger' back to 'bm_guidespark'
		query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				out['account_id'][j], \
				out['account_name'][j].replace("'",""), \
				cur_sfdc, \
				cur_industry, \
				out['in_benchmark'][j], \
				out['account_reach_ranking'][j], \
				out['account_reach_ranking_%'][j], \
				out['Nvideo'][j], \
				out['Nvideo_bin'][j], \
				out['BEEs'][j], \
				out['BEE_bin'][j], \
				out['days_since_OE'][j], \
				out['date'][j], \
				out['program_launch_date'][j], \
				out['calculated_program_launch_date'][j], \
				out['reach'][j], \
				out['bm_trigger'][j], \
				out['bm_trigger'][j], \
				#out['bm_guidespark'][j], \
				out['bm_industry'][j], \
				out['bm_library_size'][j], \
				out['bm_bee'][j] )

		#print(query)
		cur.execute(query)
		con.commit() # necessary to finish statement


def create_BENCHMARK_SOLUTION_ROW_mysql_table(con,DBNAME, TABLENAME):

	cur = con.cursor()
	query = 'USE %s' % DBNAME;
	cur.execute(query);

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		account_id	  	  				 		INT, \
		account		  	  	 					VARCHAR(100), \
		solution	  	  	 					VARCHAR(100), \
		sfdc		  	  				 		VARCHAR(19), \
		industry			  	  			 	VARCHAR(60), \
		in_benchmark							TINYINT UNSIGNED, \
		account_reach_ranking					MEDIUMINT UNSIGNED, \
		account_reach_ranking_percentile		FLOAT(12,7), \
		account_reach_ranking_BEE				MEDIUMINT UNSIGNED, \
		account_reach_ranking_BEE_percentile	FLOAT(12,7), \
		library_size	  	 		 			SMALLINT UNSIGNED, \
		library_size_bin				  	  	VARCHAR(12), \
		BEE								  	  	INT UNSIGNED, \
		BEE_bin						  		  	VARCHAR(20), \
		program_day		  				  	 	SMALLINT, \
		cur_date	  		 			 	 	DATETIME, \
		program_launch_date					 	DATETIME, \
		calculated_program_launch_date			DATETIME, \
		account_reach					 	 	FLOAT(12,7), \
		bm_trigger						  		FLOAT(12,7), \
		bm_guidespark					  		FLOAT(12,7), \
		bm_industry			 			 		FLOAT(12,7), \
		bm_library_size_reach					FLOAT(12,7), \
		bm_company_reach						FLOAT(12,7) )" % TABLENAME 

	cur.execute(query)
	con.commit()

def insert_into_BENCHMARK_SOLUTION_ROW_mysql_DB(con,DBNAME,TABLENAME,out):

	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################

	start = time.time()		
	for j in range(0,len(out)):

		if ((j % 1000) == 999):
			printf('%5d of %5d Elements ... %.3f sec\n',j+1,len(out),time.time()-start)

		try:
			cur_industry = out['industry'][j].replace("'","")
		except:
			cur_industry = "" 
		try:
			cur_sfdc = out['sfdc'][j]
		except:
			cur_sfdc = "" 

		## Eventually change the 2nd 'bm_trigger' back to 'bm_guidespark'
		query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				out['account_id'][j], \
				out['account_name'][j].replace("'",""), \
				out['solution'][j], \
				cur_sfdc, \
				cur_industry, \
				out['in_benchmark'][j], \
				out['account_reach_ranking'][j], \
				out['account_reach_ranking_%'][j], \
				out['account_reach_ranking_BEE'][j], \
				out['account_reach_ranking_BEE_%'][j], \
				out['Nvideo'][j], \
				out['Nvideo_bin'][j], \
				out['BEEs'][j], \
				out['BEE_bin'][j], \
				out['days_since_OE'][j], \
				out['date'][j], \
				out['program_launch_date'][j], \
				out['calculated_program_launch_date'][j], \
				out['reach'][j], \
				out['bm_trigger'][j], \
				out['bm_trigger'][j], \
				#out['bm_guidespark'][j], \
				out['bm_industry'][j], \
				out['bm_library_size'][j], \
				out['bm_bee'][j] )

		#print(query)
		cur.execute(query)
		con.commit() # necessary to finish statement

def create_ACCOUNT_REACH_mysql_table(con,DBNAME, TABLENAME):

	cur = con.cursor()
	query = 'USE %s' % DBNAME;
	cur.execute(query);

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		cur_year	  	  	 			VARCHAR(15), \
		account_name  	  	 			VARCHAR(100), \
		account_id	  	  	 			INT UNSIGNED, \
		in_benchmark	   				TINYINT UNSIGNED, \
		Nparent							INT UNSIGNED, \
		Nuser							INT UNSIGNED, \
		industry_name	  	  			VARCHAR(60), \
		AVG_target_audience	  			FLOAT(15,2), \
		BEEs			  	  			INT UNSIGNED, \
		BEE_bin			  	  			VARCHAR(20), \
		library_size	  	  			SMALLINT UNSIGNED, \
		library_size_bin  	  			VARCHAR(20), \
		USER_reach_raw					FLOAT(15,9), \
		USER_reach						FLOAT(15,9), \
		gs_percentile					FLOAT(15,9), \
		industry_percentile				FLOAT(15,9), \
		bee_percentile					FLOAT(15,9), \
		Nvideo_percentile				FLOAT(15,9), \
		gs_reach						FLOAT(15,9), \
		industry_reach					FLOAT(15,9), \
		bee_reach						FLOAT(15,9), \
		Nvideo_reach					FLOAT(15,9) )" % TABLENAME

	cur.execute(query)
	con.commit()

def insert_into_ACCOUNT_REACH_mysql_DB(con,DBNAME,TABLENAME,out):

	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################

	start = time.time()		
	for j in range(0,len(out)):

		if ((j % 1000) == 999):
			printf('%5d of %5d Elements ... %.3f sec\n',j+1,len(out),time.time()-start)

		try:
			cur_industry = out['industry_name'][j].replace("'","")
		except:
			cur_industry = "" 
		try:
			cur_account_name = out['account_name'][j].replace("'","")
		except:
			cur_account_name = "" 

		query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				out['cur_year'][j], \
				cur_account_name, \
				out['account_id'][j], \
				out['in_benchmark'][j], \
				out['Nparent'][j], \
				out['Nuser'][j], \
				cur_industry, \
				out['AVG_target_audience'][j], \
				out['BEEs'][j], \
				out['BEE_bin'][j], \
				out['Nvideo'][j], \
				out['Nvideo_bin'][j], \
				out['USER_reach'][j], \
				out['USER_reach_corrected'][j], \
				out['gs_percentile'][j], \
				out['industry_percentile'][j], \
				out['bee_percentile'][j], \
				out['Nvideo_percentile'][j], \
				out['gs_reach'][j], \
				out['industry_reach'][j], \
				out['bee_reach'][j], \
				out['Nvideo_reach'][j] )

		#print(query)
		cur.execute(query)
		con.commit() # necessary to finish statement

def create_ACCOUNT_VIDEO_REACH_mysql_table(con,DBNAME, TABLENAME):

	cur = con.cursor()
	query = 'USE %s' % DBNAME;
	cur.execute(query);

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		cur_year	  	  	 					VARCHAR(15), \
		account_name  	  			 			VARCHAR(100), \
		account_id	  	  	 					INT UNSIGNED, \
		in_benchmark	   	 					TINYINT UNSIGNED, \
		Nparent									INT UNSIGNED, \
		Nuser									INT UNSIGNED, \
		industry_name	  	  					VARCHAR(60), \
		BEEs			  	  					INT UNSIGNED, \
		BEE_bin			  	  					VARCHAR(20), \
		library_size	  	  					SMALLINT UNSIGNED, \
		library_size_bin  	  					VARCHAR(20), \
		video_id								INT UNSIGNED, \
		video_title								VARCHAR(100), \
		video_category							VARCHAR(100), \
		USER_reach								FLOAT(15,9), \
		gs_video_percentile						FLOAT(15,9), \
		industry_video_percentile				FLOAT(15,9), \
		bee_video_percentile					FLOAT(15,9), \
		Nvideo_video_percentile					FLOAT(15,9), \
		gs_video_reach							FLOAT(15,9), \
		industry_video_reach					FLOAT(15,9), \
		bee_video_reach							FLOAT(15,9), \
		Nvideo_video_reach						FLOAT(15,9) )" % TABLENAME

	cur.execute(query)
	con.commit()

def insert_into_ACCOUNT_VIDEO_REACH_mysql_DB(con,DBNAME,TABLENAME,out):

	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################

	start = time.time()		
	for j in range(0,len(out)):

		if ((j % 1000) == 999):
			printf('%5d of %5d Elements ... %.3f sec\n',j+1,len(out),time.time()-start)

		try:
			cur_industry = out['industry_name'][j].replace("'","")
		except:
			cur_industry = "" 
		try:
			cur_account_name = out['account_name'][j].replace("'","")
		except:
			cur_account_name = "" 
		try:
			cur_video_title = out['video_title'][j].replace("'","")
		except:
			cur_video_title = "" 

		query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				out['cur_year'][j], \
				cur_account_name, \
				out['account_id'][j], \
				out['in_benchmark'][j], \
				out['Nparent'][j], \
				out['Nuser'][j], \
				cur_industry, \
				out['BEEs'][j], \
				out['BEE_bin'][j], \
				out['Nvideo'][j], \
				out['Nvideo_bin'][j], \
				out['video_id'][j], \
				cur_video_title, \
				out['video_category'][j], \
				out['USER_reach_corrected'][j], \
				out['gs_video_percentile'][j], \
				out['industry_video_percentile'][j], \
				out['bee_video_percentile'][j], \
				out['Nvideo_video_percentile'][j], \
				out['gs_video_reach'][j], \
				out['industry_video_reach'][j], \
				out['bee_video_reach'][j], \
				out['Nvideo_video_reach'][j] )

		cur.execute(query)
		con.commit()

def create_ACCOUNT_EFF_mysql_table(con,DBNAME, TABLENAME):

	cur = con.cursor()
	query = 'USE %s' % DBNAME;
	cur.execute(query);

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		cur_year	  	  	 			VARCHAR(15), \
		account_name  	  	 			VARCHAR(100), \
		account_id	  	  	 			INT UNSIGNED, \
		Nparent							INT UNSIGNED, \
		industry_name	  	  			VARCHAR(60), \
		BEEs			  	  			INT UNSIGNED, \
		BEE_bin			  	  			VARCHAR(20), \
		library_size	  	  			SMALLINT UNSIGNED, \
		library_size_bin  	  			VARCHAR(20), \
		MEDIAN_effectiveness_absolute	FLOAT(15,9), \
		AVG_effectiveness_absolute		FLOAT(15,9), \
		gs_percentile					FLOAT(15,9), \
		industry_percentile				FLOAT(15,9), \
		bee_percentile					FLOAT(15,9), \
		Nvideo_percentile				FLOAT(15,9), \
		gs_effectivenss_absolute		FLOAT(15,9), \
		industry_effectivenss_absolute	FLOAT(15,9), \
		bee_effectivenss_absolute		FLOAT(15,9), \
		Nvideo_effectivenss_absolute	FLOAT(15,9) )" % TABLENAME

	cur.execute(query)
	con.commit()

def insert_into_ACCOUNT_EFF_mysql_DB(con,DBNAME,TABLENAME,out):

	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################

	start = time.time()		
	for j in range(0,len(out)):

		if ((j % 1000) == 999):
			printf('%5d of %5d Elements ... %.3f sec\n',j+1,len(out),time.time()-start)

		try:
			cur_industry = out['industry_name'][j].replace("'","")
		except:
			cur_industry = "" 
		try:
			cur_account_name = out['account_name'][j].replace("'","")
		except:
			cur_account_name = "" 

		query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				out['cur_year'][j], \
				cur_account_name, \
				out['account_id'][j], \
				out['Nparent'][j], \
				cur_industry, \
				out['BEEs'][j], \
				out['BEE_bin'][j], \
				out['Nvideo'][j], \
				out['Nvideo_bin'][j], \
				out['MEDIAN_effectiveness_absolute'][j], \
				out['MEDIAN_effectiveness_absolute'][j], \
				#out['AVG_effectiveness_absolute'][j], \
				out['gs_percentile'][j], \
				out['industry_percentile'][j], \
				out['bee_percentile'][j], \
				out['Nvideo_percentile'][j], \
				out['gs_effectiveness'][j], \
				out['industry_effectiveness'][j], \
				out['bee_effectiveness'][j], \
				out['Nvideo_effectiveness'][j] )

		#print(query)
		cur.execute(query)
		con.commit() # necessary to finish statement

def create_ACCOUNT_VIDEO_EFF_mysql_table(con,DBNAME, TABLENAME):

	cur = con.cursor()
	query = 'USE %s' % DBNAME;
	cur.execute(query);

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		cur_year	  	  	 					VARCHAR(15), \
		account_name  	  			 			VARCHAR(100), \
		account_id	  	  	 					INT UNSIGNED, \
		Nparent									INT UNSIGNED, \
		industry_name	  	  					VARCHAR(60), \
		BEEs			  	  					INT UNSIGNED, \
		BEE_bin			  	  					VARCHAR(20), \
		library_size	  	  					SMALLINT UNSIGNED, \
		library_size_bin  	  					VARCHAR(20), \
		video_id								INT UNSIGNED, \
		video_title								VARCHAR(100), \
		video_category							VARCHAR(100), \
		MEDIAN_effectiveness_absolute			FLOAT(15,9), \
		AVG_effectiveness_absolute				FLOAT(15,9), \
		gs_video_percentile						FLOAT(15,9), \
		industry_video_percentile				FLOAT(15,9), \
		bee_video_percentile					FLOAT(15,9), \
		Nvideo_video_percentile					FLOAT(15,9), \
		gs_video_effectiveness_absolute			FLOAT(15,9), \
		industry_video_effectiveness_absolute	FLOAT(15,9), \
		bee_video_effectiveness_absolute		FLOAT(15,9), \
		Nvideo_video_effectiveness_absolute		FLOAT(15,9) )" % TABLENAME

	cur.execute(query)
	con.commit()

def insert_into_ACCOUNT_VIDEO_EFF_mysql_DB(con,DBNAME,TABLENAME,out):

	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################

	start = time.time()		
	for j in range(0,len(out)):

		if ((j % 1000) == 999):
			printf('%5d of %5d Elements ... %.3f sec\n',j+1,len(out),time.time()-start)

		try:
			cur_industry = out['industry_name'][j].replace("'","")
		except:
			cur_industry = "" 
		try:
			cur_account_name = out['account_name'][j].replace("'","")
		except:
			cur_account_name = "" 
		try:
			cur_video_title = out['video_title'][j].replace("'","")
		except:
			cur_video_title = "" 

		query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
											  '%s','%s')" % \
				(DBNAME,TABLENAME, \
				out['cur_year'][j], \
				cur_account_name, \
				out['account_id'][j], \
				out['Nparent'][j], \
				cur_industry, \
				out['BEEs'][j], \
				out['BEE_bin'][j], \
				out['Nvideo'][j], \
				out['Nvideo_bin'][j], \
				out['video_id'][j], \
				cur_video_title, \
				out['video_category'][j], \
				out['MEDIAN_effectiveness_absolute'][j], \
				out['AVG_effectiveness_absolute'][j], \
				out['gs_video_percentile'][j], \
				out['industry_video_percentile'][j], \
				out['bee_video_percentile'][j], \
				out['Nvideo_video_percentile'][j], \
				out['gs_video_effectiveness'][j], \
				out['industry_video_effectiveness'][j], \
				out['bee_video_effectiveness'][j], \
				out['Nvideo_video_effectiveness'][j] )

		#print(query)
		cur.execute(query)
		con.commit() # necessary to finish statement

def create_g2_PROGRAM_LAUNCH_table(con,DBNAME, TABLENAME):

	cur = con.cursor()

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s.%s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		g2_account_id					INT, \
		g2_account_name					VARCHAR(100), \
		program_launch_year				YEAR, \
		program_launch_date				DATE, \
		updated							TINYINT UNSIGNED, \
		new_launch_date					DATE)" % (DBNAME,TABLENAME) 
	
	#printf('%s\n',query.replace('\t','') )	
	cur.execute(query)
	con.commit()

def insert_into_g2_PROGRAM_LAUNCH_table(con,DBNAME,TABLENAME,out,start):

	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################
	for j in range(0,len(out)):

		#if ((j % 1000) == 999):
		printf('[create_mysql.py][insert_into_g2_PROGRAM_LAUNCH_table] %7d of %7d Elements ... %.3f sec\n',j+1,len(out),time.time()-start)

		if (pd.notnull(out["new_launch_date"][j])):
			out_new_launch = out["new_launch_date"][j]

			query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				out["id"][j], \
				out["name"][j].replace("'",""), \
				out["program_launch_year"][j], \
				out["program_launch_date"][j], \
				out["checked"][j], \
				out_new_launch)

		else:
			out_new_launch = 'NULL'

			query = "INSERT INTO %s.%s VALUES ('','%s','%s','%s','%s','%s',%s)" % \
				(DBNAME,TABLENAME, \
				out["id"][j], \
				out["name"][j].replace("'",""), \
				out["program_launch_year"][j], \
				out["program_launch_date"][j], \
				out["checked"][j], \
				out_new_launch)

		#print(query)
		cur.execute(query)
		con.commit()

def create_g2_GLOBAL_BENCHMARK_table(con,DBNAME, TABLENAME):

	cur = con.cursor()

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s.%s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		benchmark						VARCHAR(30), \
		benchmark_bin					VARCHAR(100), \
		program_day						SMALLINT UNSIGNED, \
		value							FLOAT)" % (DBNAME,TABLENAME)
	
	#printf('%s\n',query.replace('\t','') )	
	cur.execute(query)
	con.commit()

def insert_into_g2_GLOBAL_BENCHMARK_table(con,DBNAME,TABLENAME,bm_guidespark,bm_industry_df,bm_company_size_df,bm_library_size_df,start):

	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################
	for j in range(0,len(bm_guidespark)):

		if ((j % 1000) == 999):
			printf('[create_mysql.py][insert_into_g2_BENCHMARK_table] %7d of %7d Elements ... %.3f sec\n',j+1,len(bm_guidespark),time.time()-start)

		query = "INSERT INTO %s.%s VALUES ('','bm_guidespark','all','%s','%s')" % \
				(DBNAME,TABLENAME, \
				j, \
				bm_guidespark[j])

		#print(query)
		cur.execute(query)
		con.commit()

	for j in range(0,len(bm_industry_df)):

		if ((j % 1000) == 999):
			printf('[create_mysql.py][insert_into_g2_BENCHMARK_table] %7d of %7d Elements ... %.3f sec\n',j+1,len(bm_industry_df),time.time()-start)

		query = "INSERT INTO %s.%s VALUES ('','bm_industry','%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				bm_industry_df.ix[j]['industry'], \
				bm_industry_df.ix[j]['days_since_OE'], \
				bm_industry_df.ix[j]['median_reach'])

		#print(query)
		cur.execute(query)
		con.commit()

	for j in range(0,len(bm_company_size_df)):

		if ((j % 1000) == 999):
			printf('[create_mysql.py][insert_into_g2_BENCHMARK_table] %7d of %7d Elements ... %.3f sec\n',j+1,len(bm_company_size_df),time.time()-start)

		query = "INSERT INTO %s.%s VALUES ('','bm_company_size','%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				bm_company_size_df.ix[j]['BEE_bin'], \
				bm_company_size_df.ix[j]['days_since_OE'], \
				bm_company_size_df.ix[j]['median_reach'])

		#print(query)
		cur.execute(query)
		con.commit()

	for j in range(0,len(bm_library_size_df)):

		if ((j % 1000) == 999):
			printf('[create_mysql.py][insert_into_g2_BENCHMARK_table] %7d of %7d Elements ... %.3f sec\n',j+1,len(bm_library_size_df),time.time()-start)

		query = "INSERT INTO %s.%s VALUES ('','bm_library_size','%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				bm_library_size_df.ix[j]['Nvideo_bin'], \
				bm_library_size_df.ix[j]['days_since_OE'], \
				bm_library_size_df.ix[j]['median_reach'])

		#print(query)
		cur.execute(query)
		con.commit()

def create_g2_GLOBAL_BENCHMARK_SOLUTION_table(con,DBNAME, TABLENAME):

	cur = con.cursor()

	################
	## CREATE TABLE
	################

	query = "CREATE TABLE IF NOT EXISTS %s.%s ( \
		id INT(32) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, \
		benchmark						VARCHAR(30), \
		benchmark_bin					VARCHAR(100), \
		program_day						SMALLINT UNSIGNED, \
		value							FLOAT)" % (DBNAME,TABLENAME)
	
	#printf('%s\n',query.replace('\t','') )	
	cur.execute(query)
	con.commit()

def insert_into_g2_GLOBAL_BENCHMARK_SOLUTION_table(con,DBNAME,TABLENAME,bm_guidespark,bm_industry_df,bm_company_size_df,bm_library_size_df,start):

	cur = con.cursor()

	#########################
	## INSERT DATA INTO TABLE
	#########################
	for j in range(0,len(bm_guidespark)):

		if ((j % 1000) == 999):
			printf('[create_mysql.py][insert_into_g2_BENCHMARK_table] %7d of %7d Elements ... %.3f sec\n',j+1,len(bm_guidespark),time.time()-start)

		query = "INSERT INTO %s.%s VALUES ('','bm_guidespark','all','%s','%s')" % \
				(DBNAME,TABLENAME, \
				j, \
				bm_guidespark[j])

		#print(query)
		cur.execute(query)
		con.commit()

	for j in range(0,len(bm_industry_df)):

		if ((j % 1000) == 999):
			printf('[create_mysql.py][insert_into_g2_BENCHMARK_table] %7d of %7d Elements ... %.3f sec\n',j+1,len(bm_industry_df),time.time()-start)

		query = "INSERT INTO %s.%s VALUES ('','%s','bm_industry','%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				bm_industry_df.ix[j]['solution'], \
				bm_industry_df.ix[j]['industry'], \
				bm_industry_df.ix[j]['days_since_OE'], \
				bm_industry_df.ix[j]['median_reach'])

		#print(query)
		cur.execute(query)
		con.commit()

	for j in range(0,len(bm_company_size_df)):

		if ((j % 1000) == 999):
			printf('[create_mysql.py][insert_into_g2_BENCHMARK_table] %7d of %7d Elements ... %.3f sec\n',j+1,len(bm_company_size_df),time.time()-start)

		query = "INSERT INTO %s.%s VALUES ('','%s','bm_company_size','%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				bm_company_size_df.ix[j]['solution'], \
				bm_company_size_df.ix[j]['BEE_bin'], \
				bm_company_size_df.ix[j]['days_since_OE'], \
				bm_company_size_df.ix[j]['median_reach'])

		#print(query)
		cur.execute(query)
		con.commit()

	for j in range(0,len(bm_library_size_df)):

		if ((j % 1000) == 999):
			printf('[create_mysql.py][insert_into_g2_BENCHMARK_table] %7d of %7d Elements ... %.3f sec\n',j+1,len(bm_library_size_df),time.time()-start)

		query = "INSERT INTO %s.%s VALUES ('','%s','bm_library_size','%s','%s','%s')" % \
				(DBNAME,TABLENAME, \
				bm_library_size_df.ix[j]['solution'], \
				bm_library_size_df.ix[j]['Nvideo_bin'], \
				bm_library_size_df.ix[j]['days_since_OE'], \
				bm_library_size_df.ix[j]['median_reach'])

		#print(query)
		cur.execute(query)
		con.commit()

