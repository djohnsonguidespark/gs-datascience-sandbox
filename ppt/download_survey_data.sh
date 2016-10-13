#!/bin/sh
echo "[upload_benchmark_files.sh]: DOWNLOAD SURVEY RESULTS"
/usr/bin/scp -P 8861 -i /home/djohnson/.ssh/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r analytics@analytics-mysql-n1.guidespark.net:/home/analytics/analytics_sandbox/viz/survey_tables.sql . 
retvalue=$?

mysql -uroot sandbox_prod < survey_tables.sql

