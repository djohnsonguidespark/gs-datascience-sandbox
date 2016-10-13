#!/bin/sh

echo "[upload_benchmark_files.sh]: UPLOAD FILES TO SERVER"
/usr/bin/scp -P 8861 -i /home/djohnson/.ssh/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r percentile.py analytics@analytics-mysql-n2.guidespark.net:/home/analytics/
retvalue=$?
/usr/bin/scp -P 8861 -i /home/djohnson/.ssh/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r aer_daily.py analytics@analytics-mysql-n2.guidespark.net:/home/analytics/
retvalue=$?
/usr/bin/scp -P 8861 -i /home/djohnson/.ssh/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r aer_libs.py analytics@analytics-mysql-n2.guidespark.net:/home/analytics/
retvalue=$?
/usr/bin/scp -P 8861 -i /home/djohnson/.ssh/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r benchmark_common_libs.py analytics@analytics-mysql-n2.guidespark.net:/home/analytics/
retvalue=$?
/usr/bin/scp -P 8861 -i /home/djohnson/.ssh/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r benchmark_create_mysql.py analytics@analytics-mysql-n2.guidespark.net:/home/analytics/
retvalue=$?
/usr/bin/scp -P 8861 -i /home/djohnson/.ssh/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r Production_Benchmarks_20150406_v13.sql analytics@analytics-mysql-n2.guidespark.net:/home/analytics/
retvalue=$?
/usr/bin/scp -P 8861 -i /home/djohnson/.ssh/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r update_benchmark_table.sh analytics@analytics-mysql-n2.guidespark.net:/home/analytics/
retvalue=$?


