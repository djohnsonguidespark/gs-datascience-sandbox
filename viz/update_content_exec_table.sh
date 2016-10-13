#!/bin/sh

DATETIME="$(date +'%Y-%m-%d')"

echo "[update_edits_table.sh]: RUN Attask_Hours_20150818.sql"
mysql -A -uroot << eof
source Attask_Hours_20150818.sql;
eof

rm -f content_exec_tables.sql

echo "[update_edits_table.sh]: EXPORT CRITICAL TABLES"
mysqldump -uroot content_prod \
TMP_ATTASK_HOUR_SPLIT \
TMP_ATTASK_WEEKLY_HOURS_PER_PROJECT \
TMP_ATTASK_WEEKLY_HOURS_PER_PROJECT_SUMMARY \
TMP_ATTASK_WEEKLY_HOURS \
TMP_ATTASK_WEEKLY_HOURS_NONCUSTOMER \
TMP_ATTASK_WEEKLY_HOURS_ROLE_SUMMARY \
TMP_ATTASK_WEEKLY_HOURS_SUMMARY \
TMP_ATTASK_WEEKLY_HOURS_PER_USER_PLANNED \
TMP_ATTASK_WEEKLY_HOURS_PER_USER_ACTUAL \
TMP_ATTASK_WEEKLY_HOURS_PER_USER \
TMP_HEADCOUNT \
TMP_HEADCOUNT_YonY \
> content_exec_tables.sql 

echo "[update_content_exec_table.sh]: WRITE TO SERVER"
/usr/bin/scp -i /home/analytics/.ssh/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r content_exec_tables.sql deploy@viz.guidespark.net:/home/deploy/

echo "[update_content_exec_table.sh]: UPDATE ATTASK TABLES"
/usr/bin/ssh -i /home/analytics/.ssh/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no deploy@viz.guidespark.net './content_exec_mysql.sh'


