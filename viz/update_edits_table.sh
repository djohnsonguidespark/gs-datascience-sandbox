#!/bin/sh

DATETIME="$(date +'%Y-%m-%d')"

echo "[update_edits_table.sh]: CURRENT DATE ... $DATETIME"
echo "[update_edits_table.sh]: RUN QC_sandbox_edits_20150724_v4.sql"
mysql -A -uroot << eof
DROP DATABASE IF EXISTS edits_prod;
CREATE DATABASE IF NOT EXISTS edits_prod;
eof

cd /home/analytics/analytics_sandbox/user_table/
python create_user_table.py > user_table_log
cd /home/analytics/analytics_sandbox/viz/

mysql -A -uroot << eof
use guidespark2_prod; source QC_sandbox_edits_20150724_v4.sql;
eof

rm -f edits_tables.sql

echo "[update_edits_table.sh]: EXPORT CRITICAL TABLES"
mysqldump -uroot edits_prod \
TMP_EDIT_SUMMARY \
TMP_ALL_VERSIONS \
TMP_PUBLISH_DATE \
TMP_CUSTOMER_TOUCH \
TMP_CUSTOMER_TOUCH_week \
TMP_EDIT_PERCENTAGE_PREVIEW_week \
TMP_EDIT_PERCENTAGE_QC_week \
TMP_EDIT_PERCENTAGE_QC_PREVIEW_week \
TMP_EDIT_PERCENTAGE_week \
TMP_EDIT_PERCENTAGE_USER_day \
TMP_EDIT_PERCENTAGE_USER_PREVIEW_day \
TMP_EDIT_PERCENTAGE_USER_QC_day \
> edits_tables.sql 

#echo "[update_edits_table.sh]: WRITE TO SERVER"
#/usr/bin/scp -i /home/analytics/.ssh/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r edits_tables.sql deploy@viz.guidespark.net:/home/deploy/

#echo "[update_edits_table.sh]: UPDATE EDITS TABLES"
#/usr/bin/ssh -i /home/analytics/.ssh/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no deploy@viz.guidespark.net './edits_mysql.sh'


