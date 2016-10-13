#!/bin/sh

DATETIME="$(date +'%Y-%m-%d')"

echo "[update_stats_table.sh]: DELETE EXISTING TABLES / RECREATE TABLES"
mysql -uroot << eof
CREATE DATABASE IF NOT EXISTS stats_prod;
DROP TABLE IF EXISTS stats_prod.TMP_YoY_STATS_account;
DROP TABLE IF EXISTS stats_prod.TMP_YoY_STATS_OE_account;
DROP TABLE IF EXISTS stats_prod.TMP_HOURLY_STATS;
DROP TABLE IF EXISTS stats_prod.TMP_HOURLY_STATS_account;
DROP TABLE IF EXISTS stats_prod.TMP_HOURLY_STATS_SUMMARY_account;
DROP TABLE IF EXISTS stats_prod.TMP_DAILY_STATS;
DROP TABLE IF EXISTS stats_prod.TMP_DAILY_STATS_account;
DROP TABLE IF EXISTS stats_prod.TMP_DAILY_STATS_SUMMARY_account;
DROP TABLE IF EXISTS stats_prod.TMP_WEEKLY_STATS;
DROP TABLE IF EXISTS stats_prod.TMP_WEEKLY_STATS_account;
DROP TABLE IF EXISTS stats_prod.TMP_WEEKLY_STATS_SUMMARY_account;
DROP TABLE IF EXISTS stats_prod.TMP_MONTHLY_STATS;
DROP TABLE IF EXISTS stats_prod.TMP_MONTHLY_STATS_account;
DROP TABLE IF EXISTS stats_prod.TMP_MONTHLY_STATS_SUMMARY_account;
DROP TABLE IF EXISTS stats_prod.TMP_MONTHLY_STATS_YonY;
source Usage_stats_20151007.sql
eof

sudo mysqldump -uroot stats_prod > stats_tables.sql

#echo "[update_stats_table.sh]: WRITE TO SERVER"
#/usr/bin/scp -i /home/analytics/.ssh/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r stats_tables.sql deploy@viz.guidespark.net:/home/deploy/
#
#echo "[update_stats_table.sh]: UPDATE STATS TABLES"
#/usr/bin/ssh -i /home/analytics/.ssh/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no deploy@viz.guidespark.net './stats_mysql.sh'
 
