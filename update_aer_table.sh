#!/bin/sh

DATETIME_OLD="20140527"
DATETIME="$(date +'%Y-%m-%d')"

#echo "[update_aer_table.sh]: UPDATE G2 DATABASE ... guidespark2-db-backup-${DATETIME}_0930.sql"
#echo "[update_aer_table.sh]:  ******** WARNING: WATCH for names with TEMP or DEMO ******** "
#python g2_update.py ${DATETIME}

echo "[update_aer_table.sh]: RUN AER_Benchmarks_20150406_v10.sql"
mysql -A -uroot << eof
use guidespark2_prod; source AER_Benchmarks_20150406_v10_BAK.sql;
eof

#python ../AER/aer_daily.py

echo "[update_aer_table.sh]: EXPORT CRITICAL TABLES"
mysqldump -uroot guidespark2_prod \
AER_PROGRAM_BENCHMARK_GS \
AER_REACH_SUMMARY_account \
AER_REACH_SUMMARY_VIDEO_account \
AER_REACH_GS_account \
AER_REACH_GS_VIDEO_account \
AER_REACH_INDUSTRY_account \
AER_REACH_INDUSTRY_VIDEO_account \
AER_EFFECTIVENESS_SUMMARY_account \
AER_EFFECTIVENESS_SUMMARY_VIDEO_account \
AER_EFFECTIVENESS_GS_account \
AER_EFFECTIVENESS_GS_VIDEO_account \
AER_EFFECTIVENESS_INDUSTRY_account \
AER_EFFECTIVENESS_INDUSTRY_VIDEO_account \
> aer_tables.sql

#echo "[update_aer_table.sh]: WRITE TO SERVER"
#/usr/bin/scp -i /home/djohnson/.ssh/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r aer_tables.sql deploy@viz.guidespark.net:/home/deploy/viz/




