#!/bin/sh

DATETIME="$(date +'%Y-%m-%d')"

echo "[update_aer_table.sh]: RUN G2_Benchmarks_PROD_20140820_G2query_v48.sql"
mysql -A -uroot << eof
use guidespark2_prod; source G2_Benchmarks_PROD_20140820_G2query_v48.sql; source D2.sql;
eof

#echo "[update_aer_table.sh]: EXPORT CRITICAL TABLES"
#mysqldump -uroot sandbox_prod > ./backup_db/sandbox_prod_backup.sql 

