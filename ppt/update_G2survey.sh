#!/bin/sh

DATETIME="$(date +'%Y-%m-%d')"

echo "[update_aer_table.sh]: RUN G2_survey.sql"
mysql -A -uroot << eof
use guidespark2_prod; source G2_survey.sql;
eof

