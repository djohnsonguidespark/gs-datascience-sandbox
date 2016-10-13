#!/bin/sh

DATETIME="$(date +'%Y-%m-%d')"

echo "[update_edits_table.sh]: CURRENT DATE ... $DATETIME"

rm -rf edits_tables_TEST.sql

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
TMP_EDIT_PERCENTAGE_USER_PREVIEW_day \
TMP_EDIT_PERCENTAGE_USER_QC_day \
TMP_EDIT_PERCENTAGE_USER_day \
> edits_tables_TEST.sql 


