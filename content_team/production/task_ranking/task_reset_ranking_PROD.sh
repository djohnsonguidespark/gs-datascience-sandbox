#!/bin/sh

exit_val=1
n=0
while [ $exit_val -ne 0 ]
do
	python task_reset_ranking_PROD.py
	exit_val=$?  # return from python script
	n=`expr $n + 1`
	if [ $n -gt 4 ]
	then
		exit_val=0
	fi
	echo "[update_g2_table.sh]: exit_val=$exit_val"
done
python email_attachment_reset.py

