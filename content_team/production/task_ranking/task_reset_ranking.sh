#/bin/sh

exit_val=1
while [[ "$exit_val" -ne "0" ]]
do
	python task_reset_ranking_PROD.py
	exit_val=$?  # return from python script
	echo "[update_g2_table.sh]: exit_val=$exit_val"
done
