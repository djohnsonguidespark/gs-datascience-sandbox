#!/bin/sh

######################
# Create RDR history
######################
# 1) Create rdr history
python update_rdr_history_model.py

######################
# Opportunity History
######################
# 1) Create op_history
# File Output: sdata_op_history_RSF_RSF.csv
python update_op_history_model.py

# 2) Create op cross_validation files 
# File Output: sdata_op_history_RSF_<datetime>.csv
python update_op_cross_validation.py

# 3) Create sdata_op_allday_RNN.csv 
# File Output: sdata_op_alldayRNN.csv
python update_op_alldays_RNN.py

######################
# Combine rdr/op 
######################

# 1) Combine rdr/op
# File Output: sdata_all_history_RSF.csv & sdata_all_history_COX.csv
python combine_rdr_op_history.py

# 2) Create rdr & op cross_validation files 
# File Output: sdata_all_history_RSF_<datetime>.csv 
python update_act_cross_validation.py 1 &
python update_act_cross_validation.py 2 &
python update_act_cross_validation.py 3 &
python update_act_cross_validation.py 4 &
python update_act_cross_validation.py 5 &
python update_act_cross_validation.py 6 & 
python update_act_cross_validation.py 7  

# Create sdata_act_allday_RNN_<filenum>.csv
python update_act_alldays_RNN.py 1 & 
python update_act_alldays_RNN.py 3 &
python update_act_alldays_RNN.py 4 &
python update_act_alldays_RNN.py 2

# Make sure all update_act_alldays_RNN.py are complete before start 
sleep 900 
 
# Combine sdata_act_allday_RNN_<filenum>.csv
# File Output: sdata_act_allday_RNN.csv
python combine_act_alldays_RNN.py

