#!/bin/sh

# 2) Create rdr & op cross_validation files 
# File Output: sdata_all_history_RSF_<datetime>.csv 
python update_act_cross_validation.py 1 &
python update_act_cross_validation.py 2 &
python update_act_cross_validation.py 3 &
python update_act_cross_validation.py 4 &
python update_act_cross_validation.py 5 &
python update_act_cross_validation.py 6 & 
python update_act_cross_validation.py 7  


