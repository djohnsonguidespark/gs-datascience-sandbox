#!/bin/sh

python update_churn_model_data.py
python product_flow.py
python churn_timeseries.py
Rscript cox_churn.R
python update_sfdc_churn.py

