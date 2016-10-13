#!/bin/sh

DATETIME='2016091'

echo $DATETIME

mv ./output/model_performance_${DATETIME}* ../churn_BAK/outputBAK/
mv ./output/final_pred_${DATETIME}* ../churn_BAK/outputBAK/
mv ./logs/update_churn_data_${DATETIME}* ../churn_BAK/logsBAK/
mv ./output/GLOBAL_product_progression_${DATETIME}* ../churn_BAK/outputBAK/
mv ./output/library_completion_${DATETIME}* ../churn_BAK/outputBAK/
mv ./output/merged_history_${DATETIME}* ../churn_BAK/outputBAK/
mv ./output/sfdc_opportunity_history_${DATETIME}* ../churn_BAK/outputBAK/

