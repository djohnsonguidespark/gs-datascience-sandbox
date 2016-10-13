#!/bin/sh

OPID='0063800000cDwYL'
ALLDAY='sdata_allday_RNN.csv'
COMPFILE='sdata_op_history_RSF_20160902_ORIG.csv'

head -n1 ./output/${ALLDAY} > test_${ALLDAY} | grep ${OPID} ./output/${ALLDAY} >> test_${ALLDAY}
head -n1 ../churn_BAK/outputBAK/${COMPFILE} > test_${COMPFILE} | grep ${OPID} ../churn_BAK/outputBAK/${COMPFILE} >> test_${COMPFILE}
