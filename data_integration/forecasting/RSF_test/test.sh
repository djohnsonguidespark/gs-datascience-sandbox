#!/bin/sh

Rscript randomForestSRC_forecast_Account_CV.R 1 &
Rscript randomForestSRC_forecast_Account_CV.R 2 & 
Rscript randomForestSRC_forecast_Account_CV.R 3 &
Rscript randomForestSRC_forecast_Account_CV.R 4 &
Rscript randomForestSRC_forecast_Account_CV.R 5

