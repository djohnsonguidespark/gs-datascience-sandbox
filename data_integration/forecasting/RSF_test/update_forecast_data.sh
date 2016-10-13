#!/bin/sh

Rscript randomForestSRC_forecast_Account_CV.R
Rscript randomForestSRC_iteration_Account.R
Rscript randomForestSRC_production_Account.R

