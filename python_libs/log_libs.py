#! /usr/bin/env python

###################################
# Created by DKJ ... 9/12/2016
# This library initializes logging 
###################################

# Logging
import logging
from logging.config import fileConfig

def init_logging():
	fileConfig('/home/analytics/analytics_sandbox/python_libs/logging_config.ini')
	logging.basicConfig(level=logging.INFO)
	logger = logging.getLogger()
	LOG = logging.getLogger()
	
	# create file logging format
	handler = logging.FileHandler('file.log')
	formatter = logging.Formatter('[%(asctime)s][%(filename)s][%(funcName)s][Line %(lineno)5d] %(levelname)s - %(message)s')
	handler.setFormatter(formatter)
	
	# add the handlers to the logger
	LOG.addHandler(handler) ## All LOG entries go to the log file

	return LOG

