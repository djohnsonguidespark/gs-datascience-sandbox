#! /usr/bin/env python

import sys
import re 
import csv
import MySQLdb as mdb
import pandas as pd
from openpyxl import load_workbook
import time
import collections
from simple_salesforce import Salesforce
from dateutil.relativedelta import relativedelta
from lifelines.datasets import load_rossi
from lifelines import CoxPHFitter

rossi_dataset = load_rossi()
cf = CoxPHFitter()
cf.fit(rossi_dataset, 'week', event_col='arrest', strata=['race'])

cf.print_summary()  # access the results using cf.summary


