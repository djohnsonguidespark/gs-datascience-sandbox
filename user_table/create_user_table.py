#! /usr/bin/env python

import sys
import re
import csv
from decimal import Decimal
import MySQLdb as mdb
import pandas as pd
from openpyxl import load_workbook
import time
import collections
from matplotlib.pyplot import *
import matplotlib.pyplot as plt
import matplotlib.pylab as plab
import matplotlib.colors as colors
import matplotlib.cm as cmx
import itertools

sys.path.insert(0,'/home/analytics/analytics_sandbox/python_libs');

from create_mysql import *

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',50)

cur_datetime = datetime.now()

DBNAME = "edits_prod"
WRITE_TABLE = "TMP_USER_LOOKUP"

con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

user_lookup_df = pd.read_csv('g2_users_completed_20151015.csv')

start = time.time()
drop_mysql_table(con,DBNAME,WRITE_TABLE)
create_g2_USER_LOOKUP_table(con,DBNAME,WRITE_TABLE)
insert_into_g2_USER_LOOKUP_table(con,DBNAME,WRITE_TABLE,user_lookup_df,start)

