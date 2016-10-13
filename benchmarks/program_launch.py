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

from benchmark_common_libs import *
from benchmark_create_mysql import *
from aer_libs import *

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',50)

cur_datetime = datetime.now()

DBNAME = "benchmark_prod"
WRITE_TABLE = "TMP_PROGRAM_LAUNCH"

con = None
con = mdb.connect('localhost','root','','');
cur = con.cursor()

start = time.time()

program_launch_2014_df = pd.read_csv('ProgramLaunchDate_2014.csv')
program_launch_2015_df = pd.read_csv('ProgramLaunchDate_2015.csv')
program_launch_df = program_launch_2014_df.append(program_launch_2015_df,ignore_index=True)
 
drop_mysql_table(con,DBNAME,WRITE_TABLE)
create_g2_PROGRAM_LAUNCH_table(con,DBNAME,WRITE_TABLE)
insert_into_g2_PROGRAM_LAUNCH_table(con,DBNAME,WRITE_TABLE,program_launch_df,start)

