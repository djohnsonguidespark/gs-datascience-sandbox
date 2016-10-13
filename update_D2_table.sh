#!/bin/sh

mysql -A -uroot << eof
source D2.sql;
eof

