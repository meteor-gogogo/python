#!/usr/bin/env python
# coding=utf-8
from datetime import datetime,timedelta,date
from loaddata import loaddata
import time
if __name__ == '__main__':
    starttime = int(time.mktime(time.strptime(str(date.today() + timedelta(days=-1)),"%Y-%m-%d")))
    print(starttime)