#!/usr/bin/env python
# coding=utf-8
from configparser import ConfigParser
from run import baserun
from utils import output
from utils import robot
import traceback
import time
import os
import sys

start = time.time()
tic = lambda: 'at %1.1f seconds' % (time.time() - start)

if __name__ == "__main__":

    if len(sys.argv) < 2:
        output.usage()

    if sys.argv[1] == '-h':
        output.usage()

    excutetype = sys.argv[1]
    if not excutetype or excutetype not in ['product', 'activity', 'new', 'activityincrease', 'activityproductincrease', 'productincrease', 'brand', 'category']:
        output.output_info("excutetype is error, please -h check the manual")

    curPath = os.path.abspath(os.path.dirname(__file__))
    configPath = os.path.abspath(curPath + '/config/config.ini')
    config=ConfigParser()
    config.read(configPath)
    try:
        baserunobj = baserun.BaseRun(excutetype, config)
        baserunobj.excuteRun()
    except Exception as e:
        msg = traceback.format_exc()
        output.output_info("mysql insert data to aplum es accurs error, the error:%s"%str(msg))
        robot.alarmDD('aplum_product_new_es/app.py: err=%s'%str(e))

    print("current time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print('Ended Polling: %s' % tic())
    print("success\n\n")
