#!/usr/bin/python3
# coding=utf-8

import sys
import traceback
import manage
import lib.robot as robot

Days=1
if __name__ == '__main__':
    if len(sys.argv) <=1:
        print ("default data %d days"%(Days))
    else:
        Days = int(sys.argv[1])
        print ("calc data %d days"%(Days))

    try:
        manage.run(Days)
    except Exception as e:
        print ("manage run error")
        msg = traceback.format_exc()
        print (msg)
        robot.alarmDD('recommendation/main.py: err=%s'%str(e))
