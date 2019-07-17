#!/usr/bin/env python
# coding=utf-8
from utils import output
import ctrrun
import getopt
import sys

if __name__ == '__main__':
    try:
        listid = 0
        listtype = "total"
        shortargs = 'ht:i:'
        longargs = ['directory-prefix=', 'format', '--f_long=']
        opts, args = getopt.getopt( sys.argv[1:], shortargs, longargs )
    except getopt.GetoptError:
        output.usage()

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            output.usage()
        elif opt == '-t':
            listtype = arg
        elif opt == '-i':
            listid = arg
    if listtype not in ['category', 'brand', 'search', 'other', 'activity', 'recommend', 'index', 'total']:
        output.output_info("listtype is error, please -h check the manual") 
    try:
        ctrRunObj = ctrrun.CtrRun(listtype, listid)
        ctrRunObj.run()
    except Exception as e:
        output.output_info("ctr run accurs error, the error:%s"%str(e)) 
