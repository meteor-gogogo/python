#!/usr/bin/env python
# coding=utf-8

from utils import output
import getopt
import sys
import user_pro_run

if __name__ == '__main__':
    try:
        beforedays = 1
        shortargs = "hd:"
        opts, shortargs = getopt.getopt(sys.argv[1:], shortargs)
    except getopt.GetoptError:
        output.usage()

    for opt, arg in opts:
        if opt == '-h':
            output.usage()
        elif opt == '-d':
            beforedays = arg
    # print(beforedays)
    if beforedays not in [str(i) for i in range(30)]:
        output.output_info("输入的天数出错,请输入1--30")

    try:
        pro_run = user_pro_run.UserProRun(beforedays)
        pro_run.run()
    except Exception as e:
        output.output_info("UserProRun accurs error, the error:%s"%str(e))