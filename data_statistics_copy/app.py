#!/usr/bin/env python
# coding=utf-8
from utils import output
import ctrrun
import getopt
import sys


if __name__ == '__main__':
    opts = dict()
    list_id = 0
    list_type = "total"
    short_args = 'ht:i:'
    long_args = ['directory-prefix=', 'format', 'f_long=']
    try:
        opts, args = getopt.getopt(sys.argv[1:], short_args, long_args)
    except getopt.GetoptError:
        output.usage()

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            output.usage()
        elif opt == "-t":
            list_type = arg
        elif opt == "-i":
            list_id = arg

    if list_type not in ['category', 'brand', 'search', 'other', 'index', 'activity', 'recommend', 'total']:
        output.output_info("list_type is error, please -h check the manual")

    try:
        ctr_run_obj = ctrrun.CtrRun(list_type, list_id)
        ctr_run_obj.run()
    except Exception as e:
        output.output_info("ctr_run occurs error, the error is %s" % str(e))

