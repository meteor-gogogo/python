#!/usr/bin/env python
# coding=utf-8
import sys

def output_info(info):
    print(info)
    sys.exit()

def usage():
    print ('app.py usage:')
    print ('-h, print help message.')
    print ('-t, 类型与src_page_type相同[category, brand, search, other, activity, recommend, index, total]')
    print ('-i, 具体页面与src_page_id相同')
    sys.exit()
