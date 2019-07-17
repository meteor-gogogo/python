#!/usr/bin/env python
# coding=utf-8
import sys

def output_info(info):
    print(info)
    sys.exit()

def usage():
    print ('app.py usage:')
    print ('-h, print help message.')
    print ('python app.py [new, activity, product, activityincrease, activityproductincrease, productincrease, brand, category]')
    print ('example: python app.py product')
    sys.exit()
