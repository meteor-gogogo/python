#!/usr/bin/env python
# coding=utf-8
from utils import output
import businessfind
import getopt
import sys

if __name__ == '__main__':
    try:
        day = 5
        bufRunObj = businessfind.BufRun(day)
        bufRunObj.run()
    except Exception as e:
        output.output_info("buf run accurs error, the error:%s"%str(e))