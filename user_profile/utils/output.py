#!/usr/bin/env python
# coding=utf-8

import sys


def output_info(info):
    print(info)
    sys.exit()


def usage():
    print('app.py usage:')
    print('-h print help message.')
    print('-d 统计距今天n天前的数据')
    sys.exit()
