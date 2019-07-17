#!/usr/bin/env python
# coding=utf-8
import sys


def usage():
    print("app.py usage:")
    print("-h print help message")
    print("-t 类型与Src_page_type的类型相同，"
          "为以下列表的一种['category', 'brand', 'search', 'other', 'recommend', 'activity', 'index', 'total']")
    print("-i 具体页面与Src_page_id相同")
    sys.exit()


def output_info(info):
    print(info)
    sys.exit()
