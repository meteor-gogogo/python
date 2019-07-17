#!/usr/bin/env python
# coding=utf-8

class ProductSort(object):
    def __init__(self, pid):
        self.id = pid
        self.category = 999999
        self.sort_score = 0.0
        self.category_sort_score = 0.0 #无货降权
        self.sort_score_ios = 0.0
        self.sort_score_android = 0.0
        self.show_rate = 1.0
        self.show_dec_sort = 0.0


