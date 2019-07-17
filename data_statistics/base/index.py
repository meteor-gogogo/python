#!/usr/bin/env python
# coding=utf-8
from .baselist import BaseList

class IndexList(BaseList):
    @classmethod
    def is_registrar_for(cls, listtype):
        return listtype == 'index'

