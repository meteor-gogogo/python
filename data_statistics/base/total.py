#!/usr/bin/env python
# coding=utf-8
from .baselist import BaseList

class TotalList(BaseList):
    @classmethod
    def is_registrar_for(cls, listtype):
        return listtype == 'total'

