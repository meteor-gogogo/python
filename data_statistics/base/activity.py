#!/usr/bin/env python
# coding=utf-8
from .baselist import BaseList

class ActivityList(BaseList):
    @classmethod
    def is_registrar_for(cls, listtype):
        return listtype == 'activity'

