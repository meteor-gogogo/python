#!/usr/bin/env python
# coding=utf-8
from .baselist import BaseList


class OtherList(BaseList):
    @classmethod
    def is_registrar_for(cls, list_type):
        return list_type == 'other'
