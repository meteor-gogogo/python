#!/usr/bin/env python
# coding=utf-8
from datetime import datetime, timedelta, date
import pandas as pd

class BaseExcel(object):

    def __init__(self, filename, sheetname, encolumns, newcolumns):
        self.filename = filename
        self.sheetname = sheetname
        self.encolumns = encolumns
        self.newcolumns = newcolumns

    def createExcelWriter(self):
        self.writer = pd.ExcelWriter(self.filename)

    def data2excel(self, data, listtype):
        data = pd.DataFrame(data, columns = self.encolumns)
        data = data.rename(columns = self.newcolumns)
        data.to_excel(self.writer, self.sheetname[listtype])

    def saveExcelWriter(self):
        self.writer.save()  # 保存Excel表格
        self.writer.close()  # 关闭Excel表格


