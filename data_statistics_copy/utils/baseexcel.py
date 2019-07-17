#!/usr/bin/env python
# coding=utf-8
import pandas as pd


class BaseExcel(object):
    def __init__(self, filename, sheetname, encolumns, newcolumns):
        self.filename = filename
        self.sheetname = sheetname
        self.encolumns = encolumns
        self.newcolumns = newcolumns
        self.writer = self.create_excel_writer()

    def create_excel_writer(self):
        return pd.ExcelWriter(self.filename)

    def data_to_excel(self, data, list_type):
        data = pd.DataFrame(data=data, columns=self.encolumns)
        data = data.rename(columns=self.newcolumns)
        data.to_excel(self.writer, self.sheetname[list_type])

    def save_excel(self):
        self.writer.save()
        self.writer.close()
