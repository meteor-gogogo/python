#!/usr/bin/env python
# coding=utf-8
from datetime import date

class BaseList(object):

    def __init__(self, listtype, datetimestr, lastdate, starttime, endtime, sensors, es):
        self.date = datetimestr
        self.lastdate = lastdate
        self.starttime = starttime
        self.endtime = endtime
        self.listtype = listtype
        self.es = es
        self.sensors = sensors
        self.day = self.date.strftime("%Y-%m-%d")

    def getDataDic(self):
        ctrinfo = dict()
        ctrdict = dict()
        ctrinfo['date'] = self.day
        ctrinfo['shownum'] = self.es.getTotalShowNum(self.starttime,self.endtime, self.listtype)
        ctrinfo['uv_list'] = self.sensors.getListUv(self.day, self.listtype)
        ctrinfo['pv_list'] = self.sensors.getListPv(self.day, self.listtype)
        ctrinfo['uv_detail'] = self.sensors.getDetailUv(self.day, self.listtype)
        ctrinfo['pv_detail'] = self.sensors.getDetailPv(self.day, self.listtype)
        ctrinfo['pv_detail_distinct'] = self.sensors.getDetailDistinctPv(self.day, self.listtype)
        ctrinfo['pv_detail_sid'] = self.sensors.getDetailSidPv(self.day, self.listtype)
        ctrinfo['listaddcartnum'] = self.sensors.getListAddCartNum(self.date, self.listtype)
        ctrinfo['addcartnum'],ctrinfo['totalcartcp'] = self.sensors.getAddCartNumANDPrice(self.date, self.listtype)
        ctrinfo['orderuv'] = self.sensors.getOrderUv(self.lastdate, self.date, self.listtype)
        ctrinfo['ordernum'] = self.sensors.getOrderNum(self.lastdate, self.date, self.listtype)
        ctrinfo['payordernum'],ctrinfo['totalordercp'],ctrinfo['totalorderpp'] = self.sensors.getPayOrderDetailNumANDPrice(self.lastdate, self.date, self.listtype)
        ctrdict[self.day] = ctrinfo
        return ctrdict
