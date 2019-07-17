#!/usr/bin/env python
# coding=utf-8
from datetime import datetime, timedelta, date
import base.baselist as baselist
import base.total as total
import base.category as category
import base.brand as brand
import base.search as search
import base.activity as activity
import base.other as other
import base.recommend as recommend
import base.index as index
from models import es
from models import sensors
import time

class DataOperator(object):

    def __init__(self, listtype, config):
        today = date.today()
        self.date = today + timedelta(days=-1)
        self.lastdate = today + timedelta(days=-8)
        self.starttime = int(time.mktime(time.strptime(str(self.date), "%Y-%m-%d")))
        self.endtime = int(time.mktime(time.strptime(str(today), "%Y-%m-%d")))
        self.listtype = listtype
        self.config = config
        self.initEs()
        self.initSensorsUrl()

    def initEs(self):
        eshost = self.config.get("ELASTICSEARCH", "host")
        esuser = self.config.get("ELASTICSEARCH", "user")
        espasswd = self.config.get("ELASTICSEARCH", "password")
        esport = self.config.get("ELASTICSEARCH", "port")
        self.esoperator = es.EsOperator(eshost, esuser, espasswd, esport)

    def initSensorsUrl(self):
        sensorsurl = self.config.get("SENSORS", "url")
        sensorstoken = self.config.get("SENSORS", "token")
        url = "%s?token=%s"%(sensorsurl, sensorstoken)
        self.sensorsoperator = sensors.SensorsOperator(url)

    def getDataByType(self, listtype):
        print(listtype,  baselist.BaseList.__subclasses__())
        for cls in baselist.BaseList.__subclasses__():
            print('===', listtype,  cls,  cls.is_registrar_for(listtype))
            if cls.is_registrar_for(listtype):
                return cls(self.listtype, self.date, self.lastdate, self.starttime, self.endtime, self.sensorsoperator, self.esoperator).getDataDic()

    def getAllData(self):
        allobj = total.TotalList(self.listtype, self.date, self.lastdate, self.starttime, self.endtime, self.sensorsoperator, self.esoperator)
        totalctrdict =allobj.getDataDic()
        return totalctrdict

    def getCategoryData(self):
        categoryobj = category.CategoryList(self.listtype, self.date, self.lastdate, self.starttime, self.endtime, self.sensorsoperator, self.esoperator)
        categoryctrdict =categoryobj.getDataDic()
        return categoryctrdict

    def getBrandData(self):
        brandobj = brand.BrandList(self.listtype, self.date, self.lastdate, self.starttime, self.endtime, self.sensorsoperator, self.esoperator)
        brandctrdict =brandobj.getDataDic()
        return brandctrdict

    def getSearchData(self):
        searchobj = search.SearchList(self.listtype, self.date, self.lastdate, self.starttime, self.endtime, self.sensorsoperator, self.esoperator)
        searchctrdict =searchobj.getDataDic()
        return searchctrdict

    def getActivityData(self):
        activityobj = activity.ActivityList(self.listtype, self.date, self.lastdate, self.starttime, self.endtime, self.sensorsoperator, self.esoperator)
        activityctrdict =activityobj.getDataDic()
        return activityctrdict

    def getOtherData(self):
        otherobj = other.OtherList(self.listtype, self.date, self.lastdate, self.starttime, self.endtime, self.sensorsoperator, self.esoperator)
        otherctrdict =otherobj.getDataDic()
        return otherctrdict

    def getIndexData(self):
        indexobj = index.IndexList(self.listtype, self.date, self.lastdate, self.starttime, self.endtime, self.sensorsoperator, self.esoperator)
        indexctrdict =indexobj.getDataDic()
        return indexctrdict

    def getRecommendData(self):
        recommendobj = recommend.RecommendList(self.listtype, self.date, self.lastdate, self.starttime, self.endtime, self.sensorsoperator, self.esoperator)
        recommendctrdict =recommendobj.getDataDic()
        return recommendctrdict
