#!/usr/bin/env python
# coding=utf-8
from datetime import datetime, timedelta, date
from configparser import ConfigParser
from utils import output, baseexcel, basemail
from baseoperator import dataoperator
import pandas as pd
import sys
import os

class CtrRun(object):

    def __init__(self, listtype, listid):
        self.listtype = listtype
        self.listid = listid
        self.sheetname = {"category":"分类页", "index":"首页", "total":"总数", "brand":"品牌页", "search":"搜索页", "activity":"活动页", "other":"其他页", "recommend":"推荐页"}
        self.encolumns = ['date', 'shownum', 'uv_list', 'pv_list', 'uv_detail', 'pv_detail', 'pv_detail_sid', 'pv_detail_distinct', 'listaddcartnum', 'addcartnum','totalcartcp', 'orderuv', 'ordernum', 'payordernum', 'totalordercp', 'totalorderpp']
        self.newcolumns = {'date':"日期", 'shownum':"活动页曝光量", 'uv_list':"活动页总uv数", 'pv_list':"活动页总pv数", 'uv_detail':"活动页到达详情页总uv", 'pv_detail':"活动页到达详情页总曝光pv(不滤重)", 'pv_detail_sid':"活动页到达详情页总曝光量PV(滤重pv)", 'pv_detail_distinct':"活动页到达详情页总曝光量(滤重同一商品)", 'listaddcartnum':"活动页列表加购数", 'addcartnum':"活动页加购数",'totalcartcp':"活动页加购商品折扣总价", 'orderuv':"活动页支付订单uv数", 'ordernum':"活动页支付订单数", 'payordernum':"活动页支付订单商品数", 'totalordercp':"活动页支付订单商品折扣总价", 'totalorderpp':"活动页支付订单商品实付总价"}

    def run(self):
        today = date.today()
        dateobj = today + timedelta(days=-1)
        datestr = dateobj.strftime("%Y-%m-%d")
        curPath = os.path.abspath(os.path.dirname(__file__))
        configPath = os.path.abspath(curPath + '/config/config.ini')
        config=ConfigParser()
        config.read(configPath)
        operatorobj = dataoperator.DataOperator(self.listtype, config)
        if self.listtype == "total":
            print('======总数 ======')
            totalctrdict = operatorobj.getAllData()
            totaldf= pd.DataFrame(list(totalctrdict.values()))
            print('======分类页 ======')
            categoryctrdict = operatorobj.getCategoryData()
            categorydf= pd.DataFrame(list(categoryctrdict.values()))
            print('======搜索页 ======')
            searchctrdict = operatorobj.getSearchData()
            searchdf= pd.DataFrame(list(searchctrdict.values()))
            print('======活动页 ======')
            activityctrdict = operatorobj.getActivityData()
            activitydf= pd.DataFrame(list(activityctrdict.values()))
            print('======品牌页 ======')
            brandctrdict = operatorobj.getBrandData()
            branddf= pd.DataFrame(list(brandctrdict.values()))
            print('======其他页 ======')
            otherctrdict = operatorobj.getOtherData()
            otherdf= pd.DataFrame(list(otherctrdict.values()))
            print('======首页 ======')
            indexctrdict = operatorobj.getIndexData()
            indexdf= pd.DataFrame(list(indexctrdict.values()))
            print('======推荐页 ======')
            recommendctrdict = operatorobj.getRecommendData()
            recommenddf= pd.DataFrame(list(recommendctrdict.values()))

            filename = "ctr_total_day_%s.xls" % datestr
            BaseExcelObj = baseexcel.BaseExcel(filename, self.sheetname, self.encolumns, self.newcolumns)
            BaseExcelObj.createExcelWriter()
            BaseExcelObj.data2excel(totaldf, "total")
            BaseExcelObj.data2excel(categorydf, "category")
            BaseExcelObj.data2excel(searchdf, "search")
            BaseExcelObj.data2excel(activitydf, "activity")
            BaseExcelObj.data2excel(branddf, "brand")
            BaseExcelObj.data2excel(otherdf, "other")
            BaseExcelObj.data2excel(indexdf, "index")
            BaseExcelObj.data2excel(recommenddf, "recommend")
            BaseExcelObj.saveExcelWriter()
            subject = "数据统计"
            content = "数据见附件"
            #addrlist = ['suzerui@aplum.com.cn', 'liudiyuhan@aplum.com.cn', 'wengxuejie@aplum.com.cn', 'pangbo@aplum.com.cn', 'liwenlong@aplum.com.cn']
            addrlist = ['wengxuejie@aplum.com.cn']
            attachmentFilePath = os.path.abspath(curPath + '/' + filename)
            BaseMailObj = basemail.BaseMail(config, subject, content, addrlist, attachmentFilePath)
            BaseMailObj.sendMail()
        else:
            if self.listid == 0:
                typectrdict = operatorobj.getDataByType(self.listtype)
                typedf= pd.DataFrame(list(typectrdict.values()))
                filename = "ctr_%s_day_%s.xls" %(self.listtype, datestr)
                BaseExcelObj = baseexcel.BaseExcel(filename, self.sheetname, self.encolumns, self.newcolumns)
                BaseExcelObj.createExcelWriter()
                BaseExcelObj.data2excel(typedf, self.listtype)
                BaseExcelObj.saveExcelWriter()
                subject = "%s数据统计"%self.sheetname[self.listtype]
                content = "数据见附件"
                #addrlist = ['suzerui@aplum.com.cn', 'liudiyuhan@aplum.com.cn', 'wengxuejie@aplum.com.cn', 'pangbo@aplum.com.cn', 'liwenlong@aplum.com.cn']
                addrlist = ['wengxuejie@aplum.com.cn']
                attachmentFilePath = os.path.abspath(curPath + '/' + filename)
                BaseMailObj = basemail.BaseMail(config, subject, content, addrlist, attachmentFilePath)
                BaseMailObj.sendMail()
            else:
                pass
