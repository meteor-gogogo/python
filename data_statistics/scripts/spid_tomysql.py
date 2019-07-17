#!/usr/bin/env python
# coding=utf-8
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta, date
import requests
import time
import json
import MySQLdb
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import os
import sys
import traceback
from sqlalchemy import create_engine

mysqlhost = 'rr-2ze7824567d8dgu1y.mysql.rds.aliyuncs.com'
mysqlusername = 'plumdb'
mysqlpasswd = 'plumdb@2018'
db = 'aplum'

statmysqlhost = 'pc-2ze47js669jk7k187.rwlb.rds.aliyuncs.com'
statmysqlusername = 'plumdb'
statmysqlpasswd = 'plumdb@2019'
statdb = 'aplum_stat'

es = Elasticsearch(["es-cn-0pp0vm2ju000a003g.elasticsearch.aliyuncs.com"], http_auth=('elastic', 'NHRHpqeFn8'),
                   port=9200)
url = "http://websa.aplum-inc.com/api/sql/query?token=15ba27be5b7895379a0574993db783281988c16f95969fe423847d31ed95728d"
starttime = time.time()
tic = lambda: 'at %1.1f seconds' % (time.time() - starttime)

namedict = {'stat_date': "日期", 'type': "类型", 'pv_show': "列表总曝光量", 'uv_list': "列表页uv", 'pv_list': "列表页pv",
            'uv_detail': "详情页uv", 'pv_detail': "详情页曝光pv(不滤重)", 'pv_detail_distinct': "详情页曝光PV(滤重同一商品)",
            'pv_detail_sid': "详情页曝光PV(滤重pv)", 'pv_cart_list': "单日列表加购数", 'pv_cart': "单日加购数",
            'totalcartsp': "单日加购商品在售总价", 'totalcartcp': "单日加购商品折扣总价", 'uv_order': "单日订单uv", 'pv_order': "单日订单数",
            'pv_order_product': "单日订单商品数", 'totalordersp': "单日订单商品在售总价", 'totalordercp': "单日订单商品折扣总价",
            'totalorderpp': "单日订单商品实付总价"}

typedict = {'category': "分类页", 'activity': "活动页", 'search': "搜索页", 'brand': "品牌页", 'index': "首页",
            'other': '其他页', 'recommend': "推荐页", 'all': "总数"}

platformlist = ['IOS客户端', 'Android客户端', '微信小程序', '普通网页', '微信内网页', '百度小程序']


def alarm(userlist, msg):
    url = 'http://47.93.240.37:8083/ps'
    users = ','.join(userlist)
    url = "%s?msg=%s&email=%s" % (url, msg, users)
    r = requests.get(url)
    if r.status_code == 200:
        return True
    else:
        return False

def sendMain(addressList, attachmentName):
    sender = 'aplumctr@aplum-inc.com'  # 发送邮件的邮箱地址
    user = 'aplumctr@aplum-inc.com'  # 发送邮件的邮箱地址
    passwd = 'qZdCAf3MCDv8SXi'  # 发送邮件的邮箱密码
    smtpserver = 'smtp.aplum-inc.com'  # 发送邮件的邮件服务器
    receivers = []  # 接收邮件的邮箱地址
    subject = "{} spid_ctr入库信息".format(datestr)
    filePath = './%s' % attachmentName
    baseName = os.path.basename(filePath)
    attachdisposition = "attachment;filename =%s" % baseName  # 发送附件html格式
    sendfile = open(filePath, 'rb').read()
    att = MIMEText(sendfile, 'plain', 'utf-8')
    att["Content-Type"] = "application/octet-stream"
    att["Content-Disposition"] = attachdisposition  # 发送附件html格式
    msgText = MIMEText('您好，{} spid_ctr入库成功 0.0'.format(datestr), 'plain', 'utf-8')

    for addr in addressList:
        receivers.append(addr)
    receiver = ';'.join(receivers)
    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = subject
    msgRoot['From'] = sender
    msgRoot['To'] = receiver
    msgRoot.attach(msgText)  # 添加正文
    msgRoot.attach(att)  # 发送文件格式
    # 发送邮件
    try:
        smtp = smtplib.SMTP_SSL(smtpserver, 465)
        smtp.login(user, passwd)
        smtp.sendmail(sender, addressList, msgRoot.as_string())
        print(datetime.now().strftime("%Y.%m.%d-%H:%M:%S"), '发送成功')
    except Exception as e:
        print(e)
    finally:
        os.remove(filePath)
        smtp.quit()


def initDataDict(pf, type, spid):
    pidinfo = dict()
    pidinfo['stat_date'] = date
    pidinfo['platform'] = pf
    pidinfo['src_page_type'] = type
    pidinfo['src_page_group'] = ''
    pidinfo['src_page_id'] = spid
    pidinfo['show_uv'] = 0
    pidinfo['show_pv'] = 0
    pidinfo['show_total_saleprice'] = 0.0
    pidinfo['show_total_discountprice'] = 0.0
    pidinfo['list_uv'] = 0
    pidinfo['list_pv'] = 0
    pidinfo['detail_uv'] = 0
    pidinfo['detail_pv'] = 0
    pidinfo['detail_distinct_pv'] = 0
    pidinfo['detail_sid_pv'] = 0
    pidinfo['detail_total_saleprice'] = 0.0
    pidinfo['detail_total_discountprice'] = 0.0
    pidinfo['detail_distinct_total_saleprice_'] = 0.0
    pidinfo['detail_distinct_total_discountprice'] = 0.0
    pidinfo['cart_uv'] = 0
    pidinfo['cart_pv'] = 0
    pidinfo['cart_total_saleprice'] = 0.0
    pidinfo['cart_total_discountprice'] = 0.0
    pidinfo['cart_list_uv'] = 0
    pidinfo['cart_list_pv'] = 0
    pidinfo['cart_list_total_saleprice'] = 0.0
    pidinfo['cart_list_total_discountprice'] = 0.0
    pidinfo['wish_uv'] = 0
    pidinfo['wish_pv'] = 0
    pidinfo['wish_total_saleprice'] = 0.0
    pidinfo['wish_total_discountprice'] = 0.0
    pidinfo['order_uv'] = 0
    pidinfo['order_pv'] = 0
    pidinfo['order_product_pv'] = 0
    pidinfo['order_total_saleprice'] = 0.0
    pidinfo['order_total_discountprice'] = 0.0
    pidinfo['order_total_realpayprice'] = 0.0

    return pidinfo


def getSqlResult(sql):
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        dataarr = datastr.split("\n")
        return dataarr
    else:
        print("sa hive sql accur error, sql为{}".format(sql))


def getEsResult(date, doc):
    p = dict()
    p['request_timeout'] = 30
    return es.search(index='aplum_ctr-{}'.format(date), doc_type='product', body=doc, params=p)


def getPfShowList(pf):
    if pf == 'IOS客户端':
        pflist = ["ios_app", "ios_app_native"]
    elif pf == 'Android客户端':
        pflist = ["android_app", "android_app_native"]
    elif pf == '微信小程序':
        pflist = ["ios_wechat", "android_wechat", "ipad_wechat","wechat_native"]
    elif pf == '普通网页':
        pflist = ["m_wechat", "web_wechat"]
    return pflist


def getPfDeviceList(pf):
    if pf == '普通网页':
        devicelist = ["iX-Pd-Identify"]
    elif pf == 'Android客户端':
        devicelist = ["X-Pd-Imei"]
    elif pf == 'IOS客户端':
        devicelist = ["X-Pd-Idfa"]
    elif pf == '微信小程序':
        devicelist = ['X-Pd-Identify']
    return devicelist


# 获取单日各详细分类的曝光pv及uv
def getShowInfo(pf, sptype, date, spiddict):
    datestr = date.strftime("%Y-%m-%d")
    if pf == '微信内网页' or pf == '百度小程序':
        return spiddict
    else:
        pflist_show = getPfShowList(pf)
        esdoc = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "Src_page_type.keyword": sptype
                            }
                        },
                        {
                            "terms": {
                                "X-Pd-Os.keyword": pflist_show
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "idcount": {
                    "cardinality": {
                        "field": "Src_page_id.keyword"
                    }
                }
            }
        }
        # print(esdoc)
        searched = getEsResult(datestr, esdoc)
        totalshownum = searched['aggregations']['idcount']['value']
        size = 1000
        if totalshownum <= size:
            pagesize = 1
        else:
            pagesize = int(totalshownum / size + 2)
        devicelist = getPfDeviceList(pf)
        for devicetype in devicelist:
            aggfield = devicetype + ".keyword"
            for num in range(0, pagesize):
                spiddoc = {
                    "size": 0,
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "terms": {
                                        "X-Pd-Os.keyword": pflist_show
                                    }
                                },
                                {
                                    "term": {
                                        "Src_page_type.keyword": sptype
                                    }
                                }
                            ]
                        }
                    },
                    "aggs": {
                        "idcount": {
                            "terms": {
                                "field": "Src_page_id.keyword",
                                "include": {
                                    "partition": num,
                                    "num_partitions": pagesize
                                },
                                "size": 5000
                            },
                            "aggs": {
                                "DEV": {
                                    "cardinality": {
                                        "field": aggfield
                                    }
                                }
                            }
                        }
                    }
                }
                # print(spiddoc)
                spidsearched = getEsResult(datestr, spiddoc)
                buckets = spidsearched['aggregations']['idcount']['buckets']
                for b in buckets:
                    spid = str(b['key'])
                    hitnum = int(b['doc_count'])
                    uv_show = int(b['DEV']['value'])
                    spidinfo = initDataDict(pf, sptype, spid)
                    spidinfo['show_pv'] = hitnum
                    spidinfo['show_uv'] = uv_show
                    key = pf + sptype + spid
                    spiddict[key] = spidinfo
                time.sleep(0.5)
    return spiddict


# 获取单日各详细分类的列表pv及uv
def getListInfo(sptype, date, datadict):
    sql = "SELECT  src_page_id as spid, plumfrontend AS pf, COUNT(*) AS pv_list, COUNT( DISTINCT ( distinct_ID ) ) AS uv_list FROM EVENTS WHERE EVENT = 'ViewProductList' AND date = '{0}' AND sid != '' AND src_page_type = '{1}'  GROUP BY spid,pf".format(
        date, sptype)
    dataarr = getSqlResult(sql)
    if dataarr[0] == '':
        return datadict
    else:
        for i in range(0, len(dataarr)):
            if len(dataarr[i]) == 0:
                continue
            else:
                datajson = json.loads(dataarr[i])
            spid = str(datajson['spid'])
            pf = str(datajson['pf'])
            key = pf + sptype + spid
            if key in datadict:
                pidinfo = datadict[key]
            else:
                pidinfo = initDataDict(pf, sptype, spid)
                k = pf + sptype + spid
                datadict[k] = pidinfo
            pv_list = datajson['pv_list']
            uv_list = datajson['uv_list']
            pidinfo['list_pv'] = pv_list
            pidinfo['list_uv'] = uv_list
    return datadict


# 获取单日各详细分类的详情信息,含不去重uv,pv,在售总价及折扣总价等
def getDetailInfo(sptype, date, datadict):
    sql = "SELECT  src_page_id as spid, plumfrontend as pf,COUNT(*) AS pv_detail, COUNT( DISTINCT ( distinct_ID ) ) AS uv_detail, SUM(productsale_price) AS totalsp,SUM(productdiscount_price) AS totalcp FROM EVENTS WHERE EVENT = 'ViewProduct' AND date = '{0}' AND sid != '' AND src_page_type = '{1}'  GROUP BY spid,pf".format(
        date, sptype)
    dataarr = getSqlResult(sql)
    if dataarr[0] == '':
        return datadict
    else:
        for i in range(0, len(dataarr)):
            if len(dataarr[i]) == 0:
                continue
            else:
                datajson = json.loads(dataarr[i])
            spid = str(datajson['spid'])
            pf = str(datajson['pf'])
            key = pf + sptype + spid
            if key in datadict:
                pidinfo = datadict[key]
            else:
                pidinfo = initDataDict(pf, sptype, spid)
                k = pf + sptype + spid
                datadict[k] = pidinfo
            uv_detail = datajson['uv_detail']
            pv_detail = datajson['pv_detail']
            totalsp = datajson['totalsp']
            totalcp = datajson['totalcp']
            pidinfo['detail_pv'] = pv_detail
            pidinfo['detail_uv'] = uv_detail
            pidinfo['detail_total_saleprice'] = totalsp
            pidinfo['detail_total_discountprice'] = totalcp
    return datadict


# 获取单日各详细分类的去重详情信息,含去重uv,pv,在售总价及折扣总价等
def getDetailDistinctAndSidInfo(sptype, date, datadict):
    sql = "SELECT spid,pf,count(*) as pv_detail_distinct,count(distinct(ta.sid)) AS pv_detail_sid,sum(sp) as totalsp,sum(cp) as totalcp FROM (SELECT sid,plumfrontend AS pf, distinct_ID as uid, productid as pid, src_page_id as spid, productdiscount_price as sp, productsale_price as cp FROM EVENTS WHERE EVENT = 'ViewProduct' AND date = '{}' AND sid != '' AND src_page_type = '{}'  GROUP BY sid,spid,uid,productid,sp,cp,pf) ta GROUP BY spid,pf".format(
        date, sptype)
    dataarr = getSqlResult(sql)
    if dataarr[0] == '':
        return datadict
    else:
        for i in range(0, len(dataarr)):
            if len(dataarr[i]) == 0:
                continue
            else:
                datajson = json.loads(dataarr[i])
            spid = str(datajson['spid'])
            pf = str(datajson['pf'])
            key = pf + sptype + spid
            if key in datadict:
                pidinfo = datadict[key]
            else:
                pidinfo = initDataDict(pf, sptype, spid)
                k = pf + sptype + spid
                datadict[k] = pidinfo
            pv_detail_distinct = datajson['pv_detail_distinct']
            pv_detail_sid = datajson['pv_detail_sid']
            totalsp = datajson['totalsp']
            totalcp = datajson['totalcp']
            pidinfo['detail_distinct_pv'] = pv_detail_distinct
            pidinfo['detail_sid_pv'] = pv_detail_sid
            pidinfo['detail_distinct_total_saleprice'] = totalsp
            pidinfo['detail_distinct_total_discountprice'] = totalcp
    return datadict

# 获取单日各详细分类的收藏信息,含uv,pv,在售总价及折扣总价等
def getWIshInfo(sptype, date, datadict):
    sql = "SELECT  src_page_id as spid, plumfrontend as pf,COUNT(*) AS pv_wish, COUNT( DISTINCT ( distinct_ID ) ) AS uv_wish, SUM(productsale_price) AS totalsp,SUM(productdiscount_price) AS totalcp FROM EVENTS WHERE EVENT = 'AddWish' AND date = '{0}' AND sid != '' AND src_page_type = '{1}'  GROUP BY spid,pf".format(
        date, sptype)
    dataarr = getSqlResult(sql)
    if dataarr[0] == '':
        return datadict
    else:
        for i in range(0, len(dataarr)):
            if len(dataarr[i]) == 0:
                continue
            else:
                datajson = json.loads(dataarr[i])
            spid = str(datajson['spid'])
            pf = str(datajson['pf'])
            key = pf + sptype + spid
            if key in datadict:
                pidinfo = datadict[key]
            else:
                pidinfo = initDataDict(pf, sptype, spid)
                k = pf + sptype + spid
                datadict[k] = pidinfo
            pv_wish = datajson['uv_wish']
            uv_wish = datajson['pv_wish']
            totalsp = datajson['totalsp']
            totalcp = datajson['totalcp']
            pidinfo['wish_pv'] = pv_wish
            pidinfo['wish_uv'] = uv_wish
            pidinfo['wish_total_saleprice'] = totalsp
            pidinfo['wish_total_discountprice'] = totalcp
    return datadict

# 获取单日各详细分类的列表加购信息,含uv,pv,在售总价及折扣总价等
def getListAddCartInfo(sptype, date, datadict):
    sql = "SELECT tc.spid,tc.pf,count(tc.sid) as pv_cart_list,count(distinct(tc.uid)) as uv_cart_list,sum(tc.sp) as totalsp,sum(tc.cp) as totalcp FROM (SELECT ta.sid,ta.uid,ta.sp,ta.cp,tb.spid,ta.pf FROM (SELECT sid, distinct_ID as uid, plumfrontend AS pf, productid AS pid, productsale_price as sp, productdiscount_price as cp FROM EVENTS WHERE EVENT = 'AddCart' AND src_page_type = 'list' AND sid != '' AND date = '{0}'  GROUP BY sid, pid, sp,cp,uid, pf ) ta LEFT JOIN (SELECT sid, distinct_ID as uid, src_page_type AS sptype, src_page_id AS spid FROM EVENTS WHERE EVENT IN ( 'ViewProduct', 'ViewProductList' ) AND date = '{1}' AND sid != '' AND src_page_type = '{2}'  GROUP BY sid,uid, sptype, spid ) tb ON ta.sid = tb.sid AND ta.uid = tb.uid) tc WHERE tc.spid != '' GROUP BY tc.spid,tc.pf ".format(
        date, date, sptype)
    dataarr = getSqlResult(sql)
    if dataarr[0] == '':
        return datadict
    else:
        for i in range(0, len(dataarr)):
            if len(dataarr[i]) == 0:
                continue
            else:
                datajson = json.loads(dataarr[i])
            spid = str(datajson['spid'])
            pf = str(datajson['pf'])
            key = pf + sptype + spid
            if key in datadict:
                pidinfo = datadict[key]
            else:
                pidinfo = initDataDict(pf, sptype, spid)
                k = pf + sptype + spid
                datadict[k] = pidinfo
            pv_cart_list = datajson['pv_cart_list']
            uv_cart_list = datajson['uv_cart_list']
            totalsp = datajson['totalsp']
            totalcp = datajson['totalcp']
            pidinfo['cart_list_pv'] = pv_cart_list
            pidinfo['cart_list_uv'] = uv_cart_list
            pidinfo['cart_list_total_saleprice'] = totalsp
            pidinfo['cart_list_total_discountprice'] = totalcp
    return datadict


# 获取单日各详细分类的加购信息,含uv,pv,在售总价及折扣总价等
def getAddCartInfo(sptype, date, datadict):
    sql = "SELECT tc.spid,tc.pf,count(tc.sid) as pv_cart,count(distinct(tc.uid)) as uv_cart,sum(tc.sp) as totalsp,sum(tc.cp) as totalcp FROM (SELECT ta.sid,ta.uid,ta.sp,ta.cp,ta.pf,tb.spid FROM (SELECT sid, distinct_ID as uid, plumfrontend AS pf,productid AS pid, productsale_price as sp, productdiscount_price as cp FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date = '{0}' GROUP BY sid, pid, sp, cp,uid,pf ) ta LEFT JOIN (SELECT sid, distinct_ID as uid, src_page_type AS sptype, src_page_id AS spid FROM EVENTS WHERE EVENT IN ( 'ViewProduct', 'ViewProductList' ) AND date = '{1}' AND sid != '' AND src_page_type = '{2}'  GROUP BY sid,uid, sptype, spid ) tb ON ta.sid = tb.sid AND ta.uid = tb.uid) tc WHERE tc.spid != '' GROUP BY tc.spid,tc.pf ".format(
        date, date, sptype)
    dataarr = getSqlResult(sql)
    if dataarr[0] == '':
        return datadict
    else:
        for i in range(0, len(dataarr)):
            if len(dataarr[i]) == 0:
                continue
            else:
                datajson = json.loads(dataarr[i])
            spid = str(datajson['spid'])
            pf = str(datajson['pf'])
            key = pf + sptype + spid
            if key in datadict:
                pidinfo = datadict[key]
            else:
                pidinfo = initDataDict(pf, sptype, spid)
                k = pf + sptype + spid
                datadict[k] = pidinfo
            pv_cart = datajson['pv_cart']
            uv_cart = datajson['uv_cart']
            totalsp = datajson['totalsp']
            totalcp = datajson['totalcp']
            pidinfo['cart_pv'] = pv_cart
            pidinfo['cart_uv'] = uv_cart
            pidinfo['cart_total_saleprice'] = totalsp
            pidinfo['cart_total_discountprice'] = totalcp
    return datadict


# 获取单日各详细分类的订单信息,含uv,pv,在售总价及折扣总价等
def getOrderProductInfo(sptype, lastdate, date, datadict):
    sql = "SELECT spid, pf,count( * ) AS pv_order_product,count(distinct(tf.uid)) as uv_order,sum( tf.saleprice ) AS totalsp,	sum( tf.countprice ) AS totalcp, sum( tf.payprice ) AS totalpp FROM (SELECT te.uid, te.pid, te.oid, te.time, te.sptype, te.spid,te.pf, te.saleprice, te.countprice, te.payprice, row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time DESC ) rk FROM (SELECT tc.pid, tc.time, tc.sptype, td.oid, tc.uid, tc.spid, td.saleprice, td.countprice, td.payprice,td.pf FROM (SELECT ta.sid, ta.pid, ta.time, tb.sptype, tb.spid, ta.uid FROM (SELECT	sid, productid AS pid, time, distinct_ID AS uid FROM EVENTS WHERE	EVENT = 'AddCart' AND sid != '' AND date BETWEEN  '{0}' AND '{1}' GROUP BY sid, pid, time, uid ) ta LEFT JOIN (SELECT sid,	src_page_type AS sptype, src_page_id AS spid FROM EVENTS WHERE EVENT IN ( 'ViewProductList', 'ViewProduct' ) AND date BETWEEN  '{2}' AND '{3}' AND sid != '' GROUP BY sid, sptype, spid) tb ON ta.sid = tb.sid ) tc LEFT JOIN (SELECT plumfrontend AS pf,orderid AS oid, productid AS pid, distinct_ID AS uid, orderitem_saleprice AS saleprice, orderitem_discountprice AS countprice, orderitem_realpayprice AS payprice FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' GROUP BY oid, pid, uid, saleprice, countprice, payprice,pf ) td ON tc.pid = td.pid AND tc.uid = td.uid ) te WHERE te.oid > 0 ) tf WHERE tf.rk = 1 AND tf.sptype = '{5}' GROUP BY	spid,pf ".format(
        lastdate, date, lastdate, date, date, sptype)
    dataarr = getSqlResult(sql)
    if dataarr[0] == '':
        return datadict
    else:
        for i in range(0, len(dataarr)):
            if len(dataarr[i]) == 0:
                continue
            else:
                datajson = json.loads(dataarr[i])
            spid = str(datajson['spid'])
            pf = str(datajson['pf'])
            key = pf + sptype + spid
            if key in datadict:
                pidinfo = datadict[key]
            else:
                pidinfo = initDataDict(pf, sptype, spid)
                k = pf + sptype + spid
                datadict[k] = pidinfo
            pv_order_product = datajson['pv_order_product']
            uv_order = datajson['uv_order']
            totalsp = datajson['totalsp']
            totalcp = datajson['totalcp']
            totalpp = datajson['totalpp']
            pidinfo['order_product_pv'] = pv_order_product
            pidinfo['order_uv'] = uv_order
            pidinfo['order_total_saleprice'] = totalsp
            pidinfo['order_total_discountprice'] = totalcp
            pidinfo['order_total_realpayprice'] = totalpp
    return datadict


# 获取单日订单数
def getOrderNum(sptype, lastdate, date, datadict):
    sql = "SELECT spid,pf, count(distinct(tf.oid)) as pv_order FROM (SELECT te.uid, te.pf,te.pid, te.oid, te.time, te.sptype, te.spid, te.saleprice, te.countprice, te.payprice, row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time DESC ) rk FROM (SELECT	tc.pid, tc.time, tc.sptype, td.oid, tc.uid, tc.spid, td.saleprice, td.countprice, td.payprice,td.pf FROM (SELECT ta.sid, ta.pid, ta.time, tb.sptype, tb.spid, ta.uid FROM (SELECT	sid, productid AS pid, time, distinct_ID AS uid FROM EVENTS WHERE	EVENT = 'AddCart' AND sid != '' AND date BETWEEN  '{0}' AND '{1}' GROUP BY sid, pid, time, uid ) ta LEFT JOIN (SELECT sid,	src_page_type AS sptype, src_page_id AS spid FROM EVENTS WHERE EVENT IN ( 'ViewProductList', 'ViewProduct' )	AND date BETWEEN  '{2}' AND '{3}' AND sid != '' GROUP BY sid, sptype, spid) tb ON ta.sid = tb.sid ) tc LEFT JOIN (SELECT plumfrontend AS pf,orderid AS oid, productid AS pid, distinct_ID AS uid, orderitem_saleprice AS saleprice, orderitem_discountprice AS countprice, orderitem_realpayprice AS payprice FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}'  GROUP BY oid, pid, uid, saleprice, countprice, payprice,pf ) td ON tc.pid = td.pid AND tc.uid = td.uid ) te WHERE te.oid > 0 ) tf WHERE tf.rk = 1 AND tf.sptype = '{5}' GROUP BY	spid,pf ".format(
        lastdate, date, lastdate, date, date, sptype)
    dataarr = getSqlResult(sql)
    if dataarr[0] == '':
        return datadict
    else:
        for i in range(0, len(dataarr)):
            if len(dataarr[i]) == 0:
                continue
            else:
                datajson = json.loads(dataarr[i])
            spid = str(datajson['spid'])
            pf = str(datajson['pf'])
            key = pf + sptype + spid
            if key in datadict:
                pidinfo = datadict[key]
            else:
                pidinfo = initDataDict(pf, sptype, spid)
                k = pf + sptype + spid
                datadict[k] = pidinfo
            pv_order = datajson['pv_order']
            pidinfo['order_pv'] = pv_order
    return datadict


# 获取数据集的字典
def getDataDict(pf, sptype, lastdate, date, datadict):
    datadict = getShowInfo(pf, sptype, date, datadict)
    if pf == platformlist[len(platformlist) - 1]:
        datadict = getListInfo(sptype, date, datadict)
        datadict = getDetailInfo(sptype, date, datadict)
        datadict = getDetailDistinctAndSidInfo(sptype, date, datadict)
        datadict = getWIshInfo(sptype,date,datadict)
        datadict = getAddCartInfo(sptype, date, datadict)
        datadict = getListAddCartInfo(sptype, date, datadict)
        datadict = getOrderProductInfo(sptype, lastdate, date, datadict)
        datadict = getOrderNum(sptype, lastdate, date, datadict)
    return datadict


if __name__ == '__main__':
    try:

        if len(sys.argv) == 1:
            today = date.today()
            date = today + timedelta(days=-1)
        else:
            datestr = sys.argv[1]
            date = datetime.strptime(datestr, '%Y-%m-%d').date()
            today = date + timedelta(days=1)
        lastdate = date + timedelta(days=-7)
        datestr = date.strftime("%Y-%m-%d")
        print(date, today)
        typelist = typedict.keys()
        datadict_total = dict()
        for pf in platformlist:
            for sptype in typelist:
                if sptype == 'all':
                    continue
                print('======{}-{}======'.format(pf, sptype))
                datadict = getDataDict(pf, sptype, lastdate, date, datadict_total)
            # print(datadict)
        # print(datadict_total)
        engine = create_engine(
            "mysql+pymysql://{0}:{1}@{2}/{3}?charset={4}".format(statmysqlusername, statmysqlpasswd, statmysqlhost,
                                                                 statdb,
                                                                 'utf8'))
        con = engine.connect()  # 创建连接
        dfa = pd.DataFrame(list(datadict_total.values()),
                           columns=['stat_date', 'platform', 'src_page_type', 'src_page_group', 'src_page_id',
                                    'show_uv', 'show_pv', 'show_total_saleprice', 'show_total_discountprice', 'list_uv',
                                    'list_pv', 'detail_uv', 'detail_pv', 'detail_distinct_pv', 'detail_sid_pv',
                                    'detail_total_saleprice', 'detail_total_discountprice',
                                    'detail_distinct_total_saleprice', 'detail_distinct_total_discountprice', 'cart_uv',
                                    'cart_pv', 'cart_total_saleprice', 'cart_total_discountprice', 'cart_list_uv',
                                    'cart_list_pv', 'cart_list_total_saleprice', 'cart_list_total_discountprice',
                                    'wish_uv', 'wish_pv', 'wish_total_saleprice', 'wish_total_discountprice',
                                    'order_uv', 'order_pv', 'order_product_pv', 'order_total_saleprice',
                                    'order_total_discountprice', 'order_total_realpayprice'])

        print(dfa.head())
        totalnum = dfa.shape[0]
        EachInsert = 5000
        times = int(totalnum / EachInsert + 1)
        for t in range(int(times)):
            pidStr = []
            start = t * EachInsert
            end = (t + 1) * EachInsert
            # print(start, end)
            dfa[start:end].to_sql(name='spid_ctr_daily', con=con, if_exists='append', index=False)
        con.close()
        print('数据写入成功')
        filename = './' + "spidctr_{}.csv".format(datestr)
        dfa.to_csv(filename, index=True, sep=',',
                   columns=['stat_date', 'platform', 'src_page_type', 'src_page_group', 'src_page_id',
                            'show_uv', 'show_pv', 'show_total_saleprice', 'show_total_discountprice', 'list_uv',
                            'list_pv', 'detail_uv', 'detail_pv', 'detail_distinct_pv', 'detail_sid_pv',
                            'detail_total_saleprice', 'detail_total_discountprice',
                            'detail_distinct_total_saleprice', 'detail_distinct_total_discountprice', 'cart_uv',
                            'cart_pv', 'cart_total_saleprice', 'cart_total_discountprice', 'cart_list_uv',
                            'cart_list_pv', 'cart_list_total_saleprice', 'cart_list_total_discountprice',
                            'wish_uv', 'wish_pv', 'wish_total_saleprice', 'wish_total_discountprice',
                            'order_uv', 'order_pv', 'order_product_pv', 'order_total_saleprice',
                            'order_total_discountprice', 'order_total_realpayprice'])
        print("current time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print('Ended Polling: %s' % tic())
        addr_list = ['suzerui@aplum.com.cn']
        sendMain(addr_list, filename)
    except Exception as e:
        msg = traceback.format_exc()
        print(" spid_tomysql accurs error, the error:%s" % str(msg))
        userlist = ['suzerui@aplum.com']
        err = "spid单日ctr数据入库出错"
        alarm(userlist, err)
