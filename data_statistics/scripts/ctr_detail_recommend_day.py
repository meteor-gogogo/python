#!/usr/bin/env python
# coding=utf-8
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta, date
import requests
import time
import json
import xlsxwriter
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import os
import sys
import traceback
from pyecharts.charts import Page, Bar, Pie, Grid
from pyecharts.globals import ThemeType
from pyecharts import options as opts

es = Elasticsearch(["es-cn-0pp0vm2ju000a003g.elasticsearch.aliyuncs.com"], http_auth=('elastic', 'NHRHpqeFn8'),
                   port=9200)
url = "http://websa.aplum-inc.com/api/sql/query?token=15ba27be5b7895379a0574993db783281988c16f95969fe423847d31ed95728d"
starttime = time.time()
tic = lambda: 'at %1.1f seconds' % (time.time() - starttime)

namedict = {'stat_date': "日期", 'type': "类型", 'show_uv': "曝光uv", 'show_pv': "曝光量", 'list_uv': "列表uv", 'list_pv': "列表pv",
            'detail_uv': "详情uv", 'detail_pv': "详情页pv(不滤重)", 'detail_distinct_pv': "详情PV(滤重同一商品)",
            'detail_sid_pv': "详情(滤重pv)", 'detail_total_saleprice': '详情商品在售总价', 'detail_total_discountprice': '详情商品折扣总价',
            'detail_distinct_total_saleprice': '详情商品在售总价(滤重同一商品)',
            'detail_distinct_total_discountprice': '详情商品在售总价(滤重同一商品)', 'cart_list_uv': "列表加购uv",
            'cart_list_pv': "列表加购pv", 'cart_list_total_saleprice': "列表加购商品在售总价",
            'cart_list_total_discountprice': "列表加购商品折扣总价", 'cart_uv': "加购uv", 'cart_pv': "加购pv",
            'cart_total_saleprice': "加购在售总价", 'cart_total_discountprice': "加购折扣总价", 'order_uv': "订单uv",
            'order_pv': "单日订单数", 'order_product_pv': "订单商品数", 'order_total_saleprice': "单日订单商品在售总价",
            'order_total_discountprice': "单日订单商品折扣总价", 'order_total_realpayprice': "订单实付总价"}
recommendspiddict = {'index': "首页猜你喜欢", 'brand': "详情页品牌信息", 'morelike': "详情页为你推荐", 'detail': "详情页看了又看",
                     'blackcard': "黑卡页面列表", 'home': "我的页面猜你喜欢",
                     'similar': '购物车看相似', 'cart': "购物车页面推荐列表"}


def sendMain(addressList, file1,file2):
    sender = 'aplumctr@aplum-inc.com'  # 发送邮件的邮箱地址
    user = 'aplumctr@aplum-inc.com'  # 发送邮件的邮箱地址
    passwd = 'qZdCAf3MCDv8SXi'  # 发送邮件的邮箱密码
    smtpserver = 'smtp.aplum-inc.com'  # 发送邮件的邮件服务器
    receivers = []  # 接收邮件的邮箱地址
    subject = "昨日推荐ctr数据"

    filePath = './' + '{}'.format(file1)
    baseName = os.path.basename(filePath)
    attachdisposition = "attachment;filename =%s" % baseName  # 发送附件html格式
    sendfile = open(filePath, 'rb').read()
    att = MIMEText(sendfile, 'plain', 'utf-8')
    att["Content-Type"] = "application/octet-stream"
    att["Content-Disposition"] = attachdisposition  # 发送附件html格式
    filePath2 = './' + '{}'.format(file2)
    baseName2 = os.path.basename(filePath2)
    attachdisposition2 = "attachment;filename =%s" % baseName2  # 发送附件html格式
    sendfile2 = open(filePath2, 'rb').read()
    att2 = MIMEText(sendfile2, 'plain', 'utf-8')
    att2["Content-Type"] = "application/octet-stream"
    att2["Content-Disposition"] = attachdisposition2  # 发送附件html格式
    msgText = MIMEText('您好，昨日推荐ctr数据见附件', 'plain', 'utf-8')

    for addr in addressList:
        receivers.append(addr)
    receiver = ';'.join(receivers)
    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = subject
    msgRoot['From'] = sender
    msgRoot['To'] = receiver
    msgRoot.attach(msgText)  # 添加正文
    msgRoot.attach(att)  # 发送文件格式
    msgRoot.attach(att2)  # 发送文件格式
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
        os.remove(filePath2)
        smtp.quit()


def getEsResult(date, doc):
    p = dict()
    p['request_timeout'] = 30
    return es.search(index='aplum_ctr-{}'.format(date), doc_type='product', body=doc, params=p)


def getSqlResult(sql):
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        dataarr = datastr.split("\n")
        return dataarr
    else:
        print("sa hive sql accur error, sql为{}".format(sql))


def initDataDict(spid):
    pidinfo = dict()
    pidinfo['src_page_id'] = spid
    pidinfo['show_uv'] = 0
    pidinfo['uv_show_imei'] = 0
    pidinfo['uv_show_idfa'] = 0
    pidinfo['show_pv'] = 0
    pidinfo['list_uv'] = 0
    pidinfo['list_pv'] = 0
    pidinfo['detail_uv'] = 0
    pidinfo['detail_pv'] = 0
    pidinfo['detail_distinct_pv'] = 0
    pidinfo['detail_sid_pv'] = 0
    pidinfo['detail_total_saleprice'] = 0.0
    pidinfo['detail_total_discountprice'] = 0.0
    pidinfo['detail_distinct_total_saleprice'] = 0.0
    pidinfo['detail_distinct_total_discountprice'] = 0.0
    pidinfo['cart_uv'] = 0
    pidinfo['cart_pv'] = 0
    pidinfo['cart_total_saleprice'] = 0.0
    pidinfo['cart_total_discountprice'] = 0.0
    pidinfo['cart_list_uv'] = 0
    pidinfo['cart_list_pv'] = 0
    pidinfo['cart_list_total_saleprice'] = 0.0
    pidinfo['cart_list_total_discountprice'] = 0.0
    pidinfo['order_uv'] = 0
    pidinfo['order_pv'] = 0
    pidinfo['order_product_pv'] = 0
    pidinfo['order_total_saleprice'] = 0.0
    pidinfo['order_total_discountprice'] = 0.0
    pidinfo['order_total_realpayprice'] = 0.0
    return pidinfo


# 获取曝光量, 返回一个字典
def getShowPv(sptype, date):
    spiddict = dict()
    esdoc = {
        "size": 0,
        "query": {
            "bool": {
                "must_not": [
                    {"term": {
                        "Src_page_type.keyword": ""
                    }}
                ],
                "must": [
                    {
                        "term": {
                            "Src_page_type.keyword": sptype
                        }
                    },
                    {
                        "terms": {
                            "X-Pd-Os.keyword": [
                                "ios_app",
                                "android_app",
                                "ios_app_native",
                                "android_app_native"
                            ]
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
    searched = getEsResult(date, esdoc)
    totalnum = searched['aggregations']['idcount']['value']
    size = 5000
    if totalnum <= size:
        pagesize = 1
    else:
        pagesize = int(totalnum / size + 2)
    for num in range(0, pagesize):
        spiddoc = {
            "size": 0,
            "query": {
                "bool": {
                    "must_not": [
                        {"term": {
                            "Src_page_type.keyword": ""
                        }}
                    ],
                    "must": [
                        {"term": {
                            "Src_page_type.keyword": sptype
                        }},
                        {
                            "terms": {
                                "X-Pd-Os.keyword": [
                                    "ios_app",
                                    "android_app",
                                    "ios_app_native",
                                    "android_app_native"
                                ]
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
                        "size": size
                    }
                }
            }
        }
        spidsearched = getEsResult(date, spiddoc)
        buckets = spidsearched['aggregations']['idcount']['buckets']
        for b in buckets:
            spid = str(b['key'])
            hitnum = int(b['doc_count'])
            spidinfo = initDataDict(spid)
            spidinfo['show_pv'] = hitnum
            spiddict[spid] = spidinfo
    return spiddict


# 获取曝光uv
def getPidShowUv(sptype, date, totalnum, datadict):
    size = 1000
    if totalnum <= size:
        pagesize = 1
    else:
        pagesize = int(totalnum / size + 2)
    devicelist = ['X-Pd-Imei', 'X-Pd-Idfa']
    for devicetype in devicelist:
        aggfield = devicetype + ".keyword"
        for num in range(0, pagesize):
            spiddoc = {
                "size": 0,
                "query": {
                    "bool": {
                        "must_not": [
                            {
                                "term": {
                                    aggfield: ""
                                }
                            }
                        ],
                        "must": [
                            {
                                "term": {
                                    "Src_page_type.keyword": sptype
                                }
                            },
                            {
                                "terms": {
                                    "X-Pd-Os.keyword": [
                                        "ios_app",
                                        "android_app",
                                        "ios_app_native",
                                        "android_app_native"
                                    ]
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
                            "size": 2000
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

            spidsearched = getEsResult(date, spiddoc)
            buckets = spidsearched['aggregations']['idcount']['buckets']
            for b in buckets:
                spid = str(b['key'])
                uv_show = int(b['DEV']['value'])
                if spid in datadict:
                    pidinfo = datadict[spid]
                    if devicetype == 'X-Pd-Imei':
                        pidinfo['uv_show_imei'] = uv_show
                    elif devicetype == 'X-Pd-Idfa':
                        pidinfo['uv_show_idfa'] = uv_show
    return datadict


# 获取单日各详细分类的列表pv及uv
def getListInfo(sptype, date, datadict):
    sql = "SELECT  src_page_id as spid, COUNT(*) AS pv_list, COUNT( DISTINCT ( distinct_ID ) ) AS uv_list FROM EVENTS WHERE EVENT = 'ViewProductList' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) AND date = '{0}' AND sid != '' AND src_page_type = '{1}'  GROUP BY spid".format(
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
            if spid in datadict:
                pidinfo = datadict[spid]
            else:
                pidinfo = initDataDict(spid)
                datadict[spid] = pidinfo
            pv_list = datajson['pv_list']
            uv_list = datajson['uv_list']
            pidinfo['list_pv'] = pv_list
            pidinfo['list_uv'] = uv_list
            pidinfo['show_uv'] = pidinfo['uv_show_imei'] + pidinfo['uv_show_idfa']
    return datadict


# 获取单日各详细分类的详情信息,含不去重uv,pv,在售总价及折扣总价等
def getDetailInfo(sptype, date, datadict):
    sql = "SELECT  src_page_id as spid, COUNT(*) AS pv_detail, COUNT( DISTINCT ( distinct_ID ) ) AS uv_detail, SUM(productsale_price) AS totalsp,SUM(productdiscount_price) AS totalcp FROM EVENTS WHERE EVENT = 'ViewProduct' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) AND date = '{0}' AND sid != '' AND src_page_type = '{1}'  GROUP BY spid".format(
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
            if spid in datadict:
                pidinfo = datadict[spid]
            else:
                pidinfo = initDataDict(spid)
                datadict[spid] = pidinfo
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
    sql = "SELECT spid, count(*) as pv_detail_distinct,count(distinct(ta.sid)) AS pv_detail_sid,sum(sp) as totalsp,sum(cp) as totalcp FROM (SELECT sid, distinct_ID as uid, productid as pid, src_page_id as spid, productdiscount_price as sp, productsale_price as cp FROM EVENTS WHERE EVENT = 'ViewProduct' AND date = '{}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) AND src_page_type = '{}'  GROUP BY sid,spid,uid,productid,sp,cp) ta GROUP BY spid".format(
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
            if spid in datadict:
                pidinfo = datadict[spid]
            else:
                pidinfo = initDataDict(spid)
                datadict[spid] = pidinfo
            pv_detail_distinct = datajson['pv_detail_distinct']
            pv_detail_sid = datajson['pv_detail_sid']
            totalsp = datajson['totalsp']
            totalcp = datajson['totalcp']
            pidinfo['detail_distinct_pv'] = pv_detail_distinct
            pidinfo['detail_sid_pv'] = pv_detail_sid
            pidinfo['detail_distinct_total_saleprice'] = totalsp
            pidinfo['detail_distinct_total_discountprice'] = totalcp
    return datadict


# 获取单日各详细分类的列表加购信息,含uv,pv,在售总价及折扣总价等
def getListAddCartInfo(sptype, date, datadict):
    sql = "SELECT tc.spid,count(tc.sid) as pv_cart_list,count(distinct(tc.uid)) as uv_cart_list,sum(tc.sp) as totalsp,sum(tc.cp) as totalcp FROM (SELECT ta.sid,ta.uid,ta.sp,ta.cp,tb.spid FROM (SELECT sid, distinct_ID as uid, productid AS pid, productsale_price as sp, productdiscount_price as cp FROM EVENTS WHERE EVENT = 'AddCart' AND src_page_type = 'list' AND sid != '' AND date = '{0}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, pid, sp,cp,uid ) ta LEFT JOIN (SELECT sid, distinct_ID as uid, src_page_type AS sptype, src_page_id AS spid FROM EVENTS WHERE EVENT IN ( 'ViewProduct', 'ViewProductList' ) AND date = '{1}' AND sid != '' AND src_page_type = '{2}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,uid, sptype, spid ) tb ON ta.sid = tb.sid AND ta.uid = tb.uid) tc WHERE tc.spid != '' GROUP BY tc.spid ".format(
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
            if spid in datadict:
                pidinfo = datadict[spid]
            else:
                pidinfo = initDataDict(spid)
                datadict[spid] = pidinfo
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
    sql = "SELECT tc.spid,count(tc.sid) as pv_cart,count(distinct(tc.uid)) as uv_cart,sum(tc.sp) as totalsp,sum(tc.cp) as totalcp FROM (SELECT ta.sid,ta.uid,ta.sp,ta.cp,tb.spid FROM (SELECT sid, distinct_ID as uid, productid AS pid, productsale_price as sp, productdiscount_price as cp FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date = '{0}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, pid, sp, cp,uid ) ta LEFT JOIN (SELECT sid, distinct_ID as uid, src_page_type AS sptype, src_page_id AS spid FROM EVENTS WHERE EVENT IN ( 'ViewProduct', 'ViewProductList' ) AND date = '{1}' AND sid != '' AND src_page_type = '{2}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,uid, sptype, spid ) tb ON ta.sid = tb.sid AND ta.uid = tb.uid) tc WHERE tc.spid != '' GROUP BY tc.spid ".format(
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
            if spid in datadict:
                pidinfo = datadict[spid]
            else:
                pidinfo = initDataDict(spid)
                datadict[spid] = pidinfo
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
    sql = "SELECT spid, count( * ) AS pv_order_product,count(distinct(tf.uid)) as uv_order,sum( tf.saleprice ) AS totalsp,	sum( tf.countprice ) AS totalcp, sum( tf.payprice ) AS totalpp FROM (SELECT te.uid, te.pid, te.oid, te.time, te.sptype, te.spid,te.saleprice, te.countprice, te.payprice, row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time DESC ) rk FROM (SELECT tc.pid, tc.time, tc.sptype, td.oid, tc.uid, tc.spid, td.saleprice, td.countprice, td.payprice FROM (SELECT ta.sid, ta.pid, ta.time, tb.sptype, tb.spid, ta.uid FROM (SELECT	sid, productid AS pid, time, distinct_ID AS uid FROM EVENTS WHERE	EVENT = 'AddCart' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) AND date BETWEEN  '{0}' AND '{1}' GROUP BY sid, pid, time, uid ) ta LEFT JOIN (SELECT sid,	src_page_type AS sptype, src_page_id AS spid FROM EVENTS WHERE EVENT IN ( 'ViewProductList', 'ViewProduct' ) AND date BETWEEN  '{2}' AND '{3}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) AND sid != '' GROUP BY sid, sptype, spid) tb ON ta.sid = tb.sid ) tc LEFT JOIN (SELECT orderid AS oid, productid AS pid, distinct_ID AS uid, orderitem_saleprice AS saleprice, orderitem_discountprice AS countprice, orderitem_realpayprice AS payprice FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' GROUP BY oid, pid, uid, saleprice, countprice, payprice ) td ON tc.pid = td.pid AND tc.uid = td.uid ) te WHERE te.oid > 0 ) tf WHERE tf.rk = 1 AND tf.sptype = '{5}' GROUP BY spid ".format(
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
            if spid in datadict:
                pidinfo = datadict[spid]
            else:
                pidinfo = initDataDict(spid)
                datadict[spid] = pidinfo
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
    sql = "SELECT spid,count(distinct(tf.oid)) as pv_order FROM (SELECT te.uid, te.pid, te.oid, te.time, te.sptype, te.spid, te.saleprice, te.countprice, te.payprice, row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time DESC ) rk FROM (SELECT	tc.pid, tc.time, tc.sptype, td.oid, tc.uid, tc.spid, td.saleprice, td.countprice, td.payprice FROM (SELECT ta.sid, ta.pid, ta.time, tb.sptype, tb.spid, ta.uid FROM (SELECT sid, productid AS pid, time, distinct_ID AS uid FROM EVENTS WHERE	EVENT = 'AddCart' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) AND date BETWEEN  '{0}' AND '{1}' GROUP BY sid, pid, time, uid ) ta LEFT JOIN (SELECT sid,	src_page_type AS sptype, src_page_id AS spid FROM EVENTS WHERE EVENT IN ( 'ViewProductList', 'ViewProduct' ) AND date BETWEEN  '{2}' AND '{3}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) AND sid != '' GROUP BY sid, sptype, spid) tb ON ta.sid = tb.sid ) tc LEFT JOIN (SELECT orderid AS oid, productid AS pid, distinct_ID AS uid, orderitem_saleprice AS saleprice, orderitem_discountprice AS countprice, orderitem_realpayprice AS payprice FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) AND date = '{4}'  GROUP BY oid, pid, uid, saleprice, countprice, payprice ) td ON tc.pid = td.pid AND tc.uid = td.uid ) te WHERE te.oid > 0 ) tf WHERE tf.rk = 1 AND tf.sptype = '{5}' GROUP BY spid ".format(
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
            if spid in datadict:
                pidinfo = datadict[spid]
            else:
                pidinfo = initDataDict(spid)
                datadict[spid] = pidinfo
            pv_order = datajson['pv_order']
            pidinfo['order_pv'] = pv_order
    return datadict


def getXlsxFmt():
    name = workbook.add_format(
        {'font_name': '微软雅黑', 'font_size': 10, 'border': 1, 'bold': True, 'font_color': 'white'})
    detail = workbook.add_format({'font_name': '微软雅黑', 'font_size': 10, 'border': 1})
    money = workbook.add_format(
        {'num_format': '_(¥* #,##0_);_(¥* (#,##0);_(¥* "-"??_);_(@_)', 'font_name': '微软雅黑', 'font_size': 10,
         'border': 1})
    name.set_bg_color('#538DD5')
    name.set_text_wrap()
    return name, detail, money


def getNumFmt(k, detail, money):
    if 'price' in k:
        style = money
    else:
        style = detail
    return style


def createRecommendInfo(datadict):
    sheet = workbook.add_worksheet('{} 推荐数据'.format(datestr))
    sheet.freeze_panes(0, 2)
    sheet.set_column('A:A', 10)
    sheet.set_column('B:B', 18)
    sheet.set_column('C:AD', 13)
    name, detail, money = getXlsxFmt()
    sheet.write('A1', 'spid', name)
    sheet.write('B1', '名称', name)
    i = 0
    for spid in datadict:
        pidinfo = datadict[spid]
        spidname = recommendspiddict[spid]
        sheet.write(i + 1, 0, spid, name)
        sheet.write(i + 1, 1, spidname, name)
        j = 2
        for k in pidinfo:
            if k == 'src_page_id' or k == 'uv_show_imei' or k == 'uv_show_idfa':
                continue
            if i == 0:
                sheet.write(i, j, namedict[k], name)
            style = getNumFmt(k, detail, money)
            sheet.write(i + 1, j, pidinfo[k], style)
            j = j + 1
        i = i + 1


def createRecommendSepInfo(datadict):
    name, detail, money = getXlsxFmt()
    for spid in datadict:
        pidinfo = datadict[spid]
        spidname = recommendspiddict[spid]
        sheet = workbook.add_worksheet(spidname)
        sheet.set_column('A:AD', 13)
        sheet.write(0, 0, '日期', name)
        sheet.write(1, 0, datestr, detail)
        j = 1
        for k in pidinfo:
            if k == 'src_page_id' or k == 'uv_show_imei' or k == 'uv_show_idfa':
                continue
            sheet.write(0, j, namedict[k], name)
            style = getNumFmt(k, detail, money)
            sheet.write(1, j, pidinfo[k], style)
            j = j + 1


def createEcharts(datadict):
    page = Page()
    bar = Bar(init_opts=opts.InitOpts(theme=ThemeType.CHALK))
    pie_show = Pie(init_opts=opts.InitOpts(theme=ThemeType.CHALK))
    pie_list = Pie(init_opts=opts.InitOpts(theme=ThemeType.CHALK))
    pie_detail = Pie(init_opts=opts.InitOpts(theme=ThemeType.CHALK))
    pie_cart = Pie(init_opts=opts.InitOpts(theme=ThemeType.CHALK))
    pie_order_product = Pie(init_opts=opts.InitOpts(theme=ThemeType.CHALK))
    spidlist = list()
    show_list = list()
    list_list = list()
    detail_list = list()
    cart_list = list()
    order_product_list = list()
    show_dict = dict()
    list_dict = dict()
    detail_dict = dict()
    cart_dict = dict()
    order_product_dict = dict()
    for spid in datadict:
        spidlist.append(spid)
        show_list.append(datadict[spid]['show_pv'])
        list_list.append(datadict[spid]['list_pv'])
        detail_list.append(datadict[spid]['detail_pv'])
        cart_list.append(datadict[spid]['cart_pv'])
        order_product_list.append(datadict[spid]['order_product_pv'])
        show_dict[spid] = datadict[spid]['show_pv']
        list_dict[spid] = datadict[spid]['list_pv']
        detail_dict[spid] = datadict[spid]['detail_pv']
        cart_dict[spid] = datadict[spid]['cart_pv']
        order_product_dict[spid] = datadict[spid]['order_product_pv']
    bar.add_xaxis(spidlist)
    bar.add_yaxis('曝光', show_list).add_yaxis('列表',list_list).add_yaxis('详情',detail_list).add_yaxis('加购', cart_list).add_yaxis('订单商品', order_product_list)
    bar.set_global_opts(title_opts=opts.TitleOpts(title="曝光->订单") )
    bar.set_series_opts(label_opts=opts.LabelOpts(is_show=False), markpoint_opts=opts.MarkPointOpts(
        data=[opts.MarkPointItem(type_="max", name="最大值"), opts.MarkPointItem(type_="min", name="最小值"), ]))
    pie_show.add("曝光", [list(z) for z in zip(show_dict.keys(), show_dict.values())])
    pie_show.set_global_opts(title_opts=opts.TitleOpts("曝光数据"))
    pie_list.add("列表", [list(z) for z in zip(list_dict.keys(), list_dict.values())])
    pie_list.set_global_opts(title_opts=opts.TitleOpts("列表数据"))
    pie_detail.add("详情", [list(z) for z in zip(detail_dict.keys(), detail_dict.values())])
    pie_detail.set_global_opts(title_opts=opts.TitleOpts("详情数据"))
    pie_cart.add("加购", [list(z) for z in zip(cart_dict.keys(), cart_dict.values())])
    pie_cart.set_global_opts(title_opts=opts.TitleOpts("加购数据"))
    pie_order_product.add("订单商品", [list(z) for z in zip(order_product_dict.keys(), order_product_dict.values())])
    pie_order_product.set_global_opts(title_opts=opts.TitleOpts("订单商品数"))
    page.add(bar).add(pie_show).add(pie_list).add(pie_detail).add(pie_cart).add(pie_order_product)
    page.render('recommend.html')


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
        print(lastdate, date, today)
        print('======推荐页商品单日详细CTR ======')
        datestr = date.strftime("%Y-%m-%d")
        datadict = getShowPv('recommend', datestr)
        totalnum = len(datadict)
        datadict = getPidShowUv('recommend', datestr, totalnum, datadict)
        datadict = getListInfo('recommend', datestr, datadict)
        datadict = getDetailInfo('recommend', datestr, datadict)
        datadict = getDetailDistinctAndSidInfo('recommend', datestr, datadict)
        datadict = getListAddCartInfo('recommend', datestr, datadict)
        datadict = getAddCartInfo('recommend', datestr, datadict)
        datadict = getOrderProductInfo('recommend', lastdate, datestr, datadict)
        datadict = getOrderNum('recommend', lastdate, datestr, datadict)
        print(datadict)
        filename = "ctr_recommend_%s.xlsx" % datestr
        workbook = xlsxwriter.Workbook(filename)
        createRecommendSepInfo(datadict)
        createEcharts(datadict)
        workbook.close()

        print("current time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print('Ended Polling: %s' % tic())
        addr_list = ['liudiyuhan@aplum.com.cn','liwenlong@aplum.com.cn', 'pangbo@aplum.com.cn', 'suzerui@aplum.com.cn']
        sendMain(addr_list, filename,'recommend.html')

    except Exception as e:
        msg = traceback.format_exc()
        print(" delete index accurs error, the error:%s" % str(msg))
