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

es = Elasticsearch(["es-cn-0pp0vm2ju000a003g.elasticsearch.aliyuncs.com"], http_auth=('elastic', 'NHRHpqeFn8'),
                   port=9200)
url = "http://websa.aplum-inc.com/api/sql/query?token=15ba27be5b7895379a0574993db783281988c16f95969fe423847d31ed95728d"

start = time.time()
tic = lambda: 'at %1.1f seconds' % (time.time() - start)

namedict = {'shownum': "列表总曝光量", 'uv_list': "列表页uv", 'pv_list': "列表页pv", 'uv_detail': "详情页uv",
            'pv_detail': "详情页曝光pv(不滤重)", 'pv_detail_distinct': "详情页曝光PV(滤重同一商品)", 'pv_detail_sid': "详情页曝光PV(滤重pv)",
            'listaddcartnum': "单日列表加购数", 'addcartnum': "单日加购数", 'totalcartsp': "单日加购商品在售总价",
            'totalcartcp': "单日加购商品折扣总价", 'orderuv': "单日订单uv", 'ordernum': "单日订单数", 'payordernum': "单日订单商品数",
            'totalordersp': "单日订单商品在售总价", 'totalordercp': "单日订单商品折扣总价", 'totalorderpp': "单日订单商品实付总价",
            'listpercent': "流量占比", 'shownumpercent': "曝光占比", 'ordernumpercent': "订单占比",
            'detailtocartpercent': "详情页到达购物车", 'showtocartpercent': "购物车到达率", 'ctrpercent': "曝光ctr",
            'pvcvrpercent': "点击CVR", 'show_eachuser': "浏览深度(用户查看商品数)", 'showcvrpercent': "曝光CVR",
            'rpm_total': "RPM千次展示GMV", 'gmv_total': "千次请求GMV",
            'uvvalue_total': "UV价值", 'ordervalue_total': "客单价"}

typedict = {'category': "分类页", 'activity': "活动页", 'search': "搜索页", 'brand': "品牌页", 'index': "首页",
            'other': '其他页', 'recommend': "推荐页", 'all': "总数"}


def sendMain(addressList, attachmentName):
    sender = 'aplum2016@163.com'  # 发送邮件的邮箱地址
    user = 'aplum2016@163.com'  # 发送邮件的邮箱地址
    passwd = '123qweASD'  # 发送邮件的邮箱密码
    smtpserver = 'smtp.163.com'  # 发送邮件的邮件服务器
    receivers = []  # 接收邮件的邮箱地址
    subject = "{} ctr统计".format(datestr)

    filePath = './%s' % attachmentName
    baseName = os.path.basename(filePath)
    attachdisposition = "attachment;filename =%s" % baseName  # 发送附件html格式
    sendfile = open(filePath, 'rb').read()
    att = MIMEText(sendfile, 'plain', 'utf-8')
    att["Content-Type"] = "application/octet-stream"
    att["Content-Disposition"] = attachdisposition  # 发送附件html格式
    msgText = MIMEText('您好，{} ctr数据见附件'.format(datestr), 'plain', 'utf-8')

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


# 获取单日商品总曝光量
def getTotalShowNum(date, sptype):
    if sptype == 'all':
        esdoc = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
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
                    ],
                    "must_not": [
                        {"term": {
                            "Src_page_type.keyword": ""
                        }}
                    ]
                }
            }
        }
    else:
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
            }
        }
    searched = getEsResult(date, esdoc)
    hitnum = searched['hits']['total']
    return hitnum


# 获取单日列表页uv总量
def getListUv(date, sptype):
    if sptype == 'all':
        sql = "SELECT COUNT(distinct(distinct_ID)) as uv_list FROM (SELECT src_page_type as sptype, distinct_ID, CASE WHEN src_page_type like '%_search%' THEN 'oldversion' ELSE src_page_type END as type FROM EVENTS WHERE EVENT = 'ViewProductList'  AND date = '{0}'  AND src_page_type != ''  AND plumfrontend IN ( 'IOS客户端', 'Android客户端' )) ta WHERE ta.type != 'oldversion'".format(
            date)
    else:
        sql = "SELECT COUNT(distinct(distinct_ID)) AS uv_list FROM EVENTS WHERE EVENT = 'ViewProductList' AND date ='{0}' AND src_page_type = '{1}' AND plumfrontend in ('IOS客户端','Android客户端') ".format(
            date, sptype)

    dataarr = getSqlResult(sql)
    datajson = json.loads(dataarr[0])
    if 'uv_list' in datajson:
        uv_list = datajson["uv_list"]
    else:
        uv_list = 0
    return uv_list


# 获取单日列表页pv总量
def getListPv(date, sptype):
    if sptype == 'all':
        sql = "SELECT COUNT(*) as pv_list FROM (SELECT src_page_type as sptype, CASE WHEN src_page_type like '%_search%' THEN 'oldversion' ELSE src_page_type END as type FROM EVENTS WHERE EVENT = 'ViewProductList'  AND date = '{0}'  AND src_page_type != ''  AND plumfrontend IN ( 'IOS客户端', 'Android客户端' )) ta WHERE ta.type != 'oldversion'".format(
            date)
    else:
        sql = "SELECT COUNT(src_page_type) AS pv_list FROM EVENTS WHERE EVENT = 'ViewProductList' AND date ='{0}' AND src_page_type = '{1}' AND plumfrontend in ('IOS客户端','Android客户端') ".format(
            date, sptype)
    dataarr = getSqlResult(sql)
    datajson = json.loads(dataarr[0])
    if 'pv_list' in datajson:
        pv_list = datajson["pv_list"]
    else:
        pv_list = 0
    return pv_list


# 获取单日详情页uv总量
def getDetailUv(date, sptype):
    if sptype == 'all':
        sql = "SELECT COUNT(distinct(distinct_ID)) as uv_detail FROM (SELECT src_page_type as sptype, distinct_ID, CASE WHEN src_page_type like '%_search%' THEN 'oldversion' ELSE src_page_type END as type FROM EVENTS WHERE EVENT = 'ViewProduct'  AND date = '{0}'  AND src_page_type != ''  AND plumfrontend IN ( 'IOS客户端', 'Android客户端' )) ta WHERE ta.type != 'oldversion'".format(
            date)
    else:
        sql = "SELECT COUNT(distinct(distinct_ID)) AS uv_detail FROM EVENTS WHERE EVENT = 'ViewProduct' AND date ='{0}' AND src_page_type = '{1}' AND plumfrontend in ('IOS客户端','Android客户端') ".format(
            date, sptype)
    dataarr = getSqlResult(sql)
    datajson = json.loads(dataarr[0])
    if 'uv_detail' in datajson:
        uv_detail = datajson['uv_detail']
    else:
        uv_detail = 0
    return uv_detail


# 获取单日详情页pv总量(不去重)
def getDetailPv(date, sptype):
    if sptype == 'all':
        sql = "SELECT COUNT(*) as pv_detail FROM (SELECT src_page_type as sptype, CASE WHEN src_page_type like '%_search%' THEN 'oldversion' ELSE src_page_type END as type FROM EVENTS WHERE EVENT = 'ViewProduct'  AND date = '{0}'  AND src_page_type != ''  AND plumfrontend IN ( 'IOS客户端', 'Android客户端' )) ta WHERE ta.type != 'oldversion'".format(
            date)
    else:
        sql = "SELECT COUNT(src_page_id) AS pv_detail FROM EVENTS WHERE EVENT = 'ViewProduct' AND date ='{0}' AND src_page_type = '{1}' AND plumfrontend in ('IOS客户端','Android客户端') ".format(
            date, sptype)
    dataarr = getSqlResult(sql)
    datajson = json.loads(dataarr[0])
    if 'pv_detail' in datajson:
        pv_detail = datajson['pv_detail']
    else:
        pv_detail = 0
    return pv_detail


# 获取单日详情页去重pv总量(去重单次曝光多次详情页点击的同一个商品)
def getDetailDistinctPv(date, sptype):
    if sptype == 'all':
        sql = "SELECT COUNT(*) as pv_detail_distinct FROM (SELECT tb.sid,tb.productid,tb.type FROM (SELECT * from (SELECT sid, productid, src_page_type as sptype, CASE WHEN src_page_type like '%_search%' THEN 'oldversion' ELSE src_page_type END as type FROM EVENTS WHERE EVENT = 'ViewProduct' AND date = '{0}' AND src_page_type != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' )) ta WHERE ta.type != 'oldversion') tb GROUP BY tb.sid, tb.productid,tb.type) tc".format(
            date)
    else:
        sql = "SELECT COUNT(*) as pv_detail_distinct FROM (SELECT sid, productid FROM events WHERE event='ViewProduct' and date = '{0}' and src_page_type = '{1}' and plumfrontend in ('IOS客户端','Android客户端') GROUP BY sid, productid) ta".format(
            date, sptype)
    dataarr = getSqlResult(sql)
    datajson = json.loads(dataarr[0])
    if 'pv_detail_distinct' in datajson:
        pv_detail_distinct = datajson['pv_detail_distinct']
    else:
        pv_detail_distinct = 0
    return pv_detail_distinct


# 获取单日详情页去重pv总量(单次次曝光的多次详情页点击只计算一次)
def getDetailSidPv(date, sptype):
    if sptype == 'all':
        sql = "SELECT COUNT(*) as pv_detail_sid FROM (SELECT ta.dsid,ta.type FROM  (SELECT  distinct(sid) as dsid, src_page_type AS sptype, CASE  WHEN src_page_type LIKE '%_search%' THEN 'oldversion' ELSE src_page_type  END AS type FROM EVENTS WHERE EVENT = 'ViewProduct'  AND date = '{0}' AND src_page_type != ''  AND plumfrontend IN ( 'IOS客户端', 'Android客户端' )  ) ta WHERE ta.type != 'oldversion' ) tb ".format(
            date)
    else:
        sql = "SELECT COUNT(distinct(sid)) AS pv_detail_sid FROM EVENTS WHERE EVENT = 'ViewProduct' AND date ='{0}' and src_page_type = '{1}' and plumfrontend in ('IOS客户端','Android客户端') ".format(
            date, sptype)
    dataarr = getSqlResult(sql)
    datajson = json.loads(dataarr[0])
    if 'pv_detail_sid' in datajson:
        pv_detail_sid = datajson['pv_detail_sid']
    else:
        pv_detail_sid = 0
    return pv_detail_sid


# 获取单日列表加购数量
def getListAddCartNum(date, sptype):
    if sptype == 'all':
        sql = " SELECT count( * ) AS listaddcartnum FROM (SELECT ta.sid, ta.pid, tb.sptype FROM ( SELECT sid, productid AS pid FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND src_page_type = 'list' AND date = '{0}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, pid ) ta LEFT JOIN ( SELECT sid, src_page_type AS sptype FROM EVENTS WHERE EVENT IN ('ViewProduct', 'ViewProductList' ) AND date = '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) AND sid != '' GROUP BY sid, sptype) tb ON ta.sid = tb.sid ) tc WHERE tc.sptype != '' ".format(
            date, date)
    else:
        sql = " SELECT tc.sptype AS type, count( tc.sptype ) AS listaddcartnum FROM( SELECT ta.sid, ta.pid, tb.sptype FROM ( SELECT sid, productid AS pid FROM EVENTS WHERE EVENT = 'AddCart' AND src_page_type = 'list' AND sid != '' AND date = '{0}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, pid ) ta LEFT JOIN ( SELECT sid, src_page_type AS sptype FROM EVENTS WHERE EVENT IN ( 'ViewProduct', 'ViewProductList' ) AND date = '{1}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, sptype) tb ON ta.sid = tb.sid) tc WHERE tc.sptype = '{2}' GROUP BY type".format(
            date, date, sptype)
    dataarr = getSqlResult(sql)
    datajson = json.loads(dataarr[0])
    if 'listaddcartnum' in datajson:
        listaddcartnum = datajson["listaddcartnum"]
    else:
        listaddcartnum = 0
    return listaddcartnum


# 获取单日加购数量及商品折扣总价
def getAddCartNumANDPrice(date, sptype):
    if sptype == 'all':
        sql = " SELECT count( * ) AS addcartnum, sum(tc.saleprice) as totalsp,sum(tc.countprice) as totalcp FROM (SELECT ta.sid, ta.pid, tb.sptype,ta.saleprice,ta.countprice FROM ( SELECT sid, productid AS pid,productsale_price as saleprice, productdiscount_price as countprice FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date = '{0}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, pid, saleprice, countprice ) ta LEFT JOIN ( SELECT sid, src_page_type AS sptype FROM EVENTS WHERE EVENT IN ( 'ViewProduct', 'ViewProductList' ) AND date = '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) AND sid != '' GROUP BY sid, sptype) tb ON ta.sid = tb.sid ) tc WHERE tc.sptype != '' ".format(
            date, date)
    else:
        sql = " SELECT tc.sptype AS type, count( tc.sptype ) AS addcartnum, sum(tc.saleprice) as totalsp,sum(tc.countprice) as totalcp FROM( SELECT ta.sid, ta.pid, tb.sptype, ta.saleprice,ta.countprice FROM ( SELECT sid, productid AS pid,productsale_price as saleprice, productdiscount_price as countprice FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date = '{0}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, pid, saleprice, countprice ) ta LEFT JOIN ( SELECT sid, src_page_type AS sptype FROM EVENTS WHERE EVENT IN ( 'ViewProduct', 'ViewProductList' ) AND date = '{1}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, sptype) tb ON ta.sid = tb.sid) tc WHERE tc.sptype = '{2}' GROUP BY type".format(
            date, date, sptype)
    dataarr = getSqlResult(sql)
    datajson = json.loads(dataarr[0])
    if 'addcartnum' in datajson:
        addcartnum = datajson["addcartnum"]
    else:
        addcartnum = 0
    if 'totalsp' in datajson:
        totalsp = datajson['totalsp']
    else:
        totalsp = 0
    if 'totalcp' in datajson:
        totalcp = datajson['totalcp']
    else:
        totalcp = 0
    return addcartnum, totalsp, totalcp


# 获取单日订单uv数
def getOrderUv(lastdate, date, sptype):
    if sptype == 'all':
        sql = " SELECT count(distinct(tg.uid)) as orderuv FROM (SELECT tf.sptype,tf.oid,tf.uid FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf GROUP BY tf.sptype,tf.oid,tf.uid) tg WHERE tg.sptype != '' ".format(
            lastdate, date, lastdate, date, date)
    else:
        sql = " SELECT count(distinct(tg.uid)) as orderuv FROM (SELECT tf.sptype,tf.oid,tf.uid FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf GROUP BY tf.sptype,tf.oid,tf.uid) tg WHERE tg.sptype = '{5}' ".format(
            lastdate, date, lastdate, date, date, sptype)
    dataarr = getSqlResult(sql)
    datajson = json.loads(dataarr[0])
    if 'orderuv' in datajson:
        orderuv = datajson["orderuv"]
    else:
        orderuv = 0
    return orderuv


# 获取单日订单数量(订单维度)
def getOrderNum(lastdate, date, sptype):
    if sptype == 'all':
        sql = " SELECT count(*) as ordernum FROM (SELECT tf.sptype,tf.oid,tf.uid FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf GROUP BY tf.sptype,tf.oid,tf.uid) tg WHERE tg.sptype != '' ".format(
            lastdate, date, lastdate, date, date)
    else:
        sql = " SELECT count(*) as ordernum FROM (SELECT tf.sptype,tf.oid,tf.uid FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf GROUP BY tf.sptype,tf.oid,tf.uid) tg WHERE tg.sptype = '{5}' ".format(
            lastdate, date, lastdate, date, date, sptype)
    dataarr = getSqlResult(sql)
    datajson = json.loads(dataarr[0])
    if 'ordernum' in datajson:
        ordernum = datajson["ordernum"]
    else:
        ordernum = 0
    return ordernum


# 获取单日订单数量(商品维度)及商品折扣总价和用户实付总价
def getPayOrderDetailNumANDPrice(lastdate, date, sptype):
    if sptype == 'all':
        sql = " SELECT count(tf.sptype) as payordernum, sum (tf.saleprice) as totalsp,sum(tf.countprice) as totalcp,sum(tf.payprice) as totalpp FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,te.saleprice,te.countprice,te.payprice, row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf,td.saleprice,td.countprice,td.payprice FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid,orderitem_saleprice as saleprice, orderitem_discountprice as countprice, orderitem_realpayprice as payprice FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid,saleprice,countprice,payprice) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf WHERE rk = 1 AND tf.sptype != ''".format(
            lastdate, date, lastdate, date, date)
    else:
        sql = " SELECT count(tf.sptype) as payordernum, sum(tf.saleprice) as totalsp,sum(tf.countprice) as totalcp,sum(tf.payprice) as totalpp FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,te.saleprice,te.countprice,te.payprice, row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf,td.saleprice,td.payprice,td.countprice FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid, orderitem_saleprice as saleprice, orderitem_discountprice as countprice, orderitem_realpayprice as payprice FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid,saleprice,countprice, payprice) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf WHERE rk = 1 AND tf.sptype = '{5}'".format(
            lastdate, date, lastdate, date, date, sptype)
    dataarr = getSqlResult(sql)
    datajson = json.loads(dataarr[0])
    if 'payordernum' in datajson:
        payordernum = datajson["payordernum"]
    else:
        payordernum = 0
    if 'totalsp' in datajson:
        totalsp = datajson['totalsp']
    else:
        totalsp = 0
    if 'totalcp' in datajson:
        totalcp = datajson['totalcp']
    else:
        totalcp = 0
    if 'totalpp' in datajson:
        totalpp = datajson['totalpp']
    else:
        totalpp = 0

    return payordernum, totalsp, totalcp, totalpp


def getGrowthDict(dict1, dict2, sptype):
    datadict = dict()
    detaildict = dict1[sptype]
    detaildict2 = dict2[sptype]
    ctrinfo = dict()
    for k in detaildict:
        if k in detaildict2:
            r = (float(detaildict2[k]) - float(detaildict[k])) / float(detaildict[k])
            ctrinfo[k] = r
    datadict[sptype] = ctrinfo
    return datadict


# 获取数据集的字典
def getDataDict(date, lastdate, sptype, pl_all, sh_all, pa_all):
    ctrdict = dict()
    ctrinfo = dict()
    date = date.strftime("%Y-%m-%d")
    shownum = getTotalShowNum(date, sptype)
    # shownum = 686688
    uv_list = getListUv(date, sptype)
    pv_list = getListPv(date, sptype)
    uv_detail = getDetailUv(date, sptype)
    pv_detail = getDetailPv(date, sptype)
    pv_detail_distinct = getDetailDistinctPv(date, sptype)
    pv_detail_sid = getDetailSidPv(date, sptype)
    listaddcartnum = getListAddCartNum(date, sptype)
    addcartnum, totalcartsp, totalcartcp = getAddCartNumANDPrice(date, sptype)
    time.sleep(2)
    orderuv = getOrderUv(lastdate, date, sptype)
    time.sleep(2)
    ordernum = getOrderNum(lastdate, date, sptype)
    time.sleep(2)
    payordernum, totalordersp, totalordercp, totalorderpp = getPayOrderDetailNumANDPrice(lastdate, date, sptype)
    listpercent = float(pv_list) / float(pl_all)
    shownumpercent = float(shownum) / float(sh_all)
    ordernumpercent = float(payordernum) / float(pa_all)
    detailtocartpercent = float(addcartnum) / float(pv_detail_distinct)
    showtocartpercent = float(addcartnum) / float(shownum)
    ctrpercent = float(pv_detail) / float(shownum)
    pvcvrpercent = float(payordernum) / float(pv_detail)
    showcvrpercent = float(payordernum) / float(shownum)
    show_eachuser = float(shownum) / float(uv_list)
    rpm_total = float(totalorderpp) / float(shownum) * 1000
    gmv_total = float(totalorderpp) / float(pv_list) * 1000
    uvvalue_total = float(totalorderpp) / float(uv_list)
    ordervalue_total = float(totalorderpp) / float(orderuv)

    ctrinfo['shownum'] = shownum
    ctrinfo['uv_list'] = uv_list
    ctrinfo['pv_list'] = pv_list
    ctrinfo['uv_detail'] = uv_detail
    ctrinfo['pv_detail'] = pv_detail
    ctrinfo['pv_detail_distinct'] = pv_detail_distinct
    ctrinfo['pv_detail_sid'] = pv_detail_sid
    ctrinfo['listaddcartnum'] = listaddcartnum
    # ctrinfo['addcartnum'], ctrinfo['totalcartsp'], ctrinfo['totalcartcp'] = addcartnum, totalcartsp, totalcartcp
    ctrinfo['addcartnum'], ctrinfo['totalcartcp'] = addcartnum, totalcartcp
    ctrinfo['orderuv'], ctrinfo['ordernum'] = orderuv, ordernum
    # ctrinfo['payordernum'], ctrinfo['totalordersp'], ctrinfo['totalordercp'], ctrinfo[
    #     'totalorderpp'] = payordernum, totalordersp, totalordercp, totalorderpp
    ctrinfo['payordernum'], ctrinfo['totalordercp'], ctrinfo['totalorderpp'] = payordernum, totalordercp, totalorderpp
    ctrinfo['listpercent'] = listpercent
    ctrinfo['shownumpercent'] = shownumpercent
    ctrinfo['ordernumpercent'] = ordernumpercent
    ctrinfo['detailtocartpercent'] = detailtocartpercent
    ctrinfo['showtocartpercent'] = showtocartpercent
    ctrinfo['ctrpercent'] = ctrpercent
    ctrinfo['pvcvrpercent'] = pvcvrpercent
    ctrinfo['showcvrpercent'] = showcvrpercent
    ctrinfo['show_eachuser'] = round(show_eachuser)
    ctrinfo['rpm_total'] = rpm_total
    ctrinfo['gmv_total'] = gmv_total
    ctrinfo['uvvalue_total'] = uvvalue_total
    ctrinfo['ordervalue_total'] = ordervalue_total

    ctrdict[sptype] = ctrinfo
    return ctrdict


def getXlsxFmt():
    name1 = workbook.add_format(
        {'font_name': '微软雅黑', 'font_size': 10, 'border': 1, 'bold': True, 'font_color': 'white'})
    name2 = workbook.add_format(
        {'font_name': '微软雅黑', 'font_size': 10, 'border': 1, 'bold': True, 'font_color': 'white'})
    name3 = workbook.add_format(
        {'font_name': '微软雅黑', 'font_size': 10, 'border': 1, 'bold': True, 'font_color': 'white'})
    detail = workbook.add_format({'font_name': '微软雅黑', 'font_size': 10, 'border': 1})
    money1 = workbook.add_format(
        {'num_format': '_(¥* #,##0_);_(¥* (#,##0);_(¥* "-"??_);_(@_)', 'font_name': '微软雅黑', 'font_size': 10,
         'border': 1})
    money2 = workbook.add_format(
        {'num_format': '_(¥* #,##0.00_);_(¥* (#,##0.00);_(¥* "-"??_);_(@_)', 'font_name': '微软雅黑', 'font_size': 10,
         'border': 1})
    percent = workbook.add_format({'num_format': '0.0000%', 'font_name': '微软雅黑', 'font_size': 10, 'border': 1})
    percent3 = workbook.add_format({'num_format': '0.00%', 'font_name': '微软雅黑', 'font_size': 10, 'border': 1})
    percent1 = workbook.add_format(
        {'num_format': '0.00%', 'font_name': '微软雅黑', 'font_size': 10, 'border': 1, 'font_color': 'green'})
    percent2 = workbook.add_format(
        {'num_format': '0.00%', 'font_name': '微软雅黑', 'font_size': 10, 'border': 1, 'font_color': '#D60000'})
    name1.set_bg_color('#C65911')
    name1.set_text_wrap()
    name2.set_bg_color('#305496')
    name2.set_text_wrap()
    name3.set_bg_color('#7030A0')
    name3.set_text_wrap()
    return name1, name2, name3, detail, money1, money2, percent, percent1, percent2, percent3


def getPercentFmt(num, fmt1, fmt2):
    if float(num) > 0:
        fmt = fmt2
    else:
        fmt = fmt1
    return fmt


def getNumFmt(k, j, money1, money2, percent, percent3, fmt3):
    if 'total' in k:
        if j > 22:
            fmt = money2
        else:
            fmt = money1
    elif 'percent' in k:
        if 'ordernum' in k or 'list' in k or 'shownum' in k:
            fmt = percent3
        else:
            fmt = percent
    else:
        fmt = fmt3
    return fmt


def getNameFmt(j, name1, name2, name3):
    if j < 18:
        fmt = name1
    elif j < 23:
        fmt = name2
    else:
        fmt = name3
    return fmt


def createCtrData(workbook, datadict, datadict_gr):
    if len(datadict) == 0:
        return False
    name1, name2, name3, detail, money1, money2, percent, percent1, percent2, percent3 = getXlsxFmt()
    sheet = workbook.add_worksheet('昨日ctr')
    sheet.freeze_panes(0, 2)
    sheet.set_column('B:B', 10)
    sheet.set_column('C:AE', 13)
    i = 1
    l = 35
    typelist = typedict.keys()
    for sptype in typelist:
        typename = typedict[sptype]
        sheet.merge_range(i, 1, i + 2, 1, typename, name1)
        sheet.write(i, 2, u"日期", name1)
        sheet.write(i + 1, 2, datestr, name1)
        sheet.write(i + 2, 2, '同比增长', name1)
        keylist = datadict_gr[sptype].keys()
        # print(keylist)
        j = 3
        if sptype != 'all':
            sheet.write(l, 1, typename, name1)
            sheet.write(l, 2, datadict[sptype]['listpercent'], percent3)
            sheet.write(l, 3, datadict[sptype]['shownumpercent'], percent3)
            sheet.write(l, 4, datadict[sptype]['ordernumpercent'], percent3)
        else:
            sheet.write('B35', '页面', name1)
            sheet.write('C35', '流量占比', name1)
            sheet.write('D35', '曝光占比', name1)
            sheet.write('E35', '订单占比', name1)
        for k in keylist:
            style = getNameFmt(j, name1, name2, name3)
            sheet.write(i, j, namedict[k], style)
            style2 = getNumFmt(k, j, money1, money2, percent, percent3, detail)
            sheet.write(i + 1, j, datadict[sptype][k], style2)
            style3 = getPercentFmt(datadict_gr[sptype][k], percent1, percent2)
            sheet.write(i + 2, j, datadict_gr[sptype][k], style3)
            j = j + 1
        i = i + 4
        l = l + 1

    chart1 = workbook.add_chart({'type': 'bar'})
    chart1.add_series({'name': '=昨日ctr!$C$35',
                       'categories': '=昨日ctr!$B$36:$B$42',
                       'values': '==昨日ctr!$C$36:$C$42'})
    chart1.add_series({'name': '=昨日ctr!$D$35',
                       'categories': '=昨日ctr!$B$36:$B$42',
                       'values': '==昨日ctr!$D$36:$D$42'})
    chart1.add_series({'name': '=昨日ctr!$E$35',
                       'categories': '=昨日ctr!$B$36:$B$42',
                       'values': '==昨日ctr!$E$36:$E$42'})
    chart1.set_style(18)
    sheet.insert_image('AG3', '/home/aplum/work/work_szr/info.png', {'x_scale': 1.0, 'y_scale': 1.2})
    sheet.insert_chart('G35', chart1, {'x_offset': 25, 'y_offset': 10})
    return sheet


es = Elasticsearch(["es-cn-0pp0vm2ju000a003g.elasticsearch.aliyuncs.com"], http_auth=('elastic', 'NHRHpqeFn8'),
                   port=9200)
url = "http://websa.aplum-inc.com/api/sql/query?token=15ba27be5b7895379a0574993db783281988c16f95969fe423847d31ed95728d"

start = time.time()
tic = lambda: 'at %1.1f seconds' % (time.time() - start)

if __name__ == '__main__':
    try:
        if len(sys.argv) == 1:
            today = date.today()
            date = today + timedelta(days=-1)
        else:
            datestr = sys.argv[1]
            date = datetime.strptime(datestr, '%Y-%m-%d').date()
            today = date + timedelta(days=1)
        yestoday = date + timedelta(days=-1)
        yeslastdate = yestoday + timedelta(days=-7)
        lastdate = date + timedelta(days=-7)
        datestr = date.strftime("%Y-%m-%d")
        filename = "ctr_summary_day_%s.xlsx" % datestr
        workbook = xlsxwriter.Workbook(filename)
        print(date, today)
        typelist = typedict.keys()
        datadict_total = dict()
        datadict_yes_total = dict()
        datadict_gr_total = dict()
        pvlist_all = getListPv(date, 'all')
        shownum_all = getTotalShowNum(date, 'all')
        payordernum_all, a, b, c = getPayOrderDetailNumANDPrice(lastdate, date, 'all')
        pvlist_all_yes = getListPv(yestoday, 'all')
        shownum_all_yes = getTotalShowNum(yestoday, 'all')
        payordernum_all_yes, d, e, f = getPayOrderDetailNumANDPrice(yeslastdate, yestoday, 'all')
        for sptype in typelist:
            print('======{}======'.format(sptype))
            print('=' * 8 + '当日' + '=' * 8)
            datadict = getDataDict(date, lastdate, sptype, pvlist_all, shownum_all, payordernum_all)
            datadict_total[sptype] = datadict[sptype]
            # print(datadict)
            print('=' * 8 + '昨日' + '=' * 8)
            datadict_yes = getDataDict(yestoday, yeslastdate, sptype, pvlist_all_yes, shownum_all_yes,
                                       payordernum_all_yes)
            datadict_yes_total[sptype] = datadict_yes[sptype]
            # print(datadict_yes)
            print('=' * 8 + '增长' + '=' * 8)
            datadict_gr = getGrowthDict(datadict_yes, datadict, sptype)
            datadict_gr_total[sptype] = datadict_gr[sptype]
            # print(datadict_gr)
        createCtrData(workbook, datadict_total, datadict_gr_total)
        # print(datadict_total)
        # print(datadict_yes_total)
        # print(datadict_gr_total)
        workbook.close()
        print("current time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print('Ended Polling: %s' % tic())
        addr_list = ['suzerui@aplum.com.cn', 'liudiyuhan@aplum.com.cn', 'pangbo@aplum.com.cn', 'liwenlong@aplum.com.cn']
        sendMain(addr_list, filename)
    except Exception as e:
        msg = traceback.format_exc()
        print(" delete index accurs error, the error:%s" % str(msg))
