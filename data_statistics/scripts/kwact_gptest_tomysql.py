#!/usr/bin/env python
# coding=utf-8
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta, date
import requests
import time
import redis
import json
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import os
import sys
import traceback
from sqlalchemy import create_engine

es = Elasticsearch(["es-cn-0pp0vm2ju000a003g.elasticsearch.aliyuncs.com"], http_auth=('elastic', 'NHRHpqeFn8'),
                   port=9200)
url = "http://websa.aplum-inc.com/api/sql/query?token=15ba27be5b7895379a0574993db783281988c16f95969fe423847d31ed95728d"

statmysqlhost = 'pc-2ze47js669jk7k187.rwlb.rds.aliyuncs.com'
statmysqlusername = 'plumdb'
statmysqlpasswd = 'plumdb@2019'
statdb = 'aplum_stat'

start = time.time()
tic = lambda: 'at %1.1f seconds' % (time.time() - start)

# statmysqlhost = '127.0.0.1'
# statmysqlusername = 'root'
# statmysqlpasswd = '12345'
# statdb = 'aplum_stat'

def alarm(userlist, msg):
    url = 'http://47.93.240.37:8083/ps'
    users = ','.join(userlist)
    url = "%s?msg=%s&email=%s" % (url, msg, users)
    r = requests.get(url)
    if r.status_code == 200:
        return True
    else:
        return False


def sendMain(addressList):
    sender = 'aplumctr@aplum-inc.com'  # 发送邮件的邮箱地址
    user = 'aplumctr@aplum-inc.com'  # 发送邮件的邮箱地址
    passwd = 'qZdCAf3MCDv8SXi'  # 发送邮件的邮箱密码
    smtpserver = 'smtp.aplum-inc.com'  # 发送邮件的邮件服务器
    receivers = []  # 接收邮件的邮箱地址
    subject = "{}{}AbTest ctr数据入库信息".format(datestr, type)
    msgText = MIMEText('{} {}ABTest ctr入库成功0.0'.format(datestr, type), 'plain', 'utf-8')
    for addr in addressList:
        receivers.append(addr)
    receiver = ','.join(receivers)
    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = subject
    msgRoot['From'] = sender
    msgRoot['To'] = receiver
    msgRoot.attach(msgText)  # 添加正文
    # 发送邮件
    try:
        smtp = smtplib.SMTP_SSL(smtpserver, 465)
        smtp.login(user, passwd)
        smtp.sendmail(sender, addressList, msgRoot.as_string())
        print(datetime.now().strftime("%Y.%m.%d-%H:%M:%S"), '发送成功')
    except Exception as e:
        print(e)
    finally:
        # os.remove(filePath)
        smtp.quit()


# 获取redis中的搜索词
def getKeywordList(sptype):
    if sptype == 'search':
        keywordlist = []
        r = redis.Redis(host='r-2zec5074fca1ade4.redis.rds.aliyuncs.com', password='dL5pr5RkJBQEuqj', port=6379, db=1,
                        decode_responses=True, charset='utf-8')
        keys = r.keys(pattern='scs:*')
        for key in keys:
            key = key[4:]
            if '\'' in key:
                key = key.replace('\'', '\'\'')
            keywordlist.append(key)
    elif sptype == 'activity':
        keywordlist = ['250', '253', '552', '2244', '2245', '2343', '2344']
    return keywordlist


def getPidStr(pidList):
    pidStr = []
    for p in pidList:
        pidStr.append(str(p))
    idsstr = '\',\''.join(pidStr)
    return idsstr


def getTypeStr(sptype):
    if sptype == 'activity':
        typestr = 'src_page_id'
    elif sptype == 'search':
        typestr = 'lower(src_page_id)'
    return typestr


# 获取曝光总数据
def getGroupTestHitnum(date, sptype, spidlist):
    datestr = date.strftime("%Y-%m-%d")
    esdoc = {
        "size": 0,
        "query": {
            "bool": {
                "must_not": [
                    {
                        "term": {
                            "X-Pd-Identify.keyword": ""
                        }
                    }
                ],
                "must": [
                    {
                        "terms": {
                            "Src_page_id.keyword": spidlist
                        }
                    },
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
                    "field": "ABTest.keyword",
                    "size": 10000
                },
                "aggs": {
                    "DEV": {
                        "cardinality": {
                            "field": "X-Pd-Identify.keyword"
                        }
                    }
                }
            }
        }
    }
    searched = es.search(index="aplum_ctr-{}".format(datestr), doc_type="product", body=esdoc)
    buckets = searched['aggregations']['idcount']['buckets']
    pv_show_a = 0
    pv_show_b = 0
    uv_show_a = 0
    uv_show_b = 0
    for b in buckets:
        k = b['key']
        pv_show = int(b['doc_count'])
        uv_show = int(b['DEV']['value'])

        if k == 'alpha':
            pv_show_a = pv_show
            uv_show_a = uv_show
        if k == 'beta':
            pv_show_b = pv_show
            uv_show_b = uv_show
    return uv_show_a, uv_show_b, pv_show_a, pv_show_b


# 获取测试用户和非测试用户的列表pv及uv(只计算登录用户)
def getGroupTestListPvAndUv(sptype, spidlist, group, date):
    idstr = getPidStr(spidlist)
    typestr = getTypeStr(sptype)
    sql = "SELECT COUNT(*) as pv_list,count(distinct(ta.uid)) as uv_list FROM (SELECT distinct_ID, CASE WHEN distinct_ID like '%dev%' or distinct_ID like '%un%' THEN 'unlogin' ELSE distinct_ID END as uid,ExperimentGroup as testgp FROM EVENTS WHERE EVENT = 'ViewProductList' AND date = '{0}' AND sid != '' AND src_page_type = '{1}' AND {4} in ('{2}')  AND plumfrontend IN ( 'IOS客户端', 'Android客户端') ) ta WHERE ta.uid != 'unlogin' AND ta.testgp = '{3}'".format(
        date, sptype, idstr, group, typestr)
    # print sql
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        data = datastr.split("\n")
        if data[0] == '':
            pv_list = 0
            uv_list = 0
        else:
            datajson = json.loads(data[0])
            pv_list = datajson["pv_list"]
            uv_list = datajson["uv_list"]
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return uv_list, pv_list


# 获取分组测试的详情pv及uv(只计算登录用户)
def getGroupTestDetailPvAndUv(sptype, spidlist, group, date):
    idstr = getPidStr(spidlist)
    typestr = getTypeStr(sptype)
    sql = "SELECT date,count(*) as pv_detail,count(distinct(distinct_ID)) as uv_detail,SUM(productsale_price) AS totalsp,SUM(productdiscount_price) AS totalcp FROM EVENTS WHERE EVENT = 'ViewProduct' AND sid  in ( SELECT distinct(tb.sid) FROM (SELECT sid,distinct_ID, CASE WHEN distinct_ID like '%dev%' or distinct_ID like '%un%' THEN 'unlogin' ELSE distinct_ID END as uid,ExperimentGroup as testgp FROM EVENTS WHERE EVENT = 'ViewProductList' AND date = '{0}' AND src_page_type = '{1}' AND {5} in ('{2}') AND sid != '' AND ExperimentGroup = '{3}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) ) tb WHERE tb.uid != 'unlogin') AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY date ".format(
        date, sptype, idstr, group, date, typestr)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        data = datastr.split("\n")
        if data[0] == '':
            uv_detail = 0
            pv_detail = 0
            totalsp = 0
            totalcp = 0
        else:
            datajson = json.loads(data[0])
            pv_detail = datajson["pv_detail"]
            uv_detail = datajson["uv_detail"]
            totalsp = datajson['totalsp']
            totalcp = datajson['totalcp']

    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return uv_detail, pv_detail, totalsp, totalcp


# 获取分组测试的加购数及商品加购折扣总价,
def getGroupTestAddCartInfo(sptype, spidlist, group, date):
    idstr = getPidStr(spidlist)
    typestr = getTypeStr(sptype)
    sql = "SELECT date,count( * ) AS pv_cart, count(distinct (distinct_ID)) as uv_cart,sum(productsale_price) as totalcartsp, sum(productdiscount_price) as totalcartcp  FROM EVENTS WHERE EVENT = 'AddCart' AND sid IN ( SELECT DISTINCT	( tb.sid ) FROM	( SELECT sid, distinct_ID, CASE WHEN distinct_ID LIKE '%dev%' or distinct_ID like '%un%' THEN 'unlogin' ELSE distinct_ID END AS uid, ExperimentGroup AS testgp FROM EVENTS WHERE EVENT = 'ViewProductList' AND date = '{0}' AND src_page_type = '{1}' AND {5} in ('{2}') AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) AND sid != '' AND ExperimentGroup = '{3}' ) tb WHERE tb.uid != 'unlogin') AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端') GROUP BY date".format(
        date, sptype, idstr, group, date, typestr)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        data = datastr.split("\n")
        # print(data[0])
        if data[0] == '':
            uv_cart = 0
            pv_cart = 0
            totalcartsp = 0
            totalcartcp = 0
        else:
            datajson = json.loads(data[0])
            uv_cart = datajson["uv_cart"]
            pv_cart = datajson["pv_cart"]
            totalcartsp = datajson["totalcartsp"]
            totalcartcp = datajson["totalcartcp"]
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return uv_cart, pv_cart, totalcartsp, totalcartcp


# 获取分组测试的订单数据
def getGroupTestOrderInfo(sptype, spidlist, group, date):
    idstr = getPidStr(spidlist)
    typestr = getTypeStr(sptype)
    sql = "SELECT tf.tgroup,count(*) as pv_order_product,count(distinct(tf.uid)) as uv_order,sum(tf.saleprice) as totalordersp, sum(tf.countprice) as totalordercp,sum(tf.payprice) as totalorderpp FROM (SELECT te.uid, te.pid, te.oid, te.time, te.spid,te.sptype, te.tgroup, te.saleprice, te.countprice, te.payprice, row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time DESC ) rk FROM (SELECT tc.pid, tc.time, tc.spid,tc.sptype, td.oid, tc.uid, tc.tgroup, td.saleprice, td.countprice, td.payprice FROM (SELECT ta.sid, ta.pid, ta.time, tb.spid,tb.sptype, tb.tgroup, ta.uid FROM ( SELECT sid, productid AS pid, time, distinct_ID AS uid FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date = '{0}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端') GROUP BY sid, pid, time, uid ) ta LEFT JOIN ( SELECT sid, {6} as spid,src_page_type AS sptype, ExperimentGroup AS tgroup FROM EVENTS WHERE EVENT = 'ViewProductList' AND date = '{1}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, spid,sptype, tgroup ) tb ON ta.sid = tb.sid ) tc LEFT JOIN ( SELECT orderid AS oid, productid AS pid,distinct_ID AS uid, orderitem_saleprice as saleprice,orderitem_discountprice as countprice,orderitem_realpayprice as payprice FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{2}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid, pid, uid, saleprice, countprice,payprice ) td ON tc.pid = td.pid AND tc.uid = td.uid ) te WHERE te.oid > 0) tf WHERE tf.rk = 1 AND tf.sptype = '{3}' AND tf.spid in ('{4}') AND tf.tgroup = '{5}'GROUP BY tf.tgroup ".format(
        date, date, date, sptype, idstr, group, typestr)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    # print(sql)
    if r.status_code == 200:
        datastr = r.text
        data = datastr.split("\n")
        # print(data[0])
        if data[0] == '':
            pv_order_product = 0
            uv_order = 0
            totalordersp = 0
            totalorderpp = 0
            totalordercp = 0
        else:
            datajson = json.loads(data[0])
            pv_order_product = datajson["pv_order_product"]
            uv_order = datajson["uv_order"]
            totalordersp = datajson["totalordersp"]
            totalordercp = datajson["totalordercp"]
            totalorderpp = datajson["totalorderpp"]
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return uv_order, pv_order_product, totalordersp, totalordercp, totalorderpp


# 获取数据集的字典
def getDataDict(date, sptype, spidlist):
    ctrdict = dict()
    uv_show_a, uv_show_b, pv_show_a, pv_show_b = getGroupTestHitnum(date, sptype, spidlist)
    # uv_show_a, uv_show_b, uv_show_c, pv_show_a, pv_show_b, pv_show_c = 666,777,888,6666,7777,8888
    if sptype == 'search':
        grouplist = ['search_A', 'search_B']
    elif sptype == 'activity':
        grouplist = ['activity-auto_A', 'activity-auto_B']

    for gp in grouplist:
        ctrinfo = dict()
        ctrinfo['stat_date'] = date
        ctrinfo['groupname'] = gp
        if gp.split("_")[1] == 'B':
            group = ''
            ctrinfo['show_pv'] = pv_show_b
            ctrinfo['show_uv'] = uv_show_b
        else:
            ctrinfo['show_pv'] = pv_show_a
            ctrinfo['show_uv'] = uv_show_a
            if sptype == 'search':
                group = 'productSearchIndex.groupA'
            elif sptype == 'activity':
                group = 'productActidIndex.groupA'

        ctrinfo['list_uv'], ctrinfo['list_pv'] = getGroupTestListPvAndUv(sptype, spidlist, group, date)
        ctrinfo['detail_uv'], ctrinfo['detail_pv'], ctrinfo['detail_total_saleprice'], ctrinfo[
            'detail_total_discountprice'] = getGroupTestDetailPvAndUv(sptype, spidlist, group, date)
        ctrinfo['cart_uv'], ctrinfo['cart_pv'], ctrinfo['cart_total_saleprice'], ctrinfo[
            'cart_total_discountprice'] = getGroupTestAddCartInfo(sptype, spidlist, group, date)
        ctrinfo['order_uv'], ctrinfo['order_product_pv'], ctrinfo['order_total_saleprice'], ctrinfo[
            'order_total_discountprice'], ctrinfo['order_total_realpayprice'] = getGroupTestOrderInfo(sptype, spidlist,
                                                                                                      group, date)

        ctrdict[gp] = ctrinfo
    return ctrdict


if __name__ == '__main__':
    try:
        engine = create_engine(
            "mysql+pymysql://{0}:{1}@{2}/{3}?charset={4}".format(statmysqlusername, statmysqlpasswd, statmysqlhost,
                                                                 statdb,
                                                                 'utf8'))
        con = engine.connect()  # 创建连接
        if len(sys.argv) == 2:
            type = sys.argv[1]
            type = str(type)
            today = date.today()
            date = today + timedelta(days=-1)
        elif len(sys.argv) == 3:
            type = sys.argv[1]
            type = str(type)
            datestr = sys.argv[2]
            date = datetime.strptime(datestr, '%Y-%m-%d').date()
            today = date + timedelta(days=1)

        datestr = date.strftime("%Y-%m-%d")
        filename = "ctr_abtest_%s.xlsx" % (datestr)
        print(date, today)
        print('======{}分组测试用户CTR数据 ======'.format(type))

        spidlist = getKeywordList(type)
        datadict = getDataDict(date, type, spidlist)
        dfa = pd.DataFrame(list(datadict.values()),
                           columns=['stat_date', 'groupname', 'show_uv', 'show_pv', 'list_uv', 'list_pv', 'detail_uv',
                                    'detail_pv', 'detail_total_saleprice', 'detail_total_discountprice', 'cart_uv',
                                    'cart_pv', 'cart_total_saleprice', 'cart_total_discountprice', 'order_uv',
                                    'order_product_pv', 'order_total_saleprice', 'order_total_discountprice',
                                    'order_total_realpayprice'])
        print(dfa.head(5))
        dfa.to_sql(name='grouptest_ctr_daily', con=con, if_exists='append', index=False)
        print('数据写入成功')
        print("current time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print('Ended Polling: %s' % tic())
        addr_list = ['suzerui@aplum.com.cn']
        sendMain(addr_list)
    except Exception as e:
        msg = traceback.format_exc()
        print(" gptest_tomysql accurs error, the error:%s" % str(msg))
        userlist = ['suzerui@aplum.com']
        type = sys.argv[1]
        type = str(type)
        err = "{} ABTest数据入库出错".format(type)
        alarm(userlist, err)
