#!/usr/bin/env python
# coding=utf-8
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta, date
import requests
import time
import MySQLdb
import json
import xlsxwriter
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import os

mysqlhost = 'rr-2ze7824567d8dgu1y.mysql.rds.aliyuncs.com'
mysqlusername = 'plumdb'
mysqlpasswd = 'plumdb@2018'
db = 'aplum'


def getWeekNum(num):
    now = datetime.now()
    week_start = now - timedelta(days=now.weekday() + num)
    weekday = week_start.strftime("%Y-%m-%d")
    lastweektimestamp = int(time.mktime(time.strptime(weekday, "%Y-%m-%d")))
    return lastweektimestamp


def sendMain(addressList, attachmentName):
    sender = 'aplum2016@163.com'  # 发送邮件的邮箱地址
    user = 'aplum2016@163.com'  # 发送邮件的邮箱地址
    passwd = '123qweASD'  # 发送邮件的邮箱密码
    smtpserver = 'smtp.163.com'  # 发送邮件的邮件服务器
    receivers = []  # 接收邮件的邮箱地址
    subject = "上周ctr统计"

    filePath = '/home/aplum/inner/data_statistics/scripts/%s' % attachmentName
    baseName = os.path.basename(filePath)
    attachdisposition = "attachment;filename =%s" % baseName  # 发送附件html格式
    sendfile = open(filePath, 'rb').read()
    att = MIMEText(sendfile, 'plain', 'utf-8')
    att["Content-Type"] = "application/octet-stream"
    att["Content-Disposition"] = attachdisposition  # 发送附件html格式
    msgText = MIMEText('您好，上周ctr数据见附件', 'plain', 'utf-8')

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


def getMonday(today):
    week_Mon = today - timedelta(days=today.weekday())
    date1 = week_Mon.strftime("%Y-%m-%d")
    return date1


def getFriday(today):
    week_Fri = today - timedelta(days=today.weekday() - 4)
    date1 = week_Fri.strftime("%Y-%m-%d")
    return date1


def getOnsaleRate(cursor, es, weektime, nowtime):
    cursor.execute("set names utf8")
    sql = 'select count(id) as num from t_product where status = "onsale"'
    try:
        cursor.execute(sql)
        results = cursor.fetchone()
        totalnum = results['num']
        # print totalnum
    except  Exception as e:
        print("getonsalerate mysql accur exceptioon, error=%s" % str(e))

    esdoc = {
        "size": 0,
        "query": {
            "bool": {
                "must_not": [
                    {"term": {
                        "Src_page_type.keyword": ""
                    }}
                ],
                "filter": {
                    "range": {
                        "Createtime": {
                            "gte": weektime,
                            "lt": nowtime
                        }
                    }
                },
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
                    },
                    {
                        "term": {
                            "Status.keyword": "onsale"
                        }
                    }
                ]
            }
        },
        "aggs": {
            "idcount": {
                "cardinality": {
                    "field": "ItemActions"
                }
            }
        }
    }
    searched = es.search(index="aplum_ctr*", doc_type="product", body=esdoc)
    hitnum = searched['aggregations']['idcount']['value']
    size = 10000
    pagesize = int(hitnum / 10000 + 2)
    totalsum = 0
    addnum = 0
    for num in range(0, pagesize):
        padoc = {
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
                            "match": {
                                "Status.keyword": "onsale"
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
                    ],
                    "filter": {
                        "range": {
                            "Createtime": {
                                "gte": weektime,
                                "lt": nowtime
                            }
                        }
                    }
                }
            },
            "aggs": {
                "idcount": {
                    "terms": {
                        "field": "ItemActions",
                        "include": {
                            "partition": num,
                            "num_partitions": pagesize
                        },
                        "size": size
                    }
                }
            }
        }
        searched = es.search(index="aplum_ctr*", doc_type="product", body=padoc)
        idsdict = searched['aggregations']['idcount']['buckets']
        idslist = list()
        for idinfo in idsdict:
            idslist.append(str(idinfo['key']))
        totalsum += len(idslist)
        idstr = ','.join(idslist)
        if len(idstr) == 0:
            addnum = 0
        else:
            # 统计曝光时未售出,当前状态已售出的商品数量,将统计结果记为addnum
            sql = 'select count(id) as num from t_product where id in (%s) and status != "onsale"' % idstr
            try:
                cursor.execute(sql)
                results = cursor.fetchone()
                idtotalnum = results['num']
                addnum += idtotalnum
            except  Exception as e:
                print("getonsalerate mysql accur exceptioon, error=%s" % str(e))
    totalnum += addnum
    if int(totalnum) == 0:
        onsalerate = 0
    else:
        onsalerate = float(totalsum) / float(totalnum)
    return onsalerate, totalsum, totalnum


def getTotalRate(cursor, es, weektime, nowtime):
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
                        "match": {
                            "Status.keyword": "onsale"
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
                ],
                "filter": {
                    "range": {
                        "Createtime": {
                            "gte": weektime,
                            "lt": nowtime
                        }
                    }
                }
            }
        },
        "aggs": {
            "idcount": {
                "cardinality": {
                    "field": "ItemActions"
                }
            }
        }
    }
    searched = es.search(index="aplum_ctr*", doc_type="product", body=esdoc)
    hitnum = searched['aggregations']['idcount']['value']
    cursor.execute("set names utf8")
    sql = 'select count(id) as num from t_product where status in ("onsale", "sold")'
    try:
        cursor.execute(sql)
        results = cursor.fetchone()
        totalnum = results['num']
    except  Exception as e:
        print("gettotalrate mysql accur exceptioon, error=%s" % str(e))
    if int(totalnum) == 0:
        totalrate = 0
    else:
        totalrate = float(hitnum) / float(totalnum)
    return totalrate, hitnum, totalnum


# 获取整年的日期list从周五开始
def get_week_of_year(year):
    temp = year
    map = {}
    year = str(year) + '-01-01'
    # 每一年的一月一号
    year_of_date = datetime.strptime(year, '%Y-%m-%d')
    # 如果一号大于周4，则属于上一年最后一周
    # 计算周几
    which_week = year_of_date.isoweekday()
    # 闰年
    total_of_year = 365
    range_start = 0
    if temp % 100 != 0 and temp % 4 == 0:
        total_of_year = 366
    if year.find('01-01') != -1:
        if which_week > 4:
            add = 7 - which_week + 1
            year_of_date = year_of_date + timedelta(add)
            total_of_year = total_of_year - add
        else:
            add = 1 - which_week
            if which_week != 1:
                year_of_date = year_of_date + timedelta(add)
                total_of_year = total_of_year - add
    # 上面已经计算出了周一，加四天从周五开始
    year_of_date = year_of_date + timedelta(4)
    final_year_array = []
    for i in range(range_start, total_of_year):
        final_year = year_of_date + timedelta(i)
        final_year_array.append(final_year.strftime('%Y-%m-%d'))
    return final_year_array


def reshape(lst, n):
    size = len(lst) // n
    if (len(lst) % n) != 0:
        size = size + 1
    return [lst[i * n:(i + 1) * n] for i in range(size)]


# 获取上周五到本周四的list
def getlastweeklist():
    now = datetime.now()
    # s上周五的日期
    week_start = now - timedelta(days=now.weekday() + 3)
    weekday = week_start.strftime("%Y-%m-%d")
    yearinfo = datetime.now().year
    week_of_date = reshape(get_week_of_year(int(yearinfo)), 7)
    for da in week_of_date:
        if weekday in da:
            return da
    return []


def getlastweekstr(weeklist):
    return "%s 至 %s " % (weeklist[0], weeklist[len(weeklist) - 1])


# 获取单日商品总曝光量
def getTotalShowNum(es, starttime, endtime, sptype):
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
                    ],
                    "filter": {
                        "range": {
                            "Createtime": {
                                "gte": starttime,
                                "lt": endtime
                            }
                        }
                    }
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
                    ],
                    "filter": {
                        "range": {
                            "Createtime": {
                                "gte": starttime,
                                "lt": endtime
                            }
                        }
                    }
                }
            }
        }
    searched = es.search(index="aplum_ctr*", doc_type="product", body=esdoc)
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
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            uv_list = 0
        else:
            dataarr = datastr.split("\n")
            datajson = json.loads(dataarr[0])
            uv_list = datajson["uv_list"]
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return uv_list


# 获取单日列表页pv总量
def getListPv(date, sptype):
    if sptype == 'all':
        sql = "SELECT COUNT(*) as pv_list FROM (SELECT src_page_type as sptype, CASE WHEN src_page_type like '%_search%' THEN 'oldversion' ELSE src_page_type END as type FROM EVENTS WHERE EVENT = 'ViewProductList'  AND date = '{0}'  AND src_page_type != ''  AND plumfrontend IN ( 'IOS客户端', 'Android客户端' )) ta WHERE ta.type != 'oldversion'".format(
            date)
    else:
        sql = "SELECT COUNT(src_page_type) AS pv_list FROM EVENTS WHERE EVENT = 'ViewProductList' AND date ='{0}' AND src_page_type = '{1}' AND plumfrontend in ('IOS客户端','Android客户端') ".format(
            date, sptype)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            pv_list = 0
        else:
            dataarr = datastr.split("\n")
            datajson = json.loads(dataarr[0])
            pv_list = datajson["pv_list"]
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return pv_list


# 获取单日详情页uv总量
def getDetailUv(date, sptype):
    if sptype == 'all':
        sql = "SELECT COUNT(distinct(distinct_ID)) as uv_detail FROM (SELECT src_page_type as sptype, distinct_ID, CASE WHEN src_page_type like '%_search%' THEN 'oldversion' ELSE src_page_type END as type FROM EVENTS WHERE EVENT = 'ViewProduct'  AND date = '{0}'  AND src_page_type != ''  AND plumfrontend IN ( 'IOS客户端', 'Android客户端' )) ta WHERE ta.type != 'oldversion'".format(
            date)
    else:
        sql = "SELECT COUNT(distinct(distinct_ID)) AS uv_detail FROM EVENTS WHERE EVENT = 'ViewProduct' AND date ='{0}' AND src_page_type = '{1}' AND plumfrontend in ('IOS客户端','Android客户端') ".format(
            date, sptype)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            uv_detail = 0
        else:
            dataarr = datastr.split("\n")
            datajson = json.loads(dataarr[0])
            uv_detail = datajson["uv_detail"]
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return uv_detail


# 获取单日详情页pv总量(不去重)
def getDetailPv(date, sptype):
    if sptype == 'all':
        sql = "SELECT COUNT(*) as pv_detail FROM (SELECT src_page_type as sptype, CASE WHEN src_page_type like '%_search%' THEN 'oldversion' ELSE src_page_type END as type FROM EVENTS WHERE EVENT = 'ViewProduct'  AND date = '{0}'  AND src_page_type != ''  AND plumfrontend IN ( 'IOS客户端', 'Android客户端' )) ta WHERE ta.type != 'oldversion'".format(
            date)
    else:
        sql = "SELECT COUNT(src_page_id) AS pv_detail FROM EVENTS WHERE EVENT = 'ViewProduct' AND date ='{0}' AND src_page_type = '{1}' AND plumfrontend in ('IOS客户端','Android客户端') ".format(
            date, sptype)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            pv_detail = 0
        else:
            dataarr = datastr.split("\n")
            datajson = json.loads(dataarr[0])
            pv_detail = datajson["pv_detail"]
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return pv_detail


# 获取单日详情页去重pv总量(去重单次曝光多次详情页点击的同一个商品)
def getDetailDistinctPv(url, date, sptype):
    if sptype == 'all':
        sql = "SELECT COUNT(*) as pv_detail_distinct FROM (SELECT tb.sid,tb.productid,tb.type FROM (SELECT * from (SELECT sid, productid, src_page_type as sptype, CASE WHEN src_page_type like '%_search%' THEN 'oldversion' ELSE src_page_type END as type FROM EVENTS WHERE EVENT = 'ViewProduct' AND date = '{0}' AND src_page_type != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' )) ta WHERE ta.type != 'oldversion') tb GROUP BY tb.sid, tb.productid,tb.type) tc".format(
            date)
    else:
        sql = "SELECT COUNT(*) as pv_detail_distinct FROM (SELECT sid, productid FROM events WHERE event='ViewProduct' and date = '{0}' and src_page_type = '{1}' and plumfrontend in ('IOS客户端','Android客户端') GROUP BY sid, productid) ta".format(
            date, sptype)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            pv_detail_distinct = 0
        else:
            dataarr = datastr.split("\n")
            datajson = json.loads(dataarr[0])
            pv_detail_distinct = datajson["pv_detail_distinct"]
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return pv_detail_distinct


# 获取单日详情页去重pv总量(单次次曝光的多次详情页点击只计算一次)
def getDetailSidPv(url, date, sptype):
    if sptype == 'all':
        sql = "SELECT COUNT(*) as pv_detail_sid FROM (SELECT ta.dsid,ta.type FROM  (SELECT  distinct(sid) as dsid, src_page_type AS sptype, CASE  WHEN src_page_type LIKE '%_search%' THEN 'oldversion' ELSE src_page_type  END AS type FROM EVENTS WHERE EVENT = 'ViewProduct'  AND date = '{0}' AND src_page_type != ''  AND plumfrontend IN ( 'IOS客户端', 'Android客户端' )  ) ta WHERE ta.type != 'oldversion' ) tb ".format(
            date)
    else:
        sql = "SELECT COUNT(distinct(sid)) AS pv_detail_sid FROM EVENTS WHERE EVENT = 'ViewProduct' AND date ='{0}' and src_page_type = '{1}' and plumfrontend in ('IOS客户端','Android客户端') ".format(
            date, sptype)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            pv_detail_sid = 0
        else:
            dataarr = datastr.split("\n")
            datajson = json.loads(dataarr[0])
            pv_detail_sid = datajson["pv_detail_sid"]
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return pv_detail_sid


# 获取单日列表加购数量
def getListAddCartNum(url, date, sptype):
    if sptype == 'all':
        sql = " SELECT count( * ) AS listaddcartnum FROM (SELECT ta.sid, ta.pid, tb.sptype FROM ( SELECT sid, productid AS pid FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND src_page_type = 'list' AND date = '{0}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, pid ) ta LEFT JOIN ( SELECT sid, src_page_type AS sptype FROM EVENTS WHERE EVENT IN ('ViewProduct', 'ViewProductList' ) AND date = '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) AND sid != '' GROUP BY sid, sptype) tb ON ta.sid = tb.sid ) tc WHERE tc.sptype != '' ".format(
            date, date)
    else:
        sql = " SELECT tc.sptype AS type, count( tc.sptype ) AS listaddcartnum FROM( SELECT ta.sid, ta.pid, tb.sptype FROM ( SELECT sid, productid AS pid FROM EVENTS WHERE EVENT = 'AddCart' AND src_page_type = 'list' AND sid != '' AND date = '{0}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, pid ) ta LEFT JOIN ( SELECT sid, src_page_type AS sptype FROM EVENTS WHERE EVENT IN ( 'ViewProduct', 'ViewProductList' ) AND date = '{1}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, sptype) tb ON ta.sid = tb.sid) tc WHERE tc.sptype = '{2}' GROUP BY type".format(
            date, date, sptype)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            listaddcartnum = 0
        else:
            dataarr = datastr.split("\n")
            datajson = json.loads(dataarr[0])
            listaddcartnum = datajson["listaddcartnum"]
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return listaddcartnum


# 获取单日加购数量及商品折扣总价
def getAddCartNumANDPrice(url, date, sptype):
    if sptype == 'all':
        sql = " SELECT count( * ) AS addcartnum, sum(tc.countprice) as totalcp FROM (SELECT ta.sid, ta.pid, tb.sptype,ta.saleprice,ta.countprice FROM ( SELECT sid, productid AS pid,productsale_price as saleprice, productdiscount_price as countprice FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date = '{0}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, pid, saleprice, countprice ) ta LEFT JOIN ( SELECT sid, src_page_type AS sptype FROM EVENTS WHERE EVENT IN ( 'ViewProduct', 'ViewProductList' ) AND date = '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) AND sid != '' GROUP BY sid, sptype) tb ON ta.sid = tb.sid ) tc WHERE tc.sptype != '' ".format(
            date, date)
    else:
        sql = " SELECT tc.sptype AS type, count( tc.sptype ) AS addcartnum, sum(tc.countprice) as totalcp FROM( SELECT ta.sid, ta.pid, tb.sptype, ta.saleprice,ta.countprice FROM ( SELECT sid, productid AS pid,productsale_price as saleprice, productdiscount_price as countprice FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date = '{0}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, pid, saleprice, countprice ) ta LEFT JOIN ( SELECT sid, src_page_type AS sptype FROM EVENTS WHERE EVENT IN ( 'ViewProduct', 'ViewProductList' ) AND date = '{1}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, sptype) tb ON ta.sid = tb.sid) tc WHERE tc.sptype = '{2}' GROUP BY type".format(
            date, date, sptype)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            addcartnum = 0
        else:
            dataarr = datastr.split("\n")
            datajson = json.loads(dataarr[0])
            addcartnum = datajson["addcartnum"]
            totalcp = datajson["totalcp"]
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return addcartnum, totalcp


# 获取单日订单uv数
def getOrderUv(url, lastdate, date, sptype):
    if sptype == 'all':
        sql = " SELECT count(distinct(tg.uid)) as orderuv FROM (SELECT tf.sptype,tf.oid,tf.uid FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf GROUP BY tf.sptype,tf.oid,tf.uid) tg WHERE tg.sptype != '' ".format(
            lastdate, date, lastdate, date, date)
    else:
        sql = " SELECT count(distinct(tg.uid)) as orderuv FROM (SELECT tf.sptype,tf.oid,tf.uid FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf GROUP BY tf.sptype,tf.oid,tf.uid) tg WHERE tg.sptype = '{5}' ".format(
            lastdate, date, lastdate, date, date, sptype)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            orderuv = 0
        else:
            dataarr = datastr.split("\n")
            datajson = json.loads(dataarr[0])
            orderuv = datajson["orderuv"]
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return orderuv


# 获取单日订单数量(订单维度)
def getOrderNum(url, lastdate, date, sptype):
    if sptype == 'all':
        sql = " SELECT count(*) as ordernum FROM (SELECT tf.sptype,tf.oid,tf.uid FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf GROUP BY tf.sptype,tf.oid,tf.uid) tg WHERE tg.sptype != '' ".format(
            lastdate, date, lastdate, date, date)
    else:
        sql = " SELECT count(*) as ordernum FROM (SELECT tf.sptype,tf.oid,tf.uid FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf GROUP BY tf.sptype,tf.oid,tf.uid) tg WHERE tg.sptype = '{5}' ".format(
            lastdate, date, lastdate, date, date, sptype)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            ordernum = 0
        else:
            dataarr = datastr.split("\n")
            datajson = json.loads(dataarr[0])
            ordernum = datajson["ordernum"]
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return ordernum


# 获取单日订单数量(商品维度)及商品折扣总价和用户实付总价
def getPayOrderDetailNumANDPrice(url, lastdate, date, sptype):
    if sptype == 'all':
        sql = " SELECT count(tf.sptype) as payordernum, sum(tf.countprice) as totalcp,sum(tf.payprice) as totalpp FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,te.countprice,te.payprice, row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf,td.countprice,td.payprice FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid,orderitem_discountprice as countprice, orderitem_realpayprice as payprice FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid,countprice,payprice) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf WHERE rk = 1 AND tf.sptype != ''".format(
            lastdate, date, lastdate, date, date)
    else:
        sql = " SELECT count(tf.sptype) as payordernum,sum(tf.countprice) as totalcp,sum(tf.payprice) as totalpp FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,te.countprice,te.payprice, row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf,td.payprice,td.countprice FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid,orderitem_discountprice as countprice, orderitem_realpayprice as payprice FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid,countprice, payprice) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf WHERE rk = 1 AND tf.sptype = '{5}'".format(
            lastdate, date, lastdate, date, date, sptype)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            payordernum = 0
        else:
            dataarr = datastr.split("\n")
            datajson = json.loads(dataarr[0])
            payordernum = datajson["payordernum"]
            totalcp = datajson["totalcp"]
            totalpp = datajson["totalpp"]
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return payordernum, totalcp, totalpp

# 获取数据集的字典
def getDataDict(weeklist, weekday05, sptype, es):
    if len(weeklist) == 0:
        return 0
    ctrdict = dict()
    i = 0
    for day in weeklist:
        ctrinfo = dict()
        date = datetime.strptime(day, "%Y-%m-%d")
        lastdate = date + timedelta(days=-7)
        lastdate = lastdate.strftime("%Y-%m-%d")
        k = i + 1
        starttime = int(time.mktime(time.strptime(day, "%Y-%m-%d")))
        if i == len(weeklist) - 1:
            endtime = int(time.mktime(time.strptime(str(weekday05), "%Y-%m-%d")))
        else:
            endtime = int(time.mktime(time.strptime(str(weeklist[k]), "%Y-%m-%d")))
        ctrinfo['shownum'] = getTotalShowNum(es, starttime, endtime, sptype)
        # ctrinfo['shownum'] = 10000
        ctrinfo['uv_list'] = getListUv(day, sptype)
        ctrinfo['pv_list'] = getListPv(day, sptype)
        ctrinfo['uv_detail'] = getDetailUv(day, sptype)
        ctrinfo['pv_detail'] = getDetailPv(day, sptype)
        ctrinfo['pv_detail_distinct'] = getDetailDistinctPv(url, day, sptype)
        ctrinfo['pv_detail_sid'] = getDetailSidPv(url, day, sptype)
        ctrinfo['listaddcartnum'] = getListAddCartNum(url, date, sptype)
        ctrinfo['addcartnum'], ctrinfo['totalcartcp'] = getAddCartNumANDPrice(url, date, sptype)
        ctrinfo['orderuv'] = getOrderUv(url, lastdate, date, sptype)
        ctrinfo['ordernum'] = getOrderNum(url, lastdate, date, sptype)
        ctrinfo['payordernum'], ctrinfo['totalordercp'], ctrinfo['totalorderpp'] = getPayOrderDetailNumANDPrice(url,
                                                                                                                lastdate,
                                                                                                                date,
                                                                                                                sptype)
        ctrdict[day] = ctrinfo
        i = i + 1
    return ctrdict


def getXlsxFmt():
    fmt = workbook.add_format()
    fmt2 = workbook.add_format()
    money = workbook.add_format({'num_format': '¥  #,##0'})
    money.set_font("微软雅黑")
    money.set_font_size(10)
    money.set_border()
    fmt.set_font("微软雅黑")
    fmt.set_font_size(10)
    fmt.set_bg_color('#8EA9DB')
    fmt.set_font_color('white')
    fmt.set_bold()
    fmt.set_border()
    fmt.set_text_wrap()
    fmt2.set_font("微软雅黑")
    fmt2.set_font_size(10)
    fmt2.set_border()
    return fmt, fmt2, money


def createCtrData(workbook, datadict, sptype):
    if len(datadict) == 0:
        return False
    if sptype == 'all':
        typename = '总数'
    elif sptype == 'category':
        typename = '分类页'
    elif sptype == 'activity':
        typename = '活动页'
    elif sptype == 'search':
        typename = '搜索页'
    elif sptype == 'brand':
        typename = '品牌页'
    elif sptype == 'index':
        typename = '首页'
    elif sptype == 'other':
        typename = '其他页'
    elif sptype == 'recommend':
        typename = '推荐页'
    fmt, fmt2, money = getXlsxFmt()
    sheet = workbook.add_worksheet(typename)
    sheet.set_column('A:Z', 13)
    sheet.write(0, 0, u"日期", fmt)
    sheet.write(0, 1, u"列表总曝光量", fmt)
    sheet.write(0, 2, u"列表页uv", fmt)
    sheet.write(0, 3, u"列表页pv", fmt)
    sheet.write(0, 4, u"详情页uv", fmt)
    sheet.write(0, 5, u"详情页曝光pv(不滤重)", fmt)
    sheet.write(0, 6, u"详情页曝光量PV(滤重pv)", fmt)
    sheet.write(0, 7, u"详情页曝光量(滤重同一商品)", fmt)
    sheet.write(0, 8, u"单日列表加购数", fmt)
    sheet.write(0, 9, u"单日总加购数", fmt)
    sheet.write(0, 10, u"单日加购商品折扣总价", fmt)
    sheet.write(0, 11, u"单日支付订单uv数", fmt)
    sheet.write(0, 12, u"单日支付订单数", fmt)
    sheet.write(0, 13, u"单日支付订单商品数", fmt)
    sheet.write(0, 14, u"单日支付订单商品折扣总价", fmt)
    sheet.write(0, 15, u"单日支付订单商品实付总价", fmt)
    i = 1
    keyslist = datadict.keys()
    for pid in keyslist:
        sheet.write(i, 0, pid, fmt2)
        sheet.write(i, 1, datadict[pid]['shownum'], fmt2)
        sheet.write(i, 2, datadict[pid]['uv_list'], fmt2)
        sheet.write(i, 3, datadict[pid]['pv_list'], fmt2)
        sheet.write(i, 4, datadict[pid]['uv_detail'], fmt2)
        sheet.write(i, 5, datadict[pid]['pv_detail'], fmt2)
        sheet.write(i, 6, datadict[pid]['pv_detail_sid'], fmt2)
        sheet.write(i, 7, datadict[pid]['pv_detail_distinct'], fmt2)
        sheet.write(i, 8, datadict[pid]['listaddcartnum'], fmt2)
        sheet.write(i, 9, datadict[pid]['addcartnum'], fmt2)
        sheet.write(i, 10, datadict[pid]['totalcartcp'], money)
        sheet.write(i, 11, datadict[pid]['orderuv'], fmt2)
        sheet.write(i, 12, datadict[pid]['ordernum'], fmt2)
        sheet.write(i, 13, datadict[pid]['payordernum'], fmt2)
        sheet.write(i, 14, datadict[pid]['totalordercp'], money)
        sheet.write(i, 15, datadict[pid]['totalorderpp'], money)
        i = i + 1
    return sheet


def createtotalratesheet(workbook, lastweekstr, totalrate, hitnum, totalnum):
    fmt, fmt2, money = getXlsxFmt()
    sheet = workbook.add_worksheet(u"商品有效曝光占比")
    sheet.set_column('A:Z', 10)
    sheet.write(0, 0, u"时间", fmt)
    sheet.write(0, 1, u"曝光商品总数", fmt)
    sheet.write(0, 2, u"总商品数量", fmt)
    sheet.write(0, 3, u"有效曝光比", fmt)
    sheet.write(1, 0, lastweekstr, fmt2)
    sheet.write(1, 1, hitnum, fmt2)
    sheet.write(1, 2, totalnum, fmt2)
    sheet.write(1, 3, totalrate, fmt2)
    return sheet


def createonsalerate(workbook, lastweekstr, onsalerate, totalsum, totalnum):
    sheet = workbook.add_worksheet(u"在售商品有效曝光占比")
    fmt, fmt2, money = getXlsxFmt()
    sheet.set_column('A:Z', 10)
    sheet.write(0, 0, u"时间",fmt)
    sheet.write(0, 1, u"在售商品曝光总数", fmt)
    sheet.write(0, 2, u"总在售商品数量", fmt)
    sheet.write(0, 3, u"在售商品有效曝光比", fmt)
    sheet.write(1, 0, lastweekstr, fmt2)
    sheet.write(1, 1, totalsum, fmt2)
    sheet.write(1, 2, totalnum, fmt2)
    sheet.write(1, 3, onsalerate, fmt2)
    return sheet

start = time.time()
tic = lambda: 'at %1.1f seconds' % (time.time() - start)
if __name__ == '__main__':
    db = MySQLdb.connect(mysqlhost, mysqlusername, mysqlpasswd, db, charset='utf8')
    es = Elasticsearch(["es-cn-0pp0vm2ju000a003g.elasticsearch.aliyuncs.com"], http_auth=('elastic', 'NHRHpqeFn8'),
                       port=9200)
    url = "http://websa.aplum-inc.com/api/sql/query?token=15ba27be5b7895379a0574993db783281988c16f95969fe423847d31ed95728d"
    cursor = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    # 获取上周五0时0分的时间戳
    weeklist = getlastweeklist()
    # weeklist = ['2019-04-11', '2019-04-12', '2019-04-13', '2019-04-14', '2019-04-15']
    weektime = int(time.mktime(time.strptime(str(weeklist[0]), "%Y-%m-%d")))
    today = date.today()
    weekday05 = getFriday(today)
    weekday05time = int(time.mktime(time.strptime(str(weekday05), "%Y-%m-%d")))
    lastweekstr = getlastweekstr(weeklist)
    datestr = today.strftime("%Y-%m-%d")
    filename = "ctr_total_week_%s.xlsx" % datestr
    workbook = xlsxwriter.Workbook(filename)
    print(lastweekstr)
    print('======商品曝光量（商品去重）/ 总商品数量======')
    totalrate, hitnum, totalnum = getTotalRate(cursor, es, weektime, weekday05time)
    print(totalrate)
    print('======在售商品曝光量（商品去重） /总在售商品数量======')
    onsalerate, totalsum, totalnum = getOnsaleRate(cursor, es, weektime, weekday05time)
    print(onsalerate)
    createtotalratesheet(workbook, lastweekstr, totalrate, hitnum, totalnum)
    createonsalerate(workbook, lastweekstr, onsalerate, totalsum, totalnum)
    typelist = ['all', 'category', 'search', 'activity', 'brand', 'other', 'index', 'recommend']
    for sptype in typelist:
        print('======{}======'.format(sptype))
        datadict = getDataDict(weeklist, weekday05, sptype, es)
        createCtrData(workbook, datadict, sptype)
    cursor.close()  # 断开游标
    db.close()  # 断开数据库

    workbook.close()
    print("current time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print('Ended Polling: %s' % tic())
    # addr_list = ['461235890@qq.com', 'liudiyuhan@163.com', 'wengxuejie0524@163.com', 'pangbo@aplum.com',
    #              '116019204@qq.com']
    addr_list = ['suzerui@aplum.com.cn', 'liudiyuhan@aplum.com.cn', 'wengxuejie@aplum.com.cn', 'pangbo@aplum.com.cn',
                 'liwenlong@aplum.com.cn']
    sendMain(addr_list, filename)

