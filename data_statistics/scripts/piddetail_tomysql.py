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


def getTableName(pystr):
    if 'ctr_tomysql_new' in pystr:
        tablename = 'type_ctr_total_daily'
    elif 'spid_tomysql' in pystr:
        tablename = 'spid_ctr_daily'
    elif 'piddetail_tomysql' in pystr:
        tablename = 'product_ctr_daily'
    elif 'type_tomysql' in pystr:
        tablename = 'type_ctr_daily'
    return tablename

def checkData(datestr, pystr):
    t_name = getTableName(pystr)
    datanum = getDataNum(datestr, t_name)
    print('当前重复数据: {}'.format(datanum))
    if datanum > 0:
        deleteData(datestr, t_name)
    else:
        return

def getDataNum(datestr, t_name):
    sql = "SELECT count(1) as num FROM `{0}` WHERE stat_date = '{1}'".format(t_name, datestr)
    cursor_stat.execute(sql)
    results = cursor_stat.fetchall()
    for k in results:
        num = int(k['num'])
    return num

def deleteData(datestr, t_name):
    sql = "DELETE  FROM `{0}` WHERE stat_date = '{1}'".format(t_name, datestr)
    print(sql)
    cursor_stat.execute(sql)
    db_stat.commit()

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
    subject = "{}商品ctr存储".format(datestr)

    baseName = os.path.basename(attachmentName)
    attachdisposition = "attachment;filename =%s" % baseName  # 发送附件html格式
    sendfile = open(attachmentName, 'rb').read()
    att = MIMEText(sendfile, 'plain', 'utf-8')
    att["Content-Type"] = "application/octet-stream"
    att["Content-Disposition"] = attachdisposition  # 发送附件html格式
    msgText = MIMEText('{}数据写入成功 0.0'.format(datestr), 'plain', 'utf-8')

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
        os.remove(attachmentName)
        smtp.quit()


def initDataDict():
    pidinfo = dict()
    pidinfo['stat_date'] = date
    pidinfo['product_id'] = 0
    pidinfo['pv_show'] = 0
    pidinfo['seller_id'] = 0
    pidinfo['brand_id'] = 0
    pidinfo['category_id'] = 0
    pidinfo['onsale_time'] = 0
    pidinfo['pv_detail'] = 0
    pidinfo['pv_wish'] = 0
    pidinfo['pv_cart'] = 0
    pidinfo['uv_show'] = 0
    pidinfo['uv_show_imei'] = 0
    pidinfo['uv_show_idfa'] = 0
    pidinfo['uv_detail'] = 0
    pidinfo['uv_cart'] = 0
    pidinfo['uv_wish'] = 0
    pidinfo['original_price'] = 0.0
    pidinfo['avg_discount_price'] = 0.0
    pidinfo['avg_sale_price'] = 0.0
    return pidinfo


def getEsResult(date, doc):
    p = dict()
    p['request_timeout'] = 30
    return es.search(index='aplum_ctr-{}'.format(date), doc_type='product', body=doc, params=p)


# 获取src_page_id的在es中的曝光以及spidlist
def getPidShowPvAndDict(date):
    piddict = dict()
    esdoc = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "Status": "onsale"
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
    searched = getEsResult(date, esdoc)
    totalnum = searched['aggregations']['idcount']['value']
    size = 2000
    if totalnum <= size:
        pagesize = 1
    else:
        pagesize = int(totalnum / size + 2)
    for num in range(0, pagesize):
        spiddoc = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "Status": "onsale"
                            }
                        }
                    ]
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

        spidsearched = getEsResult(date, spiddoc)
        buckets = spidsearched['aggregations']['idcount']['buckets']
        for b in buckets:
            k = int(b['key'])
            pv_show = int(b['doc_count'])
            pidinfo = initDataDict()
            pidinfo['pv_show'] = pv_show
            pidinfo['product_id'] = k
            piddict[k] = pidinfo
    return piddict


def getPidShowUv(date, totalnum, piddict):
    size = 2000
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
                        "must": [
                            {
                                "term": {
                                    "Status": "onsale"
                                }
                            }
                        ],
                        "must_not": [
                            {
                                "term": {
                                    devicetype: ""
                                }
                            }
                        ]
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
                pid = int(b['key'])
                uv_show = int(b['DEV']['value'])
                if pid in piddict:
                    pidinfo = piddict[pid]
                    if devicetype == 'X-Pd-Imei':
                        pidinfo['uv_show_imei'] = uv_show
                    elif devicetype == 'X-Pd-Idfa':
                        pidinfo['uv_show_idfa'] = uv_show
            time.sleep(0.5)
    return piddict


# 获取神策中商品的详情pv和uv
def getDetailPvAndUv(piddict, date):
    sql = "SELECT productid as pid, AVG(productsale_price) as avg_sale_price, AVG(productdiscount_price) avg_discount_price,count(*) as pv_detail,count(distinct(distinct_ID)) as uv_detail FROM EVENTS WHERE EVENT = 'ViewProduct' AND date = '{0}' AND productsold = 0 GROUP BY pid".format(
        date)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        data = datastr.split("\n")
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    for i in range(0, len(data)):
        if len(data[i]) == 0:
            continue
        else:
            datajson = json.loads(data[i])
        pid = int(datajson['pid'])
        if pid in piddict:
            pidinfo = piddict[pid]
        else:
            pidinfo = initDataDict()
            pidinfo['product_id'] = pid
            piddict[pid] = pidinfo
        if 'avg_sale_price' in datajson:
            avg_sale_price = datajson['avg_sale_price']
        else:
            avg_sale_price = 0
        if 'avg_discount_price' in datajson:
            avg_discount_price = datajson['avg_discount_price']
        else:
            avg_discount_price = 0
        if 'pv_detail' in datajson:
            pv_detail = datajson['pv_detail']
        else:
            pv_detail = 0
        if 'uv_detail' in datajson:
            uv_detail = datajson['uv_detail']
        else:
            uv_detail = 0
        pidinfo['pv_detail'] = pv_detail
        pidinfo['uv_detail'] = uv_detail
        pidinfo['avg_sale_price'] = avg_sale_price
        pidinfo['avg_discount_price'] = avg_discount_price
    return piddict


# 获取神策中商品的收藏数和加购uv
def getWishPvAndUv(piddict, date):
    sql = "SELECT productid as pid,count(*) as pv_wish,count(distinct(distinct_ID)) as uv_wish FROM EVENTS WHERE EVENT = 'AddWish' AND date = '{0}' GROUP BY pid".format(
        date)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        data = datastr.split("\n")
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    for i in range(0, len(data)):
        if len(data[i]) == 0:
            continue
        else:
            datajson = json.loads(data[i])
        pid = int(datajson['pid'])
        if pid in piddict:
            pidinfo = piddict[pid]
        else:
            pidinfo = initDataDict()
            pidinfo['product_id'] = pid
            piddict[pid] = pidinfo
        if 'pv_wish' in datajson:
            pv_wish = datajson['pv_wish']
        else:
            pv_wish = 0
        if 'uv_wish' in datajson:
            uv_wish = datajson['uv_wish']
        else:
            uv_wish = 0
        pidinfo['pv_wish'] = pv_wish
        pidinfo['uv_wish'] = uv_wish
    return piddict


# 获取神策中商品的加购数和加购uv
def getCartPvAndUv(piddict, date):
    sql = "SELECT productid as pid,count(*) as pv_cart,count(distinct(distinct_ID)) as uv_cart FROM EVENTS WHERE EVENT = 'AddCart' AND date = '{0}' GROUP BY pid".format(
        date)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        data = datastr.split("\n")
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    for i in range(0, len(data)):
        if len(data[i]) == 0:
            continue
        else:
            datajson = json.loads(data[i])
        pid = int(datajson['pid'])
        if pid in piddict:
            pidinfo = piddict[pid]
        else:
            pidinfo = initDataDict()
            pidinfo['product_id'] = pid
            piddict[pid] = pidinfo
        if 'pv_cart' in datajson:
            pv_cart = datajson['pv_cart']
        else:
            pv_cart = 0
        if 'uv_cart' in datajson:
            uv_cart = datajson['uv_cart']
        else:
            uv_cart = 0
        pidinfo['pv_cart'] = pv_cart
        pidinfo['uv_cart'] = uv_cart
    return piddict


# 获取各分类下id的详细名称
def getPidInfo(datadict):
    pidList = list(datadict.keys())
    pLen = len(pidList)
    EveryCount = 2000
    # start = 0
    # end = EveryCount 每次查询的id次数
    times = int(pLen / EveryCount + 1)
    for t in range(int(times)):
        pidStr = []
        start = t * EveryCount
        end = (t + 1) * EveryCount
        if end >= pLen:
            end = pLen
        for p in pidList[start:end]:
            pidStr.append(str(p))

        idsstr = '\',\''.join(pidStr)
        sql = "SELECT id,user_id as seller_id,brand_id,category_id,onsale_time,original_price FROM `t_product` t WHERE id in ('{0}')".format(
            idsstr)
        cursor.execute(sql)
        results = cursor.fetchall()
        for p in results:
            pid = int(p['id'])
            if pid in datadict:
                datadict[pid]['seller_id'] = p['seller_id']
                datadict[pid]['brand_id'] = p['brand_id']
                datadict[pid]['category_id'] = int(p['category_id'])
                datadict[pid]['onsale_time'] = p['onsale_time']
                datadict[pid]['original_price'] = float(p['original_price'])
                datadict[pid]['uv_show'] = datadict[pid]['uv_show_imei'] + datadict[pid]['uv_show_idfa']

    return datadict


if __name__ == '__main__':
    try:
        db = MySQLdb.connect(mysqlhost, mysqlusername, mysqlpasswd, db, charset='utf8')
        cursor = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        db_stat = MySQLdb.connect(statmysqlhost, statmysqlusername, statmysqlpasswd, statdb, charset='utf8')
        cursor_stat = db_stat.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        if len(sys.argv) == 1:
            today = date.today()
            date = today + timedelta(days=-1)
        else:
            datestr = sys.argv[1]
            date = datetime.strptime(datestr, '%Y-%m-%d').date()
            today = date + timedelta(days=1)
        datestr = date.strftime("%Y-%m-%d")
        print(date, today)
        pystr = str(sys.argv[0])
        checkData(datestr, pystr)
        piddict = getPidShowPvAndDict(date)
        totalnum = len(piddict)
        piddict = getPidShowUv(date, totalnum, piddict)
        piddict = getDetailPvAndUv(piddict, date)
        piddict = getWishPvAndUv(piddict, date)
        piddict = getCartPvAndUv(piddict, date)
        piddict = getPidInfo(piddict)
        engine = create_engine(
            "mysql+pymysql://{0}:{1}@{2}/{3}?charset={4}".format(statmysqlusername, statmysqlpasswd, statmysqlhost,
                                                                 statdb,
                                                                 'utf8'))

        con = engine.connect()  # 创建连接
        dfa = pd.DataFrame(list(piddict.values()),
                           columns=['stat_date', 'product_id', 'seller_id', 'brand_id', 'category_id', 'original_price',
                                    'avg_sale_price', 'avg_discount_price', 'onsale_time', 'pv_show', 'pv_detail',
                                    'pv_wish', 'pv_cart', 'uv_show', 'uv_detail', 'uv_cart', 'uv_wish'])
        totalnum = dfa.shape[0]
        EachInsert = 5000
        times = int(totalnum / EachInsert + 1)
        for t in range(int(times)):
            pidStr = []
            start = t * EachInsert
            end = (t + 1) * EachInsert
            print(start, end)
            dfa[start:end].to_sql(name='product_ctr_daily', con=con, if_exists='append', index=False)

        print('数据写入成功')
        print(dfa.head(10))
        datestr = date.strftime("%Y-%m-%d")
        filename = './' + 'piddetail_{}.csv'.format(datestr)
        dfa.to_csv(filename,
                   columns=['stat_date', 'product_id', 'seller_id', 'brand_id', 'category_id', 'original_price',
                            'avg_sale_price', 'avg_discount_price', 'onsale_time', 'pv_show', 'pv_detail', 'pv_wish',
                            'pv_cart', 'uv_show', 'uv_detail', 'uv_cart', 'uv_wish'],
                   index=False, sep=',')
        cursor_stat.close()
        db_stat.close()
        cursor.close()  # 断开游标
        db.close()  # 断开数据库
        con.close()
        addr_list = ['suzerui@aplum.com.cn']
        print("current time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print('Ended Polling: %s' % tic())
        sendMain(addr_list, filename)

    except Exception as e:
        msg = traceback.format_exc()
        print(" piddetail_tomysql accurs error, the error:%s" % str(msg))
        userlist = ['suzerui@aplum.com']
        err = "在售商品单日ctr数据入库出错"
        alarm(userlist, err)
