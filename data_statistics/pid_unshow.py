#!/usr/bin/env python
# coding=utf-8
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta, date
import requests
import time
import MySQLdb
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import os
import traceback
import xlsxwriter

mysqlhost = 'rr-2ze7824567d8dgu1y.mysql.rds.aliyuncs.com'
mysqlusername = 'plumdb'
mysqlpasswd = 'plumdb@2018'
db = 'aplum'


es = Elasticsearch(["es-cn-0pp0vm2ju000a003g.elasticsearch.aliyuncs.com"], http_auth=('elastic', 'NHRHpqeFn8'),
                   port=9200)
url = "http://websa.aplum-inc.com/api/sql/query?token=15ba27be5b7895379a0574993db783281988c16f95969fe423847d31ed95728d"
starttime = time.time()
tic = lambda: 'at %1.1f seconds' % (time.time() - starttime)


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
    subject = "{}单日曝光小于10次商品".format(datestr)

    baseName = os.path.basename(attachmentName)
    attachdisposition = "attachment;filename =%s" % baseName  # 发送附件html格式
    sendfile = open(attachmentName, 'rb').read()
    att = MIMEText(sendfile, 'plain', 'utf-8')
    att["Content-Type"] = "application/octet-stream"
    att["Content-Disposition"] = attachdisposition  # 发送附件html格式
    msgText = MIMEText('附件是 {} 单日曝光小于10次商品, 请查收'.format(datestr), 'plain', 'utf-8')

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


def getXlsxFmt():
    name = workbook.add_format(
        {'font_name': '微软雅黑', 'font_size': 11, 'border': 1, 'bold': True, 'font_color': 'white'})
    detail = workbook.add_format({'font_name': '微软雅黑', 'font_size': 11, 'border': 1})
    money = workbook.add_format(
        {'num_format': '_(¥* #,##0_);_(¥* (#,##0);_(¥* "-"??_);_(@_)', 'font_name': '微软雅黑', 'font_size': 11,
         'border': 1})
    percent = workbook.add_format({'num_format': '0.00%', 'font_name': '微软雅黑', 'font_size': 10, 'border': 1})
    num = workbook.add_format({'num_format': '0.00', 'font_name': '微软雅黑', 'font_size': 10, 'border': 1})
    name.set_bg_color('#305496')
    name.set_align('center')
    # detail.set_text_wrap()
    return name, detail, money, percent, num

def createUnshowPid(piddict):
    sheet = workbook.add_worksheet('{} 未曝光商品统计'.format(yestodaystr))
    sheet.set_column('A:B', 12)
    sheet.set_column('C:D', 28)
    sheet.set_column('E:E', 12)
    sheet.set_column('F:F', 13)
    name, detail, money, percent, num = getXlsxFmt()
    sheet.write(0, 0, "商品id", name)
    sheet.write(0, 1, '曝光数', name)
    sheet.write(0, 2, '商品名称', name)
    sheet.write(0, 3, '商品品牌', name)
    sheet.write(0, 4, '商品折扣价', name)
    sheet.write(0, 5, '起售日期', name)
    i = 1
    for k in piddict:
        sheet.write(i, 0, k, detail)
        sheet.write(i, 1, piddict[k]['show_pv'], detail)
        sheet.write(i, 2, piddict[k]['pname'], detail)
        sheet.write(i, 3, piddict[k]['bname'], detail)
        sheet.write(i, 4, piddict[k]['discount_price'], num)
        sheet.write(i, 5, piddict[k]['onsale_date'], detail)
        i = i + 1

def getEsResult(date, doc):
    p = dict()
    p['request_timeout'] = 30
    return es.search(index='aplum_ctr-{}'.format(date), doc_type='product', body=doc, params=p)

# 获取昨日在售商品的曝光数据, 并筛选出曝光次数小于10次的商品
def getPidShowPv(date, piddict):
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
    size = 8000
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
            show_pv = int(b['doc_count'])
            if k in piddict:
                if show_pv > 10:
                    del piddict[k]
                else:
                    piddict[k]['show_pv'] = show_pv

    return piddict

# 获取当前在售商品id及上架时间
def getPidInfo(date_timestamp):
    piddict = dict()
    EveryCount = 5000
    # start = 0
    # end = EveryCount 每次查询的id次数
    mark = 0
    while True:
        sql = "SELECT ta.id, ta.name as pname, tb.name as bname, ta.discount_price, from_unixtime( onsale_time, '%Y-%m-%d' ) AS onsale_date FROM `t_product` ta LEFT JOIN t_brand tb ON ta.brand_id = tb.id WHERE ta.id > {0} AND ta.onsale_time < {1} AND ta.status = 'onsale' ORDER BY ta.id limit {2}".format(
            mark, date_timestamp, EveryCount)
        cursor.execute(sql)
        results = cursor.fetchall()
        if len(results) == 0:
            break
        else:
            mark = results[-1]['id']
        for p in results:
            pid = int(p['id'])
            datadict = dict()
            datadict['id'] = pid
            datadict['pname'] = str(p['pname'])
            datadict['bname'] = str(p['bname'])
            datadict['discount_price'] = float(p['discount_price'])
            datadict['onsale_date'] = str(p['onsale_date'])
            datadict['show_pv'] = 0
            piddict[pid] = datadict

    return piddict


if __name__ == '__main__':
    try:
        db = MySQLdb.connect(mysqlhost, mysqlusername, mysqlpasswd, db, charset='utf8')
        cursor = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)

        datestr = date.today().strftime("%Y-%m-%d")
        date_timestamp = int(time.mktime(time.strptime(datestr, "%Y-%m-%d")))
        print(datestr, date_timestamp)
        piddict = getPidInfo(date_timestamp)
        totalnum = len(piddict)
        print(totalnum)
        yestodaystr = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
        print(yestodaystr)
        piddict = getPidShowPv(yestodaystr, piddict)
        print(len(piddict))
        filename = "pid_unshow_{}.xlsx".format(yestodaystr)
        workbook = xlsxwriter.Workbook(filename)
        createUnshowPid(piddict)
        workbook.close()

        cursor.close()  # 断开游标
        db.close()  # 断开数据库
        addr_list = ['suzerui@aplum.com.cn', 'liwenlong@aplum.com.cn', 'lihongge@aplum.com.cn',
                     'yangyanchuang@aplum.com.cn', 'liudiyuhan@aplum.com.cn', 'liudiyuhan@aplum.com.cn']
        print("current time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print('Ended Polling: %s' % tic())
        sendMain(addr_list, filename)

    except Exception as e:
        msg = traceback.format_exc()
        print(" piddetail_tomysql accurs error, the error:%s" % str(msg))
        userlist = ['suzerui@aplum.com']
        err = "在售商品单日ctr数据入库出错"
        # alarm(userlist, err)
