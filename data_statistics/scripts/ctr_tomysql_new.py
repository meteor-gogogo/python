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

start = time.time()
tic = lambda: 'at %1.1f seconds' % (time.time() - start)

mysqlhost = 'rr-2ze7824567d8dgu1y.mysql.rds.aliyuncs.com'
mysqlusername = 'plumdb'
mysqlpasswd = 'plumdb@2018'
db = 'aplum'

statmysqlhost = 'pc-2ze47js669jk7k187.rwlb.rds.aliyuncs.com'
statmysqlusername = 'plumdb'
statmysqlpasswd = 'plumdb@2019'
statdb = 'aplum_stat'

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


def sendMain(addressList):
    sender = 'aplumctr@aplum-inc.com'  # 发送邮件的邮箱地址
    user = 'aplumctr@aplum-inc.com'  # 发送邮件的邮箱地址
    passwd = 'qZdCAf3MCDv8SXi'  # 发送邮件的邮箱密码
    smtpserver = 'smtp.aplum-inc.com'  # 发送邮件的邮件服务器
    receivers = []  # 接收邮件的邮箱地址
    subject = "{} ctr后台自然及活动列表页日报数据入库信息".format(datestr)
    msgText = MIMEText('{}ctr后台自然及活动列表页入库成功0.0'.format(datestr), 'plain', 'utf-8')
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


def getPidStr(pidList):
    pidStr = []
    for p in pidList:
        pidStr.append(str(p))
    idsstr = '\',\''.join(pidStr)
    return idsstr


# 获取系统维护活动id
def getAutoActivityIdList():
    autoActidList = []
    sql = "SELECT id FROM `t_activity` WHERE operation_type = 'auto'"
    cursor_aplum.execute(sql)
    results = cursor_aplum.fetchall()
    for k in results:
        k = str(k['id'])
        autoActidList.append(k)
    return autoActidList


# 获取系统维护活动的日报后台展示数据
def getAutoActidData(date, autoActidList):
    pidstr = getPidStr(autoActidList)
    sql = "SELECT stat_date, MAX(CASE WHEN src_page_id in ('{0}') THEN 'activity_auto' ELSE '' END) AS src_page_type, SUM( show_pv ) as show_pv, SUM( list_uv ) as list_uv, SUM( list_pv ) as list_pv, SUM( detail_uv ) as detail_uv, SUM( detail_pv ) as detail_pv, SUM( detail_sid_pv ) as detail_sid_pv, SUM( detail_distinct_pv ) as detail_distinct_pv, SUM( cart_list_pv ) as cart_list_pv, SUM( cart_pv ) as cart_pv, SUM( cart_total_discountprice ) as cart_total_discountprice, SUM( order_uv ) as order_uv, SUM( order_pv ) as order_pv, SUM( order_product_pv ) as order_product_pv, SUM( order_total_discountprice ) as order_total_discountprice, SUM( order_total_realpayprice ) as order_total_realpayprice FROM spid_ctr_daily WHERE src_page_id in ('{1}') AND src_page_type = 'activity' AND stat_date = '{2}' ".format(
        pidstr, pidstr, date)
    print(sql)
    cursor_stat.execute(sql)
    results = cursor_stat.fetchall()
    return results

# 获取人工维护活动的日报后台展示数据
def getManualActidData(date, autoActidList):
    pidstr = getPidStr(autoActidList)
    sql = "SELECT stat_date, MAX(CASE WHEN src_page_id not in ('{0}') THEN 'activity_manual' ELSE '' END) AS src_page_type, SUM( show_pv ) as show_pv, SUM( list_uv ) as list_uv, SUM( list_pv ) as list_pv, SUM( detail_uv ) as detail_uv, SUM( detail_pv ) as detail_pv, SUM( detail_sid_pv ) as detail_sid_pv, SUM( detail_distinct_pv ) as detail_distinct_pv, SUM( cart_list_pv ) as cart_list_pv, SUM( cart_pv ) as cart_pv, SUM( cart_total_discountprice ) as cart_total_discountprice, SUM( order_uv ) as order_uv, SUM( order_pv ) as order_pv, SUM( order_product_pv ) as order_product_pv, SUM( order_total_discountprice ) as order_total_discountprice, SUM( order_total_realpayprice ) as order_total_realpayprice FROM spid_ctr_daily WHERE src_page_id not in ('{1}')  AND src_page_type = 'activity' AND stat_date = '{2}' ".format(
        pidstr, pidstr, date)
    print(sql)
    cursor_stat.execute(sql)
    results = cursor_stat.fetchall()
    return results


# 获取活动日报后台展示总数据(人工维护和系统维护之和)
def getActivityData(date):
    sql = "SELECT stat_date,src_page_type, SUM( show_pv ) as show_pv, SUM( list_uv ) as list_uv, SUM( list_pv ) as list_pv, SUM( detail_uv ) as detail_uv, SUM( detail_pv ) as detail_pv, SUM( detail_sid_pv ) as detail_sid_pv, SUM( detail_distinct_pv ) as detail_distinct_pv, SUM( cart_list_pv ) as cart_list_pv, SUM( cart_pv ) as cart_pv, SUM( cart_total_discountprice ) as cart_total_discountprice, SUM( order_uv ) as order_uv, SUM( order_pv ) as order_pv, SUM( order_product_pv ) as order_product_pv, SUM( order_total_discountprice ) as order_total_discountprice, SUM( order_total_realpayprice ) as order_total_realpayprice FROM spid_ctr_daily WHERE src_page_type = 'activity' AND stat_date = '{0}' ".format(
        date)
    cursor_stat.execute(sql)
    results = cursor_stat.fetchall()
    return results


# 获取搜索分类及品牌列表的日报后台展示数据
def getTypeData(date):
    sql = "SELECT stat_date, src_page_type, SUM( show_pv ) as show_pv, SUM( list_uv ) as list_uv, SUM( list_pv ) as list_pv, SUM( detail_uv ) as detail_uv, SUM( detail_pv ) as detail_pv, SUM( detail_sid_pv ) as detail_sid_pv, SUM( detail_distinct_pv ) as detail_distinct_pv, SUM( cart_list_pv ) as cart_list_pv, SUM( cart_pv ) as cart_pv, SUM( cart_total_discountprice ) as cart_total_discountprice, SUM( order_uv ) as order_uv, SUM( order_pv ) as order_pv, SUM( order_product_pv ) as order_product_pv, SUM( order_total_discountprice ) as order_total_discountprice, SUM( order_total_realpayprice ) as order_total_realpayprice FROM type_ctr_daily WHERE  src_page_type != 'activity' AND src_page_type != 'all' AND stat_date = '{0}' GROUP BY src_page_type".format(
        date)
    cursor_stat.execute(sql)
    results = cursor_stat.fetchall()

    return results


# 获取搜索分类及品牌列表三者之和的日报后台展示数据
def getData_List_All(date):
    sql = "SELECT stat_date, MAX(CASE WHEN src_page_type in ('search','category','brand') THEN 'list_all' ELSE '' END) AS src_page_type, SUM( show_pv ) AS show_pv, SUM( list_uv ) AS list_uv, SUM( list_pv ) AS list_pv, SUM( detail_uv ) AS detail_uv, SUM( detail_pv ) AS detail_pv, SUM( detail_sid_pv ) AS detail_sid_pv, SUM( detail_distinct_pv ) AS detail_distinct_pv, SUM( cart_list_pv ) AS cart_list_pv, SUM( cart_pv ) AS cart_pv, SUM( cart_total_discountprice ) AS cart_total_discountprice, SUM( order_uv ) AS order_uv, SUM( order_pv ) AS order_pv, SUM( order_product_pv ) AS order_product_pv, SUM( order_total_discountprice ) AS order_total_discountprice, SUM( order_total_realpayprice ) AS order_total_realpayprice FROM type_ctr_daily WHERE src_page_type in ('search','category','brand') AND stat_date = '{0}'".format(
        date)
    cursor_stat.execute(sql)
    results = cursor_stat.fetchall()

    return results


if __name__ == '__main__':
    try:
        db_aplum = MySQLdb.connect(mysqlhost, mysqlusername, mysqlpasswd, db, charset='utf8')
        cursor_aplum = db_aplum.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        db_stat = MySQLdb.connect(statmysqlhost, statmysqlusername, statmysqlpasswd, statdb, charset='utf8')
        cursor_stat = db_stat.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        engine = create_engine(
            "mysql+pymysql://{0}:{1}@{2}/{3}?charset={4}".format(statmysqlusername, statmysqlpasswd, statmysqlhost,
                                                                 statdb, 'utf8'))
        con = engine.connect()  # 创建连接
        if len(sys.argv) == 1:
            today = date.today()
            date = today + timedelta(days=-1)
        else:
            datestr = sys.argv[1]
            date = datetime.strptime(datestr, '%Y-%m-%d').date()
        datestr = date.strftime("%Y-%m-%d")
        print(datestr)
        pystr = str(sys.argv[0])
        checkData(datestr, pystr)
        autoActidList = getAutoActivityIdList()
        results_activity_auto = getAutoActidData(date,autoActidList)
        results_activity_manual = getManualActidData(date,autoActidList)
        results_activity_all = getActivityData(date)
        results_type = getTypeData(date)
        results_all = getData_List_All(date)
        # print(results_activity_auto)
        # print(results_activity_manual)
        # print(results_activity_all)
        df_activity_auto = pd.DataFrame(list(results_activity_auto),
                               columns=['stat_date', 'src_page_type', 'show_pv', 'list_uv', 'list_pv',
                                        'detail_uv', 'detail_pv', 'detail_distinct_pv', 'detail_sid_pv', 'cart_list_pv',
                                        'cart_pv', 'cart_total_discountprice', 'order_uv', 'order_pv',
                                        'order_product_pv', 'order_total_discountprice', 'order_total_realpayprice'])
        df_activity_manual = pd.DataFrame(list(results_activity_manual),
                               columns=['stat_date', 'src_page_type', 'show_pv', 'list_uv', 'list_pv',
                                        'detail_uv', 'detail_pv', 'detail_distinct_pv', 'detail_sid_pv', 'cart_list_pv',
                                        'cart_pv', 'cart_total_discountprice', 'order_uv', 'order_pv',
                                        'order_product_pv', 'order_total_discountprice', 'order_total_realpayprice'])
        df_activity_all = pd.DataFrame(list(results_activity_all),
                               columns=['stat_date', 'src_page_type', 'show_pv', 'list_uv', 'list_pv',
                                        'detail_uv', 'detail_pv', 'detail_distinct_pv', 'detail_sid_pv', 'cart_list_pv',
                                        'cart_pv', 'cart_total_discountprice', 'order_uv', 'order_pv',
                                        'order_product_pv', 'order_total_discountprice', 'order_total_realpayprice'])
        df_type = pd.DataFrame(list(results_type),
                               columns=['stat_date', 'src_page_type', 'show_pv', 'list_uv', 'list_pv',
                                        'detail_uv', 'detail_pv', 'detail_distinct_pv', 'detail_sid_pv', 'cart_list_pv',
                                        'cart_pv', 'cart_total_discountprice', 'order_uv', 'order_pv',
                                        'order_product_pv', 'order_total_discountprice', 'order_total_realpayprice'])
        df_all = pd.DataFrame(list(results_all),
                              columns=['stat_date', 'src_page_type', 'show_pv', 'list_uv', 'list_pv',
                                       'detail_uv', 'detail_pv', 'detail_distinct_pv', 'detail_sid_pv', 'cart_list_pv',
                                       'cart_pv', 'cart_total_discountprice', 'order_uv', 'order_pv',
                                       'order_product_pv', 'order_total_discountprice', 'order_total_realpayprice'])
        df_new = df_type.append(df_all).append(df_activity_auto).append(df_activity_manual).append(df_activity_all)
        print(df_new)
        df_new.to_sql(name='type_ctr_total_daily', con=con, if_exists='append', index=False)
        cursor_stat.close()
        db_stat.close()
        cursor_aplum.close()
        db_aplum.close()
        con.close()
        print('写入成功')
        print("current time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print('Ended Polling: %s' % tic())
        addr_list = ['suzerui@aplum.com.cn']
        sendMain(addr_list)
    except Exception as e:
        msg = traceback.format_exc()
        print(" ctr_tomysql accurs error, the error:%s" % str(msg))
        userlist = ['suzerui@aplum.com']
        err = "sptype后台自然及活动列表页数据(不分端)入库出错"
        alarm(userlist, err)
