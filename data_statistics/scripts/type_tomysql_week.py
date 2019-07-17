#!/usr/bin/env python
# coding=utf-8
from datetime import datetime, timedelta, date
import time
import MySQLdb
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import requests
import sys
import traceback
from sqlalchemy import create_engine


start = time.time()
tic = lambda: 'at %1.1f seconds' % (time.time() - start)


statmysqlhost = 'pc-2ze47js669jk7k187.rwlb.rds.aliyuncs.com'
statmysqlusername = 'plumdb'
statmysqlpasswd = 'plumdb@2019'
statdb = 'aplum_stat'

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
    subject = "{} type ctr(含活动)周数据入库信息".format(datestr)
    msgText = MIMEText('{}至{} type ctr(含活动)入库成功0.0'.format(lastMonday,lastMonday), 'plain', 'utf-8')
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


def getLastMonday(datestr):
    today = datetime.strptime(datestr, '%Y-%m-%d').date()
    week_start = today - timedelta(days=today.weekday() + 7)
    return week_start


def getLastSunday(datestr):
    today = datetime.strptime(datestr, '%Y-%m-%d').date()
    week_end = today - timedelta(days=today.weekday() + 1)
    return week_end


# 获取各分类下id的详细名称
def getTypeWeekData(startdate, enddate):
    sql = "SELECT src_page_type, max(case when stat_date='{0}' then stat_date else 0 end) as 'startdate', max(case when stat_date='{1}' then stat_date else 0 end) as 'enddate', SUM( show_pv ) as show_pv, SUM( list_uv ) as list_uv, SUM( list_pv ) as list_pv, SUM( detail_uv ) as detail_uv, SUM( detail_pv ) as detail_pv, SUM( detail_sid_pv ) as detail_sid_pv, SUM( detail_distinct_pv ) as detail_distinct_pv, SUM( cart_list_pv ) as cart_list_pv, SUM( cart_pv ) as cart_pv, SUM( cart_total_discountprice ) as cart_total_discountprice, SUM( order_uv ) as order_uv, SUM( order_pv ) as order_pv, SUM( order_product_pv ) as order_product_pv, SUM( order_total_discountprice ) as order_total_discountprice, SUM( order_total_realpayprice ) as order_total_realpayprice FROM type_ctr_total_daily WHERE src_page_type != 'all'  AND stat_date BETWEEN '{2}' AND '{3}' GROUP BY src_page_type".format(
        startdate, enddate, startdate, enddate)
    cursor.execute(sql)
    results = cursor.fetchall()

    return results



if __name__ == '__main__':
    try:
        db = MySQLdb.connect(statmysqlhost, statmysqlusername, statmysqlpasswd, statdb, charset='utf8')
        cursor = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        engine = create_engine(
            "mysql+pymysql://{0}:{1}@{2}/{3}?charset={4}".format(statmysqlusername, statmysqlpasswd, statmysqlhost,
                                                                 statdb,
                                                                 'utf8'))
        con = engine.connect()  # 创建连接
        if len(sys.argv) == 1:
            date = date.today()
        else:
            datestr = sys.argv[1]
            date = datetime.strptime(datestr, '%Y-%m-%d').date()
        datestr = date.strftime("%Y-%m-%d")
        lastMonday = getLastMonday(datestr)
        lastSunday = getLastSunday(datestr)

        datadict_all = dict()
        print(lastMonday, lastSunday)
        results = getTypeWeekData(lastMonday, lastSunday)
        df_all = pd.DataFrame(list(results),
                              columns=['src_page_type', 'startdate', 'enddate', 'show_pv', 'list_uv', 'list_pv',
                                       'detail_uv', 'detail_pv', 'detail_distinct_pv', 'detail_sid_pv', 'cart_list_pv',
                                       'cart_pv', 'cart_total_discountprice', 'order_uv', 'order_pv',
                                       'order_product_pv', 'order_total_discountprice', 'order_total_realpayprice'])
        print(df_all.head())

        df_all.to_sql(name='type_ctr_total_weekly', con=con, if_exists='append', index=False)
        cursor.close()
        db.close()
        con.close()
        print('写入成功')
        print("current time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print('Ended Polling: %s' % tic())
        addr_list = ['suzerui@aplum.com.cn']
        sendMain(addr_list)
    except Exception as e:
        msg = traceback.format_exc()
        print(" sptype_tomysql_week accurs error, the error:%s" % str(msg))
        userlist = ['suzerui@aplum.com']
        err = "sptype周数据(含活动)入库出错"
        alarm(userlist, err)
