#!/usr/bin/env python
# coding=utf-8
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
import MySQLdb
import pandas as pd
import numpy
from dateutil.relativedelta import relativedelta

# result_dict_list = list()

url = "http://websa.aplum-inc.com/api/sql/query?token=15ba27be5b7895379a0574993db783281988c16f95969fe423847d31ed95728d"

mysqlhost = 'rr-2ze7824567d8dgu1y.mysql.rds.aliyuncs.com'
mysqlusername = 'plumdb'
mysqlpasswd = 'plumdb@2018'
db = 'aplum'

mysql_host = "rm-2zeixwnpc34127h5f191-vpc-rw.mysql.rds.aliyuncs.com"
mysql_user = "plumdb"
mysql_passwd = "plumdb@2018"
mysql_db = "aplum_mis"

start = time.time()
tic = lambda: 'at %1.1f seconds' % (time.time() - start)


def get_month_seller_income(source, db_aplum, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp):
    register_seller_list = list()
    month_seller_income = 0.00
    if source == 'all':
        sql = "select c.second_id from (select a.second_id, a.source, a.createtime, b.distinct_id from " \
              "(select distinct second_id, source, createtime from users where  " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{2}' " \
              "and '{3}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source == 'nature':
        sql = "select c.second_id from (select a.second_id, a.source, b.distinct_id from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and source " \
              "not in({2}) union select first_id, source, second_id from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select " \
              "e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' " \
              "then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from " \
              "events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n " \
              "on m.first_id=n.distinct_id)tmp where tmp.khd in ('百度小程序', '微信小程序','IOS','Android'))a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{7}' " \
              "and '{8}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_Android':
        sql = "select c.second_id from (select a.second_id, a.source, b.distinct_id from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and source " \
              "not in({2}) union select first_id, source, second_id from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select " \
              "e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' " \
              "then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from " \
              "events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n " \
              "on m.first_id=n.distinct_id)tmp where tmp.khd = 'Android')a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{7}' " \
              "and '{8}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_IOS':
        sql = "select c.second_id from (select a.second_id, a.source, b.distinct_id from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and source " \
              "not in({2}) union select first_id, source, second_id from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select " \
              "e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' " \
              "then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from " \
              "events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n " \
              "on m.first_id=n.distinct_id)tmp where tmp.khd = 'IOS')a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{7}' " \
              "and '{8}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_wechat':
        sql = "select c.second_id from (select a.second_id, a.source, b.distinct_id from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and source " \
              "not in({2}) union select first_id, source, second_id from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select " \
              "e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' " \
              "then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from " \
              "events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n " \
              "on m.first_id=n.distinct_id)tmp where tmp.khd = '微信小程序')a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{7}' " \
              "and '{8}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_baidu':
        sql = "select c.second_id from (select a.second_id, a.source, b.distinct_id from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and source " \
              "not in({2}) union select first_id, source, second_id from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select " \
              "e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' " \
              "then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from " \
              "events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n " \
              "on m.first_id=n.distinct_id)tmp where tmp.khd = '百度小程序')a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{7}' " \
              "and '{8}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == '抖音kol':
        sql = "select c.distinct_id from (select a.distinct_id from " \
              "(select distinct distinct_id from events where event = 'registerSuccessAct' and PartnerOldUser " \
              "is null and regexp_like(distinct_id, '^[0-9]+$') and date between '{0}' and '{1}')" \
              "a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{2}' " \
              "and '{3}') b on a.distinct_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_date_tmp, end_date_tmp, start_date_tmp, end_date_tmp)
        payload = {'q': sql, 'format': 'json'}
        r = requests.post(url, data=payload)
        if r.status_code == 200:
            datastr = r.text
            if len(datastr) == 0:
                month_seller_income = 0.00
            else:
                dataarr = datastr.split('\n')
                for data in dataarr:
                    try:
                        datajson = json.loads(data)
                        if str(datajson) == '{}':
                            continue
                        register_seller_list.append(int(datajson['distinct_id']))
                    except json.decoder.JSONDecodeError as identifier:
                        pass
        else:
            print("sa hive sql accur error, sql为%s" % sql)
        if len(register_seller_list) == 0:
            month_seller_income = 0.00
        else:
            cursor = db_aplum.cursor(cursorclass=MySQLdb.cursors.DictCursor)
            start_timestamp_tmp = int(start_timestamp / 1000)
            end_timestamp_tmp = int(end_timestamp / 1000)
            register_seller_list_tmp = "'" + "','".join(str(i) for i in register_seller_list) + "'"
            sql = "select sum(a.sum_income) as count_source from (select tp.user_id, sum(tdi.settle_price) as " \
                  "sum_income FROM t_order td LEFT JOIN t_order_item tdi on tdi.order_id=td.id left join t_product tp " \
                  "on tdi.product_id=tp.id where td.status not in ('new','topay','cancel') and td.splitted='0' and " \
                  "td.order_time >= {0} and td.order_time < {1} and tp.user_id in ({2}) " \
                  "group by tp.user_id)a".format(start_timestamp_tmp, end_timestamp_tmp, register_seller_list_tmp)
            # print(sql)
            cursor.execute(sql)
            source_data = cursor.fetchall()
            cursor.close()
            register_seller_list.clear()
            for row in source_data:
                # row = process_price(row)
                # if
                # print(row)
                if row['count_source'] == None:
                    month_seller_income = 0.00
                    continue
                else:
                    month_seller_income = round(float(row['count_source']), 2)
        return month_seller_income
    elif source == '微信朋友圈':
        sql = "select c.second_id from (select a.second_id, a.source, a.createtime, b.distinct_id from " \
              "(select distinct second_id, source, createtime from users where source = 'weixin' and adaccount = 'weixin-rd' and " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{2}' " \
              "and '{3}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source == '微信公众号':
        sql = "select c.second_id from (select a.second_id, a.source, a.createtime, b.distinct_id from " \
              "(select distinct second_id, source, createtime from users where source = 'weixin' and adaccount = 'weixin-ios-rdpyq' and " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{2}' " \
              "and '{3}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    else:
        sql = "select c.second_id from (select a.second_id, a.source, a.createtime, b.distinct_id from " \
              "(select distinct second_id, source, createtime from users where source in ({0}) and " \
              "createtime >= {1} and createtime < {2})a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{3}' " \
              "and '{4}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(source_by, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            month_seller_income = 0.00
        else:
            dataarr = datastr.split('\n')
            for data in dataarr:
                try:
                    datajson = json.loads(data)
                    if str(datajson) == '{}':
                        continue
                    register_seller_list.append(int(datajson['second_id']))
                except json.decoder.JSONDecodeError as identifier:
                    pass
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    if len(register_seller_list) == 0:
        month_seller_income = 0.00
    else:
        cursor = db_aplum.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        start_timestamp_tmp = int(start_timestamp / 1000)
        end_timestamp_tmp = int(end_timestamp / 1000)
        register_seller_list_tmp = "'" + "','".join(str(i) for i in register_seller_list) + "'"
        sql = "select sum(a.sum_income) as count_source from (select tp.user_id, sum(tdi.settle_price) as " \
              "sum_income FROM t_order td LEFT JOIN t_order_item tdi on tdi.order_id=td.id left join t_product tp " \
              "on tdi.product_id=tp.id where td.status not in ('new','topay','cancel') and td.splitted='0' and " \
              "td.order_time >= {0} and td.order_time < {1} and tp.user_id in ({2}) " \
              "group by tp.user_id)a".format(start_timestamp_tmp, end_timestamp_tmp, register_seller_list_tmp)
        # print(sql)
        cursor.execute(sql)
        source_data = cursor.fetchall()
        cursor.close()
        register_seller_list.clear()
        for row in source_data:
            # row = process_price(row)
            # if
            # print(row)
            if row['count_source'] is None:
                month_seller_income = 0.00
                continue
            else:
                month_seller_income = round(float(row['count_source']), 2)
    return month_seller_income


def get_post_seller_num(source, db_aplum, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp):
    post_seller_num = 0
    register_seller_list = list()
    if source == 'all':
        sql = "select c.second_id from (select a.second_id, a.source, a.createtime, b.distinct_id from " \
              "(select distinct second_id, source, createtime from users where  " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{2}' " \
              "and '{3}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source == 'nature':
        sql = "select c.second_id from (select a.second_id, a.source, b.distinct_id from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} " \
              "and createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select " \
              "e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' " \
              "then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from " \
              "events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n " \
              "on m.first_id=n.distinct_id)tmp where tmp.khd in ('百度小程序', '微信小程序','IOS','Android'))a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{7}' " \
              "and '{8}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_Android':
        sql = "select c.second_id from (select a.second_id, a.source, b.distinct_id from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} " \
              "and createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select " \
              "e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' " \
              "then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from " \
              "events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n " \
              "on m.first_id=n.distinct_id)tmp where tmp.khd = 'Android')a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{7}' " \
              "and '{8}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_IOS':
        sql = "select c.second_id from (select a.second_id, a.source, b.distinct_id from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} " \
              "and createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select " \
              "e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' " \
              "then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from " \
              "events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n " \
              "on m.first_id=n.distinct_id)tmp where tmp.khd = 'IOS')a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{7}' " \
              "and '{8}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_wechat':
        sql = "select c.second_id from (select a.second_id, a.source, b.distinct_id from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} " \
              "and createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select " \
              "e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' " \
              "then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from " \
              "events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n " \
              "on m.first_id=n.distinct_id)tmp where tmp.khd = '微信小程序')a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{7}' " \
              "and '{8}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_baidu':
        sql = "select c.second_id from (select a.second_id, a.source, b.distinct_id from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} " \
              "and createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select " \
              "e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' " \
              "then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from " \
              "events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n " \
              "on m.first_id=n.distinct_id)tmp where tmp.khd = '百度小程序')a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{7}' " \
              "and '{8}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == '抖音kol':
        sql = "select c.distinct_id from (select a.distinct_id from " \
              "(select distinct distinct_id from events where event = 'registerSuccessAct' and PartnerOldUser is null " \
              "and regexp_like(distinct_id, '^[0-9]+$') and date between '{0}' and '{1}')a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{2}' " \
              "and '{3}') b on a.distinct_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_date_tmp, end_date_tmp, start_date_tmp, end_date_tmp)
        payload = {'q': sql, 'format': 'json'}
        r = requests.post(url, data=payload)
        if r.status_code == 200:
            datastr = r.text
            if len(datastr) == 0:
                post_seller_num = 0
            else:
                dataarr = datastr.split('\n')
                for data in dataarr:
                    try:
                        datajson = json.loads(data)
                        if str(datajson) == '{}':
                            continue
                        register_seller_list.append(int(datajson['distinct_id']))
                    except json.decoder.JSONDecodeError as identifier:
                        pass
        else:
            print("sa hive sql accur error, sql为%s" % sql)
        if len(register_seller_list) == 0:
            post_seller_num = 0
        else:
            cursor = db_aplum.cursor(cursorclass=MySQLdb.cursors.DictCursor)
            start_timestamp_tmp = int(start_timestamp / 1000)
            end_timestamp_tmp = int(end_timestamp / 1000)
            register_seller_list_tmp = "'" + "','".join(str(i) for i in register_seller_list) + "'"
            sql = "select count(distinct user_id) as count_source from t_seller_express where user_id in ({0}) and " \
                  "create_time >= {1} and create_time < {2} and express_no is not null".format(
                register_seller_list_tmp, start_timestamp_tmp, end_timestamp_tmp)
            # print(sql)
            cursor.execute(sql)
            source_data = cursor.fetchall()
            cursor.close()
            register_seller_list.clear()
            for row in source_data:
                # row = process_price(row)
                # print(row)
                post_seller_num = int(row['count_source'])
        return post_seller_num
    elif source == '微信朋友圈':
        sql = "select c.second_id from (select a.second_id, a.source, a.createtime, b.distinct_id from " \
              "(select distinct second_id, source, createtime from users where source = 'weixin' and adaccount = 'weixin-rd' and " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{2}' " \
              "and '{3}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source == '微信公众号':
        sql = "select c.second_id from (select a.second_id, a.source, a.createtime, b.distinct_id from " \
              "(select distinct second_id, source, createtime from users where source = 'weixin' and adaccount = 'weixin-ios-rdpyq' and " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{2}' " \
              "and '{3}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    else:
        sql = "select c.second_id from (select a.second_id, a.source, a.createtime, b.distinct_id from " \
              "(select distinct second_id, source, createtime from users where source in ({0}) and " \
              "createtime >= {1} and createtime < {2})a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{3}' " \
              "and '{4}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(source_by, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            post_seller_num = 0
        else:
            dataarr = datastr.split('\n')
            for data in dataarr:
                try:
                    datajson = json.loads(data)
                    if str(datajson) == '{}':
                        continue
                    register_seller_list.append(int(datajson['second_id']))
                except json.decoder.JSONDecodeError as identifier:
                    pass
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    if len(register_seller_list) == 0:
        post_seller_num = 0
    else:
        cursor = db_aplum.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        start_timestamp_tmp = int(start_timestamp / 1000)
        end_timestamp_tmp = int(end_timestamp / 1000)
        register_seller_list_tmp = "'" + "','".join(str(i) for i in register_seller_list) + "'"
        sql = "select count(distinct user_id) as count_source from t_seller_express where user_id in ({0}) and " \
              "create_time >= {1} and create_time < {2} and express_no is not null".format(
            register_seller_list_tmp, start_timestamp_tmp, end_timestamp_tmp)
        # print(sql)
        cursor.execute(sql)
        source_data = cursor.fetchall()
        cursor.close()
        register_seller_list.clear()
        for row in source_data:
            # row = process_price(row)
            # print(row)
            post_seller_num = int(row['count_source'])
    return post_seller_num


def get_register_seller_num(source, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp):
    register_seller_num = 0
    if source == 'all':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, a.createtime, b.distinct_id from " \
              "(select distinct second_id, source, createtime from users where " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{2}' " \
              "and '{3}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source == 'nature':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, b.distinct_id from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} " \
              "and createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select " \
              "e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' " \
              "then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from " \
              "events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n " \
              "on m.first_id=n.distinct_id)tmp where tmp.khd in ('百度小程序', '微信小程序','IOS','Android'))a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{7}' " \
              "and '{8}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_IOS':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, b.distinct_id from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} " \
              "and createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select " \
              "e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' " \
              "then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from " \
              "events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n " \
              "on m.first_id=n.distinct_id)tmp where tmp.khd = 'IOS')a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{7}' " \
              "and '{8}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_Android':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, b.distinct_id from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} " \
              "and createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select " \
              "e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' " \
              "then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from " \
              "events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n " \
              "on m.first_id=n.distinct_id)tmp where tmp.khd = 'Android')a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{7}' " \
              "and '{8}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_wechat':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, b.distinct_id from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} " \
              "and createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select " \
              "e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' " \
              "then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from " \
              "events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n " \
              "on m.first_id=n.distinct_id)tmp where tmp.khd = '微信小程序')a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{7}' " \
              "and '{8}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_baidu':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, b.distinct_id from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} " \
              "and createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select " \
              "e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' " \
              "then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from " \
              "events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n " \
              "on m.first_id=n.distinct_id)tmp where tmp.khd = '百度小程序')a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{7}' " \
              "and '{8}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == '抖音kol':
        sql = "select count(*) as count_source from (select distinct a.distinct_id from " \
              "(select distinct distinct_id from events where event = 'registerSuccessAct' and PartnerOldUser " \
              "is null and regexp_like(distinct_id, '^[0-9]+$') and date between '{0}' and '{1}')" \
              "a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{2}' " \
              "and '{3}') b on a.distinct_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_date_tmp, end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == '微信朋友圈':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, a.createtime, b.distinct_id from " \
              "(select distinct second_id, source, createtime from users where source = 'weixin' and adaccount = 'weixin-rd' and " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{2}' " \
              "and '{3}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source == '微信公众号':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, a.createtime, b.distinct_id from " \
              "(select distinct second_id, source, createtime from users where source = 'weixin' and adaccount = 'weixin-ios-rdpyq' and " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{2}' " \
              "and '{3}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    else:
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, a.createtime, b.distinct_id from " \
              "(select distinct second_id, source, createtime from users where source in ({0}) and " \
              "createtime >= {1} and createtime < {2})a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{3}' " \
              "and '{4}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(source_by, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            register_seller_num = 0
        else:
            dataarr = datastr.split('\n')
            for data in dataarr:
                try:
                    datajson = json.loads(data)
                    if str(datajson) == '{}':
                        continue
                    register_seller_num = int(datajson['count_source'])
                except json.decoder.JSONDecodeError as identifier:
                    pass
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return register_seller_num


def get_order_sum_realpayprice(source, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp):
    order_sum_realpayprice = 0.00
    if source == 'all':
        sql = "select sum(c.orderitem_realpayprice) sum_realpay from (select a.second_id, a.source, a.createtime, " \
              "b.orderitem_realpayprice from (select distinct second_id, source, createtime from users where " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select * from events where event = 'PayOrderDetail' and date between '{2}' and '{3}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source == 'nature':
        sql = "select sum(c.orderitem_realpayprice) sum_realpay from (select a.second_id, a.source,  " \
              "b.orderitem_realpayprice from (select tmp.second_id, tmp.source from (select * from( select u.first_id, " \
              "u.source, u.second_id from (select first_id, source, second_id from users where createtime >= {0} " \
              "and createtime < {1} and source not in({2}) union select first_id, source, second_id from users " \
              "where createtime >= {3} and createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m " \
              "left join (select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  " \
              "'微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' " \
              "group by e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where tmp.khd in ('百度小程序', '微信小程序','IOS','Android'))a left join " \
              "(select * from events where event = 'PayOrderDetail' and date between '{7}' and '{8}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_IOS':
        sql = "select sum(c.orderitem_realpayprice) sum_realpay from (select a.second_id, a.source,  " \
              "b.orderitem_realpayprice from (select tmp.second_id, tmp.source from (select * from( select u.first_id, " \
              "u.source, u.second_id from (select first_id, source, second_id from users where createtime >= {0} " \
              "and createtime < {1} and source not in({2}) union select first_id, source, second_id from users " \
              "where createtime >= {3} and createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m " \
              "left join (select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  " \
              "'微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' " \
              "group by e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where tmp.khd = 'IOS')a left join " \
              "(select * from events where event = 'PayOrderDetail' and date between '{7}' and '{8}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_Android':
        sql = "select sum(c.orderitem_realpayprice) sum_realpay from (select a.second_id, a.source,  " \
              "b.orderitem_realpayprice from (select tmp.second_id, tmp.source from (select * from( select u.first_id, " \
              "u.source, u.second_id from (select first_id, source, second_id from users where createtime >= {0} " \
              "and createtime < {1} and source not in({2}) union select first_id, source, second_id from users " \
              "where createtime >= {3} and createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m " \
              "left join (select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  " \
              "'微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' " \
              "group by e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where tmp.khd = 'Android')a left join " \
              "(select * from events where event = 'PayOrderDetail' and date between '{7}' and '{8}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_wechat':
        sql = "select sum(c.orderitem_realpayprice) sum_realpay from (select a.second_id, a.source,  " \
              "b.orderitem_realpayprice from (select tmp.second_id, tmp.source from (select * from( select u.first_id, " \
              "u.source, u.second_id from (select first_id, source, second_id from users where createtime >= {0} " \
              "and createtime < {1} and source not in({2}) union select first_id, source, second_id from users " \
              "where createtime >= {3} and createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m " \
              "left join (select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  " \
              "'微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' " \
              "group by e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where tmp.khd = '微信小程序')a left join " \
              "(select * from events where event = 'PayOrderDetail' and date between '{7}' and '{8}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_baidu':
        sql = "select sum(c.orderitem_realpayprice) sum_realpay from (select a.second_id, a.source,  " \
              "b.orderitem_realpayprice from (select tmp.second_id, tmp.source from (select * from( select u.first_id, " \
              "u.source, u.second_id from (select first_id, source, second_id from users where createtime >= {0} " \
              "and createtime < {1} and source not in({2}) union select first_id, source, second_id from users " \
              "where createtime >= {3} and createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m " \
              "left join (select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  " \
              "'微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' " \
              "group by e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where tmp.khd = '百度小程序')a left join " \
              "(select * from events where event = 'PayOrderDetail' and date between '{7}' and '{8}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif str(source).endswith('KOL') or source in ('小红书', 'channel_kol'):
        sql = "select sum(b.orderitem_realpayprice) as sum_realpay from (select second_id from users where " \
              "firstordertime >= {0} and firstordertime < {1} and source in ({2}))a left join (select " \
              "distinct_id, orderitem_realpayprice from events where event = 'PayOrderDetail' and date " \
              "between '{3}' and '{4}')b on a.second_id = b.distinct_id where b.distinct_id is not null"\
            .format(start_timestamp, end_timestamp, source_by, start_date_tmp, end_date_tmp)
    elif source == '抖音kol':
        register_seller_list = list()
        sql = "select distinct distinct_id from events where event = 'registerSuccessAct' " \
              "and PartnerOldUser is null and regexp_like(distinct_id, '^[0-9]+$') and date between '{0}' " \
              "and '{1}'".format(start_date_tmp, end_date_tmp)
        payload = {'q': sql, 'format': 'json'}
        r = requests.post(url, data=payload)
        if r.status_code == 200:
            datastr = r.text
            if len(datastr) == 0:
                order_user_num = 0
            else:
                dataarr = datastr.split('\n')
                for data in dataarr:
                    try:
                        datajson = json.loads(data)
                        if str(datajson) == '{}':
                            continue
                        register_seller_list.append(datajson['distinct_id'])
                    except json.decoder.JSONDecodeError as identifier:
                        pass
        else:
            print("sa hive sql accur error, sql为%s" % sql)
        cursor = db_aplum.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        start_timestamp_tmp = int(start_timestamp / 1000)
        end_timestamp_tmp = int(end_timestamp / 1000)
        register_seller_list_tmp = "'" + "','".join(str(i) for i in register_seller_list) + "'"
        sql = "SELECT sum(td.realpay_price) as count_source FROM t_order td where td.status not in " \
              "('new','topay','cancel') AND td.parent_id='0' and td.order_time>={0} and " \
              "td.order_time<{1} and user_id in ({2})".format(start_timestamp_tmp, end_timestamp_tmp, register_seller_list_tmp)
        # print(sql)
        cursor.execute(sql)
        source_data = cursor.fetchall()
        cursor.close()
        register_seller_list.clear()
        for row in source_data:
            # row = process_price(row)
            # print(row)
            order_sum_realpayprice = round(float(row['count_source']), 2)
        return order_sum_realpayprice
    elif source == '微信朋友圈':
        sql = "select sum(c.orderitem_realpayprice) sum_realpay from (select a.second_id, a.source, a.createtime, " \
              "b.orderitem_realpayprice from (select distinct second_id, source, createtime from users where " \
              "source = 'weixin' and adaccount = 'weixin-rd' and createtime >= {0} and createtime < {1})a left join " \
              "(select * from events where event = 'PayOrderDetail' and date between '{2}' and '{3}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source == '微信公众号':
        sql = "select sum(c.orderitem_realpayprice) sum_realpay from (select a.second_id, a.source, a.createtime, " \
              "b.orderitem_realpayprice from (select distinct second_id, source, createtime from users where " \
              "source = 'weixin' and adaccount = 'weixin-ios-rdpyq' and createtime >= {0} and createtime < {1})a left join " \
              "(select * from events where event = 'PayOrderDetail' and date between '{2}' and '{3}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    else:
        sql = "select sum(c.orderitem_realpayprice) sum_realpay from (select a.second_id, a.source, a.createtime, " \
              "b.orderitem_realpayprice from (select distinct second_id, source, createtime from users where " \
              "source in ({0}) and createtime >= {1} and createtime < {2})a left join " \
              "(select * from events where event = 'PayOrderDetail' and date between '{3}' and '{4}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(source_by, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    # print(sql)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            order_sum_realpayprice = 0.00
        else:
            dataarr = datastr.split('\n')
            for data in dataarr:
                try:
                    datajson = json.loads(data)
                    if str(datajson) == '{}':
                        continue
                    order_sum_realpayprice = round(float(datajson['sum_realpay']), 2)
                except json.decoder.JSONDecodeError as identifier:
                    pass
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return order_sum_realpayprice


def get_order_num(source, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp):
    order_num = 0
    if source == 'all':
        sql = "select count(distinct c.orderid) as count_source from (select a.second_id, a.source, a.createtime, b.orderid from " \
              "(select distinct second_id, source, createtime from users where  " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select distinct orderid, distinct_id from events where event = 'PayOrderDetail' and " \
              "date between '{2}' and '{3}') b on a.second_id = b.distinct_id where " \
              "b.distinct_id is not null)c".format(start_timestamp, end_timestamp, start_date_tmp,
                                                   end_date_tmp)
    elif source == 'nature':
        sql = "select count(distinct c.orderid) as count_source from (select a.second_id, a.source, b.orderid from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join " \
              "(select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序'" \
              " else  '微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{5}' and " \
              "'{6}' group by e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where tmp.khd in('IOS','Android','微信小程序','百度小程序'))a left join " \
              "(select distinct orderid, distinct_id from events where event = 'PayOrderDetail' and " \
              "date between '{7}' and '{8}') b on a.second_id = b.distinct_id where " \
              "b.distinct_id is not null)c".format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                                                   end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_IOS':
        sql = "select count(distinct c.orderid) as count_source from (select a.second_id, a.source, b.orderid from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join " \
              "(select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序'" \
              " else  '微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{5}' and " \
              "'{6}' group by e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where tmp.khd = 'IOS')a left join " \
              "(select distinct orderid, distinct_id from events where event = 'PayOrderDetail' and " \
              "date between '{7}' and '{8}') b on a.second_id = b.distinct_id where " \
              "b.distinct_id is not null)c".format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                                                   end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_Android':
        sql = "select count(distinct c.orderid) as count_source from (select a.second_id, a.source, b.orderid from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join " \
              "(select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序'" \
              " else  '微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{5}' and " \
              "'{6}' group by e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where tmp.khd = 'Android')a left join " \
              "(select distinct orderid, distinct_id from events where event = 'PayOrderDetail' and " \
              "date between '{7}' and '{8}') b on a.second_id = b.distinct_id where " \
              "b.distinct_id is not null)c".format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                                                   end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_wechat':
        sql = "select count(distinct c.orderid) as count_source from (select a.second_id, a.source, b.orderid from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id,source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join " \
              "(select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序'" \
              " else  '微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{5}' and " \
              "'{6}' group by e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where tmp.khd = '微信小程序')a left join " \
              "(select distinct orderid, distinct_id from events where event = 'PayOrderDetail' and " \
              "date between '{7}' and '{8}') b on a.second_id = b.distinct_id where " \
              "b.distinct_id is not null)c".format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                                                   end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_baidu':
        sql = "select count(distinct c.orderid) as count_source from (select a.second_id, a.source, b.orderid from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join " \
              "(select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序'" \
              " else  '微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{5}' and " \
              "'{6}' group by e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where tmp.khd = '百度小程序')a left join " \
              "(select distinct orderid, distinct_id from events where event = 'PayOrderDetail' and " \
              "date between '{7}' and '{8}') b on a.second_id = b.distinct_id where " \
              "b.distinct_id is not null)c".format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                                                   end_date_tmp, start_date_tmp, end_date_tmp)
    elif str(source).endswith('KOL') or source in ('小红书', 'channel_kol'):
        sql = "select count(distinct b.orderid) as count_source from (select second_id from users where " \
              "firstordertime >= {0} and firstordertime < {1} and source in ({2}))a left join (select distinct_id, " \
              "orderid from events where event = 'PayOrderDetail' and date between '{3}' and '{4}')b on a.second_id " \
              "= b.distinct_id where b.distinct_id is not null"\
            .format(start_timestamp, end_timestamp, source_by, start_date_tmp, end_date_tmp)
    elif source == '抖音kol':
        register_seller_list = list()
        sql = "select distinct distinct_id from events where event = 'registerSuccessAct' " \
              "and PartnerOldUser is null and regexp_like(distinct_id, '^[0-9]+$') and date between '{0}' " \
              "and '{1}'".format(start_date_tmp, end_date_tmp)
        payload = {'q': sql, 'format': 'json'}
        r = requests.post(url, data=payload)
        if r.status_code == 200:
            datastr = r.text
            if len(datastr) == 0:
                order_user_num = 0
            else:
                dataarr = datastr.split('\n')
                for data in dataarr:
                    try:
                        datajson = json.loads(data)
                        if str(datajson) == '{}':
                            continue
                        register_seller_list.append(datajson['distinct_id'])
                    except json.decoder.JSONDecodeError as identifier:
                        pass
        else:
            print("sa hive sql accur error, sql为%s" % sql)
        cursor = db_aplum.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        start_timestamp_tmp = int(start_timestamp / 1000)
        end_timestamp_tmp = int(end_timestamp / 1000)
        register_seller_list_tmp = "'" + "','".join(str(i) for i in register_seller_list) + "'"
        sql = "SELECT count(td.id) as count_source FROM t_order td where td.status not in ('new','topay','cancel')" \
              " AND td.parent_id='0' and td.order_time>={0} and td.order_time<{1} and user_id in ({2})".format(
            start_timestamp_tmp, end_timestamp_tmp, register_seller_list_tmp
        )
        # print(sql)
        cursor.execute(sql)
        source_data = cursor.fetchall()
        cursor.close()
        register_seller_list.clear()
        for row in source_data:
            # row = process_price(row)
            # print(row)
            order_num = int(row['count_source'])
        return order_num
    elif source == '微信朋友圈':
        sql = "select count(distinct c.orderid) as count_source from (select a.second_id, a.source, a.createtime, b.orderid from " \
              "(select distinct second_id, source, createtime from users where source = 'weixin' and adaccount = 'weixin-rd' and " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select distinct orderid, distinct_id from events where event = 'PayOrderDetail' and " \
              "date between '{2}' and '{3}') b on a.second_id = b.distinct_id where " \
              "b.distinct_id is not null)c".format(start_timestamp, end_timestamp, start_date_tmp,
                                                   end_date_tmp)
    elif source == '微信公众号':
        sql = "select count(distinct c.orderid) as count_source from (select a.second_id, a.source, a.createtime, b.orderid from " \
              "(select distinct second_id, source, createtime from users where source = 'weixin' and adaccount = 'weixin-ios-rdpyq' and " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select distinct orderid, distinct_id from events where event = 'PayOrderDetail' and " \
              "date between '{2}' and '{3}') b on a.second_id = b.distinct_id where " \
              "b.distinct_id is not null)c".format(start_timestamp, end_timestamp, start_date_tmp,
                                                   end_date_tmp)
    else:
        sql = "select count(distinct c.orderid) as count_source from (select a.second_id, a.source, a.createtime, b.orderid from " \
              "(select distinct second_id, source, createtime from users where source in ({0}) and " \
              "createtime >= {1} and createtime < {2})a left join " \
              "(select distinct orderid, distinct_id from events where event = 'PayOrderDetail' and " \
              "date between '{3}' and '{4}') b on a.second_id = b.distinct_id where " \
              "b.distinct_id is not null)c".format(source_by, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            order_num = 0
        else:
            dataarr = datastr.split('\n')
            for data in dataarr:
                try:
                    datajson = json.loads(data)
                    if str(datajson) == '{}':
                        continue
                    order_num = int(datajson['count_source'])
                except json.decoder.JSONDecodeError as identifier:
                    pass
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return order_num


def get_order_user_num(source, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp):
    order_user_num = 0
    if source == 'all':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, a.createtime from " \
              "(select distinct second_id, source, createtime from users where  " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select distinct distinct_id from events where event = 'PayOrderDetail' and date between '{2}' and '{3}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source == 'nature':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select e.distinct_id, " \
              "case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' then 'Android' " \
              "when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from events e where " \
              "e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n on " \
              "m.first_id=n.distinct_id)tmp where tmp.khd in('IOS','Android','微信小程序','百度小程序'))a left join " \
              "(select distinct distinct_id from events where event = 'PayOrderDetail' and date between '{7}' and '{8}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_IOS':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select e.distinct_id, " \
              "case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' then 'Android' " \
              "when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from events e where " \
              "e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n on " \
              "m.first_id=n.distinct_id)tmp where tmp.khd = 'IOS')a left join " \
              "(select distinct distinct_id from events where event = 'PayOrderDetail' and date between '{7}' and '{8}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_Android':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select e.distinct_id, " \
              "case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' then 'Android' " \
              "when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from events e where " \
              "e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n on " \
              "m.first_id=n.distinct_id)tmp where tmp.khd = 'Android')a left join " \
              "(select distinct distinct_id from events where event = 'PayOrderDetail' and date between '{7}' and '{8}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_wechat':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select e.distinct_id, " \
              "case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' then 'Android' " \
              "when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from events e where " \
              "e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n on " \
              "m.first_id=n.distinct_id)tmp where tmp.khd = '微信小程序')a left join " \
              "(select distinct distinct_id from events where event = 'PayOrderDetail' and date between '{7}' and '{8}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_baidu':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select e.distinct_id, " \
              "case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' then 'Android' " \
              "when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from events e where " \
              "e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n on " \
              "m.first_id=n.distinct_id)tmp where tmp.khd = '百度小程序')a left join " \
              "(select distinct distinct_id from events where event = 'PayOrderDetail' and date between '{7}' and '{8}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif str(source).endswith('KOL') or source in ('小红书', 'channel_kol'):
        sql = "select count(b.distinct_id) as count_source from (select second_id from users where " \
              "firstordertime >= {0} and firstordertime < {1} and source in ({2}))a left join (select " \
              "distinct_id from events where event = 'PayOrderDetail' and date between '{3}' and '{4}')b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null"\
            .format(start_timestamp, end_timestamp, source_by, start_date_tmp, end_date_tmp)
    elif source == '抖音kol':
        register_seller_list = list()
        sql = "select distinct distinct_id from events where event = 'registerSuccessAct' " \
              "and PartnerOldUser is null and regexp_like(distinct_id, '^[0-9]+$') and date between '{0}' " \
              "and '{1}'".format(start_date_tmp, end_date_tmp)
        payload = {'q': sql, 'format': 'json'}
        r = requests.post(url, data=payload)
        if r.status_code == 200:
            datastr = r.text
            if len(datastr) == 0:
                order_user_num = 0
            else:
                dataarr = datastr.split('\n')
                for data in dataarr:
                    try:
                        datajson = json.loads(data)
                        if str(datajson) == '{}':
                            continue
                        register_seller_list.append(datajson['distinct_id'])
                    except json.decoder.JSONDecodeError as identifier:
                        pass
        else:
            print("sa hive sql accur error, sql为%s" % sql)
        cursor = db_aplum.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        start_timestamp_tmp = int(start_timestamp / 1000)
        end_timestamp_tmp = int(end_timestamp / 1000)
        register_seller_list_tmp = "'" + "','".join(str(i) for i in register_seller_list) + "'"
        sql = "select count(distinct user_id) as count_source from t_order where status not in ('new','topay','cancel')" \
              " AND parent_id='0'and user_id in ({0}) and " \
              "order_time >= {1} and order_time < {2}".format(
            register_seller_list_tmp, start_timestamp_tmp, end_timestamp_tmp)
        # print(sql)
        cursor.execute(sql)
        source_data = cursor.fetchall()
        cursor.close()
        register_seller_list.clear()
        for row in source_data:
            # row = process_price(row)
            # print(row)
            order_user_num = int(row['count_source'])
        return order_user_num
    elif source == '微信朋友圈':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, a.createtime from " \
              "(select distinct second_id, source, createtime from users where source = 'weixin' and adaccount = 'weixin-rd' and " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select distinct distinct_id from events where event = 'PayOrderDetail' and date between '{2}' and '{3}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source == '微信公众号':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, a.createtime from " \
              "(select distinct second_id, source, createtime from users where source = 'weixin' and adaccount = 'weixin-ios-rdpyq' and " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select distinct distinct_id from events where event = 'PayOrderDetail' and date between '{2}' and '{3}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    else:
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, a.createtime from " \
              "(select distinct second_id, source, createtime from users where source in ({0}) and " \
              "createtime >= {1} and createtime < {2})a left join " \
              "(select distinct distinct_id from events where event = 'PayOrderDetail' and date between '{3}' and '{4}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(source_by, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            order_user_num = 0
        else:
            dataarr = datastr.split('\n')
            for data in dataarr:
                try:
                    datajson = json.loads(data)
                    if str(datajson) == '{}':
                        continue
                    order_user_num = int(datajson['count_source'])
                except json.decoder.JSONDecodeError as identifier:
                    pass
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return order_user_num


def get_register_num(source, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp):
    register_num = 0
    if source == 'all':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, a.createtime from " \
              "(select distinct second_id, source, createtime from users where createtime >= {0} " \
              "and createtime < {1})a  left join (select * from events where event = '$SignUp' and date between '{2}' " \
              "and '{3}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source == 'nature':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select e.distinct_id, " \
              "case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' then 'Android' " \
              "when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from events e where " \
              "e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n on " \
              "m.first_id=n.distinct_id)tmp where tmp.khd in('IOS','Android','微信小程序','百度小程序'))a  " \
              "left join (select * from events where event = '$SignUp' and date between '{7}' " \
              "and '{8}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_IOS':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select e.distinct_id, " \
              "case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' then 'Android' " \
              "when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from events e where " \
              "e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n on " \
              "m.first_id=n.distinct_id)tmp where tmp.khd = 'IOS')a  " \
              "left join (select * from events where event = '$SignUp' and date between '{7}' " \
              "and '{8}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_Android':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select e.distinct_id, " \
              "case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' then 'Android' " \
              "when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from events e where " \
              "e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n on " \
              "m.first_id=n.distinct_id)tmp where tmp.khd = 'Android')a  " \
              "left join (select * from events where event = '$SignUp' and date between '{7}' " \
              "and '{8}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_wechat':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select e.distinct_id, " \
              "case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' then 'Android' " \
              "when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from events e where " \
              "e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n on " \
              "m.first_id=n.distinct_id)tmp where tmp.khd = '微信小程序')a  " \
              "left join (select * from events where event = '$SignUp' and date between '{7}' " \
              "and '{8}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == 'nature_baidu':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "source not in({2}) union select first_id, source, second_id from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m left join (select e.distinct_id, " \
              "case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' then 'Android' " \
              "when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from events e where " \
              "e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by e.distinct_id,khd)n on " \
              "m.first_id=n.distinct_id)tmp where tmp.khd = '百度小程序')a  " \
              "left join (select * from events where event = '$SignUp' and date between '{7}' " \
              "and '{8}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp,
                    end_date_tmp, start_date_tmp, end_date_tmp)
    elif source == '微信朋友圈':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, a.createtime from " \
              "(select distinct second_id, source, createtime from users where source = 'weixin' and adaccount = 'weixin-rd' and createtime >= {0} " \
              "and createtime < {1})a  left join (select * from events where event = '$SignUp' and date between '{2}' " \
              "and '{3}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source == '微信公众号':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, a.createtime from " \
              "(select distinct second_id, source, createtime from users where source = 'weixin' and adaccount = 'weixin-ios-rdpyq' and createtime >= {0} " \
              "and createtime < {1})a  left join (select * from events where event = '$SignUp' and date between '{2}' " \
              "and '{3}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source == '抖音kol':
        sql = "select count(distinct distinct_id) as count_source from events where event = 'registerSuccessAct' " \
              "and PartnerOldUser is null and regexp_like(distinct_id, '^[0-9]+$') and date between '{0}' " \
              "and '{1}'".format(start_date_tmp, end_date_tmp)
    else:
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, a.createtime from " \
              "(select distinct second_id, source, createtime from users where source in ({0}) and createtime >= {1} " \
              "and createtime < {2})a  left join (select * from events where event = '$SignUp' and date between '{3}' " \
              "and '{4}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(source_by, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            register_num = 0
        else:
            dataarr = datastr.split('\n')
            for data in dataarr:
                try:
                    datajson = json.loads(data)
                    if str(datajson) == '{}':
                        continue
                    register_num = int(datajson['count_source'])
                except json.decoder.JSONDecodeError as identifier:
                    pass
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return register_num


def get_day_add_activate(source, source_by, url, start_timestamp, end_timestamp):
    count_source = 0
    start_timestamp_tmp = int(end_timestamp - 24 * 3600 * 1000)
    if source == 'all':
        sql = "select count(*) as count_source from users where  " \
              "createtime >= {0} and createtime < {1}".format(start_timestamp_tmp, end_timestamp)
    elif source == 'nature':
        sql = "select count(*) as count_source from (select * from( select u.first_id, u.source from (select first_id, second_id," \
              "source from users where createtime >= {0} and createtime < {1} and source not in({2}) union select " \
              "first_id, second_id,source from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m " \
              "left join (select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  " \
              "'微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by " \
              "e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where " \
              "tmp.khd in('IOS','Android','微信小程序','百度小程序')".format(start_timestamp_tmp, end_timestamp, source_by,
                                                                   start_timestamp_tmp, end_timestamp,
                                                                   start_date_tmp, end_date_tmp)
    elif source == 'nature_IOS':
        sql = "select count(*) as count_source from (select * from( select u.first_id, u.source from (select first_id, second_id," \
              "source from users where createtime >= {0} and createtime < {1} and source not in({2}) union select " \
              "first_id, second_id,source from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m " \
              "left join (select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  " \
              "'微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by " \
              "e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where " \
              "tmp.khd = 'IOS'".format(start_timestamp_tmp, end_timestamp, source_by, start_timestamp_tmp, end_timestamp,
                                       start_date_tmp, end_date_tmp)
    elif source == 'nature_Android':
        sql = "select count(*) as count_source from (select * from( select u.first_id, u.source from (select first_id, second_id," \
              "source from users where createtime >= {0} and createtime < {1} and source not in({2}) union select " \
              "first_id,second_id, source from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m " \
              "left join (select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  " \
              "'微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by " \
              "e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where " \
              "tmp.khd = 'Android'".format(start_timestamp_tmp, end_timestamp, source_by, start_timestamp_tmp, end_timestamp,
                                           start_date_tmp, end_date_tmp)
    elif source == 'nature_wechat':
        sql = "select count(*) as count_source from (select * from( select u.first_id, u.source from (select first_id, second_id, " \
              "source from users where createtime >= {0} and createtime < {1} and source not in({2}) union select " \
              "first_id, second_id, source from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m " \
              "left join (select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  " \
              "'微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by " \
              "e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where " \
              "tmp.khd = '微信小程序'".format(start_timestamp_tmp, end_timestamp, source_by, start_timestamp_tmp, end_timestamp,
                                         start_date_tmp, end_date_tmp)
    elif source == 'nature_baidu':
        sql = "select count(*) as count_source from (select * from( select u.first_id, u.source from (select first_id, second_id, " \
              "source from users where createtime >= {0} and createtime < {1} and source not in({2}) union select " \
              "first_id, second_id, source from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m " \
              "left join (select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  " \
              "'微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by " \
              "e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where " \
              "tmp.khd = '百度小程序'".format(start_timestamp_tmp, end_timestamp, source_by, start_timestamp_tmp, end_timestamp,
                                         start_date_tmp, end_date_tmp)
    elif source == '微信朋友圈':
        sql = "select count(*) as count_source from users where createtime >= {0} and createtime < {1}" \
              " and source = 'weixin' and adaccount = 'weixin-rd'".format(start_timestamp_tmp, end_timestamp)
    elif source == '微信公众号':
        sql = "select count(*) as count_source from users where createtime >= {0} and createtime < {1}" \
              " and source = 'weixin' and adaccount = 'weixin-ios-rdpyq'".format(start_timestamp_tmp, end_timestamp)
    elif source == '抖音kol':
        sql = "select count(distinct distinct_id) as count_source from events where event = 'registerSuccessAct' " \
              "and PartnerOldUser is null and regexp_like(distinct_id, '^[0-9]+$') and date = '{0}'".format(end_date_tmp)
    else:
        sql = "select count(*) as count_source from users where source in ({0}) and " \
              "createtime >= {1} and createtime < {2}".format(source_by, start_timestamp_tmp, end_timestamp)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            count_source = 0
        else:
            dataarr = datastr.split('\n')
            for data in dataarr:
                try:
                    datajson = json.loads(data)
                    if str(datajson) == '{}':
                        continue
                    count_source = int(datajson['count_source'])
                except json.decoder.JSONDecodeError as identifier:
                    pass
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return count_source


def get_add_activate(source, source_by, url, start_timestamp, end_timestamp):
    count_source = 0
    if source == 'all':
        sql = "select count(*) as count_source from users where  " \
          "createtime >= {0} and createtime < {1}".format(start_timestamp, end_timestamp)
    elif source == 'nature':
        sql = "select count(*) as count_source from (select * from( select u.first_id, u.source from (select first_id, second_id," \
              "source from users where createtime >= {0} and createtime < {1} and source not in({2}) union select " \
              "first_id, second_id,source from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m " \
              "left join (select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  " \
              "'微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by " \
              "e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where " \
              "tmp.khd in('IOS','Android','微信小程序','百度小程序')".format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp,
                                       start_date_tmp, end_date_tmp)
    elif source == 'nature_IOS':
        sql = "select count(*) as count_source from (select * from( select u.first_id, u.source from (select first_id, second_id," \
              "source from users where createtime >= {0} and createtime < {1} and source not in({2}) union select " \
              "first_id, second_id,source from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m " \
              "left join (select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  " \
              "'微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by " \
              "e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where " \
              "tmp.khd = 'IOS'".format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source == 'nature_Android':
        sql = "select count(*) as count_source from (select * from( select u.first_id, u.source from (select first_id, second_id," \
              "source from users where createtime >= {0} and createtime < {1} and source not in({2}) union select " \
              "first_id,second_id, source from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m " \
              "left join (select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  " \
              "'微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by " \
              "e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where " \
              "tmp.khd = 'Android'".format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source == 'nature_wechat':
        sql = "select count(*) as count_source from (select * from( select u.first_id, u.source from (select first_id, second_id, " \
              "source from users where createtime >= {0} and createtime < {1} and source not in({2}) union select " \
              "first_id, second_id, source from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m " \
              "left join (select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  " \
              "'微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by " \
              "e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where " \
              "tmp.khd = '微信小程序'".format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source == 'nature_baidu':
        sql = "select count(*) as count_source from (select * from( select u.first_id, u.source from (select first_id, second_id, " \
              "source from users where createtime >= {0} and createtime < {1} and source not in({2}) union select " \
              "first_id, second_id, source from users where createtime >= {3} and " \
              "createtime < {4} and source is null)u  group by u.first_id,u.second_id,u.source) m " \
              "left join (select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  " \
              "'微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{5}' and '{6}' group by " \
              "e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where " \
              "tmp.khd = '百度小程序'".format(start_timestamp, end_timestamp, source_by, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source == '微信朋友圈':
        sql = "select count(*) as count_source from users where createtime >= {0} and createtime < {1}" \
              " and source = 'weixin' and adaccount = 'weixin-rd'".format(start_timestamp, end_timestamp)
    elif source == '微信公众号':
        sql = "select count(*) as count_source from users where createtime >= {0} and createtime < {1}" \
              " and source = 'weixin' and adaccount = 'weixin-ios-rdpyq'".format(start_timestamp, end_timestamp)
    elif source == '抖音kol':
        sql = "select count(distinct distinct_id) as count_source from events where event = 'registerSuccessAct' " \
              "and PartnerOldUser is null and regexp_like(distinct_id, '^[0-9]+$') and date between '{0}' " \
              "and '{1}'".format(start_date_tmp, end_date_tmp)
    else:
        sql = "select count(*) as count_source from users where source in ({0}) and " \
            "createtime >= {1} and createtime < {2}".format(source_by, start_timestamp, end_timestamp)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            count_source = 0
        else:
            dataarr = datastr.split('\n')
            for data in dataarr:
                try:
                    datajson = json.loads(data)
                    if str(datajson) == '{}':
                        continue
                    count_source = int(datajson['count_source'])
                except json.decoder.JSONDecodeError as identifier:
                    pass
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    return count_source


def get_costs_by_sql(db_market, sql_costs):
    costs = 0.0
    cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    cursor.execute(sql_costs)
    source_data = cursor.fetchall()
    cursor.close()
    for row in source_data:
        if row['sum_costs'] is None:
            costs = 0.0
        else:
            costs = float(row['sum_costs'])
    return costs


def get_source(db_market, source_dict):
    cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    sql = "select id, name, second_name, source from t_market_source"
    cursor.execute(sql)
    source_data = cursor.fetchall()
    cursor.close()
    for row in source_data:
        # row = process_price(row)
        id = str(row['id'])
        source_dict.update({id: row})
    return source_dict


def save_dict_to_csv(dict_sum):
    # line = ''
    today = date.today()
    with open('/home/aplum/work_lh/data_dict_to_csv/{0}-day-dict.csv'.format(today), 'a+', newline='') as file:
        # for x in dict_sum.items():
        #     file.write(str(x) + '\n')

        file.write(str(dict_sum) + '\n')


def get_all_data(db_aplum, source, costs, source_by, start_date_tmp, end_date_tmp, start_timestamp,
                           end_timestamp):
    result_dict = dict()
    # 来源
    # source = str(source_dict[k]['second_name'])
    # 当前月份
    month = start_date_tmp
    # 当前月份花费
    # costs = float(get_costs_by_source_and_month(source_dict, costs_dict, source, date_tmp))
    # 当日激活
    current_day_activate = int(get_day_add_activate(source, source_by, url, start_timestamp, end_timestamp))
    # 新增激活
    add_activate = int(get_add_activate(source, source_by, url, start_timestamp, end_timestamp))
    # 注册数
    register_num = int(get_register_num(source, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp))
    # 订单用户数
    order_user_num = int(
        get_order_user_num(source, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp))
    # 订单数
    order_num = int(get_order_num(source, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp))
    # 订单金额
    order_sum_realpayprice = float(
        get_order_sum_realpayprice(source, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp))
    # 注册卖家数
    register_seller_num = int(
        get_register_seller_num(source, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp))
    # 寄出卖家数
    post_seller_num = int(
        get_post_seller_num(source, db_aplum, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp))
    # 当月卖家收入
    month_seller_income = round(float(
        get_month_seller_income(source, db_aplum, source_by, url, start_timestamp, end_timestamp, start_date_tmp,
                                end_date_tmp)),
        2)
    order_user_post_seller = order_user_num + post_seller_num
    if add_activate == 0:
        register_rate = 0.00
        order_user_trans_rate = 0.00
    else:
        register_rate = int(register_num / add_activate * 10000)
        order_user_trans_rate = int(order_num / add_activate * 10000)

    if order_num == 0:
        customer_cost = 0.00
    else:
        customer_cost = round((order_sum_realpayprice / order_num), 2)

    if register_seller_num == 0:
        register_post_trans_rate = 0.00
    else:
        if post_seller_num == 0:
            register_post_trans_rate = 0.00
        else:
            register_post_trans_rate = int(post_seller_num / register_seller_num * 10000)

    key = str(source) + '&' + str(date.today() + timedelta(days=-1))
    result_dict[key] = dict()
    result_dict[key]['source'] = source
    result_dict[key]['month'] = str(date.today() + timedelta(days=-1))
    result_dict[key]['costs'] = costs
    result_dict[key]['today_actived_num'] = current_day_activate
    result_dict[key]['new_actived_user'] = add_activate
    result_dict[key]['avg_actived_costs'] = 0.0
    result_dict[key]['new_registered_user'] = register_num
    result_dict[key]['avg_registered_costs'] = 0.0
    result_dict[key]['avg_registered_rate'] = register_rate
    result_dict[key]['new_ordered_user'] = order_user_num
    result_dict[key]['avg_ordered_user_costs'] = 0.0
    result_dict[key]['avg_ordered_rate'] = order_user_trans_rate
    result_dict[key]['new_ordered_num'] = order_num
    result_dict[key]['avg_ordered_costs'] = 0.0
    result_dict[key]['kdj_costs'] = customer_cost
    result_dict[key]['order_costs'] = order_sum_realpayprice
    result_dict[key]['roi'] = 0.0
    result_dict[key]['new_seller'] = register_seller_num
    result_dict[key]['avg_seller_costs'] = 0.0
    result_dict[key]['new_jcmjs'] = post_seller_num
    result_dict[key]['avg_jcmjs_costs'] = 0.0
    result_dict[key]['avg_jcmjs_rate'] = register_post_trans_rate
    result_dict[key]['dyscje'] = month_seller_income
    result_dict[key]['seller_roi'] = 0.0
    result_dict[key]['ddyj_mjjc'] = order_user_post_seller
    result_dict[key]['mmjpjcb'] = 0.0
    fields = ['source', 'month', 'costs', 'today_actived_num', 'new_actived_user', 'avg_actived_costs',
              'new_registered_user', 'avg_registered_costs', 'avg_registered_rate', 'new_ordered_user', 'avg_ordered_user_costs'
              , 'avg_ordered_rate', 'new_ordered_num', 'avg_ordered_costs', 'kdj_costs', 'order_costs', 'roi',
              'new_seller', 'avg_seller_costs', 'new_jcmjs', 'avg_jcmjs_costs'
              , 'avg_jcmjs_rate', 'dyscje', 'seller_roi', 'ddyj_mjjc', 'mmjpjcb']
    print(result_dict)
    save_dict_to_csv(result_dict)


def get_source_by_by_sql(db_market, sql):
    source_list_tmp = list()
    cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    cursor.execute(sql)
    source_data = cursor.fetchall()
    cursor.close()
    for row in source_data:
        if row['source'] is None:
            continue
        source_list_tmp.append(str(row['source']))
    if len(source_list_tmp) == 0:
        return ''

    if len(source_list_tmp) == 1:
        source_by = "'" + str(source_list_tmp[0]) + "'"
    else:
        source_by = "'" + "','".join(str(i) for i in source_list_tmp) + "'"
    return source_by


if __name__ == '__main__':
    result_dict_list = list()
    source_dict = dict()
    db_market = MySQLdb.connect(mysql_host, mysql_user, mysql_passwd, mysql_db, charset='utf8')
    db_aplum = MySQLdb.connect(mysqlhost, mysqlusername, mysqlpasswd, db, charset='utf8')
    source_dict = get_source(db_market, source_dict)
    source_list = ['all', 'nature', 'nature_IOS', 'nature_Android', 'nature_wechat', 'nature_baidu', 'channel_all',
                   'channel_flow', 'channel_kol']
    # source_list = ['微信朋友圈', '微信公众号']
    for k in source_dict.keys():
        # 来源
        source = str(source_dict[k]['second_name'])
        if source in source_list:
            continue
        else:
            source_list.append(source)
    print("source_list:" + str(len(source_list)))
    today = date.today()
    end_date_tmp = today + timedelta(days=-1)
    start_date_tmp = str(end_date_tmp.replace(day=1))
    print(start_date_tmp)
    start_timestamp = int(
        time.mktime(time.strptime('{0} 00:00:00'.format(start_date_tmp), '%Y-%m-%d %H:%M:%S')) * 1000)
    end_timestamp = int(
        time.mktime(time.strptime('{0} 00:00:00'.format(today), '%Y-%m-%d %H:%M:%S')) * 1000)

    for source in source_list:
        # date_tmp = date_tmp + '-01'
        print(source)
        costs = 0.00
        source_list_tmp = list()
        sid_list = list()
        if source == 'all':
            # all统计不需要过滤source字段, source_by给空是为了调用该方法
            source_by = ''
            sql_costs = "select sum(costs) as sum_costs from t_market_cost where costs_date = '{0}'"\
                .format(start_date_tmp)
            costs = get_costs_by_sql(db_market, sql_costs)
            get_all_data(db_aplum, source, costs, source_by,
                                    start_date_tmp,
                                    end_date_tmp,
                                    start_timestamp, end_timestamp)
        elif source in ('channel_all', 'nature', 'nature_IOS', 'nature_Android',
                        'nature_wechat', 'nature_baidu'):
            # sql_source = "select source from t_market_source "
            # source_by = get_source_by_by_sql(db_market, sql_source)
            sql_source = "select source from t_market_cost where source in (select source from t_market_source " \
                         "where name = '信息流') union (select source from t_market_cost where source in (select source " \
                         "from t_market_source where name = '博主kol') and costs_date = '{0}')".format(start_date_tmp)
            source_by = get_source_by_by_sql(db_market, sql_source)
            if source == 'channel_all':
                sql_costs = "select sum(costs) as sum_costs from t_market_cost where costs_date = '{0}'" \
                    .format(start_date_tmp)
                costs = get_costs_by_sql(db_market, sql_costs)
            else:
                costs = 0.0
            get_all_data(db_aplum, source, costs, source_by,
                                    start_date_tmp,
                                    end_date_tmp,
                                    start_timestamp, end_timestamp)
        elif source == 'channel_kol':
            sql_source = "select source from t_market_cost where source in (select source from t_market_source " \
                         "where name = '博主kol') and costs_date = '{0}'".format(start_date_tmp)
            # print(sql_source)
            source_by = get_source_by_by_sql(db_market, sql_source)
            if source_by == '':
                continue
            sql_costs = "select sum(costs) as sum_costs from t_market_cost where costs_date = '{0}' " \
                        "and source in ({1})" \
                .format(start_date_tmp, source_by)
            costs = get_costs_by_sql(db_market, sql_costs)
            get_all_data(db_aplum, source, costs, source_by,
                                    start_date_tmp, end_date_tmp,
                                    start_timestamp, end_timestamp)
        elif source == 'channel_flow':
            sql_source = "select source from t_market_source where name = '信息流'"
            source_by = get_source_by_by_sql(db_market, sql_source)
            sql_costs = "select sum(costs) as sum_costs from t_market_cost where costs_date = '{0}' " \
                        "and source in ({1})" \
                .format(start_date_tmp, source_by)
            costs = get_costs_by_sql(db_market, sql_costs)
            get_all_data(db_aplum, source, costs, source_by,
                                    start_date_tmp, end_date_tmp,
                                    start_timestamp, end_timestamp)
        elif source == '抖音kol':
            sql_costs = "select sum(costs) as sum_costs from t_market_cost where source in(select source from " \
                        "t_market_source where second_name = '抖音kol') and " \
                        "costs_date = '{0}'" \
                .format(start_date_tmp)
            costs = get_costs_by_sql(db_market, sql_costs)
            if costs == 0.0:
                continue
            source_by = ''
            get_all_data(db_aplum, source, costs, source_by, start_date_tmp, end_date_tmp, start_timestamp, end_timestamp)
        else:
            if str(source).endswith('KOL'):
                sql_source = "select source from t_market_cost where source in (select source from t_market_source " \
                             "where second_name = '{0}') and costs_date = '{1}'".format(source, start_date_tmp)
                source_by = get_source_by_by_sql(db_market, sql_source)
                if source_by == '':
                    continue
            else:
                sql_source = "select source from t_market_source where second_name = '{0}'".format(source)
                source_by = get_source_by_by_sql(db_market, sql_source)
            sql_costs = "select sum(costs) as sum_costs from t_market_cost where costs_date = '{0}' " \
                        "and source in ({1})" \
                .format(start_date_tmp, source_by)
            costs = get_costs_by_sql(db_market, sql_costs)
            get_all_data(db_aplum, source, costs, source_by, start_date_tmp, end_date_tmp, start_timestamp, end_timestamp)