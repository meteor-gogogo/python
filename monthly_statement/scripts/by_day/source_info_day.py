#!/usr/bin/env python
# coding=utf-8
from datetime import datetime, timedelta, date
import requests
import time
import json
import getopt
from sqlalchemy import create_engine
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


def get_month_seller_income(nature_dict, source, aplum_cursor, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp):
    register_seller_list = list()
    month_seller_income = 0.00
    if source == 'all':
        sql = "select c.second_id from (select a.second_id, a.source, a.createtime, b.distinct_id from " \
              "(select distinct second_id, source, createtime from users where  " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{2}' " \
              "and '{3}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source in ('nature', 'nature_Android', 'nature_IOS', 'nature_wechat', 'nature_baidu'):
        sql = "select c.second_id from (select a.second_id, a.source, b.distinct_id from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and (source " \
              "not in({2}) or source is null))u  group by u.first_id,u.second_id,u.source) m left join (select " \
              "e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' " \
              "then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from " \
              "events e where e.event='GeneralOpenApp' and date between '{3}' and '{4}' group by e.distinct_id,khd)n " \
              "on m.first_id=n.distinct_id)tmp where tmp.khd in ({5}))a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{6}' " \
              "and '{7}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_date_tmp, end_date_tmp, nature_dict[source],
                    start_date_tmp, end_date_tmp)
    elif source == '抖音kol':
        sql = "select c.distinct_id from (select a.distinct_id from " \
              "(select distinct distinct_id from events where event = 'registerSuccessAct' and PartnerOldUser " \
              "is null and regexp_like(distinct_id, '^[0-9]+$') and date between '{0}' and '{1}' and partner in ({4}))" \
              "a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{2}' " \
              "and '{3}') b on a.distinct_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_date_tmp, end_date_tmp, start_date_tmp, end_date_tmp, source_by)
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
            # cursor = db_aplum.cursor(cursorclass=MySQLdb.cursors.DictCursor)
            start_timestamp_tmp = int(start_timestamp / 1000)
            end_timestamp_tmp = int(end_timestamp / 1000)
            register_seller_list_tmp = "'" + "','".join(str(i) for i in register_seller_list) + "'"
            sql = "select sum(a.sum_income) as count_source from (select tp.user_id, sum(tdi.settle_price) as " \
                  "sum_income FROM t_order td LEFT JOIN t_order_item tdi on tdi.order_id=td.id left join t_product tp " \
                  "on tdi.product_id=tp.id where td.status not in ('new','topay','cancel') and td.splitted='0' and " \
                  "td.order_time >= {0} and td.order_time < {1} and tp.user_id in ({2}) " \
                  "group by tp.user_id)a".format(start_timestamp_tmp, end_timestamp_tmp, register_seller_list_tmp)
            # print(sql)
            aplum_cursor.execute(sql)
            source_data = aplum_cursor.fetchall()

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
        # cursor = db_aplum.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        start_timestamp_tmp = int(start_timestamp / 1000)
        end_timestamp_tmp = int(end_timestamp / 1000)
        register_seller_list_tmp = "'" + "','".join(str(i) for i in register_seller_list) + "'"
        sql = "select sum(a.sum_income) as count_source from (select tp.user_id, sum(tdi.settle_price) as " \
              "sum_income FROM t_order td LEFT JOIN t_order_item tdi on tdi.order_id=td.id left join t_product tp " \
              "on tdi.product_id=tp.id where td.status not in ('new','topay','cancel') and td.splitted='0' and " \
              "td.order_time >= {0} and td.order_time < {1} and tp.user_id in ({2}) " \
              "group by tp.user_id)a".format(start_timestamp_tmp, end_timestamp_tmp, register_seller_list_tmp)
        # print(sql)
        aplum_cursor.execute(sql)
        source_data = aplum_cursor.fetchall()

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


def get_post_seller_num(nature_dict, source, aplum_cursor, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp):
    post_seller_num = 0
    register_seller_list = list()
    if source == 'all':
        sql = "select c.second_id from (select a.second_id, a.source, a.createtime, b.distinct_id from " \
              "(select distinct second_id, source, createtime from users where  " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{2}' " \
              "and '{3}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source in ('nature', 'nature_Android', 'nature_IOS', 'nature_wechat', 'nature_baidu'):
        sql = "select c.second_id from (select a.second_id, a.source, b.distinct_id from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "(source not in({2}) or source is null))u  group by u.first_id,u.second_id,u.source) m left join (select " \
              "e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' " \
              "then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from " \
              "events e where e.event='GeneralOpenApp' and date between '{3}' and '{4}' group by e.distinct_id,khd)n " \
              "on m.first_id=n.distinct_id)tmp where tmp.khd in ({5}))a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{6}' " \
              "and '{7}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_date_tmp, end_date_tmp, nature_dict[source],
                    start_date_tmp, end_date_tmp)
    elif source == '抖音kol':
        sql = "select c.distinct_id from (select a.distinct_id from " \
              "(select distinct distinct_id from events where event = 'registerSuccessAct' and PartnerOldUser is null " \
              "and regexp_like(distinct_id, '^[0-9]+$') and date between '{0}' and '{1}' and partner in ({4}))a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{2}' " \
              "and '{3}') b on a.distinct_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_date_tmp, end_date_tmp, start_date_tmp, end_date_tmp, source_by)
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
            # cursor = db_aplum.cursor(cursorclass=MySQLdb.cursors.DictCursor)
            start_timestamp_tmp = int(start_timestamp / 1000)
            end_timestamp_tmp = int(end_timestamp / 1000)
            register_seller_list_tmp = "'" + "','".join(str(i) for i in register_seller_list) + "'"
            sql = "select count(distinct user_id) as count_source from t_seller_express where user_id in ({0}) and " \
                  "create_time >= {1} and create_time < {2} and express_no is not null".format(
                register_seller_list_tmp, start_timestamp_tmp, end_timestamp_tmp)
            # print(sql)
            aplum_cursor.execute(sql)
            source_data = aplum_cursor.fetchall()

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
        # cursor = db_aplum.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        start_timestamp_tmp = int(start_timestamp / 1000)
        end_timestamp_tmp = int(end_timestamp / 1000)
        register_seller_list_tmp = "'" + "','".join(str(i) for i in register_seller_list) + "'"
        sql = "select count(distinct user_id) as count_source from t_seller_express where user_id in ({0}) and " \
              "create_time >= {1} and create_time < {2} and express_no is not null".format(
            register_seller_list_tmp, start_timestamp_tmp, end_timestamp_tmp)
        # print(sql)
        aplum_cursor.execute(sql)
        source_data = aplum_cursor.fetchall()
        # cursor.close()
        register_seller_list.clear()
        for row in source_data:
            # row = process_price(row)
            # print(row)
            post_seller_num = int(row['count_source'])
    return post_seller_num


def get_register_seller_num(nature_dict, source, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp):
    if source == 'all':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, a.createtime, b.distinct_id from " \
              "(select distinct second_id, source, createtime from users where " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{2}' " \
              "and '{3}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source in ('nature', 'nature_Android', 'nature_IOS', 'nature_wechat', 'nature_baidu'):
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, b.distinct_id from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "(source not in({2}) or source is null))u  group by u.first_id,u.second_id,u.source) m left join (select " \
              "e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' " \
              "then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from " \
              "events e where e.event='GeneralOpenApp' and date between '{3}' and '{4}' group by e.distinct_id,khd)n " \
              "on m.first_id=n.distinct_id)tmp where tmp.khd in ({5}))a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{6}' " \
              "and '{7}') b on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_date_tmp, end_date_tmp, nature_dict[source],
                    start_date_tmp, end_date_tmp)
    elif source == '抖音kol':
        sql = "select count(*) as count_source from (select distinct a.distinct_id from " \
              "(select distinct distinct_id from events where event = 'registerSuccessAct' and PartnerOldUser " \
              "is null and regexp_like(distinct_id, '^[0-9]+$') and date between '{0}' and '{1}' and partner in ({4}))" \
              "a left join " \
              "(select distinct distinct_id from events where event = 'SellerJoin' and date between '{2}' " \
              "and '{3}') b on a.distinct_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_date_tmp, end_date_tmp, start_date_tmp, end_date_tmp, source_by)
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

    register_seller_num = get_count_source_by_sql(sql, url)
    return register_seller_num


def get_order_sum_realpayprice(nature_dict, aplum_cursor, source, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp):
    order_sum_realpayprice = 0.00
    if source == 'all':
        sql = "select sum(c.orderitem_realpayprice) sum_realpay from (select a.second_id, a.source, a.createtime, " \
              "b.orderitem_realpayprice from (select distinct second_id, source, createtime from users where " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select * from events where event = 'PayOrderDetail' and date between '{2}' and '{3}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source in ('nature', 'nature_Android', 'nature_IOS', 'nature_wechat', 'nature_baidu'):
        sql = "select sum(c.orderitem_realpayprice) sum_realpay from (select a.second_id, a.source,  " \
              "b.orderitem_realpayprice from (select tmp.second_id, tmp.source from (select * from( select u.first_id, " \
              "u.source, u.second_id from (select first_id, source, second_id from users where createtime >= {0} " \
              "and createtime < {1} and (source not in({2}) or source is null))u  group by u.first_id,u.second_id,u.source) m " \
              "left join (select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  " \
              "'微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{3}' and '{4}' " \
              "group by e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where tmp.khd in ({5}))a left join " \
              "(select * from events where event = 'PayOrderDetail' and date between '{6}' and '{7}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_date_tmp, end_date_tmp, nature_dict[source],
                    start_date_tmp, end_date_tmp)
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
              "and '{1}' and partner in ({2})".format(start_date_tmp, end_date_tmp, source_by)
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
        # cursor = db_aplum.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        start_timestamp_tmp = int(start_timestamp / 1000)
        end_timestamp_tmp = int(end_timestamp / 1000)
        register_seller_list_tmp = "'" + "','".join(str(i) for i in register_seller_list) + "'"
        sql = "SELECT sum(td.realpay_price) as count_source FROM t_order td where td.status not in " \
              "('new','topay','cancel') AND td.parent_id='0' and td.order_time>={0} and " \
              "td.order_time<{1} and user_id in ({2})".format(start_timestamp_tmp, end_timestamp_tmp, register_seller_list_tmp)
        # print(sql)
        aplum_cursor.execute(sql)
        source_data = aplum_cursor.fetchall()

        register_seller_list.clear()
        for row in source_data:
            # row = process_price(row)
            # print(row)
            if row['count_source'] is None:
                order_sum_realpayprice = 0.0
            else:
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


def get_order_num(nature_dict, aplum_cursor, source, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp):
    order_num = 0
    if source == 'all':
        sql = "select count(distinct c.orderid) as count_source from (select a.second_id, a.source, a.createtime, b.orderid from " \
              "(select distinct second_id, source, createtime from users where  " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select distinct orderid, distinct_id from events where event = 'PayOrderDetail' and " \
              "date between '{2}' and '{3}') b on a.second_id = b.distinct_id where " \
              "b.distinct_id is not null)c".format(start_timestamp, end_timestamp, start_date_tmp,
                                                   end_date_tmp)
    elif source in ('nature', 'nature_Android', 'nature_IOS', 'nature_wechat', 'nature_baidu'):
        sql = "select count(distinct c.orderid) as count_source from (select a.second_id, a.source, b.orderid from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "(source not in({2}) or source is null))u  group by u.first_id,u.second_id,u.source) m left join " \
              "(select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序'" \
              " else  '微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{3}' and " \
              "'{4}' group by e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where tmp.khd in({5}))a left join " \
              "(select distinct orderid, distinct_id from events where event = 'PayOrderDetail' and " \
              "date between '{6}' and '{7}') b on a.second_id = b.distinct_id where " \
              "b.distinct_id is not null)c".format(start_timestamp, end_timestamp, source_by, start_date_tmp,
                                                   end_date_tmp, nature_dict[source], start_date_tmp, end_date_tmp)
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
              "and '{1}' and partner in ({2})".format(start_date_tmp, end_date_tmp, source_by)
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
        # cursor = db_aplum.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        start_timestamp_tmp = int(start_timestamp / 1000)
        end_timestamp_tmp = int(end_timestamp / 1000)
        register_seller_list_tmp = "'" + "','".join(str(i) for i in register_seller_list) + "'"
        sql = "SELECT count(td.id) as count_source FROM t_order td where td.status not in ('new','topay','cancel')" \
              " AND td.parent_id='0' and td.order_time>={0} and td.order_time<{1} and user_id in ({2})".format(
            start_timestamp_tmp, end_timestamp_tmp, register_seller_list_tmp
        )
        # print(sql)
        aplum_cursor.execute(sql)
        source_data = aplum_cursor.fetchall()

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

    order_num = get_count_source_by_sql(sql, url)
    return order_num


def get_order_user_num(nature_dict, aplum_cursor, source, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp):
    order_user_num = 0
    if source == 'all':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, a.createtime from " \
              "(select distinct second_id, source, createtime from users where  " \
              "createtime >= {0} and createtime < {1})a left join " \
              "(select distinct distinct_id from events where event = 'PayOrderDetail' and date between '{2}' and '{3}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source in ('nature', 'nature_Android', 'nature_IOS', 'nature_wechat', 'nature_baidu'):
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "(source not in({2}) or source is null))u  group by u.first_id,u.second_id,u.source) m left join (select e.distinct_id, " \
              "case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' then 'Android' " \
              "when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from events e where " \
              "e.event='GeneralOpenApp' and date between '{3}' and '{4}' group by e.distinct_id,khd)n on " \
              "m.first_id=n.distinct_id)tmp where tmp.khd in({5}))a left join " \
              "(select distinct distinct_id from events where event = 'PayOrderDetail' and date between '{6}' and '{7}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_date_tmp, end_date_tmp, nature_dict[source],
                    start_date_tmp, end_date_tmp)
    elif str(source).endswith('KOL') or source in ('小红书', 'channel_kol'):
        sql = "select count(distinct b.distinct_id) as count_source from (select second_id from users where " \
              "firstordertime >= {0} and firstordertime < {1} and source in ({2}))a left join (select " \
              "distinct_id from events where event = 'PayOrderDetail' and date between '{3}' and '{4}')b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null"\
            .format(start_timestamp, end_timestamp, source_by, start_date_tmp, end_date_tmp)
    elif source == '抖音kol':
        register_seller_list = list()
        sql = "select distinct distinct_id from events where event = 'registerSuccessAct' " \
              "and PartnerOldUser is null and regexp_like(distinct_id, '^[0-9]+$') and date between '{0}' " \
              "and '{1}' and partner in ({2})".format(start_date_tmp, end_date_tmp, source_by)
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

        start_timestamp_tmp = int(start_timestamp / 1000)
        end_timestamp_tmp = int(end_timestamp / 1000)
        register_seller_list_tmp = "'" + "','".join(str(i) for i in register_seller_list) + "'"
        sql = "select count(distinct user_id) as count_source from t_order where status not in ('new','topay','cancel')" \
              " AND parent_id='0'and user_id in ({0}) and " \
              "order_time >= {1} and order_time < {2}".format(
            register_seller_list_tmp, start_timestamp_tmp, end_timestamp_tmp)
        # print(sql)
        aplum_cursor.execute(sql)
        source_data = aplum_cursor.fetchall()

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

    order_user_num = get_count_source_by_sql(sql, url)
    return order_user_num


def get_register_num(nature_dict, source, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp):
    if source == 'all':
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source, a.createtime from " \
              "(select distinct second_id, source, createtime from users where createtime >= {0} " \
              "and createtime < {1})a  left join (select * from events where event = '$SignUp' and date between '{2}' " \
              "and '{3}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
    elif source in ('nature', 'nature_Android', 'nature_IOS', 'nature_wechat', 'nature_baidu'):
        sql = "select count(*) as count_source from (select distinct a.second_id, a.source from " \
              "(select tmp.second_id, tmp.source from (select * from( select u.first_id, u.source, u.second_id from " \
              "(select first_id, source, second_id from users where createtime >= {0} and createtime < {1} and " \
              "(source not in({2}) or source is null))u  group by u.first_id,u.second_id,u.source) m left join (select e.distinct_id, " \
              "case when e.plumfrontend='IOS客户端' then 'IOS' when e.plumfrontend='Android客户端' then 'Android' " \
              "when e.plumfrontend='百度小程序' then '百度小程序' else  '微信小程序' end khd from events e where " \
              "e.event='GeneralOpenApp' and date between '{3}' and '{4}' group by e.distinct_id,khd)n on " \
              "m.first_id=n.distinct_id)tmp where tmp.khd in({5}))a  " \
              "left join (select * from events where event = '$SignUp' and date between '{6}' " \
              "and '{7}') b " \
              "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
            .format(start_timestamp, end_timestamp, source_by, start_date_tmp, end_date_tmp, nature_dict[source],
                    start_date_tmp, end_date_tmp)
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

    register_num = get_count_source_by_sql(sql, url)
    return register_num


def get_day_add_activate(nature_dict, source, source_by, url, start_timestamp, end_timestamp):
    start_timestamp_tmp = int(end_timestamp - 24 * 3600 * 1000)
    if source == 'all':
        sql = "select count(*) as count_source from users where  " \
              "createtime >= {0} and createtime < {1}".format(start_timestamp_tmp, end_timestamp)
    elif source in ('nature', 'nature_Android', 'nature_IOS', 'nature_wechat', 'nature_baidu'):
        sql = "select count(*) as count_source from (select * from( select u.first_id, u.source from (select first_id, second_id," \
              "source from users where createtime >= {0} and createtime < {1} and (source not in({2}) or source is null))u  group by u.first_id,u.second_id,u.source) m " \
              "left join (select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  " \
              "'微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{3}' and '{4}' group by " \
              "e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where " \
              "tmp.khd in({5})".format(start_timestamp_tmp, end_timestamp, source_by, start_date_tmp, end_date_tmp, nature_dict[source])
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

    count_source = get_count_source_by_sql(sql, url)
    return count_source


def get_add_activate(nature_dict, source, source_by, url, start_timestamp, end_timestamp):
    if source == 'all':
        sql = "select count(*) as count_source from users where  " \
          "createtime >= {0} and createtime < {1}".format(start_timestamp, end_timestamp)
    elif source in ('nature', 'nature_Android', 'nature_IOS', 'nature_wechat', 'nature_baidu'):
        sql = "select count(*) as count_source from (select * from( select u.first_id, u.source from (select first_id, second_id," \
              "source from users where createtime >= {0} and createtime < {1} and (source not in({2}) or source is null))u  group by u.first_id,u.second_id,u.source) m " \
              "left join (select e.distinct_id, case when e.plumfrontend='IOS客户端' then 'IOS' when " \
              "e.plumfrontend='Android客户端' then 'Android' when e.plumfrontend='百度小程序' then '百度小程序' else  " \
              "'微信小程序' end khd from events e where e.event='GeneralOpenApp' and date between '{3}' and '{4}' group by " \
              "e.distinct_id,khd)n on m.first_id=n.distinct_id)tmp where " \
              "tmp.khd in({5})".format(start_timestamp, end_timestamp, source_by, start_date_tmp, end_date_tmp, nature_dict[source])
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

    count_source = get_count_source_by_sql(sql, url)
    return count_source


def get_count_source_by_sql(sql, url):
    count_source = 0
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


def get_costs_by_sql(market_cursor, sql_costs):
    costs = 0.0
    market_cursor.execute(sql_costs)
    source_data = market_cursor.fetchall()

    for row in source_data:
        if row['sum_costs'] is None:
            costs = 0.0
        else:
            costs = float(row['sum_costs'])
    return costs


def get_source(market_cursor, source_dict):
    sql = "select id, name, second_name, source from t_market_source"
    market_cursor.execute(sql)
    source_data = market_cursor.fetchall()
    for row in source_data:
        # row = process_price(row)
        id = str(row['id'])
        source_dict.update({id: row})
    return source_dict


def save_dict_to_csv(dict_sum, today):
    # line = ''
    # today = date.today() + timedelta(days=-1)
    with open('/home/aplum/work_lh/data_dict_to_csv/{0}-day-dict.csv'.format(today), 'a+', newline='') as file:
        # for x in dict_sum.items():
        #     file.write(str(x) + '\n')

        file.write(str(dict_sum) + '\n')


def data_to_mysql(today):
    position_dict = {'B站KOL': 400,
                     '小红书KOL': 401,
                     '微信KOL': 402,
                     '微博KOL': 403,
                     '抖音kol': 404,
                     '美拍KOL': 405,
                     '豆瓣KOL': 406,
                     'b站': 607,
                     '头条': 608,
                     '广点通': 609,
                     '微博': 610,
                     '快手': 611,
                     '知乎': 612,
                     'CPA': 613,
                     'Google': 614,
                     'inmobi': 615,
                     '友盟': 616,
                     '最右': 617,
                     '爱奇艺': 618,
                     '百度信息流': 619,
                     '百度搜索': 620,
                     '豆瓣': 621,
                     '微信MP': 622,
                     '微信朋友圈': 623,
                     '微信公众号': 624
                     }
    # db_market = MySQLdb.connect(mysql_host, mysql_user, mysql_passwd, mysql_db, charset='utf8')
    # 最终结果列表
    # result_list = list()
    # timestamp = int(time.time())
    # print(timestamp)
    # today = date.today() + timedelta(days=-1)
    file_path = '/home/aplum/work_lh/data_dict_to_csv/{0}-day-dict.csv'.format(today)
    # file_path = '/home/liuhang/2019-08-16-day-dict.csv'
    result_dict = dict()
    month_set = set()
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        # line = file.readline()
        for line in lines:
            # print(line)
            line_dict = eval(line)
            for key in line_dict.keys():
                month_set.add(str(line_dict[key]['month']))
                # print(key)
                result_dict[key] = list()
                source = str(line_dict[key]['source'])
                if source == 'all':
                    source_type = 0
                elif source == 'nature':
                    source_type = 1
                elif source == 'channel_all':
                    source_type = 2
                elif source == 'channel_flow':
                    source_type = 3
                elif source == 'channel_kol':
                    source_type = 4
                elif source == 'nature_Android':
                    source_type = 6
                elif source == 'nature_IOS':
                    source_type = 7
                elif source == 'nature_wechat':
                    source_type = 8
                elif source == 'nature_baidu':
                    source_type = 9
                else:
                    source_type = 5
                # if source_type in (6, 7, 8, 9):
                #     continue
                result_dict[key].append(int(source_type))
                if source_type == 5:
                    source_tmp = str(line_dict[key]['source'])
                else:
                    source_tmp = ''
                result_dict[key].append(source_tmp)
                result_dict[key].append(str(line_dict[key]['month']))
                result_dict[key].append(float(line_dict[key]['costs']))
                result_dict[key].append(int(line_dict[key]['today_actived_num']))
                result_dict[key].append(int(line_dict[key]['new_actived_user']))
                result_dict[key].append(float(line_dict[key]['avg_actived_costs']))
                result_dict[key].append(int(line_dict[key]['new_registered_user']))
                result_dict[key].append(float(line_dict[key]['avg_registered_costs']))
                result_dict[key].append(int(line_dict[key]['avg_registered_rate']))
                result_dict[key].append(int(line_dict[key]['new_ordered_user']))
                result_dict[key].append(float(line_dict[key]['avg_ordered_user_costs']))
                result_dict[key].append(int(line_dict[key]['avg_ordered_rate']))
                result_dict[key].append(int(line_dict[key]['new_ordered_num']))
                result_dict[key].append(float(line_dict[key]['avg_ordered_costs']))
                result_dict[key].append(float(line_dict[key]['kdj_costs']))
                result_dict[key].append(float(line_dict[key]['order_costs']))
                result_dict[key].append(float(line_dict[key]['roi']))
                result_dict[key].append(int(line_dict[key]['ddyj_mjjc']))
                result_dict[key].append(float(line_dict[key]['mmjpjcb']))
                timestamp = int(time.time())
                result_dict[key].append(timestamp)
                if source_type == 5:
                    result_dict[key].append(int(position_dict[source]))
                else:
                    result_dict[key].append(200)
    # for i in month_set:
    #     key_channel_all = 'channel_all&' + str(i)
    #     key_nature = 'nature&' + str(i)
    #     result_dict[key_channel_all] = list()
    #     result_dict[key_nature] = list()
    #     result_dict[key_channel_all].append(2)
    #     result_dict[key_channel_all].append('')
    #     result_dict[key_channel_all].append(str(i))
    #     for num in range(4, 20):
    #         result_dict[key_channel_all].append(0.0)
    #     result_dict[key_channel_all].append(int(time.time()))
    #     result_dict[key_channel_all].append(2)
    #     result_dict[key_channel_all].append('')
    #     result_dict[key_channel_all].append(str(i))
    #     for num in range(4, 20):
    #         result_dict[key_channel_all].append(0.0)
    #     result_dict[key_channel_all].append(int(time.time()))
    for i in month_set:
        key_channel_all = 'channel_all&' + str(i)
        key_nature = 'nature&' + str(i)
        # key_channel_kol = 'channel_kol&' + str(i)
        # key_channel_flow = 'channel_flow&' + str(i)
        result_dict[key_channel_all] = [0.0] * 22
        result_dict[key_channel_all][0] = 2
        result_dict[key_channel_all][1] = ''
        result_dict[key_channel_all][2] = str(i)
        result_dict[key_nature] = [0.0] * 22
        result_dict[key_channel_all][0] = 1
        result_dict[key_channel_all][1] = ''
        result_dict[key_channel_all][2] = str(i)

    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        # line = file.readline()
        for line in lines:
            line_dict = eval(line)
            for key in line_dict.keys():
                # 将博主的激活注册相关信息算到自然量中,并且channel_all的相关信息与channel_flow保持一致
                if str(line_dict[key]['source']).endswith('KOL') or str(line_dict[key]['source']) in ('channel_kol',
                                                                                                      '抖音kol'):
                    result_dict[key][4] = 0
                    result_dict[key][5] = 0
                    result_dict[key][6] = 0.0
                    result_dict[key][7] = 0
                    result_dict[key][8] = 0.0
                    result_dict[key][9] = 0

                else:
                    continue

    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        # line = file.readline()
        for line in lines:
            line_dict = eval(line)
            for key in line_dict.keys():
                source = str(line_dict[key]['source'])
                if source in ('头条信息流', '微博信息流'):
                    continue
                if source == 'CPA':
                    key_flow = 'channel_flow&' + str(line_dict[key]['month'])
                    for i in range(3, 20):
                        result_dict[key_flow][i] = result_dict[key_flow][i] + result_dict[key][i]

                if source == '抖音kol':
                    key_kol = 'channel_kol&' + str(line_dict[key]['month'])
                    if key_kol not in result_dict.keys():
                        result_dict[key_kol] = result_dict[key]
                    else:
                        for i in range(4, 20):
                            result_dict[key_kol][i] = result_dict[key_kol][i] + result_dict[key][i]

    for month in month_set:
        key_all_all = 'all&' + str(month)
        key_nature = 'nature&' + str(month)
        key_all = 'channel_all&' + str(month)
        key_flow = 'channel_flow&' + str(month)
        key_kol = 'channel_kol&' + str(month)
        result_dict[key_all][0] = 2
        result_dict[key_all][1] = ''
        result_dict[key_all][2] = str(month)
        if key_kol not in result_dict.keys():
            result_dict[key_kol] = [0.0] * 22
            result_dict[key_kol][0] = 4
            result_dict[key_kol][1] = ''
            result_dict[key_kol][2] = str(month)
            result_dict[key_kol][3] = 0.0
            # result_dict[key_kol] = [0.0] * 23
            for i in range(4, 20):
                result_dict[key_kol][i] = 0.0
            result_dict[key_kol][20] = int(time.time())
            result_dict[key_kol][21] = 200
        if key_flow not in result_dict.keys():
            result_dict[key_flow] = [0.0] * 22
            result_dict[key_flow][0] = 3
            result_dict[key_flow][1] = ''
            result_dict[key_flow][2] = str(month)
            result_dict[key_flow][3] = 0.0
            # result_dict[key_kol] = [0.0] * 23
            for i in range(4, 20):
                result_dict[key_flow][i] = 0.0
            result_dict[key_flow][20] = int(time.time())
            result_dict[key_flow][21] = 200

        for i in range(3, 20):
            result_dict[key_all][i] = result_dict[key_flow][i] + result_dict[key_kol][i]
        result_dict[key_all][20] = int(time.time())
        result_dict[key_all][21] = 200

        result_dict[key_nature][0] = 1
        result_dict[key_nature][1] = ''
        result_dict[key_nature][2] = str(month)
        result_dict[key_nature][3] = 0.0
        for i in range(4, 20):
            result_dict[key_nature][i] = result_dict[key_all_all][i] - result_dict[key_all][i]
        result_dict[key_nature][20] = int(time.time())
        result_dict[key_nature][21] = 200

    write_dict_to_mysql(today, result_dict)


def write_dict_to_mysql(today, result_dict):
    fields = ['type', 'second_name', 'exe_date', 'costs', 'today_actived_num', 'new_actived_user', 'avg_actived_costs',
              'new_registered_user', 'avg_registered_costs', 'avg_registered_rate', 'new_ordered_user',
              'avg_ordered_user_costs', 'avg_ordered_rate', 'new_ordered_num', 'avg_ordered_costs', 'kdj_costs',
              'order_costs', 'roi', 'ddyj_mjjc', 'mmjpjcb', 'create_time', 'position']
    df = pd.DataFrame(list(result_dict.values()), columns=fields)
    print(df)
    # writer = pd.ExcelWriter('/home/aplum/work_lh/data_dict_to_csv/{0}.xlsx'.format(today))
    # df.to_excel(excel_writer=writer, index=False, sheet_name='月表', encoding='utf-8')
    # writer.save()
    # writer.close()
    # yesterday_month = str(date.today() + timedelta(days=-1))
    checkData(today)
    engine = create_engine(
        "mysql+pymysql://{0}:{1}@{2}/{3}?charset={4}".format(mysql_user, mysql_passwd, mysql_host,
                                                             mysql_db,
                                                             'utf8'))
    # 创建连接
    con = engine.connect()
    df.to_sql(name='t_market_day_buyer', con=con, if_exists='append', index=False, chunksize=10000)
    print("写入数据成功")


def checkData(datestr):
    t_name = "t_market_day_buyer"
    datanum = getDataNum(datestr, t_name)
    print('当前重复数据: {}'.format(datanum))
    if datanum > 0:
        deleteData(datestr, t_name)
    else:
        return


def getDataNum(datestr, t_name):
    cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    sql = "SELECT count(1) as num FROM `{0}` WHERE exe_date = '{1}'".format(t_name, datestr)
    cursor.execute(sql)
    source_data = cursor.fetchall()
    cursor.close()
    for k in source_data:
        num = int(k['num'])
    return num


def deleteData(datestr, t_name):
    cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    sql = "DELETE  FROM `{0}` WHERE exe_date = '{1}'".format(t_name, datestr)
    print(sql)
    cursor.execute(sql)
    db_market.commit()


def get_all_data(nature_dict, aplum_cursor, source, costs, source_by, start_date_tmp, end_date_tmp, start_timestamp,
                           end_timestamp):
    result_dict = dict()
    # 来源
    # source = str(source_dict[k]['second_name'])
    # 当前月份
    # month = start_date_tmp
    # 当前月份花费
    # costs = float(get_costs_by_source_and_month(source_dict, costs_dict, source, date_tmp))
    # 当日激活
    current_day_activate = int(get_day_add_activate(nature_dict, source, source_by, url, start_timestamp, end_timestamp))
    # 新增激活
    add_activate = int(get_add_activate(nature_dict, source, source_by, url, start_timestamp, end_timestamp))
    # 注册数
    register_num = int(get_register_num(nature_dict, source, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp))
    # 订单用户数
    order_user_num = int(
        get_order_user_num(nature_dict, aplum_cursor, source, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp))
    # 订单数
    order_num = int(get_order_num(nature_dict, aplum_cursor, source, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp))
    # 订单金额
    order_sum_realpayprice = float(
        get_order_sum_realpayprice(nature_dict, aplum_cursor, source, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp))
    # 注册卖家数
    register_seller_num = int(
        get_register_seller_num(nature_dict, source, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp))
    # 寄出卖家数
    post_seller_num = int(
        get_post_seller_num(nature_dict, source, aplum_cursor, source_by, url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp))
    # 当月卖家收入
    month_seller_income = round(float(
        get_month_seller_income(nature_dict, source, aplum_cursor, source_by, url, start_timestamp, end_timestamp, start_date_tmp,
                                end_date_tmp)),
        2)
    order_user_post_seller = order_user_num + post_seller_num
    # if add_activate == 0:
    #     register_rate = 0.00
    #     order_user_trans_rate = 0.00
    # else:
    #     register_rate = int(register_num / add_activate * 10000)
    #     order_user_trans_rate = int(order_num / add_activate * 10000)
    #
    # if order_num == 0:
    #     customer_cost = 0.00
    # else:
    #     customer_cost = round((order_sum_realpayprice / order_num), 2)
    #
    # if register_seller_num == 0:
    #     register_post_trans_rate = 0.00
    # else:
    #     if post_seller_num == 0:
    #         register_post_trans_rate = 0.00
    #     else:
    #         register_post_trans_rate = int(post_seller_num / register_seller_num * 10000)

    key = str(source) + '&' + str(end_date_tmp)
    result_dict[key] = dict()
    result_dict[key]['source'] = source
    result_dict[key]['month'] = str(end_date_tmp)
    result_dict[key]['costs'] = costs
    result_dict[key]['today_actived_num'] = current_day_activate
    result_dict[key]['new_actived_user'] = add_activate
    result_dict[key]['avg_actived_costs'] = 0.0
    result_dict[key]['new_registered_user'] = register_num
    result_dict[key]['avg_registered_costs'] = 0.0
    result_dict[key]['avg_registered_rate'] = 0.0
    result_dict[key]['new_ordered_user'] = order_user_num
    result_dict[key]['avg_ordered_user_costs'] = 0.0
    result_dict[key]['avg_ordered_rate'] = 0.0
    result_dict[key]['new_ordered_num'] = order_num
    result_dict[key]['avg_ordered_costs'] = 0.0
    result_dict[key]['kdj_costs'] = 0.0
    result_dict[key]['order_costs'] = order_sum_realpayprice
    result_dict[key]['roi'] = 0.0
    result_dict[key]['new_seller'] = register_seller_num
    result_dict[key]['avg_seller_costs'] = 0.0
    result_dict[key]['new_jcmjs'] = post_seller_num
    result_dict[key]['avg_jcmjs_costs'] = 0.0
    result_dict[key]['avg_jcmjs_rate'] = 0.0
    result_dict[key]['dyscje'] = month_seller_income
    result_dict[key]['seller_roi'] = 0.0
    result_dict[key]['ddyj_mjjc'] = order_user_post_seller
    result_dict[key]['mmjpjcb'] = 0.0

    print(result_dict)
    save_dict_to_csv(result_dict, end_date_tmp)


def get_source_by_by_sql(market_cursor, sql):
    source_list_tmp = list()
    # print(sql)
    market_cursor.execute(sql)
    source_data = market_cursor.fetchall()
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


def check_source(market_cursor, source, yesterday):
    flag = True
    if source == 'channel_all':
        sql_source = "select source from t_market_cost where costs_date = '{0}' and source in (select source from " \
                     "t_market_source".format(yesterday)
        source_by = get_source_by_by_sql(market_cursor, sql_source)
    elif source == 'channel_kol':
        sql_source = "select source from t_market_cost where source in (select source from t_market_source " \
                     "where name = '博主kol' and second_name like '%KOL') and costs_date = '{0}'".format(yesterday)
        source_by = get_source_by_by_sql(market_cursor, sql_source)
    elif source == 'channel_flow':
        sql_source = "select source from t_market_cost where source in (select source from t_market_source " \
                     "where name = '信息流' and source != 'cpa') and costs_date = '{0}'".format(yesterday)
        source_by = get_source_by_by_sql(market_cursor, sql_source)
    else:
        sql_source = "select source from t_market_cost where source in (select source from t_market_source " \
                     "where second_name = '{0}') and costs_date = '{1}'".format(source, yesterday)
        source_by = get_source_by_by_sql(market_cursor, sql_source)
    if source_by == '':
        flag = False
    return flag


if __name__ == '__main__':
    for i in range(1, 29):
        delete_day = i
        try:
            shortargs = 'd:'
            opts, args = getopt.getopt(sys.argv[1:], shortargs)
        except getopt.GetoptError:
            print('args error')

        for opt, arg in opts:
            if opt == '-d':
                delete_day = int(arg)

        # 定义渠道来源字典
        source_dict = dict()
        # 获得市场报表相关渠道,成本连接
        db_market = MySQLdb.connect(mysql_host, mysql_user, mysql_passwd, mysql_db, charset='utf8')
        market_cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        # 获得快递,订单相关信息连接
        db_aplum = MySQLdb.connect(mysqlhost, mysqlusername, mysqlpasswd, db, charset='utf8')
        aplum_cursor = db_aplum.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        source_dict = get_source(market_cursor, source_dict)
        # 定义自然量字典
        nature_dict = {"nature": "'百度小程序', '微信小程序','IOS','Android'",
                       "nature_IOS": "'IOS'",
                       "nature_Android": "'Android'",
                       "nature_wechat": "'微信小程序'",
                       "nature_baidu": "'百度小程序'"
                       }

        # 定义报表展示渠道列表,展示层面为second_name
        source_list = ['channel_all', 'nature', 'nature_IOS', 'nature_Android', 'nature_wechat', 'nature_baidu', 'all',
                       'channel_flow', 'channel_kol']
        # source_list = ['channel_flow']
        for k in source_dict.keys():
            # 来源
            source = str(source_dict[k]['second_name'])
            if source in source_list:
                continue
            else:
                source_list.append(source)
        print("source_list:" + str(len(source_list)))
        # 统计日期2019-08-14
        # 起止日期: 2019-08-01 ~ 2019-08-13(前后包含)
        # 起止时间戳: 2019-08-01 00:00:00 ~ 2019-08-14 00:00:00
        today = date.today()
        end_date_tmp = today + timedelta(days=-delete_day)
        end_date_tmp_second = end_date_tmp + timedelta(days=1)
        start_date_tmp = str(end_date_tmp.replace(day=1))
        print(start_date_tmp)
        start_timestamp = int(
            time.mktime(time.strptime('{0} 00:00:00'.format(start_date_tmp), '%Y-%m-%d %H:%M:%S')) * 1000)
        end_timestamp = int(
            time.mktime(time.strptime('{0} 00:00:00'.format(end_date_tmp_second), '%Y-%m-%d %H:%M:%S')) * 1000)

        for source in source_list:
            print(source)
            costs = 0.00
            if source in ('CPA', '小红书', '其他', '大众点评'):
                # print(source)
                continue
            if source == 'all':
                # all统计不需要过滤source字段, source_by给空是为了调用该方法
                source_by = ''
                sql_costs = "select sum(costs) as sum_costs from t_market_day_cost where costs_date between '{0}' and" \
                            " '{1}' and source not in ('wxpyq', 'wxgzh')".format(start_date_tmp, end_date_tmp)
                costs = get_costs_by_sql(market_cursor, sql_costs)
                get_all_data(nature_dict, aplum_cursor, source, costs, source_by, start_date_tmp, end_date_tmp,
                             start_timestamp, end_timestamp)
            elif source in ('channel_all', 'nature', 'nature_IOS', 'nature_Android',
                            'nature_wechat', 'nature_baidu'):
                # sql_source = "select source from t_market_source "
                # source_by = get_source_by_by_sql(db_market, sql_source)
                sql_source = "select source from t_market_day_cost where costs_date = '{0}' and source in (select " \
                             "source from t_market_source)".format(end_date_tmp)
                source_by = get_source_by_by_sql(market_cursor, sql_source)
                if source_by == '':
                    print('{0} 无投放  统计结束'.format(end_date_tmp))
                    exit(0)
                sql_source = "select distinct source from t_market_day_cost where costs_date between '{0}' and '{1}' and " \
                             "source in (select source from t_market_source)".format(start_date_tmp, end_date_tmp)
                source_by = get_source_by_by_sql(market_cursor, sql_source)
                if source == 'channel_all':
                    sql_costs = "select sum(costs) as sum_costs from t_market_day_cost where costs_date between '{0}' and" \
                                " '{1}' and source in ({2})" \
                        .format(start_date_tmp, end_date_tmp, source_by)
                    costs = get_costs_by_sql(market_cursor, sql_costs)
                else:
                    costs = 0.0
                get_all_data(nature_dict, aplum_cursor, source, costs, source_by, start_date_tmp, end_date_tmp,
                             start_timestamp, end_timestamp)
            elif source == 'channel_kol':
                sql_source = "select distinct source from t_market_day_cost where source in (select source from " \
                             "t_market_source where name = '博主kol' and second_name like '%KOL') and costs_date between " \
                             "'{0}' and '{1}'".format(start_date_tmp, end_date_tmp)
                # print(sql_source)
                source_by = get_source_by_by_sql(market_cursor, sql_source)
                if source_by == '':
                    continue
                sql_costs = "select sum(costs) as sum_costs from t_market_day_cost where costs_date between '{0}' " \
                            "and '{1}' and source in ({2})".format(start_date_tmp, end_date_tmp, source_by)
                # print(sql_costs)
                costs = get_costs_by_sql(market_cursor, sql_costs)
                get_all_data(nature_dict, aplum_cursor, source, costs, source_by, start_date_tmp, end_date_tmp,
                             start_timestamp, end_timestamp)
            elif source == 'channel_flow':
                sql_source = "select distinct source from t_market_day_cost where source in (select source from " \
                             "t_market_source where name = '信息流' and source not in ('cpa', 'wxpyq', 'wxgzh'))" \
                             " and costs_date between '{0}' and '{1}'".format(start_date_tmp, end_date_tmp)
                source_by = get_source_by_by_sql(market_cursor, sql_source)
                # print(source_by)
                if source_by == '':
                    continue
                sql_costs = "select sum(costs) as sum_costs from t_market_day_cost where costs_date between '{0}' " \
                            "and '{1}' and source in ({2})".format(start_date_tmp, end_date_tmp, source_by)
                # print(sql_costs)
                costs = get_costs_by_sql(market_cursor, sql_costs)
                get_all_data(nature_dict, aplum_cursor, source, costs, source_by, start_date_tmp, end_date_tmp,
                             start_timestamp, end_timestamp)
            elif source == '抖音kol':
                sql_costs = "select sum(costs) as sum_costs from t_market_day_cost where source in(select source from " \
                            "t_market_source where second_name = '抖音kol') and costs_date between '{0}' and '{1}'" \
                    .format(start_date_tmp, end_date_tmp)
                costs = get_costs_by_sql(market_cursor, sql_costs)
                # print(costs)
                if costs == 0.0:
                    continue
                sql_partner = "select partner from t_toutiao_partner where start_time >= {0} and start_time < {1}"\
                    .format(int(start_timestamp / 1000), int(end_timestamp / 1000))
                partner_list = list()
                market_cursor.execute(sql_partner)
                source_data = market_cursor.fetchall()

                for row in source_data:
                    if row['partner'] is None:
                        continue
                    else:
                        partner_list.append(str(row['partner']))

                if len(partner_list) == 1:
                    source_by = "'" + str(partner_list[0]) + "'"
                else:
                    source_by = "'" + "','".join(str(i) for i in partner_list) + "'"
                # print(source_by)
                get_all_data(nature_dict, aplum_cursor, source, costs, source_by, start_date_tmp, end_date_tmp, start_timestamp, end_timestamp)
            else:
                sql_source = "select distinct source from t_market_day_cost where source in (select source from " \
                             "t_market_source where second_name = '{0}') and costs_date between '{1}' and '{2}'"\
                    .format(source, start_date_tmp, end_date_tmp)
                source_by = get_source_by_by_sql(market_cursor, sql_source)
                if source_by == '':
                    continue
                sql_costs = "select sum(costs) as sum_costs from t_market_day_cost where costs_date between '{0}' and" \
                            " '{1}' and source in ({2})".format(start_date_tmp, end_date_tmp, source_by)
                # print(sql_costs)
                costs = get_costs_by_sql(market_cursor, sql_costs)
                get_all_data(nature_dict, aplum_cursor, source, costs, source_by, start_date_tmp, end_date_tmp, start_timestamp,
                             end_timestamp)
        data_to_mysql(end_date_tmp)
        market_cursor.close()
        aplum_cursor.close()
