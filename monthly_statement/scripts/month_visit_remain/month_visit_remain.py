#!/usr/bin/env python
# coding=utf-8
from datetime import datetime, timedelta, date
import requests
import time
import json
import xlsxwriter
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import os
import sys
import traceback
import MySQLdb
import pandas as pd
import numpy

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

# mysql_host = "127.0.0.1"
# mysql_user = "aplum"
# mysql_passwd = "plum2016"
# mysql_db = "aplum_mis"

start = time.time()
tic = lambda: 'at %1.1f seconds' % (time.time() - start)


def get_count_date_by_sql(costs, source, url, sql, start_date_tmp):
    result_dict = dict()
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if source == 'all':
        type = 0
    elif source == 'nature':
        type = 1
    elif source == 'channel_all':
        type = 2
    elif source == 'channel_flow':
        type = 3
    elif source == 'channel_kol':
        type = 4
    elif source == 'flow_IOS':
        type = 10
    elif source == 'flow_Android':
        type = 11
    elif source == 'flow_wechat':
        type = 12
    elif source == 'flow_baidu':
        type = 13
    else:
        type = 5
    key_source = str(source)
    key_date = str(start_date_tmp)
    key = key_source + '&' + key_date
    result_dict[key] = list()
    result_dict[key].append(int(type))
    if type == 5:
        result_dict[key].append(source)
    else:
        result_dict[key].append('')
    result_dict[key].append(start_date_tmp)
    result_dict[key].append(costs)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:
            add_activate = 0
        else:
            dataarr = datastr.split('\n')
            for data in dataarr:
                try:
                    datajson = json.loads(data)
                    result_dict[key].append(int(datajson['totalusernum']))
                except json.decoder.JSONDecodeError as identifier:
                    pass
    else:
        print("sa hive sql accur error, sql为%s" % sql)
    if len(result_dict[key]) < 19:
        interval = int(19 - len(result_dict[key]))
        for i in range(interval):
            result_dict[key].append(str(0))
    elif len(result_dict[key]) > 19:
        result_dict[key] = result_dict[key][0: 19]
    else:
        pass
    if int(result_dict[key][4]) == 0:
        result_dict[key][5] = 0
        result_dict[key][6] = 0
        result_dict[key][7] = 0
        result_dict[key][8] = 0
        result_dict[key][9] = 0
        result_dict[key][10] = 0
        result_dict[key][11] = 0
        result_dict[key][12] = 0
        result_dict[key][13] = 0
        result_dict[key][14] = 0
        result_dict[key][15] = 0
        result_dict[key][16] = 0
        result_dict[key][17] = 0
    else:
        result_dict[key][5] = int(int(result_dict[key][5]) / int(result_dict[key][4]) * 10000)
        result_dict[key][6] = int(int(result_dict[key][6]) / int(result_dict[key][4]) * 10000)
        result_dict[key][7] = int(int(result_dict[key][7]) / int(result_dict[key][4]) * 10000)
        result_dict[key][8] = int(int(result_dict[key][8]) / int(result_dict[key][4]) * 10000)
        result_dict[key][9] = int(int(result_dict[key][9]) / int(result_dict[key][4]) * 10000)
        result_dict[key][10] = int(int(result_dict[key][10]) / int(result_dict[key][4]) * 10000)
        result_dict[key][11] = int(int(result_dict[key][11]) / int(result_dict[key][4]) * 10000)
        result_dict[key][12] = int(int(result_dict[key][12]) / int(result_dict[key][4]) * 10000)
        result_dict[key][13] = int(int(result_dict[key][13]) / int(result_dict[key][4]) * 10000)
        result_dict[key][14] = int(int(result_dict[key][14]) / int(result_dict[key][4]) * 10000)
        result_dict[key][15] = int(int(result_dict[key][15]) / int(result_dict[key][4]) * 10000)
        result_dict[key][16] = int(int(result_dict[key][16]) / int(result_dict[key][4]) * 10000)
        result_dict[key][17] = int(int(result_dict[key][17]) / int(result_dict[key][4]) * 10000)
    result_dict[key][18] = int(time.time())
    # write_dict_to_mysql()
    print(result_dict[key])
    return result_dict


def get_activate_date_by_source_date(start_timestamp, end_timestamp, source_by, costs, flow_dict, url, db_market, source, start_date_tmp, end_date_tmp):
    start_date_tmp_second = str(str(start_date_tmp).split('-')[0]) + '-' + str(
        str(start_date_tmp).split('-')[1]) + '-02'
    end_date_tmp_event = (datetime.strptime(start_date_tmp, '%Y-%m-%d') + relativedelta(months=13)).strftime('%Y-%m-%d')
    print(end_date_tmp_event)
    # print(start_date_tmp_second)
    if source == 'all':
        sql = "select * from (SELECT from_unixtime(unix_timestamp(date_sub( date, dayofmonth( date ) - 1 ))," \
              " 'yyyy-MM-dd') the_month, count( DISTINCT user_id ) totalusernum FROM EVENTS WHERE date < '{0}' and " \
              "EVENT = 'GeneralOpenApp' AND user_id IN (SELECT DISTINCT u.id FROM (select * from events where date >= '{1}' and date <'{2}' " \
              "and EVENT = 'GeneralOpenApp' AND IsFirstTime = 1) e	LEFT JOIN " \
              "(select id from users)u ON e.user_id = u.id ) GROUP BY the_month having the_month >= " \
              "'{3}' union select '{4}' as the_month,0 from users limit 1)tmp ORDER BY tmp.the_month ASC"\
            .format(end_date_tmp_event, start_date_tmp, end_date_tmp, start_date_tmp, start_date_tmp_second)
        result_dict = get_count_date_by_sql(costs, source, url, sql, start_date_tmp)
    # source_list = ['all', 'nature', 'channel_all', 'channel_flow', 'channel_kol']
    elif source == 'nature':

        source_list_tmp = list()
        cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        sql = "select source from t_market_source"
        cursor.execute(sql)
        source_data = cursor.fetchall()
        cursor.close()
        for row in source_data:
            if str(row) is None:
                continue
            source_list_tmp.append(str(row['source']))
        if len(source_list_tmp) == 1:
            source_by = "'" + str(source_list_tmp[0]) + "'"
        else:
            source_by = "'" + "','".join(str(i) for i in source_list_tmp) + "'"

        sql = "select * from (SELECT from_unixtime(unix_timestamp(date_sub( date, dayofmonth( date ) - 1 )), 'yyyy-MM-dd') " \
              "the_month, count( DISTINCT user_id ) totalusernum FROM EVENTS WHERE date < '{5}' and	EVENT = 'GeneralOpenApp' " \
              "AND user_id IN (SELECT DISTINCT u.id FROM EVENTS e	LEFT JOIN (select id from users where " \
              "source not in ({0}) or source is null)u " \
              "ON e.user_id = u.id WHERE e.date >= '{1}' and e.date <'{2}' and e.EVENT = 'GeneralOpenApp' " \
              "AND e.IsFirstTime = 1 ) GROUP BY the_month having the_month >= '{3}' union select '{4}' " \
              "as the_month,0 from users limit 1)tmp ORDER BY tmp.the_month ASC"\
            .format(source_by, start_date_tmp, end_date_tmp, start_date_tmp, start_date_tmp_second, end_date_tmp_event)
        result_dict = get_count_date_by_sql(costs, source, url, sql, start_date_tmp)
    elif source == 'channel_all':
        source_list_tmp = list()
        cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        sql = "select source from t_market_source"
        cursor.execute(sql)
        source_data = cursor.fetchall()
        cursor.close()
        for row in source_data:
            if str(row) is None:
                continue
            source_list_tmp.append(str(row['source']))
        if len(source_list_tmp) == 1:
            source_by = "'" + str(source_list_tmp[0]) + "'"
        else:
            source_by = "'" + "','".join(str(i) for i in source_list_tmp) + "'"

        # sql = "select * from (SELECT from_unixtime(unix_timestamp(date_sub( date, dayofmonth( date ) - 1 )), 'yyyy-MM-dd') " \
        #       "the_month, count( DISTINCT user_id ) totalusernum FROM EVENTS WHERE	EVENT = 'GeneralOpenApp' " \
        #       "AND user_id IN (SELECT DISTINCT u.id FROM EVENTS e	LEFT JOIN (select id from users where source " \
        #       "in ({0}))u ON e.user_id = u.id WHERE e.date >= '{1}' and e.date " \
        #       "<'{2}' and e.EVENT = 'GeneralOpenApp' AND e.IsFirstTime = 1 ) GROUP BY the_month having " \
        #       "the_month >= '{3}' union select '{4}' as the_month,count(distinct b.id) as totalusernum " \
        #       "from (SELECT DISTINCT u.id, e.time FROM	EVENTS e LEFT JOIN (select id from users where source in " \
        #       "({5}))u ON e.user_id = u.id WHERE e.date >= '{6}' and e.date " \
        #       "<'{7}' and e.EVENT = 'GeneralOpenApp' AND e.IsFirstTime = 1)a left join (SELECT DISTINCT	u1.id, " \
        #       "e1.time FROM	EVENTS e1 LEFT JOIN (select id from users )u1 ON e1.user_id = u1.id WHERE e1.date >= " \
        #       "'{8}' and e1.date <'{9}' and	e1.EVENT = 'GeneralOpenApp')b on a.id = b.id and a.time " \
        #       "!= b.time where b.id is not null)tmp ORDER BY tmp.the_month ASC".format(source_by, start_date_tmp,
        #                                                                                end_date_tmp,
        #                                                                                start_date_tmp,
        #                                                                                start_date_tmp_second, source_by,
        #                                                                                start_date_tmp, end_date_tmp,
        #                                                                                start_date_tmp, end_date_tmp)
        sql = "select * from (SELECT from_unixtime(unix_timestamp(date_sub( date, dayofmonth( date ) - 1 )), 'yyyy-MM-dd') " \
              "the_month, count( DISTINCT user_id ) totalusernum FROM EVENTS WHERE date < '{5}' and	EVENT = 'GeneralOpenApp' " \
              "AND user_id IN (SELECT DISTINCT u.id FROM EVENTS e	LEFT JOIN (select id from users where source " \
              "in ({0}))u ON e.user_id = u.id WHERE e.date >= '{1}' and e.date " \
              "<'{2}' and e.EVENT = 'GeneralOpenApp' AND e.IsFirstTime = 1 ) GROUP BY the_month having " \
              "the_month >= '{3}' union select '{4}' as the_month,0 from users limit 1)tmp ORDER BY tmp.the_month ASC"\
            .format(source_by, start_date_tmp, end_date_tmp, start_date_tmp, start_date_tmp_second, end_date_tmp_event)
        result_dict = get_count_date_by_sql(costs, source, url, sql, start_date_tmp)
    elif source == 'channel_flow':
        source_list_tmp = list()
        cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        sql = "select source from t_market_source where name = '信息流'"
        cursor.execute(sql)
        source_data = cursor.fetchall()
        cursor.close()
        for row in source_data:
            if str(row) is None:
                continue
            source_list_tmp.append(str(row['source']))
        if len(source_list_tmp) == 1:
            source_by = "'" + str(source_list_tmp[0]) + "'"
        else:
            source_by = "'" + "','".join(str(i) for i in source_list_tmp) + "'"

        sql = "select * from (SELECT from_unixtime(unix_timestamp(date_sub( date, dayofmonth( date ) - 1 )), 'yyyy-MM-dd') " \
              "the_month, count( DISTINCT user_id ) totalusernum FROM EVENTS WHERE date < '{5}' and	EVENT = 'GeneralOpenApp' " \
              "AND user_id IN (SELECT DISTINCT u.id FROM EVENTS e	LEFT JOIN (select id from users where source " \
              "in ({0}))u ON e.user_id = u.id WHERE e.date >= '{1}' and e.date " \
              "<'{2}' and e.EVENT = 'GeneralOpenApp' AND e.IsFirstTime = 1 ) GROUP BY the_month having " \
              "the_month >= '{3}' union select '{4}' as the_month,0 from users limit 1)tmp ORDER BY tmp.the_month ASC"\
            .format(source_by, start_date_tmp, end_date_tmp, start_date_tmp, start_date_tmp_second, end_date_tmp_event)
        # print(sql)
        result_dict = get_count_date_by_sql(costs, source, url, sql, start_date_tmp)
    elif source == 'channel_kol':
        source_list_tmp = list()
        cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        sql = "select source from t_market_source where name = '博主kol'"
        cursor.execute(sql)
        source_data = cursor.fetchall()
        cursor.close()
        for row in source_data:
            if str(row) is None:
                continue
            source_list_tmp.append(str(row['source']))
        if len(source_list_tmp) == 1:
            source_by = "'" + str(source_list_tmp[0]) + "'"
        else:
            source_by = "'" + "','".join(str(i) for i in source_list_tmp) + "'"

        sql = "select * from (SELECT from_unixtime(unix_timestamp(date_sub( date, dayofmonth( date ) - 1 )), 'yyyy-MM-dd') " \
              "the_month, count( DISTINCT user_id ) totalusernum FROM EVENTS WHERE date < '{5}' and	EVENT = 'GeneralOpenApp' " \
              "AND user_id IN (SELECT DISTINCT u.id FROM EVENTS e	LEFT JOIN (select id from users where source " \
              "in ({0}))u ON e.user_id = u.id WHERE e.date >= '{1}' and e.date " \
              "<'{2}' and e.EVENT = 'GeneralOpenApp' AND e.IsFirstTime = 1 ) GROUP BY the_month having " \
              "the_month >= '{3}' union select '{4}' as the_month,0 from users limit 1)tmp ORDER BY tmp.the_month ASC"\
            .format(source_by, start_date_tmp, end_date_tmp, start_date_tmp, start_date_tmp_second, end_date_tmp_event)
        result_dict = get_count_date_by_sql(costs, source, url, sql, start_date_tmp)
    elif source in ('flow_IOS', 'flow_Android', 'flow_wechat', 'flow_baidu'):
        source_list_tmp = list()
        cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        sql = "select source from t_market_source where name = '信息流'"
        cursor.execute(sql)
        source_data = cursor.fetchall()
        cursor.close()
        for row in source_data:
            if str(row) is None:
                continue
            source_list_tmp.append(str(row['source']))
        if len(source_list_tmp) == 1:
            source_by = "'" + str(source_list_tmp[0]) + "'"
        else:
            source_by = "'" + "','".join(str(i) for i in source_list_tmp) + "'"

        sql = "select * from (SELECT from_unixtime(unix_timestamp(date_sub( date, dayofmonth( date ) - 1 )), 'yyyy-MM-dd') " \
              "the_month, count( DISTINCT user_id ) totalusernum FROM EVENTS WHERE date < '{6}' and	EVENT = 'GeneralOpenApp' " \
              "AND user_id IN (SELECT DISTINCT u.id FROM (select *, case when plumfrontend='IOS客户端' then 'IOS' " \
              "when plumfrontend = 'Android客户端' then 'Android' when plumfrontend='百度小程序' then '百度小程序' " \
              "else  '微信小程序' end khd from events)e	LEFT JOIN (select id from users where source " \
              "in ({0}))u ON e.user_id = u.id WHERE e.date >= '{1}' and e.date " \
              "<'{2}' and e.EVENT = 'GeneralOpenApp' AND e.IsFirstTime = 1 and e.khd = '{3}') GROUP BY the_month " \
              "having " \
              "the_month >= '{4}' union select '{5}' as the_month,0 from users limit 1 )tmp ORDER BY " \
              "tmp.the_month ASC".format(source_by, start_date_tmp, end_date_tmp, flow_dict[source], start_date_tmp,
                                         start_date_tmp_second, end_date_tmp_event)
        # print(sql)
        result_dict = get_count_date_by_sql(costs, source, url, sql, start_date_tmp)
    elif source == 'CPA':
        sql = "select * from (SELECT from_unixtime(unix_timestamp(date_sub( date, dayofmonth( date ) - 1 )), 'yyyy-MM-dd') " \
              "the_month, count( DISTINCT user_id ) totalusernum FROM EVENTS WHERE date < '{7}' and	EVENT = 'GeneralOpenApp' " \
              "AND user_id IN (SELECT DISTINCT u.id FROM EVENTS e	LEFT JOIN (select id from users where createtime >= {0} " \
              " and createtime < {1} and adaccount in ({2}))u ON e.user_id = u.id WHERE e.date >= '{3}' and e.date " \
              "<'{4}' and e.EVENT = 'GeneralOpenApp' AND e.IsFirstTime = 1 ) GROUP BY the_month having " \
              "the_month >= '{5}' union select '{6}' as the_month,0 from users limit 1)tmp ORDER BY tmp.the_month ASC"\
            .format(start_timestamp, end_timestamp, source_by, start_date_tmp, end_date_tmp, start_date_tmp, start_date_tmp_second, end_date_tmp_event)
        result_dict = get_count_date_by_sql(costs, source, url, sql, start_date_tmp)
    elif source in ('微信公众号', '微信朋友圈'):
        # print(start_timestamp)
        # print(end_timestamp)
        # print(source_by)
        # print(start_date_tmp)
        # print(end_date_tmp)
        # print(start_date_tmp_second)
        sql = "select * from (SELECT from_unixtime(unix_timestamp(date_sub( date, dayofmonth( date ) - 1 )), 'yyyy-MM-dd') " \
              "the_month, count( DISTINCT user_id ) totalusernum FROM EVENTS WHERE date < '{7}' and	EVENT = 'GeneralOpenApp' " \
              "AND user_id IN (SELECT DISTINCT u.id FROM EVENTS e	LEFT JOIN (select id from users where createtime >= {0} " \
              " and createtime < {1} {2})u ON e.user_id = u.id WHERE e.date >= '{3}' and e.date " \
              "<'{4}' and e.EVENT = 'GeneralOpenApp' AND e.IsFirstTime = 1 ) GROUP BY the_month having " \
              "the_month >= '{5}' union select '{6}' as the_month,0 from users limit 1)tmp ORDER BY tmp.the_month ASC"\
            .format(start_timestamp, end_timestamp, source_by, start_date_tmp, end_date_tmp, start_date_tmp, start_date_tmp_second, end_date_tmp_event)
        # print(sql)
        result_dict = get_count_date_by_sql(costs, source, url, sql, start_date_tmp)
    elif source == '抖音kol':
        sql = "select * from (SELECT from_unixtime(unix_timestamp(date_sub( date, dayofmonth( date ) - 1 )), 'yyyy-MM-dd') " \
              "the_month, count( DISTINCT user_id ) totalusernum FROM EVENTS WHERE date < '{0}' and	EVENT = 'GeneralOpenApp' " \
              "AND user_id IN (SELECT DISTINCT u.user_id FROM EVENTS e	LEFT JOIN (select user_id from events where event = 'registerSuccessAct' " \
              "and PartnerOldUser is null and regexp_like(distinct_id, '^[0-9]+$') and date >= '{0}' " \
              "and date < '{1}')u ON e.user_id = u.user_id WHERE e.date >= '{2}' and e.date " \
              "<'{3}' and e.EVENT = 'GeneralOpenApp' AND e.IsFirstTime = 1 ) GROUP BY the_month having " \
              "the_month >= '{4}' union select '{5}' as the_month,0 from users limit 1)tmp ORDER BY tmp.the_month ASC"\
            .format(start_date_tmp, end_date_tmp, start_date_tmp, end_date_tmp, start_date_tmp, start_date_tmp_second, end_date_tmp_event)
        result_dict = get_count_date_by_sql(costs, source, url, sql, start_date_tmp)
    else:
        source_list_tmp = list()
        cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        sql = "select source from t_market_source where second_name = '{0}'".format(source)
        cursor.execute(sql)
        source_data = cursor.fetchall()
        cursor.close()
        for row in source_data:
            if str(row) is None:
                continue
            source_list_tmp.append(str(row['source']))
        if len(source_list_tmp) == 1:
            source_by = "'" + str(source_list_tmp[0]) + "'"
        else:
            source_by = "'" + "','".join(str(i) for i in source_list_tmp) + "'"
        # print(source_by)

        sql = "select * from (SELECT from_unixtime(unix_timestamp(date_sub( date, dayofmonth( date ) - 1 )), 'yyyy-MM-dd') " \
              "the_month, count( DISTINCT user_id ) totalusernum FROM EVENTS WHERE date < '{5}' and	EVENT = 'GeneralOpenApp' " \
              "AND user_id IN (SELECT DISTINCT u.id FROM EVENTS e	LEFT JOIN (select id from users where source " \
              "in ({0}))u ON e.user_id = u.id WHERE e.date >= '{1}' and e.date " \
              "<'{2}' and e.EVENT = 'GeneralOpenApp' AND e.IsFirstTime = 1 ) GROUP BY the_month having " \
              "the_month >= '{3}' union select '{4}' as the_month,0 from users limit 1)tmp ORDER BY tmp.the_month ASC".format(
            source_by, start_date_tmp, end_date_tmp,
            start_date_tmp, start_date_tmp_second, end_date_tmp_event)
        # print(sql)
        result_dict = get_count_date_by_sql(costs, source, url, sql, start_date_tmp)
    # print(result_dict)

    return result_dict


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


def get_end_date_tmp(start_date_tmp):
    current_month = int(str(start_date_tmp).split('-')[1])
    month_tmp = current_month + 1
    if month_tmp < 10:
        end_date_tmp = str(str(start_date_tmp).split('-')[0]) + '-0' + str(month_tmp) + '-01'
    elif month_tmp > 12:
        end_date_tmp = str(int(str(start_date_tmp).split('-')[0]) + 1) + '-0' + str(month_tmp - 12) + '-01'
    else:
        end_date_tmp = str(str(start_date_tmp).split('-')[0]) + '-' + str(month_tmp) + '-01'

    return end_date_tmp


def get_date_list(start_date):
    date_list = list()
    date_list.append(start_date)
    for i in range(1, 35):
        month_tmp = int(str(start_date).split('-')[1]) + i
        if month_tmp < 10:
            date_tmp = str(str(start_date).split('-')[0]) + '-0' + str(month_tmp) + '-01'
        elif month_tmp > 12:
            date_tmp = str(int(str(start_date).split('-')[0]) + 1) + '-0' + str(month_tmp - 12) + '-01'
        else:
            date_tmp = str(str(start_date).split('-')[0]) + '-' + str(month_tmp) + '-01'
        date_list.append(date_tmp)
    return date_list


def get_start_end_date():
    today = date.today()
    start_month = int(str(today).split('-')[1]) + 2
    if start_month == 0:
        start_month_tmp = 12
    elif start_month < 10:
        start_month_tmp = '0' + str(start_month)
    else:
        start_month_tmp = str(start_month)
    start_date = str(int(str(today).split('-')[0]) - 3) + '-' + str(start_month_tmp) + '-01'
    end_date = str(int(str(today).split('-')[0]) - 1) + '-' + str(str(today).split('-')[1]) + '-01'
    return start_date, end_date


def write_dict_to_mysql(result_dict):
    fields = ['type', 'second_name', 'exe_date', 'costs', 'active_num', 'm0', 'm1', 'm2', 'm3', 'm4', 'm5', 'm6', 'm7',
              'm8', 'm9', 'm10', 'm11', 'm12', 'create_time']
    df = pd.DataFrame(list(result_dict.values()), columns=fields)
    # print(df.head(5))
    engine = create_engine(
        "mysql+pymysql://{0}:{1}@{2}/{3}?charset={4}".format(mysql_user, mysql_passwd, mysql_host,
                                                             mysql_db,
                                                             'utf8'))
    # 创建连接
    con = engine.connect()
    df.to_sql(name='t_market_month_user_remain', con=con, if_exists='append', index=False, chunksize=10000)
    print("写入数据成功")


def get_costs_by_source_date(db_market, source, start_date_tmp):
    costs = 0.0
    cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    if source in ('all', 'channel_all'):
        sql = "select sum(costs) as sum_costs from t_market_cost where costs_date = '{0}'".format(start_date_tmp)
    elif source == 'channel_flow':
        sql = "select sum(costs) as sum_costs from t_market_cost where source in (select source from t_market_source" \
              " where name = '信息流') and costs_date = '{0}'".format(start_date_tmp)
    elif source == 'channel_kol':
        sql = "select sum(costs) as sum_costs from t_market_cost where source in (select source from t_market_source" \
              " where name = '博主kol') and costs_date = '{0}'".format(start_date_tmp)
    else:
        sql = "select sum(costs) as sum_costs from t_market_cost where source in (select source from t_market_source" \
              " where second_name = '{0}') and costs_date = '{1}'".format(source, start_date_tmp)
    cursor.execute(sql)
    source_data = cursor.fetchall()
    cursor.close()
    for row in source_data:
        if row['sum_costs'] is None:
            costs = 0.0
        else:
            costs = round(float(row['sum_costs']), 2)
    return costs


if __name__ == '__main__':
    result_dict = dict()
    # start_date, end_date = get_start_end_date()
    date_list = ['2018-09-01', '2018-10-01', '2018-11-01',
                 '2018-12-01', '2019-01-01', '2019-02-01', '2019-03-01', '2019-04-01',
                 '2019-05-01', '2019-06-01', '2019-07-01', '2019-08-01']
    # print(date_list)

    # date_list = ['2019-05-01', '2019-06-01', '2019-07-01']
    source_dict = dict()
    db_market = MySQLdb.connect(mysql_host, mysql_user, mysql_passwd, mysql_db, charset='utf8')
    db_aplum = MySQLdb.connect(mysqlhost, mysqlusername, mysqlpasswd, db, charset='utf8')
    source_dict = get_source(db_market, source_dict)
    source_list = ['all', 'nature', 'channel_all', 'channel_flow', 'channel_kol', 'flow_IOS', 'flow_Android',
                   'flow_wechat', 'flow_baidu']
    # source_list = ['all']
    # source_list = ['微信朋友圈', '微信公众号', 'CPA', '抖音kol']
    # source_list = ['友盟', '快手', 'b站', '微博KOL', 'B站KOL', '小红KOL']

    flow_dict = {"flow_IOS": "IOS",
                 "flow_Android": "Android",
                 "flow_wechat": "微信小程序",
                 "flow_baidu": "百度小程序"
                 }
    for k in source_dict.keys():
        # 来源
        source = str(source_dict[k]['second_name'])
        if source in source_list:
            continue
        else:
            source_list.append(source)
    print("source_list:" + str(len(source_list)))
    for current_date in date_list:
    # for source in source_list:
        for source in source_list:
            if source in ('CPA', '微信朋友圈', '微信公众号', '抖音kol'):
                print(source)
                continue
            else:
            # for current_date in date_list:
                start_date_tmp = current_date
                end_date_tmp = get_end_date_tmp(start_date_tmp)
                print(source)
                print(start_date_tmp + '>>>>>>>>>' + end_date_tmp + '\n')
                start_timestamp = int(
                    time.mktime(
                        time.strptime('{0} 00:00:00'.format(start_date_tmp), '%Y-%m-%d %H:%M:%S')) * 1000)

                end_timestamp = int(
                    time.mktime(
                        time.strptime('{0} 00:00:00'.format(end_date_tmp), '%Y-%m-%d %H:%M:%S')) * 1000)
                # date_tmp = date_tmp + '-01'
                # print(source)
                # print(start_date_tmp)
                if source in ('nature', 'flow_IOS', 'flow_Android', 'flow_wechat', 'flow_baidu'):
                    costs = 0.0
                else:
                    costs = get_costs_by_source_date(db_market, source, start_date_tmp)
                # source_list_tmp = list()
                # sid_list = list()
                source_by = ''
                result_dict = get_activate_date_by_source_date(start_timestamp, end_timestamp, source_by, costs, flow_dict, url, db_market, source, start_date_tmp,
                                                               end_date_tmp)
                print(result_dict)
                write_dict_to_mysql(result_dict)
