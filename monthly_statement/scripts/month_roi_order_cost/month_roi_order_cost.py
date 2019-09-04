#!/usr/bin/env python
# coding=utf-8
from datetime import datetime, timedelta, date
import requests
import time
import json
import xlsxwriter
from sqlalchemy import create_engine
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

# mysql_host = "127.0.0.1"
# mysql_user = "aplum"
# mysql_passwd = "plum2016"
# mysql_db = "aplum_mis"

db_market = MySQLdb.connect(mysql_host, mysql_user, mysql_passwd, mysql_db, charset='utf8')
market_cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)


def write_dict_to_csv(date_now, result_dict):
    # today = date.today()
    with open('/home/aplum/work_lh/data_dict_to_csv/{0}-monthroi-dict.csv'.format(date_now), 'a+', newline='') as file:
        # for x in dict_sum.items():
        #     file.write(str(x) + '\n')

        file.write(str(result_dict) + '\n')


def get_count_date_by_sql(costs, source, url, sql, start_date_tmp):
    tmp_dict = dict()
    for i in range(25):
        key = str((datetime.strptime(start_date_tmp, '%Y-%m-%d') + relativedelta(months=i)).strftime('%Y-%m-%d'))
        # print(key)
        tmp_dict[key] = dict()
        key_user = key + 'user'
        key_sum = key + 'sum'
        tmp_dict[key][key_user] = list()
        # print(tmp_dict[key][key_user])
        tmp_dict[key][key_sum] = list()

    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        if len(datastr) == 0:

            key_source = str(source)
            key_date = str(start_date_tmp)
            key = key_source + '&' + key_date
            result_dict[key] = list(0 for i in range(31))
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
            else:
                type = 5

            result_dict[key][0] = int(type)
            result_dict[key][1] = 0
            if type == 5:
                result_dict[key][2] = source
            else:
                result_dict[key][2] = ''
            result_dict[key][3] = start_date_tmp
            result_dict[key][4] = costs
            for i in range(5, 30):
                result_dict[key][i] = 0
            print(result_dict[key])
            write_dict_to_csv(start_date_tmp, result_dict)
            result_dict.clear()

            key_source = str(source)
            key_date = str(start_date_tmp)
            key = key_source + '&' + key_date
            result_dict[key] = list(0 for i in range(31))
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
            else:
                type = 5

            result_dict[key][0] = int(type)
            result_dict[key][1] = 1
            if type == 5:
                result_dict[key][2] = source
            else:
                result_dict[key][2] = ''
            result_dict[key][3] = start_date_tmp
            result_dict[key][4] = costs
            for i in range(5, 30):
                result_dict[key][i] = 0
            print(result_dict[key])
            write_dict_to_csv(start_date_tmp, result_dict)
            result_dict.clear()
        else:
            dataarr = datastr.split('\n')
            # key_num = 0
            for data in dataarr:
                # key_num += 1
                # if key_num > 25:
                #     continue
                try:
                    datajson = json.loads(data)
                    # print(datajson)
                    # print(len(datajson))
                    key_date = str(datajson['the_month'])
                    if key_date not in tmp_dict.keys():
                        continue
                    if len(datajson) < 3:
                        tmp_dict[key_date][key_date + 'sum'].append(0)
                    else:
                        tmp_dict[key_date][key_date + 'sum'].append(float(datajson['sum_realpay']))
                    # print(datajson['distinct_id'])
                    # print(datajson['sum_realpay'])
                    # print(key_date)
                    # print(tmp_dict[key_date])
                    # print(tmp_dict[key_date][key_date + 'user'])
                    # print(tmp_dict[key_date][key_date + 'sum'])
                    tmp_dict[key_date][key_date + 'user'].append(int(datajson['distinct_id']))
                    # result_dict[key].append(int(datajson['totalusernum']))
                except json.decoder.JSONDecodeError as identifier:
                    pass
            # if len(tmp_dict[key_date][key_date + 'sum'])
            key_source = str(source)
            key_date = str(start_date_tmp)
            key = key_source + '&' + key_date
            result_dict[key] = list(0 for i in range(31))
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
            else:
                type = 5

            result_dict[key][0] = int(type)
            result_dict[key][1] = 0
            if type == 5:
                result_dict[key][2] = source
            else:
                result_dict[key][2] = ''
            result_dict[key][3] = start_date_tmp
            result_dict[key][4] = costs
            current_date_tmp = date.today().replace(day=1)
            for i in range(5, 30):
                # print(i)
                sum_num = 0.0
                for j in range(i - 4):
                    key_date = str((datetime.strptime(start_date_tmp, '%Y-%m-%d') + relativedelta(months=j)).strftime('%Y-%m-%d'))
                    if key_date in tmp_dict.keys():
                        for money in tmp_dict[key_date][key_date + 'sum']:
                            # print(i)
                            sum_num += money
                # print(sum_num)
                # print(result_dict[key])
                date_tmp = (datetime.strptime(start_date_tmp, '%Y-%m-%d') + relativedelta(months=(i - 5))).strftime(
                    '%Y-%m-%d')
                # print(date_tmp)
                # print(current_date_tmp)
                if str(date_tmp) == str(current_date_tmp):
                    break
                result_dict[key][i] = int(sum_num)

            result_dict[key][30] = int(time.time())
            print(result_dict[key])
            write_dict_to_csv(start_date_tmp, result_dict)
            # write_dict_to_mysql(result_dict)
            result_dict.clear()
            key_source = str(source)
            key_date = str(start_date_tmp)
            key = key_source + '&' + key_date
            result_dict[key] = list(0 for i in range(31))
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
            else:
                type = 5

            result_dict[key][0] = int(type)
            result_dict[key][1] = 1
            if type == 5:
                result_dict[key][2] = source
            else:
                result_dict[key][2] = ''
            result_dict[key][3] = start_date_tmp
            result_dict[key][4] = costs
            current_date_tmp = date.today().replace(day=1)
            for i in range(5, 30):
                user_set = set()
                for j in range(i - 4):
                    key_date = str((datetime.strptime(start_date_tmp, '%Y-%m-%d') + relativedelta(months=j)).strftime('%Y-%m-%d'))
                    if key_date in tmp_dict.keys():
                        for user in tmp_dict[key_date][key_date + 'user']:
                            user_set.add(user)
                # print(len(user_set))
                date_tmp = (datetime.strptime(start_date_tmp, '%Y-%m-%d') + relativedelta(months=(i - 5))).strftime('%Y-%m-%d')
                if str(date_tmp) == str(current_date_tmp):
                    break
                result_dict[key][i] = len(user_set)
            result_dict[key][30] = int(time.time())
            print(result_dict[key])
            write_dict_to_csv(start_date_tmp, result_dict)
            # write_dict_to_mysql(result_dict)
            result_dict.clear()
    else:
        print("sa hive sql accur error, sql为%s" % sql)


def get_activate_date_by_source_date(source_by, costs, start_timestamp, end_timestamp, url, db_market, source, start_date_tmp, end_date_tmp):
    # print(start_date_tmp_second)
    if source == 'all':
        sql = "SELECT from_unixtime(unix_timestamp(date_sub( date, dayofmonth( date ) - 1 )),'yyyy-MM-dd') " \
              "the_month, distinct_id, sum(orderitem_realpayprice) as sum_realpay FROM EVENTS WHERE " \
              "EVENT = 'PayOrderDetail' 	AND distinct_id IN (select second_id from users " \
              "where createtime >= {0}  and createtime < {1} ) GROUP BY the_month,distinct_id " \
              " HAVING the_month >= '{2}' ORDER BY the_month ASC".format(start_timestamp, end_timestamp, start_date_tmp)
        # print(sql)
        get_count_date_by_sql(costs, source, url, sql, start_date_tmp)
    elif source == 'nature':
        sql = "SELECT from_unixtime(unix_timestamp(date_sub( date, dayofmonth( date ) - 1 )),'yyyy-MM-dd') " \
              "the_month, distinct_id, sum(orderitem_realpayprice) as sum_realpay FROM EVENTS WHERE " \
              "EVENT = 'PayOrderDetail' 	AND distinct_id IN (select second_id from users " \
              "where createtime >= {0}  and createtime < {1} and (source not in ({2}) or source is null) ) GROUP BY the_month,distinct_id " \
              " HAVING the_month >= '{3}' ORDER BY the_month ASC".format(start_timestamp, end_timestamp, source_by, start_date_tmp)

        get_count_date_by_sql(costs, source, url, sql, start_date_tmp)
    elif str(source).endswith('KOL') or source in ('小红书', 'channel_kol'):
        sql = "SELECT from_unixtime(unix_timestamp(date_sub( date, dayofmonth( date ) - 1 )),'yyyy-MM-dd') " \
              "the_month, distinct_id, sum(orderitem_realpayprice) as sum_realpay FROM EVENTS WHERE " \
              "EVENT = 'PayOrderDetail' 	AND distinct_id IN (select second_id from users " \
              "where firstordertime >= {0} and firstordertime < {1} and source in ({2})) GROUP BY the_month,distinct_id " \
              " HAVING the_month >= '{3}' ORDER BY the_month ASC".format(start_timestamp, end_timestamp, source_by,
                                                                         start_date_tmp)
        # print(sql)
        get_count_date_by_sql(costs, source, url, sql, start_date_tmp)
    elif source == 'CPA':
        sql = "SELECT from_unixtime(unix_timestamp(date_sub( date, dayofmonth( date ) - 1 )),'yyyy-MM-dd') " \
              "the_month, distinct_id, sum(orderitem_realpayprice) as sum_realpay FROM EVENTS WHERE " \
              "EVENT = 'PayOrderDetail' 	AND distinct_id IN (select second_id from users " \
              "where createtime >= {0}  and createtime < {1} and adaccount in ({2})) GROUP BY the_month,distinct_id " \
              " HAVING the_month >= '{3}' ORDER BY the_month ASC".format(start_timestamp, end_timestamp, source_by, start_date_tmp)
        # print(sql)
        get_count_date_by_sql(costs, source, url, sql, start_date_tmp)
    elif source in ('微信公众号', '微信朋友圈'):
        sql = "SELECT from_unixtime(unix_timestamp(date_sub( date, dayofmonth( date ) - 1 )),'yyyy-MM-dd') " \
              "the_month, distinct_id, sum(orderitem_realpayprice) as sum_realpay FROM EVENTS WHERE " \
              "EVENT = 'PayOrderDetail' 	AND distinct_id IN (select second_id from users " \
              "where createtime >= {0}  and createtime < {1} {2} ) GROUP BY the_month,distinct_id " \
              " HAVING the_month >= '{3}' ORDER BY the_month ASC".format(start_timestamp, end_timestamp, source_by, start_date_tmp)

        get_count_date_by_sql(costs, source, url, sql, start_date_tmp)
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
        if len(register_seller_list) == 0:
            key_source = str(source)
            key_date = str(start_date_tmp)
            key = key_source + '&' + key_date
            result_dict[key] = list(0 for i in range(31))
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
            else:
                type = 5

            result_dict[key][0] = int(type)
            result_dict[key][1] = 0
            if type == 5:
                result_dict[key][2] = source
            else:
                result_dict[key][2] = ''
            result_dict[key][3] = start_date_tmp
            result_dict[key][4] = costs
            for i in range(5, 30):
                result_dict[key][i] = 0
            print(result_dict)
            write_dict_to_csv(start_date_tmp, result_dict)
            # write_dict_to_mysql(result_dict)
            result_dict.clear()

            key_source = str(source)
            key_date = str(start_date_tmp)
            key = key_source + '&' + key_date
            result_dict[key] = list(0 for i in range(31))
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
            else:
                type = 5

            result_dict[key][0] = int(type)
            result_dict[key][1] = 1
            if type == 5:
                result_dict[key][2] = source
            else:
                result_dict[key][2] = ''
            result_dict[key][3] = start_date_tmp
            result_dict[key][4] = costs
            for i in range(5, 30):
                result_dict[key][i] = 0
            print(result_dict)
            write_dict_to_csv(start_date_tmp, result_dict)
            # write_dict_to_mysql(result_dict)
            result_dict.clear()
        else:
            cursor = db_aplum.cursor(cursorclass=MySQLdb.cursors.DictCursor)
            register_seller_list_tmp = "'" + "','".join(str(i) for i in register_seller_list) + "'"
            sql_pay = "SELECT FROM_UNIXTIME(order_time,'%Y-%m-01') the_month, user_id distinct_id,	sum( td.realpay_price ) " \
                      "sum_realpay FROM t_order td WHERE td.STATUS NOT IN ( 'new', 'topay', 'cancel' ) " \
                      "AND td.parent_id = '0' AND user_id IN ( {0} ) GROUP BY	the_month, user_id " \
                      "HAVING the_month >= '{1}' ORDER BY the_month ASC".format(register_seller_list_tmp, start_date_tmp)
            cursor.execute(sql_pay)
            source_data = cursor.fetchall()
            cursor.close()
            register_seller_list.clear()
            # key_num = 0
            tmp_dict = dict()
            for i in range(25):
                key = str(
                    (datetime.strptime(start_date_tmp, '%Y-%m-%d') + relativedelta(months=i)).strftime('%Y-%m-%d'))
                # print(key)
                tmp_dict[key] = dict()
                key_user = key + 'user'
                key_sum = key + 'sum'
                tmp_dict[key][key_user] = list()
                # print(tmp_dict[key][key_user])
                tmp_dict[key][key_sum] = list()
            for data in source_data:
                # key_num += 1
                # if key_num > 25:
                #     continue
                try:
                    # datajson = json.loads(data)
                    # print(datajson)
                    # print(len(datajson))
                    key_date = str(data['the_month'])
                    if key_date not in tmp_dict.keys():
                        continue
                    if len(data) < 3:
                        tmp_dict[key_date][key_date + 'sum'].append(0)
                    else:
                        tmp_dict[key_date][key_date + 'sum'].append(float(data['sum_realpay']))
                    # print(datajson['distinct_id'])
                    # print(datajson['sum_realpay'])
                    # print(key_date)
                    # print(tmp_dict[key_date])
                    # print(tmp_dict[key_date][key_date + 'user'])
                    # print(tmp_dict[key_date][key_date + 'sum'])
                    tmp_dict[key_date][key_date + 'user'].append(int(data['distinct_id']))
                    # result_dict[key].append(int(datajson['totalusernum']))
                except json.decoder.JSONDecodeError as identifier:
                    pass
            # if len(tmp_dict[key_date][key_date + 'sum'])
            key_source = str(source)
            key_date = str(start_date_tmp)
            key = key_source + '&' + key_date
            result_dict[key] = list(0 for i in range(31))
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
            else:
                type = 5

            result_dict[key][0] = int(type)
            result_dict[key][1] = 0
            if type == 5:
                result_dict[key][2] = source
            else:
                result_dict[key][2] = ''
            result_dict[key][3] = start_date_tmp
            result_dict[key][4] = costs
            current_date_tmp = date.today().replace(day=1)
            for i in range(5, 30):
                # print(i)
                sum_num = 0.0
                for j in range(i - 4):
                    key_date = str(
                        (datetime.strptime(start_date_tmp, '%Y-%m-%d') + relativedelta(months=j)).strftime('%Y-%m-%d'))
                    if key_date in tmp_dict.keys():
                        for money in tmp_dict[key_date][key_date + 'sum']:
                            # print(i)
                            sum_num += money
                # print(sum_num)
                # print(result_dict[key])
                date_tmp = (datetime.strptime(start_date_tmp, '%Y-%m-%d') + relativedelta(months=(i - 5))).strftime(
                    '%Y-%m-%d')
                # print(date_tmp)
                # print(current_date_tmp)
                if str(date_tmp) == str(current_date_tmp):
                    break
                result_dict[key][i] = int(sum_num)

            result_dict[key][30] = int(time.time())
            print(result_dict)
            write_dict_to_csv(start_date_tmp, result_dict)
            # write_dict_to_mysql(result_dict)
            result_dict.clear()
            key_source = str(source)
            key_date = str(start_date_tmp)
            key = key_source + '&' + key_date
            result_dict[key] = list(0 for i in range(31))
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
            else:
                type = 5

            result_dict[key][0] = int(type)
            result_dict[key][1] = 1
            if type == 5:
                result_dict[key][2] = source
            else:
                result_dict[key][2] = ''
            result_dict[key][3] = start_date_tmp
            result_dict[key][4] = costs
            current_date_tmp = date.today().replace(day=1)
            for i in range(5, 30):
                user_set = set()
                for j in range(i - 4):
                    key_date = str(
                        (datetime.strptime(start_date_tmp, '%Y-%m-%d') + relativedelta(months=j)).strftime('%Y-%m-%d'))
                    if key_date in tmp_dict.keys():
                        for user in tmp_dict[key_date][key_date + 'user']:
                            user_set.add(user)
                # print(len(user_set))
                date_tmp = (datetime.strptime(start_date_tmp, '%Y-%m-%d') + relativedelta(months=(i - 5))).strftime(
                    '%Y-%m-%d')
                if str(date_tmp) == str(current_date_tmp):
                    break
                result_dict[key][i] = len(user_set)
            result_dict[key][30] = int(time.time())
            print(result_dict)
            write_dict_to_csv(start_date_tmp, result_dict)
            # write_dict_to_mysql(result_dict)
            result_dict.clear()
    else:
        sql = "SELECT from_unixtime(unix_timestamp(date_sub( date, dayofmonth( date ) - 1 )),'yyyy-MM-dd') " \
              "the_month, distinct_id, sum(orderitem_realpayprice) as sum_realpay FROM EVENTS WHERE " \
              "EVENT = 'PayOrderDetail' 	AND distinct_id IN (select second_id from users " \
              "where createtime >= {0}  and createtime < {1} and source in ({2})) GROUP BY the_month,distinct_id " \
              " HAVING the_month >= '{3}' ORDER BY the_month ASC".format(start_timestamp, end_timestamp, source_by,
                                                                         start_date_tmp)
        # print(sql)
        get_count_date_by_sql(costs, source, url, sql, start_date_tmp)


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
    # end_date_tmp = datetime.strptime(end_date_tmp, '%Y-%m-%d') + timedelta(days=-1)
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


def write_dict_to_mysql(fields, con, result_dict):

    df = pd.DataFrame(list(result_dict.values()), columns=fields)
    # print(df.head(5))

    df.to_sql(name='t_market_month_roi', con=con, if_exists='append', index=False, chunksize=10000)
    # print("写入数据成功")


def get_costs_by_sql(db_market, sql_costs):
    costs = 0.0
    cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    cursor.execute(sql_costs)
    source_data = cursor.fetchall()
    cursor.close()
    # print(source_data)
    for row in source_data:
        # print(row['sum_costs'])
        if row['sum_costs'] is None:
            costs = 0.0
        else:
            costs = float(row['sum_costs'])
    return costs


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


def check_data(datestr):
    t_name = "t_market_month_roi"
    datanum = get_data_num(datestr, t_name)
    print('当前重复数据: {}'.format(datanum))
    if datanum > 0:
        delete_data(datestr, t_name)
    else:
        return


def get_data_num(datestr, t_name):
    cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    sql = "SELECT count(1) as num FROM `{0}` WHERE exe_date = '{1}'".format(t_name, datestr)
    cursor.execute(sql)
    source_data = cursor.fetchall()
    cursor.close()
    for k in source_data:
        num = int(k['num'])
    return num


def delete_data(datestr, t_name):
    cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    sql = "DELETE  FROM `{0}` WHERE exe_date = '{1}'".format(t_name, datestr)
    print(sql)
    cursor.execute(sql)
    db_market.commit()


def file_data_to_mysql(date_list):
    # 将落盘文件写入到数据库中
    for current_date in date_list:
        # 检查数据库中该日期的数据是否存在,如果存在,先删除后插入
        check_data(current_date)
        file_path = '/home/aplum/work_lh/data_dict_to_csv/{0}-monthroi-dict.csv'.format(current_date)
        with open(file_path, 'r', encoding='utf-8')as file:
            # 初始化channel_all, channel_flow, channel_kol渠道数据列表
            # 这么做的原因是为了保持与月报报表的数据对齐,因为月报报表的这几个渠道的计算都是应推广要求后期进行计算出来的,而不是统计出来的
            current_timestamp = int(time.time())
            key_channel_all_roi_list = [2, 0, '', str(current_date), 0.0]
            for i in range(26):
                key_channel_all_roi_list.append(0)
            key_channel_all_roi_list.append(current_timestamp)
            key_channel_all_cac_list = [2, 1, '', str(current_date), 0.0]
            for i in range(26):
                key_channel_all_cac_list.append(0)
            key_channel_all_cac_list.append(current_timestamp)

            # key_channel_flow = 'channel_flow&' + str(current_date)
            key_channel_flow_roi_list = [3, 0, '', str(current_date), 0.0]
            for i in range(26):
                key_channel_flow_roi_list.append(0)
            key_channel_flow_roi_list.append(current_timestamp)
            key_channel_flow_cac_list = [3, 1, '', str(current_date), 0.0]
            for i in range(26):
                key_channel_flow_cac_list.append(0)
            key_channel_flow_cac_list.append(current_timestamp)

            # key_channel_kol = 'channel_kol&' + str(current_date)
            key_channel_kol_roi_list = [4, 0, '', str(current_date), 0.0]
            for i in range(26):
                key_channel_kol_roi_list.append(0)
            key_channel_kol_roi_list.append(current_timestamp)
            key_channel_kol_cac_list = [4, 1, '', str(current_date), 0.0]
            for i in range(26):
                key_channel_kol_cac_list.append(0)
            key_channel_kol_cac_list.append(current_timestamp)

            lines = file.readlines()
            for line in lines:
                line_dict = eval(line)
                # 读取每一行,直接写入MySQL,再把数据统计到相应的渠道
                write_dict_to_mysql(fields, con, line_dict)
                for key in line_dict.keys():
                    source = str(line_dict[key][2])
                    type = int(line_dict[key][0])
                    # 如果是all渠道,统计跳过
                    if type == 0:
                        continue
                    # 统计微信MP,因为微信MP是这两个的汇总渠道,所以跳过这两个渠道
                    if source in ('微信朋友圈', '微信公众号'):
                        continue
                    else:
                        sub_type = int(line_dict[key][1])
                        for index in range(4, 30):
                            # 子类型为0,是ROI
                            if sub_type == 0:
                                # 无论属于channel_kol,还是属于channel_flow,都隶属于channel_all,所以不用判断直接统计
                                key_channel_all_roi_list[index] = key_channel_all_roi_list[index] + line_dict[key][
                                    index]
                                # 后缀为KOL,或者kol的统计到channel_kol,否则统计到channel_flow
                                if source.endswith('KOL') or source.endswith('kol'):
                                    key_channel_kol_roi_list[index] = key_channel_kol_roi_list[index] + line_dict[key][
                                        index]
                                else:
                                    key_channel_flow_roi_list[index] = key_channel_flow_roi_list[index] + \
                                                                       line_dict[key][index]
                            else:
                                key_channel_all_cac_list[index] = key_channel_all_cac_list[index] + line_dict[key][
                                    index]
                                if source.endswith('KOL') or source.endswith('kol'):
                                    key_channel_kol_cac_list[index] = key_channel_kol_cac_list[index] + line_dict[key][
                                        index]
                                else:
                                    key_channel_flow_cac_list[index] = key_channel_flow_cac_list[index] + \
                                                                       line_dict[key][index]
            # 组装字典,写入MySQL,清空列表字典,准备读取下一个文件
            channel_all_roi_dict = {'channel_all_roi': key_channel_all_roi_list}
            write_dict_to_mysql(fields, con, channel_all_roi_dict)
            key_channel_all_roi_list.clear()
            channel_all_roi_dict.clear()
            channel_all_cac_dict = {'channel_all_cac': key_channel_all_cac_list}
            write_dict_to_mysql(fields, con, channel_all_cac_dict)
            key_channel_all_cac_list.clear()
            channel_all_cac_dict.clear()
            channel_flow_roi_dict = {'channel_flow_roi': key_channel_flow_roi_list}
            write_dict_to_mysql(fields, con, channel_flow_roi_dict)
            key_channel_flow_roi_list.clear()
            channel_flow_roi_dict.clear()
            channel_flow_cac_dict = {'channel_flow_cac': key_channel_flow_cac_list}
            write_dict_to_mysql(fields, con, channel_flow_cac_dict)
            key_channel_flow_cac_list.clear()
            channel_flow_cac_dict.clear()
            channel_kol_roi_dict = {'channel_kol_roi': key_channel_kol_roi_list}
            write_dict_to_mysql(fields, con, channel_kol_roi_dict)
            key_channel_kol_roi_list.clear()
            channel_kol_roi_dict.clear()
            channel_kol_cac_dict = {'channel_kol_cac': key_channel_kol_cac_list}
            write_dict_to_mysql(fields, con, channel_kol_cac_dict)
            key_channel_kol_cac_list.clear()
            channel_kol_cac_dict.clear()


if __name__ == '__main__':
    start = int(time.time())
    # df的fields
    fields = ['type', 'sub_type', 'second_name', 'exe_date', 'costs', 'm0', 'm1', 'm2', 'm3', 'm4', 'm5', 'm6', 'm7',
              'm8',
              'm9', 'm10', 'm11', 'm12', 'm13', 'm14', 'm15', 'm16', 'm17', 'm18',
              'm19', 'm20', 'm21', 'm22', 'm23', 'm24', 'create_time']
    engine = create_engine(
        "mysql+pymysql://{0}:{1}@{2}/{3}?charset={4}".format(mysql_user, mysql_passwd, mysql_host,
                                                             mysql_db,
                                                             'utf8'))
    # 创建连接
    con = engine.connect()
    result_dict = dict()
    # 定义查询的月份列表
    today = date.today()
    current_month_date = today.replace(day=1)
    date_list = list()
    for i in range(25, 0, -1):
        date_list.append(str(current_month_date + relativedelta(months=-i)))
    # date_list = ['2019-08-01']

    # 检查将要写入的文件是否已经存在,如果存在,删除
    for current_date in date_list:
        file_path = '/home/aplum/work_lh/data_dict_to_csv/{0}-monthroi-dict.csv'.format(current_date)
        if os.path.exists(file_path):
            os.remove(file_path)

    source_dict = dict()
    db_market = MySQLdb.connect(mysql_host, mysql_user, mysql_passwd, mysql_db, charset='utf8')
    market_cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    db_aplum = MySQLdb.connect(mysqlhost, mysqlusername, mysqlpasswd, db, charset='utf8')
    source_dict = get_source(db_market, source_dict)
    source_list = ['all', 'CPA', '微信朋友圈', '微信公众号', '抖音kol']
    for k in source_dict.keys():
        # 来源
        source = str(source_dict[k]['second_name'])
        if source in source_list:
            continue
        else:
            source_list.append(source)
    print("source_list:" + str(len(source_list)))
    for source in source_list:
        if source == 'CPA':
            # continue
            date_account_dict = {"2018-11-01": "'cpa-zshd','cpa-zshd1'", "2018-12-01": "'cpa-zshd','cpa-zshd3','cpa-zshd4','cpa-zshd5'",
                                 "2019-01-01": "'cpa-zshd','cpa-zshd1','cpa-zshd2','cpa-zshd3','cpa-zshd4'",
                                 "2019-03-01": "'cpa-zshd','cpa-zshd1','cpa-zshd2','cpa-zshd3','cpa-zshd4','cpa-zshd5','cpa-zshd6','cpa-zshd7','cpa-zshd9','cpa-zshd10'",
                                 "2019-04-01": "'cpa-zshd12','cpa-zshd5','cpa-zshd6','cpa-zshd15','cpa-zshd7','cpa-zshd10','cpa-zshd9','cpa-yl01','cpa-yl05'",
                                 "2019-05-01": "'cpa-zshd12','cpa-zshd11','cpa-zshd15','cpa-zshd5','cpa-zshd7'"}
            for start_date_tmp in date_account_dict.keys():
                end_date_tmp = get_end_date_tmp(start_date_tmp)
                print(start_date_tmp + ' ==> ' + end_date_tmp)
                start_timestamp = int(
                    time.mktime(time.strptime('{0} 00:00:00'.format(start_date_tmp), '%Y-%m-%d %H:%M:%S')) * 1000)
                # 2019-07-31 ==> 2019-08-01
                end_date_tmp_tmp = (datetime.strptime(end_date_tmp, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                end_timestamp = int(
                    time.mktime(time.strptime('{0} 00:00:00'.format(end_date_tmp_tmp), '%Y-%m-%d %H:%M:%S')) * 1000)
                source_by = str(date_account_dict[start_date_tmp])
                sql_costs = "select sum(costs) as sum_costs from t_market_cost where source = 'cpa'" \
                            " and costs_date = '{0}'"\
                    .format(start_date_tmp)
                costs = get_costs_by_sql(db_market, sql_costs)
                get_activate_date_by_source_date(source_by, costs, start_timestamp, end_timestamp,
                                                 url, db_market, source, start_date_tmp, end_date_tmp)
        elif source == '微信公众号':
            date_account_dict = {"2018-11-01": "",
                                 "2018-12-01": "",
                                 "2019-01-01": "",
                                 "2019-02-01": "",
                                 "2019-03-01": "",
                                 "2019-04-01": "'weixin-ios-rdpyq'",
                                 "2019-05-01": "'weixin-ios-rdpyq'",
                                 "2019-06-01": "'weixin-ios-rdpyq'",
                                 "2019-07-01": "'weixin-ios-rdpyq'",
                                 "2019-08-01": "'weixin-ios-rdpyq'"
                                 }
            for start_date_tmp in date_account_dict.keys():
                end_date_tmp = get_end_date_tmp(start_date_tmp)
                print(start_date_tmp + ' ==> ' + end_date_tmp)
                start_timestamp = int(
                    time.mktime(time.strptime('{0} 00:00:00'.format(start_date_tmp), '%Y-%m-%d %H:%M:%S')) * 1000)
                # 2019-07-31 ==> 2019-08-01
                end_date_tmp_tmp = (datetime.strptime(end_date_tmp, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                end_timestamp = int(
                    time.mktime(time.strptime('{0} 00:00:00'.format(end_date_tmp_tmp), '%Y-%m-%d %H:%M:%S')) * 1000)
                sql_costs = "select sum(costs) as sum_costs from t_market_cost where source = 'wxgzh' and " \
                            "costs_date = '{0}'" \
                    .format(start_date_tmp)
                costs = get_costs_by_sql(db_market, sql_costs)
                if start_date_tmp in ('2018-11-01', '2018-12-01', '2019-01-01', '2019-02-01', '2019-03-01'):
                    source_by = "and source = 'weixin'"
                else:
                    source_by = "and source = 'weixin' and adaccount = {0}".format(str(date_account_dict[start_date_tmp]))
                get_activate_date_by_source_date(source_by, costs, start_timestamp, end_timestamp,
                                                 url, db_market, source, start_date_tmp, end_date_tmp)
        elif source == '微信朋友圈':
            date_account_dict = {"2019-04-01": "'weixin-rd'",
                                 "2019-05-01": "'weixin-rd'",
                                 "2019-06-01": "'weixin-rd'",
                                 "2019-07-01": "'weixin-rd'",
                                 "2019-08-01": "'weixin-rd'",
                                 }
            for start_date_tmp in date_account_dict.keys():
                end_date_tmp = get_end_date_tmp(start_date_tmp)
                print(start_date_tmp + ' ==> ' + end_date_tmp)
                start_timestamp = int(
                    time.mktime(time.strptime('{0} 00:00:00'.format(start_date_tmp), '%Y-%m-%d %H:%M:%S')) * 1000)
                # 2019-07-31 ==> 2019-08-01
                end_date_tmp_tmp = (datetime.strptime(end_date_tmp, '%Y-%m-%d') + timedelta(days=1)).strftime(
                    '%Y-%m-%d')
                end_timestamp = int(
                    time.mktime(time.strptime('{0} 00:00:00'.format(end_date_tmp_tmp), '%Y-%m-%d %H:%M:%S')) * 1000)
                sql_costs = "select sum(costs) as sum_costs from t_market_cost where source = 'wxpyq' and " \
                            "costs_date = '{0}'" \
                    .format(start_date_tmp)
                costs = get_costs_by_sql(db_market, sql_costs)
                source_by = "and source = 'weixin' and adaccount = {0}".format(str(date_account_dict[start_date_tmp]))
                get_activate_date_by_source_date(source_by, costs, start_timestamp, end_timestamp,
                                                 url, db_market, source, start_date_tmp, end_date_tmp)
        elif source == '抖音kol':
            date_list = ['2019-05-01', '2019-06-01', '2019-07-01', '2019-08-01']
            for start_date_tmp in date_list:
                end_date_tmp = get_end_date_tmp(start_date_tmp)
                print(start_date_tmp + ' ==> ' + end_date_tmp)
                start_timestamp = int(
                    time.mktime(time.strptime('{0} 00:00:00'.format(start_date_tmp), '%Y-%m-%d %H:%M:%S')) * 1000)
                # 2019-07-31 ==> 2019-08-01
                end_date_tmp_tmp = (datetime.strptime(end_date_tmp, '%Y-%m-%d') + timedelta(days=1)).strftime(
                    '%Y-%m-%d')
                end_timestamp = int(
                    time.mktime(time.strptime('{0} 00:00:00'.format(end_date_tmp_tmp), '%Y-%m-%d %H:%M:%S')) * 1000)
                sql_costs = "select sum(costs) as sum_costs from t_market_cost where source in(select source from " \
                            "t_market_source where second_name = '抖音kol') and costs_date = '{0}'" \
                    .format(start_date_tmp)
                costs = get_costs_by_sql(db_market, sql_costs)
                if costs == 0.0:
                    print('source为' + str(source) + ',costs为' + str(costs) + ',统计跳过')
                    continue
                sql_partner = "select partner from t_toutiao_partner where start_time >= {0} and start_time < {1}" \
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
                get_activate_date_by_source_date(source_by, costs, start_timestamp, end_timestamp,
                                                 url, db_market, source, start_date_tmp, end_date_tmp)
        else:
            for current_date in date_list:
                start_date_tmp = current_date
                end_date_tmp = get_end_date_tmp(start_date_tmp)
                start_timestamp = int(
                    time.mktime(time.strptime('{0} 00:00:00'.format(start_date_tmp), '%Y-%m-%d %H:%M:%S')) * 1000)

                end_timestamp = int(
                    time.mktime(time.strptime('{0} 00:00:00'.format(end_date_tmp), '%Y-%m-%d %H:%M:%S')) * 1000)
                print(source)
                print(start_date_tmp + '>>>>>>>>>' + end_date_tmp + '\n')
                # date_tmp = date_tmp + '-01'

                if source == 'all':
                    # all统计不需要过滤source字段, source_by给空是为了调用该方法
                    source_by = ''
                    sql_costs = "select sum(costs) as sum_costs from t_market_cost where costs_date = '{0}' and " \
                                "source not in ('wxpyq', 'wxgzh')" \
                        .format(start_date_tmp)
                    costs = get_costs_by_sql(db_market, sql_costs)
                    get_activate_date_by_source_date(source_by, costs, start_timestamp, end_timestamp, url, db_market, source, start_date_tmp, end_date_tmp)
                elif source in ('channel_all', 'nature'):
                    sql_source = "select distinct source from t_market_cost where costs_date == '{0}' and " \
                         "source in (select source from t_market_source)".format(start_date_tmp)
                    source_by = get_source_by_by_sql(db_market, sql_source)
                    if source == 'channel_all':
                        sql_costs = "select sum(costs) as sum_costs from t_market_cost where costs_date = '{0}'" \
                            .format(start_date_tmp)
                        costs = get_costs_by_sql(db_market, sql_costs)
                    else:
                        costs = 0.0
                    get_activate_date_by_source_date(source_by, costs, start_timestamp, end_timestamp, url, db_market, source, start_date_tmp, end_date_tmp)
                elif source == 'channel_kol':
                    sql_source = "select source from t_market_cost where source in (select source from t_market_source " \
                                 "where name = '博主kol') and costs_date = '{0}'".format(start_date_tmp)
                    source_by = get_source_by_by_sql(db_market, sql_source)
                    if source_by == '':
                        continue
                    sql_costs = "select sum(costs) as sum_costs from t_market_cost where costs_date = '{0}' " \
                                "and source in ({1})" \
                        .format(start_date_tmp, source_by)
                    costs = get_costs_by_sql(db_market, sql_costs)
                    get_activate_date_by_source_date(source_by, costs, start_timestamp, end_timestamp, url, db_market, source, start_date_tmp, end_date_tmp)
                elif source == 'channel_flow':
                    sql_source = "select source from t_market_source where name = '信息流'"
                    source_by = get_source_by_by_sql(db_market, sql_source)
                    sql_costs = "select sum(costs) as sum_costs from t_market_cost where costs_date = '{0}' " \
                                "and source in ({1})" \
                        .format(start_date_tmp, source_by)
                    costs = get_costs_by_sql(db_market, sql_costs)
                    get_activate_date_by_source_date(source_by, costs, start_timestamp, end_timestamp, url, db_market, source, start_date_tmp, end_date_tmp)
                else:
                    # 博主激活码的统计要严格按照投放时间统计,所以需要加时间限制条件
                    if str(source).endswith('KOL') or source == '小红书':
                        sql_source = "select source from t_market_cost where source in (select source from t_market_source " \
                                     "where second_name = '{0}') and costs_date = '{1}'".format(source, start_date_tmp)
                        source_by = get_source_by_by_sql(db_market, sql_source)
                        if source_by == '':
                            continue
                    else:
                        sql_source = "select source from t_market_source where second_name = '{0}'".format(source)
                        source_by = get_source_by_by_sql(db_market, sql_source)
                        if source_by == '':
                            continue
                    sql_costs = "select sum(costs) as sum_costs from t_market_cost where costs_date = '{0}' " \
                                "and source in ({1})" \
                        .format(start_date_tmp, source_by)
                    costs = get_costs_by_sql(db_market, sql_costs)
                    get_activate_date_by_source_date(source_by, costs, start_timestamp, end_timestamp, url, db_market, source, start_date_tmp, end_date_tmp)
    file_data_to_mysql(date_list)
    print('共耗时: {0}秒'.format(int(time.time() - start)))
