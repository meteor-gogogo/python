#!/usr/bin/env python
# coding=utf-8
from datetime import datetime, timedelta, date
import requests
import time
import json
import MySQLdb
import pandas as pd
import numpy
from sqlalchemy import create_engine

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


def get_post_seller_num(db_aplum):
    seller_dict = dict()
    cursor = db_aplum.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    sql = "SELECT FROM_UNIXTIME(m.scsjsj,'%Y-%m') as the_month ,count(m.id) as count_seller from(SELECT tp.user_id  id, min(tp.onsale_time) " \
          "scsjsj from t_product tp WHERE tp.onsale_time!=0 GROUP BY tp.user_id) m " \
          "GROUP BY FROM_UNIXTIME(m.scsjsj,'%Y-%m') order by FROM_UNIXTIME(m.scsjsj,'%Y-%m') ASC"
    cursor.execute(sql)
    costs_data = cursor.fetchall()
    cursor.close()
    for row in costs_data:
        # row = process_price(row)
        seller_dict[str(row['the_month'])] = int(row['count_seller'])
    return seller_dict


def get_order_user_num(url, date_list):
    buyer_dict = dict()
    # date_list = ['2016-10', '2016-11', '2016-12', '2017-01', '2017-02', '2017-03', '2017-04', '2017-05', '2017-06',
    #              '2017-07', '2017-08', '2017-09', '2017-10', '2017-11', '2017-12', '2018-01', '2018-02', '2018-03',
    #              '2018-04', '2018-05', '2018-06', '2018-07', '2018-08', '2018-09', '2018-10', '2018-11', '2018-12',
    #              '2019-01', '2019-02', '2019-03', '2019-04', '2019-05', '2019-06', '2019-07']
    for date_tmp in date_list:
        start_date_tmp = date_tmp + '-01'
        end_date_tmp = get_end_date_tmp(date_tmp)
        start_timestamp, end_timestamp = get_timestamp(start_date_tmp, end_date_tmp)
        sql = "SELECT from_unixtime(unix_timestamp(date_sub( date, dayofmonth( date ) - 1 )), 'yyyy-MM-dd') " \
              "the_month, count(distinct e.user_id)as count_source FROM EVENTS e	LEFT JOIN users u ON e.user_id = u.id " \
              "WHERE u.firstordertime >= {0} and u.firstordertime < {1} AND " \
              "e.EVENT =  'PayOrderDetail' and e.date between '{2}' and '{3}' group by the_month"\
            .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
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
                        buyer_date = str(datajson['the_month'])
                        buyer_dict[buyer_date] = int(datajson['count_source'])
                    except json.decoder.JSONDecodeError as identifier:
                        pass
        else:
            print("sa hive sql accur error, sql为%s" % sql)
    return buyer_dict


def get_costs_by_month(db_market):
    costs_dict = dict()
    cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    sql = "select costs_date, sum(costs) as sum_costs from t_market_cost group by costs_date order by costs_date asc"
    cursor.execute(sql)
    costs_data = cursor.fetchall()
    cursor.close()
    for row in costs_data:
        # row = process_price(row)
        costs_dict[str(row['costs_date'])] = float(row['sum_costs'])
    return costs_dict


def get_end_date_tmp(date_tmp):
    current_month = int(date_tmp.split('-')[1])
    current_year = int(date_tmp.split('-')[0])
    next_month = int(current_month + 1)
    if next_month < 10:
        end_date_tmp = str(current_year) + '-0' + str(next_month) + '-01'
    elif next_month > 12:
        current_year = current_year + 1
        end_date_tmp = str(current_year) + '-01' + '-01'
    else:
        end_date_tmp = str(current_year) + '-' + str(next_month) + '-01'
    end_date_tmp = (datetime.strptime(end_date_tmp, '%Y-%m-%d') + timedelta(days=-1)).strftime('%Y-%m-%d')
    return end_date_tmp


def get_timestamp(start_date_tmp, end_date_tmp):
    start_timestamp = int(
        time.mktime(time.strptime('{0} 00:00:00'.format(start_date_tmp), '%Y-%m-%d %H:%M:%S')) * 1000)

    end_date_timestamp = (datetime.strptime(end_date_tmp, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    end_timestamp = int(
        time.mktime(time.strptime('{0} 00:00:00'.format(end_date_timestamp), '%Y-%m-%d %H:%M:%S')) * 1000)
    return start_timestamp, end_timestamp


if __name__ == '__main__':
    result_dict_list = list()
    source_dict = dict()
    db_market = MySQLdb.connect(mysql_host, mysql_user, mysql_passwd, mysql_db, charset='utf8')
    db_aplum = MySQLdb.connect(mysqlhost, mysqlusername, mysqlpasswd, db, charset='utf8')

    # date_list = ['2016-10', '2016-11', '2016-12', '2017-01', '2017-02', '2017-03', '2017-04', '2017-05', '2017-06',
    #              '2017-07', '2017-08', '2017-09', '2017-10', '2017-11', '2017-12', '2018-01', '2018-02', '2018-03',
    #              '2018-04', '2018-05', '2018-06', '2018-07', '2018-08', '2018-09', '2018-10', '2018-11', '2018-12',
    #              '2019-01', '2019-02', '2019-03', '2019-04', '2019-05', '2019-06', '2019-07']

    date_list = ['2019-08']

    costs_dict = get_costs_by_month(db_market)
    buyer_dict = get_order_user_num(url, date_list)
    seller_dict = get_post_seller_num(db_aplum)

    fields = ['exe_date', 'costs', 'new_buyer', 'new_seller', 'avg_costs']
    result_dict_tmp = dict()
    for i in fields:
        result_dict_tmp[i] = list()
    for date in date_list:
        date_tmp = str(date) + '-01'
        result_dict_tmp['exe_date'].append(date_tmp)

        if date_tmp in costs_dict.keys():
            costs = costs_dict[date_tmp]
        else:
            costs = 0.0
        result_dict_tmp['costs'].append(costs)

        if date_tmp in buyer_dict.keys():
            buyer = buyer_dict[date_tmp]
        else:
            buyer = 0
        result_dict_tmp['new_buyer'].append(buyer)

        if date in seller_dict.keys():
            seller = seller_dict[date]
        else:
            seller = 0
        result_dict_tmp['new_seller'].append(seller)

        if int(buyer + seller) == 0:
            avg_cost = 0.0
        else:
            avg_cost = round(float(costs / (seller + buyer)), 2)
        result_dict_tmp['avg_costs'].append(avg_cost)

    df = pd.DataFrame(result_dict_tmp, columns=fields)
    print(df)
    engine = create_engine(
        "mysql+pymysql://{0}:{1}@{2}/{3}?charset={4}".format(mysql_user, mysql_passwd, mysql_host,
                                                             mysql_db,
                                                             'utf8'))
    # 创建连接
    con = engine.connect()
    df.to_sql(name='t_market_month_avg', con=con, if_exists='append', index=False, chunksize=10000)
    print("写入数据成功")
