#!/usr/bin/env python
# coding=utf-8
from datetime import datetime, timedelta, date
import requests
import time
import json
import pandas as pd

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


# 订单总金额
def get_order_sum_realpayprice(url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp):
    order_sum_realpayprice = 0.00
    sql = "select sum(c.orderitem_realpayprice) sum_realpay from (select a.second_id, a.source, a.createtime, " \
          "b.orderitem_realpayprice from (select distinct second_id, source, createtime from users where " \
          "createtime >= {0} and createtime < {1})a left join " \
          "(select * from events where event = 'PayOrderDetail' and date between '{2}' and '{3}') b " \
          "on a.second_id = b.distinct_id where b.distinct_id is not null)c" \
        .format(start_timestamp, end_timestamp, start_date_tmp, end_date_tmp)
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


# 获取商品数量,不去重
def get_product_num(url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp):
    sql = "select count(c.productid) as count_source from (select a.second_id, a.source, a.createtime, b.productid from " \
          "(select distinct second_id, source, createtime from users where  " \
          "createtime >= {0} and createtime < {1})a left join " \
          "(select productid, distinct_id from events where event = 'PayOrderDetail' and " \
          "date between '{2}' and '{3}') b on a.second_id = b.distinct_id where " \
          "b.distinct_id is not null)c".format(start_timestamp, end_timestamp, start_date_tmp,
                                               end_date_tmp)
    order_num = get_count_source_by_sql(sql, url)
    return order_num


# 订单数
def get_order_num(url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp):
    sql = "select count(distinct c.orderid) as count_source from (select a.second_id, a.source, a.createtime, b.orderid from " \
          "(select distinct second_id, source, createtime from users where  " \
          "createtime >= {0} and createtime < {1})a left join " \
          "(select distinct orderid, distinct_id from events where event = 'PayOrderDetail' and " \
          "date between '{2}' and '{3}') b on a.second_id = b.distinct_id where " \
          "b.distinct_id is not null)c".format(start_timestamp, end_timestamp, start_date_tmp,
                                               end_date_tmp)
    order_num = get_count_source_by_sql(sql, url)
    return order_num


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


def get_all_data(result_dict, start_date_tmp, end_date_tmp, start_timestamp, end_timestamp):
    # 当前月份
    month = start_date_tmp

    # 订单数
    order_num = int(get_order_num(url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp))

    # 商品数
    product_num = int(get_product_num(url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp))

    # 订单金额
    order_sum_realpayprice = float(
        get_order_sum_realpayprice(url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp))
    result_dict['month'].append(month)
    result_dict['new_ordered_num'].append(order_num)
    result_dict['product_num'].append(product_num)
    result_dict['order_costs'].append(order_sum_realpayprice)
    return result_dict


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
    result_dict = dict()
    result_dict['month'] = list()
    result_dict['new_ordered_num'] = list()
    result_dict['product_num'] = list()
    result_dict['order_costs'] = list()

    # start_date = '2018-11-01'
    date_list = ['2016-10', '2016-11', '2016-12', '2017-01', '2017-02', '2017-03', '2017-04', '2017-05', '2017-06',
                 '2017-07', '2017-08', '2017-09', '2017-10', '2017-11', '2017-12', '2018-01', '2018-02', '2018-03',
                 '2018-04', '2018-05', '2018-06', '2018-07', '2018-08', '2018-09', '2018-10', '2018-11', '2018-12',
                 '2019-01', '2019-02', '2019-03', '2019-04', '2019-05', '2019-06', '2019-07']

    for date_tmp in date_list:
        # 开始日期,结束日期,例如: 2018-11-01 ~ 2018-11-30,程序采用date between形式,左右日期都包含
        start_date_tmp = date_tmp + '-01'
        end_date_tmp = get_end_date_tmp(date_tmp)

        # 开始时间戳: 2018-11-01 00:00:00  结束时间戳: 2018-12-01 00:00:00
        start_timestamp, end_timestamp = get_timestamp(start_date_tmp, end_date_tmp)

        result_dict = get_all_data(result_dict, start_date_tmp, end_date_tmp, start_timestamp, end_timestamp)
        print(result_dict)

    fields = ['month', 'new_ordered_num', 'product_num', 'order_costs']
    df = pd.DataFrame(result_dict, columns=fields)
    today = date.today()
    writer = pd.ExcelWriter('/home/aplum/work_lh/product_{0}.xlsx'.format(today))
    df.to_excel(excel_writer=writer, index=False, sheet_name='product', encoding='utf-8')
    writer.save()
    writer.close()
    print(tic())
