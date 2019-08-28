#!/usr/bin/env python
# coding=utf-8
from datetime import datetime, timedelta, date
import requests
import time
import json
import pandas as pd
import MySQLdb

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


def get_data(date_tmp, aplum_cursor, start_date_tmp, end_date_tmp):
    sql = "SELECT count(distinct  tp.user_id)  卖家人数, count(distinct  tp.id)  提交商品数, " \
          "count(case  when  tp.onsale_time!=0  then  tp.id  end)  上架商品数,count(distinct case when  " \
          "(case  when  tp.status='sold'  then  tdd.realpay_price  end)>0  then    tp.user_id  end)  " \
          "有过售出人数, count(case  when  (case  when  tp.status='sold'  then  tdd.realpay_price  end)>0  " \
          "then    tp.id  end)  售出件数, sum(case  when  (case  when  tp.status='sold'  then tdd.realpay_price  end)>0" \
          "  then    tdd.settle_price  else  0  end)  卖家收入 from  t_product  tp LEFT  JOIN  t_seller  ts  on  " \
          "tp.user_id=ts.id LEFT  JOIN  (SELECT tdi.product_id,td.id,tdi.realpay_price,tdi.discount_price," \
          "tdi.settle_price,td.order_time,tdi.plum_income FROM  t_order  td LEFT  JOIN  t_order_item  tdi  on  " \
          "tdi.order_id=td.id where  td.status  not  in  ('new','topay','cancel') and  td.splitted='0' and  " \
          "tdi.refund_status  in  ('no',  'rejected') and  td.order_time>=UNIX_TIMESTAMP('{0}') and  " \
          "td.order_time<UNIX_TIMESTAMP('{1}') GROUP  BY  tdi.product_id)  tdd  on  tdd.product_id=tp.id " \
          "WHERE tp.provider	in('secondhand','resell') and  tp.create_time>=unix_timestamp('{2}') and  " \
          "tp.create_time<unix_timestamp('{3}') and  tp.user_id  in(SELECT  m.user_id from" \
          "( SELECT tp.user_id,min(FROM_UNIXTIME(tp.create_time,'%Y-%m'))  nian from  t_product  tp LEFT  JOIN  " \
          "t_seller  ts  on  tp.user_id=ts.id  WHERE tp.provider in('secondhand','resell') group  by  tp.user_id)  " \
          "m where  m.nian='{4}')".format(start_date_tmp, end_date_tmp, start_date_tmp, end_date_tmp, date_tmp)
    aplum_cursor.execute(sql)
    source_data = aplum_cursor.fetchall()
    for row in source_data:
        account_num = row['卖家人数']
        order_sum_realpayprice = row['提交商品数']
        order_num = row['上架商品数']
        plum_income = row['有过售出人数']
        liquan_count = row['售出件数']
        liquan_sum = row['卖家收入']
    return account_num, order_sum_realpayprice, order_num, plum_income, liquan_count, liquan_sum

def get_user_id(aplum_cursor, start_date_tmp, end_date_tmp):
    user_list = list()
    sql = "SELECT distinct td.user_id as id FROM  t_order  td where td.status  not  in  ('new','topay','cancel') AND  " \
          "td.parent_id='0' and  td.order_count=1 and  td.order_time>=UNIX_TIMESTAMP('{0}') and  " \
          "td.order_time<UNIX_TIMESTAMP('{1}')".format(start_date_tmp, end_date_tmp)
    aplum_cursor.execute(sql)
    source_data = aplum_cursor.fetchall()
    for row in source_data:
        if row['id'] is None:
            continue
        user_list.append(str(row['id']))
    if len(user_list) == 0:
        return ''

    if len(user_list) == 1:
        source_by = "'" + str(user_list[0]) + "'"
    else:
        source_by = "'" + "','".join(str(i) for i in user_list) + "'"
    return source_by


def get_all_data(date_tmp, aplum_cursor, result_dict, start_date_tmp, end_date_tmp, start_timestamp, end_timestamp):
    # 当前月份
    month = start_date_tmp
    #
    # user_id_list = get_user_id(aplum_cursor, start_date_tmp, end_date_tmp)
    # if user_id_list == '':
    #     account_num = 0
    #     order_sum_realpayprice = 0.0
    #     order_num = 0
    #     plum_income = 0.0
    #     liquan_count = 0
    #     liquan_sum = 0.0
    # else:
    account_num, order_sum_realpayprice, order_num, plum_income, liquan_count, liquan_sum = \
        get_data(date_tmp, aplum_cursor, start_date_tmp, end_date_tmp)
        #
        # # 订单数
        # order_num = int(get_order_num(url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp))
        #
        # # 商品数
        # product_num = int(get_product_num(url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp))
        #
        # # 订单金额
        # order_sum_realpayprice = float(
        #     get_order_sum_realpayprice(url, start_timestamp, end_timestamp, start_date_tmp, end_date_tmp))
    result_dict['月份'].append(month)
    result_dict['卖家人数'].append(account_num)
    result_dict['提交商品数'].append(order_sum_realpayprice)
    result_dict['上架商品数'].append(order_num)
    result_dict['有过售出人数'].append(plum_income)
    result_dict['售出件数'].append(liquan_count)
    result_dict['卖家收入'].append(liquan_sum)
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
    # end_date_tmp = (datetime.strptime(end_date_tmp, '%Y-%m-%d') + timedelta(days=-1)).strftime('%Y-%m-%d')
    return end_date_tmp


def get_timestamp(start_date_tmp, end_date_tmp):
    start_timestamp = int(
        time.mktime(time.strptime('{0} 00:00:00'.format(start_date_tmp), '%Y-%m-%d %H:%M:%S')) * 1000)

    end_date_timestamp = (datetime.strptime(end_date_tmp, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    end_timestamp = int(
        time.mktime(time.strptime('{0} 00:00:00'.format(end_date_timestamp), '%Y-%m-%d %H:%M:%S')) * 1000)
    return start_timestamp, end_timestamp


if __name__ == '__main__':
    # 获得快递,订单相关信息连接
    db_aplum = MySQLdb.connect(mysqlhost, mysqlusername, mysqlpasswd, db, charset='utf8')
    aplum_cursor = db_aplum.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    result_dict = dict()
    result_dict['月份'] = list()
    result_dict['卖家人数'] = list()
    result_dict['提交商品数'] = list()
    result_dict['上架商品数'] = list()
    result_dict['有过售出人数'] = list()
    result_dict['售出件数'] = list()
    result_dict['卖家收入'] = list()

    # start_date = '2018-11-01'
    date_list = ['2018-01', '2018-02', '2018-03',
                 '2018-04', '2018-05', '2018-06', '2018-07', '2018-08', '2018-09', '2018-10', '2018-11', '2018-12',
                 '2019-01', '2019-02', '2019-03', '2019-04', '2019-05', '2019-06', '2019-07', '2019-08']

    for date_tmp in date_list:
        # 开始日期,结束日期,例如: 2018-11-01 ~ 2018-11-30,程序采用date between形式,左右日期都包含
        start_date_tmp = date_tmp + '-01'
        end_date_tmp = get_end_date_tmp(date_tmp)

        # 开始时间戳: 2018-11-01 00:00:00  结束时间戳: 2018-12-01 00:00:00
        start_timestamp, end_timestamp = get_timestamp(start_date_tmp, end_date_tmp)

        result_dict = get_all_data(date_tmp, aplum_cursor, result_dict, start_date_tmp, end_date_tmp, start_timestamp, end_timestamp)
        # print(result_dict)

    fields = ['月份', '卖家人数', '提交商品数', '上架商品数', '有过售出人数', '售出件数', '卖家收入']
    df = pd.DataFrame(result_dict, columns=fields)
    today = date.today()
    writer = pd.ExcelWriter('/home/aplum/work_lh/seller_{0}.xlsx'.format(today))
    df.to_excel(excel_writer=writer, index=False, sheet_name='product', encoding='utf-8')
    writer.save()
    writer.close()
    print(tic())
