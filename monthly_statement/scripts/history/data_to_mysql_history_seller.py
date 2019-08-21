#!/usr/bin/env python
# coding=utf-8
import pandas as pd
from datetime import date, timedelta, datetime
import time
from sqlalchemy import create_engine
import MySQLdb

mysql_host = 'rm-2zeixwnpc34127h5f191-vpc-rw.mysql.rds.aliyuncs.com'
mysql_user = 'plumdb'
mysql_passwd = 'plumdb@2018'
mysql_db = 'aplum_mis'

mysql_host = '127.0.0.1'
mysql_user = 'aplum'
mysql_passwd = 'plum2016'
mysql_db = 'aplum_mis'


def write_dict_to_mysql(result_dict_va):
    fields = ['type', 'second_name', 'exe_date', 'costs', 'today_actived_num', 'new_actived_user', 'avg_actived_costs',
              'new_registered_user', 'avg_registered_costs', 'avg_registered_rate', 'new_ordered_user',
              'avg_ordered_user_costs', 'avg_ordered_rate', 'new_ordered_num', 'avg_ordered_costs', 'kdj_costs',
              'order_costs', 'roi', 'new_seller', 'avg_seller_costs', 'new_jcmjs', 'avg_jcmjs_costs', 'avg_jcmjs_rate',
              'dyscje', 'seller_roi', 'ddyj_mjjc', 'mmjpjcb', 'create_time']
    df = pd.DataFrame(list(result_dict_va.values()), columns=fields)
    print(df)
    # writer = pd.ExcelWriter('/home/aplum/work_lh/data_dict_to_csv/2019-08-15-seller.xlsx')
    # df.to_excel(excel_writer=writer, index=False, sheet_name='月表', encoding='utf-8')
    # writer.save()
    # writer.close()
    # yesterday_month = str((date.today() + timedelta(days=-1)).replace(day=1))
    # checkData(yesterday_month)
    engine = create_engine(
        "mysql+pymysql://{0}:{1}@{2}/{3}?charset={4}".format(mysql_user, mysql_passwd, mysql_host,
                                                             mysql_db,
                                                             'utf8'))
    # 创建连接
    con = engine.connect()
    df.to_sql(name='t_market_month_seller', con=con, if_exists='append', index=False, chunksize=10000)
    print("写入数据成功")


def get_costs_by_source_date(cursor, source, start_date_tmp):
    costs = 0.0
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
    for row in source_data:
        if row['sum_costs'] is None:
            costs = 0.0
        else:
            costs = round(float(row['sum_costs']), 2)
    return costs


if __name__ == '__main__':
    today = date.today()
    db_market = MySQLdb.connect(mysql_host, mysql_user, mysql_passwd, mysql_db, charset='utf8')
    cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    # file_path = '/home/aplum/work_lh/data_dict_to_csv/2019-08-15-dict.csv'
    file_path = '/home/liuhang/2019-08-15-dict.csv'
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
                source = str(line_dict[key]['source'])
                if source in ('头条信息流', '微博信息流'):
                    continue
                result_dict[key] = list()
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
                elif source.endswith('KOL') or source.endswith('kol'):
                    source_type = 15
                else:
                    source_type = 14
                # if source_type in (6, 7, 8, 9):
                #     continue
                result_dict[key].append(int(source_type))
                if source_type in (14, 15):
                    source_tmp = str(line_dict[key]['source'])
                else:
                    source_tmp = ''
                result_dict[key].append(source_tmp)
                result_dict[key].append(str(line_dict[key]['month']))
                result_dict[key].append(round(float(line_dict[key]['costs']), 2))
                # if source_type in (1, 6, 7, 8, 9):
                #     result_dict[key].append(0.0)
                # else:
                #     result_dict[key].append(get_costs_by_source_date(cursor, source, str(line_dict[key]['month'])))
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

                result_dict[key].append(int(line_dict[key]['new_seller']))
                result_dict[key].append(float(line_dict[key]['avg_seller_costs']))
                result_dict[key].append(int(line_dict[key]['new_jcmjs']))
                result_dict[key].append(float(line_dict[key]['avg_jcmjs_costs']))
                result_dict[key].append(int(line_dict[key]['avg_jcmjs_rate']))
                result_dict[key].append(float(line_dict[key]['dyscje']))
                result_dict[key].append(float(line_dict[key]['seller_roi']))

                result_dict[key].append(int(line_dict[key]['ddyj_mjjc']))
                result_dict[key].append(float(line_dict[key]['mmjpjcb']))
                timestamp = int(time.time())
                result_dict[key].append(timestamp)
    for i in month_set:
        key_channel_all = 'channel_all&' + str(i)
        key_nature = 'nature&' + str(i)
        result_dict[key_channel_all] = [0.0] * 28
        result_dict[key_nature] = [0.0] * 28
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        # line = file.readline()
        for line in lines:
            line_dict = eval(line)
            for key in line_dict.keys():
                source = str(line_dict[key]['source'])
                if source in ('头条信息流', '微博信息流'):
                    continue
                if str(line_dict[key]['source']).endswith('KOL') or str(line_dict[key]['source']) in ('channel_kol', '抖音kol'):
                    # key_tmp = 'nature&' + str(line_dict[key]['month'])
                    # result_dict[key_tmp][4] = int(line_dict[key]['today_actived_num']) + int(result_dict[key_tmp][4])
                    # result_dict[key_tmp][5] = int(line_dict[key]['new_actived_user']) + int(result_dict[key_tmp][5])
                    # result_dict[key_tmp][7] = int(line_dict[key]['new_registered_user']) + int(result_dict[key_tmp][7])
                    # # result_dict[key_tmp][8] = int(line_dict[key]['today_actived_num']) + int(result_dict[key_tmp][4])
                    # result_dict[key_tmp][9] = int(int(result_dict[key_tmp][7]) / int(result_dict[key_tmp][5]) * 10000)

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
                    for i in range(3, 27):
                        result_dict[key_flow][i] = result_dict[key_flow][i] + result_dict[key][i]
                if source == '抖音kol':
                    key_kol = 'channel_kol&' + str(line_dict[key]['month'])
                    for i in range(3, 27):
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
            result_dict[key_kol] = [0.0] * 28
            result_dict[key_kol][0] = 4
            result_dict[key_kol][1] = ''
            result_dict[key_kol][2] = str(month)
            result_dict[key_kol][3] = 0.0
            # result_dict[key_kol] = [0.0] * 23
            for i in range(4, 27):
                result_dict[key_kol][i] = 0.0
            result_dict[key_kol][20] = int(time.time())

        for i in range(3, 27):
            result_dict[key_all][i] = result_dict[key_flow][i] + result_dict[key_kol][i]
        result_dict[key_all][27] = int(time.time())

        result_dict[key_nature][0] = 1
        result_dict[key_nature][1] = ''
        result_dict[key_nature][2] = str(month)
        result_dict[key_nature][3] = 0.0
        for i in range(3, 27):
            result_dict[key_nature][i] = result_dict[key_all_all][i] - result_dict[key_all][i]
        result_dict[key_nature][27] = int(time.time())

    write_dict_to_mysql(result_dict)

