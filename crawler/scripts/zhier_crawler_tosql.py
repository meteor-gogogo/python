#!/usr/bin/env python
# coding=utf-8
import pandas as pd
import requests
from datetime import date, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import MySQLdb
import json
import os

stat_mysql_host = 'pc-2ze47js669jk7k187.rwlb.rds.aliyuncs.com'
stat_mysql_username = 'plumdb'
stat_mysql_passwd = 'plumdb@2019'
stat_db = 'aplum_stat'

if __name__ == '__main__':
    # 最终结果列表
    result_list = list()
    today = date.today()
    result_dict = dict()
    file_path = '/home/aplum/work_lh/zhierdata/{0}/'.format(today)
    path_dir = os.listdir(file_path)
    for all_dir in path_dir:
        child = os.path.join('%s%s' % (file_path, all_dir))
        with open(child, 'r', encoding='utf-8') as file:
            line = file.readline()
            line_json = json.loads(line, encoding='utf-8')
            goods_list = line_json['data']['list']
            # print(goods_list)
            result_list.append(goods_list)
            file.close()

    fields = list(result_list[0][0].keys())
    fields.append('date')
    for goods_list_tmp in result_list:
        for goods_tmp in goods_list_tmp:
            # print(goods_tmp)
            key_tmp = goods_tmp['id']
            # if key_tmp in result_dict.keys():
            #     pass
            result_dict[key_tmp] = dict()
            for key in goods_tmp.keys():
                if key == 'topicTag':
                    result_dict[key_tmp][key] = ''
                else:
                    result_dict[key_tmp][key] = goods_tmp[key]
            result_dict[key_tmp]['date'] = str(today)

    # 利用pandas创建DataFrame，指定列名
    # print(result_dict.values())
    df = pd.DataFrame(result_dict.values(), columns=fields)
    engine = create_engine(
        "mysql+pymysql://{0}:{1}@{2}/{3}?charset={4}".format(stat_mysql_username, stat_mysql_passwd, stat_mysql_host,
                                                             stat_db,
                                                             'utf8'))
    # 创建连接
    con = engine.connect()
    df.to_sql(name='zhier_crawler_daily', con=con, if_exists='append', index=False, chunksize=10000)
    print("写入数据成功")
