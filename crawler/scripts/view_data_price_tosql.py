#!/usr/bin/env python
# coding=utf-8
import pandas as pd
import requests
from datetime import date, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pymysql
import json
import os
import random

stat_mysql_host = 'localhost'
stat_mysql_username = 'root'
stat_mysql_passwd = 'abcdefg'
stat_db = 'aplum'

if __name__ == '__main__':
    # 最终结果列表
    result_list = list()
    today = date.today()
    result_dict = dict()
    file_path = '/data/'
    path_dir = os.listdir(file_path)
    for all_dir in path_dir:
        child = os.path.join('%s%s' % (file_path, all_dir))
        with open(child, 'r', encoding='utf-8') as file:
            for line in file:
                line_json = json.loads(line, encoding='utf-8')
                product_list = line_json['ItemActions']
                for product in product_list:
                    if len(list(product.keys())) != 3:
                        continue
                    key = product['id']
                    if key in result_dict.keys():
                        continue
                    result_dict[key] = dict()
                    result_dict[key]['id'] = product['id']
                    result_dict[key]['position'] = product['position']
                    result_dict[key]['status'] = product['status']
                    original_price = float(random.randint(1000, 10000))
                    discount_price = round(original_price - original_price / 15, 2)
                    result_dict[key]['original_price'] = original_price
                    result_dict[key]['discount_price'] = discount_price
                    result_dict[key]['discount_rate'] = round((discount_price / original_price), 2)
                    result_dict[key]['sale_price'] = round(discount_price + discount_price * 0.2, 2)
            file.close()

    fields = ['id', 'position', 'status', 'original_price', 'discount_price', 'discount_rate', 'sale_price']
    # for goods_list_tmp in result_list:
    #     for goods_tmp in goods_list_tmp:
    #         # print(goods_tmp)
    #         key_tmp = goods_tmp['id']
    #         # if key_tmp in result_dict.keys():
    #         #     pass
    #         result_dict[key_tmp] = dict()
    #         for key in goods_tmp.keys():
    #             if key == 'topicTag':
    #                 result_dict[key_tmp][key] = ''
    #             else:
    #                 result_dict[key_tmp][key] = goods_tmp[key]
    #         result_dict[key_tmp]['date'] = str(today)

    # 利用pandas创建DataFrame，指定列名
    # print(result_dict.values())
    df = pd.DataFrame(result_dict.values(), columns=fields)
    engine = create_engine(
        "mysql+pymysql://{0}:{1}@{2}/{3}?charset={4}".format(stat_mysql_username, stat_mysql_passwd, stat_mysql_host,
                                                             stat_db,
                                                             'utf8'))
    # 创建连接
    con = engine.connect()
    df.to_sql(name='t_product', con=con, if_exists='append', index=False, chunksize=10000)
    print("写入数据成功")
