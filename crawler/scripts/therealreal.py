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
from pyquery import PyQuery as pq

stat_mysql_host = 'pc-2ze47js669jk7k187.rwlb.rds.aliyuncs.com'
stat_mysql_username = 'plumdb'
stat_mysql_passwd = 'plumdb@2019'
stat_db = 'aplum_stat'

if __name__ == '__main__':
    # 最终结果列表
    result_list = list()
    today = date.today()
    result_dict = dict()
    result_dict['id'] = list()
    result_dict['brand'] = list()
    result_dict['sex'] = list()
    result_dict['category'] = list()
    result_dict['description'] = list()
    result_dict['size'] = list()
    result_dict['price'] = list()
    result_dict['status'] = list()
    # file_path = 'C:\\Users\\liuhang\\Desktop\\therealreal\\sales\\'
    file_path = '/home/aplum/work_lh/therealreal_data/therealreal/'
    # 传入文件 example.html
    # child = os.path.join('%s%s' % (file_path, all_dir))

    path_dir = os.listdir(file_path)
    for all_dir in path_dir:
        child = os.path.join('%s%s' % (file_path, all_dir))
        try:
            if child.endswith("main"):
                doc = pq(filename=child)
                doc = doc('.plp-primary-content')
                doc = doc('.product-grid')
                doc = doc('.product-card-wrapper')
                doc = doc('.product-card')
                # print(doc)
                for i in doc.items():
                    # print(i)
                    i_arr = str(i).split('"')
                    if len(i_arr) < 4:
                        continue
                    # # print(int(str(i).split('"')[3]))
                    id = int(str(i).split('"')[3])
                    if id == '':
                        continue
                    # print(id)
                    result_dict['id'].append(id)

                    sex = str(str(str(i).split('"')[5]).split('/')[2])
                    if sex == '':
                        result_dict['sex'].append('')
                    else:
                        if sex not in ('women', 'men'):
                            result_dict['sex'].append('')
                        else:
                            result_dict['sex'].append(sex)

                    if sex not in ('women', 'men'):
                        category = str(str(str(i).split('"')[5]).split('/')[2])
                    else:
                        category = str(str(str(i).split('"')[5]).split('/')[3])

                    if category == '':
                        result_dict['category'].append('')
                    else:
                        result_dict['category'].append(category)

                    detail = i('.product-card__details')
                    if detail == '':
                        result_dict['brand'].append('')
                        result_dict['description'].append('')
                        result_dict['size'].append('')
                        result_dict['price'].append('')
                        result_dict['retail'].append('')
                        result_dict['status'].append('')
                    else:
                        for de in detail.items():
                            brand = str(de('.product-card__brand').text())
                            # print(brand)
                            if brand == '':
                                result_dict['brand'].append('')
                            else:
                                result_dict['brand'].append(brand)

                            description = str(de('.product-card__description').text())
                            # print(description)
                            if description == '':
                                result_dict['description'].append('')
                            else:
                                result_dict['description'].append(description)

                            size = str(de('.product-card__sizes').text())
                            # print(size)
                            if size == '':
                                result_dict['size'].append('')
                            else:
                                result_dict['size'].append(size)

                            price = str(de('.product-card__price').text())
                            # print(price)
                            if price == '':
                                result_dict['price'].append(0)
                            else:
                                price_tmp = str(price.split('$')[1].split('.')[0])
                                if ',' in price_tmp:
                                    price = int(str(price_tmp.split(',')[0]) + str(price_tmp.split(',')[1]))
                                else:
                                    price = int(price_tmp)
                                # print(price)
                                result_dict['price'].append(price)

                            # retail = str(de('.product-card__msrp').text())
                            # print(retail)
                            # if retail == '':
                            #     result_dict['retail'].append('')
                            # else:
                            #     retail = retail.split('$')[1]
                            #     result_dict['retail'].append(retail)

                            status_arr = str(de('.product-card__status'))
                            # print(type(status_arr))
                            if status_arr == '':
                                result_dict['status'].append('onsale')
                            else:
                                result_dict['status'].append('sold')
            else:
                continue
        except Exception as e:
            continue

    fields = ['id', 'sex', 'category', 'brand', 'description', 'size', 'price',  'status']
    df = pd.DataFrame(result_dict, columns=fields)
    # print(df)
    # print(len(result_dict['id']))
    # print(len(result_dict['sex']))
    # print(len(result_dict['category']))
    # print(len(result_dict['brand']))
    # print(len(result_dict['description']))
    # print(len(result_dict['size']))
    # print(len(result_dict['price']))
    # print(len(result_dict['status']))
    # print(len(result_dict['retail']))
    engine = create_engine(
        "mysql+pymysql://{0}:{1}@{2}/{3}?charset={4}".format(stat_mysql_username, stat_mysql_passwd, stat_mysql_host,
                                                             stat_db,
                                                             'utf8'))
    # 创建连接
    con = engine.connect()
    df.to_sql(name='therealreal', con=con, if_exists='append', index=False, chunksize=10000)
    print("写入数据成功")
