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
    # 声明总页数
    page_num = 0
    # 获取总页数
    today = date.today()
    yesterday = today + timedelta(days=-1)
    result_dict = dict()
    file_path = 'C:\\Users\\liuhang\\Desktop\\plum\\爬虫\\只二\\{0}\\'.format(today)
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
                result_dict[key_tmp][key] = goods_tmp[key]
            result_dict[key_tmp]['date'] = str(today)

    # 利用pandas创建DataFrame，指定列名
    # print(result_dict.values())
    df = pd.DataFrame(result_dict.values(), columns=fields)
    # print(df.head(5))
    writer = pd.ExcelWriter('C:\\Users\\liuhang\\Desktop\\plum\\爬虫\\只二\\只二商品_{0}.xlsx'.format(today))
    df.to_excel(excel_writer=writer, index=False, sheet_name='product', encoding='utf-8')
    # explain_dict = {'source': {'1': '自营', '2': '卖家', '3': '日本直邮'},
    #                 'forbidden': {'1': '不支持退换货', '2': '支持72小时退换货'},
    #                 'onsale': {'1': '限时特价', '0': '无限时特价'},
    #                 'haveRedPacket': {'TRUE': '可领券', 'FALSE': '不可领券'},
    #                 'directDesc': {'1': '日本直邮', '0': '不是日本直邮'},
    #                 'newSign': {'1': '全新', '0': '非全新'},
    #                 'nickName': {'卖家昵称'}}
    # df2 = pd.DataFrame(explain_dict.values(), columns=['source', 'forbidden', 'onsale', 'haveRedPacket', 'directDesc',
    #                                                    'newSign', 'nickName'])
    # df2.to_excel(excel_writer=writer, index=False, sheet_name='字段解释', encoding='gbk')
    writer.save()
    writer.close()
