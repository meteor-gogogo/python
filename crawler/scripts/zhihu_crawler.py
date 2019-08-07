#!/usr/bin/env python
# coding=utf-8
import pandas as pd
import requests
from datetime import date, timedelta
import time
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
    file_path = 'C:\\Users\\liuhang\\Desktop\\zhihu\\zonghe\\jianding\\'
    path_dir = os.listdir(file_path)
    for all_dir in path_dir:
        child = os.path.join('%s%s' % (file_path, all_dir))
        with open(child, 'r', encoding='utf-8') as file:
            line = file.readline()
            line_json = json.loads(line, encoding='utf-8')
            goods_list = line_json['data']
            for goods in goods_list:
                if 'question' in goods['object'].keys():
                    goods_object = goods['object']
                    result_list.append(goods_object)
                else:
                    continue

            # print(goods_list)
                result_list.append(goods_object)
            file.close()


    fields = ['id','type', 'name', 'url', 'voteup_count', 'comment_count', 'question_time']

    for goods_tmp in result_list:
        # print(goods_tmp)
        key_tmp = goods_tmp['id']
        # if key_tmp in result_dict.keys():
        #     pass
        result_dict[key_tmp] = dict()

        result_dict[key_tmp]['id'] = str(goods_tmp['id'])
        result_dict[key_tmp]['type'] = str(goods_tmp['question']['type'])
        result_dict[key_tmp]['name'] = str(goods_tmp['question']['name'])
        result_dict[key_tmp]['url'] = 'https://www.zhihu.com/question/' + str(goods_tmp['question']['id'])
        result_dict[key_tmp]['voteup_count'] = str(goods_tmp['voteup_count'])
        result_dict[key_tmp]['comment_count'] = str(goods_tmp['comment_count'])
        timestamp = int(goods_tmp['created_time'])
        # 转换成localtime
        time_local = time.localtime(timestamp)
        # print('time_local:', time_local)
        # 转换成新的时间格式(2016-05-05 20:28:54)
        dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
        result_dict[key_tmp]['question_time'] = str(dt)

    # 利用pandas创建DataFrame，指定列名
    # print(result_dict.values())
    df = pd.DataFrame(result_dict.values(), columns=fields)
    # print(df.head(5))
    writer = pd.ExcelWriter('C:\\Users\\liuhang\\Desktop\\zhihu\\zonghe\\jianding\\综合-鉴定.xlsx'.format(today))
    df.to_excel(excel_writer=writer, index=False, sheet_name='product', encoding='utf-8')
    writer.save()
    writer.close()
