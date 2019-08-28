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
    file_path = '/home/aplum/work_lh/yiersandata/{0}/'.format(date.today())
    path_dir = os.listdir(file_path)
    for all_dir in path_dir:
        child = os.path.join('%s%s' % (file_path, all_dir))
        with open(child, 'r', encoding='utf-8') as file:
            line = file.readline()
            line_json = json.loads(line, encoding='utf-8')
            goods_list = line_json['data']['productList']
            # print(goods_list)
            result_list.append(goods_list)
            file.close()

    for goods_list_tmp in result_list:
        for goods_tmp in goods_list_tmp:
            # if len(goods_tmp) != 53:
            #     print(goods_tmp)
            #     continue
            if 'sku_info' not in goods_tmp.keys():
                key_tmp = goods_tmp['product_id']
                if key_tmp in result_dict.keys():
                    continue
                result_dict[key_tmp] = dict()
                result_dict[key_tmp]['delay'] = ''
                result_dict[key_tmp]['new_stock'] = ''
                result_dict[key_tmp]['size'] = ''
                result_dict[key_tmp]['sku_id'] = ''
                result_dict[key_tmp]['sort'] = ''
                result_dict[key_tmp]['stock'] = ''
                for key in goods_tmp.keys():
                    if key in ('picture', 'itemImgUrl', 'itemUrl', 'material_name', 'path', 'thumb_pic'):
                        continue
                    # elif key == 'sku_info':
                    #     result_dict[key_tmp]['delay'] = goods_tmp[key][i]['delay']
                    #     result_dict[key_tmp]['new_stock'] = goods_tmp[key][i]['new_stock']
                    #     result_dict[key_tmp]['size'] = goods_tmp[key][i]['size']
                    #     result_dict[key_tmp]['sku_id'] = goods_tmp[key][i]['sku_id']
                    #     result_dict[key_tmp]['sort'] = goods_tmp[key][i]['sort']
                    #     result_dict[key_tmp]['stock'] = goods_tmp[key][i]['stock']
                    else:
                        result_dict[key_tmp][key] = goods_tmp[key]
                result_dict[key_tmp]['date'] = str(today)
            else:
                sku_len = len(goods_tmp['sku_info'])
                for i in range(sku_len):
                    key_tmp = goods_tmp['product_id'] + goods_tmp['sku_info'][i]['sku_id']
                # if key_tmp in result_dict.keys():
                #     pass
                    result_dict[key_tmp] = dict()
                    for key in goods_tmp.keys():
                        if key in ('picture', 'itemImgUrl', 'itemUrl', 'material_name', 'path', 'thumb_pic'):
                            continue
                        elif key == 'sku_info':
                            result_dict[key_tmp]['delay'] = goods_tmp[key][i]['delay']
                            result_dict[key_tmp]['new_stock'] = goods_tmp[key][i]['new_stock']
                            result_dict[key_tmp]['size'] = goods_tmp[key][i]['size']
                            result_dict[key_tmp]['sku_id'] = goods_tmp[key][i]['sku_id']
                            result_dict[key_tmp]['sort'] = goods_tmp[key][i]['sort']
                            result_dict[key_tmp]['stock'] = goods_tmp[key][i]['stock']
                        else:
                            result_dict[key_tmp][key] = goods_tmp[key]
                    result_dict[key_tmp]['date'] = str(today)

    # 利用pandas创建DataFrame，指定列名
    # print(result_dict.values())
    fields = ['at', 'boxSlot', 'brand_country', 'brand_en_name', 'brand_id', 'color_id', 'color_name', 'favor',
              'hasSharePhoto', 'isPreHeat', 'is_alive', 'is_new', 'is_star', 'is_top', 'itemType',
              'lgTagUrl', 'market_price', 'material_id', 'originalPrice', 'partId',
              'presaleDisplay', 'productCategory', 'productLevel', 'productLevelName', 'product_id',
              'product_name', 'promotionTag', 'reason', 'rentDays', 'rentReduce', 'sId', 'server_time', 'sex',
              'showSalePrice', 'singleRentPriceText', 'singleRentPriceUnitText', 'singleRentPromotionTag',
              'delay', 'new_stock', 'size', 'sku_id', 'sort', 'stock', 'soldOut', 'stocknum', 'tagText', 'tagUrl',
              'type_id', 'type_name', 'wantWearCount', 'weekpdt', 'year']
    df = pd.DataFrame(result_dict.values(), columns=fields)
    print(df.head(5))
    # writer = pd.ExcelWriter('C:\\Users\\liuhang\\Desktop\\123.xlsx')
    # df.to_excel(excel_writer=writer, index=False, sheet_name='月表', encoding='utf-8')
    # writer.save()
    # writer.close()
    engine = create_engine(
        "mysql+pymysql://{0}:{1}@{2}/{3}?charset={4}".format(stat_mysql_username, stat_mysql_passwd, stat_mysql_host,
                                                             stat_db,
                                                             'utf8'))
    # 创建连接
    con = engine.connect()
    df.to_sql(name='yiersan_crawler_daily', con=con, if_exists='append', index=False, chunksize=10000)
    print("写入数据成功")
