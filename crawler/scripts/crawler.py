# coding=utf-8

import pandas as pd
import re
# import requests
import urllib.request
import urllib.parse
import json
# import urllib2
import ssl
from datetime import datetime


if __name__ == '__main__':
    context = ssl._create_unverified_context()
    # APP下划到底部，继续下划，一次刷新为1页，获取前1000页
    for page in range(2, 100):
        url = "https://app.aplum.com/product/mixed-list?cid=10&api_ver=5&vfm=index:tab2&banner_id=1169&api_ver=5&count=-1&tab=tab2&page={0}&vfm=index:tab2:beta&sid=bcf9c4c346721d1c520874a9c0abe49f".format(page)
        response = urllib.request.urlopen(url, context=context)
        page = response.read().decode('utf-8')
        data_json = json.loads(page)
        print(data_json['data']['models'])
        # url_param = urllib.parse.urlencode(headers)
        # # page = urllib3.connection_from_url(url).urlopen()
        # print(page.read())
        # r = requests.get(url, headers, False)
        # 将返回的结果json格式化
        # res_dict = r.json()
        # res_dict的样式为：
        # {'code': 0, 'msg': 'success', 'timestamp': 1560931582454, 'data': {'totalPage': 28685, 'labelsInfo': [], 'goodsList': [{'goodsId': 3469936
        # goods_list表示是一批商品的集合，每批次为20个商品，表示刷新一次，能够刷出20个商品
        # goods_list = res_dict["data"]["goodsList"]
        # print(res_dict)
        # 将这个商品列表添加到结果列表中，方便后续操作
    #     result_list.append(goods_list)
    # # 提取某个具体商品的所有属性值，放到列表中，以便初始化商品字典
    # fields = list(result_list[0][0].keys())
    # # 建立空的商品字典
    # result_dict = dict()
    # # 初始化商品字典
    # for field in fields:
    #     result_dict[field] = list()
    # # 遍历上边的结果列表，把商品信息放到商品字典中
    # for goods_list_tmp in result_list:
    #     for goods_tmp in goods_list_tmp:
    #         for key in goods_tmp.keys():
    #             # 商品字典中的值为具体属性相对应的列表，取出之前的列表，将本次的值添加到这个列表中
    #             value_list = result_dict[key]
    #             value_list.append(goods_tmp[key])
    # # 利用pandas创建DataFrame，指定列名
    # df = pd.DataFrame(result_dict, columns=fields)
    # # 写入excel中，sheet_name表示excel中下边sheet的名字
    # df.to_excel('C:\\Users\\liuhang\\Desktop\\plum\\爬虫\\心上首页推荐商品信息.xlsx', sheet_name='prooducts')
