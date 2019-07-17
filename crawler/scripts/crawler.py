# coding=utf-8

import pandas as pd
import requests
from datetime import datetime


if __name__ == '__main__':
    # 最终结果列表
    result_list = list()
    # APP下划到底部，继续下划，一次刷新为1页，获取前1000页
    for page in range(1):
        # url中包含时间戳参数
        # 因为后台有可能会校验传入的时间戳和当前时间戳的差值，如果差值过大，有可能会报错，故生成当前时间的时间戳
        timestamp = int(datetime.timestamp(datetime.now()))
        url = "https://api.91xinshang.com/home/index/v7/?typeId=0&listTime=&showZhiyou=1&abversion=b&sign=8a3d6d759e25f5442a1cfea6d866d3bc&pageIndex={0}&timestamp={1}".format(page, timestamp)
        # 定义请求头，信息来源于fiddler抓包
        headers = {
            "appId": "com.91sph.SPH0",
            "deviceType": "phone",
            "market": "appStore",
            "Proxy - Connection": "close",
            "channel": "ios",
            "appVersionCode": "47",
            "appVersion": "4.4.2",
            "Accept - Encoding": "gzip",
            "Content - Type": "application / x - www - form - urlencoded; charset = utf - 8",
            "deviceToken": "3955859E-9631 - 4CFA - A1EF - B2E706BDAE51",
            "sessionId": "",
            "User - Agent": "心上 4.4.2 rv: 8 (iPhone; iOS 12.1.4; zh_CN)",
            "Content - Length": "1000",
            "distinctId": "D6496B00-04F3-4341-9985-2F2216FCAE1B",
            "userGC": ",",
            "Connection": "close",
            "jpushId": "141fe1da9ee8baa1f26",
            "Cookie": "JSESSIONID=abcWQsx_bJMjccpWQtUTw; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2216b69a8c0920-04cd0d993ac4958-2d11714e-181760-16b69a8c09345b%22%2C%22%24device_id%22%3A%2216b69a8c0920-04cd0d993ac4958-2d11714e-181760-16b69a8c09345b%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_referrer%22%3A%22%22%2C%22%24latest_referrer_host%22%3A%22%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%7D%7D"
        }
        # 最后一个参数为verify,设置为False，表示忽略SSH验证，如果不写，默认为True，会报SSH错误
        r = requests.post(url, headers, False)
        # 将返回的结果json格式化
        res_dict = r.json()
        # res_dict的样式为：
        # {'code': 0, 'msg': 'success', 'timestamp': 1560931582454, 'data': {'totalPage': 28685, 'labelsInfo': [], 'goodsList': [{'goodsId': 3469936
        # goods_list表示是一批商品的集合，每批次为20个商品，表示刷新一次，能够刷出20个商品
        goods_list = res_dict["data"]["goodsList"]
        print(goods_list)
        # 将这个商品列表添加到结果列表中，方便后续操作
        result_list.append(goods_list)
    # 提取某个具体商品的所有属性值，放到列表中，以便初始化商品字典
    fields = list(result_list[0][0].keys())
    # 建立空的商品字典
    result_dict = dict()
    # 初始化商品字典
    for field in fields:
        result_dict[field] = list()
    # 遍历上边的结果列表，把商品信息放到商品字典中
    for goods_list_tmp in result_list:
        for goods_tmp in goods_list_tmp:
            for key in goods_tmp.keys():
                # 商品字典中的值为具体属性相对应的列表，取出之前的列表，将本次的值添加到这个列表中
                value_list = result_dict[key]
                value_list.append(goods_tmp[key])
    # 利用pandas创建DataFrame，指定列名
    df = pd.DataFrame(result_dict, columns=fields)
    # 写入excel中，sheet_name表示excel中下边sheet的名字
    df.to_excel('C:\\Users\\liuhang\\Desktop\\plum\\爬虫\\心上首页推荐商品信息.xlsx', sheet_name='prooducts')
