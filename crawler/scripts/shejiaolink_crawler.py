#!/usr/bin/env python
# coding=utf-8
import pandas as pd
import requests
from datetime import date, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import MySQLdb

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
    for page in range(1):
        url = "https://app.shejiaolink.cn/home/getGoodsCate?cateType=&secondCate=&sortName=&sortType=&customerId=&pageSize=10&page={0}&status=".format(page)
        # 定义请求头，信息来源于fiddler抓包
        headers = {
            "Host": "app.shejiaolink.cn",
            "Content-Type": "application/json",
            "Accept-Language": "zh-cn",
            "Referer": "https://servicewechat.com/wx2def03f4aea5824c/75/page-frame.html",
            "Accept": "*/*",
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 12_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/7.0.3(0x17000321) NetType/WIFI Language/zh_CN",
            "Content-Length": "0",
            "Connection": "keep-alive",
            "Accept-Encoding": "br, gzip, deflate"
        }
        # 最后一个参数为verify,设置为False，表示忽略SSH验证，如果不写，默认为True，会报SSH错误
        r = requests.post(url, headers, False)
        # 将返回的结果json格式化
        res_dict = r.json()
        # res_dict的样式为：
        # {'result': 200, 'msg': '', 'data': {'pageNum': '1', 'pageSize': '10', 'size': '10', 'orderBy': 'create_time desc', 'startRow': '1', 'endRow': '10', 'total': '2080', 'pages': '208', 'list': [{'id': '1401', 'goodsName': '古驰 蓝色 S
        page_num = int(res_dict['data']['pages'])

    for page in range(1, page_num):
        url = "https://app.shejiaolink.cn/home/getGoodsCate?cateType=&secondCate=&sortName=&sortType=&customerId=&pageSize=10&page={0}&status=".format(
            page)
        # 定义请求头，信息来源于fiddler抓包
        headers = {
            "Host": "app.shejiaolink.cn",
            "Content-Type": "application/json",
            "Accept-Language": "zh-cn",
            "Referer": "https://servicewechat.com/wx2def03f4aea5824c/75/page-frame.html",
            "Accept": "*/*",
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 12_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/7.0.3(0x17000321) NetType/WIFI Language/zh_CN",
            "Content-Length": "0",
            "Connection": "keep-alive",
            "Accept-Encoding": "br, gzip, deflate"
        }
        # 最后一个参数为verify,设置为False，表示忽略SSH验证，如果不写，默认为True，会报SSH错误
        r = requests.post(url, headers, False)
        res_dict = r.json()
        # goods_list表示是一批商品的集合，每批次为20个商品，表示刷新一次，能够刷出20个商品
        goods_list = res_dict["data"]["list"]
        # 将这个商品列表添加到结果列表中，方便后续操作
        result_list.append(goods_list)
    # 提取某个具体商品的所有属性值，放到列表中，以便初始化商品字典
    fields = list(result_list[0][0].keys())
    fields.append('date')
    # 建立空的商品字典
    result_dict = dict()
    # 初始化商品字典
    for field in fields:
        result_dict[field] = list()
    # 遍历上边的结果列表，把商品信息放到商品字典中
    day = date.today()
    # yesterday = day + timedelta(days=-1)
    for goods_list_tmp in result_list:
        for goods_tmp in goods_list_tmp:
            for key in goods_tmp.keys():
                # 商品字典中的值为具体属性相对应的列表，取出之前的列表，将本次的值添加到这个列表中
                value_list = result_dict[key]
                value_list.append(goods_tmp[key])
            timestamp_list = result_dict['date']
            timestamp_list.append(day)
    # 利用pandas创建DataFrame，指定列名
    df = pd.DataFrame(result_dict, columns=fields)

    engine = create_engine(
        "mysql+pymysql://{0}:{1}@{2}/{3}?charset={4}".format(stat_mysql_username, stat_mysql_passwd, stat_mysql_host,
                                                             stat_db,
                                                             'utf8'))
    # 创建连接
    con = engine.connect()
    df.to_sql(name='shejiaolink_crawler_daily', con=con, if_exists='append', index=False)
    print("写入数据成功")
    day = date.today()
    # yesterday = day + timedelta(days=-1)
    db = MySQLdb.connect(stat_mysql_host, stat_mysql_username, stat_mysql_passwd, stat_db, charset='utf8')
    cursor = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    cursor.execute("set names utf8")
    sql_category = "SELECT ta.`分类`, ta.`总商品数`, ta.`总金额`, ta.`平均售价`, ta.`平均市价折扣率`, ta.`平均市场价`, ta.`售出数`, ta.`售出总金额`, ta.售出总金额 / ta.售出数 AS '平均售价', ta.`售出总金额` / ta.`售出市场价总金额` as '售出折扣率', ta.`售出市场价总金额` / ta.`售出数` as '平均市场价', ta.`售出数` / ta.`总商品数` as '售出率' FROM ( SELECT goodsCategory AS '分类', count( * ) AS '总商品数', sum( salePrice ) AS '总金额', avg( salePrice ) AS '平均售价', avg( salePrice / costPrice ) AS '平均市价折扣率', avg(costPrice) as '平均市场价', SUM( CASE WHEN STATUS = 3 THEN salePrice ELSE 0 END ) AS '售出总金额', SUM( CASE WHEN STATUS = 3 THEN costPrice ELSE 0 END ) AS '售出市场价总金额', SUM( CASE WHEN STATUS = 3 THEN 1 ELSE 0 END ) AS '售出数' FROM `shejiaolink_crawler_daily` WHERE date = '{0}' GROUP BY goodsCategory ) ta".format(day)
    sql_brand = "SELECT ta.`品牌(英文)`, ta.`品牌(中文)`, ta.`商品数`, ta.`总价值`, ta.`平均售价`, ta.`平均市价折扣率`, ta.`平均市场价`, ta.`售出数`, ta.`售出总金额`, ta.售出总金额 / ta.售出数 AS '平均售价', ta.`售出总金额` / ta.`售出市场价总金额` as '售出折扣率', ta.`售出市场价总金额` / ta.`售出数` as '平均市场价', ta.`售出数` / ta.`商品数` as '售出率' FROM ( SELECT brandEn as '品牌(英文)', brandName as '品牌(中文)', count( * ) AS '商品数', sum( salePrice ) AS '总价值', avg( salePrice ) AS '平均售价', avg( salePrice / costPrice ) AS '平均市价折扣率', avg(costPrice) as '平均市场价', SUM( CASE WHEN STATUS = 3 THEN salePrice ELSE 0 END ) AS '售出总金额', SUM( CASE WHEN STATUS = 3 THEN costPrice ELSE 0 END ) AS '售出市场价总金额', SUM( CASE WHEN STATUS = 3 THEN 1 ELSE 0 END ) AS '售出数' FROM `shejiaolink_crawler_daily` WHERE date = '{0}' GROUP BY brandEN, brandName ) ta ORDER BY ta.`商品数` desc".format(day)
    cursor.execute(sql_category)
    result_category = cursor.fetchall()
    cursor.close()
    fields_category = list(result_category[0].keys())
    category_dict = dict()
    for field in fields_category:
        category_dict[field] = list()
    for row in result_category:
        for key in row.keys():
            value_list = category_dict[key]
            value_list.append(row[key])
    writer = pd.ExcelWriter("/home/aplum/tmp/{0}-products.xlsx".format(day))
    df1 = pd.DataFrame(category_dict, columns=fields_category)
    df1.to_excel(excel_writer=writer, sheet_name='category', encoding='utf-8')
    db = MySQLdb.connect(stat_mysql_host, stat_mysql_username, stat_mysql_passwd, stat_db, charset='utf8')
    cursor = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    cursor.execute("set names utf8")
    cursor.execute(sql_brand)
    result_brand = cursor.fetchall()
    fields_brand = list(result_brand[0].keys())
    brand_dict = dict()
    for field in fields_brand:
        brand_dict[field] = list()
    for row in result_brand:
        for key in row.keys():
            value_list = brand_dict[key]
            value_list.append(row[key])
    df2 = pd.DataFrame(brand_dict, columns=fields_brand)
    df2.to_excel(excel_writer=writer, sheet_name='brand', encoding='utf-8')
    writer.save()
    writer.close()
    cursor.close()
    print("查询完成")
