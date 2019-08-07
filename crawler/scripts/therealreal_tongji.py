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
    db = MySQLdb.connect(stat_mysql_host, stat_mysql_username, stat_mysql_passwd, stat_db, charset='utf8')
    cursor = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    cursor.execute("set names utf8")
    sql_category = "select brand, category, sex, count(*) 个数, avg(price) 均价, sum(case when status = 'sold' then 1 else 0 end) 售空个数, sum(case when status = 'sold' then price else 0 end) 售空金额 from therealreal group by brand, category, sex order by 个数 desc"
    sql_brand = "SELECT category, count(*) 个数 from therealreal group by category order by 个数 desc"
    cursor.execute(sql_category)
    result_category = cursor.fetchall()
    cursor.close()
    fields_category = ['品牌', '分类', '性别', '商品个数', '均价', '售空个数', '售空额']
    category_dict = dict()
    for field in fields_category:
        category_dict[field] = list()
    for row in result_category:
        category_dict['品牌'].append(row['brand'])
        category_dict['分类'].append(row['category'])
        category_dict['性别'].append(row['sex'])
        category_dict['商品个数'].append(row['个数'])
        category_dict['均价'].append(row['均价'])
        category_dict['售空个数'].append(row['售空个数'])
        category_dict['售空额'].append(row['售空金额'])
    writer = pd.ExcelWriter("/home/aplum/work_lh/products.xlsx")
    df1 = pd.DataFrame(category_dict, columns=fields_category)
    df1.to_excel(excel_writer=writer, sheet_name='therealreal', encoding='utf-8')
    db = MySQLdb.connect(stat_mysql_host, stat_mysql_username, stat_mysql_passwd, stat_db, charset='utf8')
    cursor = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    cursor.execute("set names utf8")
    cursor.execute(sql_brand)
    result_brand = cursor.fetchall()
    fields_brand = ['分类', '个数']
    brand_dict = dict()
    for field in fields_brand:
        brand_dict[field] = list()
    for row in result_brand:
        brand_dict['分类'].append(row['category'])
        brand_dict['个数'].append(row['个数'])
    df2 = pd.DataFrame(brand_dict, columns=fields_brand)
    df2.to_excel(excel_writer=writer, sheet_name='分类', encoding='utf-8')
    writer.save()
    writer.close()
    cursor.close()
    print("查询完成")