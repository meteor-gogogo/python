#!/usr/bin/env python
# coding=utf-8

from library.aplum_thrift.qserver.product_info.ttypes import *
from .score import ProductSort

IOS = 'ios'
ANDROID='android'
sysActID = set([250,253,552,2244,2245,2343,2344])


def GetProductSortSql(pidList):
    if len(pidList) == 0:
        return ""
    return "SELECT t.product_id,t.category,t.category_sort_score,t.show_rate FROM t_product_sort t where t.product_id in (%s)"%(','.join(pidList))

def GetProductSortScore(sortResults, pidList):
    if len(sortResults) == 0 and len(pidList) == 0:
        return {}
    sortDic = {}
    if len(sortResults) > 0:
        for row in sortResults:
            obj = ProductSort(int(row['product_id']))
            obj.category = int(row['category'])
            obj.show_rate = float(row['show_rate'])
            sortDic[obj.id] = obj
    for p in pidList:
        pInt = int(p)
        if pInt not in sortDic:
            sortDic[pInt] = ProductSort(pInt)
    return sortDic

def GetFeatureScore(client, row, platform,option='all'):
    sale_price = int(row['sale_price'])
    discount_price = int(row['discount_price'])
    is_promotion = 0
    if discount_price < sale_price:
        is_promotion = 1

    #兼容字段名
    argBid = ''
    if 'bid' in row:
        argBid = str(row['bid'])
    elif 'brand_id' in row:
        argBid = str(row['brand_id'])
    argCid = ''
    if 'cid' in row:
        argCid = str(row['cid'])
    elif 'category_id' in row:
        argCid = str(row['category_id'])
      
    obj = product_info(
        pid = str(row['id']),\
        discount_rate = str(row['discount_rate']),\
        bid = argBid,\
        cid = argCid,\
        source_of_supply = row['provider'],\
        degree = row['condition_level'],\
        is_promotion = str(is_promotion),\
        original_price = str(row['original_price']),\
        sale_price = str(sale_price), \
        discount_price = str(discount_price), \
        is_blackcard_member = str(row['blackcard_discount']),\
        os = platform,\
        status = row['status'])

    #print('\n'.join(['%s: %s'% item for item in obj.__dict__.items()]))

    split_os = 0
    if platform != "":
        split_os = 1
    if option == 'all':
        response = client.getCtrInfo(obj,split_os)
    elif option == 'cate':
        response = client.getCtrInfoCat(obj)

    if response.success:
        return response.weight
    return 0.0

def SetSortScore(client, row, obj):
    sort_score = GetFeatureScore(client,row, "")
    obj.sort_score = sort_score
    obj.category_sort_score = sort_score
    obj.show_dec_sort = sort_score * obj.show_rate

    obj.sort_score_ios = GetFeatureScore(client, row, IOS)
    obj.sort_score_android = GetFeatureScore(client, row, ANDROID)

