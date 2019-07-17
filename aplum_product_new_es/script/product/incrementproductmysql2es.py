#!/usr/bin/env python
# coding=utf-8
import MySQLdb
import sys
from elasticsearch import Elasticsearch
import elasticsearch.helpers
import redis
import time
import traceback

import score as ss
import rpc.client as rpc

start = time.time()
tic = lambda: 'at %1.1f seconds' % (time.time() - start)

_type = 'product'     #修改为类型名
es_url = 'http://es-cn-0pp0gwsfu0005h3gr.elasticsearch.aliyuncs.com:9200'  #修改为elasticsearch服务器
es_username = 'elastic'
es_passwd = 'Plum-ela'

mysqlhost = 'rr-2ze7824567d8dgu1y.mysql.rds.aliyuncs.com'
mysqlusername = 'plumdb'
mysqlpasswd = 'plumdb@2018'
db='aplum'

redishost = 'r-2ze4eb46b642e1b4707.redis.rds.aliyuncs.com'
redispassword =   "Plum17redis"

"""******test-begin******"""
"""
es_url = 'http://127.0.0.1:9200'  #修改为elasticsearch服务器
es_username = 'elastic'
es_passwd = 'Plum-ela'

mysqlhost = '127.0.0.1'
mysqlusername = 'aplum'
mysqlpasswd = 'plum2016'
db='aplum'

redishost = '127.0.0.1'
redispassword = ""
"""
"""*******test-end*******"""

#productKey = "PRODUCT_INDEX_CONF"

def output_info(info):
    print(info)
    sys.exit()

def getmaxupdatetime(A, key):
    maxlist = list()
    for row in A:
        maxlist.append(row['product_update_time'])
    maxupdatetime = max(maxlist)
    newmaxlist = list()
    newmaxlist.append(str(maxupdatetime))
    newmaxlist.append(key)
    maxupdatetime = max(newmaxlist)
    return str(maxupdatetime)

def GetProductSortSql(pidList):
    return "SELECT t.product_id,t.category,t.category_sort_score,t.brand FROM t_product_sort t where t.product_id in (%s)"%(','.join(pidList))

if __name__ == '__main__':
    #获取目标索引
    r = redis.StrictRedis(host=redishost, port=6379, db=3,  password = redispassword)
    #test r = redis.StrictRedis(host=redishost, port=6379, db=2)
    #currentIndex = r.get(productKey)
    currentIndex = 'aplum_product'

    es = Elasticsearch([es_url],http_auth=(es_username, es_passwd),port=9200)
    #获取mysql 数据 导入 elastic
    db = MySQLdb.connect(mysqlhost, mysqlusername,  mysqlpasswd, db, charset='utf8')
    cursor = db.cursor(cursorclass = MySQLdb.cursors.DictCursor)
    cursor.execute("set names utf8")
    timestamp = r.get("last_aplum_timestamp")
    id = 0
    maxupdatetime = ""
    while (True) :
        sql = "SELECT p.id, p.name,p.user_id AS seller_id,(case p.customer_type when 'woman' then '女' when 'man' then '男' end) AS sex,p.cooperate_type,p.condition_level,p.condition_desc,p.onsale_time,p.customer_type,p.discount_price,p.discount_rate,p.long_desc,p.size,p.treatment,p.status,p.visible,p.in_offline_shop,p.original_price,p.sale_price,p.purchase_price,p.blackcard_discount,p.provider,b.id AS bid,b.name AS brand_name,b.name_zh AS brand_name_cn,b.alias_name AS brand_alias_name,b.brand_class,b.acceptance,b.region,c.id AS cid,c.group_id,c.name AS category_name,GROUP_CONCAT(distinct  atv.value_text) as attribute,p.last_update as last_update, p.product_update_time FROM t_product p,t_brand b, t_category c,t_product_attr_value atv WHERE p.brand_id = b.id AND p.category_id = c.id  AND p.id = atv.product_id AND p.product_update_time > '%s' and p.id >%d group by atv.product_id order by p.id  limit 1000"% (str(timestamp,encoding='utf-8'), int(id))
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) == 0 :
                break
            #get product sort score
            pidList = []
            for row in results:
                pidList.append(str(row['id']))
            sortDic = ss.GetProductSortScoreByDB(cursor, pidList) 
            for row in results:
                pid = int(row['id'])
                try:
                    sortDic[pid].category_sort_score = ss.GetFeatureScore(rpc.client, row)
                except Exception as e:
                    msg = traceback.format_exc()
                    print (msg)
                    break
            #print('update sort score len:%d'%len(sortDic))
            #end
            repeatid = ''
            package = []
            for row in results:
                id = int(row['id'])
                obj = ss.ProductSort(id)
                if id in sortDic.keys() and row['status'] == 'onsale':
                    obj = sortDic[id]

                if int(row['seller_id']) == 758050 :
                    repeatid  = str(row['id'])
                else:
                    repeatid  =  "%s_%s"%(str(row['seller_id']), str(row['bid']))

                esrow = {
                    "id": id,
                    "name" : "%s%s"%(str(row['name']), str(row['sex'])),
                    "bid" : int(row['bid']),
                    "cid" : int(row['cid']),
                    "discount_price": int(row['discount_price']),
                    "status" : str(row['status']),
                    "visible": int(row['visible']),
                    "discount_rate": float(row['discount_rate']),
                    "blackcard_discount": int(row['blackcard_discount']),
                    "onsale_time": int(row['onsale_time']),
                    "customer_type": str(row['customer_type']),
                    "original_price": int(row['original_price']),
                    "in_offline_shop": int(row['in_offline_shop']),
                    "cooperate_type": str(row['cooperate_type']),
                    "sale_price": int(row['sale_price']),
                    "size": str(row['size']),
                    "purchase_price": int(row['purchase_price']),
                    "seller_id": int(row['seller_id']),
                    "sex": str(row['sex']),
                    "treatment": str(row['treatment']),
                    "acceptance": int(row['acceptance']),
                    "region": str(row['region']),
                    "brand_alias_name": str(row['brand_alias_name']),
                    "long_desc": str(row['long_desc']),
                    "condition_desc": str(row['condition_desc']),
                    "brand_name": str(row['brand_name']),
                    "category_name": str(row['category_name']),
                    "attribute": str(row['attribute']),
                    "last_update": int(row['last_update']),
                    "repeatid":repeatid,
                    "condition_level": str(row['condition_level']),
                    "brand_name_cn": str(row['brand_name_cn']),
                    "group_id": int(row['group_id']),
                    "brand_class": str(row['brand_class']),
                    "category_score": obj.category,
                    "category_sort_score": obj.category_sort_score,
                    "brand_score": obj.brand
                }
                package.append( esrow )
            #批量插入es
            actions = [
                {
                    '_index': currentIndex,  #index
                    '_type': "product",  #type
                    '_id': d['id'],
                    '_source': d
                }
                for d in package
            ]
            #print actions
            success, _ = elasticsearch.helpers.bulk( es, actions)
            nextlasttimestamp = results[len(results)-1]['product_update_time']
            id = results[len(results)-1]['id']
            maxupdatetime = getmaxupdatetime(results, maxupdatetime)
            print(id, '----', nextlasttimestamp, '------last_timestamp:', maxupdatetime)
            time.sleep(1)
        except  Exception as e:
            output_info("Error:unable to fetch data, error=%s" % (str(e),))
            break

    if not maxupdatetime:
        print("nodata")
    else:
        print("最后的更新时间为:", maxupdatetime)
        r.set('last_aplum_timestamp', maxupdatetime)
    cursor.close()     #断开游标
    db.close()    #断开数据库
    rpc.close() #断开thrift
    print('Ended Polling: %s' % tic())
    print("success")
