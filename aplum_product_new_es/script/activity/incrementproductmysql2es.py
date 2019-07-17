#!/usr/bin/env python
# coding=utf-8
import MySQLdb
import codecs
import sys
from elasticsearch import Elasticsearch
import elasticsearch.helpers
import redis
import time
start = time.time()
tic = lambda: 'at %1.1f seconds' % (time.time() - start)

_type = 'product'     #修改为类型名
es_url = 'http://es-cn-0pp0gwsfu0005h3gr.elasticsearch.aliyuncs.com:9200'  #修改为elasticsearch服务器
es_username = 'elastic'
es_passwd = 'Plum-ela'

mysqlhost = 'rm-2zeixwnpc34127h5f191-vpc-rw.mysql.rds.aliyuncs.com'
mysqlusername = 'plumdb'
mysqlpasswd = 'plumdb@2018'
db='aplum'

redishost = 'r-2ze4eb46b642e1b4707.redis.rds.aliyuncs.com'
redispassword =   "Plum17redis"

#productKey = "PRODUCT_ACTIVITY_INDEX_CONF"
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


if __name__ == '__main__':
    #获取目标索引
    r = redis.StrictRedis(host=redishost, port=6379, db=3,  password = redispassword)
    #currentIndex = r.get(productKey)
    currentIndex = 'aplum_activity'

    es = Elasticsearch([es_url],http_auth=(es_username, es_passwd),port=9200)
    #获取mysql 数据 导入 elastic
    db = MySQLdb.connect(mysqlhost, mysqlusername,  mysqlpasswd, db, charset='utf8')
    cursor = db.cursor(cursorclass = MySQLdb.cursors.DictCursor)
    cursor.execute("set names utf8")
    timestamp = r.get("last_timestamp")
    id = 0
    maxupdatetime = ""
    while (True) :
        sql = "SELECT p.id, p.name, p.user_id as seller_id,p.customer_type,p.brand_id, p.cooperate_type,p.dispatch_type,p.category_id, p.discount_price,p.size,p.original_price,p.sale_price, p.purchase_price, p.last_update,p.discount_rate, p.condition_level, p.status, p.in_offline_shop,p.onsale_time,p.blackcard_discount,p.visible,group_concat(ap.activity_id,':', ap.order SEPARATOR ',') as activityinfo, p.last_update, p.product_update_time FROM t_product p,t_activity_product ap WHERE  p.id = ap.product_id AND p.product_update_time > '%s' and p.id > %d  group by p.id order by p.id limit 1000" % (str(timestamp, encoding='utf-8'), int(id))

        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) == 0 :
                break
            package = []
            for row in results:
                esrow = {
                    "id":int(row['id']),
                    "name" : str(row['name']),
                    "bid" : int(row['brand_id']),
                    "cid" : int(row['category_id']),
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
                    "dispatch_type": str(row['dispatch_type']),
                    "size": str(row['size']),
                    "purchase_price": int(row['purchase_price']),
                    "seller_id": int(row['seller_id']),
                    "condition_level": str(row['condition_level'])
                }
                nestlist = []
                if row['activityinfo'] :
                    nestarr = row['activityinfo'].split(',')
                    for nestinfo in nestarr:
                        activityid, order = nestinfo.split(":")
                        nestdic = {
                            "id":activityid,
                            "order":order
                        }
                        nestlist.append(nestdic)
                        #activityinfo = json.dumps(nestlist)
                    activityinfo = nestlist
                esrow["activityinfo"] = activityinfo
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
            success, _ = elasticsearch.helpers.bulk( es, actions)
            nextlasttimestamp = results[len(results)-1]['product_update_time']
            id = results[len(results)-1]['id']
            maxupdatetime = getmaxupdatetime(results, maxupdatetime)
            print(id, '----', nextlasttimestamp, '------last_timestamp:', maxupdatetime, '-----timstamp:', str(timestamp), '-----success:', success)
            time.sleep(1)

        except  Exception as e:
            output_info("Error:unable to fetch data, error=%s" % (str(e),))
            break

    if not maxupdatetime :
        print("no data")
    else:
        print("最后的更新时间为:", maxupdatetime)
        r.set('last_timestamp', maxupdatetime)
    cursor.close()     #断开游标
    db.close()    #断开数据库

    print('Ended Polling: %s' % tic())
    print("success")
