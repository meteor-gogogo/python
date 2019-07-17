#!/usr/bin/env python
# coding=utf-8
'''
全量更新t_product_new 表数据 到es 中
'''
import MySQLdb
import sys
from elasticsearch import Elasticsearch
import elasticsearch.helpers
import json
import redis
import time
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

productKey = "PRODUCT_NEW_ARRIVAL_INDEX_CONF"

aliasIndex="aplum_new"
index1= "aplum_new_a1"
index2= "aplum_new_a2"

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
"""
"""*******test-end*******"""

mapping = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 1,
        "analysis": {
          "analyzer": {
            "by_smart": {
              "type": "custom",
              "tokenizer": "ik_smart",
              "filter": [
                "by_tfr",
                "by_sfr"
              ],
              "char_filter": [
                "by_cfr"
              ]
            }
          },
          "filter": {
            "by_tfr": {
              "type": "stop",
              "stopwords": [
                " "
              ]
            },
            "by_sfr": {
              "type": "synonym",
              "synonyms_path": "analysis/synonyms.txt"
            }
          },
          "char_filter": {
            "by_cfr": {
              "type": "mapping",
              "mappings": [
                "| => |"
              ]
            }
          }
        }
    },
	"mappings": {
		"product":{
			"include_in_all": "false",
			"dynamic": "true",
			"properties":{
  				"id":{
  					"type":"integer"
  				},
  				"name":{
  					"type":"text",
                    "analyzer": "ik_smart",
                    "search_analyzer": "ik_smart",
  					"fields": {
  						"py":{
  							"type":"text",
  				    		"analyzer": "pinyin",
                            "search_analyzer": "ik_smart"
  						},
                        "synonym":{
                            "type": "text",
                            "analyzer": "by_smart",
                            "search_analyzer": "by_smart"
                        }
  					}
  				},
  				"seller_id":{
  					"type":"integer"
  				},
  				"customer_type":{
  					"type":"keyword"
  				},
  				"bid":{
  					"type":"integer"
  				},
  				"dispatch_type":{
  					"type":"keyword"
  				},
  				"cid":{
  					"type":"integer"
  				},
  				"discount_price":{
  					"type":"integer"
  				},
  				"original_price":{
  					"type":"integer"
  				},
  				"sale_price":{
  					"type":"integer"
  				},
  				"purchase_price":{
  					"type":"integer"
  				},
  				"discount_rate":{
                    "type":"scaled_float",
                    "scaling_factor": 100
  				},
  				"condition_level":{
  					"type":"keyword"
  				},
  				"status":{
  					"type":"keyword"
  				},
  				"size":{
  					"type":"keyword"
  				},
  				"visible":{
  					"type":"short"
  				},
  				"in_offline_shop":{
  					"type":"integer"
  				},
  				"onsale_time":{
  					"type":"integer"
  				},
  				"blackcard_discount":{
  					"type":"short"
  				},
  				"cooperate_type":{
  					"type":"keyword"
  				},
                "newinfo": {
                    "type": "nested",
                    "properties": {
                        "versionid":    { "type": "integer"  },
                        "versionorder": { "type": "integer"  }
                    }
                }
		  	}
		}
	}
}

def output_info(info):
    print(info)
    sys.exit()

if __name__ == '__main__':
    #获取目标索引,当前索引
    targetIndex = ''
    currentIndex = ''
    try:
        es = Elasticsearch([es_url],http_auth=(es_username, es_passwd),port=9200)
        if es.indices.exists(aliasIndex):
            currentIndex =  list(es.indices.get(aliasIndex).keys())[0]
            if currentIndex == index1 :
                targetIndex = index2
            elif currentIndex == index2 :
                targetIndex = index1
            else:
                output_info("get index=%s error, index=%s"%(aliasIndex, currentIndex))
        else:
            currentIndex = index1
            targetIndex = index2

        #删除索引
        if es.indices.exists(targetIndex):
            re = es.indices.delete(index=targetIndex)
            result = json.dumps(re, indent=2)
    except Exception as e:
        output_info("get index=%s error, error=%s" % (aliasIndex,str(e)))

    #创建索引
    try:
        es.indices.create(index=targetIndex,ignore=400, body=mapping)
        #test es.indices.create(index=targetIndex,ignore=400)
    except  Exception as e:
        output_info("es create product mapping error, index=%s, error=%s" % (targetIndex, str(e)))

    #获取mysql 数据 导入 elastic
    db = MySQLdb.connect(mysqlhost, mysqlusername,  mysqlpasswd, db, charset='utf8')
    cursor = db.cursor(cursorclass = MySQLdb.cursors.DictCursor)
    cursor.execute("set names utf8")
    id = 0
    while (True) :
        sql = "SELECT p.id, p.name, p.user_id as seller_id,p.customer_type,p.brand_id, p.cooperate_type,p.dispatch_type,p.category_id, p.discount_price,p.size,p.original_price,p.sale_price, p.purchase_price, p.last_update,p.discount_rate, p.condition_level, p.status, p.in_offline_shop,p.onsale_time,p.blackcard_discount,p.visible,group_concat(pn. version_id,':', pn.order_in_version SEPARATOR ',') as newinfo  FROM t_product_new as pn , t_product as p WHERE p.id = pn.product_id AND  p.id > %d  group by p.id order by p.id  asc"% int(id)
        print(sql)
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
                if row['newinfo'] :
                    nestarr = row['newinfo'].split(',')
                    for nestinfo in nestarr:
                        versionid, versionorder = nestinfo.split(":")
                        nestdic = {
                            "versionid":versionid,
                            "versionorder":versionorder
                        }
                        nestlist.append(nestdic)
                        #activityinfo = json.dumps(nestlist)
                        newinfo = nestlist
                esrow["newinfo"] = newinfo
                package.append( esrow )
            #批量插入es
            actions = [
                {
                    '_index': targetIndex,  #index
                    '_type': "product",  #type
                    '_id':d['id'],
                    '_source': d
                }
                for d in package
            ]

            elasticsearch.helpers.bulk( es, actions)
            id = results[len(results)-1]['id']
            print(id)
            time.sleep(1)
        except  Exception as e:
            output_info("Error:unable to fetch data, error=%s" % (str(e),))
            break

    sql2 = "SELECT p.id, p.name, p.product_update_time  FROM t_product_new as pn , t_product as p WHERE p.id = pn.product_id  group by p.id  order by p.product_update_time desc limit 1"
    cursor.execute(sql2)
    result = cursor.fetchone()
    if len(result) > 0:
        r = redis.StrictRedis(host=redishost, port=6379, db=3,  password = redispassword)
        #test r = redis.StrictRedis(host=redishost, port=6379, db=2)
        r.set('last_new_timestamp', str(result['product_update_time']))
    
    print("update time:", str(result['product_update_time']))
    print("current time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    #切换index别名，原子操作
    if es.indices.exists(aliasIndex):
        doc = {
            "actions" : [
                { "remove": { "index": currentIndex, "alias": aliasIndex}},
                { "add":    { "index": targetIndex, "alias": aliasIndex}}
            ]
        }
        res = es.indices.update_aliases(body=doc)
        if res['acknowledged'] != True:
            output_info("es put alias name error, index=%s, error=%s" % (aliasIndex,str(e)))
    else:
        res = es.indices.put_alias(index=targetIndex,name=aliasIndex)
        if res['acknowledged'] != True:
            output_info("es put alias name error, index=%s, error=%s" % (aliasIndex,str(e)))

    print ("es put alias,index=%s, name=%s"%(targetIndex, aliasIndex))

    cursor.close()     #断开游标
    db.close()    #断开数据库

    print('Ended Polling: %s' % tic())
    print("success")
