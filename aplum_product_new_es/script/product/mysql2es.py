#!/usr/bin/env python
# coding=utf-8
import MySQLdb
import sys
from elasticsearch import Elasticsearch
import elasticsearch.helpers
import json
import redis
import time
import traceback

import rpc.client as rpc
import score as ss

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

#productKey="PRODUCT_INDEX_CONF"
aliasIndex="aplum_product"
index1= "aplum_a1"
index2= "aplum_a2"

mapping ={
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
            },
            "by_max_word": {
                "type": "custom",
                "tokenizer": "ik_max_word",
                "filter": ["by_tfr","by_sfr"],
                "char_filter": ["by_cfr"]
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
  						"raw":{
  							"type":"keyword"
  						},
  						"py":{
  							"type":"text",
  				    		"analyzer": "pinyin",
                            "search_analyzer": "ik_smart"
  						},
                        "synonym":{
                            "type": "text",
                            "analyzer": "by_smart",
                            "search_analyzer": "by_smart"
                        },
                        "ikmax":{
                            "type":"text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        "sy_ikmax":{
                            "type": "text",
                            "analyzer": "by_max_word",
                            "search_analyzer": "by_max_word"
                        }
  					}
  				},
                "repeatid":{
                    "type": "keyword"
                },
  				"bid":{
  					"type":"integer"
  				},
  				"brand_name":{
  					"type":"text",
  					"analyzer":"ik_smart",
                    "search_analyzer": "ik_smart",
  					"fields":{
  						"raw":{
  							"type":"keyword"
  						},
                        "synonym":{
                            "type":"text",
                            "analyzer":"by_smart",
                            "search_analyzer": "by_smart"
                        }
					}
  				},
  				"brand_name_cn":{
  					"type":"text",
  			  		"analyzer": "ik_smart",
                    "search_analyzer": "ik_smart",
                    "fields":{
                        "synonym":{
                            "type" : "text",
                            "analyzer":"by_smart",
                            "search_analyzer":"by_smart"
                        }
                    }
  				},
  				"brand_alias_name":{
  					"type":"text",
  			  		"analyzer": "ik_smart",
                    "search_analyzer": "ik_smart",
                    "fields":{
                        "synonym":{
                            "type" : "text",
                            "analyzer":"by_smart",
                            "search_analyzer":"by_smart"
                        }
                    }
  				},
  				"brand_class":{
  					"type":"keyword"
  				},
  				"acceptance":{
  					"type":"integer"
  				},
  				"region":{
  					"type": "text",
  					"analyzer":"ik_smart"
  				},
				"cid":{
 					"type":"integer"
  				},
 				"group_id":{
					"type":"integer"
  				},
  				"category_name":{
					"type":"text",
					"analyzer": "ik_smart",
					"fields":{
  						"raw":{
  							"type":"keyword"
  				  		},
  				  		"py":{
  				    		"type":"text",
  				    		"analyzer": "pinyin"
  				  		},
                        "synonym":{
                            "type" : "text",
                            "analyzer":"by_smart",
                            "search_analyzer":"by_smart"
                        }
  			  		}
  				},
  				"seller_id":{
  		    		"type":"integer"
  				},
  				"sex":{
  					"type": "text",
  					"analyzer":"ik_smart"
  				},
  				"cooperate_type":{
  		    		"type":"keyword"
  				},
  				"condition_level":{
  		    		"type":"keyword"
  				},
  				"condition_desc":{
  					"type":"text",
  			 		"analyzer": "ik_smart",
                    "search_analyzer": "ik_smart",
                    "fields":{
                        "synonym":{
                            "type" : "text",
                            "analyzer":"by_smart",
                            "search_analyzer":"by_smart"
                        }
                    }
  				},
  				"long_desc":{
  					"type":"text",
  					"analyzer": "ik_smart",
                    "search_analyzer": "ik_smart",
                    "fields":{
                        "synonym":{
                            "type" : "text",
                            "analyzer":"by_smart",
                            "search_analyzer":"by_smart"
                        }
                    }
  				},
  				"size":{
  					"type":"keyword"
  				},
  				"treatment":{
  					"type":"keyword"
  				},
  				"status":{
  					"type":"keyword"
  				},
  				"visible":{
  					"type":"byte"
  				},
  				"in_offline_shop":{
  					"type":"byte"
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
  				"blackcard_discount":{
  					"type":"integer"
  				},
  				"attribute":{
  					"type":"text",
  					"analyzer": "ik_smart",
                    "search_analyzer": "ik_smart",
                    "fields":{
                        "synonym":{
                            "type" : "text",
                            "analyzer":"by_smart",
                            "search_analyzer":"by_smart"
                        }
                    }
  				},
                "customer_type":{
                    "type":"keyword"
                },
                "onsale_time":{
                    "type":"integer"
                },
                "discount_price":{
                    "type":"scaled_float",
                    "scaling_factor": 100
                },
                "discount_rate":{
                    "type":"scaled_float",
                    "scaling_factor": 100
                },
                "category_score":{
                    "type":"integer"
                },
                "category_sort_score":{
                    "type":"double"
                },
                "brand_score":{
                    "type":"integer"
                }
		  	}
		}
	}
}

def output_info(info):
    print(info)
    sys.exit()


if __name__ == '__main__':
    # 获取目标索引
    targetIndex = ''
    currentIndex = ''
    try:
        es = Elasticsearch([es_url], http_auth=(es_username, es_passwd), port=9200)
        if es.indices.exists(aliasIndex):
            currentIndex = list(es.indices.get(aliasIndex).keys())[0]
            if currentIndex == index1:
                targetIndex = index2
            elif currentIndex == index2:
                targetIndex = index1
            else:
                output_info("get index=%s error, index=%s" % (aliasIndex, currentIndex))
        else:
            currentIndex = index1
            targetIndex = index2

        # 删除索引
        if es.indices.exists(targetIndex):
            re = es.indices.delete(index=targetIndex)
            result = json.dumps(re, indent=2)
    except Exception as e:
        output_info("get index=%s error, error=%s" % (aliasIndex, str(e)))

    # 创建索引
    try:
        es.indices.create(index=targetIndex, ignore=400, body=mapping)
        # test es.indices.create(index=targetIndex,ignore=400)
    except  Exception as e:
        output_info("es create product mapping error, index=%s, error=%s" % (targetIndex, str(e)))

    # 获取mysql 数据 导入 elastic
    db = MySQLdb.connect(mysqlhost, mysqlusername,  mysqlpasswd, db, charset='utf8')
    cursor = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    cursor.execute("set names utf8")
    id = 0
    while (True) :
        sql ="SELECT p.id, p.name,p.user_id AS seller_id,(case p.customer_type when 'woman' then '女' when 'man' then '男' end) AS sex,p.cooperate_type,p.condition_level,p.condition_desc,p.onsale_time,p.customer_type,p.discount_price,p.discount_rate,p.long_desc,p.size,p.treatment,p.status,p.visible,p.in_offline_shop,p.original_price,p.sale_price,p.purchase_price,p.blackcard_discount,p.provider,b.id AS bid,b.name AS brand_name,b.name_zh AS brand_name_cn,b.alias_name AS brand_alias_name,b.brand_class,b.acceptance,b.region,c.id AS cid,c.group_id,c.name AS category_name,GROUP_CONCAT(distinct  atv.value_text) as attribute,p.last_update as last_update FROM t_product as p join (select id from t_product where id > %d  and status IN ('sold', 'onsale') and visible = 1 and  in_offline_shop =0 ORDER BY id ASC limit 2000) as p1 using(id),t_brand b, t_category c,t_product_attr_value atv WHERE p.brand_id = b.id AND p.category_id = c.id  AND p.id = atv.product_id group by atv.product_id order by p.id asc " % int(id)
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) == 0:
                break

            # begin: get product sort score
            pidList = []
            for row in results:
                pidList.append(str(row['id']))
            # get socre from DB
            sortDic = ss.GetProductSortScoreByDB(cursor, pidList)
            # get feature score from  thrift
            for row in results:
                pid = int(row['id'])
                try:
                    sortDic[pid].category_sort_score = ss.GetFeatureScore(rpc.client, row)
                except Exception as e:
                    msg = traceback.format_exc()
                    print(msg)
                    break
            print('update sort score len:%d' % len(sortDic))
            # end

            repeatid = ''
            package = []
            for row in results:
                id = int(row['id'])
                obj = ss.ProductSort(id)
                if id in sortDic.keys():
                    obj = sortDic[id]
                if int(row['seller_id']) == 758050:
                    repeatid = str(row['id'])
                else:
                    repeatid = "%s_%s" % (str(row['seller_id']), str(row['bid']))
                esrow = {
                    "id": id,
                    "name": "%s%s" % (str(row['name']), str(row['sex'])),
                    "bid": int(row['bid']),
                    "cid": int(row['cid']),
                    "discount_price": int(row['discount_price']),
                    "status": str(row['status']),
                    "visible": int(row['visible']),
                    "discount_rate": float(row['discount_rate']),
                    "blackcard_discount": int(row['blackcard_discount']),
                    "onsale_time": int(row['onsale_time']),
                    "customer_type": str(row['customer_type']),
                    "repeatid": repeatid,
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
                    "condition_level": str(row['condition_level']),
                    "brand_name_cn": str(row['brand_name_cn']),
                    "group_id": int(row['group_id']),
                    "brand_class": str(row['brand_class']),
                    "category_score": obj.category,
                    "category_sort_score": obj.category_sort_score,
                    "brand_score": obj.brand
                }
                package.append(esrow)
            # 批量插入es
            actions = [
                {
                    '_index': targetIndex,  # index
                    '_type': "product",  # type
                    '_id':d['id'],
                    '_source': d
                }
                for d in package
            ]

            elasticsearch.helpers.bulk( es, actions)
            id = results[len(results)-1]['id']
            print(id)
            time.sleep(1)
        except Exception as e:
            output_info("Error:unable to fetch data, error=%s" % (str(e),))
            break

    r = redis.StrictRedis(host=redishost, port=6379, db=3,  password = redispassword)
    # test r = redis.StrictRedis(host=redishost, port=6379, db=2)
    sql2 = "select product_update_time from t_product  order by product_update_time desc limit 1"
    cursor.execute(sql2)
    result = cursor.fetchone()
    if len(result) > 0:
        r.set('last_aplum_timestamp', str(result['product_update_time']))

    # 切换index别名，原子操作
    if es.indices.exists(aliasIndex):
        doc = {
            "actions": [
                {"remove": {"index": currentIndex, "alias": aliasIndex}},
                {"add":    {"index": targetIndex, "alias": aliasIndex}}
            ]
        }
        res = es.indices.update_aliases(body=doc)
        if res['acknowledged'] != True:
            output_info("es put alias name error, index=%s, error=%s" % (aliasIndex, str(e)))
    else:
        res = es.indices.put_alias(index=targetIndex, name=aliasIndex)
        if res['acknowledged'] != True:
            output_info("es put alias name error, index=%s, error=%s" % (aliasIndex, str(e)))

    print("es put alias,index=%s, name=%s" % (targetIndex, aliasIndex))

    cursor.close()     # 断开游标
    db.close()    # 断开数据库
    rpc.close()  # 断开thrift
    print(targetIndex)
    print("current time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print('Ended Polling: %s' % tic())
    print("success")
    sys.exit()
