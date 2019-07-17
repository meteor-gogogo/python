#!/usr/bin/env python
# coding=utf-8
import MySQLdb
import sys
from elasticsearch import Elasticsearch
import elasticsearch.helpers
import json
import time
start = time.time()
tic = lambda: 'at %1.1f seconds' % (time.time() - start)

_type = 'category'     #修改为类型名
es_url = 'http://es-cn-0pp0gwsfu0005h3gr.elasticsearch.aliyuncs.com:9200'  #修改为elasticsearch服务器
es_username = 'elastic'
es_passwd = 'Plum-ela'

mysqlhost = 'rr-2ze7824567d8dgu1y.mysql.rds.aliyuncs.com'
mysqlusername = 'plumdb'
mysqlpasswd = 'plumdb@2018'
db='aplum'

index1= "aplum_category_1"
index2= "aplum_category_2"
aliasIndex = "aplum_category"

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
    "category": {
      "include_in_all": "false",
      "dynamic": "true",
      "properties": {
        "id": {
          "type": "integer"
        },
        "category_name": {
          "type": "text",
          "analyzer": "ik_smart",
          "search_analyzer": "ik_smart",
          "fields": {
            "raw": {
              "type": "keyword"
            },
            "synonym": {
              "type": "text",
              "analyzer": "by_smart",
              "search_analyzer": "by_smart"
            }
          }
        },
        "cid": {
          "type": "integer"
        }
      }
    }
  }
}

def output_info(info):
    print(info)
    sys.exit()

if __name__ == '__main__':
    #删除索引
    targetIndex = ''
    try:
        es = Elasticsearch([es_url],http_auth=(es_username, es_passwd),port=9200)
        if es.indices.exists(aliasIndex):
            indexname =  list(es.indices.get(aliasIndex).keys())[0]
            if indexname == index1:
                targetIndex = index2
            else:
                targetIndex = index1
        else:
            targetIndex = index1
        re = es.indices.delete(index=targetIndex, ignore=[400, 404])
        result = json.dumps(re, indent=2)
    except Exception as e:
        output_info("es delete product index error, index=%s, error=%s" % (aliasIndex,str(e)))
    #创建索引
    try:
        result=es.indices.create(index=targetIndex,ignore=400, body=mapping)
        if es.indices.exists(aliasIndex):
            indexname =  list(es.indices.get(aliasIndex).keys())[0]
            if indexname == index1:
                res = es.indices.delete_alias(index=index1, name=aliasIndex)
                if res['acknowledged'] == True:
                    res = es.indices.put_alias(index=index2,name=aliasIndex)
                    if res['acknowledged'] != True:
                        output_info("es put alias name error, index=%s, error=%s" % (aliasIndex,str(e)))
                else:
                    output_info("es delete alias name error, index=%s, error=%s" % (aliasIndex,str(e)))
            else:
                res = es.indices.delete_alias(index=index2, name=aliasIndex)
                if res['acknowledged'] != True:
                    output_info("es delete alias name error, index=%s, error=%s" % (aliasIndex,str(e)))
                res = es.indices.put_alias(index=index1,name=aliasIndex)
                if res['acknowledged'] != True:
                    output_info("es put alias name error, index=%s, error=%s" % (aliasIndex,str(e)))
        else:
            res = es.indices.put_alias(index=index1,name=aliasIndex)
    except  Exception as e:
        output_info("es create product mapping error, index=%s, error=%s" % (aliasIndex, str(e)))

    #获取mysql 数据 导入 elastic
    db = MySQLdb.connect(mysqlhost, mysqlusername,  mysqlpasswd, db, charset='utf8')
    cursor = db.cursor(cursorclass = MySQLdb.cursors.DictCursor)
    cursor.execute("set names utf8")
    id = 0
    while (True) :
        sql ="SELECT id, name FROM t_category b  WHERE id > %d  order by id asc limit 1000" % int(id)
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) == 0 :
                break
            package = []
            for row in results:
                esrow = {
                    "cid":int(row['id']),
                    "category_name" : str(row['name']),
                }
                package.append( esrow )
            #批量插入es
            actions = [
                {
                    '_index': aliasIndex,  #index
                    '_type': _type,  #type
                    '_id':d['cid'],
                    '_source': d
                }
                for d in package
            ]

            elasticsearch.helpers.bulk( es, actions)
            id = results[len(results)-1]['id']
            print(id)
            time.sleep(2)
        except  Exception as e:
            output_info("Error:unable to fetch data, error=%s" % (str(e),))
            break

    cursor.close()     #断开游标
    db.close()    #断开数据库
    print(targetIndex)
    print('Ended Polling: %s' % tic())
    print("success")
