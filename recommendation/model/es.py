from elasticsearch import Elasticsearch
from collections import defaultdict
import lib.day as day

ES = Elasticsearch(["es-cn-0pp0vm2ju000a003g.elasticsearch.aliyuncs.com"],http_auth=('elastic', 'NHRHpqeFn8'),port=9200)

def _search(doc):
   #print doc
   p = dict()
   p['request_timeout'] = 30
   return ES.search(index='aplum_ctr*', doc_type='product', body=doc, params=p)

def batchEs(values,everycount):
    vLen = len(values)
    times = int(vLen/everycount)+1
    for t in range(times):
        start = t*everycount
        end = (t+1)*everycount
        if end>= vLen:
            end = vLen

        yield values[start:end]

def _buildProductDoc(Days,pidList, size):
  doc = {
      "size":0,
      "query": {
          "bool": {
              "filter":[{
                  "range":{
                      "Createtime": {
                          "gt": day.startUnixtime(Days)
                          #"lte": day.endUnixtime()
                      }
                    }
                  },
                  {"terms":{"ItemActions": pidList}}
              ]
          }
      }, 
      "aggs": {
          "idgroup": {
              "terms": {
                  "field": "ItemActions",
                  "size": size
              }
          }
      }   
  }
  return doc

def _parseProductEs(dic, data):
    buckets = data['aggregations']['idgroup']['buckets']
    for b in buckets:
       dic[str(b['key'])] = int(b['doc_count'])
    
_everycount=5000
def loadProductView(Days, pidList):
    dic = {}
    for batch in batchEs(pidList, _everycount):
        results = _search(_buildProductDoc(Days, batch, _everycount))
        _parseProductEs(dic, results)
    
    return dic

def buildUserViewDoc(days,uid):
  doc = {
    "size": 10000, 
    "query": {
    "bool": {
         "filter":[
             {"term":{"X-Pd-Identify": uid}},
             {"range": {
               "Createtime": {"gt": day.startUnixtime(days)}
               }
             }
         ]
    }
    },
    "sort":{
      "Createtime":{"order":"desc"}
    }
  }
  return doc

def loadUserView(days,uid):
    pids = []
    results = _search(buildUserViewDoc(days,uid))
    data = results['hits']['hits']
    #print(day.startUnixtime(days))
    for d in data:
        id = d['_source']['ItemActions']
        #print (type(id)) int
        pids.append(int(id))
        
    return pids
    
MaxUserSize=100
def buildBatchView(days, users):
  doc = {
    "query": {
      "bool": { 
        "filter": [
          {"terms":{"X-Pd-Identify": users}},
          {"range":{"Createtime":{"gt": day.startUnixtime(days)}}}
        ]
      }
    },
    "aggs": {
      "user": {
        "terms": {
          "field": "X-Pd-Identify.keyword",
          "size": MaxUserSize
          },
          "aggs": {
            "pid": {
              "terms": {
               "field": "ItemActions",
               "size": 1000
               }
            }
          }
        }
      }
    }
  return doc

def loadBatchView(days, users):
   users = [int(uid) for uid in users]
   showDic = defaultdict(list)
   for batch in batchEs(users, MaxUserSize):
       result = _search(buildBatchView(days,batch))
       buckets = result['aggregations']['user']['buckets']
       #print ('uid bucket len = %d'%len(buckets)) #存在有点击，没曝光的用户
       for b in buckets:
           uid = b['key'] # uid is string 
           pid = b['pid']['buckets']
           #print ('pid bucket len = %d'%len(pid))
           for p in pid:
               showDic[uid].append(str(p['key']))
           
   return showDic

