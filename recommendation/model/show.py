# coding=utf-8
import model.es as e
import lib.day as day
from collections import defaultdict
import time

def buildBatchShow(days, userList, minCount):
  doc = {
    "query": {
      "bool": { 
        "filter": [
          {"terms":{"X-Pd-Identify": userList}},
          {"range":{"Createtime":{"gt": day.startUnixtime(days)}}}
        ]
      }
    },
    "aggs": {
      "user": {
        "terms": {
          "field": "X-Pd-Identify.keyword",
          "size": MaxUserShowSize
          },
          "aggs": {
            "pid": {
              "terms": {
               "field": "ItemActions",
               "size": 500
               },
               "aggs":{
                 "count_filter":{
                   "bucket_selector":{
                     "buckets_path":{"idCount":"_count"},
                     "script":{"source":"params.idCount>=%d"%minCount}
                   }
               }
            }
          }
        }
      }
    }
  }
  return doc

MaxUserShowSize=200
def loadBatchShow(days, userList, minCount):
   print('load user show,len=%d,minCount=%d'%(len(userList),minCount))
   start = time.time()
   userList = [int(uid) for uid in userList if uid.isdigit()]
   showDic = defaultdict(dict)
   for batch in e.batchEs(userList, MaxUserShowSize):
       result = e._search(buildBatchShow(days, batch, minCount))
       buckets = result['aggregations']['user']['buckets']
       #print ('uid bucket len = %d'%len(buckets)) #存在有点击，没曝光的用户
       for b in buckets:
           uid = b['key'] # uid is string 
           pids = b['pid']['buckets']
           #print ('pid bucket len = %d'%len(pid))
           for p in pids:
               id = str(p['key'])
               count = int(p['doc_count'])
               showDic[uid][id] = count
           
   print('calc userShow cost time: %f' % (time.time() - start))
   return showDic


if __name__ == '__main__':
    print('test')
    #uid = '4044301'
    #res = show.loadBatchShow(7,[uid], 50)
    #for k,v in res[uid].items():
    #    print (k,v)
    #return
    #pids = [['2623573',0.8],['2623567',0.82],['2623564',0.76]]
    #print(pids)
    #print(rank.colorAndMateial('1000415',pids))

