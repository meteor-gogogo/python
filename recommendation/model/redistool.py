# coding=utf-8

import redis

conn = redis.StrictRedis(host='r-2zec5074fca1ade4.redis.rds.aliyuncs.com', port=6379, db=1, password="dL5pr5RkJBQEuqj")

def setRecommend(data,prefix,expire):
    batch = 1000
    mark = 0
    pipe = conn.pipeline()
    for key,products in data.items():
        pipe.set(prefix + str(key), ','.join(products), ex=expire)
        mark +=1
        if mark == batch:
            pipe.execute()
            mark = 0
    if mark >0:
        pipe.execute()

