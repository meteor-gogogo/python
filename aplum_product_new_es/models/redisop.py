#!/usr/bin/env python
# coding=utf-8
import redis

class RedisOperator(object):

    def __init__(self, host, port, password, dbnum):
        self.pool = redis.ConnectionPool(host=host, port=port, db=dbnum, password=password, encoding='utf-8', decode_responses=True)
        self.client = redis.StrictRedis(connection_pool=self.pool)

    def outQueue(self, key):
        res = self.client.rpop(key)
        return res

    def setValue(self, key, value):
        res = self.client.set(key, value)
        return res

    def getValue(self, key):
        res = self.client.get(key)
        return res
