#!/usr/bin/env python
# coding=utf-8
from elasticsearch import Elasticsearch
import elasticsearch.helpers
from utils import output
from .baseinstance import basecategory

class CategoryEsOperator(object):
    def __init__(self, host, user, password, port):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.es = Elasticsearch([self.host], http_auth=(self.user, self.password), port=self.port)
        self.cagetoryObj = basecategory.BaseCategory()
        self.aliasIndex=self.cagetoryObj.aliasIndex
        self.index1= self.cagetoryObj.index1
        self.index2= self.cagetoryObj.index2

    def getMapping(self):
        mapping = self.cagetoryObj.getMapping()
        return mapping

    def operatorindex(self):
        targetIndex = ''
        currentIndex = ''
        try:
            if self.es.indices.exists(self.aliasIndex):
                currentIndex =  list(self.es.indices.get(self.aliasIndex).keys())[0]
                if currentIndex == self.index1 :
                    targetIndex = self.index2
                elif currentIndex == self.index2 :
                    targetIndex = self.index1
                else:
                    output.output_info("get index=%s error, index=%s"%(self.aliasIndex, currentIndex))
            else:
                currentIndex = self.index1
                targetIndex = self.index2

            mapping = self.getMapping()
            #删除索引,再创建
            if self.es.indices.exists(targetIndex):
                res = self.es.indices.delete(index=targetIndex)
                print("delete", res)
                if not res['acknowledged']:
                    output.output_info("delete index error")
            res = self.es.indices.create(index=targetIndex,ignore=400, body=mapping)
            print("create :", res)
            if not res['acknowledged']:
                output.output_info("create Index error")
            return currentIndex, targetIndex
        except Exception as e:
            output.output_info("get index=%s error, error=%s" % (self.aliasIndex,str(e)))

    def bulkInsertData(self, data):
        try:
            return elasticsearch.helpers.bulk( self.es, data)
        except Exception as e:
            output.output_info("Error:unable to fetch data, error=%s" % (str(e),))

    def changeAliasName(self, currentIndex, targetIndex):
        if self.es.indices.exists(self.aliasIndex):
            doc = {
                "actions" : [
                    { "remove": { "index": currentIndex, "alias": self.aliasIndex}},
                    { "add":    { "index": targetIndex, "alias": self.aliasIndex}}
                ]
            }
            res = self.es.indices.update_aliases(body=doc)
            print(res, doc)
            if res['acknowledged'] != True:
                output.output_info("es put alias name error, index=%s" % (self.aliasIndex,))
        else:
            res = self.es.indices.put_alias(index=targetIndex,name=self.aliasIndex)
            print(res)
            if res['acknowledged'] != True:
                output.output_info("es put alias name error, index=%s" % (self.aliasIndex,))
        return True
