#!/usr/bin/env python
# coding=utf-8
from elasticsearch import Elasticsearch

class EsOperator(object):
    def __init__(self, host, user, password, port):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.index = "aplum_ctr*"
        self.doctype = "product"
        self.es = Elasticsearch([self.host], http_auth=(self.user, self.password), port=self.port)

    def getTotalShowNum(self, starttime, endtime, sptype):
        message = [
            {
                "terms": {
                    "X-Pd-Os.keyword": [
                        "ios_app",
                        "android_app",
                        "ios_app_native",
                        "android_app_native"
                    ]
                }
            }
        ]
        if sptype != "total" :
            mustdoc = {
                "term": {
                    "Src_page_type.keyword": sptype
                }
            }
            message.append(mustdoc)

        esdoc = {
            "size": 0,
            "query": {
                "bool": {
                    "must": message,
                    "must_not": [
                        {"term": {
                            "Src_page_type.keyword": ""
                        }}
                    ],
                    "filter": {
                        "range": {
                            "Createtime": {
                                "gte": starttime,
                                "lt": endtime
                            }
                        }
                    }
                }
            }
        }
        searched = self.es.search(index=self.index, doc_type=self.doctype, body=esdoc)
        hitnum = searched['hits']['total']
        return hitnum

