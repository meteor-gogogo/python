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

    def get_total_show_num(self, start_time, end_time, sptype):
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
        if sptype != "total":
            must_doc = {
                "term": {
                    "Src_page_type.keyword": sptype
                }
            }
            message.append(must_doc)

        es_doc = {
            "size": 0,
            "query": {
                "bool": {
                    "must": message,
                    "must_not": [
                        {
                            "term": {
                                "Src_page_type.keyword": {
                                    "value": ""
                                }
                            }
                        }
                    ],
                    "filter": {
                        "range": {
                            "Createtime": {
                                "gte": start_time,
                                "lte": end_time
                            }
                        }
                    }
                }
            }
        }
        searched = self.es.search(self.index, self.doctype, es_doc)
        hit_num = searched['hits']['total']
        return hit_num

