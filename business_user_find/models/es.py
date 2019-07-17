#!/usr/bin/env python
# coding=utf-8

from models import mysql
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta, date
import time


class EsOperator(object):
    def __init__(self, host, user, password, port, filename, days):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.filename = filename
        self.beforedays = -days
        self.index = "aplum_ctr*"
        self.doctype = "product"
        self.es = Elasticsearch([self.host], http_auth=(self.user, self.password), port=self.port)

    def get_user_action_count(self, uid_list):

        print(len(uid_list))
        if len(uid_list) % 100 == 0:
            batch = int(len(uid_list) / 100)
        else:
            batch = int(len(uid_list) / 100 + 1)
        print(batch)

        lines = list()
        middle = dict()

        for i in range(batch):

            if i == batch - 1:
                start_num = i * 100
                end_num = len(uid_list)
            else:
                start_num = i * 100
                end_num = (i + 1) * 100

            uids = uid_list[start_num:end_num]

            today = date.today()
            starttime = int(time.mktime(time.strptime(str(today + timedelta(days=self.beforedays)), "%Y-%m-%d")))
            endtime = int(time.mktime(time.strptime(str(today), "%Y-%m-%d")))
            # print(starttime, endtime)
            message = {
                "size": 0,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "Src_page_id.keyword": {
                                        "value": "4"
                                    }
                                }
                            },
                            {
                                "term": {
                                    "Src_page_type.keyword": {
                                        "value": "activity"
                                    }
                                }
                            },
                            {
                                "terms": {
                                    "X-Pd-Identify.keyword": uids
                                }
                            }
                        ],
                        "must_not": [
                            {
                                "term": {
                                    "X-Pd-Identify.keyword": {
                                        "value": ""
                                    }
                                }
                            }
                        ],
                        "filter": {
                            "range": {
                                "Createtime": {
                                    "gte": starttime,
                                    "lte": endtime
                                }
                            }
                        }
                    }
                },
                "aggs": {
                    "idcount": {
                        "terms": {
                            "field": "X-Pd-Identify.keyword",
                            "size": 100,
                            "order": {
                                "_count": "desc"
                            }
                        },
                        "aggs": {
                            "DEV": {
                                "cardinality": {
                                    "field": "ItemActions"
                                }
                            }
                        }
                    }
                }
            }

            # {
            #           "key": "49965903",
            #           "doc_count": 3561,
            #           "DEV": {
            #             "value": 1314
            #           }
            #         }
            searched = self.es.search(index=self.index, doc_type=self.doctype, body=message)
            buckets = searched["aggregations"]["idcount"]["buckets"]

            for row in buckets:
                uid = row["key"]
                count = row["DEV"]["value"]
                middle.update({uid: count})
                # print(uid)
                # print(count)
                # print(len(middle))

            # sorted(middle.items(), key=lambda x: x[1], reverse=True)

        for key in middle.keys():
            line = str(key) + "," + str(middle[key]) + "\n"
            lines.append(line)

        with open(self.filename, "w") as file:
            file.write("uid,count\n")
            file.writelines(lines)
            file.close()
        return middle
