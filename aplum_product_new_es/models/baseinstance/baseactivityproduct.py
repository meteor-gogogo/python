#!/usr/bin/env python
# coding=utf-8

class BaseActivityProduct(object):

    def __init__(self):
        self.aliasIndex="aplum_activity"
        self.index1= "aplum_activity_a1"
        self.index2= "aplum_activity_a2"

    def getMapping(self):
        mapping = {
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
                                "py":{
                                    "type":"text",
                                    "analyzer": "pinyin",
                                    "search_analyzer": "ik_smart"
                                },
                                "synonym":{
                                    "type": "text",
                                    "analyzer": "by_smart",
                                    "search_analyzer": "by_smart"
                                }
                            }
                        },
                        "seller_id":{
                            "type":"integer"
                        },
                        "customer_type":{
                            "type":"keyword"
                        },
                        "bid":{
                            "type":"integer"
                        },
                        "dispatch_type":{
                            "type":"keyword"
                        },
                        "cid":{
                            "type":"integer"
                        },
                        "discount_price":{
                            "type":"integer"
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
                        "discount_rate":{
                            "type":"scaled_float",
                            "scaling_factor": 100
                        },
                        "condition_level":{
                            "type":"keyword"
                        },
                        "status":{
                            "type":"keyword"
                        },
                        "size":{
                            "type":"keyword"
                        },
                        "visible":{
                            "type":"short"
                        },
                        "in_offline_shop":{
                            "type":"integer"
                        },
                        "onsale_time":{
                            "type":"integer"
                        },
                        "blackcard_discount":{
                            "type":"short"
                        },
                        "cooperate_type":{
                            "type":"keyword"
                        },
                        "activityinfo": {
                            "type": "nested",
                            "properties": {
                                "id":    { "type": "integer"  },
                                "order": { "type": "double"  },
                                "ctr":   { "type": "double"  }
                            }
                        }
                    }
                }
            }
        }
        return mapping
