#!/usr/bin/env python
# coding=utf-8

class BaseProduct(object):

    def __init__(self):
        self.aliasIndex="aplum_product"
        self.index1= "aplum_a1"
        self.index2= "aplum_a2"

    def getMapping(self):
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
                        "filter": ["by_tfr","by_sfr"],
                        "char_filter": ["by_cfr"]
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
                                "raw":{
                                    "type":"keyword"
                                },
                                "py":{
                                    "type":"text",
                                    "analyzer": "pinyin",
                                    "search_analyzer": "ik_smart"
                                },
                                "synonym":{
                                    "type": "text",
                                    "analyzer": "by_smart",
                                    "search_analyzer": "by_smart"
                                },
                                "ikmax":{
                                    "type":"text",
                                    "analyzer": "ik_max_word",
                                    "search_analyzer": "ik_max_word"
                                },
                                "sy_ikmax":{
                                    "type": "text",
                                    "analyzer": "by_max_word",
                                    "search_analyzer": "by_max_word"
                                }
                            }
                        },
                        "repeatid":{
                            "type": "keyword"
                        },
                        "bid":{
                            "type":"integer"
                        },
                        "brand_name":{
                            "type":"text",
                            "analyzer":"ik_smart",
                            "search_analyzer": "ik_smart",
                            "fields":{
                                "raw":{
                                    "type":"keyword"
                                },
                                "synonym":{
                                    "type":"text",
                                    "analyzer":"by_smart",
                                    "search_analyzer": "by_smart"
                                }
                            }
                        },
                        "brand_name_cn":{
                            "type":"text",
                            "analyzer": "ik_smart",
                            "search_analyzer": "ik_smart",
                            "fields":{
                                "synonym":{
                                    "type" : "text",
                                    "analyzer":"by_smart",
                                    "search_analyzer":"by_smart"
                                }
                            }
                        },
                        "brand_alias_name":{
                            "type":"text",
                            "analyzer": "ik_smart",
                            "search_analyzer": "ik_smart",
                            "fields":{
                                "synonym":{
                                    "type" : "text",
                                    "analyzer":"by_smart",
                                    "search_analyzer":"by_smart"
                                }
                            }
                        },
                        "brand_class":{
                            "type":"keyword"
                        },
                        "acceptance":{
                            "type":"integer"
                        },
                        "region":{
                            "type": "text",
                            "analyzer":"ik_smart"
                        },
                        "cid":{
                            "type":"integer"
                        },
                        "group_id":{
                            "type":"integer"
                        },
                        "category_name":{
                            "type":"text",
                            "analyzer": "ik_smart",
                            "fields":{
                                "raw":{
                                    "type":"keyword"
                                },
                                "py":{
                                    "type":"text",
                                    "analyzer": "pinyin"
                                },
                                "synonym":{
                                    "type" : "text",
                                    "analyzer":"by_smart",
                                    "search_analyzer":"by_smart"
                                }
                            }
                        },
                        "seller_id":{
                            "type":"integer"
                        },
                        "sex":{
                            "type": "text",
                            "analyzer":"ik_smart"
                        },
                        "cooperate_type":{
                            "type":"keyword"
                        },
                        "condition_level":{
                            "type":"keyword"
                        },
                        "condition_desc":{
                            "type":"text",
                            "analyzer": "ik_smart",
                            "search_analyzer": "ik_smart",
                            "fields":{
                                "synonym":{
                                    "type" : "text",
                                    "analyzer":"by_smart",
                                    "search_analyzer":"by_smart"
                                }
                            }
                        },
                        "long_desc":{
                            "type":"text",
                            "analyzer": "ik_smart",
                            "search_analyzer": "ik_smart",
                            "fields":{
                                "synonym":{
                                    "type" : "text",
                                    "analyzer":"by_smart",
                                    "search_analyzer":"by_smart"
                                }
                            }
                        },
                        "size":{
                            "type":"keyword"
                        },
                        "treatment":{
                            "type":"keyword"
                        },
                        "status":{
                            "type":"keyword"
                        },
                        "visible":{
                            "type":"byte"
                        },
                        "in_offline_shop":{
                            "type":"byte"
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
                        "blackcard_discount":{
                            "type":"integer"
                        },
                        "attribute":{
                            "type":"text",
                            "analyzer": "ik_smart",
                            "search_analyzer": "ik_smart",
                            "fields":{
                                "synonym":{
                                    "type" : "text",
                                    "analyzer":"by_smart",
                                    "search_analyzer":"by_smart"
                                }
                            }
                        },
                        "live":{
                            "type":"text",
                            "analyzer": "ik_smart",
                            "search_analyzer": "ik_smart"
                        },
                        "customer_type":{
                            "type":"keyword"
                        },
                        "onsale_time":{
                            "type":"integer"
                        },
                        "discount_price":{
                            "type":"scaled_float",
                            "scaling_factor": 100
                        },
                        "discount_rate":{
                            "type":"scaled_float",
                            "scaling_factor": 100
                        },
                        "category_score":{
                            "type":"integer"
                        },
                        "category_sort_score":{
                            "type":"double"
                        },
                        "brand_score":{
                            "type":"integer"
                        }
                    }
                }
            }
        }
        return mapping
