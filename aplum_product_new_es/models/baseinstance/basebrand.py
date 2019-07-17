#!/usr/bin/env python
# coding=utf-8

class BaseBrand(object):

    def __init__(self):
        self.aliasIndex="aplum_brand"
        self.index1= "aplum_brand_1"
        self.index2= "aplum_brand_2"

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
            "brand": {
              "include_in_all": "false",
              "dynamic": "true",
              "properties": {
                "id": {
                  "type": "integer"
                },
                "brand_name": {
                  "type": "text",
                  "analyzer": "ik_smart",
                  "search_analyzer": "ik_smart",
                  "fields": {
                    "raw": {
                      "type": "keyword"
                    },
                    "synonym": {
                      "type": "text",
                      "analyzer": "by_smart",
                      "search_analyzer": "by_smart"
                    }
                  }
                },
                "bid": {
                  "type": "integer"
                },
                "brand_alias_name": {
                  "type": "text",
                  "analyzer": "ik_smart",
                  "search_analyzer": "ik_smart",
                  "fields": {
                    "synonym": {
                      "type": "text",
                      "analyzer": "by_smart",
                      "search_analyzer": "by_smart"
                    }
                  }
                },
                "brand_sug":{
                    "type": "completion",
                    "analyzer":"by_smart",
                    "search_analyzer": "by_smart"
                },
                "brand_name_sug":{
                    "type": "completion",
                    "analyzer":"by_smart",
                    "search_analyzer": "by_smart"
                }
              }
            }
          }
        }
        return mapping
