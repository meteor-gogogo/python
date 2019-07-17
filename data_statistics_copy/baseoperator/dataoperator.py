#!/usr/bin/env python
# coding=utf-8
from datetime import timedelta, date
from base import baselist
from base import total
from base import activity
from base import brand
from base import category
from base import index
from base import other
from base import recommend
from base import search
from models import es
from models import sensors
import time


class DataOperator(object):
    def __init__(self, list_type, config):
        today = date.today()
        self.date = today + timedelta(days=-1)
        self.last_date = today + timedelta(days=-8)
        self.start_time = int(time.mktime(time.strptime(str(self.date), '%Y-%m-%d')))
        self.end_time = int(time.mktime(time.strptime(str(today), '%Y-%m-%d')))
        self.list_type = list_type
        self.config = config
        self.es_operator = self.init_es()
        self.sensors_operator = self.init_sensors_url()

    def init_es(self):
        es_host = self.config.get('ELASTICSEARCH', 'host')
        es_user = self.config.get('ELASTICSEARCH', 'user')
        es_password = self.config.get('ELASTICSEARCH', 'password')
        es_port = self.config.get('ELASTICSEARCH', 'port')
        return es.EsOperator(es_host, es_user, es_password, es_port)

    def init_sensors_url(self):
        sensors_url = self.config.get('SENSORS', 'url')
        sensors_token = self.config.get('SENSORS', 'token')
        url = '%s?token=%s' % (sensors_url, sensors_token)
        return sensors.SensorsOperator(url)

    def get_data_by_type(self, list_type):
        print(list_type, baselist.BaseList.__subclasses__())
        for cls in baselist.BaseList.__subclasses__():
            print('===', list_type, cls, super(baselist.BaseList, cls).is_registrar_for(list_type))
            if super(baselist.BaseList, cls).is_registrar_for(list_type):
                return cls(self.list_type, self.date, self.last_date, self.start_time, self.end_time,
                           self.sensors_operator, self.es_operator).get_data_dict()

    def get_all_data(self):
        all_obj = total.TotalList(self.list_type, self.date, self.last_date, self.start_time, self.end_time,
                                  self.sensors_operator, self.es_operator)
        total_ctr_dict = all_obj.get_data_dict()
        return total_ctr_dict

    def get_category_data(self):
        category_obj = category.CategoryList(self.list_type, self.date, self.last_date, self.start_time, self.end_time,
                                             self.sensors_operator, self.es_operator)
        category_ctr_dict = category_obj.get_data_dict()
        return category_ctr_dict

    def get_brand_data(self):
        brand_obj = brand.BrandList(self.list_type, self.date, self.last_date, self.start_time, self.end_time,
                                    self.sensors_operator, self.es_operator)
        brand_ctr_dict = brand_obj.get_data_dict()
        return brand_ctr_dict

    def get_search_data(self):
        search_obj = search.SearchList(self.list_type, self.date, self.last_date, self.start_time, self.end_time,
                                       self.sensors_operator, self.es_operator)
        search_ctr_dict = search_obj.get_data_dict()
        return search_ctr_dict

    def get_activity_data(self):
        activity_obj = activity.ActivityList(self.list_type, self.date, self.last_date, self.start_time, self.end_time,
                                             self.sensors_operator, self.es_operator)
        activity_ctr_dict = activity_obj.get_data_dict()
        return activity_ctr_dict

    def get_other_data(self):
        other_obj = other.OtherList(self.list_type, self.date, self.last_date, self.start_time, self.end_time,
                                    self.sensors_operator, self.es_operator)
        other_ctr_dict = other_obj.get_data_dict()
        return other_ctr_dict

    def get_index_data(self):
        index_obj = index.IndexList(self.list_type, self.date, self.last_date, self.start_time, self.end_time,
                                    self.sensors_operator, self.es_operator)
        index_ctr_dict = index_obj.get_data_dict()
        return index_ctr_dict

    def get_recommend_data(self):
        recommend_obj = recommend.RecommendList(self.list_type, self.date, self.last_date, self.start_time,
                                                self.end_time, self.sensors_operator, self.es_operator)
        recommend_ctr_dict = recommend_obj.get_data_dict()
        return recommend_ctr_dict
