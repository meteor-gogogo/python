#!/usr/bin/env python
# coding=utf-8


class BaseList(object):
    def __init__(self, list_type, datetime_str, last_date, start_time, end_time, sensors, es):
        self.date = datetime_str
        self.last_date = last_date
        self.start_time = start_time
        self.end_time = end_time
        self.list_type = list_type
        self.sensors = sensors
        self.es = es
        self.day = self.date.strftime("%Y-%m-%d")

    def get_data_dict(self):
        ctr_info = dict()
        ctr_dict = dict()
        ctr_info['date'] = self.day
        ctr_info['shownum'] = self.es.get_total_show_num(self.start_time, self.end_time, self.list_type)
        ctr_info['uv_list'] = self.sensors.get_list_uv(self.day, self.list_type)
        ctr_info['pv_list'] = self.sensors.get_list_pv(self.day, self.list_type)
        ctr_info['uv_detail'] = self.sensors.get_detail_uv(self.day, self.list_type)
        ctr_info['pv_detail'] = self.sensors.get_detail_pv(self.day, self.list_type)
        ctr_info['pv_detail_distinct'] = self.sensors.get_detail_distinct_pv(self.day, self.list_type)
        ctr_info['pv_detail_sid'] = self.sensors.get_detail_sid_pv(self.day, self.list_type)
        ctr_info['listaddcartnum'] = self.sensors.get_list_add_cart_num(self.date, self.list_type)
        ctr_info['addcartnum'], ctr_info['totalcartcp'] = self.sensors.\
            get_add_cart_num_and_price(self.date, self.list_type)
        ctr_info['orderuv'] = self.sensors.get_order_uv(self.last_date, self.date, self.list_type)
        ctr_info['ordernum'] = self.sensors.get_order_num(self.last_date, self.date, self.list_type)
        ctr_info['payordernum'], ctr_info['totalordercp'], ctr_info['totalorderpp'] = self.sensors.\
            get_payorder_detail_numa_and_price(self.last_date, self.date, self.list_type)
        ctr_dict[self.day] = ctr_info
        return ctr_dict
