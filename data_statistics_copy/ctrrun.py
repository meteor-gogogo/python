#!/usr/bin/env python
# coding=utf-8
from datetime import timedelta, date
from configparser import ConfigParser
from utils import baseexcel, basemail
from baseoperator import dataoperator
import pandas as pd
import os


class CtrRun(object):
    def __init__(self, list_type, list_id):
        self.list_type = list_type
        self.list_id = list_id
        self.sheet_name = {'category': '分类页', 'index': '首页', 'total': '总数', 'brand': '品牌页',
                           'search': '搜索页', 'activity': '活动页', 'other': '其他页', 'recommend': '推荐页'}
        self.en_columns = ['data', 'show_num', 'uv_list', 'pv_list', 'uv_detail', 'pv_detail', 'pv_detail_sid',
                           'pv_detail_distinct', 'list_add_cart_num', 'add_cart_num', 'total_cart_cp', 'order_uv',
                           'order_num', 'pay_order_num', 'total_order_cp', 'total_order_pp']
        self.new_columns = {'data': '日期', 'show_num': '活动页曝光量', 'uv_list': '活动页总uv数',
                            'pv_list': '活动页总pv数', 'uv_detail': '活动页到达详情页总uv数',
                            'pv_detail': '活动页到达详情页总pv数(不滤重)',
                            'pv_detail_sid': '活动页到达详情页总曝光量pv(滤重)',
                            'pv_detail_distinct': '活动页到达详情页总曝光量(滤重同一商品)',
                            'list_add_cart_num': '活动页列表加购数', 'add_cart_num': '活动页加购数',
                            'total_cart_cp': '活动页加购商品折扣总价', 'order_uv': '活动页支付订单uv数',
                            'order_num': '活动页支付订单数', 'pay_order_num': '活动页支付订单商品数',
                            'total_order_cp': '活动页支付订单商品折扣总价',
                            'total_order_pp': '活动页支付订单商品实付总价'}

    def run(self):
        # 获取当前日期
        today = date.today()

        # 获取昨天日期
        yesterday = today + timedelta(days=-1)

        # 格式化日期
        yesterday = yesterday.strftime('%Y-%m-%d')

        # 获取当前文件的绝对路径
        cur_path = os.path.abspath(os.path.dirname(__file__))

        # 获取配置文件的绝对路径
        config_path = os.path.abspath(cur_path + '/config/config.ini')

        # 获取ConfigParser对象
        config = ConfigParser()

        # 读取配置文件信息
        config.read(config_path)

        # 初始化一个dataoperator对象
        operator_obj = dataoperator.DataOperator(self.list_type, config)
        if self.list_type == 'total':
            print('=====总数=====')
            total_ctr_dict = operator_obj.get_all_data()
            total_df = pd.DataFrame(list(total_ctr_dict.values()))
            print('=====分类页=====')
            category_ctr_dict = operator_obj.get_category_data()
            category_df = pd.DataFrame(list(category_ctr_dict.values()))
            print('=====搜索页=====')
            search_ctr_dict = operator_obj.get_search_data()
            search_df = pd.DataFrame(list(search_ctr_dict.values()))
            print('=====活动页=====')
            activity_ctr_dict = operator_obj.get_activity_data()
            activity_df = pd.DataFrame(list(activity_ctr_dict.values()))
            print('=====品牌页=====')
            brand_ctr_dict = operator_obj.get_brand_data()
            brand_df = pd.DataFrame(list(brand_ctr_dict.values()))
            print('=====其他页=====')
            other_ctr_dict = operator_obj.get_other_data()
            other_df = pd.DataFrame(list(other_ctr_dict.values()))
            print('=====首页=====')
            index_ctr_dict = operator_obj.get_index_data()
            index_df = pd.DataFrame(list(index_ctr_dict.values()))
            print('=====推荐页=====')
            recommend_ctr_dict = operator_obj.get_recommend_data()
            recommend_df = pd.DataFrame(list(recommend_ctr_dict.values()))

            # 定义文件名称
            file_name = 'ctr_total_day_%s.xls' % yesterday

            # 获得excel操作对象
            baseexcel_obj = baseexcel.BaseExcel(filename=file_name, sheetname=self.sheet_name,
                                                encolumns=self.en_columns, newcolumns=self.new_columns)
            baseexcel_obj.create_excel_writer()
            baseexcel_obj.data_to_excel(data=total_df, list_type='total')
            baseexcel_obj.data_to_excel(data=category_df, list_type='category')
            baseexcel_obj.data_to_excel(data=search_df, list_type='search')
            baseexcel_obj.data_to_excel(data=activity_df, list_type='activity')
            baseexcel_obj.data_to_excel(data=brand_df, list_type='brand')
            baseexcel_obj.data_to_excel(data=other_df, list_type='other')
            baseexcel_obj.data_to_excel(data=index_df, list_type='index')
            baseexcel_obj.data_to_excel(data=recommend_df, list_type='recommend')
            baseexcel_obj.save_excel()

            # 发送邮件相关内容
            subject = '数据统计'
            content = '数据见附件'
            addr_list = ['wengxuejie@aplum.com.cn']
            attachment_filepath = os.path.abspath(cur_path + '/' + file_name)
            basemail_obj = basemail.BaseMail(config=config, subject=subject, content=content, receivers=addr_list,
                                             file_path=attachment_filepath)
            basemail_obj.send_mail()
        else:
            if self.list_id == 0:
                type_ctr_dict = operator_obj.get_data_by_type(list_type=self.list_type)
                type_df = pd.DataFrame(list(type_ctr_dict.values()))
                file_name = 'ctr_%s_day_%s.xls' % (self.list_type, yesterday)
                baseexcel_obj = baseexcel.BaseExcel(filename=file_name, sheetname=self.sheet_name,
                                                    encolumns=self.en_columns, newcolumns=self.new_columns)
                baseexcel_obj.create_excel_writer()
                baseexcel_obj.data_to_excel(data=type_df, list_type=self.list_type)
                baseexcel_obj.save_excel()

                subject = '%s数据统计' % self.sheet_name[self.list_type]
                content = '数据见附件'
                attachment_filepath = os.path.abspath(cur_path + '/' + file_name)
                addr_list = ['wengxuejie@aplum.com.cn']

                basemail_obj = basemail.BaseMail(config=config, subject=subject, content=content, receivers=addr_list,
                                                 file_path=attachment_filepath)
                basemail_obj.send_mail()
            else:
                pass
