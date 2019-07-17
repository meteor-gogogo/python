#!/usr/bin/env python
# coding=utf-8
from datetime import date
from configparser import ConfigParser
from models import mysql, sensors
from dataoperator import datamanage
import os
from models import es


class BufRun(object):

    def __init__(self, day):
        self.day = day

    def run(self):
        cur_path = os.path.abspath(os.path.dirname(__file__))
        config_path = os.path.abspath(cur_path + '/config/config.ini')
        config = ConfigParser()
        config.read(config_path)
        today = date.today().strftime("%Y-%m-%d")
        mysql_operator = mysql.MysqlOperator(config)
        dict_list = mysql_operator.get_uid_list()
        uid_products_count_dict = dict_list[0]
        uid_list = dict_list[1]
        filename = '/home/aplum/tmp/' + str(today) + '-user_find.csv'
        esop = es.EsOperator(host=config.get("ELASTICSEARCH", "host"), user=config.get("ELASTICSEARCH", "user"),
                             password=config.get("ELASTICSEARCH", "password"), port=config.get("ELASTICSEARCH", "port"),
                             filename=filename, days=self.day)

        uid_count_dict = esop.get_user_action_count(uid_list)
        senop = sensors.SensorsOperator(
            url=str(config.get("SENSORS", "url")) + "?token=" + str(config.get("SENSORS", "token")))
        uid_pid_dict = senop.get_user_pids(uid_list)
        # for key in uid_pid_dict.keys():
        #     pid_list = uid_pid_dict[key]
        #     if pid_list:
        #         for pid in pid_list:
        #             print(key, pid)

        # 汇总数据,生成结构uid --> pid --> 购买商品数 --> 最新上架点击量,并且将数据存到MySQL表中,准备与商品表联合查询品类信息
        print("汇总数据")
        manage = datamanage.DataManage(config)
        manage.aggregate(uid_products_count_dict, uid_count_dict, uid_pid_dict)
