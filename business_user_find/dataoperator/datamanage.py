#!/usr/bin/env python
# coding=utf-8

from models import mysql


class DataManage(object):

    def __init__(self,config):
        self.config = config

    def aggregate(self,uid_productscount_dict:dict,uid_count_dict:dict,uid_pid_dict:dict):
        lines = list()
        print("uid_productscount_dict : " + str(len(uid_productscount_dict)))
        for key in uid_productscount_dict.keys():
            uid = key
            products_count = uid_productscount_dict[uid]
            # print(products_count)
            view_count = uid_count_dict.get(str(uid), "0")
            # 如果view_count为0 跳过
            if view_count == "0":
                continue
            # print(view_count)
            pid_list = uid_pid_dict.get(str(uid))
            # print(len(pid_list))
            if pid_list:
                for pid in pid_list:
                    line = str(uid) + "," + str(products_count) + "," + str(view_count) + "," + str(pid)
                    lines.append(line)
            else:
                continue

        # 将数据保存到MySQL
        operator = mysql.MysqlOperator(self.config)
        operator.save_data_tomysql(lines)

