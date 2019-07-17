#!/usr/bin/env python
# coding=utf-8
import configparser
from datetime import datetime, timedelta, date
import csv


def load_data(config, day_start, days):
    click_file_path = config.get("PATH", "click_file_path")
    click_file_suffix = config.get("PATH", "click_file_suffix")
    addwish_file_path = config.get("PATH", "addwish_file_path")
    addwish_file_suffix = config.get("PATH", "addwish_file_suffix")
    payorder_file_path = config.get("PATH", "payorder_file_path")
    payorder_file_suffix = config.get("PATH", "payorder_file_suffix")
    click_tag = config.get("PATH", "click_file_tag")
    click_weight = 1
    addwish_weight = 2
    pauorder_weight = 4
    addwish_tag = config.get("PATH", "addwish_file_tag")
    payorder_tag = config.get("PATH", "payorder_file_tag")
    print(1)

    print(2)
    for i in range(days):
        # 按时间的权重,距离当前时间越远,权重越低,每天按照0.01进行衰减
        weight = 1 - (days - i) / 100
        # 从距离当前时间最远的一天开始算起
        current_day = str(day_start + timedelta(days=i))
        print(current_day)
        click_filename = click_file_path + current_day + click_file_suffix
        # 点击,加购,订单的权重分别为1,2,4
        click_weight = 1 * weight
        click_dict = load_data_dict(click_filename, click_tag, click_weight)
        addwish_filename = addwish_file_path + current_day + addwish_file_suffix
        addwish_weight = 2 * weight
        addwish_dict = load_data_dict(addwish_filename, addwish_tag, addwish_weight)
        payorder_filename = payorder_file_path + current_day + payorder_file_suffix
        payorder_weight = 4 * weight
        payorder_dict = load_data_dict(payorder_filename, payorder_tag, payorder_weight)
        day_dict = union_dict(union_dict(click_dict, addwish_dict), payorder_dict)
        # 判断,如果是第一次进行叠加计算,直接将当前数据赋值给结果字典
        if i == 0 :
            sum_dict = day_dict
        else:
            # 如果不是第一次进行叠加计算,需要汇总当前的数据和之前叠加之后的结果数据
            sum_dict = union_dict(sum_dict, day_dict)

    print(3)
    # 将汇总的结果数据打印到控制台验证结果是否有效
    for tmp in sum_dict:
        for tmp1 in sum_dict[tmp]:
            print(str(tmp) + "-->" + str(tmp1) + "-->" + str(sum_dict[tmp][tmp1]))
    print(4)


# 从指定的文件路径,根据所需字段的下标tag,和不同的权重,读取数据
def load_data_dict(filename, tag, weight):
    uid_pid_count = dict()
    with open(filename, 'r') as f:
        next(f)
        for row in f:
            # print(row)
            fields = row.split(",")
            # print(type(tag[0]))  str
            # print(tag[0])
            # print(tag[1])
            uid = fields[int(tag[1])]
            if uid.startswith("dev:"):
                continue
            pid = fields[int(tag[3])]
            pid_count = uid_pid_count.setdefault(uid, dict())
            pid_count[pid] = pid_count.setdefault(pid, weight) + weight
    return uid_pid_count


# 负责中间字典数据的汇总操作
def union_dict(dict1, dict2):
    for key in dict1:
        pid_dict = dict1[key]
        for pid_key in pid_dict:
            for key2 in dict2:
                if key == key2:
                    for pid_key2 in dict2[key2]:
                        if pid_key == pid_key2:
                            dict1[key][pid_key] = dict1[key][pid_key] + dict2[key2][pid_key2]
    return dict1
