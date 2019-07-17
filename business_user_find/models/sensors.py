#!/usr/bin/env python
# coding=utf-8

from utils import output
from datetime import date,datetime,timedelta
import requests
import json
class SensorsOperator(object):
    def __init__(self,url):
        self.url = url

    def get_user_pids(self,userlist:list):
        uids = ""
        list_length = len(userlist)
        num = 0
        for i in userlist:
            num += 1
            if num == list_length:
                uids = uids + "'" + str(i) + "'"
            else:
                uids = uids + "'" + str(i) + "'" + ","

        # print(uids)

        try:
            sql = "SELECT ta.distinct_id as uid,group_concat(cast(ta.productid as string), ',') as pid \
                    FROM \
                    (SELECT distinct_id,productid \
                    FROM \
                        EVENTS \
                    WHERE EVENT = 'PayOrderDetail' \
                    and distinct_id in ({0}) \
                    AND date < '{1}'\
                    GROUP BY distinct_id,productid) ta\
                    GROUP BY ta.distinct_id".format(uids,date.today().strftime("%Y-%m-%d"))
            payload ={'q': sql, 'format': 'json'}
            r = requests.post(self.url, data=payload)

            result = list()
            if r.status_code == 200:
                datastr = r.text
                if len(datastr) == 0:
                    total = 0
                else:
                    dataarr = datastr.split("\n")
                    for data in dataarr:
                        try:
                            jsondata = json.loads(data)
                            result.append([jsondata["uid"], jsondata["pid"]])
                        except json.decoder.JSONDecodeError as identifier:
                            pass
            else:
                print("sensors sql accurs error" + r.status_code)

            uid_pid_dict = dict()
            for row in result:
                uid = row[0]
                pids = str(row[1])
                pids_list = pids.split(",")
                uid_pid_dict.update({uid: pids_list})

            return uid_pid_dict
        except Exception as e:
            output.output_info("sensors get_user_pids accurs a error :%s"%str(e))