from datetime import datetime, date, timedelta
import time
import pandas as pd
from utils import output
import csv
import json
import requests


sensors_url = "http://websa.aplum-inc.com/api/sql/query?token=15ba27be5b7895379a0574993db783281988c16f95969fe423847d31ed95728d"
if __name__ == '__main__':
    starttime = time.time()
    tic = lambda: '%1.1f seconds' % (time.time() - starttime)
    # data_dict = dict()
    # data_dict['user_id'] = list()
    # data_dict['time'] = list()
    # data_dict['productid'] = list()
    lastdate = datetime.strptime('2018-05-29', '%Y-%m-%d')
    for i in range(1, 5):
        datetmp = lastdate + timedelta(days=i)
        datetmp = date.strftime(datetmp, '%Y-%m-%d')
        sql = "select user_id, distinct_id, event, time, productid from events where date = '{0}' and event in ('GeneralOpenApp', 'ViewProduct', 'AddCart', 'AddWish')".format(datetmp)
        pay_load = {'q': sql, 'format': 'json'}
        r = requests.post(sensors_url, pay_load)
        if r.status_code == 200:
            datastr = r.text
            if len(datastr) == 0:
                uv_list = 0
            else:
                dataarr = datastr.split('\n')
                with open("C:\\Users\\liuhang\\Desktop\\datatmp\\plum_{0}.csv".format(datetmp), 'w', newline='') as file:
                    line = 'user_id,distinct_id,event,time,productid\n'
                    file.write(line)
                    for line in dataarr:
                        if len(line) != 0:
                            data_json = json.loads(line, encoding='utf-8')
                            event_name = data_json['event']
                            if event_name == 'GeneralOpenApp':
                                row = str(data_json['user_id']) + ',' + str(data_json['distinct_id']) + ',' + str(data_json['event']) + ',' + str(
                                    data_json['time']) + '\n'
                            else:
                                row = str(data_json['user_id']) + ',' + str(data_json['distinct_id']) + ',' + str(data_json['event']) + ',' + str(
                                    data_json['time']) + ',' + \
                                      str(data_json['productid']) + '\n'
                            file.write(row)
                    file.flush()
                    file.close()
        else:
            output.output_info("get_list_uv sql occurs a error, sql is %s" % sql)
    print('耗时: %s' % tic())

