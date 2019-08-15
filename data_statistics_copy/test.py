from datetime import datetime, date, timedelta
import time
import pandas as pd
from utils import output
import csv
import json
import requests
from dateutil.relativedelta import relativedelta


sensors_url = "http://websa.aplum-inc.com/api/sql/query?token=15ba27be5b7895379a0574993db783281988c16f95969fe423847d31ed95728d"
if __name__ == '__main__':
    for i in range(0,0):
        print(i)
    print((datetime.strptime('2018-11-01', '%Y-%m-%d') + relativedelta(months=0)).strftime('%Y-%m-%d'))
    # print(str((date.today() + timedelta(days=-1)).replace(day=1)))
    # # print(int('01'))
    # today = date.today().replace(day=1)
    # print(today)
    # start_month = int(str(today).split('-')[1]) - 1
    # if start_month == 0:
    #     start_month_tmp = 12
    # elif start_month < 10:
    #     start_month_tmp = '0' + str(start_month)
    # else:
    #     start_month_tmp = str(start_month)
    # start_date = str(int(str(today).split('-')[0]) - 1) + '-' + str(start_month_tmp) + '-01'
    # end_date = str(int(str(today).split('-')[0]) - 1) + '-' + str(str(today).split('-')[1]) + '-01'
    # print(start_date)
    # print(end_date)
    # print((datetime.strptime(str(date.today()), '%Y-%m-%d') + timedelta(days=-1)).strftime('%Y-%m-%d'))
    # print(int(str(date.today()).split('-')[1]))
    # print(str(datetime.strptime(str(date.today()), '%Y-%m-%d').strftime('%Y-%m')))
    # # print(date.today())
    start_timestamp = int(time.mktime(time.strptime('2016-10-01 00:00:00', '%Y-%m-%d %H:%M:%S')) * 1000)
    end_timestamp = int(time.mktime(time.strptime('2019-08-12 00:00:00', '%Y-%m-%d %H:%M:%S')) * 1000)
    print(start_timestamp)
    print(end_timestamp)
    # print(int(time.mktime(time.strptime('2018-11-01 00:00:00', '%Y-%m-%d %H:%M:%S')) * 1000))
    # print(int(time.mktime(time.strptime('2018-12-01 00:00:00', '%Y-%m-%d %H:%M:%S')) * 1000))
    # print(int(time.mktime(time.strptime('2019-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')) * 1000))
    # print(int(time.mktime(time.strptime('2019-02-01 00:00:00', '%Y-%m-%d %H:%M:%S')) * 1000))
    # print(int(time.mktime(time.strptime('2019-03-01 00:00:00', '%Y-%m-%d %H:%M:%S')) * 1000))
    # print(int(time.mktime(time.strptime('2019-04-01 00:00:00', '%Y-%m-%d %H:%M:%S')) * 1000))
    # print(int(time.mktime(time.strptime('2019-05-01 00:00:00', '%Y-%m-%d %H:%M:%S')) * 1000))
    # print(int(time.mktime(time.strptime('2019-06-01 00:00:00', '%Y-%m-%d %H:%M:%S')) * 1000))
    # result_list = list()
    # for i in range(10):
    #     result_list.append(i)
    # print("','".join(str(i) for i in result_list))
    #
    # print((datetime.strptime('2019-07-01', '%Y-%m-%d') + timedelta(days=-1)).strftime('%Y-%m-%d'))
    # starttime = time.time()
    # tic = lambda: '%1.1f seconds' % (time.time() - starttime)
    # # data_dict = dict()
    # # data_dict['user_id'] = list()
    # # data_dict['time'] = list()
    # # data_dict['productid'] = list()
    # lastdate = datetime.strptime('2018-05-29', '%Y-%m-%d')
    # for i in range(1, 5):
    #     datetmp = lastdate + timedelta(days=i)
    #     datetmp = date.strftime(datetmp, '%Y-%m-%d')
    #     sql = "select user_id, distinct_id, event, time, productid from events where date = '{0}' and event in ('GeneralOpenApp', 'ViewProduct', 'AddCart', 'AddWish')".format(datetmp)
    #     pay_load = {'q': sql, 'format': 'json'}
    #     r = requests.post(sensors_url, pay_load)
    #     if r.status_code == 200:
    #         datastr = r.text
    #         if len(datastr) == 0:
    #             uv_list = 0
    #         else:
    #             dataarr = datastr.split('\n')
    #             with open("C:\\Users\\liuhang\\Desktop\\datatmp\\plum_{0}.csv".format(datetmp), 'w', newline='') as file:
    #                 line = 'user_id,distinct_id,event,time,productid\n'
    #                 file.write(line)
    #                 for line in dataarr:
    #                     if len(line) != 0:
    #                         data_json = json.loads(line, encoding='utf-8')
    #                         event_name = data_json['event']
    #                         if event_name == 'GeneralOpenApp':
    #                             row = str(data_json['user_id']) + ',' + str(data_json['distinct_id']) + ',' + str(data_json['event']) + ',' + str(
    #                                 data_json['time']) + '\n'
    #                         else:
    #                             row = str(data_json['user_id']) + ',' + str(data_json['distinct_id']) + ',' + str(data_json['event']) + ',' + str(
    #                                 data_json['time']) + ',' + \
    #                                   str(data_json['productid']) + '\n'
    #                         file.write(row)
    #                 file.flush()
    #                 file.close()
    #     else:
    #         output.output_info("get_list_uv sql occurs a error, sql is %s" % sql)
    # print('耗时: %s' % tic())

