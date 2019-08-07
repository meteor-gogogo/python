#!/usr/bin/env python
# coding=utf-8
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta, date
import requests
import time
import traceback

es = Elasticsearch(["es-cn-0pp127498000e77vd.elasticsearch.aliyuncs.com"], http_auth=('elastic', 'Ut3DSB36m8z5vBL'),
                   port=9200)


def alarm(userlist, msg):
    url = 'http://47.93.240.37:8083/ps'
    users = ','.join(userlist)
    url = "%s?msg=%s&email=%s" % (url, msg, users)
    r = requests.get(url)
    if r.status_code == 200:
        return True
    else:
        return False


# 获取es的磁盘占用率
def getDiskUsedRate():
    indexinfo = es.cat.allocation(format='json')
    disk_used = 0
    disk_total = 0
    for p in indexinfo:
        disk_used += float(str(p['disk.used'])[:-2])
        disk_total += float(str(p['disk.total'])[:-2])
    disk_used_rate = round(disk_used / disk_total, 4)
    return float(disk_used_rate)


# 获取ctr索引列表
def getIndexList():
    data = es.indices.get(index='logstash-nginx-*').keys()
    indexlist = list(data)
    return indexlist


# 获取ctr索引列表中最早的索引
def getEarlistIndex(leasttimestamp, indexlist):
    minstamp = leasttimestamp
    for index in indexlist:
        datestr = index[15:]
        timestamp = int(time.mktime(time.strptime(datestr, "%Y.%m.%d")))
        minstamp = min(minstamp, timestamp)
        date = datetime.fromtimestamp(minstamp).date().strftime("%Y.%m.%d")
        earlistindex = 'logstash-nginx-' + date
    return earlistindex


# 如果磁盘占用超过一定阈值,删除历史索引
def deleteIndex(disk_used_rate, leasttimestamp, indexlist):
    if disk_used_rate >= 0.20:
        earlistindex = getEarlistIndex(leasttimestamp, indexlist)
        datestr = earlistindex[15:]
        for i in range(0, 7):
            date = datetime.strptime(datestr, '%Y.%m.%d').date()
            date = date + timedelta(days=i)
            currentdatestr = date.strftime("%Y.%m.%d")
            index = 'logstash-nginx-' + currentdatestr
            # print(index)
            deleteRes = es.indices.delete(index=index, ignore=[400, 404])
            if deleteRes['acknowledged'] == True:
                print('{} 删除成功'.format(index))
            else:
                print('{} 删除失败'.format(index))
    else:
        return


if __name__ == '__main__':
    try:
        leasttimestamp = int(time.mktime(time.strptime(date.today().strftime("%Y-%m-%d"), "%Y-%m-%d")))
        disk_used_rate = getDiskUsedRate()
        percent = str(round(disk_used_rate * 100, 2)) + '%'
        timestr = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        todaystr = date.today().strftime("%Y-%m-%d")
        print('当前时间: {} nginx_log es磁盘使用率为{}'.format(timestr, percent))
        indexlist = getIndexList()
        deleteIndex(disk_used_rate, leasttimestamp, indexlist)



    except Exception as e:
        msg = traceback.format_exc()
        print("delete nginx_log accurs error, the error:%s" % str(msg))
        userlist = ['liwenlong@aplum.com', 'suzerui@aplum.com']
        err = "清理nginx_log历史数据出错"
        alarm(userlist, err)
