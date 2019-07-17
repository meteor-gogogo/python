#!/usr/bin/env python
# coding=utf-8
import MySQLdb
import datetime as dt
from datetime import datetime,timedelta
import requests
import getopt
import random
import redis
import json
import sys

mysqlhost = 'rr-2ze7824567d8dgu1y.mysql.rds.aliyuncs.com'
mysqlusername = 'plumdb'
mysqlpasswd = 'plumdb@2018'
dbname='aplum'

redishost = 'r-2zec5074fca1ade4.redis.rds.aliyuncs.com'
redispassword = "dL5pr5RkJBQEuqj"

def usage():
    print('coldboot.py -d <daynum> -k <num>')
    print('or: coldboot.py --day=<daynum> --knum=<num>')
    sys.exit(2)

def output_info(info):
    print(info)
    sys.exit(1)

def getdaytime(day):
    if not day:
        return False,False
    today=dt.date.today()
    now_time = dt.datetime.now()
    change_time = now_time + dt.timedelta(days=-day)
    beforeday = change_time .strftime('%Y-%m-%d')
    return today, beforeday

def getCidList(cursor, days):
    if not days:
        return []
    cursor.execute("set names utf8")
    sql = "select tp.category_id, count(*) as num  from t_order_item oi join t_product  tp on oi.product_id = tp.id  group by tp.category_id order by num desc limit 20"
    cursor.execute(sql)
    results = cursor.fetchall()
    return results

def getPidSaList(cidlist, days):
    if not cidlist or not days:
        return []
    cids = ''
    for cid in cidlist:
        cids += "'%s',"%cid
    cids = cids.strip(",")
    url = "http://websa.aplum-inc.com/api/sql/query?token=15ba27be5b7895379a0574993db783281988c16f95969fe423847d31ed95728d"
    today, beforeday = getdaytime(days)
    if not today and not beforeday:
        return []
    sql = "SELECT productid, count(*) as num FROM EVENTS WHERE EVENT = 'ViewProduct' AND  date BETWEEN '%s' AND '%s' AND Src_page_type = 'category' AND src_page_id in (%s) group by productid order by num desc limit 2000"%(beforeday, today, cids)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data = payload)
    if r.status_code == 200 :
        data = r.text
        return data.split("\n")
    else:
        return []

def getOnsaleProductList(cursor, pidlist, knum):
    if not pidlist:
        return []
    pids = ','.join(pidlist)
    cursor.execute("set names utf8")
    sql = "select id, name , category_id,  status from t_product  where id not in (select tap.product_id from t_activity ta join t_activity_product tap on ta.id = tap.activity_id where ta.activity_type = 'seckill' ) and id not in (select ref_id from t_lottery where prize_type = 'product') and id in (%s) and status = 'onsale' and in_offline_shop=0  order by FIELD(id, %s)  limit %d"%(pids, pids, knum)
    cursor.execute(sql)
    results = cursor.fetchall()
    return results

if __name__ == "__main__":
    argv = sys.argv[1:]
    if len(argv) == 0:
        usage()
    try:
        opts, args = getopt.getopt(argv, "hd:k:", ["help", "day=", "knum="])
    except getopt.GetoptError:
        usage()

    daynum  = 1
    knum = 1
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
        elif opt in ("-d", "--day"):
            daynum = int(arg)
        elif opt in ("-k", "--knum"):
            knum = int(arg)

    if not daynum:
        daynum = 7
    if not knum:
        knum = 1000

    db = MySQLdb.connect(mysqlhost, mysqlusername,  mysqlpasswd, dbname, charset='utf8')
    cursor = db.cursor(cursorclass = MySQLdb.cursors.DictCursor)
    cidlist = []
    mysqllist = getCidList(cursor, daynum)
    if not mysqllist or len(mysqllist) == 0 :
        output_info("getCidList error")

    for data in mysqllist:
        cidlist.append(data['category_id'])
    pidlist = []
    pidsalist = getPidSaList(cidlist, daynum)
    if len(pidsalist) == 0 :
        output_info("getpidsalist error")

    for data in pidsalist:
        if data == '':
            continue
        d = json.loads(data,encoding='utf-8')
        pidlist.append(str(d.get('productid')))

    result = getOnsaleProductList(cursor, pidlist, knum)
    if not result or len(result) == 0 :
        output_info("getOnsaleProductList error")

    a = []
    newpidlist = []
    for info in result:
        #print(info['name'], info['category_id'])
        a.append(info['name'])
        newpidlist.append(str(info['id']))
    r = redis.StrictRedis(host=redishost, port=6379, db=1,  password = redispassword)
    for i in range(10):
        keyword = "hotproduct:%d"%(i,)
        recommendpidlist = random.sample(newpidlist, 100)
        recommendpids = ','.join(recommendpidlist)
        r.set(keyword, recommendpids)
        print(random.sample(a, 100))

    cursor.close()     #断开游标
    db.close()    #断开数据库
