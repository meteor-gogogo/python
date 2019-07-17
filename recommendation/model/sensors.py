# coding=utf-8

import requests
import json
import lib.day as day

#_sensorURL = "http://sa.aplum-inc.com:8107/api/sql/query?token=15ba27be5b7895379a0574993db783281988c16f95969fe423847d31ed95728d"
_sensorURL = "http://websa.aplum-inc.com/api/sql/query?token=15ba27be5b7895379a0574993db783281988c16f95969fe423847d31ed95728d"
_everycount=50000

def _request(sql):
    #print (sql)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(_sensorURL, data = payload)
    if r.status_code == 200:
        return r.text.split("\n")
    else:
        print ('errorCode='+str(r.status_code))
    return []   

def loadUserList(date):
    userList = []
    start = 0
    end = _everycount
    loop = True
    while loop:
        sql = "select * from(select row_number() over (order by distinct_id) as rnum,EVENTS.distinct_id from EVENTS where EVENT='ViewProduct' and date>='%s' group by distinct_id)t where t.rnum>=%d and t.rnum<%d"%(date, start, end)
        data = _request(sql)
        for d in data:
            if d == "":
                continue
            row = json.loads(d,encoding='utf-8')
            userList.append(row.get("distinct_id"))

        #update iterator
        if len(data) < _everycount:
            break
        start = end
        end += _everycount
        #break

    return userList
           
def loadUserCart(days):
    date = day.startDate(days)
    time = day.startHourMinite(days)
    userProducts = dict()
    curID = '0'
    while True:
        sql = "SELECT ta.distinct_id,group_concat(cast(ta.productid as string), ',') as pids FROM (SELECT distinct_id,productid FROM EVENTS WHERE EVENT = 'AddCart' AND date>='%s' AND time>'%s' GROUP BY distinct_id,productid) ta group by distinct_id having ta.distinct_id>'%s' order by distinct_id limit %d"%(date, time, curID,_everycount)
        data = _request(sql)
        #print (data)
        if len(data) == 0 or (len(data)==1 and data[0]==""):
            break

        for d in data:
            if d == "":
               continue
            row = json.loads(d,encoding='utf-8')
            uid = row.get('distinct_id')
            if uid.find('unknown')>=0:
                continue
            """
            if uid.find('dev')>=0:
                continue
            """
            pids = row.get('pids')
            if uid != "" and pids != "":
               userProducts[uid] = pids.split(",") #pid is string
            #update iterator
            curID = uid

            #print ("userProductsLen:" + str(len(userProducts)))

        if len(data)< _everycount:
            break

        #break
    return userProducts 

def loadUserDetailPage(days, isDev=True):
    date = day.startDate(days)
    time = day.startHourMinite(days)
    userProducts = dict()
    start= 0
    end = _everycount
    while True:
        sql = "select * from (select row_number() over (order by distinct_id) as rnum,EVENTS.distinct_id, EVENTS.productid from EVENTS WHERE EVENT = 'ViewProduct' AND date>='%s' and time>'%s' GROUP BY distinct_id,productid)t where t.rnum >=%d and t.rnum<%d"%(date, time, start, end)
        #print (sql)
        data = _request(sql)
        #print (data)
        for d in data:
            if d == "":
               continue
            row = json.loads(d,encoding='utf-8')
            uid = row.get('distinct_id')
            if uid.find('unknown') >=0:
                continue
            if isDev is False:
                if uid.isdigit() is False:
                     continue
            pid = str(row.get('productid'))#pid == string

            if uid != "" and pid != "":
                if uid in userProducts:
                    userProducts[uid].append(pid)
                else:
                    userProducts[uid] = [pid]

        #update iterator
        if len(data)< _everycount:
            break
        start = end
        end += _everycount
        #break

    return userProducts

def _productIDStr(item):
    return str(item.get('productid'))
def _pageID(item):
    return item.get('src_page_id')
def _count(item):
    return item.get('count')


def batchProduct(values):
    batch=5000
    vLen = len(values)
    times = int(vLen/batch)+1
    #print (times)
    for t in range(times):
        start = t*batch
        end = (t+1)*batch
        if end>= vLen:
            end = vLen

        tmp = values[start:end]
        #print (tmp)
        yield ','.join(tmp)

def _buildProductSql(Days, pidStr):
    return "select ProductID, count(*) as count from events where event='ViewProduct' and date>='%s' and ProductID in (%s) group by ProductID"%(day.startDate(Days), pidStr)

def _parseProduct(dic, data):
    for d in data:
        if d == "":
            continue
        item = json.loads(d,encoding='utf-8')
        dic[_productIDStr(item)] = _count(item)

def loadProductPage(Days, pidList):
   dic = {} 
   for batch in batchProduct(pidList):
      data = _request(_buildProductSql(Days, batch)) 
      #print ("pageDataLen: "+ str(len(data)))
      _parseProduct(dic, data)
      
   return dic
