# coding=utf-8

import json
import model.sensors as se
import lib.day as day
from collections import defaultdict


class Click(object):
    def __init__(self,id,time):
        self.id = id
        self.time = time
    
def loadUserClickPage(days,seconds):
    clickPageDic = defaultdict(dict)
    userClick = dict()
    date = day.startDate(days)
    step = 100000
    start = 0
    end = step    
    
    while True:
        sql = "SELECT uid,bid,time from (select distinct_ID AS uid,src_page_id AS bid,time,row_number() over (ORDER BY distinct_ID,time) as rnum FROM EVENTS WHERE EVENT='ViewProductList' AND src_page_type='brand' AND date>='%s') t where t.rnum>=%d and t.rnum<%d order by uid,time"%(date,start,end) 
        #print (sql)
        data = se._request(sql)
        for d in data:
            if d == "":
               continue
            row = json.loads(d,encoding='utf-8')
            uid = row.get('uid')
            bid = int(row.get('bid')) #bid is int
            time = day.str2Timestamp(row.get('time'))

            #同一个用户，只存储最近的一次点击事件
            if uid != "" and bid>0 and time !="":
                if uid in userClick:
                    last = userClick[uid]
                    if time - last.time < seconds and bid != last.id:
                        if bid in clickPageDic[last.id]:
                            clickPageDic[last.id][bid] +=1
                        else:
                            clickPageDic[last.id][bid] =1

                    #update last click
                    if time > last.time:    
                        userClick[uid] = Click(bid,time)
                else:
                   userClick[uid] = Click(bid,time)

        #update iterator
        if len(data)< step:
            break
        start = end

    return clickPageDic

def loadUserClick(days,seconds):
    clickDic = defaultdict(dict)
    userClick = dict()
    date = day.startDate(days)
    step = 100000
    start = 0
    end = step    
    while True:
        sql = "SELECT uid,pid,time from (select distinct_ID AS uid,productid AS pid,time,row_number() over (ORDER BY distinct_ID,time) as rnum FROM EVENTS WHERE EVENT='ViewProduct' AND date>='%s') t where t.rnum>=%d and t.rnum<%d order by uid,time"%(date,start,end) 
        #print (sql)
        data = se._request(sql)
        for d in data:
            if d == "":
               continue
            row = json.loads(d,encoding='utf-8')
            uid = row.get('uid')
            pid = str(row.get('pid')) #pid is string
            time = day.str2Timestamp(row.get('time'))
            #print(uid,pid,time)
            #print(type(uid),type(pid),type(time))
            #print('time len=%d'%len(time))

            #同一个用户，只存储最近的一次点击事件
            if uid != "" and pid != "" and time !="":
                if uid in userClick:
                    last = userClick[uid]
                    if time - last.time < seconds and pid != last.id:
                        if pid in clickDic[last.id]:
                            clickDic[last.id][pid] +=1
                        else:
                            clickDic[last.id][pid] =1

                    #update last click
                    if time > last.time:    
                        userClick[uid] = Click(pid,time)
                else:
                   userClick[uid] = Click(pid,time)

        #update iterator
        if len(data)< step:
            break
        start = end
        end += step

    return clickDic
       


