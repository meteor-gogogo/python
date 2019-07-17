#!/usr/bin/python3
# coding=utf-8

import sys
import time
import lib.day as day
import model.product as pro
import model.click as se
from operator import itemgetter
import operator
from collections import defaultdict
import model.redistool as redis

import traceback
import lib.robot as robot

Days=1
def manage():
    startRec = time.time()
    print(day.unix2date(startRec))

    #从神策获取，每个用户点击的商品，商品之间的间隔为15分钟之内
    clickDic = se.loadUserClick(Days, 15*60)
    print ("user click product: %d"%len(clickDic))

    onsale = set(pro.construct())
    print ("onsale products: %d"%len(onsale))

    #sort by count desc
    final = defaultdict(list)
    mininum = 40
    maxnum = 200
    less40 = 0
    for pid,products in clickDic.items():
        #已售商品不做推荐
        if pid not in onsale:
            continue
        res = sorted(products.items(), key=itemgetter(1), reverse=True)
        tmp = list()
        #去除已售商品
        for productID,_ in res:
            if productID in onsale:
                tmp.append(productID)

        if len(tmp) < mininum:
            less40 +=1
        elif len(tmp) > maxnum:
            tmp = tmp[:maxnum]
        if len(tmp)>0:
            final[pid] = tmp

    print('less than %dCount=%d'%(mininum, less40))
    print('recommend onsale product=%d'%len(final))
    #save
    redis.setRecommend(final, 'look:',3600*24*3)

    print ('looklook cost time: %f\n\n\n' % (time.time() - startRec))

if __name__ == '__main__':
    if len(sys.argv) <=1:
        print ("default data %d days"%(Days))
    else:
        Days = int(sys.argv[1])
        print ("calc data %d days"%(Days))

    try:
        manage()
    except Exception as e:
        print ("manage run error")
        msg = traceback.format_exc()
        print (msg)
        robot.alarmDD('recommendation/looklook.py: err=%s'%str(e))


