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
import model.brand as br
import model.excel as excel

def calcBrands(data):
    brandDic= br.constructBrand()
    Look = 0
    top = 10
    lookData = list()
    for bid,brands in data.items():
       if bid not in brandDic:
           print("err bid",bid)
           continue
       Look +=1
       res = sorted(brands.items(), key=itemgetter(1), reverse=True)[:top]
       for lookbid, count in res:
           if lookbid not in brandDic:
               print("err lookbid",lookbid)
               continue
           lookData.append([brandDic[bid].name,brandDic[lookbid].name,count])

    print("totalBrand=%d, Look=%d"%(len(brandDic), Look))
    return lookData

def viewProductListLook(Days):
    clickPageDic = se.loadUserClickPage(Days, 60*60)
    print ("user click page: %d"%len(clickPageDic))

    lookData = calcBrands(clickPageDic)
    excel.saveBrandXlsx(lookData,"BrandPageLook.xlsx")

def viewProductLook(Days):
    #从神策获取，每个用户点击的商品，商品之间的间隔为60分钟之内
    clickDic = se.loadUserClick(Days, 60*60)
    print ("user click product: %d"%len(clickDic))

    pidSet = set()
    for pid,products in clickDic.items():
        pidSet.add(pid)
        for lookid,_ in products.items():
            pidSet.add(lookid)
    print("pidset = %d"%len(pidSet))

    #获取商品字典,包括已售及不可见
    productDic = pro.getProductDicByID(list(pidSet))
    print ("looklook products: %d"%len(productDic))

    #sort by count desc
    final = defaultdict(dict)
    for pid,products in clickDic.items():
        headBid = productDic[pid].brand
        for lookid,count in products.items():
            lookBid = productDic[lookid].brand
            if headBid != lookBid:
                if lookBid in final[headBid]:
                    final[headBid][lookBid] += count 
                else:
                    final[headBid][lookBid] = count 

    lookData = calcBrands(final)
    excel.saveBrandXlsx(lookData,"BrandLook.xlsx")
    #save
    #redis.setRecommend(final, 'look:',3600*24*3)

Days=1
if __name__ == '__main__':
    if len(sys.argv) <=1:
        print ("default data %d days"%(Days))
    else:
        Days = int(sys.argv[1])
        print ("calc data %d days"%(Days))

    startRec = time.time()
    print(day.unix2date(startRec))

    if len(sys.argv)==3 and sys.argv[2] == 'page':
        viewProductListLook(Days)
    else:
    #默认获取商品详情页的品牌后继
        viewProductLook(Days)

    print ('brand-looklook cost time: %f\n\n\n' % (time.time() - startRec))

