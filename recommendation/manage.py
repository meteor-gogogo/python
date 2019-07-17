#!/usr/bin/python3
# coding=utf-8

import time
import pickle
import lib.day as day
from controller.cartCF import Cart
from controller.detailPageCF import DetailPage
import controller.valid as valid
import controller.reRank as rank
import controller.userProfile as up
import controller.varietyResort as resort
import model.product as Product
from  model.wishlist import  Wish
import model.redistool as redis
import model.product as Product
#from collections import defaultdict
"""
@param Days:    训练数据的天数，X天前~当前时间,单位: %Y-%m-%d %H:%M:%S
"""
def run(days):
    startRec = time.time()
    print(day.unix2date(startRec))

    #recall from CF
    wish = Wish(day.startUnixtime(days+3))
    wishData = wish.loadData()
    print('total wish users(%d days): %d'%(days+3,len(wishData)))

    cart = Cart(K=64, days=days)
    cartDic = cart.recommend(wishData)
    print ("total cart users: %d\n"%len(cartDic))

    detailPage = DetailPage(K=48, days=days)
    detailPageDic = detailPage.recommend()
    print ("total detailPage users: %d\n"%len(detailPageDic))

    onsale = set(Product.construct())
    print ("onsale products: %d"%len(onsale))

    userCF=valid.userCF(days, onsale, cartDic, detailPageDic)
    print()

    #get user profile
    userProfile = up.GetProfileProducts(list(userCF.keys()))
    
    #修改权重，rerank
    userCF = rank.AssignPRWeight(userCF, userProfile)

    #remove score
    userFinal = valid.remove(userCF)
    #with open('data/userFinal.pickle', 'wb') as f:
    #   pickle.dump(userFinal, f, pickle.HIGHEST_PROTOCOL)

    #save, expire 15days ,beta
    redis.setRecommend(userFinal, 'cf:',3600*24*15)

    #加入交叉规则
    #with open('data/userFinal.pickle', 'rb') as f:
    #    userFinal = pickle.load(f)
    startReRank = time.time()
    for user, products in userFinal.items():
        userFinal[user] = resort.VarietyReRank(user, products)
        #print("user=%s, final=%d"%(user,len(userFinal[user])))
    print ('profile rekank cost time: %f' % (time.time() - startReRank))
        
    #查看前后变化
    ###valid.changeState(userFinal)

    #save, expire 15days,alpha
    redis.setRecommend(userFinal, 'pr:',3600*24*15)

    print ('total recommend cost time: %f\n\n\n' % (time.time() - startRec))

