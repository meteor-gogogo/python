# coding=utf-8

import time
import model.profile as p
from collections import defaultdict

CID='cid'
BID='bid'
PRICE= 'price'
PRANGE = 0.2
WEIGHT = 'weight'

class Catbrdprice(object):
    def __init__(self,txt):
        self.items = list()
        tastes = txt.split('&')
        for t in tastes:
            values  = t.split(':')
            if len(values) < 4:
                continue
            tmp = dict()
            tmp[CID] = int(values[0])
            tmp[BID] = int(values[1])
            tmp[WEIGHT] = float(values[2])
            tmp[PRICE] = float(values[3])

            if len(tmp)>0:
                self.items.append(tmp)

def GetProfileProducts(uids):

    #get user profile
    userProfileStr = p.LoadUserProfile(uids)
    userProfile = dict()
    for user,profile in userProfileStr.items():
        userProfile[user] = Catbrdprice(profile['catbrdprice'])
        #print('\n'.join(['%s:%s' % item for item in userProfile[user].__dict__.items()]))

    #get user products,暂不用做召回
    #userProducts = dict()

    return userProfile

def SetWeight(profile, product):

    #print('\n'.join(['%s:%s' % item for item in profile.__dict__.items()]))
    #print('\n'.join(['%s:%s' % item for item in product.__dict__.items()]))
    for item in profile.items:
       #if product.category == item[CID] and product.brand == item[BID] and \
       if product.category == item[CID] and \
               item[PRICE]*(1-PRANGE)<= product.discountPrice and \
               product.discountPrice<=item[PRICE]*(1+PRANGE):
                   return item[WEIGHT]
    
    #print(product.brand, type(product.brand))
    #print(product.category, type(product.category))
    #print(product.discountPrice, type(product.discountPrice))
    
    return 0.0

