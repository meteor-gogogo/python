# coding: utf-8 -*-
import model.sensors as se 
from lib.collaborativeFilter import CollaborativeFilter
import time

class Cart:
    def __init__(self, K, days):
        self.K= K
        self.days = days

    def _loadData(self):
        return se.loadUserCart(self.days)
   
    def recommend(self,wishData=dict()):
        print ('start cart recommend')
        train = self._loadData()
        #print('before wish,len=%d\n'%len(train))
        if len(wishData)>0:
            for user,pids in wishData.items():
                if user in train:
                    cartPids = set(train[user])
                    for p in pids:
                        if p not in cartPids:
                            train[user].append(p)
                else:
                    #print('not cart,only wish,user=%s,len=%d'%(user,len(pids)))
                    train[user] = pids
        #print('after wish,len=%d'%len(train))
        cf = CollaborativeFilter()
        cf.itemSimilarity(train)

        start = time.time()
        ru = dict()
        for user, productList in train.items():
            if user.find('dev') >=0:
                continue
            ru[user] = cf.recommend(productList, self.K)
            #ru[user] = [pid for pid,score in tmp]
        print('cart recommend cost time: %f' % (time.time() - start))
        return ru
  
