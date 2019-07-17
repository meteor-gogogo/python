# coding: utf-8 -*-
import model.sensors as se 
from lib.collaborativeFilter import CollaborativeFilter
import time

class DetailPage:
    def __init__(self, K, days):
        self.K= K
        self.days = days
        self.MaxScan = 100 #最大的浏览记录,当前一周97%以上用户少于100

    def _loadData(self):
        return se.loadUserDetailPage(self.days)

    def _filterData(self,data):
        for user in list(data.keys()):
            if len(data[user]) >= self.MaxScan:
                #del data[user]
                data[user] = data[user][:10]#数据平滑
        return data
   
    def recommend(self):
        print ('start detailPage recommend')
        data = self._loadData()
        train = self._filterData(data)
        cf = CollaborativeFilter()
        cf.itemSimilarity(train, punish=True)

        start = time.time()
        ru = dict()
        for user, productList in train.items():
            if user.find('dev') >=0:
                continue
            ru[user]= cf.recommend(productList, self.K)
            #ru[user] = [pid for pid,score in tmp]

        print('detailPage recommend cost time: %f' % (time.time() - start))
        return ru
  
