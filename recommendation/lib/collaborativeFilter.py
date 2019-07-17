# coding=utf-8

from collections import defaultdict
from operator import itemgetter
import math
import operator
import time

class CollaborativeFilter:
    def __init__(self):
        self.simListCache = defaultdict(list)

    """
    @param train:     训练数据 ｛UserID ：商品列表｝
    @param punish:    是否惩罚活跃用户
    @return:          商品相似度矩阵 {商品ID: ｛商品ID：相似度｝}
    """
    def itemSimilarity(self, train, punish=False):
        print ("calc itemSim, trainLen=%d"%len(train))
        start = time.time()
        #calculate co-rated users between items
        coMatrix = dict()    # 物品两两共现矩阵，两个物品同时被一个人喜欢的人数
        N = defaultdict(int)   # 喜欢物品的人数
        for _, items in train.items():
            for i in items:
                coMatrix.setdefault(i, dict()) 
                N[i] += 1
                for j in items:
                    if i == j:
                        continue
                    coMatrix[i].setdefault(j, 0.0)
                    if punish is False:
                        coMatrix[i][j] += 1
                    else:
                        coMatrix[i][j] += 1 / math.log(1+len(items) * 1.0)
    
        #calculate finial similarity matrix 
        self.simMatrix = defaultdict(dict)
        maxSimItem = defaultdict(float)
        for i, related in coMatrix.items():
            for j, cij in related.items():
                self.simMatrix[i][j] = cij / math.sqrt(N[i] * N[j])
                if maxSimItem[j] < self.simMatrix[i][j]:
                    maxSimItem[j] = self.simMatrix[i][j]

        #ItemCF-Norm
        for i, related in coMatrix.items():
            for j, cij in related.items():
                #print (maxItemSim[j],itemSimMatrix[i][j])
                self.simMatrix[i][j] = self.simMatrix[i][j]/maxSimItem[j]

        print('calc similarity cost time: %f' % (time.time() - start))
        #self.simMatrix = simMatrix

    """
    @param productList:   用户行为的商品列表（比如已加购的商品、已浏览的商品）
    @param K:     查找最相似的商品个数,需要调的参数
    @param num:   返回推荐的商品数，默认值为-1，全部推荐
    @return:      按相似性打分倒序list[ [商品ID(string), 相似性打分情况(float)],... ]
    """
    def recommend(self, productList, K, num = -1):
        r = defaultdict(float)
        pi = 1 #当前假定，用户加购的每个商品，权重pi相同等于1
        productSet = set(productList)
        for pid in productList:
            if pid not in self.simListCache:
                self.simListCache[pid] = sorted(self.simMatrix[pid].items(), \
                        key = itemgetter(1), reverse=True)[0:K]

            for j, itemSim in self.simListCache[pid]:
                if j in productSet:
                    continue
                if int(itemSim*1000000) == 0:
                    continue

                r[j] += pi * itemSim

        if num == -1:
            return sorted(r.items(), key=itemgetter(1), reverse=True)
        else:
            return sorted(r.items(), key=itemgetter(1), reverse=True)[: num]

        #return dict(sorted(r.items(), key=itemgetter(1), reverse=True))
