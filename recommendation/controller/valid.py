# coding: utf-8 -*-
import model.redistool as r
import model.show as show
import math
#param num:  redis存储的，商品推荐个数
num=160

#onsale: onsale product set   
#param cartDic:       key=user_id ,value[[product_id, score], ...]
#param detailPageDic: key=user_id ,value[[product_id, score], ...]
def userCF(days, onsale, cartDic, detailPageDic):
    userDic = dict()
    users = set(cartDic.keys()) | set(detailPageDic.keys())

    #get user product show
    showDic= show.loadBatchShow(days,list(users), 10)

    n0_80 = 0
    for user in users:
        tmp = list() #total recommend
        if user in cartDic and user in detailPageDic:
            cartLen = len(cartDic[user])
            detailLen = len(detailPageDic[user])
            for i in range(cartLen+detailLen):
                if i < detailLen:
                    tmp.append(detailPageDic[user][i])
                if i < cartLen:
                    tmp.append(cartDic[user][i])
        else: 
            if user in detailPageDic:
                tmp += detailPageDic[user]
            if user in cartDic:
                tmp += cartDic[user]

        pids = list()
        pidSet = set()
        for pid,score in tmp:
            if pid not in onsale:
                continue
            if pid in pidSet:
                continue
            if pid in showDic[user]:
                score = score*(1/math.log(showDic[user][pid],5))
            pids.append([pid,score])
            pidSet.add(pid)
            if len(pids)>num:
                break
        if len(pids) >= num/2:
            userDic[user] = pids
        else:
            n0_80 +=1

    print ("      total users: %d"%len(users))
    print ("cf final users(>=80): " + str(len(userDic)))
    print ("cf less than 80users: " + str(n0_80))

    return userDic


#param userDic: key=user_id ,value[[product_id, score], ...]
def remove(userDic):
   for user in userDic.keys():
       products = userDic[user]
       userDic[user] = [pid for pid,score in products]

   return userDic

#抽取用户，查看商品变化
def changeState(userDic):
    users = list(userDic.keys())
    step = 100
    loop = int(len(users)/step)

    validCount = 0
    changeCount16 = 0
    changeCount32 = 0
    for i in range(loop):
        index = i*step
        uid = users[index]
        key = 'cf:' + uid
        if r.conn.exists(key):
            validCount +=1
            value = str(r.conn.get(key))
            oldProducts = value.split(',')
            newProducts = userDic[uid]
            change = True
            for i in range(32):
                if oldProducts[i] == newProducts[i]:
                    change = False
                    break
                if i == 16:
                    changeCount16 +=1
            if change:
                changeCount32 +=1
    
    print('totalCount=%d, step=%d, validCount=%d'%(len(users), step, validCount))
    print('changeCount16=%d, changeCount32=%d'%(changeCount16, changeCount32))


