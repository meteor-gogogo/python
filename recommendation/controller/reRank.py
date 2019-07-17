# coding: utf-8 -*-

#import pickle
#import lib.weight as wt
#from controller.attr import Attr
#from operator import itemgetter
#import operator
#from collections import defaultdict
import model.product as pro
import controller.userProfile as up

def AssignPRWeight(userProducts,userProfile):
    cf_w = 0.9
    pr_w = 0.1

    validW = 0
    notValidW = 0
    productDic = pro.getProductDic()
    for uid in userProducts.keys():
        if uid not in userProfile:
            #print('notpr:%s'%uid)
            continue
        tmp = list()
        products = userProducts[uid]
        for pid, score in products:
            if pid not in productDic:
                tmp.append([pid, score])
                continue
            #print(uid, pid, score)
            #print(type(pid),type(uid)) pid & uid is string
            prScore = up.SetWeight(userProfile[uid], productDic[pid])
            w = score * cf_w +  prScore* pr_w
            tmp.append([pid,w])
            #print (pid, score, w)
           
            if prScore >0:
                validW +=1
            else:
                notValidW +=1

        userProducts[uid] = tmp
    print('equalProduct=%d, notEqual=%d'%(validW, notValidW))
    
    return userProducts

'''
with open('data/productAttr.pickle', 'rb') as f:
    proDic = pickle.load(f)

with open('data/userAttr.pickle', 'rb') as f:
    userAttrDic = pickle.load(f)

#uid is string
def colorAndMateial(uid, products):
    if uid not in userAttrDic:
        return products

    u = userAttrDic[str(uid)]
    if pro.color not in u or pro.material not in u:
        return products

    colorDic = u[pro.color]
    matDic = u[pro.material]
    #print(colorDic)
    #print(matDic)
    result = defaultdict(float)
    for pid,score in products:
        if pid not in proDic:
            result[pid] = score
            #print ('not in proDic,id=%s'%pid)
            continue
        pAttr = proDic[pid].attr
        if pro.color not in pAttr or pro.material not in pAttr:
            result[pid] = score
            #print ('in proDic,no color or material,attr id=%s'%pid)
            continue
        pColor = pAttr[pro.color]
        pMat = pAttr[pro.material]
        #print(pid,pColor,pMat)
        w = 0.0
        #calc color weight
        if pColor in colorDic:
            obj = colorDic[pColor]
            w += wt.smooth(obj.showCount, obj.clickCount, obj.alpha, obj.beta)
        else:
            w += colorDic['mean']

        #calc material weight
        if pMat in matDic:
            obj = matDic[pMat]
            w += wt.smooth(obj.showCount, obj.clickCount, obj.alpha, obj.beta)
        else:
            w += matDic['mean']

        if w <= 0:
            result[pid]=score
        else:
            result[pid]= score*0.8 + score*0.2*w

    
    return sorted(result.items(), key=itemgetter(1), reverse=True)
'''
