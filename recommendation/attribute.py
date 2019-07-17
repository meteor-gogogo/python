#!/usr/bin/python3
# coding=utf-8

'''
import sys
import model.product as pro
import model.es as es
import model.sensors as se
import lib.weight as w
import lib.day as day
from controller.attr import Attr
from collections import defaultdict
import pickle
import time
import numpy as np
import joblib

def calcAttrPreference(pDic,pids):
    attrColor = defaultdict(int)
    attrMat = defaultdict(int)

    attrDic = dict()
    attrDic[pro.color] = attrColor
    attrDic[pro.material] = attrMat

    for pid in pids:
       if pid in pDic:
           p =pDic[pid]
           for k,v in p.attr.items():
               #if k == pro.color:
               #    print(pid,v)
               attrDic[k][v] +=1

    return attrDic

meanK='mean'
varK='var'
Days=1
if __name__ == '__main__':
    if len(sys.argv) <=1:
        print ("default data %d days"%(Days))
    else:
        Days = int(sys.argv[1])
        print ("calc data %d days"%(Days))

    startRec = time.time()
    print(day.unix2date(startRec))

    pDic = pro.getProductAttrBatch()
    with open('data/productAttr.pickle', 'wb') as f:
       pickle.dump(pDic,f,pickle.HIGHEST_PROTOCOL)
    #with open('data/productAttr.pickle', 'rb') as f:
    #    pDic = pickle.load(f)
    #print (pDic['798'].attr[pro.material])
    print ('onsale product len = %d'%len(pDic))

    clickDic = se.loadUserDetailPage(Days,False) #点击
    #with open('data/click.pickle', 'wb') as f:
    #    pickle.dump(clickDic, f, pickle.HIGHEST_PROTOCOL)
    #with open('data/click.pickle', 'rb') as f:
    #    clickDic = pickle.load(f)
    print('total click = %d'%len(clickDic))

    
    showDic = es.loadBatchView(Days, clickDic.keys()) #曝光
    #with open('data/show.pickle', 'wb') as f:
    #    joblib.dump(showDic, f, pickle.HIGHEST_PROTOCOL)
    #with open('data/show.pickle', 'rb') as f:
    #    showDic = joblib.load(f)
    print('total show = %d'%len(showDic))
    #print ('mid cost time: %f\n' % (time.time() - startRec))


    userDic = defaultdict(dict)
    for t_uid,pids in clickDic.items():
        clickList = clickDic[t_uid]
        if len(clickList)==0:
            continue
        #print ("user=%s, click len = %d"%(t_uid, len(clickList)))
        userClick = calcAttrPreference(pDic, clickList)
        
        showList = showDic[t_uid]
        #print ("user=%s, show len = %d"%(t_uid, len(showList)))
        if len(showList)==0:
            continue
        userShow=calcAttrPreference(pDic,showList)
        
        colorShow = userShow[pro.color]
        matShow = userShow[pro.material]
        
        #calc color attribute
        colorDic = dict()
        colorCTR = []
        #print ('total color count=%d'%len(userClick[pro.color]))
        for color, count in userClick[pro.color].items():
            if color in colorShow:
               attr = Attr(color, pro.color)
               ctr = float(count)/float(colorShow[color])
               if ctr >1:
                   ctr = 1

               colorCTR.append(ctr)
               attr.clickCount = count
               attr.showCount = colorShow[color]
               attr.ctr = ctr
               colorDic[color] = attr
               #print ("%s,%f"%(color,ctr))
               del colorShow[color]
            else:
               pass
               #print ("%s,count=%d,no color show"%(color,count))

        for color,_ in colorShow.items():
            colorCTR.append(0)

        if len(colorCTR)>0:
            mean = np.mean(colorCTR)
            var = np.var(colorCTR)
            #print ("color mean=%f"%(np.mean(colorCTR)))
            #print ("color var=%f"%(np.var(colorCTR)))
            #TODO calc alpha,beta
            if 0<var and var<=1 and 0<mean and mean<=1:
                for k, v in colorDic.items():
                    colorDic[k].alpha = w.ctrAlpha(mean, var)
                    colorDic[k].beta = w.ctrBeta(mean, var)

                #print ("color invalCount=%f"%(invalCount))
                colorDic[meanK] = mean
                colorDic[varK] = var
                userDic[t_uid][pro.color] = colorDic
        
        #calc material attribute
        matDic = dict()
        matCTR = []
        #print ('total material count=%d'%len(userClick[pro.material]))
        for  mat, count in userClick[pro.material].items():
            if mat in matShow:
               attr = Attr(mat,pro.material)
               ctr = float(count)/float(matShow[mat])
               if ctr >1:
                   ctr = 1
               matCTR.append(ctr)
               attr.clickCount = count
               attr.showCount = matShow[mat]
               attr.ctr = ctr
               matDic[mat] = attr
               #print ("%s,%f"%(mat,ctr))
               del matShow[mat]
            else:
               #print ("%s,count=%d,no material show"%(mat,count))
               pass

        for mat,_ in matShow.items():
            matCTR.append(0)

        if len(matDic)>0:
            mean = np.mean(matCTR)
            var = np.var(matCTR)
            #print ("mat mean=%f"%(np.mean(matCTR)))
            #print ("mat var=%f"%(np.var(matCTR)))
            #TODO calc alpha,beta
            if 0<var and var<=1 and 0<mean and mean<=1:
                for k, v in matDic.items():
                    matDic[k].alpha = w.ctrAlpha(mean, var)
                    matDic[k].beta = w.ctrBeta(mean, var)

                #print ("mat invalCount=%f"%(invalCount))

                matDic[meanK] = mean
                matDic[varK] = var
                userDic[t_uid][pro.material] = matDic

        #print('')


    with open('data/userAttr.pickle', 'wb') as f:
        pickle.dump(userDic, f, pickle.HIGHEST_PROTOCOL)
        
    print ('attribute cost time: %f\n\n\n' % (time.time() - startRec))
'''

if __name__ == '__main__':
    print(test)
    '''  
    attrUserCount = 0
    for uid,products in userDic.items():
        TestA = [pid for pid,score in products]
        if ab.isTest(uid):
            attrUserCount +=1
            userDic[uid] = rank.colorAndMateial(uid, products)
    print('valid attr(color and material) count = %d'%attrUserCount)
    '''
