from typing import Dict,List
import model.redistool as redis
import model.mysql as mysql
import model.productCate as productCate

from collections import deque
from collections import defaultdict

CategoryDic = productCate.GetProductCategoryDict()

def filterPids(pids:List[str])->List[str]:
    #过滤无效pid
    outPids=list()
    for pid in pids:
        if pid in CategoryDic.keys():
            outPids.append(pid)
    return outPids

def GetHotProductsDict()->Dict[str,List[str]]:
    '''获取redis中全部热门商品id'''
    pidDict=dict()
    #获取热门商品id
    for i in range(10):
        pidRedisResult=redis.conn.get('hotproduct:'+str(i))
        if pidRedisResult !=None:
            pidStr=str(pidRedisResult,encoding='utf-8')
            pidArr=pidStr.split(',')
            pidDict[str(i)]= filterPids(pidArr)
    return pidDict

#合并召回商品id和热门商品id
def AddHotProducts(uid:str,pids:List[str])->List[str]:
    gid=str(int(uid)%10)
    if gid not in hpDict:
        return pids

    pids=pids+hpDict[gid]
    mergedPids=list(set(pids))#去重
    mergedPids.sort(key=pids.index) #按源顺序重排
    return mergedPids

class PRQueue:
    def __init__(self, products, page):
        self.work = deque(products)
        self.buff = deque()
        self.page = page

    def PopOne(self):
        if len(self.work) > 0:
            return self.work.popleft()
        return -1

    def PopPage(self):
        page = self.page
        workLen = len(self.work)
        if workLen < page:
            page = workLen
        tmp = list()
        for i in range(page):
            tmp.append(self.work.popleft())
        return tmp

    #插入不符合目标值的，从左向右
    def PushBuff(self, pid):
        self.buff.append(pid)
    #从列表里剔除的，从右向左
    def PushBuffLeft(self, pid):
        self.buff.appendleft(pid)
        
    def ReBuild(self):
        #buff从右弹出，work从左插入
        bufLen = len(self.buff)
        for i in range(bufLen):
            self.work.appendleft(self.buff.pop())
            #item = self.buff.pop()
            #print(item)
            #self.work.appendleft(item)

#倒序获取不符合规则id,
#不存在返回值targetCid=0，targetPid=0 ，如果有必然错了
#1 取最多的分类,
#2 优先级低的分类
def Match(dic,products, big=True):
    targetCid = 0
    num = 0
    for k,v in dic.items():
        if v > num:
            num = v
            targetCid = k
    targetPid = 0
    for id in products[::-1]:
        cid = 0
        if big:
            cid = CategoryDic[id].root_cid
        else:
            cid = CategoryDic[id].cid
        if cid == targetCid:
            targetPid = id
            break

    return targetPid,targetCid

#最后一页不满足规则的，直接丢掉
def VarietyReRank(uid:str,pids:List[str])->List[str]:
    pids = filterPids(pids)
    products = AddHotProducts(uid, pids)
    final = list()
    size = 8
    maxLen = 160

    pr = PRQueue(products, size)
    while True:
        if len(final)>= maxLen:
            break
        tmp = pr.PopPage()
        if len(tmp) < size:
            break

        #统计大分类
        big = defaultdict(int)
        for i in tmp:
            big[CategoryDic[i].root_cid] +=1
        #大分类规则匹配,至少2个大分类
        if len(big) <=1:
            rmPid,rmCid = Match(big, tmp)
            #print('big rm: id=%s, cid=%s\n'%(rmPid,rmCid))
            tmp.remove(rmPid)
            big[rmCid] -=1
            pr.PushBuffLeft(rmPid) #下一页的第一个位置
            while True:
                id = pr.PopOne()
                if id == -1:
                    return final
                cid = CategoryDic[id].root_cid #大分类
                #print('chang: id=%s,big cid=%s'%(id, cid))
                if cid != rmCid:
                    tmp.append(id)
                    break
                else:
                    pr.PushBuff(id)
        #队列重排
        pr.ReBuild()

        #统计小分类
        small = defaultdict(int)
        for i in tmp:
            small[CategoryDic[i].cid] +=1
        #小分类匹配, 至少4个小分类
        while len(small) < 4:
            rmPid, rmCid = Match(small, tmp, big=False)
            #print('small rm: id=%s, cid=%s\n'%(rmPid,rmCid))
            tmp.remove(rmPid)
            small[rmCid] -=1
            pr.PushBuffLeft(rmPid) #下一页的第一个位置
            while True:
                id = pr.PopOne()
                if id == -1:
                    return final
                cid = CategoryDic[id].cid#小分类
                #print('chang: id=%s,small cid=%s'%(id, cid))
                if cid != rmCid:
                    tmp.append(id)
                    small[cid] +=1
                    break
                else:
                    pr.PushBuff(id)
        #队列重排
        pr.ReBuild()

        '''##test
        for id in tmp:
            o = CategoryDic[id]
            print('pid=%s,root=%s, cid=%s'%(id, o.root_cid, o.cid))
        print('\n\n\n')
        '''##end
        #extend 
        final.extend(tmp)
    
    return final
'''    
def VarietyResort(uid:str,pids:List[str])->List[str]:
    # 穿插排序，每8个商品需要属于2个大分类和4个小分类
    #合并召回商品id和热门商品id
    gid=str(int(uid)%10)
    mergedPids=list()
    if hpDict[gid]!=None:
        pids=pids+hpDict[gid]
        mergedPids=list(set(pids))
        mergedPids.sort(key=pids.index)
    pageRange=int(len(mergedPids)/8)
    if pageRange>20:
        pageRange=20
    # print('mergedPids',len(mergedPids))
    #获取排序所需分类数据 
    mergedPids=filterPids(mergedPids)
    # print('mergedPids',len(mergedPids))
    #穿插排序取最多160个商品
    endIndex=160
    for i in range(pageRange):
        bigCategoryDict=dict()
        smallCategoryDict=dict()
        batchIndex=0
        for j in range(i*8,len(mergedPids)):
            #统计每8个一组的商品大分类和小分类数量
            bigCID=product.PRDict[mergedPids[j]].root_cid
            cid=product.PRDict[mergedPids[j]].cid
            if batchIndex<8:
                batchIndex+=1
                if bigCID in bigCategoryDict.keys():
                    bigCategoryDict[bigCID].append(j)
                else:
                    bigCategoryDict[bigCID]=[j]

                if cid in smallCategoryDict.keys():
                    smallCategoryDict[cid].append(j)
                else:
                    smallCategoryDict[cid]=[j]
                continue
            # print('---------',i,'--------------')
            # print(bigCategoryDict)
            # print(smallCategoryDict)
            # print(len(bigCategoryDict))
            # print(len(smallCategoryDict))
            #前8个满足，结束当前内层循环
            if len(bigCategoryDict)>=2 and len(smallCategoryDict)>=4:
                break
            elif len(bigCategoryDict)==1:#只有一个大分类，优先满足2个大分类
                if bigCID in bigCategoryDict.keys():
                    continue
                #当小分类满足条件时，将当前商品id与第8个商品id进行交换
                if len(smallCategoryDict)>=4:
                    #合并顺延
                    changeA=mergedPids[i*8+7]
                    changeB=mergedPids[j]
                    # mergedPids=mergedPids[:i*8+7]+changeB+mergedPids[i*8+8:j]+changeA+mergedPids[j+1:]
                    mergedPids[i*8+7],mergedPids[j]=mergedPids[j],mergedPids[i*8+7]
                    bigCategoryDict[bigCID]=[j]
                    continue
                # print('bigCID:',bigCID)
                #当小分类小于4时，从后往前遍历，将当前商品id与第一个商品所在小分类数量>1的商品id进行交换
                for k in range((i+1)*8-1,i*8-1,-1):
                    # print('k',k)
                    tmpCID=product.PRDict[mergedPids[k]].cid
                    # print('tmpCID',tmpCID)
                    if len(smallCategoryDict[tmpCID])>1:
                        mergedPids[k],mergedPids[j]=mergedPids[j],mergedPids[k]
                        for bigKey in bigCategoryDict.keys():
                            bigCategoryDict[bigKey].pop()
                            break
                        bigCategoryDict[bigCID]=[j]
                        smallCategoryDict[tmpCID].pop()
                        smallCategoryDict[cid]=[j]
                        break
            else:#小分类不满足条件,从后往前遍历，将当前商品id与第一个商品所在小分类数量>1的商品id进行交换
                if cid in smallCategoryDict.keys():
                    continue
                # print('cid:',cid)
                for k in range((i+1)*8-1,i*8-1,-1):
                    # print('k',k)
                    tmpCID=product.PRDict[mergedPids[k]].cid
                    # print('tmpCID',tmpCID)
                    if len(smallCategoryDict[tmpCID])>1:
                        mergedPids[k],mergedPids[j]=mergedPids[j],mergedPids[k]
                        smallCategoryDict[tmpCID].pop()
                        smallCategoryDict[cid]=[j]
                        break
        #遍历到结束仍不满足，结束循环，返回结束位置
        if len(bigCategoryDict)<2 or len(smallCategoryDict)<4:
            endIndex=(i-1)*8
            break
    if endIndex<80:
        return list()
    return mergedPids[:endIndex]
'''

#全局热门商品数据    
hpDict=GetHotProductsDict()
# print('-----------------------')
# print('热门商品ID')
# for key in hpDict.keys():
#     print(key)
#     print(','.join(hpDict[key]))
# print('-----------------------')

if __name__ == '__main__':
    uid='4002'
    pids=['1183745']
    print(len(product.PRDict))
    result=VarietyResort(uid,pids)
    print(len(result))
    print(result)

