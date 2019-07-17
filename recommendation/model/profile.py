# coding=utf-8
import model.profileDB as db
from collections import defaultdict


def batch(values):
    _everycount = 1000
    vLen = len(values)
    times = int(vLen/_everycount)+1
    for t in range(times):
        start = t*_everycount
        end = (t+1)*_everycount
        if end>= vLen:
            end = vLen
        tmp = []
        for v in values[start:end]:
            vstr = str(v) 
            if vstr.isdigit() is False:
                continue
            tmp.append(vstr)

        yield ','.join(tmp)

def LoadUserProfile(uidList):
   print('load user profile')
   dic = {} 
   for b in batch(uidList):
       sql ="select t.userid,t.category,t.catbrdprice,t.complet from userprofile t where t.userid in (%s)"%(b)
       #print (sql)
       results = db.SelectFetchAll(sql)
       for row in results:
           tmp = dict()
           tmp['category'] = row['category']
           tmp['catbrdprice'] = row['catbrdprice']
           tmp['complet'] = row['complet']
           dic[str(row['userid'])] = tmp
      
           #print('\n'.join(['%s:%s' % item for item in tmp.items()]))

    
   #print('\n'.join(['%s:%s' % item for item in dic.items()]))
   print ('profile len = %d\n'%len(dic))
   return dic
