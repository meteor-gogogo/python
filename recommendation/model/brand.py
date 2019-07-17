# coding=utf-8
import model.mysql as db

class Brand(object):
    def __init__(self, bid, name, nameZH, brandClass):
        self.id = bid
        self.name = name
        self.nameZH = nameZH
        self.brandClass = brandClass

_brandDic= dict()
def constructBrand():
    if len(_brandDic) >0:
        return _brandDic

    mark=0
    while True:
        statement="SELECT t.id,t.name,t.name_zh,t.brand_class FROM t_brand t where t.id>%d order by t.id limit %d"%(mark,db.Limit);
        results = db.SelectFetchAll(statement)
        if len(results)==0:
            break
        else:
            mark = results[-1]['id']
        for p in results:
            id = p['id'] #id is int
            obj = Brand(id,p['name'], p['name_zh'], p['brand_class'])
            _brandDic[id] = obj        
            #print('\n'.join(['%s:%s' % item for item in obj.__dict__.items()]))

    return _brandDic 

