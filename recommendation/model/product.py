# coding=utf-8
import model.mysql as db
import model.sensors as se
import model.es as es
import re
from collections import defaultdict

class Product(object):
    def __init__(self,pid, bid, cid, discountPrice):
        self.id = pid
        self.status = ''
        self.name = ''
        self.brand = bid
        self.category = cid
        self.discountPrice = discountPrice
        self.attr = defaultdict(str)

_productDic={}
def getProductDic():
    if len(_productDic) == 0:
        construct()

    return _productDic

def construct():
    #print ('product construct')
    pidList = []
    #mark=2118632
    mark=0
    while True:
        statement="SELECT t.id,t.status,t.brand_id,t.category_id,t.discount_price FROM t_product t WHERE t.id>%d and t.visible=1 and t.in_offline_shop=0 and t.status in ('onsale') order by t.id limit %d"%(mark,db.Limit);
        results = db.SelectFetchAll(statement)
        if len(results)==0:
            break
        else:
            mark = results[-1]['id']
            #print mark
        for p in results:
            #id = int(p['id'])
            id = str(p['id'])
            pidList.append(id)
            obj = Product(id,p['brand_id'], p['category_id'], p['discount_price'])
            obj.status = p['status']
            _productDic[id] = obj        
        #break

    return pidList          

def batchProduct(values, everycount):
    vLen = len(values)
    times = int(vLen/everycount)+1
    for t in range(times):
        start = t*everycount
        end = (t+1)*everycount
        if end>= vLen:
            end = vLen
        yield ",".join(values[start:end])
        
def getProductDicByID(pidList):
    productDic = dict()
    count = 2000
    for batch in batchProduct(pidList, count):
        sql="SELECT t.id,t.status,t.brand_id,t.category_id,t.discount_price FROM t_product t WHERE t.id in (%s)"%(batch);
        results = db.SelectFetchAll(sql)
        for p in results:
            id = str(p['id'])
            obj = Product(id,p['brand_id'], p['category_id'], p['discount_price'])
            obj.status = p['status']
            productDic[id] = obj        
            #print('\n'.join(['%s:%s' % item for item in obj.__dict__.items()]))

    return productDic

_productCtrDic={}
def calcCtr(Days, pidList):
    if len(_productCtrDic) > 0:
        return _productCtrDic
    #key=pid(string), value=count
    productPageDic = se.loadProductPage(Days, pidList)
    #print (len(productPageDic))
    #print (type(list(productPageDic.keys())[0]))  #== string
    productViewDic = es.loadProductView(Days, pidList)
    #print (len(productViewDic))
    #print (type(list(productViewDic.keys())[0])) #== string

    #key=pid(string), value=ctr
    for p in pidList:
        if p in productPageDic.keys() and p in productViewDic.keys():
            pageCount = productPageDic[p]
            viewCount = productViewDic[p]
            if viewCount > 0:
                _productCtrDic[p] = float(pageCount)/float(viewCount)

    return _productCtrDic

def getProductAttr(pid):
    attrDic= dict()
    statement="select tp.product_id, ta.name,tp.value_text from t_product_attr_value tp join t_attr_and_value ta on tp.attr_id = ta.id where tp.product_id=%d"%(int(pid))
    results = db.SelectFetchAll(statement)
    if len(results)>0:
        for p in results:
            key =  p['name']
            value = p['value_text']
            if key != "" and value != "":
                attrDic[key] = value

    return attrDic

color="颜色"
material="材质"
def getProductAttrBatch():
    if len(_productDic) == 0:
        construct()

    for k,v in _productDic.items():
        attr = getProductAttr(k)
        if len(attr)>0:
            if color in attr:
                tmp = attr[color].split(";")[0].strip()
                if len(tmp)>2 and tmp.find('色')>0:
                    tmp = tmp.split(" ")[0]
                    if len(tmp)>2:
                        tmp = re.sub('色.*','',tmp)
                if len(tmp) ==1 and tmp.find('色')== -1:
                    tmp += '色'

                if len(tmp)>0:
                    _productDic[k].attr[color]= tmp.strip()

            if material in attr:
                tmp = attr[material].split(";")[0]
                if tmp.find('；'):
                    tmp = tmp.split("；")[0]
                if len(tmp)>0:
                    tmp = re.sub('[.*\d*(%|%|％)|.*\d]|.*\:|.*/', '', tmp)
                    tmp = re.sub('材质：|面料：', '', tmp)
                    if tmp.find(','):
                        tmp = tmp.split(",")[0]
                    if tmp.find('，'):
                        tmp = tmp.split("，")[0]

                    tmp = tmp.strip()
                    if len(tmp)>2:
                        tmp = tmp.split(' ')[0]

                    if tmp.find('色')==-1 and len(tmp)>0:
                        _productDic[k].attr[material]= tmp.strip()

    return _productDic

