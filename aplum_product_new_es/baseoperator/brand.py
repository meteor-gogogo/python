#!/usr/bin/env python
# coding=utf-8
from .baseop import BaseOperator
from utils import output
from collections import defaultdict
import time

class BrandOperator(BaseOperator):

    @classmethod
    def is_registrar_for(cls, excutetype):
        return excutetype == 'brand'

    def getOnsaleCount(self):
        print("start get the number of onsale-product in each brand")
        bidCount = defaultdict(int)
        bid = 0
        while(True):
            sql = "SELECT brand_id,COUNT(*) as total FROM t_product WHERE brand_id>%d and status in ('onsale','sold') and visible=1 GROUP BY brand_id ORDER BY brand_id asc limit 500"%bid
            results = self.mysqloperator.getResults(sql)
            if len(results) == 0 :
                break
            else:
                bid = results[-1]['brand_id']
            for row in results:
                bidCount[int(row['brand_id'])] = int(row['total'])
        return bidCount

    def run(self):
        bidCount = self.getOnsaleCount()
        #print('total=%d, lv=%d,other=%d'%(len(bidCount),bidCount[327], bidCount[9999]))
        print("start process Index, delete targetIndex, then create targetIndex")
        currentIndex, targetIndex = self.esoperator.operatorindex()
        print("batch get t_brand data")
        id = 0
        while (True) :
            sql ="SELECT id,name,alias_name FROM t_brand b WHERE id>%d and acceptance>0 order by id asc limit 1000" % int(id)
            results = self.mysqloperator.getResults(sql)
            if len(results) == 0 :
                break
            package = []
            for row in results:
                bid = int(row['id'])
                esrow = {
                    "bid": bid,
                    "brand_name" : str(row['name']),
                    "brand_alias_name": str(row['alias_name']),
                    "brand_sug": {"input":str(row['name']),"weight": bidCount[bid]},
                    "brand_name_sug": {"input":str(row['alias_name']),"weight": bidCount[bid]}
                }
                package.append( esrow )
            #批量插入es
            actions = [
                {
                    '_index': targetIndex,  #index
                    '_type': "brand",  #type
                    '_source': d
                }
                for d in package
            ]
            print("batch insert data to es")
            success, _ = self.esoperator.bulkInsertData(actions)
            id = results[len(results)-1]['id']
            print(id, " success:", success)
            time.sleep(1)

        print("change alias name")
        self.esoperator.changeAliasName(currentIndex, targetIndex)

