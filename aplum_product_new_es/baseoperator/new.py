#!/usr/bin/env python
# coding=utf-8
from .baseop import BaseOperator
from utils import output
import time

class NewProductOperator(BaseOperator):

    @classmethod
    def is_registrar_for(cls, excutetype):
        return excutetype == 'new'

    def run(self):
        print("start process Index, delete targetIndex, then create targetIndex")
        currentIndex, targetIndex = self.esoperator.operatorindex()
        print("batch get t_product_new data")
        id = 0
        while (True) :
            sql = "SELECT p.id, p.name, p.user_id as seller_id,p.customer_type,p.brand_id, p.cooperate_type,p.dispatch_type,p.category_id, p.discount_price,p.size,p.original_price,p.sale_price, p.purchase_price, p.last_update,p.discount_rate, p.condition_level, p.status, p.in_offline_shop,p.onsale_time,p.blackcard_discount,p.visible,group_concat(pn.version_id,':', pn.order_in_version SEPARATOR ',') as newinfo  FROM t_product_new as pn , t_product as p WHERE p.id = pn.product_id AND  p.id > %d  group by p.id order by p.id  asc limit 1000"% int(id)
            results = self.mysqloperator.getResults(sql)
            if len(results) == 0 :
                break
            package = []
            for row in results:
                esrow = {
                    "id":int(row['id']),
                    "name" : str(row['name']),
                    "bid" : int(row['brand_id']),
                    "cid" : int(row['category_id']),
                    "discount_price": int(row['discount_price']),
                    "status" : str(row['status']),
                    "visible": int(row['visible']),
                    "discount_rate": float(row['discount_rate']),
                    "blackcard_discount": int(row['blackcard_discount']),
                    "onsale_time": int(row['onsale_time']),
                    "customer_type": str(row['customer_type']),
                    "original_price": int(row['original_price']),
                    "in_offline_shop": int(row['in_offline_shop']),
                    "cooperate_type": str(row['cooperate_type']),
                    "sale_price": int(row['sale_price']),
                    "dispatch_type": str(row['dispatch_type']),
                    "size": str(row['size']),
                    "purchase_price": int(row['purchase_price']),
                    "seller_id": int(row['seller_id']),
                    "condition_level": str(row['condition_level'])
                }
                nestlist = []
                if row['newinfo'] :
                    nestarr = row['newinfo'].split(',')
                    for nestinfo in nestarr:
                        versionid, versionorder = nestinfo.split(":")
                        nestdic = {
                            "versionid":versionid,
                            "versionorder":versionorder
                        }
                        nestlist.append(nestdic)
                        #activityinfo = json.dumps(nestlist)
                        newinfo = nestlist
                esrow["newinfo"] = newinfo
                package.append( esrow )
            #批量插入es
            actions = [
                {
                    '_index': targetIndex,  #index
                    '_type': "product",  #type
                    '_id':d['id'],
                    '_source': d
                }
                for d in package
            ]
            print("batch insert data to es")
            success, _ = self.esoperator.bulkInsertData(actions)
            id = results[len(results)-1]['id']
            print(id, " success:", success)
            time.sleep(0.3)

        print("change alias name")
        self.esoperator.changeAliasName(currentIndex, targetIndex)

