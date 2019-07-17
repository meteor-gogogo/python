#!/usr/bin/env python
# coding=utf-8
from .baseop import BaseOperator
from utils import tools
from utils import output, scoreUtil
import library.rpc.client as rpc
import time
import traceback
import sys

class ActivityProductIncreaseOperator(BaseOperator):

    @classmethod
    def is_registrar_for(cls, excutetype):
        return excutetype == 'activityproductincrease'

    def initRpc(self):
        host = self.config.get("THRIFT", "host")
        port = self.config.get("THRIFT", "port")
        self.rpc = rpc.RpcClient(host,port)

    def run(self):
        self.initRpc()
        currentIndex = self.esoperator.getAliasIndex()
        print("get current index: ", currentIndex)
        print("activityproductincrease, get increase last time")
        timestamp = self.redisoperator.getValue("last_timestamp")
        print(timestamp)
        maxupdatetime = ""
        id = 0
        while (True) :
            sql = "SELECT p.id, p.name, p.user_id as seller_id,p.customer_type,p.brand_id, p.cooperate_type,p.dispatch_type,p.category_id, p.discount_price,p.size,p.original_price,p.sale_price, p.purchase_price, p.last_update,p.discount_rate, p.condition_level, p.status, p.in_offline_shop,p.onsale_time,p.blackcard_discount,p.provider,p.visible,group_concat(ap.activity_id,':', ap.order SEPARATOR ',') as activityinfo, p.last_update, p.product_update_time FROM t_product p,t_activity_product ap WHERE  p.id = ap.product_id AND p.product_update_time > '%s' and p.id > %d  group by p.id order by p.id limit 1000" % (str(timestamp), int(id))
            results = self.mysqloperator.getResults(sql)
            if len(results) == 0 :
                break
            #ctr-score--begin-----
            ctrScore = {}
            for row in results:
                pid = int(row['id'])
                try:
                    ctrScore[pid] = scoreUtil.GetFeatureScore(self.rpc.client, row, "")
                    #print('act_pro_inc_test: pid=%d'%pid, ctrScore[pid])
                except Exception as e:
                    msg = traceback.format_exc()
                    ctrScore[pid] = 0.02
                    print ("%d ctr_sort_act_product_inc, error:"%pid, msg)
                    continue
            #ctr-score--end-------
            package = []
            for row in results:
                id = int(row['id'])
                esrow = {
                    "id": id,
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
                if row['activityinfo'] :
                    nestarr = row['activityinfo'].split(',')
                    for nestinfo in nestarr:
                        activityid, order = nestinfo.split(":")
                        if int(activityid) in scoreUtil.sysActID:
                            nestdic = {
                                "id": int(activityid),
                                "order": float(order),
                                "ctr": ctrScore[id]
                            }
                        else:
                            nestdic = {
                                "id": int(activityid),
                                "order": float(order)
                            }
                        nestlist.append(nestdic)
                esrow["activityinfo"] = nestlist
                package.append( esrow )
            #批量插入es
            actions = [
                {
                    '_index': currentIndex,  #index
                    '_type': "product",  #type
                    '_id':d['id'],
                    '_source': d
                }
                for d in package
            ]
            print("batch insert data to es")
            success, _ = self.esoperator.bulkInsertData(actions)
            nextlasttimestamp = results[len(results)-1]['product_update_time']
            id = results[len(results)-1]['id']
            maxupdatetime = tools.getmaxupdatetime(results, maxupdatetime)
            print(id, '----', nextlasttimestamp, '------last_timestamp:', maxupdatetime, '-----timstamp:', str(timestamp), '-----success:', success)
            time.sleep(0.3)
        if not maxupdatetime :
            print("waiting...")
        else:
            print("最后的更新时间为:", maxupdatetime)
            self.redisoperator.setValue('last_timestamp', maxupdatetime)
