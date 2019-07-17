#!/usr/bin/env python
# coding=utf-8
from .baseop import BaseOperator
from utils import tools, score, scoreUtil,live
import library.rpc.client as rpc
import traceback
import time
import sys

class ProductIncreaseOperator(BaseOperator):

    @classmethod
    def is_registrar_for(cls, excutetype):
        return excutetype == 'productincrease'

    def initRpc(self):
        host = self.config.get("THRIFT", "host")
        port = self.config.get("THRIFT", "port")
        self.rpcclient = rpc.RpcClient(host,port)

    def run(self):
        self.initRpc()
        print("start process Index, delete targetIndex, then create targetIndex")
        currentIndex = self.esoperator.getAliasIndex()
        print("currentIndex:", currentIndex)
        print("get increase time")
        timestamp = self.redisoperator.getValue("last_aplum_timestamp")
        print(timestamp)
        print("start increasement batch get t_product data")
        maxupdatetime = ""
        id = 0
        while (True) :
            sql = "SELECT p.id, p.name,p.user_id AS seller_id,(case p.customer_type when 'woman' then '女' when 'man' then '男' end) AS sex,p.cooperate_type,p.condition_level,p.condition_desc,p.onsale_time,p.customer_type,p.discount_price,p.discount_rate,p.long_desc,p.size,p.treatment,p.status,p.visible,p.in_offline_shop,p.original_price,p.sale_price,p.purchase_price,p.blackcard_discount,p.provider,b.id AS bid,b.name AS brand_name,b.name_zh AS brand_name_cn,b.alias_name AS brand_alias_name,b.brand_class,b.acceptance,b.region,c.id AS cid,c.group_id,c.name AS category_name,GROUP_CONCAT(distinct  atv.value_text) as attribute,p.last_update as last_update, p.product_update_time FROM t_product p,t_brand b, t_category c,t_product_attr_value atv WHERE p.brand_id = b.id AND p.category_id = c.id  AND p.id = atv.product_id AND p.product_update_time > '%s' and p.id >%d group by atv.product_id order by p.id  limit 1000"% (str(timestamp), int(id))
            results = self.mysqloperator.getResults(sql)
            if len(results) == 0 :
                break
            pidList = []
            for row in results:
                pidList.append(str(row['id']))
            sortSql = scoreUtil.GetProductSortSql(pidList)
            sortResults = self.mysqloperator.getResults(sortSql)
            sortDic = scoreUtil.GetProductSortScore(sortResults, pidList)
            for row in results:
                pid = int(row['id'])
                try:
                    #print('\n'.join(['%s:%s' % item for item in sortDic[pid].__dict__.items()]))
                    scoreUtil.SetSortScore(self.rpcclient.client, row, sortDic[pid])
                except Exception as e:
                    msg = traceback.format_exc()
                    print ("%d ctr_sort_product_increae, error:"%pid, msg)
                    continue
            #get live message
            liveDic = live.GetLiveProduct(self.mysqloperator, pidList)
            #end

            repeatid = ''
            expandname = ''
            package = []
            for row in results:
                id = int(row['id'])
                obj = score.ProductSort(id)
                if id in sortDic.keys():
                    obj = sortDic[id]
                    if row['status'] == 'sold':
                        obj.category_sort_score = 0.0
                if int(row['seller_id']) == 758050 :
                    repeatid  = str(row['id'])
                else:
                    repeatid  =  "%s_%s"%(str(row['seller_id']), str(row['bid']))

                if str(row['condition_level']) == 'brand_new':
                    expandname = "全新 %s"%str(row['name'])
                else:
                    expandname = str(row['name'])

                size = str(row['size'])
                attribute = str(row['attribute'])
                if size.isdigit():
                    attribute = "%s %s"%(attribute,size + "码")

                esrow = {
                    "id": id,
                    "name" : "%s %s"%(expandname, str(row['sex'])),
                    "bid" : int(row['bid']),
                    "cid" : int(row['cid']),
                    "discount_price": int(row['discount_price']),
                    "status" : str(row['status']),
                    "visible": int(row['visible']),
                    "discount_rate": float(row['discount_rate']),
                    "blackcard_discount": int(row['blackcard_discount']),
                    "onsale_time": int(row['onsale_time']),
                    "customer_type": str(row['customer_type']),
                    "repeatid":repeatid,
                    "original_price": int(row['original_price']),
                    "in_offline_shop": int(row['in_offline_shop']),
                    "cooperate_type": str(row['cooperate_type']),
                    "sale_price": int(row['sale_price']),
                    "size": str(row['size']),
                    "purchase_price": int(row['purchase_price']),
                    "seller_id": int(row['seller_id']),
                    "sex": str(row['sex']),
                    "treatment": str(row['treatment']),
                    "acceptance": int(row['acceptance']),
                    "region": str(row['region']),
                    "brand_alias_name": str(row['brand_alias_name']),
                    "long_desc": str(row['long_desc']),
                    "condition_desc": str(row['condition_desc']),
                    "brand_name": str(row['brand_name']),
                    "category_name": str(row['category_name']),
                    "attribute": attribute,
                    "last_update": int(row['last_update']),
                    "condition_level": str(row['condition_level']),
                    "brand_name_cn": str(row['brand_name_cn']),
                    "group_id": int(row['group_id']),
                    "brand_class": str(row['brand_class']),
                    "category_score": obj.category,
                    "sort_score": obj.sort_score,
                    "category_sort_score": obj.category_sort_score,
                    "show_dec_sort": obj.show_dec_sort,
                    "sort_score_ios": obj.sort_score_ios,
                    "sort_score_android": obj.sort_score_android,
                    "live": liveDic[id]
                }
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
            self.redisoperator.setValue('last_aplum_timestamp', maxupdatetime)
