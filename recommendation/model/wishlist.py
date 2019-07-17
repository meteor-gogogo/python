# coding: utf-8 -*-

import model.mysql as db
from collections import defaultdict


MaxWish=300
class Wish:
    def __init__(self, unixtime):
        self.ctime = unixtime

    def loadData(self):
        userData = defaultdict(list)   
        mark=0
        while True:
            statement="SELECT t.id,t.user_id,t.product_id FROM t_wishlist t where t.id >%d and t.create_time>%d order by t.id limit %d"%(mark, self.ctime, db.Limit);
            results = db.SelectFetchAll(statement)
            if len(results)==0:
                break
            else:
                mark = results[-1]['id']
            for res in results:
                uid = str(res['user_id'])
                pid = str(res['product_id'])
                if len(userData[uid])< MaxWish:
                    userData[uid].append(pid)

        return userData 

