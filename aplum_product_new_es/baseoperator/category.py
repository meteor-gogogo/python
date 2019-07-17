#!/usr/bin/env python
# coding=utf-8
from .baseop import BaseOperator
from utils import output
import time

class CategoryOperator(BaseOperator):

    @classmethod
    def is_registrar_for(cls, excutetype):
        return excutetype == 'category'

    def run(self):
        print("start process Index, delete targetIndex, then create targetIndex")
        currentIndex, targetIndex = self.esoperator.operatorindex()
        print("batch get t_category data")
        id = 0
        while (True) :
            sql ="SELECT id, name FROM t_category b  WHERE id > %d  order by id asc limit 1000" % int(id)
            results = self.mysqloperator.getResults(sql)
            if len(results) == 0 :
                break
            package = []
            for row in results:
                esrow = {
                    "cid":int(row['id']),
                    "category_name" : str(row['name']),
                }
                package.append( esrow )
            #批量插入es
            actions = [
                {
                    '_index': targetIndex,  #index
                    '_type': "category",  #type
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

