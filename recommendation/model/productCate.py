import model.mysql as db
from typing import Dict

class ProductResort:
    def __init__(self, pid, cid,root_cid):
        self.pid = pid
        self.cid = cid
        self.root_cid=root_cid

CategoryDic=dict()
def BuildProductResort():
    '''获取穿插排序需要的商品分类信息'''
    mark=0
    while True:
        statement="SELECT a.id pid,a.category_id cid,left(b.path, LOCATE('.',b.path)-1) root_cid  FROM t_product a left join t_category  b on a.category_id=b.id  WHERE a.id>%d and a.visible=1 and a.in_offline_shop=0 and a.status in ('onsale','sold') and a.category_id>0 order by a.id limit %d"%(mark,db.Limit)
        results = db.SelectFetchAll(statement)
        if len(results)==0:
            break
        else:
            mark = results[-1]['pid']
            #print mark
        for row in results:
            CategoryDic[str(row['pid'])]=ProductResort(str(row['pid']),row['cid'],row['root_cid'])

def GetProductCategoryDict():
    if len(CategoryDic) <=0:
        BuildProductResort()       
    return CategoryDic

