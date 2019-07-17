# coding=utf-8
import numpy as np
import pandas as pd


if __name__ == '__main__':
    # 读取excel中的数据
    df = pd.DataFrame(pd.read_excel('C:\\Users\\liuhang\\Desktop\\plum\\爬虫\\心上首页推荐商品信息.xlsx'))
    # 根据sellerId字段分组，汇总个数，再转成DataFrame
    df1 = pd.DataFrame(df.groupby('sellerId')['goodsId'].count())
    # print(df1.head(5))
    # 根据sellerId字段分组，求和salePrice字段，再转成DataFrame
    df2 = pd.DataFrame(df.groupby('sellerId')['salePrice'].agg(np.sum))
    # 两个DataFrame进行join，连接条件为sellerId相等
    result = df1.join(df2, on='sellerId')
    result.to_excel('C:\\Users\\liuhang\\Desktop\\plum\\爬虫\\心上首页推荐商品卖家信息.xlsx')
