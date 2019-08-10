import pandas as pd
import numpy as np

if __name__ == '__main__':
    # url = 'https://raw.githubusercontent.com/justmarkham/DAT8/master/data/chipotle.tsv'
    chipo = pd.read_csv('C:\\MyWork\\code\\pandas_exercise\\data\\data.csv', sep='\t')
    # chipo.to_csv('C:\\MyWork\\code\\pandas_exercise\\data\\data.csv', sep='\t')
    c = chipo.groupby('item_name')
    print(c.head)