# coding=utf-8

import MySQLdb
from collections import defaultdict

#mysqlhost = 'rm-2zeixwnpc34127h5f191-vpc-rw.mysql.rds.aliyuncs.com'
mysqlhost = 'rr-2ze7824567d8dgu1y.mysql.rds.aliyuncs.com'
mysqlusername = 'plumdb'
mysqlpasswd = 'plumdb@2018'
dbname='aplum'

db = MySQLdb.connect(mysqlhost, mysqlusername,  mysqlpasswd, dbname, charset='utf8')
_cursor = db.cursor(cursorclass = MySQLdb.cursors.DictCursor)
_cursor.execute("set names utf8")

Limit=3000
def SelectFetchAll(sql):
    _cursor.execute(sql)
    return _cursor.fetchall()

def InsertOrUpdate(sql):
    try:
        _cursor.execute(sql)
        db.commit()
        return True
    except:
        db.rollback()
        return False

def close():
    #print _cursor
    #print db
    _cursor.close()
    db.close()

def getExperiment():
    exp = dict()
    sql = 'SELECT t.name,t.code,t.start_time,t.end_time,t.strategy_data FROM t_experiment t limit 100' 
    results = SelectFetchAll(sql)
    for res in results:
        code = res['code']
        tmp = ''
        if res['strategy_data'].find('\r\n'):
            tmp = res['strategy_data'].split('\r\n')
        else:
            tmp = res['strategy_data'].split('\n')
        if len(tmp)<2:
            continue
        res['group'] = int(tmp[0].split('=')[1])
        res['uids'] = tmp[1].split('=')[1]
        exp[code] = res

    return exp
    
