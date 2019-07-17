# coding=utf-8

import MySQLdb
from collections import defaultdict

mysqlhost = 'pc-2ze47js669jk7k187.rwlb.rds.aliyuncs.com'
mysqlusername = 'plumdb'
mysqlpasswd = 'plumdb@2019'
dbname='aplum_user_profile'

db = MySQLdb.connect(mysqlhost, mysqlusername,  mysqlpasswd, dbname, charset='utf8')
_cursor = db.cursor(cursorclass = MySQLdb.cursors.DictCursor)
_cursor.execute("set names utf8")

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
    _cursor.close()
    db.close()

