#!/usr/bin/env python
# coding=utf-8
import MySQLdb

class MysqlOperator(object) :

    def __init__(self, host, user, password, dbname):
        self.host = host
        self.user = user
        self.password = password
        self.dbname = dbname
        self.db = MySQLdb.connect(self.host, self.user, self.password, self.dbname, charset='utf8')
        self.cursor = self.db.cursor(cursorclass = MySQLdb.cursors.DictCursor)
        self.cursor.execute("set names utf8")

    def getResults(self, sql):
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        return results

    def getOneResult(self, sql):
        self.cursor.execute(sql)
        results = self.cursor.fetchone()
        return results

    def __del__( self ):
        self.cursor.close()     #断开游标
        self.db.close()    #断开数据库





