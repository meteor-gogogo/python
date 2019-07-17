#!/usr/bin/env python
# coding=utf-8
from models import newproductesop
from models import activityproductesop
from models import productesop
from models import brandop
from models import categoryop
from models import mysqlop
from models import redisop

class BaseOperator(object):

    def __init__(self, config, excutetype):
        self.config = config
        self.excutetype = excutetype
        self.initEs()
        self.initMysql()
        self.initRedis()

    def initRedis(self):
        redishost = self.config.get("REDIS", "host")
        redispassword = self.config.get("REDIS", "password")
        port = self.config.get("REDIS", "port")
        self.redisoperator = redisop.RedisOperator(redishost, port, redispassword, 3)

    def initEs(self):
        eshost = self.config.get("ELASTICSEARCH", "host")
        esuser = self.config.get("ELASTICSEARCH", "user")
        espasswd = self.config.get("ELASTICSEARCH", "password")
        esport = self.config.get("ELASTICSEARCH", "port")
        if self.excutetype == 'new':
            self.esoperator = newproductesop.NewProductEsOperator(eshost, esuser, espasswd, esport)
        elif self.excutetype == 'brand':
            self.esoperator = brandop.brandEsOperator(eshost, esuser, espasswd, esport)
        elif self.excutetype == 'category':
            self.esoperator = categoryop.CategoryEsOperator(eshost, esuser, espasswd, esport)
        elif self.excutetype in ['activity', 'activityincrease', 'activityproductincrease']:
            self.esoperator = activityproductesop.ActivityProductEsOperator(eshost,esuser, espasswd, esport)
        elif self.excutetype in ['product', 'productincrease']:
            self.esoperator = productesop.ProductEsOperator(eshost, esuser, espasswd, esport)

    def initMysql(self):
        mysqlhost = self.config.get("MYSQL", "host")
        mysqluser = self.config.get("MYSQL", "user")
        mysqlpasswd = self.config.get("MYSQL", "password")
        mysqldbname = self.config.get("MYSQL", "database")
        self.mysqloperator = mysqlop.MysqlOperator(mysqlhost, mysqluser, mysqlpasswd, mysqldbname)

