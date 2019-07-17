#!/usr/bin/env python
# coding=utf-8
import baseoperator.baseop as baseop
import baseoperator.new as new
import baseoperator.brand as brand
import baseoperator.category as category
import baseoperator.activity as activity
import baseoperator.product as product
import baseoperator.productincrease as productincrease
import baseoperator.activityincrease as activityincrease
import baseoperator.activityproductincrease as activityproductincrease

class BaseRun(object):

    def __init__(self, excutetype, config):
        self.excutetype = excutetype
        self.config = config

    def excuteRun(self):
        for cls in baseop.BaseOperator.__subclasses__():
            #print('===', self.excutetype,  cls,  cls.is_registrar_for(self.excutetype))
            if cls.is_registrar_for(self.excutetype):
                cls(self.config, self.excutetype).run()

    def newRun(self):
        newopObj = new.NewProductOperator(self.config, self.excutetype)
        newopObj.run()

    def brandRun(self):
        brandObj = brand.BrandOperator(self.config, self.excutetype)
        brandObj.run()

    def categoryRun(self):
        categoryObj = category.CategoryOperator(self.config, self.excutetype)
        categoryObj.run()

    def activityRun(self):
        activityopObj = activity.ActivityProductOperator(self.config, self.excutetype)
        activityopObj.run()

    def productRun(self):
        productopObj = product.ProductOperator(self.config, self.excutetype)
        productopObj.run()

    def productincreaseRun(self):
        productincreaseopObj = productincrease.ProductIncreaseOperator(self.config, self.excutetype)
        productincreaseopObj.run()

    def activityincreaseRun(self):
        activityincreaseObj = activityincrease.ActivityIncreaseOperator(self.config, self.excutetype)
        activityincreaseObj.run()

    def activityproductincreaseRun(self):
        activityproductincreaseObj = activityproductincrease.ActivityProductIncreaseOperator(self.config, self.excutetype)
        activityproductincreaseObj.run()
