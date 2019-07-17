#!/usr/bin/env python
# coding=utf-8
import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from models import redisop
import baseoperator.new as new
from configparser import ConfigParser
import time

if __name__ == '__main__':
    curPath = os.path.abspath(os.path.dirname(__file__))
    configPath = os.path.join(curPath, '../config/config.ini')
    config=ConfigParser()
    config.read(configPath)
    redishost = config.get("REDIS", "host")
    redispassword = config.get("REDIS", "password")
    port = config.get("REDIS", "port")
    key = "PRODUCT_ONSALE_ES"
    redisObj = redisop.RedisOperator(redishost, port, redispassword, 7)

    while 1:
        try:
            res = redisObj.outQueue(key)
            if res:
                start = time.time()
                tic = lambda: 'at %1.1f seconds' % (time.time() - start)
                print("current time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                #displayOperator = display.DisplayOperator(config)
                displayOperator = new.NewProductOperator(config, "new")
                displayOperator.run()
                print('Ended Polling: %s' % tic())
                print("success")
        except Exception as e:
            print("excute the script error,error=%s"%str(e))
        finally:
            print("waiting...")
            time.sleep(5)
