# coding: utf-8 -*-

import model.mysql as db

testTable = db.getExperiment()
def isTest(uid, code='recommendIndex'):
    #print (testTable)
    uid = int(uid)
    exp = testTable[code]

    #未分组，内部人员查看
    if str(uid) in exp['uids']:
        return True

    if exp['group'] <= 0:
        return False
    #exp['group'] = 10
    if uid <=0:
        return False
    r = 100/exp['group']
    if uid%r == 0:
        return True

    #A组是实验组 B组是对照组(大多数在这个组)
    return False

