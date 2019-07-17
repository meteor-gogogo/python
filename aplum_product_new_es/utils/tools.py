#!/usr/bin/env python
# coding=utf-8

def getmaxupdatetime(A, key):
    maxlist = list()
    for row in A:
        maxlist.append(row['product_update_time'])
    maxupdatetime = max(maxlist)
    newmaxlist = list()
    newmaxlist.append(str(maxupdatetime))
    newmaxlist.append(key)
    maxupdatetime = max(newmaxlist)
    return str(maxupdatetime)

def getmaxactivityid(A, key):
    maxlist = list()
    for row in A:
        maxlist.append(int(row['id']))
    maxactivityid = max(maxlist)
    newmaxlist = list()
    newmaxlist.append(key)
    newmaxlist.append(maxactivityid)
    maxactivityid = max(newmaxlist)
    return int(maxactivityid)
