# coding=utf-8
import math

def ctrAlpha(mean, var):
    if var <= 0 or mean <= 0 or var>1 or mean>1:
        return 0
    return mean*( (mean*(1-mean)/math.pow(var,2) )- 1)

def ctrBeta(mean, var):
    if var <= 0 or mean <= 0 or var>1 or mean>1:
        return 0
    return (1-mean)*( ( mean*(1-mean)/math.pow(var,2) )- 1)

def smooth(showCount, clickCount, alpha, beta):
    if alpha <= 0 or beta<=0:
        return 0

    return (clickCount+alpha)/(showCount+alpha+beta)

