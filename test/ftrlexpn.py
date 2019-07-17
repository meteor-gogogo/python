# coding:UTF-8
# !/usr/bin/python

import configparser
import os
import sys
import gzip
import datetime
# from datetime import datetime
from math import exp, log, sqrt
from pickle import dump, load
import random
from csv import DictReader

from urllib3.connectionpool import xrange


def getnextlog(str1, str2):
    today = datetime.date.today()
    tomorrow = today + datetime.timedelta(days=0)
    return str1 + '/' + tomorrow.strftime('%Y-%m-%d') + '.' + str2


def hashstr(str):
    # s = str.decode("utf-8")
    s = str
    seed = 31
    h = 0
    for c in s:
        h = int(seed * h) + ord(c)
    return h


def pricedeal(str):
    val = int(float(str))

    if val < 500:
        return val / 20
    elif val < 1000:
        return 50 + (val - 500) / 50
    elif val < 5000:
        return 60 + (val - 1000) / 100
    else:
        return 100 + (val - 5000) / 200


class readconfig:

    def __init__(self, filepath=None):
        if filepath:
            configpath = filepath
        else:
            root_dir = os.path.dirname(os.path.abspath('.'))
            configpath = os.path.join(root_dir, "config.ini")
        self.cf = configparser.ConfigParser()
        self.cf.read(configpath)

    def get_para(self, section, param):
        value = self.cf.get(section, param)
        return value


class ftrl_proximal(object):

    def __init__(self, alpha, beta, L1, L2, D, interaction=False):
        self.alpha = alpha
        self.beta = beta
        self.L1 = L1
        self.L2 = L2
        self.D = D
        self.interaction = interaction

        self.n = [0.] * D
        self.z = [0.] * D
        self.w = [0.] * D

    def _indices(self, x):
        for i in x:
            yield i

        if self.interaction:
            D = self.D
            L = len(x)
            for i in xrange(1, L):
                for j in xrange(i + 1, L):
                    yield (i * j) % D

    def predict(self, x):
        alpha = self.alpha
        beta = self.beta
        L1 = self.L1
        L2 = self.L2

        n = self.n
        z = self.z
        w = self.w

        wTx = 0.
        for i in x:
            sign = -1. if z[i] < 0 else 1.

            if sign * z[i] <= L1:
                w[i] = 0
            else:
                w[i] = (sign * L1 - z[i]) / ((beta + sqrt(n[i])) / alpha + L2)

            wTx += w[i]
        return 1. / (1. + exp(-max(min(wTx, 35.), -35.)))

    def update(self, x, p, y):
        alpha = self.alpha

        n = self.n
        z = self.z
        w = self.w

        g = p - y

        for i in self._indices(x):
            sigma = (sqrt(n[i] + g * g) - sqrt(n[i])) / alpha
            z[i] += g - sigma * w[i]
            n[i] += g * g


def logloss(p, y):
    p = max(min(p, 1. - 10e-15), 10e-15)
    return -log(p) if y == 1. else -log(1 - p)


def write_learner(learner, model_save):
    with gzip.open(model_save, "wb") as model_file:
        dump(learner, model_file)


def load_learner(model_save):
    with gzip.open(model_save, "rb") as model_file:
        learner = load(model_file)
    return learner


def data(path, D):
    for line in open(path, 'r'):
        line = line.strip()
        c = line.split(',')
        # print line
        # print c
        y = 0.
        if c[1] == '1':
            y = 1.
        # print c[1], y
        c[2] = c[2][6:]

        x = [0]

        for i in range(1, len(c) - 1):
            index = abs(hashstr(str(i) + '_' + str(i) + '_' + c[i + 1])) % D
            x.append(index)
        yield x, y


def data1(path, D):
    #DictReader对读进来的数据生成一个字典,key是字段名的列表,value是值
    #enumerate是给一个可迭代的对象加个索引 , t为索引,row为原来数据
    for t, row in enumerate(DictReader(open(path))):

        #sid,is_click,before_dev_id,after_user_id,is_before_or_after,
        # pid,os,terminal_type,city,discount_rate,bid,cid,source_of_supply,degree,
        # is_promotion,original_price,sale_price,discount_price,is_blackcard_member,ip,position,
        # timestamp,dayOfWeek,isWeekend,hour,cid_1,cid_2,Status

        # process id
        del row['sid'] #解除对sid的引用
        del row['before_dev_id']
        # del row['after_user_id']
        del row['is_before_or_after']
        # del row['pid']
        # del row['os']
        # del row['terminal_type']
        del row['city']
        del row['ip']
        del row['timestamp']
        # del row['dayOfWeek']
        # del row['isWeekend']
        del row['position']
        # del row['is_blackcard_member']
        # del row['is_promotion']
        # del row['sale_price']
        # del row['source_of_supply']
        # del row['degree']
        del row['cid_1']
        del row['isWeekend']
        del row['dayOfWeek']
        del row['hour']
        del row['cid_2']
        # if row['after_user_id'] == '000':
        #    continue;
        if row['after_user_id'] == '':
            row['after_user_id'] = 'null'
        # process clicks
        y = 0.
        if 'is_click' in row:
            if row['is_click'] == '1':
                y = 1.
            del row['is_click']

        '''
        val = int(int(float(row['original_price'])) / 100)
        if val == 0:
           val = 1
        row['original_price'] = str(float(val) / 10 )

        val = int(int(float(row['sale_price'])) / 100)
        if val == 0:
           val = 1
        row['sale_price'] = str(float(val) / 10 )

        val = int(int(float(row['discount_price'])) / 100)
        if val == 0:
           val = 1
        row['discount_price'] = str(float(val) / 10 )
        '''
        row['original_price'] = str(pricedeal(row['original_price']))
        # print(row['original_price'])
        row['sale_price'] = str(pricedeal(row['sale_price']))
        row['discount_price'] = str(pricedeal(row['discount_price']))

        x = [0]
        index = abs(hashstr('zuhe1_' + row['bid'] + row['original_price'])) % D
        x.append(index)
        index = abs(hashstr('zuhe2_' + row['bid'] + row['discount_price'])) % D
        # print('zuhe2_'+row['bid']+row['discount_price'])
        x.append(index)
        index = abs(hashstr('zuhe3_' + row['after_user_id'] + row['cid'])) % D
        x.append(index)
        index = abs(hashstr('zuhe4_' + row['after_user_id'] + row['bid'])) % D
        x.append(index)
        # index = abs(hashstr('zuhe5_'+row['after_user_id'] + row['cid_1'])) %D
        # x.append(index)
        # index = abs(hashstr('zuhe6_'+row['ip'] + row['bid'])) %D
        # x.append(index)
        index = abs(hashstr('zuhe7_' + row['bid'] + row['cid'])) % D
        x.append(index)
        index = abs(hashstr('zuhe8_' + row['after_user_id'] + row['discount_price'])) % D
        x.append(index)
        index = abs(hashstr('zuhe9_' + row['bid'] + row['discount_rate'])) % D
        x.append(index)
        index = abs(hashstr('zuhe10_' + row['bid'] + row['sale_price'])) % D
        x.append(index)
        index = abs(hashstr('zuhe11_' + row['source_of_supply'] + row['bid'])) % D
        x.append(index)
        index = abs(hashstr('zuhe12_' + row['source_of_supply'] + row['discount_price'])) % D
        x.append(index)
        index = abs(hashstr('zuhe13_' + row['source_of_supply'] + row['degree'])) % D
        x.append(index)
        index = abs(hashstr('zuhe14_' + row['after_user_id'] + row['degree'])) % D
        x.append(index)
        index = abs(hashstr('zuhe15_' + row['after_user_id'] + row['source_of_supply'])) % D
        x.append(index)
        index = abs(hashstr('zuhe99_' + row['after_user_id'] + row['bid'] + row['cid'])) % D
        x.append(index)
        # build x
        # x = [0]  # 0 is the index of the bias term
        i = 0
        # for key in sorted(row):  # sort is for preserving feature ordering
        for key in row:
            value = row[key]
            # print (key)
            # one-hot encode everything with hash trick
            index = abs(hashstr(key + '_' + value)) % D
            i += 1
            x.append(index)

        yield x, y


if __name__ == "__main__":
    print(sys.argv[1])
    confile = sys.argv[1]
    rconfig = readconfig(confile)

    # model para
    L1 = float(rconfig.get_para("Model", "L1"))
    L2 = float(rconfig.get_para("Model", "L2"))
    alpha = float(rconfig.get_para("Model", "alpha"))
    beta = float(rconfig.get_para("Model", "beta"))
    D = int(rconfig.get_para("Model", "D"))
    print(L1, L2, alpha, beta)
    # learner = ftrl_proximal(alpha, beta, L1, L2, D)

    # train para
    epoch = rconfig.get_para("Train", "epoch")
    datafile = rconfig.get_para("Train", "datafile")
    isfirst = rconfig.get_para("Train", "isfirst")
    holdout = int(rconfig.get_para("Train", "holdout"))
    model_save = rconfig.get_para("Train", "model_save")
    datadir = rconfig.get_para("Train", "datadir")
    start = datetime.datetime.now()

    if isfirst == "1":
        learner = ftrl_proximal(alpha, beta, L1, L2, D)
        for e in range(int(epoch)):
            loss = 0.
            count = 0
            i = 0
            for x, y in data1(datafile, D):
                i += 1
                p = learner.predict(x)
                if i % holdout == 0:
                    loss += logloss(p, y)
                    # print p,y,loss
                    # print logloss(p,y)
                    count += 1
                else:
                    learner.update(x, p, y)
                if i % 500000 == 0 and i > 1:
                    print(' %s\tencountered: %d\tcurrent logloss: %f/%d=%f elapsed time: %s' % (
                        datetime.datetime.now(), i, loss, count, loss / count, str(datetime.datetime.now() - start)))

            print('Epoch %d finished, holdout logloss: %f, elapsed time: %s' % (
                e, loss / count, str(datetime.datetime.now() - start)))
        write_learner(learner, model_save)
    else:
        learner = load_learner(model_save)
        for e in range(int(epoch)):
            loss = 0.
            count = 0
            i = 0
            for x, y in data1(datafile, D):
                i += 1
                p = learner.predict(x)
                if i % holdout == 0:
                    loss += logloss(p, y)
                    count += 1
                else:
                    learner.update(x, p, y)
                if i % 500000 == 0 and i > 1:
                    print('increase %s\tencountered: %d\tcurrent logloss: %f/%d=%f elapsed time: %s' % (
                        datetime.datetime.now(), i, loss, count, loss / count, str(datetime.datetime.now() - start)))
            print('Increase Epoch %d finished, holdout logloss: %f elapsed time: %s' % (
                e, loss / count, str(datetime.datetime.now() - start)))
        write_learner(learner, model_save)

    # filename = getnextlog(datadir,'product_view_click.csv')
    # rconfig.cf.set("Train", "datafile",filename)
    # with open(confile,"w+") as f:
    #   rconfig.cf.write(f)



