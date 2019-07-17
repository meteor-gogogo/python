#!/usr/bin/env python
# coding=utf-8
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta, date
import requests
import time
import json
import MySQLdb
import xlsxwriter
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import os

mysqlhost = 'rr-2ze7824567d8dgu1y.mysql.rds.aliyuncs.com'
mysqlusername = 'plumdb'
mysqlpasswd = 'plumdb@2018'
db = 'aplum'


def sendMain(addressList, attachmentName):
    sender = 'aplum2016@163.com'  # 发送邮件的邮箱地址
    user = 'aplum2016@163.com'  # 发送邮件的邮箱地址
    passwd = '123qweASD'  # 发送邮件的邮箱密码
    smtpserver = 'smtp.163.com'  # 发送邮件的邮件服务器
    receivers = []  # 接收邮件的邮箱地址
    subject = "上周ctr统计"

    # filePath = '/home/aplum/inner/aplum_product_es/statistics/%s' % attachmentName
    filePath = '/home/aplum/work/python/statistics/%s' % attachmentName
    baseName = os.path.basename(filePath)
    attachdisposition = "attachment;filename =%s" % baseName  # 发送附件html格式
    sendfile = open(filePath, 'rb').read()
    att = MIMEText(sendfile, 'plain', 'utf-8')
    att["Content-Type"] = "application/octet-stream"
    att["Content-Disposition"] = attachdisposition  # 发送附件html格式
    msgText = MIMEText('您好，上周ctr各分类详细数据见附件', 'plain', 'utf-8')

    for addr in addressList:
        receivers.append(addr)
    receiver = ';'.join(receivers)
    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = subject
    msgRoot['From'] = sender
    msgRoot['To'] = receiver
    msgRoot.attach(msgText)  # 添加正文
    msgRoot.attach(att)  # 发送文件格式
    # 发送邮件
    try:
        smtp = smtplib.SMTP_SSL(smtpserver, 465)
        smtp.login(user, passwd)
        smtp.sendmail(sender, addressList, msgRoot.as_string())
        print(datetime.now().strftime("%Y.%m.%d-%H:%M:%S"), '发送成功')
    except Exception as e:
        print(e)
    finally:
        os.remove(filePath)
        smtp.quit()


# 获取整年的日期list从周五开始
def get_week_of_year(year):
    temp = year
    map = {}
    year = str(year) + '-01-01'
    # 每一年的一月一号
    year_of_date = datetime.strptime(year, '%Y-%m-%d')
    # 如果一号大于周4，则属于上一年最后一周
    # 计算周几
    which_week = year_of_date.isoweekday()
    # 闰年
    total_of_year = 365
    range_start = 0
    if temp % 100 != 0 and temp % 4 == 0:
        total_of_year = 366
    if year.find('01-01') != -1:
        if which_week > 4:
            add = 7 - which_week + 1
            year_of_date = year_of_date + timedelta(add)
            total_of_year = total_of_year - add
        else:
            add = 1 - which_week
            if which_week != 1:
                year_of_date = year_of_date + timedelta(add)
                total_of_year = total_of_year - add
    # 上面已经计算出了周一，加四天从周五开始
    year_of_date = year_of_date + timedelta(4)
    final_year_array = []
    for i in range(range_start, total_of_year):
        final_year = year_of_date + timedelta(i)
        final_year_array.append(final_year.strftime('%Y-%m-%d'))
    return final_year_array


def reshape(lst, n):
    size = len(lst) // n
    if (len(lst) % n) != 0:
        size = size + 1
    return [lst[i * n:(i + 1) * n] for i in range(size)]


# 获取上周五到本周四的list
def getlastweeklist():
    now = datetime.now()
    week_start = now - timedelta(days=now.weekday() + 3)
    weekday = week_start.strftime("%Y-%m-%d")
    yearinfo = datetime.now().year
    week_of_date = reshape(get_week_of_year(int(yearinfo)), 7)
    for da in week_of_date:
        if weekday in da:
            return da
    return []


def getlastweekstr(weeklist):
    # weeklist = getlastweeklist()
    return "%s 至 %s " % (weeklist[0], weeklist[len(weeklist) - 1])


def getFriday(today):
    week_Fri = today - timedelta(days=today.weekday() - 4)
    date1 = week_Fri.strftime("%Y-%m-%d")
    return date1


# 获取src_page_id的在es中的曝光以及spidlist
def getKeywordListAndHitnum(es, sptype, starttime, endtime):
    if len(weeklist) == 0:
        return 0
    spiddict = dict()
    esdoc = {
        "size": 0,
        "query": {
            "bool": {
                "must_not": [
                    {"term": {
                        "Src_page_type.keyword": ""
                    }}
                ],
                "filter": {
                    "range": {
                        "Createtime": {
                            "gte": starttime,
                            "lt": endtime
                        }
                    }
                },
                "must": [
                    {
                        "term": {
                            "Src_page_type.keyword": sptype
                        }
                    },
                    {
                        "terms": {
                            "X-Pd-Os.keyword": [
                                "ios_app",
                                "android_app",
                                "ios_app_native",
                                "android_app_native"
                            ]
                        }
                    }
                ]
            }
        },
        "aggs": {
            "idcount": {
                "cardinality": {
                    "field": "Src_page_id.keyword"
                }
            }
        }
    }
    searched = es.search(index="aplum_ctr*", doc_type="product", body=esdoc)
    totalnum = searched['aggregations']['idcount']['value']
    size = 5000
    if totalnum <= size:
        pagesize = 1
    else:
        pagesize = int(totalnum / size + 2)
    for num in range(0, pagesize):
        spiddoc = {
            "size": 0,
            "query": {
                "bool": {
                    "must_not": [
                        {"term": {
                            "Src_page_type.keyword": ""
                        }}
                    ],
                    "filter": {
                        "range": {
                            "Createtime": {
                                "gte": starttime,
                                "lt": endtime
                            }
                        }
                    },
                    "must": [
                        {"term": {
                            "Src_page_type.keyword": sptype
                        }},
                        {
                            "terms": {
                                "X-Pd-Os.keyword": [
                                    "ios_app",
                                    "android_app",
                                    "ios_app_native",
                                    "android_app_native"
                                ]
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "idcount": {
                    "terms": {
                        "field": "Src_page_id.keyword",
                        "include": {
                            "partition": num,
                            "num_partitions": pagesize
                        },
                        "size": size
                    }
                }
            }
        }

        spidsearched = es.search(index="aplum_ctr*", doc_type="product", body=spiddoc)
        buckets = spidsearched['aggregations']['idcount']['buckets']
        for b in buckets:
            k = b['key']
            hitnum = int(b['doc_count'])
            spidinfo = dict()
            spidinfo['hitnum'] = hitnum
            spidinfo['pv_count'] = 0
            spidinfo['pvfrom_count'] = 0
            spidinfo['pid_name'] = ''
            spidinfo['rate_pid'] = 0
            spidinfo['rate_pidhit'] = 0
            spiddict[k] = spidinfo
    return spiddict


# 获取神策中的pv信息并计算ctr数据
def getDeatilCtr(sptype, spiddict, weeklist):
    startdate = weeklist[0]
    endate = weeklist[len(weeklist) - 1]
    sql = "SELECT ta.pid,ta.pv_count,tb.pvfrom_count FROM (SELECT src_page_id as pid,count(src_page_id) as pv_count FROM events WHERE event = 'ViewProductList' AND src_page_type = '{0}' AND date BETWEEN '{1}' AND '{2}' and plumfrontend in ('IOS客户端','Android客户端') GROUP BY pid) ta LEFT JOIN (SELECT src_page_id as pid,count(src_page_id) as pvfrom_count FROM events WHERE event = 'ViewProduct' AND src_page_type = '{3}' AND date BETWEEN '{4}' AND '{5}' and plumfrontend in ('IOS客户端','Android客户端') GROUP BY pid) tb ON ta.pid = tb.pid".format(
        sptype, startdate, endate, sptype, startdate, endate)
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        data = datastr.split("\n")
    # print data
    for i in range(0, len(data)):
        if len(data[i]) == 0:
            continue
        else:
            datajson = json.loads(data[i])
        if datajson['pid'] in spiddict:
            if 'pv_count' in datajson:
                pv_count = datajson['pv_count']
            else:
                pv_count = 0
            if 'pvfrom_count' in datajson:
                pvfrom_count = datajson['pvfrom_count']
            else:
                pvfrom_count = 0
            spiddict[datajson['pid']]['pv_count'] = pv_count
            spiddict[datajson['pid']]['pvfrom_count'] = pvfrom_count
            if pv_count == 0:
                rate_pid = 0
            else:
                rate_pid = float(pvfrom_count) / float(pv_count)
            rate_pidhit = float(pvfrom_count) / float(spiddict[datajson['pid']]['hitnum'])
            rate_pid = '%.2f%%' % float(rate_pid * 100)
            rate_pidhit = '%.2f%%' % float(rate_pidhit * 100)
            spiddict[datajson['pid']]['rate_pid'] = rate_pid
            spiddict[datajson['pid']]['rate_pidhit'] = rate_pidhit
        else:
            spidinfo = dict()
            spidinfo['hitnum'] = 0
            spidinfo['pv_count'] = 0
            spidinfo['pvfrom_count'] = 0
            spidinfo['pid_name'] = ''
            spidinfo['rate_pid'] = 0
            spidinfo['rate_pidhit'] = 0
            spiddict[datajson['pid']] = spidinfo
            if 'pv_count' in datajson:
                pv_count = datajson['pv_count']
            else:
                pv_count = 0
            if 'pvfrom_count' in datajson:
                pvfrom_count = datajson['pvfrom_count']
            else:
                pvfrom_count = 0
            spiddict[datajson['pid']]['pv_count'] = pv_count
            spiddict[datajson['pid']]['pvfrom_count'] = pvfrom_count
            if pv_count == 0:
                rate_pid = 0
            else:
                rate_pid = float(pvfrom_count) / float(pv_count)
            rate_pid = '%.2f%%' % float(rate_pid * 100)
            spiddict[datajson['pid']]['rate_pid'] = rate_pid
            spiddict[datajson['pid']]['rate_pidhit'] = 0
    return spiddict


# 获取各分类下id的详细名称
def getPidName(sptype, spiddict):
    spidList = list(spiddict.keys())
    pLen = len(spidList)
    EveryCount = 1000
    # start = 0
    # end = EveryCount 每次查询的id次数
    times = pLen / EveryCount + 1
    for t in range(int(times)):
        pidStr = []
        start = t * EveryCount
        end = (t + 1) * EveryCount
        if end >= pLen:
            end = pLen
        for p in spidList[start:end]:
            pidStr.append(str(p))

        idsstr = '\',\''.join(pidStr)
        if sptype == 'activity':
            sql = "select id, name, group_name from t_{0} where id in ('{1}')".format(sptype, idsstr)
        else:
            sql = "select id, name from t_{0} where id in ('{1}')".format(sptype, idsstr)
        # print sql1
        cursor.execute(sql)
        results = cursor.fetchall()
        for p in results:
            pid = str(p['id'])
            if pid in spiddict:
                if sptype == 'activity':
                    if len(p['group_name']) != 0:
                        spiddict[pid]['pid_name'] = p['group_name'] + ':' + p['name']
                    else:
                        spiddict[pid]['pid_name'] = p['name']
                else:
                    spiddict[pid]['pid_name'] = p['name']

def createSearchDetailCtrRate(workbook, lastweekstr, searchpiddict_final):
    if len(searchpiddict_final) == 0:
        return False
    sheet = workbook.add_worksheet(u"搜索词详细CTR")
    sheet.write(0, 0, u"搜索词")
    sheet.write(0, 1, u"搜索词商品详情页pv")
    sheet.write(0, 2, u"搜索词商品总曝光量")
    sheet.write(0, 3, u"搜索词商品总CTR")
    sheet.write(0, 4, u"搜索词曝光量(搜索词列表页pv)")
    sheet.write(0, 5, u"搜索词CTR")
    sheet.write(0, 6, lastweekstr)
    i = 1
    keyslist = searchpiddict_final.keys()
    for pid in keyslist:
        sheet.write(i, 0, pid)
        sheet.write(i, 1, searchpiddict_final[pid]['pvfrom_count'])
        sheet.write(i, 2, searchpiddict_final[pid]['hitnum'])
        sheet.write(i, 3, searchpiddict_final[pid]['rate_pidhit'])
        sheet.write(i, 4, searchpiddict_final[pid]['pv_count'])
        sheet.write(i, 5, searchpiddict_final[pid]['rate_pid'])
        i = i + 1
    return sheet


def createCategoryDetailCtrRate(workbook, lastweekstr, categorypiddict_final):
    if len(categorypiddict_final) == 0:
        return False
    sheet1 = workbook.add_worksheet(u"分类页各分类商品详细CTR")
    sheet1.write(0, 0, u"分类页各分类id")
    sheet1.write(0, 1, u"分类页各分类名称")
    sheet1.write(0, 2, u"分类页各分类商品详情页pv")
    sheet1.write(0, 3, u"分类页各分类商品总曝光量")
    sheet1.write(0, 4, u"分类页各分类商品总CTR")
    sheet1.write(0, 5, u"分类页各分类曝光量(分类页各分类列表页pv)")
    sheet1.write(0, 6, u"分类页各分类CTR")
    sheet1.write(0, 7, lastweekstr)
    i = 1
    keyslist = categorypiddict_final.keys()

    for pid in keyslist:
        showidStr = []
        if 'showid' in pid:
            showidStr.append(str(pid.split('_')[1]))
        showidsstr = '\',\''.join(showidStr)
        sql = "select show_id, show_name from t_category_search where show_id in ('{0}')".format(showidsstr)
        cursor.execute(sql)
        results = cursor.fetchall()
        for p in results:
            pid = 'showid_' + str(p['show_id'])
            if pid in categorypiddict_final:
                categorypiddict_final[pid]['pid_name'] = p['show_name']
        sheet1.write(i, 0, pid)
        sheet1.write(i, 1, categorypiddict_final[pid]['pid_name'])
        sheet1.write(i, 2, categorypiddict_final[pid]['pvfrom_count'])
        sheet1.write(i, 3, categorypiddict_final[pid]['hitnum'])
        sheet1.write(i, 4, categorypiddict_final[pid]['rate_pidhit'])
        sheet1.write(i, 5, categorypiddict_final[pid]['pv_count'])
        sheet1.write(i, 6, categorypiddict_final[pid]['rate_pid'])
        i = i + 1
    return sheet1


def createBrandDetailCtrRate(workbook, lastweekstr, brandpiddict_final):
    if len(brandpiddict_final) == 0:
        return False
    sheet2 = workbook.add_worksheet(u"品牌页各品牌商品详细CTR")
    sheet2.write(0, 0, u"品牌页各品牌id")
    sheet2.write(0, 1, u"品牌页各品牌名称")
    sheet2.write(0, 2, u"品牌页各品牌商品详情页pv")
    sheet2.write(0, 3, u"品牌页各品牌商品总曝光量")
    sheet2.write(0, 4, u"品牌页各品牌商品总CTR")
    sheet2.write(0, 5, u"品牌页各品牌曝光量(品牌页各品牌列表页pv)")
    sheet2.write(0, 6, u"品牌页各品牌CTR")
    sheet2.write(0, 7, lastweekstr)
    i = 1
    keyslist = brandpiddict_final.keys()
    # print keyslist
    for pid in keyslist:
        # print pid
        sheet2.write(i, 0, pid)
        sheet2.write(i, 1, brandpiddict_final[pid]['pid_name'])
        sheet2.write(i, 2, brandpiddict_final[pid]['pvfrom_count'])
        sheet2.write(i, 3, brandpiddict_final[pid]['hitnum'])
        sheet2.write(i, 4, brandpiddict_final[pid]['rate_pidhit'])
        sheet2.write(i, 5, brandpiddict_final[pid]['pv_count'])
        sheet2.write(i, 6, brandpiddict_final[pid]['rate_pid'])
        i = i + 1
    return sheet2


def createActivityDetailCtrRate(workbook, lastweekstr, activitypiddict_final):
    if len(activitypiddict_final) == 0:
        return False
    sheet3 = workbook.add_worksheet(u"活动页各活动商品详细CTR")
    sheet3.write(0, 0, u"活动页各活动id")
    sheet3.write(0, 1, u"活动页各活动名称")
    sheet3.write(0, 2, u"活动页各活动商品详情页pv")
    sheet3.write(0, 3, u"活动页各活动商品总曝光量")
    sheet3.write(0, 4, u"活动页各活动商品总CTR")
    sheet3.write(0, 5, u"活动页各活动曝光量(活动页各活动列表页pv)")
    sheet3.write(0, 6, u"活动页各活动CTR")
    sheet3.write(0, 7, lastweekstr)
    i = 1
    keyslist = activitypiddict_final.keys()
    # print keyslist
    for pid in keyslist:
        # print pid
        sheet3.write(i, 0, pid)
        sheet3.write(i, 1, activitypiddict_final[pid]['pid_name'])
        sheet3.write(i, 2, activitypiddict_final[pid]['pvfrom_count'])
        sheet3.write(i, 3, activitypiddict_final[pid]['hitnum'])
        sheet3.write(i, 4, activitypiddict_final[pid]['rate_pidhit'])
        sheet3.write(i, 5, activitypiddict_final[pid]['pv_count'])
        sheet3.write(i, 6, activitypiddict_final[pid]['rate_pid'])
        i = i + 1
    return sheet3


def createIndexDetailCtrRate(workbook, lastweekstr, indexpiddict_final):
    if len(indexpiddict_final) == 0:
        return False
    sheet4 = workbook.add_worksheet(u"首页各模块商品详细CTR")
    sheet4.write(0, 0, u"首页各模块id")
    sheet4.write(0, 1, u"首页各模块名称")
    sheet4.write(0, 2, u"首页各模块商品详情页pv")
    sheet4.write(0, 3, u"首页各模块商品总曝光量")
    sheet4.write(0, 4, u"首页各模块商品总CTR")
    sheet4.write(0, 5, u"首页各模块曝量(首页各模块列表页pv)")
    sheet4.write(0, 6, u"首页各模块CTR")
    sheet4.write(0, 7, lastweekstr)
    i = 1
    keyslist = indexpiddict_final.keys()
    # print keyslist
    for pid in keyslist:
        # print pid
        if pid == "tab2":
            indexpiddict_final[pid]['pid_name'] = '包袋'
        if pid == "tab3":
            indexpiddict_final[pid]['pid_name'] = '配饰'
        if pid == "tab4":
            indexpiddict_final[pid]['pid_name'] = '女装'
        if pid == "tab5":
            indexpiddict_final[pid]['pid_name'] = '女鞋'
        if pid == "tab6":
            indexpiddict_final[pid]['pid_name'] = '男士'
        sheet4.write(i, 0, pid)
        sheet4.write(i, 1, indexpiddict_final[pid]['pid_name'])
        sheet4.write(i, 2, indexpiddict_final[pid]['pvfrom_count'])
        sheet4.write(i, 3, indexpiddict_final[pid]['hitnum'])
        sheet4.write(i, 4, indexpiddict_final[pid]['rate_pidhit'])
        sheet4.write(i, 5, indexpiddict_final[pid]['pv_count'])
        sheet4.write(i, 6, indexpiddict_final[pid]['rate_pid'])
        i = i + 1
    return sheet4


def createRecommendDetailCtrRate(workbook, lastweekstr, recommendpiddict_final):
    if len(recommendpiddict_final) == 0:
        return False
    sheet5 = workbook.add_worksheet(u"推荐页各模块商品详细CTR")
    sheet5.write(0, 0, u"推荐页各模块分类id")
    sheet5.write(0, 1, u"推荐页各模块名称")
    sheet5.write(0, 2, u"推荐页各模块商品详情页pv")
    sheet5.write(0, 3, u"推荐页各模块商品总曝光量")
    sheet5.write(0, 4, u"推荐页各模块商品总CTR")
    sheet5.write(0, 5, u"推荐页各模块曝光量(推荐页各模块列表页pv)")
    sheet5.write(0, 6, u"推荐页各模块CTR")
    sheet5.write(0, 7, lastweekstr)
    i = 1
    keyslist = recommendpiddict_final.keys()
    # print keyslist
    for pid in keyslist:
        # print pid
        if pid == "index":
            recommendpiddict_final[pid]['pid_name'] = '首页推荐'
        if pid == "cart":
            recommendpiddict_final[pid]['pid_name'] = '购物车推荐'
        if pid == "blackcard":
            recommendpiddict_final[pid]['pid_name'] = '黑卡推荐'
        sheet5.write(i, 0, pid)
        sheet5.write(i, 1, recommendpiddict_final[pid]['pid_name'])
        sheet5.write(i, 2, recommendpiddict_final[pid]['pvfrom_count'])
        sheet5.write(i, 3, recommendpiddict_final[pid]['hitnum'])
        sheet5.write(i, 4, recommendpiddict_final[pid]['rate_pidhit'])
        sheet5.write(i, 5, recommendpiddict_final[pid]['pv_count'])
        sheet5.write(i, 6, recommendpiddict_final[pid]['rate_pid'])
        i = i + 1
    return sheet5


def createOtherDetailCtrRate(workbook, lastweekstr, otherpiddict_final):
    if len(otherpiddict_final) == 0:
        return False
    sheet6 = workbook.add_worksheet(u"其他页各模块商品详细CTR")
    sheet6.write(0, 0, u"其他页各模块id")
    sheet6.write(0, 1, u"其他页各模块名称")
    sheet6.write(0, 2, u"其他页各模块商品详情页pv")
    sheet6.write(0, 3, u"其他页各模块商品总曝光量")
    sheet6.write(0, 4, u"其他页各模块商品总CTR")
    sheet6.write(0, 5, u"其他页各模块曝光量(活动页各模块列表页pv)")
    sheet6.write(0, 6, u"其他页各模块CTR")
    sheet6.write(0, 7, lastweekstr)
    i = 1
    keyslist = otherpiddict_final.keys()
    # print keyslist
    for pid in keyslist:
        if pid == "lotteryCurrent":
            otherpiddict_final[pid]['pid_name'] = '抢大牌'
        if pid == "cheapLuxury":
            otherpiddict_final[pid]['pid_name'] = '白菜大牌'
        if pid == "personal":
            otherpiddict_final[pid]['pid_name'] = '个性衣橱'
        if pid == "bargain_decent":
            otherpiddict_final[pid]['pid_name'] = '百元好物'
        if pid == "allNew":
            otherpiddict_final[pid]['pid_name'] = '全新品牌'
        if pid == "allJapan":
            otherpiddict_final[pid]['pid_name'] = '日本直邮'
        if pid == "luxuries":
            otherpiddict_final[pid]['pid_name'] = '重奢'
        if pid == 'history':
            otherpiddict_final[pid]['pid_name'] = '最近浏览'
        if pid == 'luxurious_class':
            otherpiddict_final[pid]['pid_name'] = '大牌尖货'
        if pid == 'golden_sizes':
            otherpiddict_final[pid]['pid_name'] = '黄金码'
        if pid == 'design_class':
            otherpiddict_final[pid]['pid_name'] = '轻奢主义'
        if pid == 'high_level_class':
            otherpiddict_final[pid]['pid_name'] = '高街品牌'
        if pid == 'new_arrival_ver1':
            otherpiddict_final[pid]['pid_name'] = '最新上架'
        if pid == 'wishlist':
            otherpiddict_final[pid]['pid_name'] = '我的收藏'
        sheet6.write(i, 0, pid)
        sheet6.write(i, 1, otherpiddict_final[pid]['pid_name'])
        sheet6.write(i, 2, otherpiddict_final[pid]['pvfrom_count'])
        sheet6.write(i, 3, otherpiddict_final[pid]['hitnum'])
        sheet6.write(i, 4, otherpiddict_final[pid]['rate_pidhit'])
        sheet6.write(i, 5, otherpiddict_final[pid]['pv_count'])
        sheet6.write(i, 6, otherpiddict_final[pid]['rate_pid'])
        i = i + 1
    return sheet6


if __name__ == '__main__':
    db = MySQLdb.connect(mysqlhost, mysqlusername, mysqlpasswd, db, charset='utf8')
    es = Elasticsearch(["es-cn-0pp0vm2ju000a003g.elasticsearch.aliyuncs.com"], http_auth=('elastic', 'NHRHpqeFn8'),
                       port=9200)
    url = "http://websa.aplum-inc.com/api/sql/query?token=15ba27be5b7895379a0574993db783281988c16f95969fe423847d31ed95728d"
    cursor = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    weeklist = getlastweeklist()
    today = date.today()
    weekday05 = getFriday(today)
    # weekday05 = '2019-02-27'
    # weeklist = ['2019-02-19', '2019-02-20', '2019-02-21', '2019-02-22','2019-02-23', '2019-02-24', '2019-02-25', '2019-02-26']
    # print weekday05
    lastweekstr = getlastweekstr(weeklist)
    print(lastweekstr)
    starttime = int(time.mktime(time.strptime(str(weeklist[0]), "%Y-%m-%d")))
    endtime = int(time.mktime(time.strptime(str(weekday05), "%Y-%m-%d")))
    print('======搜索词详细CTR ======')
    searchpiddict = getKeywordListAndHitnum(es, "search", starttime, endtime)
    searchpiddict_final = getDeatilCtr("search", searchpiddict, weeklist)
    print(searchpiddict_final)
    print('======分类页id商品详细CTR ======')
    categorypiddict = getKeywordListAndHitnum(es, "category", starttime, endtime)
    categorypiddict_final = getDeatilCtr("category", categorypiddict, weeklist)
    getPidName("category", categorypiddict_final)
    print(categorypiddict_final)
    print('======品牌id商品详细CTR ======')
    brandpiddict = getKeywordListAndHitnum(es, "brand", starttime, endtime)
    brandpiddict_final = getDeatilCtr("brand", brandpiddict, weeklist)
    getPidName('brand', brandpiddict_final)
    # print brandpiddict_final
    print('======活动页id商品详细CTR ======')
    activitypiddict = getKeywordListAndHitnum(es, "activity", starttime, endtime)
    activitypiddict_final = getDeatilCtr("activity", activitypiddict, weeklist)
    getPidName('activity', activitypiddict_final)
    # print activitypiddict_final
    print('======首页id商品详细CTR ======')
    indexpiddict = getKeywordListAndHitnum(es, "index", starttime, endtime)
    indexpiddict_final = getDeatilCtr("index", indexpiddict, weeklist)
    # print indexpiddict_final
    print('======推荐页id商品详细CTR ======')
    recommendpiddict = getKeywordListAndHitnum(es, "recommend", starttime, endtime)
    recommendpiddict_final = getDeatilCtr("recommend", recommendpiddict, weeklist)
    # print recommendpiddict_final
    print('======其他页id商品详细CTR ======')
    otherpiddict = getKeywordListAndHitnum(es, "other", starttime, endtime)
    otherpiddict_final = getDeatilCtr("other", otherpiddict, weeklist)
    # print otherpiddict_final

    datestr = time.strftime("%Y%m%d", time.localtime())
    filename = "ctr_detail_%s.xls" % datestr
    workbook = xlsxwriter.Workbook(filename)
    createSearchDetailCtrRate(workbook, lastweekstr, searchpiddict_final)
    createCategoryDetailCtrRate(workbook, lastweekstr, categorypiddict_final)
    createActivityDetailCtrRate(workbook, lastweekstr, activitypiddict_final)
    createBrandDetailCtrRate(workbook, lastweekstr, brandpiddict_final)
    createRecommendDetailCtrRate(workbook, lastweekstr, recommendpiddict_final)
    createIndexDetailCtrRate(workbook, lastweekstr, indexpiddict_final)
    createOtherDetailCtrRate(workbook, lastweekstr, otherpiddict_final)
    cursor.close()  # 断开游标
    db.close()  # 断开数据库
    workbook.close()
    # addr_list = ['461235890@qq.com']
    addr_list = ['suzerui@aplum.com.cn']
    sendMain(addr_list, filename)
