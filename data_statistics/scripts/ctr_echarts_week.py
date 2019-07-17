#!/usr/bin/env python
# coding=utf-8
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta, date
import requests
import time
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import os
import sys
import traceback
from pyecharts.charts import Page, Bar, Pie, Line
from pyecharts.globals import ThemeType
from pyecharts import options as opts

es = Elasticsearch(["es-cn-0pp0vm2ju000a003g.elasticsearch.aliyuncs.com"], http_auth=('elastic', 'NHRHpqeFn8'),
                   port=9200)
url = "http://websa.aplum-inc.com/api/sql/query?token=15ba27be5b7895379a0574993db783281988c16f95969fe423847d31ed95728d"

start = time.time()
tic = lambda: 'at %1.1f seconds' % (time.time() - start)

namedict = {'shownum': "列表总曝光量", 'uv_list': "列表页uv", 'pv_list': "列表页pv", 'uv_detail': "详情页uv",
            'pv_detail': "详情页曝光pv(不滤重)", 'pv_detail_distinct': "详情页曝光PV(滤重同一商品)", 'pv_detail_sid': "详情页曝光PV(滤重pv)",
            'listaddcartnum': "单日列表加购数", 'addcartnum': "单日加购数", 'totalcartsp': "单日加购商品在售总价",
            'totalcartcp': "单日加购商品折扣总价", 'orderuv': "单日订单uv", 'ordernum': "单日订单数", 'payordernum': "单日订单商品数",
            'totalordersp': "单日订单商品在售总价", 'totalordercp': "单日订单商品折扣总价", 'totalorderpp': "单日订单商品实付总价",
            'listpercent': "流量占比", 'shownumpercent': "曝光占比", 'ordernumpercent': "订单占比",
            'detailtocartpercent': "详情页到达购物车", 'showtocartpercent': "购物车到达率", 'ctrpercent': "曝光ctr",
            'pvcvrpercent': "点击CVR", 'show_eachuser': "浏览深度(用户查看商品数)", 'showcvrpercent': "曝光CVR",
            'rpm_total': "RPM千次展示GMV", 'gmv_total': "千次请求GMV",
            'uvvalue_total': "UV价值", 'ordervalue_total': "客单价"}

typedict = {'category': "分类页", 'activity': "活动页", 'search': "搜索页", 'brand': "品牌页", 'index': "首页",
            'other': '其他页', 'recommend': "推荐页", 'all': "总数"}


def sendMain(addressList, attachmentName):
    sender = 'aplumctr@aplum-inc.com'  # 发送邮件的邮箱地址
    user = 'aplumctr@aplum-inc.com'  # 发送邮件的邮箱地址
    passwd = 'qZdCAf3MCDv8SXi'  # 发送邮件的邮箱密码
    smtpserver = 'smtp.aplum-inc.com'  # 发送邮件的邮件服务器
    receivers = []  # 接收邮件的邮箱地址
    subject = "{} ctr统计".format(datestr)

    filePath = './%s' % attachmentName
    f = open(filePath, 'rb')
    file = f.read()
    f.close()
    baseName = os.path.basename(filePath)
    attachdisposition = "attachment;filename =%s" % baseName  # 发送附件html格式
    sendfile = open(filePath, 'rb').read()
    att = MIMEText(sendfile, 'plain', 'utf-8')
    att["Content-Type"] = "application/octet-stream"
    att["Content-Disposition"] = attachdisposition  # 发送附件html格式
    msgText = MIMEText(file, 'html', 'utf-8')

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


def getSqlResult(sql):
    payload = {'q': sql, 'format': 'json'}
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        datastr = r.text
        dataarr = datastr.split("\n")
        return dataarr
    else:
        print("sa hive sql accur error, sql为{}".format(sql))


def getEsResult(date, doc):
    p = dict()
    p['request_timeout'] = 30
    return es.search(index='aplum_ctr-{}'.format(date), doc_type='product', body=doc, params=p)


# 获取单日商品总曝光量
def getTotalShowNum(date, sptype):
    if sptype == 'all':
        esdoc = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
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
                    ],
                    "must_not": [
                        {"term": {
                            "Src_page_type.keyword": ""
                        }}
                    ]
                }
            }
        }
    else:
        esdoc = {
            "size": 0,
            "query": {
                "bool": {
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
            }
        }
    searched = getEsResult(date, esdoc)
    hitnum = searched['hits']['total']
    return hitnum


# 获取单日列表uv及pv
def getListInfo(date, sptype):
    if sptype == 'all':
        sql = "SELECT COUNT(*) as pv_list, COUNT(distinct(distinct_ID)) as uv_list FROM EVENTS WHERE EVENT = 'ViewProductList'  AND sid != '' AND date = '{0}'  AND src_page_type in ('recommend','activity','brand','search','index','other','category')  AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) ".format(
            date)
    else:
        sql = "SELECT COUNT(*) as pv_list, COUNT(distinct(distinct_ID)) AS uv_list FROM EVENTS WHERE EVENT = 'ViewProductList' AND sid != '' AND date ='{0}' AND src_page_type = '{1}' AND plumfrontend in ('IOS客户端','Android客户端') ".format(
            date, sptype)
    dataarr = getSqlResult(sql)
    if dataarr[0] == '':
        pv_list = 0
        uv_list = 0
    else:
        datajson = json.loads(dataarr[0])
        uv_list = datajson["uv_list"]
        pv_list = datajson["pv_list"]
    return uv_list, pv_list


# 获取单日详情页uv总量
def getDetailInfo(date, sptype):
    if sptype == 'all':
        sql = "SELECT COUNT(*) as pv_detail, COUNT(distinct(distinct_ID)) as uv_detail FROM EVENTS WHERE EVENT = 'ViewProduct' AND sid != '' AND date = '{0}'  AND src_page_type in ('recommend','activity','brand','search','index','other','category')  AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) ".format(
            date)
    else:
        sql = "SELECT COUNT(*) as pv_detail, COUNT(distinct(distinct_ID)) AS uv_detail FROM EVENTS WHERE EVENT = 'ViewProduct' AND date ='{0}' AND sid != '' AND src_page_type = '{1}' AND plumfrontend in ('IOS客户端','Android客户端') ".format(
            date, sptype)
    dataarr = getSqlResult(sql)
    if dataarr[0] == '':
        pv_detail = 0
        uv_detail = 0
    else:
        datajson = json.loads(dataarr[0])
        uv_detail = datajson["uv_detail"]
        pv_detail = datajson["pv_detail"]
    return uv_detail, pv_detail


# 获取单日订单数量(商品维度)及商品折扣总价和用户实付总价
def getPayOrderDetailNumANDPrice(lastdate, date, sptype):
    if sptype == 'all':
        sql = " SELECT count(distinct(tf.uid)) as uv_order,count(tf.sptype) as pv_order_product, sum (tf.saleprice) as totalsp,sum(tf.countprice) as totalcp,sum(tf.payprice) as totalpp FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,te.saleprice,te.countprice,te.payprice, row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf,td.saleprice,td.countprice,td.payprice FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid,orderitem_saleprice as saleprice, orderitem_discountprice as countprice, orderitem_realpayprice as payprice FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid,saleprice,countprice,payprice) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf WHERE rk = 1 AND tf.sptype != ''".format(
            lastdate, date, lastdate, date, date)
    else:
        sql = " SELECT count(distinct(tf.uid)) as uv_order,count(tf.sptype) as pv_order_product, sum(tf.saleprice) as totalsp,sum(tf.countprice) as totalcp,sum(tf.payprice) as totalpp FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,te.saleprice,te.countprice,te.payprice, row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf,td.saleprice,td.payprice,td.countprice FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid, orderitem_saleprice as saleprice, orderitem_discountprice as countprice, orderitem_realpayprice as payprice FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid,saleprice,countprice, payprice) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf WHERE rk = 1 AND tf.sptype = '{5}'".format(
            lastdate, date, lastdate, date, date, sptype)
    dataarr = getSqlResult(sql)
    if dataarr[0] == '':
        uv_order = 0
        pv_order_product = 0
        order_total_realpayprice = 0
    else:
        datajson = json.loads(dataarr[0])
        uv_order = datajson["uv_order"]
        pv_order_product = datajson["pv_order_product"]
        order_total_realpayprice = datajson["totalpp"]

    return uv_order, pv_order_product, order_total_realpayprice


# 获取数据集的字典
def getDataDict(date, lastdate, sptype):
    ctrdict = dict()
    ctrinfo = dict()
    date = date.strftime("%Y-%m-%d")
    show_pv = getTotalShowNum(date, sptype)
    # show_pv = 686688
    list_uv, list_pv = getListInfo(date, sptype)
    detail_uv, detail_pv = getDetailInfo(date, sptype)
    order_uv, order_product_pv, order_total_realpayprice = getPayOrderDetailNumANDPrice(lastdate, date, sptype)
    ctr = float(detail_pv) / float(show_pv) * 100
    pv_cvr = float(order_product_pv) / float(detail_pv)
    show_cvr = float(order_product_pv) / float(show_pv)
    show_eachuser = float(show_pv) / float(list_uv)
    rpm = float(order_total_realpayprice) / float(show_pv) * 1000
    gmv = float(order_total_realpayprice) / float(list_pv) * 1000
    uvvalue = float(order_total_realpayprice) / float(list_uv)
    ordervalue = float(order_total_realpayprice) / float(order_uv)

    ctrinfo['show_pv'] = show_pv
    ctrinfo['list_uv'] = list_uv
    ctrinfo['list_pv'] = list_pv
    ctrinfo['detail_uv'] = detail_uv
    ctrinfo['detail_pv'] = detail_pv
    ctrinfo['order_uv'] = order_uv
    ctrinfo['order_product_pv'] = order_product_pv
    ctrinfo['order_total_realpayprice'] = order_total_realpayprice
    ctrinfo['ctr'] = round(ctr, 2)
    ctrinfo['pv_ctr'] = pv_cvr
    ctrinfo['show_cvr'] = show_cvr
    ctrinfo['show_eachuser'] = round(show_eachuser)
    ctrinfo['rpm'] = round(rpm, 2)
    ctrinfo['gmv'] = round(gmv, 2)
    ctrinfo['uvvalue'] = round(uvvalue, 2)
    ctrinfo['ordervalue'] = round(ordervalue)

    ctrdict[sptype] = ctrinfo
    return ctrdict


def getEchartsData(sptype, datadict):
    datelist = list()
    type_list_uv_list = list()
    type_show_pv_list = list()
    type_order_product_pv_list = list()
    type_order_totalpayprice_list = list()
    type_ctr_list = list()
    type_rpm_list = list()
    type_show_eachuser_list = list()
    type_uvvalue_list = list()
    for date in datadict:
        datelist.append(date)
        type_list_uv_list.append(datadict[date][sptype]['list_uv'])
        type_show_pv_list.append(datadict[date][sptype]['show_pv'])
        type_order_product_pv_list.append(datadict[date][sptype]['order_product_pv'])
        type_order_totalpayprice_list.append(datadict[date][sptype]['order_total_realpayprice'])
        type_ctr_list.append(datadict[date][sptype]['ctr'])
        type_rpm_list.append(datadict[date][sptype]['rpm'])
        type_show_eachuser_list.append(datadict[date][sptype]['show_eachuser'])
        type_uvvalue_list.append(datadict[date][sptype]['uvvalue'])

    return datelist, type_list_uv_list, type_show_pv_list, type_order_product_pv_list, type_order_totalpayprice_list, type_ctr_list, type_rpm_list, type_show_eachuser_list, type_uvvalue_list


def getBar(sptype, datadict):
    sptype_bar = Bar(init_opts=opts.InitOpts(theme=ThemeType.CHALK))
    sptype_line = Line(init_opts=opts.InitOpts(theme=ThemeType.CHALK))
    datelist, type_list_uv_list, type_show_pv_list, type_order_product_pv_list, type_order_totalpayprice_list, type_ctr_list, type_rpm_list, type_show_eachuser_list, type_uvvalue_list = getEchartsData(
        sptype, datadict)
    sptype_bar.add_xaxis(datelist)
    sptype_bar.add_yaxis('列表uv', type_list_uv_list).add_yaxis('曝光', type_show_pv_list)
    sptype_bar.add_yaxis('订单商品数', type_order_product_pv_list).add_yaxis('订单实付总价', type_order_totalpayprice_list)
    sptype_bar.extend_axis(yaxis=opts.AxisOpts(axislabel_opts=opts.LabelOpts(), interval=5))
    sptype_line.add_xaxis(datelist)
    sptype_line.add_yaxis('CTR(值为百分数)', type_ctr_list, yaxis_index=1).add_yaxis('RPM', type_rpm_list, yaxis_index=1)
    sptype_line.add_yaxis('用户浏览商品数', type_show_eachuser_list, yaxis_index=1).add_yaxis('UV价值', type_uvvalue_list,
                                                                                       yaxis_index=1)

    sptype_bar.set_global_opts(title_opts=opts.TitleOpts(title="{}".format(typedict[sptype])))
    sptype_bar.set_series_opts(label_opts=opts.LabelOpts(is_show=False), markpoint_opts=opts.MarkPointOpts(
        data=[opts.MarkPointItem(type_="max", name="最大值"), opts.MarkPointItem(type_="min", name="最小值"), ]))

    sptype_bar.overlap(sptype_line)

    return sptype_bar


def createEcharts(datadict):
    page = Page()
    for sptype in typelist:
        bar = getBar(sptype, datadict)
        page.add(bar)
    page.render('ctr_summary_{}.html'.format(datestr))


es = Elasticsearch(["es-cn-0pp0vm2ju000a003g.elasticsearch.aliyuncs.com"], http_auth=('elastic', 'NHRHpqeFn8'),
                   port=9200)
url = "http://websa.aplum-inc.com/api/sql/query?token=15ba27be5b7895379a0574993db783281988c16f95969fe423847d31ed95728d"

start = time.time()
tic = lambda: 'at %1.1f seconds' % (time.time() - start)

if __name__ == '__main__':
    try:
        if len(sys.argv) == 1:
            today = date.today()
            date = today
        else:
            datestr = sys.argv[1]
            date = datetime.strptime(datestr, '%Y-%m-%d').date()
        datestr = date.strftime("%Y-%m-%d")
        datadict_all = dict()
        typelist = typedict.keys()

        for i in range(0, 7):
            date = datetime.strptime(datestr, '%Y-%m-%d').date()
            date = date + timedelta(days=(i - 7))
            lastdate = date + timedelta(days=-7)
            print(date, lastdate)
            datadict_day = dict()
            for sptype in typelist:
                print('======{}======'.format(sptype))
                datadict = getDataDict(date, lastdate, sptype)
                datadict_day[sptype] = datadict[sptype]
            datadict_all[date.strftime("%Y-%m-%d")] = datadict_day
        createEcharts(datadict_all)
        print("current time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print('Ended Polling: %s' % tic())
        addr_list = ['suzerui@aplum.com.cn', 'liudiyuhan@aplum.com.cn',
                     'pangbo@aplum.com.cn', 'liwenlong@aplum.com.cn']
        sendMain(addr_list, 'ctr_summary_{}.html'.format(datestr))
    except Exception as e:
        msg = traceback.format_exc()
        print(" ctr_echarts accurs error, the error:%s" % str(msg))
