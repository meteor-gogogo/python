#!/usr/bin/env python
# coding=utf-8
from utils import output
import requests
import json

class SensorsOperator(object):
    def __init__(self, url):
        self.url = url

    # 获取单日列表页uv总量
    def getListUv(self, date, sptype):
        try:
            if sptype == 'total':
                sql = "SELECT COUNT(distinct(distinct_ID)) as uv_list FROM (SELECT src_page_type as sptype, distinct_ID, CASE WHEN src_page_type like '%_search%' THEN 'oldversion' ELSE src_page_type END as type FROM EVENTS WHERE EVENT = 'ViewProductList'  AND date = '{0}'  AND src_page_type != ''  AND plumfrontend IN ( 'IOS客户端', 'Android客户端' )) ta WHERE ta.type != 'oldversion'".format(date)
            else:
                sql = "SELECT COUNT(distinct(distinct_ID)) AS uv_list FROM EVENTS WHERE EVENT = 'ViewProductList' AND date ='{0}' AND src_page_type = '{1}' AND plumfrontend in ('IOS客户端','Android客户端') ".format(date, sptype)
            payload = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, data=payload)
            if r.status_code == 200:
                datastr = r.text.encode("utf-8")
                if len(datastr) == 0:
                    uv_list = 0
                else:
                    dataarr = datastr.split("\n".encode(encoding="utf-8"))
                    datajson = json.loads(dataarr[0], encoding='utf-8')
                    uv_list = datajson["uv_list"]
            else:
                output.output_info("getListUv sa hive sql accur error, sql为%s" % sql)
            return uv_list
        except Exception as e:
            output.output_info("sensors getListUv accurs a err: %s"% str(e))

    # 获取单日列表页pv总量
    def getListPv(self, date, sptype):
        try:
            if sptype == 'total':
                sql = "SELECT COUNT(*) as pv_list FROM (SELECT src_page_type as sptype, CASE WHEN src_page_type like '%_search%' THEN 'oldversion' ELSE src_page_type END as type FROM EVENTS WHERE EVENT = 'ViewProductList'  AND date = '{0}'  AND src_page_type != ''  AND plumfrontend IN ( 'IOS客户端', 'Android客户端' )) ta WHERE ta.type != 'oldversion'".format(date)
            else:
                sql = "SELECT COUNT(src_page_type) AS pv_list FROM EVENTS WHERE EVENT = 'ViewProductList' AND date ='{0}' AND src_page_type = '{1}' AND plumfrontend in ('IOS客户端','Android客户端') ".format(date, sptype)
            payload = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, data=payload)
            if r.status_code == 200:
                datastr = r.text.encode("utf-8")
                if len(datastr) == 0:
                    pv_list = 0
                else:
                    dataarr = datastr.split("\n".encode(encoding="utf-8"))
                    datajson = json.loads(dataarr[0], encoding='utf-8')
                    pv_list = datajson["pv_list"]
            else:
                output.output_info("getListPv sa hive sql accur error, sql为%s" % sql)
            return pv_list
        except Exception as e:
            output.output_info("sensors getListPv accurs a err: %s"% str(e))

    # 获取单日详情页uv总量
    def getDetailUv(self, date, sptype):
        try:
            if sptype == 'total':
                sql = "SELECT COUNT(distinct(distinct_ID)) as uv_detail FROM (SELECT src_page_type as sptype, distinct_ID, CASE WHEN src_page_type like '%_search%' THEN 'oldversion' ELSE src_page_type END as type FROM EVENTS WHERE EVENT = 'ViewProduct'  AND date = '{0}'  AND src_page_type != ''  AND plumfrontend IN ( 'IOS客户端', 'Android客户端' )) ta WHERE ta.type != 'oldversion'".format(date)
            else:
                sql = "SELECT COUNT(distinct(distinct_ID)) AS uv_detail FROM EVENTS WHERE EVENT = 'ViewProduct' AND date ='{0}' AND src_page_type = '{1}' AND plumfrontend in ('IOS客户端','Android客户端') ".format(date, sptype)
            payload = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, data=payload)
            if r.status_code == 200:
                datastr = r.text.encode("utf-8")
                if len(datastr) == 0:
                    uv_detail = 0
                else:
                    dataarr = datastr.split("\n".encode(encoding="utf-8"))
                    datajson = json.loads(dataarr[0], encoding='utf-8')
                    uv_detail = datajson["uv_detail"]
            else:
                output.output_info("getDetailUv sa hive sql accur error, sql为%s" % sql)

            return uv_detail
        except Exception as e:
            output.output_info("sensors getDetailUv accurs a err: %s"% str(e))

    # 获取单日详情页pv总量(不去重)
    def getDetailPv(self, date, sptype):
        try:
            if sptype == 'total':
                sql = "SELECT COUNT(*) as pv_detail FROM (SELECT src_page_type as sptype, CASE WHEN src_page_type like '%_search%' THEN 'oldversion' ELSE src_page_type END as type FROM EVENTS WHERE EVENT = 'ViewProduct'  AND date = '{0}'  AND src_page_type != ''  AND plumfrontend IN ( 'IOS客户端', 'Android客户端' )) ta WHERE ta.type != 'oldversion'".format(date)
            else:
                sql = "SELECT COUNT(src_page_id) AS pv_detail FROM EVENTS WHERE EVENT = 'ViewProduct' AND date ='{0}' AND src_page_type = '{1}' AND plumfrontend in ('IOS客户端','Android客户端') ".format(date, sptype)
            payload = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, data=payload)
            if r.status_code == 200:
                datastr = r.text.encode("utf-8")
                if len(datastr) == 0:
                    pv_detail = 0
                else:
                    dataarr = datastr.split("\n".encode(encoding="utf-8"))
                    datajson = json.loads(dataarr[0], encoding='utf-8')
                    pv_detail = datajson["pv_detail"]
            else:
                output.output_info("getDetailPv sa hive sql accur error, sql为%s" % sql)
            return pv_detail
        except Exception as e:
            output.output_info("sensors getDetailPv accurs a err: %s"% str(e))

    # 获取单日详情页去重pv总量(去重单次曝光多次详情页点击的同一个商品)
    def getDetailDistinctPv(self, date, sptype):
        try:
            if sptype == 'total':
                sql = "SELECT COUNT(*) as pv_detail_distinct FROM (SELECT tb.sid,tb.productid,tb.type FROM (SELECT * from (SELECT sid, productid, src_page_type as sptype, CASE WHEN src_page_type like '%_search%' THEN 'oldversion' ELSE src_page_type END as type FROM EVENTS WHERE EVENT = 'ViewProduct' AND date = '{0}' AND src_page_type != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' )) ta WHERE ta.type != 'oldversion') tb GROUP BY tb.sid, tb.productid,tb.type) tc".format(date)
            else:
                sql = "SELECT COUNT(*) as pv_detail_distinct FROM (SELECT sid, productid FROM events WHERE event='ViewProduct' and date = '{0}' and src_page_type = '{1}' and plumfrontend in ('IOS客户端','Android客户端') GROUP BY sid, productid) ta".format(date, sptype)
            payload = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, data=payload)
            if r.status_code == 200:
                datastr = r.text.encode("utf-8")
                if len(datastr) == 0:
                    pv_detail_distinct = 0
                else:
                    dataarr = datastr.split("\n".encode(encoding="utf-8"))
                    datajson = json.loads(dataarr[0], encoding='utf-8')
                    pv_detail_distinct = datajson["pv_detail_distinct"]
            else:
                output.output_info("getDetailDistinctPv sa hive sql accur error, sql为%s" % sql)
            return pv_detail_distinct
        except Exception as e:
            output.output_info("sensors getDetailDistinctPv accurs a err: %s"% str(e))

    # 获取单日详情页去重pv总量(单次次曝光的多次详情页点击只计算一次)
    def getDetailSidPv(self, date, sptype):
        try:
            if sptype == 'total':
                sql = "SELECT COUNT(*) as pv_detail_sid FROM (SELECT ta.dsid,ta.type FROM  (SELECT  distinct(sid) as dsid, src_page_type AS sptype, CASE  WHEN src_page_type LIKE '%_search%' THEN 'oldversion' ELSE src_page_type  END AS type FROM EVENTS WHERE EVENT = 'ViewProduct'  AND date = '{0}' AND src_page_type != ''  AND plumfrontend IN ( 'IOS客户端', 'Android客户端' )  ) ta WHERE ta.type != 'oldversion' ) tb ".format(date)
            else:
                sql = "SELECT COUNT(distinct(sid)) AS pv_detail_sid FROM EVENTS WHERE EVENT = 'ViewProduct' AND date ='{0}' and src_page_type = '{1}' and plumfrontend in ('IOS客户端','Android客户端') ".format(date, sptype)
            payload = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, data=payload)
            if r.status_code == 200:
                datastr = r.text.encode("utf-8")
                if len(datastr) == 0:
                    pv_detail_sid = 0
                else:
                    dataarr = datastr.split("\n".encode(encoding="utf-8"))
                    datajson = json.loads(dataarr[0], encoding='utf-8')
                    pv_detail_sid = datajson["pv_detail_sid"]
            else:
                output.output_info("getDetailSidPv sa hive sql accur error, sql为%s" % sql)
            return pv_detail_sid
        except Exception as e:
            output.output_info("sensors getDetailSidPv accurs a err: %s"% str(e))

    # 获取单日列表加购数量
    def getListAddCartNum(self, date, sptype):
        try:
            if sptype == 'total':
                sql = " SELECT count( * ) AS listaddcartnum FROM (SELECT ta.sid, ta.pid, tb.sptype FROM ( SELECT sid, productid AS pid FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND src_page_type = 'list' AND date = '{0}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, pid ) ta LEFT JOIN ( SELECT sid, src_page_type AS sptype FROM EVENTS WHERE EVENT IN ('ViewProduct', 'ViewProductList' ) AND date = '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) AND sid != '' GROUP BY sid, sptype) tb ON ta.sid = tb.sid ) tc WHERE tc.sptype != '' ".format(date, date)
            else:
                sql = " SELECT tc.sptype AS type, count( tc.sptype ) AS listaddcartnum FROM( SELECT ta.sid, ta.pid, tb.sptype FROM ( SELECT sid, productid AS pid FROM EVENTS WHERE EVENT = 'AddCart' AND src_page_type = 'list' AND sid != '' AND date = '{0}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, pid ) ta LEFT JOIN ( SELECT sid, src_page_type AS sptype FROM EVENTS WHERE EVENT IN ( 'ViewProduct', 'ViewProductList' ) AND date = '{1}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, sptype) tb ON ta.sid = tb.sid) tc WHERE tc.sptype = '{2}' GROUP BY type".format(date, date, sptype)
            payload = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, data=payload)
            if r.status_code == 200:
                datastr = r.text.encode("utf-8")
                if len(datastr) == 0:
                    listaddcartnum = 0
                else:
                    dataarr = datastr.split("\n".encode(encoding="utf-8"))
                    datajson = json.loads(dataarr[0], encoding='utf-8')
                    listaddcartnum = datajson["listaddcartnum"]
            else:
                output.output_info("getListAddCartNum sa hive sql accur error, sql为%s" % sql)
            return listaddcartnum
        except Exception as e:
            output.output_info("sensors getListAddCartNum accurs a err: %s"% str(e))

    # 获取单日加购数量及商品折扣总价
    def getAddCartNumANDPrice(self, date, sptype):
        try:
            if sptype == 'total':
                sql = " SELECT count( * ) AS addcartnum, sum(tc.countprice) as totalcp FROM (SELECT ta.sid, ta.pid, tb.sptype,ta.saleprice,ta.countprice FROM ( SELECT sid, productid AS pid,productsale_price as saleprice, productdiscount_price as countprice FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date = '{0}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, pid, saleprice, countprice ) ta LEFT JOIN ( SELECT sid, src_page_type AS sptype FROM EVENTS WHERE EVENT IN ( 'ViewProduct', 'ViewProductList' ) AND date = '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) AND sid != '' GROUP BY sid, sptype) tb ON ta.sid = tb.sid ) tc WHERE tc.sptype != '' ".format(date, date)
            else:
                sql = " SELECT tc.sptype AS type, count( tc.sptype ) AS addcartnum, sum(tc.countprice) as totalcp FROM( SELECT ta.sid, ta.pid, tb.sptype, ta.saleprice,ta.countprice FROM ( SELECT sid, productid AS pid,productsale_price as saleprice, productdiscount_price as countprice FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date = '{0}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, pid, saleprice, countprice ) ta LEFT JOIN ( SELECT sid, src_page_type AS sptype FROM EVENTS WHERE EVENT IN ( 'ViewProduct', 'ViewProductList' ) AND date = '{1}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid, sptype) tb ON ta.sid = tb.sid) tc WHERE tc.sptype = '{2}' GROUP BY type".format(date, date, sptype)
            payload = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, data=payload)
            if r.status_code == 200:
                datastr = r.text.encode("utf-8")
                if len(datastr) == 0:
                    addcartnum = 0
                else:
                    dataarr = datastr.split("\n".encode(encoding="utf-8"))
                    datajson = json.loads(dataarr[0], encoding='utf-8')
                    addcartnum = datajson["addcartnum"]
                    totalcp = datajson["totalcp"]
            else:
                output.output_info("getAddCartNumANDPrice sa hive sql accur error, sql为%s" % sql)
            return addcartnum,totalcp
        except Exception as e:
            output.output_info("sensors getAddCartNumANDPrice accurs a err: %s"% str(e))

    # 获取单日订单uv数
    def getOrderUv(self, lastdate, date, sptype):
        try:
            if sptype == 'total':
                sql = " SELECT count(distinct(tg.uid)) as orderuv FROM (SELECT tf.sptype,tf.oid,tf.uid FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf GROUP BY tf.sptype,tf.oid,tf.uid) tg WHERE tg.sptype != '' ".format(lastdate, date, lastdate, date, date)
            else:
                sql = " SELECT count(distinct(tg.uid)) as orderuv FROM (SELECT tf.sptype,tf.oid,tf.uid FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf GROUP BY tf.sptype,tf.oid,tf.uid) tg WHERE tg.sptype = '{5}' ".format(lastdate, date, lastdate, date, date, sptype)
            payload = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, data=payload)
            if r.status_code == 200:
                datastr = r.text.encode("utf-8")
                if len(datastr) == 0:
                    orderuv = 0
                else:
                    dataarr = datastr.split("\n".encode(encoding="utf-8"))
                    datajson = json.loads(dataarr[0], encoding='utf-8')
                    orderuv = datajson["orderuv"]
            else:
                output.output_info("getOrderUv sa hive sql accur error, sql为%s" % sql)
            return orderuv
        except Exception as e:
            output.output_info("sensors getOrderUv accurs a err: %s"% str(e))

    # 获取单日订单数量(订单维度)
    def getOrderNum(self, lastdate, date, sptype):
        try:
            if sptype == 'total':
                sql = " SELECT count(*) as ordernum FROM (SELECT tf.sptype,tf.oid,tf.uid FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf GROUP BY tf.sptype,tf.oid,tf.uid) tg WHERE tg.sptype != '' ".format(lastdate, date, lastdate, date, date)
            else:
                sql = " SELECT count(*) as ordernum FROM (SELECT tf.sptype,tf.oid,tf.uid FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf GROUP BY tf.sptype,tf.oid,tf.uid) tg WHERE tg.sptype = '{5}' ".format(lastdate, date, lastdate, date, date, sptype)
            payload = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, data=payload)
            if r.status_code == 200:
                datastr = r.text.encode("utf-8")
                if len(datastr) == 0:
                    ordernum = 0
                else:
                    dataarr = datastr.split("\n".encode(encoding="utf-8"))
                    datajson = json.loads(dataarr[0], encoding='utf-8')
                    ordernum = datajson["ordernum"]
            else:
                output.output_info("getOrderNum sa hive sql accur error, sql为%s" % sql)
            return ordernum
        except Exception as e:
            output.output_info("sensors getOrderNum accurs a err: %s"% str(e))

    # 获取单日订单数量(商品维度)及商品折扣总价和用户实付总价
    def getPayOrderDetailNumANDPrice(self, lastdate, date, sptype):
        try:
            if sptype == 'total':
                sql = " SELECT count(tf.sptype) as payordernum, sum(tf.countprice) as totalcp,sum(tf.payprice) as totalpp FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,te.countprice,te.payprice, row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf,td.countprice,td.payprice FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid,orderitem_discountprice as countprice, orderitem_realpayprice as payprice FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid,countprice,payprice) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf WHERE rk = 1 AND tf.sptype != ''".format(lastdate, date, lastdate, date, date)
            else:
                sql = " SELECT count(tf.sptype) as payordernum,sum(tf.countprice) as totalcp,sum(tf.payprice) as totalpp FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,te.countprice,te.payprice, row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf,td.payprice,td.countprice FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid,orderitem_discountprice as countprice, orderitem_realpayprice as payprice FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid,countprice, payprice) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf WHERE rk = 1 AND tf.sptype = '{5}'".format(lastdate, date, lastdate, date, date, sptype)
            payload = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, data=payload)
            if r.status_code == 200:
                datastr = r.text.encode("utf-8")
                if len(datastr) == 0:
                    payordernum = 0
                else:
                    dataarr = datastr.split("\n".encode(encoding="utf-8"))
                    datajson = json.loads(dataarr[0], encoding='utf-8')
                    payordernum = datajson["payordernum"]
                    totalcp = datajson["totalcp"]
                    totalpp = datajson["totalpp"]
            else:
                output.output_info("getPayOrderDetailNumANDPrice sa hive sql accur error, sql为%s" % sql)
            return payordernum,totalcp,totalpp
        except Exception as e:
            output.output_info("sensors getPayOrderDetailNumANDPrice accurs a err: %s"% str(e))
