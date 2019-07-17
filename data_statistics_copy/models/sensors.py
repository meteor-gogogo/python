#!/usr/bin/env python
# coding=utf-8
from utils import output
import requests
import json


class SensorsOperator(object):
    def __init__(self, url):
        self.url = url

    # 获取单日列表页uv总量
    def get_list_uv(self, date, sptype):
        uv_list = 0
        try:
            if sptype == 'total':
                sql = "select count(distinct distinct_ID) as uv_list from " \
                      "(select src_page_type as sptype, distinct_ID, " \
                      "case when src_page_type like '%_search%' then 'oldversion' else src_page_type end as type " \
                      "from events where event = 'ViewProductList' and date = '{0}' and src_page_type != '' and " \
                      "plumfrontend in ('IOS客户端', 'Android客户端')) ta where ta.type != 'oldversion'".format(date)
            else:
                sql = "select count(distinct distinct_ID) as uv_list from events where event = 'ViewProductList' " \
                      "and date = '{0}' and src_page_type = '{1}' and plumfrontend in ('IOS客户端', 'Android客户端')"\
                    .format(date, sptype)

            pay_load = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, pay_load)
            if r.status_code == 200:
                datastr = r.text.encode('utf-8')
                if len(datastr) == 0:
                    uv_list = 0
                else:
                    dataarr = datastr.split('\n'.encode('utf-8'))
                    data_json = json.loads(dataarr[0], encoding='utf-8')
                    uv_list = data_json['uv_list']
            else:
                output.output_info("get_list_uv sql occurs a error, sql is %s" % sql)
            return uv_list
        except Exception as e:
            output.output_info("sensors get_list_uv occurs a error, error is %s" % str(e))

    # 获取单日列表页pv总量
    def get_list_pv(self, date, sptype):
        pv_list = 0
        try:
            if sptype == 'total':
                sql = "select count(*) as pv_list from (select src_page_type as sptype, " \
                      "case when src_page_type like '%_search%' then 'oldversion' else src_page_type end as type " \
                      "from events where event = 'ViewProductList' and date = '{0}' and src_page_type != '' and " \
                      "plumfrontend in ('IOS客户端', 'Android客户端')) ta where ta.type != 'oldversion'".format(date)
            else:
                sql = "select count(src_page_type) as pv_list from events " \
                      "where event = 'ViewProductList' and date = '{0}' and src_page_type = '{1}' " \
                      "and plumfrontend in ('IOS客户端', Android客户端')".format(date, sptype)
            pay_load = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, pay_load)
            if r.status_code == 200:
                data_str = r.text.encode('utf-8')
                if len(data_str) == 0:
                    pv_list = 0
                else:
                    data_arr = data_str.split('\n'.encode('utf-8'))
                    data_json = json.loads(data_arr[0], encoding='utf-8')
                    pv_list = data_json['pv_list']
            else:
                output.output_info("get_list_pv sql occurs a error, sql is %s" % sql)
            return pv_list
        except Exception as e:
            output.output_info("sensors get_list_pv occurs a error, error is %s" % str(e))

    # 获取单日详情页uv总量
    def get_detail_uv(self, date, sptype):
        uv_detail = 0
        try:
            if sptype == 'total':
                sql = "select count(distinct distinct_ID) as uv_detail from" \
                      "(select src_page_type as sptype, distinct_ID, " \
                      "case when src_page_type like '%_search%' then 'oldversion' else src_page_type end as type " \
                      "from events where event = 'ViewProduct' and date = '{0}' and src_page_type != '' " \
                      "and plumfrontend in ('IOS客户端', 'Android客户端')ta where ta.type != 'oldversion')".format(date)
            else:
                sql = "select count(distinct distinct_ID) as uv_detail from events where " \
                      "event = 'ViewProduct' and date = '{0}' and src_page_type = '{1}' and " \
                      "plumfrontend in ('IOS客户端', 'Android客户端')".format(date, sptype)
            pay_load = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, pay_load)
            if r.status_code == 200:
                data_str = r.text.encode('utf-8')
                if len(data_str) == 0:
                    uv_detail = 0
                else:
                    data_arr = data_str.split('\n'.encode('utf-8'))
                    data_json = json.loads(data_arr[0], encoding='utf-8')
                    uv_detail = data_json['uv_detail']
            else:
                output.output_info("get_detail_uv sql occurs errors, sql is %s" % sql)
            return uv_detail
        except Exception as e:
            output.output_info("sensors get_detail_uv occurs error, error is %s" % str(e))

    # 获取单日详情页pv总量
    def get_detail_pv(self, date, sptype):
        pv_detail = 0
        try:
            if sptype == 'total':
                sql = "select count(*) as pv_detail from(select src_page_type as sptype, " \
                      "case when src_page_type like '%_search%' then 'oldversion' else src_page_type " \
                      "end as type from events where event = 'ViewProduct' and date = '{0}' " \
                      "and src_page_type != '' and plumfrontend in ('IOS客户端', 'Android客户端'))ta " \
                      "where ta.type != 'oldversion'".format(date)
            else:
                sql = "select count(src_page_id) as pv_detail from events where event = 'ViewProduct' " \
                      "and date = '{0}' and src_page_type = '{1}' and plumfrontend " \
                      "in ('IOS客户端', 'Android客户端')".format(date, sptype)
            pay_load = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, pay_load)
            if r.status_code == 200:
                data_str = r.text.encode('utf-8')
                if len(data_str) == 0:
                    pv_detail = 0
                else:
                    data_arr = data_str.split('\n'.encode('utf-8'))
                    data_json = json.loads(data_arr[0], encoding='utf-8')
                    pv_detail = data_json['pv_detail']
            else:
                output.output_info("get_detail_pv sql occurs errors, sql is %s" % sql)
            return pv_detail
        except Exception as e:
            output.output_info("sensors get_detail_pv occurs error, error is %s" % str(e))

    # 获取单日详情页去重pv总量(去重单次曝光多次详情页点击的同一个商品)
    def get_detail_distinct_pv(self, date, sptype):
        pv_detail_distinct = 0
        try:
            if sptype == 'total':
                sql = "select count(*) as pv_detail_distinct from (select tb.sid, tb.productid, tb.type from " \
                      "(select * from(select sid, productid, src_page_type as sptype, " \
                      "case when src_page_type like '%_search%' then 'oldversion' else src_page_type end as type " \
                      "from events where event = 'ViewProduct' and date = '{0}' and src_page_type != '' and " \
                      "plumfrontend in ('IOS客户端', 'Android客户端'))ta where ta.type != 'oldversion')tb " \
                      "group by tb.sid, tb.productid, tb.type) tc".format(date)
            else:
                sql = "select count(*) as pv_detail_distinct from " \
                      "(select sid, productid from events where event = 'ViewProduct' and date = '{0}' and " \
                      "src_page_type = '{1}' and plumfrontend in ('IOS客户端', 'Android客户端') " \
                      "group by sid, productid)ta".format(date, sptype)
            pay_load = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, pay_load)
            if r.status_code == 200:
                data_str = r.text.encode('utf-8')
                if len(data_str) == 0:
                    pv_detail_distinct = 0
                else:
                    data_arr = data_str.split('\n'.encode('utf-8'))
                    data_json = json.loads(data_arr[0], encoding='utf-8')
                    pv_detail_distinct = data_json['pv_detail_distinct']
            else:
                output.output_info("get_detail_distinct_pv sql occurs errors, sql is %s" % sql)
            return pv_detail_distinct
        except Exception as e:
            output.output_info("sensors get_detail_distinct_pv occurs errors, error is %s" % str(e))

    # 获取单日详情页去重pv总量(单次曝光的多次详情页点击只计算一次)
    def get_detail_sid_pv(self, date, sptype):
        pv_detail_sid = 0
        try:
            if sptype == 'total':
                sql = "select count(*) as pv_detail_sid from " \
                      "(select ta.dsid, ta.type from(select distinct(sid) as dsid, src_page_type as sptype, " \
                      "case when src_page_type like '%_search%' then 'oldversion' else src_page_type end as type " \
                      "from events where event = 'ViewProduct' and date = '{0}' and src_page_type != '' and " \
                      "plumfrontend in ('IOS客户端', 'Android客户端'))ta where ta.type != 'oldversion') tb".format(date)
            else:
                sql = "select count(distinct sid) as pv_detail_sid from events where event = 'ViewProduct' and " \
                      "date = '{0}' and src_page_type = '{1}' and plumfrontend in ('IOS客户端', 'Android客户端')"\
                    .format(date, sptype)
            pay_load = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, pay_load)
            if r.status_code == 200:
                data_str = r.text.encode('utf-8')
                if len(data_str) == 0:
                    pv_detail_sid = 0
                else:
                    data_arr = data_str.split('\n'.encode('utf-8'))
                    data_json = json.loads(data_arr[0], encoding='utf-8')
                    pv_detail_sid = data_json['pv_detail_sid']
            else:
                output.output_info("get_detail_sid_pv sql occurs error, sql is %s" % sql)
            return pv_detail_sid
        except Exception as e:
            output.output_info("sensors get_detail_sid_pv occurs errors, error is %s" % str(e))

    # 获取单日列表加购数量
    def get_list_add_cart_num(self, date, sptype):
        list_add_cart_num = 0
        try:
            if sptype == 'total':
                sql = "select count(*) as list_add_cart_num from(select ta.sid, ta.pid, tb.sptype from " \
                      "(select sid, productid as pid from events where event = 'AddCart' and sid != '' " \
                      "and src_page_type = 'list' and date = '{0}' and plumfrontend in ('IOS客户端', 'Android客户端') " \
                      "group by sid, pid)ta left join (select sid, src_page_type as sptype from events " \
                      "where event in ('ViewProduct', 'ViewProductList') and date = '{1}' and sid != '' " \
                      "and plumfrontend in ('IOS客户端', 'Android客户端') group by sid, sptype)tb " \
                      "on ta.sid = tb.sid)tc where tc.sptype != ''".format(date, date)
            else:
                sql = "select tc.sptype as type, count(tc.sptype) as list_add_cart_num from(select ta.sid, ta.pid, " \
                      "tb.sptype from(select sid, productid as pid from events where event = 'AddCart' and sid != '' " \
                      "and src_page_type = 'list' and date = '{0}' and plumfrontend in ('IOS客户端', 'Android客户端') " \
                      "group by sid, pid)ta left join(select sid, src_page_type as sptype from events where event in " \
                      "('ViewProduct', 'ViewProductList') and date = '{1}' and sid != '' and plumfrontend " \
                      "in ('IOS客户端', 'Android客户端') group by sid, sptype)tb on ta.sid = tb.sid)tc " \
                      "where tc.sptype = '{2}' group by type".format(date, date, sptype)
            pay_load = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, pay_load)
            if r.status_code == 200:
                data_str = r.text.encode('utf-8')
                if len(data_str) == 0:
                    list_add_cart_num = 0
                else:
                    data_arr = data_str.split('\n'.encode(encoding='utf-8'))
                    data_json = json.loads(data_arr[0], encoding='utf-8')
                    list_add_cart_num = data_json['list_add_cart_num']
            else:
                output.output_info("get_list_add_cart_num sql occurs errors, the sql is %s" % sql)
            return list_add_cart_num
        except Exception as e:
            output.output_info("sensors get_list_add_cart_num occurs errors, the error is %s" % str(e))

    # 获取单日加购数量及商品折扣总价
    def get_add_cart_num_and_price(self, date, sptype):
        add_cart_num = 0
        total_cp = 0
        try:
            if sptype == 'total':
                sql = "select count(*) as add_cart_num, sum(tc.countprice) as total_cp from (select ta.sid, ta.pid, " \
                      "tb.sptype, ta.saleprice, ta.countprice from (select sid, productid as pid, productsale_price " \
                      "as saleprice, productdiscount_price as countprice from events where event = 'AddCart' and " \
                      "sid != '' and date = '{0}' and plumfrontend in ('IOS客户端', 'Android客户端') " \
                      "group by sid, pid, saleprice, countprice ) ta left join (select sid, src_page_type as sptype " \
                      "from events where event in ('ViewProduct', 'ViewProductList') and date = '{1}' " \
                      "and plumfrontend in ('IOS客户端', 'Android客户端') and sid != '' group by sid, sptype) tb " \
                      "on ta.sid = tb.sid) tc where tc.sptype != ''".format(date, date)
            else:
                sql = "select tc.sptype as type,count(tc.sptype) as add_cart_num,sum(tc.countprice) as total_cp from " \
                      "(select ta.sid, ta.pid, tb.sptype, ta.saleprice, ta.countprice from (select sid, " \
                      "productid as pid, productsale_price as saleprice, productdiscount_price as countprice " \
                      "from events where event = 'AddCart' and date = '{0}' and sid != '' and plumfrontend " \
                      "in ('IOS客户端', 'Android客户端') group by sid, pid, saleprice, countprice)ta left join " \
                      "(select sid, src_page_type as sptype from events where event " \
                      "in ('ViewProduct', 'ViewProductList') and date = '{1}' and sid != '' and plumfrontend " \
                      "in ('IOS客户端', 'Android客户端') group by sid, sptype) tb on ta.sid = tb.sid)tc " \
                      "where tc.sptype = '{2}' group by type".format(date, date, sptype)
            pay_load = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, pay_load)
            if r.status_code == 200:
                data_str = r.text.encode('utf-8')
                if len(data_str) == 0:
                    add_cart_num = 0
                    total_cp = 0
                else:
                    data_arr = data_str.split('\n'.encode('utf-8'))
                    data_json = json.loads(data_arr[0], encoding='utf-8')
                    add_cart_num = data_json['add_cart_num']
                    total_cp = data_json['total_cp']
            else:
                output.output_info("get_add_cart_num_and_price sql occurs errors, the sql is %s" % sql)
            return add_cart_num, total_cp
        except Exception as e:
            output.output_info("sensors get_add_cart_num_and_price occurs errors, the error is %s" % str(e))

    # 获取单日订单uv数
    def get_order_uv(self, lastdate, date, sptype):
        order_uv = 0
        try:
            if sptype == 'total':
                sql = "SELECT count(distinct(tg.uid)) as orderuv FROM (SELECT tf.sptype,tf.oid,tf.uid FROM " \
                      "(SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,row_number ( ) over ( PARTITION BY te.pid "\
                      "ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf FROM " \
                      "(SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time," \
                      "distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND " \
                      "date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY " \
                      "sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE " \
                      "EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND " \
                      "plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid) tc " \
                      "LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid FROM EVENTS WHERE " \
                      "EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) " \
                      "GROUP BY oid,pid,uid) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf " \
                      "GROUP BY tf.sptype,tf.oid,tf.uid) tg " \
                      "WHERE tg.sptype != ''".format(lastdate, date, lastdate, date, date)
            else:
                sql = "SELECT count(distinct(tg.uid)) as orderuv FROM (SELECT tf.sptype,tf.oid,tf.uid FROM " \
                      "(SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,row_number ( ) over ( PARTITION BY " \
                      "te.pid ORDER BY te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf " \
                      "FROM (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid," \
                      "time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != ''" \
                      " AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) " \
                      "GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS" \
                      " WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND " \
                      "sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON " \
                      "ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid " \
                      "FROM EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN " \
                      "( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid) td ON tc.pid = td.pid AND " \
                      "tc.uid = td.uid) te WHERE te.oid > 0) tf GROUP BY tf.sptype,tf.oid,tf.uid) tg " \
                      "WHERE tg.sptype = '{5}'".format(lastdate, date, lastdate, date, date, sptype)
            pay_load = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, pay_load)
            if r.status_code == 200:
                data_str = r.text.encode('utf-8')
                if len(data_str) == 0:
                    order_uv = 0
                else:
                    data_arr = data_str.split('\n'.encode('utf-8'))
                    data_json = json.loads(data_arr[0], encoding='utf-8')
                    order_uv = data_json['orderuv']
            else:
                output.output_info("get_order_uv sql occurs errors, the sql is %s" % sql)
            return order_uv
        except Exception as e:
            output.output_info("sensors get_order_uv occurs errors, thr error is %s" % str(e))

    # 获取单日订单数量(订单维度)
    def get_order_num(self, lastdate, date, sptype):
        order_num = 0
        try:
            if sptype == 'total':
                sql = "SELECT count(*) as ordernum FROM (SELECT tf.sptype,tf.oid,tf.uid FROM (SELECT te.uid,te.pid," \
                      "te.oid,te.time,te.sptype,te.pf,row_number ( ) over ( PARTITION BY te.pid ORDER BY " \
                      "te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf FROM " \
                      "(SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time," \
                      "distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND" \
                      " date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY" \
                      " sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE" \
                      " EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' " \
                      "AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid)" \
                      " tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid FROM EVENTS WHERE" \
                      " EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' )" \
                      " GROUP BY oid,pid,uid) td ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf" \
                      " GROUP BY tf.sptype,tf.oid,tf.uid) tg " \
                      "WHERE tg.sptype != '' ".format(lastdate, date, lastdate, date, date)
            else:
                sql = "SELECT count(*) as ordernum FROM (SELECT tf.sptype,tf.oid,tf.uid FROM (SELECT te.uid,te.pid," \
                      "te.oid,te.time,te.sptype,te.pf,row_number ( ) over ( PARTITION BY te.pid ORDER BY" \
                      " te.time desc ) rk FROM (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf FROM" \
                      " (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid,time," \
                      "distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != '' AND" \
                      " date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY" \
                      " sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE" \
                      " EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != ''" \
                      " AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid =" \
                      " tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid FROM" \
                      " EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端'," \
                      " 'Android客户端' ) GROUP BY oid,pid,uid) td ON tc.pid = td.pid AND tc.uid = td.uid) te" \
                      " WHERE te.oid > 0) tf GROUP BY tf.sptype,tf.oid,tf.uid) tg" \
                      " WHERE tg.sptype = '{5}' ".format(lastdate, date, lastdate, date, date, sptype)
            pay_load = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, pay_load)
            if r.status_code == 200:
                data_str = r.text.encode('utf-8')
                if len(data_str) == 0:
                    order_num = 0
                else:
                    data_arr = data_str.split('\n'.encode('utf-8'))
                    data_json = json.loads(data_arr[0], encoding='utf-8')
                    order_num = data_json['order_num']
            else:
                output.output_info("get_order_num sql occurs errors, the sql is %s" % sql)
            return order_num
        except Exception as e:
            output.output_info("sensors get_order_num occurs errors, the error is %s" % str(e))

    # 获取单日订单数量(商品维度)及商品折扣总价和用户实付总价
    def get_pay_order_detail_num_and_price(self, lastdate, date, sptype):
        pay_order_num = 0
        total_cp = 0
        total_pp = 0
        try:
            if sptype == 'total':
                sql = "SELECT count(tf.sptype) as payordernum, sum(tf.countprice) as totalcp,sum(tf.payprice)" \
                      " as totalpp FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,te.countprice," \
                      "te.payprice, row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM" \
                      " (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf,td.countprice,td.payprice FROM" \
                      " (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid," \
                      "time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != ''" \
                      " AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY" \
                      " sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM EVENTS WHERE EVENT" \
                      " in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}' AND sid != '' AND" \
                      " plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb ON ta.sid = tb.sid)" \
                      " tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid," \
                      "orderitem_discountprice as countprice, orderitem_realpayprice as payprice FROM" \
                      " EVENTS WHERE EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN " \
                      "( 'IOS客户端', 'Android客户端' ) GROUP BY oid,pid,uid,countprice,payprice) td" \
                      " ON tc.pid = td.pid AND tc.uid = td.uid) te WHERE te.oid > 0) tf WHERE rk = 1 AND" \
                      " tf.sptype != ''".format(lastdate, date, lastdate, date, date)
            else:
                sql = "SELECT count(tf.sptype) as payordernum,sum(tf.countprice) as totalcp,sum(tf.payprice)" \
                      " as totalpp FROM (SELECT te.uid,te.pid,te.oid,te.time,te.sptype,te.pf,te.countprice," \
                      "te.payprice, row_number ( ) over ( PARTITION BY te.pid ORDER BY te.time desc ) rk FROM" \
                      " (SELECT tc.pid,tc.time,tc.sptype,td.oid,tc.uid,tc.pf,td.payprice,td.countprice FROM" \
                      " (SELECT ta.sid,ta.pid,ta.time,tb.sptype,ta.uid,ta.pf FROM (SELECT sid,productid as pid," \
                      "time,distinct_ID as uid,plumfrontend as pf FROM EVENTS WHERE EVENT = 'AddCart' AND sid != ''" \
                      " AND date BETWEEN '{0}' AND '{1}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' )" \
                      " GROUP BY sid,pid,time,uid,pf) ta LEFT JOIN (SELECT sid,src_page_type as sptype FROM" \
                      " EVENTS WHERE EVENT in ('ViewProductList','ViewProduct') AND date BETWEEN '{2}' AND '{3}'" \
                      " AND sid != '' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' ) GROUP BY sid,sptype)tb" \
                      " ON ta.sid = tb.sid) tc LEFT JOIN (SELECT orderid as oid,productid as pid,distinct_ID as uid," \
                      "orderitem_discountprice as countprice, orderitem_realpayprice as payprice FROM EVENTS WHERE" \
                      " EVENT = 'PayOrderDetail' AND date = '{4}' AND plumfrontend IN ( 'IOS客户端', 'Android客户端' )" \
                      " GROUP BY oid,pid,uid,countprice, payprice) td ON tc.pid = td.pid AND tc.uid = td.uid) te" \
                      " WHERE te.oid > 0) tf" \
                      " WHERE rk = 1 AND tf.sptype = '{5}'".format(lastdate, date, lastdate, date, date, sptype)
            pay_load = {'q': sql, 'format': 'json'}
            r = requests.post(self.url, pay_load)
            if r.status_code == 200:
                data_str = r.text.encode('utf-8')
                if len(data_str) == 0:
                    pay_order_num = 0
                    total_cp = 0
                    total_pp = 0
                else:
                    data_arr = data_str.split('\n'.encode('utf-8'))
                    data_json = json.loads(data_arr[0], encoding='utf-8')
                    pay_order_num = data_json['payordernum']
                    total_cp = data_json['totalcp']
                    total_pp = data_json['totalpp']
            else:
                output.output_info("get_pay_order_detail_num_and_price sql occurs errors, the sql is %s" % sql)
            return pay_order_num, total_cp, total_pp
        except Exception as e:
            output.output_info("sensors get_pay_order_detail_num_and_price occurs errors, the error is %s" % str(e))
