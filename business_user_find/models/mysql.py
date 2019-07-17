#!/usr/bin/env python
# coding=utf-8
from datetime import datetime, date, timedelta, time
import MySQLdb
from operator import itemgetter


class MysqlOperator(object):
    def __init__(self, config):
        self.host = config.get("MYSQL", "host")
        self.port = 3306
        self.user = config.get("MYSQL", "user")
        self.password = config.get("MYSQL", "password")
        self.database = config.get("MYSQL", "database")

    def create_connection(self):
        conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, password=self.password, charset='utf8')
        conn.select_db(self.database)
        return conn

    def get_uid_list(self):
        untildate = date.today() + timedelta(days=-1)
        realprice = 5000
        product_count = 15
        sql = "select tmp.用户,tmp.商品数 from (SELECT td.user_id as 用户,count(distinct FROM_UNIXTIME(td.order_time,'%Y-%m')) as 下单月份数,count(distinct case when td.parent_id=0 then td.id else td.parent_id end) as 订单数,count(distinct case when td.parent_id=0 then td.id else td.parent_id end) /count(distinct FROM_UNIXTIME(td.order_time,'%Y-%m')) as 平均每月下单数,count(tdi.product_id)/count(distinct case when td.parent_id=0 then td.id else td.parent_id end) as 平均订单商品数,count(tdi.product_id) as 商品数,sum(tdi.realpay_price) /count(tdi.product_id) as 平均商品均价,sum(tdi.realpay_price) as 销售额 FROM t_order td LEFT JOIN t_order_item tdi on tdi.order_id=td.id where td.status not in ('new','topay','cancel') and td.splitted='0' and td.order_time<UNIX_TIMESTAMP('{0}') AND td.user_id in (select td.user_id FROM t_order td LEFT JOIN t_order_item tdi on tdi.order_id=td.id where td.status  not in ('new','topay','cancel') and td.order_time<UNIX_TIMESTAMP('{1}') and td.splitted='0' group by td.user_id HAVING sum(tdi.realpay_price)>={2}) GROUP BY td.user_id order by sum(tdi.realpay_price) DESC) tmp where tmp.商品数 > {3}".format(
            untildate, untildate, realprice, product_count)

        try:
            conn = MysqlOperator.create_connection(self)
            cursor = conn.cursor()
            cursor.execute(sql)
            result = cursor.fetchall()
            cursor.close()
            conn.close()
            print(2)
        except Exception as e:
            return str(e)

        uid_productscount_dict = dict()
        uid_list = list()
        dict_list = list()
        if bool(result) != True:
            print("未查询到任何结果")
        else:
            for row in result:
                # print(row[0])
                uid_list.append(row[0])
                uid_productscount_dict.update({row[0]: row[1]})
        dict_list.append(uid_productscount_dict)
        dict_list.append(uid_list)
        # print(len(uid_productscount_dict))
        # print(len(dict_list))
        return dict_list

    def save_data_tomysql(self, lines: list):

        lines_length = len(lines)
        print(lines_length)
        num = 0
        pids = ""
        for line in lines:
            num += 1
            fields = line.split(",")
            if num == lines_length:
                pids = pids + "'" + str(fields[3]) + "'"
            else:
                pids = pids + "'" + str(fields[3]) + "'" + ","



        try:
            conn = MysqlOperator.create_connection(self)
            cursor = conn.cursor()
            sql = "select id as pid,category_id as cid from t_product where id in ({0})".format(pids)
            cursor.execute(sql)
            result = cursor.fetchall()
            cursor.close()
            conn.close()
            # print("jianlilianjie")
        except Exception as e:
            return str(e)

        # print(len(lines))

        result_dict = dict()
        filename = '/home/aplum/tmp/result.csv'
        cid = ""
        for line in lines:
            # line = str(line).strip()
            fields = line.split(",")
            uid = fields[0]
            products_count = int(fields[1])
            view_count = fields[2]
            # 如果统计次数为0,跳过该用户
            # if view_count == 0:
            #     continue
            # print("view_count: " + view_count)
            pid = str(fields[3])
            uid_to_list = []
            for tup in result:
                if pid == str(tup[0]):
                    cid = str(tup[1])
                    break

            # print("sql之后")
            if result_dict.get(uid):
                if result_dict.get(uid)[3].get(cid):
                    tmp = result_dict.get(uid)[3].get(cid)
                    tmp += 1
                    tmp_dict = result_dict.get(uid)[3]
                    tmp_dict.update({cid: tmp})
                else:
                    tmp_dict = result_dict.get(uid)[3]
                    tmp_dict.update({cid: 1})
            else:
                uid_to_list.append(products_count)
                uid_to_list.append(view_count)
                uid_to_list.append(pid)
                cid_count = {cid: 1}
                uid_to_list.append(cid_count)
                result_dict.update({uid: uid_to_list})

            # if result_dict.get(uid):
            #     if result_dict.get(uid)[3].get(cid):
            #         tmp = result_dict.get(uid)[3].get(cid)
            #         tmp += 1
            #         result_dict.get(uid)[3].update({cid: tmp})
            #         # result_dict.update(
            #         #     {uid: [products_count, view_count, pid, {cid: tmp}]})
            #         # if result_dict.get(uid)[3].get(cid) > 1:
            #         #     print(cid,result_dict.get(uid)[3].get(cid))
            #     else:
            #         result_dict.update({uid: [products_count, view_count, pid, {cid: 1}]})
            # else:
            #     result_dict.update({uid: [products_count, view_count, pid, {cid: 1}]})

        with open(filename, "w") as f:
            f.write("uid, products_count, view_count, pid, cid, cid_count, cid_rate\n")
            filelines = list()
            # print(len(result_dict))
            sum_count = 0
            for key in result_dict.keys():
                # 3277, 2852, 0, '2458527
                # ',((2458527, 74),),0.0003506311360448808
                uid = key
                products_count = result_dict[key][0]
                view_count = result_dict[key][1]
                pid = result_dict[key][2]
                cid_dict = result_dict[key][3]
                count = 0
                for cid_key in cid_dict.keys():
                    count += 1
                    cid = cid_key
                    cid_count = cid_dict[cid_key]
                    cid_rate = cid_count / products_count
                    # 比例小于0.8 跳过
                    # if cid_rate < 0.8:  无结果  比例太高
                    #     continue
                    line = str(uid) + "," + str(products_count) + "," + str(view_count) + "," + \
                           str(pid) + "," + str(cid) + "," + str(cid_count) + "," + str(cid_rate) + "\n"
                    filelines.append(line)
                sum_count += count
            # print(len(filelines))
            # print(sum_count)

            sorted_lines = sorted(filelines, key=itemgetter(6), reverse=True)
            f.writelines(sorted_lines)
            f.close()
