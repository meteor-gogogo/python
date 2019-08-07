#!/usr/bin/env python
# coding=utf-8
from elasticsearch import Elasticsearch
from datetime import timedelta, date
import MySQLdb
import os
from elasticsearch import helpers
import redis


# redis测试库
r = redis.Redis(host='r-2zec5074fca1ade4.redis.rds.aliyuncs.com', port='6379', password='dL5pr5RkJBQEuqj', db=2, decode_responses=True, charset='utf-8')

statmysqlhost = 'pc-2ze47js669jk7k187.rwlb.rds.aliyuncs.com'
statmysqlusername = 'plumdb'
statmysqlpasswd = 'plumdb@2019'
statdb = 'aplum_stat'

mysqlhost = 'rr-2ze7824567d8dgu1y.mysql.rds.aliyuncs.com'
mysqlusername = 'plumdb'
mysqlpasswd = 'plumdb@2018'
mysqldb = 'aplum'

# es = Elasticsearch(["es-cn-0pp0vm2ju000a003g.elasticsearch.aliyuncs.com"], http_auth=('elastic', 'NHRHpqeFn8'),
#                    port=9200)
EsCTR = Elasticsearch(["es-cn-0pp0vm2ju000a003g.elasticsearch.aliyuncs.com"], http_auth=('elastic', 'NHRHpqeFn8'),
                          port=9200)
url = "http://websa.aplum-inc.com/api/sql/query?token=15ba27be5b7895379a0574993db783281988c16f95969fe423847d31ed95728d"

# start = time.time()
# tic = lambda: 'at %1.1f seconds' % (time.time() - start)


def get_bid_sale_price_dict(bid, sale_price, is_click, ctr_dict):
    # for row in click_list:
    #     row_arr = row.split(':')
    #     is_click = int(row_arr[0])
    #     bid = str(row_arr[9])
    #     sale_price = str(row_arr[6])
    #     key = bid + '-' + sale_price
    #     if key == '':
    #         continue
    key = bid + '-' + sale_price
    bid_sale_price_pv_key = 'gbdt:16:' + key + ':pv'
    bid_sale_price_pv_value_current = ctr_dict.get(bid_sale_price_pv_key, 0)
    bid_sale_price_pv_value = 1 + bid_sale_price_pv_value_current
    ctr_dict.update({bid_sale_price_pv_key: bid_sale_price_pv_value})
    if is_click == 1:
        bid_sale_price_click_key = 'gbdt:16:' + key + ':click'
        bid_sale_price_click_value_current = ctr_dict.get(bid_sale_price_click_key, 0)
        bid_sale_price_click_value = 1 + bid_sale_price_click_value_current
        ctr_dict.update({bid_sale_price_click_key: bid_sale_price_click_value})
    return ctr_dict


def get_bid_discount_rate_dict(bid, discount_rate, is_click, ctr_dict):
    # for row in click_list:
    #     row_arr = row.split(':')
    #     is_click = int(row_arr[0])
    #     bid = str(row_arr[9])
    #     discount_rate = str(row_arr[8])
    #     key = bid + '-' + discount_rate
    #     if key == '':
    #         continue
    key = bid + '-' + discount_rate
    bid_discount_rate_pv_key = 'gbdt:15:' + key + ':pv'
    bid_discount_rate_pv_value_current = ctr_dict.get(bid_discount_rate_pv_key, 0)
    bid_discount_rate_pv_value = 1 + bid_discount_rate_pv_value_current
    ctr_dict.update({bid_discount_rate_pv_key: bid_discount_rate_pv_value})
    if is_click == 1:
        bid_discount_rate_click_key = 'gbdt:15:' + key + ':click'
        bid_discount_rate_click_value_current = ctr_dict.get(bid_discount_rate_click_key, 0)
        bid_discount_rate_click_value = 1 + bid_discount_rate_click_value_current
        ctr_dict.update({bid_discount_rate_click_key: bid_discount_rate_click_value})
    return ctr_dict


def get_bid_discount_price_dict(bid, discount_price, is_click, ctr_dict):
    # for row in click_list:
    #     row_arr = row.split(':')
    #     is_click = int(row_arr[0])
    #     bid = str(row_arr[9])
    #     discount_price = str(row_arr[7])
    #     key = bid + '-' + discount_price
    #     if key == '':
    #         continue
    key = bid + '-' + discount_price
    bid_discount_price_pv_key = 'gbdt:14:' + key + ':pv'
    bid_discount_price_pv_value_current = ctr_dict.get(bid_discount_price_pv_key, 0)
    bid_discount_price_pv_value = 1 + bid_discount_price_pv_value_current
    ctr_dict.update({bid_discount_price_pv_key: bid_discount_price_pv_value})
    if is_click == 1:
        bid_discount_price_click_key = 'gbdt:14:' + key + ':click'
        bid_discount_price_click_value_current = ctr_dict.get(bid_discount_price_click_key, 0)
        bid_discount_price_click_value = 1 + bid_discount_price_click_value_current
        ctr_dict.update({bid_discount_price_click_key: bid_discount_price_click_value})
    return ctr_dict


def get_bid_original_price_dict(bid, original_price, is_click, ctr_dict):
    # for row in click_list:
    #     row_arr = row.split(':')
    #     is_click = int(row_arr[0])
    #     bid = str(row_arr[9])
    #     original_price = str(row_arr[5])
    #     key = bid + '-' + original_price
    #     if key == '':
    #         continue
    key = bid + '-' + original_price
    bid_original_price_pv_key = 'gbdt:13:' + key + ':pv'
    bid_original_price_pv_value_current = ctr_dict.get(bid_original_price_pv_key, 0)
    bid_original_price_pv_value = 1 + bid_original_price_pv_value_current
    ctr_dict.update({bid_original_price_pv_key: bid_original_price_pv_value})
    if is_click == 1:
        bid_original_price_click_key = 'gbdt:13:' + key + ':click'
        bid_original_price_click_value_current = ctr_dict.get(bid_original_price_click_key, 0)
        bid_original_price_click_value = 1 + bid_original_price_click_value_current
        ctr_dict.update({bid_original_price_click_key: bid_original_price_click_value})
    return ctr_dict


# def get_status_dict(status, is_click, ctr_dict):
#     # onsale_num_pv = 0
#     # onsale_num_click = 0
#     # sold_num_pv = 0
#     # sold_num_click = 0
#     # for row in click_list:
#     #     row_arr = row.split(':')
#     #     is_click = int(row_arr[0])
#     #     status = int(row_arr[3])
#     #     if status:
#     #         onsale_num_pv += 1
#     #         if is_click:
#     #             onsale_num_click += 1
#     #         else:
#     #             pass
#     #     else:
#     #         sold_num_pv += 1
#     #         if is_click:
#     #             sold_num_click += 1
#     #         else:
#     #             pass
#     #     if status not in ['onsale', 'sold']:
#     #         continue
#     #     if status == 'onsale':
#     #         onsale_num_pv += 1
#     #     else:
#     #         sold_num_pv += 1
#     # status_onsale_pv_key = 'gbdt:13:onsale:pv'
#     # status_sold_pv_key = 'gbdt:13:sold:pv'
#     # ctr_dict.update({status_onsale_pv_key: onsale_num_pv})
#     # ctr_dict.update({status_sold_pv_key: sold_num_pv})
#     #
#     # status_onsale_click_key = 'gbdt:13:onsale:click'
#     # status_sold_click_key = 'gbdt:13:sold:click'
#     # ctr_dict.update({status_onsale_click_key: onsale_num_click})
#     # ctr_dict.update({status_sold_click_key: sold_num_click})
#     status_pv_key = 'gbdt:13:' + status + ':pv'
#     status_pv_value_current = ctr_dict.get(status_pv_key, 0)
#     status_pv_value = 1 + status_pv_value_current
#     ctr_dict.update({status_pv_key: status_pv_value})
#     if is_click == 1:
#         status_click_key = 'gbdt:13:' + status + ':click'
#         status_click_value_current = ctr_dict.get(status_click_key, 0)
#         status_click_value = 1 + status_click_value_current
#         ctr_dict.update({status_click_key: status_click_value})
#     return ctr_dict


def get_discount_rate_dict(discount_rate, is_click, ctr_dict):
    # for row in click_list:
    #     row_arr = row.split(':')
    #     is_click = int(row_arr[0])
    #     discount_rate = str(row_arr[8])
    #     if discount_rate == '':
    #         continue
    discount_rate_pv_key = 'gbdt:12:' + discount_rate + ':pv'
    discount_rate_pv_value_current = ctr_dict.get(discount_rate_pv_key, 0)
    discount_rate_pv_value = 1 + discount_rate_pv_value_current
    ctr_dict.update({discount_rate_pv_key: discount_rate_pv_value})
    if is_click == 1:
        discount_rate_click_key = 'gbdt:12:' + discount_rate + ':click'
        discount_rate_click_value_current = ctr_dict.get(discount_rate_click_key, 0)
        discount_rate_click_value = 1 + discount_rate_click_value_current
        ctr_dict.update({discount_rate_click_key: discount_rate_click_value})
    return ctr_dict


def get_discount_price_dict(discount_price, is_click, ctr_dict):
    # for row in click_list:
    #     row_arr = row.split(':')
    #     is_click = int(row_arr[0])
    #     discount_price = str(row_arr[7])
    #     if discount_price == '':
    #         continue
    discount_price_pv_key = 'gbdt:11:' + discount_price + ':pv'
    discount_price_pv_value_current = ctr_dict.get(discount_price_pv_key, 0)
    discount_price_pv_value = 1 + discount_price_pv_value_current
    ctr_dict.update({discount_price_pv_key: discount_price_pv_value})
    if is_click == 1:
        discount_price_click_key = 'gbdt:11:' + discount_price + ':click'
        discount_price_click_value_current = ctr_dict.get(discount_price_click_key, 0)
        discount_price_click_value = 1 + discount_price_click_value_current
        ctr_dict.update({discount_price_click_key: discount_price_click_value})
    return ctr_dict


def get_sale_price_dict(sale_price, is_click, ctr_dict):
    # for row in click_list:
    #     row_arr = row.split(':')
    #     is_click = int(row_arr[0])
    #     sale_price = str(row_arr[6])
    #     if sale_price == '':
    #         continue
    sale_price_pv_key = 'gbdt:10:' + sale_price + ':pv'
    sale_price_pv_value_current = ctr_dict.get(sale_price_pv_key, 0)
    sale_price_pv_value = 1 + sale_price_pv_value_current
    ctr_dict.update({sale_price_pv_key: sale_price_pv_value})
    if is_click == 1:
        sale_price_click_key = 'gbdt:10:' + sale_price + ':click'
        sale_price_click_value_current = ctr_dict.get(sale_price_click_key, 0)
        sale_price_click_value = 1 + sale_price_click_value_current
        ctr_dict.update({sale_price_click_key: sale_price_click_value})
    return ctr_dict


def get_original_price_dict(original_price, is_click, ctr_dict):
    original_price_pv_key = 'gbdt:9:' + original_price + ':pv'
    original_price_pv_value_current = ctr_dict.get(original_price_pv_key, 0)
    original_price_pv_value = 1 + original_price_pv_value_current
    ctr_dict.update({original_price_pv_key: original_price_pv_value})
    if is_click == 1:
        original_price_click_key = 'gbdt:9:' + original_price + ':click'
        original_price_click_value_current = ctr_dict.get(original_price_click_key, 0)
        original_price_click_value = 1 + original_price_click_value_current
        ctr_dict.update({original_price_click_key: original_price_click_value})
    return ctr_dict


def get_is_promotion_dict(is_promotion, is_click, ctr_dict):
    promotion_pv_key = 'gbdt:8:' + is_promotion + ':pv'
    promotion_pv_value_current = ctr_dict.get(promotion_pv_key, 0)
    promotion_pv_value = 1 + promotion_pv_value_current
    ctr_dict.update({promotion_pv_key: promotion_pv_value})
    if is_click == 1:
        promotion_click_key = 'gbdt:8:' + is_promotion + ':click'
        promotion_click_value_current = ctr_dict.get(promotion_click_key, 0)
        promotion_click_value = 1 + promotion_click_value_current
        ctr_dict.update({promotion_click_key: promotion_click_value})
    return ctr_dict


def get_degree_dict(degree, is_click, ctr_dict):
    degree_pv_key = 'gbdt:6:' + degree + ':pv'
    degree_pv_value_current = ctr_dict.get(degree_pv_key, 0)
    degree_pv_value = 1 + degree_pv_value_current
    ctr_dict.update({degree_pv_key: degree_pv_value})
    if is_click == 1:
        degree_click_key = 'gbdt:6:' + degree + ':click'
        degree_click_value_current = ctr_dict.get(degree_click_key, 0)
        degree_click_value = 1 + degree_click_value_current
        ctr_dict.update({degree_click_key: degree_click_value})
    return ctr_dict


def read_click_date(file_path, lastdate, duration):
    click_list = list()
    current_file = str(lastdate + timedelta(days=duration)) + ".product_view_click.csv"
    file = os.path.join('%s%s' % (file_path, current_file))
    print(file)
    with open(file, 'r', encoding='utf-8') as f:
        next(f)
        for line in f:
            line_arr = line.split(",")
            is_click = str(line_arr[1])
            is_blackcard_member = str(line_arr[18])
            position = str(line_arr[20])
            status = str(line_arr[27])
            id = str(line_arr[5])
            original_price = str(line_arr[15])
            sale_price = str(line_arr[16])
            discount_price = str(line_arr[17])
            discount_rate = str(line_arr[9])
            bid = str(line_arr[10])
            source_of_supply = str(line_arr[12])
            degree = str(line_arr[13])
            is_promotion = str(line_arr[14])
            click_event = is_click + ':' + is_blackcard_member + ':' + position + ':' + status + ':' + id + ':' + original_price + ':' + sale_price + ':' + discount_price + ':' + discount_rate + ':' + bid + ':' + source_of_supply + ':' + degree + ':' + is_promotion
            click_event = process_price(click_event)
            click_list.append(click_event)
    return click_list


def isBlackcardMember(uid, timestamp, userDict):
    if uid in userDict.keys():
        arr = userDict[uid]
        for payinfo in arr:
            print(payinfo)
            if payinfo == '':
                continue
            duration = 86400*365
            if payinfo['card_type'] == '':
                continue
            if payinfo['card_type'] == 'month':
                duration = 86400*91
            if payinfo['create_time'] <= timestamp and timestamp <= payinfo['create_time'] + duration:
                return 1
    return 0


def get_is_blackcard_member_dict(is_click, is_blackcard_member, ctr_dict):
    blackcard_click = 0
    blackcard_pv = 0
    no_blackcard_click = 0
    no_blackcard_pv = 0
    if is_blackcard_member == 1:
        blackcard_pv += 1
        if is_click:
            blackcard_click += 1
        else:
            pass
    else:
        no_blackcard_pv += 1
        if is_click:
            no_blackcard_click += 1
        else:
            pass
    blackcard_member_pv_key = 'gbdt:5:1:pv'
    ctr_dict.update({blackcard_member_pv_key: blackcard_pv})
    blackcard_member_pv_key = 'gbdt:5:0:pv'
    ctr_dict.update({blackcard_member_pv_key: no_blackcard_pv})
    blackcard_member_pv_key = 'gbdt:5:1:click'
    ctr_dict.update({blackcard_member_pv_key: blackcard_click})
    blackcard_member_pv_key = 'gbdt:5:0:click'
    ctr_dict.update({blackcard_member_pv_key: no_blackcard_click})
    return ctr_dict


def get_source_of_supply_dict(source, is_click, ctr_dict):
    source_pv_key = 'gbdt:4:' + source + ':pv'
    source_pv_value_current = ctr_dict.get(source_pv_key, 0)
    source_pv_value = 1 + source_pv_value_current
    ctr_dict.update({source_pv_key: source_pv_value})
    if is_click == 1:
        source_click_key = 'gbdt:4:' + source + ':click'
        source_click_value_current = ctr_dict.get(source_click_key, 0)
        source_click_value = 1 + source_click_value_current
        ctr_dict.update({source_click_key: source_click_value})
    return ctr_dict


def get_bid_dict(ctr_data, ctr_dict):
    for row in ctr_data:
        bid = str(row['brand_id'])
        click = row['pv_detail']
        pv = row['pv_show']
        bid_click_key = 'gbdt:3:' + bid + ':click'
        bid_click_value_current = ctr_dict.get(bid_click_key, 0)
        bid_click_value = click + bid_click_value_current
        bid_pv_key = 'gbdt:3:' + bid + ':pv'
        bid_pv_value_current = ctr_dict.get(bid_pv_key, 0)
        bid_pv_value = pv + bid_pv_value_current
        ctr_dict.update({bid_click_key: bid_click_value})
        ctr_dict.update({bid_pv_key: bid_pv_value})
    return ctr_dict


def get_cid_dict(ctr_data, ctr_dict):
    for row in ctr_data:
        category_id = str(row['category_id'])
        pv = row['pv_show']
        click = row['pv_detail']
        cid_click_key = 'gbdt:2:' + category_id + ':click'
        cid_click_value_current = ctr_dict.get(cid_click_key, 0)
        cid_click_value = click + cid_click_value_current
        cid_pv_key = 'gbdt:2:' + category_id + ':pv'
        cid_pv_value_current = ctr_dict.get(cid_pv_key, 0)
        cid_pv_value = pv + cid_pv_value_current
        ctr_dict.update({cid_click_key: cid_click_value})
        ctr_dict.update({cid_pv_key: cid_pv_value})
    return ctr_dict


def get_pid_dict(ctr_data, ctr_dict):
    for row in ctr_data:
        pid = str(row['product_id'])
        click = row['pv_detail']
        pv = row['pv_show']
        pid_click_key = 'gbdt:1:' + pid + ':click'
        pid_click_value_current = ctr_dict.get(pid_click_key, 0)
        pid_click_value = click + pid_click_value_current
        pid_pv_key = 'gbdt:1:' + pid + ':pv'
        pid_pv_value_current = ctr_dict.get(pid_pv_key, 0)
        pid_pv_value = pv + pid_pv_value_current
        ctr_dict.update({pid_click_key: pid_click_value})
        ctr_dict.update({pid_pv_key: pid_pv_value})
    return ctr_dict

# ceshi


def get_position_dict(city_name, is_click, ctr_dict):
    position_pv_key = 'gbdt:7:' + city_name + ':pv'
    position_pv_value_current = ctr_dict.get(position_pv_key, 0)
    position_pv_value = 1 + position_pv_value_current
    ctr_dict.update({position_pv_key: position_pv_value})
    if is_click == 1:
        position_click_key = 'gbdt:7:' + city_name + ':click'
        position_click_value_current = ctr_dict.get(position_click_key, 0)
        position_click_value = 1 + position_click_value_current
        ctr_dict.update({position_click_key: position_click_value})
    return ctr_dict


def get_data_from_sql(db_product):
    product_dict = dict()
    cursor = db_product.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    sql = "SELECT id, discount_rate FROM t_product"
    cursor.execute(sql)
    product_data = cursor.fetchall()
    cursor.close()
    for row in product_data:
        # row = process_price(row)
        pid = str(row['id'])
        product_dict.update({pid: row})
    return product_dict


def get_ctr_data_from_sql(db_stat, lastdate, yestoday):
    cursor = db_stat.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    sql = "SELECT product_id, brand_id, category_id, original_price, avg_sale_price, avg_discount_price, pv_show, " \
          "pv_detail FROM product_ctr_daily WHERE stat_date BETWEEN '{0}' AND '{1}'".format(lastdate, yestoday)
    cursor.execute(sql)
    ctr_data = cursor.fetchall()
    cursor.close()
    return ctr_data


def GetESData(lastdate):
    # es_dict = dict()
    CTRINDEX = 'aplum_ctr-{0}'.format(lastdate)
    ''' 获取查询每天曝光数据的sql语句 '''
    es_search_options = {
       "query": {
            "match_all": {}
        },
        # 对于命中的目标手动指定返回哪些字段
        "_source": ["X-Pd-Identify", "ItemActions", "ClientIP", "Createtime", "Status"]
    }
    _searched = helpers.scan(
        client=EsCTR,
        query=es_search_options,
        #保持游标开启10分钟,游标不适合实时搜索,更适合于后台批处理任务,性能优于深分页
        scroll='10m',
        index=CTRINDEX,
        doc_type="product",
        timeout="1m"
    )
    data = list()
    for hit in _searched:
        source = hit['_source']
        pid = str(source['ItemActions'])
        # if pid == '':
        #     continue
        uid = str(source['X-Pd-Identify'])
        # if uid == '':
        #     continue
        timestamp = str(source['Createtime'])
        # if timestamp == '':
        #     continue
        status = str(source['Status'])
        # if status == '':
        #     continue
        ip = str(source['ClientIP'])
        # if ip == '':
        #     continue
        data_tmp = pid + ':' + uid + ':' + timestamp + ':' + status + ':' + ip
        data.append(data_tmp)
    return data


def getUserDict(db_product):
    userDict = dict()
    userSQL ="SELECT user_id,card_type,create_time FROM aplum.t_discountcard_payinfo where trans_id!='' and refund_status=0"
    try:
        cursor = db_product.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        cursor.execute(userSQL)
        results = cursor.fetchall()
        for row in results:
            userDict[str(row['user_id'])] = row
    except Exception as e:
        print("Error:unable to fetch user data, error=%s" % (str(e),))
    return userDict


def save_dict_to_csv(dict_sum):
    today = date.today()
    with open('/home/aplum/work_lh/to_redis_data/{0}-day_dict.csv'.format(today), 'a+', newline='') as file:
        for x in dict_sum.items():
            file.write(str(x) + '\n')

def process_price(row):
    row_arr = row.split(':')
    # original_price = row_arr[5]
    # sale_price = row_arr[6]
    # discount_price = row_arr[7]
    val = int(int(float(row_arr[5])) / 100)
    if val == 0:
        val = 1
    original_price = str(float(val) / 10)

    val = int(int(float(row_arr[6])) / 100)
    if val == 0:
        val = 1
    sale_price = str(float(val) / 10)

    val = int(int(float(row_arr[7])) / 100)
    if val == 0:
        val = 1
    discount_price = str(float(val) / 10)
    row = row_arr[0] + ':' + row_arr[1] + ':' + row_arr[2] + ':' + row_arr[3] + ':' + row_arr[4] + ':' + original_price + ':' + sale_price + ':' + discount_price + ':' + row_arr[8] + ':' + row_arr[9] + ':' + row_arr[10] + ':' + row_arr[11] + ':' + row_arr[12]
    return row


def execute_redis(redis_result_dict):
    with r.pipeline(transaction=False) as p:
        count = 0
        for key, value in redis_result_dict.items():
            count += 1
            p.set(key, value, 60*60*24*3)
            if count % 5000 == 0:
                print("数据条数：" + str(count))
                p.execute()
                print("放入" + str((count//5000)+1) + "批次结果")
        print("数据条数：" + str(count))
        p.execute()
        print("放入" + str((count//5000)+1) + "批次结果")
        print("数据条数：" + str(count))


if __name__ == '__main__':

    click_file_path = '/data/aplum/ctr_data/product_click/'
    # 历史数据目录
    history_agg_file_path = '/data/aplum/ctr_data/product_view_click_full/'
    today = date.today()
    lastdate = today + timedelta(days=-7)
    print(lastdate)
    yestoday = today + timedelta(days=-1)
    print(yestoday)
    db_stat = MySQLdb.connect(statmysqlhost, statmysqlusername, statmysqlpasswd, statdb, charset='utf8')
    db_product = MySQLdb.connect(mysqlhost, mysqlusername, mysqlpasswd, mysqldb, charset='utf8')
    ctr_data = get_ctr_data_from_sql(db_stat, lastdate, yestoday)
    print('ctr_data: ' + str(len(ctr_data)))
    product_dict = get_data_from_sql(db_product)
    print('product_dict: ' + str(len(product_dict)))
    # num = 0
    # data_list = list()
    # while num < 1:
    #     index_date = lastdate + timedelta(days=num)
    #     print(index_date)
    #     data = GetESData(index_date)
    #     print(str(len(data)))
    #     num += 1
    #     data_list += data
    # user_dict = getUserDict(db_product)
    # for x in user_dict.items():
    #     print(x)
    # click_list = list()
    ctr_dict = dict()
    ctr_dict = get_pid_dict(ctr_data, ctr_dict)
    print('pid_dict: ' + str(len(ctr_dict)))

    ctr_dict = get_cid_dict(ctr_data, ctr_dict)
    print('cid_dict: ' + str(len(ctr_dict)))

    ctr_dict = get_bid_dict(ctr_data, ctr_dict)
    print('bid_dict: ' + str(len(ctr_dict)))
    for i in range(7):
        click_list = read_click_date(history_agg_file_path, lastdate, i)
        print('click_list: ' + str(len(click_list)))

        for row in click_list:
            row_arr = row.split(':')
            is_click = int(row_arr[0])
            is_blackcard_member = str(row_arr[1])
            if is_blackcard_member == '':
                continue
            else:
                is_blackcard_member = int(is_blackcard_member)
            position = str(row_arr[2])
            # if position == '':
            #     continue
            status = str(row_arr[3])
            pid = str(row_arr[4])
            original_price = str(row_arr[5])
            sale_price = str(row_arr[6])
            discount_price = str(row_arr[7])
            if pid in product_dict.keys():
                discount_rate = str(product_dict[pid]['discount_rate'])
                if discount_rate == '':
                    continue
            else:
                continue
            bid = str(row_arr[9])
            source_of_supply = str(row_arr[10])
            # if source_of_supply == '':
            #     continue
            degree = str(row_arr[11])
            # if degree == '':
            #     continue
            is_promotion = str(row_arr[12])



            ctr_dict = get_source_of_supply_dict(source_of_supply, is_click, ctr_dict)
            # print('source_of_supply_dict: ' + str(len(ctr_dict)))

            ctr_dict = get_is_blackcard_member_dict(is_blackcard_member, is_click, ctr_dict)
            # print('is_blackcard_member_dict: ' + str(len(ctr_dict)))

            ctr_dict = get_degree_dict(degree, is_click, ctr_dict)
            # print('degree_dict: ' + str(len(ctr_dict)))

            ctr_dict = get_position_dict(position, is_click, ctr_dict)
            # print('get_position_dict:' + str(len(ctr_dict)))

            ctr_dict = get_is_promotion_dict(is_promotion, is_click, ctr_dict)
            # print('is_promotion_dict: ' + str(len(ctr_dict)))

            ctr_dict = get_original_price_dict(original_price, is_click, ctr_dict)
            # print('original_price_dict: ' + str(len(ctr_dict)))

            ctr_dict = get_sale_price_dict(sale_price, is_click, ctr_dict)
            # print('sale_price_dict: ' + str(len(ctr_dict)))

            ctr_dict = get_discount_price_dict(discount_price, is_click, ctr_dict)
            # print('discount_price_dict: ' + str(len(ctr_dict)))

            ctr_dict = get_discount_rate_dict(discount_rate, is_click, ctr_dict)
            # print('discount_rate_dict: ' + str(len(ctr_dict)))

            # ctr_dict = get_status_dict(status, is_click, ctr_dict)
            # print('status_dict: ' + str(len(ctr_dict)))

            ctr_dict = get_bid_original_price_dict(bid, original_price, is_click, ctr_dict)
            # print('bid_original_price_dict: ' + str(len(ctr_dict)))

            ctr_dict = get_bid_discount_price_dict(bid, discount_price, is_click, ctr_dict)
            # print('bid_discount_price_dict: ' + str(len(ctr_dict)))

            ctr_dict = get_bid_discount_rate_dict(bid, discount_rate, is_click, ctr_dict)
            # print('bid_discount_rate_dict: ' + str(len(ctr_dict)))

            ctr_dict = get_bid_sale_price_dict(bid, sale_price, is_click, ctr_dict)
            # print('ctr_dict: ' + str(len(ctr_dict)))
        # dict_sum = ChainMap(pid_dict, cid_dict, bid_dict, source_of_supply_dict, is_blackcard_member_dict, degree_dict,
        #                     position_dict, is_promotion_dict, original_price_dict, sale_price_dict, discount_price_dict,
        #                     discount_rate_dict,status_dict, bid_original_price_dict, bid_discount_price_dict,
        #                     bid_discount_rate_dict, bid_sale_price_dict)
        print('ctr_dict: ' + str(len(ctr_dict)))
    # tmp_dict = dict()

    for k in list(ctr_dict.keys()):
        if k.endswith('click'):
            v = ctr_dict[k]
            pv_k = k[: -5] + 'pv'
            if pv_k not in ctr_dict.keys():
                ctr_dict[pv_k] = 0
                rate_key = k[: -5] + 'rate'
                ctr_dict[rate_key] = 0.00
                continue
            pv_value = ctr_dict[pv_k]
            # 如果点击数大于曝光数,点击率设置为0.99
            # 此情况是由于同一界面在未刷新的前提下,多次点击同一商品造成的
            if pv_value == 0:
                ctr_dict[k] = 0
                rate_key = k[: -5] + 'rate'
                ctr_dict[rate_key] = 0.00
            # 如果曝光数为0,点击数和点击率都设置为0
            # 此情况是有可能为卖家点击造成的,卖家不记录曝光量
            elif v > pv_value:
                rate_key = k[: -5] + 'rate'
                ctr_dict[rate_key] = 0.99
            else:
                rate = round((v / pv_value), 2)
                rate_key = k[: -5] + 'rate'
                ctr_dict[rate_key] = rate
        else:
            continue
    save_dict_to_csv(ctr_dict)
    # save_dict_to_csv(tmp_dict)
    execute_redis(ctr_dict)

