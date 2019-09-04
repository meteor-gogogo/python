#!/usr/bin/env python
# coding=utf-8
import requests
import MySQLdb
import time
from datetime import date, datetime, timedelta

mysql_host = 'rm-2zeixwnpc34127h5f191-vpc-rw.mysql.rds.aliyuncs.com'
mysql_user = 'plumdb'
mysql_passwd = 'plumdb@2018'
mysql_db = 'aplum_mis'

app_id = 1642281099379724
secret = 'd128482ac30293c565d5d21413d3140fec25162c'


# 从数据库拿到token, refresh_token
def get_refresh_token(aid, market_cursor):
    token = ''
    refresh_token = ''
    sql = "select aid, token, refresh_token, update_time from t_toutiao_ad_token where aid = {0}".format(aid)
    market_cursor.execute(sql)
    token_data = market_cursor.fetchall()
    for row in token_data:
        if row['refresh_token'] is None:
            print('refresh_token is null, exit')
            exit(0)
        else:
            refresh_token = row['refresh_token']
            token = row['token']
            aid = row['aid']
    return aid, token, refresh_token


# 将最新的token更新到数据库
def update_token(db_market, market_cursor, token_data, aid):
    # print(token_data['code'])
    update_sql = "update t_toutiao_ad_token set token = '{0}',refresh_token = '{1}', update_time = {2} where aid = {3}" \
        .format(token_data['data']['access_token'], token_data['data']['refresh_token'], int(time.time()), aid)
    market_cursor.execute(update_sql)
    db_market.commit()
    print('token 更新成功')


# 拿到最新的token, refresh_token
def refresh_access_token(app_id, secret, refresh_token):
    open_api_url_prefix = "https://ad.toutiao.com/open_api/"
    uri = "oauth2/refresh_token/"
    refresh_token_url = open_api_url_prefix + uri
    data = {"appid": app_id,
            "secret": secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token}
    rsp = requests.post(refresh_token_url, json=data)
    rsp_data = rsp.json()
    return rsp_data


# 获得广告主的广告投放效果数据
def get_campaign_stat(start_date, aid, access_token):
    open_api_url_prefix = "https://ad.toutiao.com/open_api/"
    uri = "2/report/ad/get/"
    url = open_api_url_prefix + uri
    params = {"advertiser_id": aid,
              "start_date": start_date,
              "end_date": start_date,
              }
    headers = {"Access-Token": access_token}
    rsp = requests.get(url, json=params, headers=headers)
    rsp_data = rsp.json()
    return rsp_data


# 获得代理商数据
def get_agent_stat(start_date, aid, access_token):
    open_api_url_prefix = "https://ad.toutiao.com/open_api/"
    uri = "2/report/agent/get/"
    url = open_api_url_prefix + uri
    params = {"start_date": start_date, "end_date": start_date,
              "advertiser_id": aid, }
    headers = {"Access-Token": access_token}
    rsp = requests.get(url, json=params, headers=headers)
    rsp_data = rsp.json()
    return rsp_data


# 获得广告主日资金流水
def get_advertiser_daily_stat(start_date, aid, access_token):
    open_api_url_prefix = "https://ad.toutiao.com/open_api/"
    uri = "2/advertiser/fund/daily_stat/"
    url = open_api_url_prefix + uri
    params = {"advertiser_id": aid,
              "start_date": start_date,
              "end_date": start_date
              }
    headers = {"Access-Token": access_token}
    rsp = requests.get(url, json=params, headers=headers)
    rsp_data = rsp.json()
    return rsp_data


# 广告相关数据存入数据库
def data_to_mysql(fandian_dict, market_cursor, aid, start_date, data):
    data_list = data['data']['list']
    show = 0
    click = 0
    cost = 0.0
    for i in data_list:
        # 曝光
        show = int(i['show'])
        # 点击
        click = int(i['click'])
        # 总花费(单位:元)
        cost = round((float(i['cost'])) / (1 + float(fandian_dict[int(aid)])), 2)
    if cost > 0.0:
        sql = "insert into t_ad_data (second_name, aid, show_num, click_num, cost, ad_date) values('头条', '{0}', " \
              "{1}, {2}, {3}, '{4}')".format(aid, show, click, cost, start_date)
        market_cursor.execute(sql)
        print('插入数据成功')
    else:
        print('cost : ' + str(cost))
        pass


def main(fandian_dict, market_cursor, start_date, ad_id):
    # 从数据库拿到aid, token, refresh_token
    aid, token, refresh_token = get_refresh_token(ad_id, market_cursor)
    # 从头条api拿到最新的token, refresh_token
    token_data = refresh_access_token(app_id, secret, refresh_token)
    # 将拿到的新的token, refresh_token更新到数据库中
    update_token(db_market, market_cursor, token_data, aid)
    token = token_data['data']['access_token']
    # aid, token, refresh_token = get_refresh_token(aid, market_cursor)
    # 获得昨日的广告相关数据
    data = get_campaign_stat(start_date, aid, token)
    # data = get_agent_stat(start_date, aid, token)
    # data = get_advertiser_daily_stat(start_date, aid, token)
    # 获取数据超时,尝试重新获取
    if data['code'] != 0:
        flag = True
        while flag:
            # data = get_advertiser_daily_stat(start_date, aid, token)
            data = get_campaign_stat(start_date, aid, token)
            if data['code'] != 0:
                flag = True
            else:
                flag = False
            # 休眠1秒,再次尝试获取数据
            time.sleep(1)
        # data = get_campaign_stat(start_date, aid, token)
        # 将拿到的广告相关数据存入数据库
        data_to_mysql(fandian_dict, market_cursor, aid, start_date, data)
    else:
        data_to_mysql(fandian_dict, market_cursor, aid, start_date, data)


def insert_cost_to_day_cost(db_market, market_cursor, start_date):
    # 将头条的总金额拆分成两份分别插入到数据库中,这样做的目的是为了日报表的统计需要拿到这两个source
    source1 = 'toutiao'
    source2 = 'douyin'
    costs = 0.0
    # 拿到所有账号在昨天的总的投放金额
    sql_cost = "select sum(cost) as sum_cost from t_ad_data where second_name = '头条' and ad_date = '{0}'".format(start_date)
    market_cursor.execute(sql_cost)
    row_data = market_cursor.fetchall()
    for row in row_data:
        if row['sum_cost'] is None:
            costs = 0.0
        else:
            costs = float(row['sum_cost'])
    print(costs)
    # 拆分金额,只需要有这两个source即可,具体多少金额都无所谓
    costs1 = costs - 1.0
    costs2 = 1.0
    # toutiao对应的sid为5, douyin对应的sid为4
    sql_insert_toutiao = "insert into t_market_day_cost (sid, costs, costs_date, source) values ({0}, {1}, '{2}', '{3}')"\
        .format(5, costs1, start_date, source1)
    market_cursor.execute(sql_insert_toutiao)
    sql_insert_douyin = "insert into t_market_day_cost (sid, costs, costs_date, source) values ({0}, {1}, '{2}', '{3}')" \
        .format(4, costs2, start_date, source2)
    market_cursor.execute(sql_insert_douyin)
    # 提交
    db_market.commit()
    print('日花费插入成功')


if __name__ == '__main__':
    fandian_dict = {
        110425205008: 0.02,
        110425160745: 0.02,
        110425077742: 0.02,
        1633753219366923: 0.02,
        103891969936: 0.035,
        104241096798: 0.035,
        104749561785: 0.035,
        104750237072: 0.035,
        104757843442: 0.035,
        104757923769: 0.035,
        108260043679: 0.035,
        108322887258: 0.035,
        108322962478: 0.035,
        108323023542: 0.035
    }
    # 获得aplum_mis数据库的连接
    db_market = MySQLdb.connect(mysql_host, mysql_user, mysql_passwd, mysql_db, charset='utf8')
    market_cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    # 定义推广账号的列表
    ad_id_list = [103891969936, 104241096798, 104749561785, 104750237072, 104757843442, 104757923769, 108260043679,
                   108322887258, 108322962478, 108323023542, 110425205008, 110425160745, 110425077742, 1633753219366923]
    # 查询日期为昨天
    start_date_tmp = '2019-08-01'
    for i in range(31):
        start_date = (datetime.strptime(start_date_tmp, '%Y-%m-%d') + timedelta(days=i)).strftime('%Y-%m-%d')
        # start_date = str(date.today() + timedelta(days=-1))
        for ad_id in ad_id_list:
            print(ad_id)
            # 主要处理函数
            main(fandian_dict, market_cursor, start_date, ad_id)
            # 有写数据库的操作,提交一下
            db_market.commit()
        # 汇总各推广账号的数据插入到日成本表一条数据
        insert_cost_to_day_cost(db_market, market_cursor, start_date)
