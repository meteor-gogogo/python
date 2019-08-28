#!/usr/bin/env python
# coding=utf-8
import requests
import MySQLdb
import time
from datetime import date, datetime, timedelta
import configparser
import os


mysql_host = 'rm-2zeixwnpc34127h5f191-vpc-rw.mysql.rds.aliyuncs.com'
mysql_user = 'plumdb'
mysql_passwd = 'plumdb@2018'
mysql_db = 'aplum_mis'

app_id = 1642281099379724
secret = 'd128482ac30293c565d5d21413d3140fec25162c'


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


def update_token(db_market, market_cursor, token_data, aid):
    # print(token_data['code'])
    update_sql = "update t_toutiao_ad_token set token = '{0}',refresh_token = '{1}', update_time = {2} where aid = {3}"\
        .format(token_data['data']['access_token'], token_data['data']['refresh_token'], int(time.time()), aid)
    market_cursor.execute(update_sql)
    db_market.commit()
    print('token 更新成功')


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


def get_campaign_stat(start_date, aid, access_token):
    open_api_url_prefix = "https://ad.toutiao.com/open_api/"
    uri = "2/report/ad/get/"
    url = open_api_url_prefix + uri
    params = {"advertiser_id": aid,
              "start_date": start_date,
              "end_date": start_date,
              # "time_granularity": "STAT_TIME_GRANULARITY_DAILY",
              # "group_by": ["STAT_GROUP_BY_FIELD_ID"],
              # "filtering": {"campaign_id": 1}
              }
    headers = {"Access-Token": access_token}
    rsp = requests.get(url, json=params, headers=headers)
    rsp_data = rsp.json()
    return rsp_data


def data_to_mysql(market_cursor, aid, start_date, data):
    data_list = data['data']['list']
    show = 0
    click = 0
    cost = 0.0
    for i in data_list:
        show = int(i['show'])
        click = int(i['click'])
        cost = float(i['cost'])
    sql = "insert into t_ad_data (second_name, aid, show_num, click_num, cost, ad_date) values('头条', '{0}', " \
          "{1}, {2}, {3}, '{4}')".format(aid, show, click, cost, start_date)
    # print(sql)
    market_cursor.execute(sql)


def main(market_cursor, start_date, ad_id):
    # db_market = MySQLdb.connect(mysql_host, mysql_user, mysql_passwd, mysql_db, charset='utf8')
    # market_cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    aid, token, refresh_token = get_refresh_token(ad_id, market_cursor)
    token_data = refresh_access_token(app_id, secret, refresh_token)
    # print(token_data)
    update_token(db_market, market_cursor, token_data, aid)
    token = token_data['data']['access_token']
    # aid, token, refresh_token = get_refresh_token(aid, market_cursor)
    data = get_campaign_stat(start_date, aid, token)
    if data['code'] != 0:
        flag = True
        while flag:
            data = get_campaign_stat(start_date, aid, token)
            if data['code'] != 0:
                flag = True
            else:
                flag = False
        # data = get_campaign_stat(start_date, aid, token)
        data_to_mysql(market_cursor, aid, start_date, data)
    else:
        data_to_mysql(market_cursor, aid, start_date, data)


if __name__ == '__main__':
    db_market = MySQLdb.connect(mysql_host, mysql_user, mysql_passwd, mysql_db, charset='utf8')
    market_cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    ad_id_list = [103891969936, 104241096798, 104749561785, 104750237072, 104757843442, 104757923769, 108260043679,
                  108322887258, 108322962478, 108323023542]
    start_date = str(date.today() + timedelta(days=-1))
    for ad_id in ad_id_list:
        main(market_cursor, start_date, ad_id)
