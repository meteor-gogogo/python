#!/usr/bin/env python
# coding=utf-8
import requests
import MySQLdb
import time
import configparser
import os


def get_refresh_token(market_cursor):
    aid = ''
    token = ''
    refresh_token = ''
    sql = "select aid, token, refresh_token, update_time from t_toutiao_ad_token"
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
    print(token_data['code'])
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


def get_campaign_stat(access_token):
    open_api_url_prefix = "https://ad.toutiao.com/open_api/"
    uri = "2/report/ad/get/"
    url = open_api_url_prefix + uri
    params = {"advertiser_id": 0,
              "start_date": "2017-01-20",
              "end_date": "2017-02-18",
              "time_granularity": "STAT_TIME_GRANULARITY_DAILY",
              "group_by": ["STAT_GROUP_BY_FIELD_ID"],
              "filtering": {"campaign_id": 1}
              }
    headers = {"Access-Token": access_token}
    rsp = requests.get(url, json=params, headers=headers)
    rsp_data = rsp.json()
    return rsp_data


def main():
    config = configparser.ConfigParser()
    dir_path = os.path.abspath(os.path.dirname(__file__))
    config_path = os.path.abspath(dir_path + '/config/config.ini')
    config.read(config_path)
    app_id = config.get('douyin', 'app_id')
    secret = config.get('douyin', 'secret')
    mysql_host = config.get('aplum_mis_db', 'mysql_host')
    mysql_user = config.get('aplum_mis_db', 'mysql_user')
    mysql_passwd = config.get('aplum_mis_db', 'mysql_passwd')
    mysql_db = config.get('aplum_mis_db', 'mysql_db')
    db_market = MySQLdb.connect(mysql_host, mysql_user, mysql_passwd, mysql_db, charset='utf8')
    market_cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    aid, token, refresh_token = get_refresh_token(market_cursor)
    token_data = refresh_access_token(app_id, secret, refresh_token)
    print(token_data)
    update_token(db_market, market_cursor, token_data, aid)
    aid, token, refresh_token = get_refresh_token(market_cursor)
    access_token = token
    data = get_campaign_stat(access_token)
    print(data)


if __name__ == '__main__':
    main()
