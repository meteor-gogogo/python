import redis
import requests
import time
import MySQLdb
from datetime import date, datetime, timedelta


mysql_host = 'rm-2zeixwnpc34127h5f191-vpc-rw.mysql.rds.aliyuncs.com'
mysql_user = 'plumdb'
mysql_passwd = 'plumdb@2018'
mysql_db = 'aplum_mis'

# redis库
r = redis.Redis(host='r-2ze4eb46b642e1b4707.redis.rds.aliyuncs.com', port='6379', password='Plum17redis', db=3,
                decode_responses=True, charset='utf-8')


def get_token_from_redis(ad_id):
    key = 'TENCENT_SOCIAL_ADS_TOKEN_' + str(ad_id)
    return r.get(key)


def get_data(token, ad_id, start_date, end_date):
    url = """https://api.e.qq.com/v1.1/daily_reports/get?access_token={0}&timestamp={1}&nonce={2}&account_id={3}&level=REPORT_LEVEL_ADVERTISER&date_range={{"start_date":"{4}","end_date":"{5}"}}&fields=["date","impression","click","cost"]""".format(token, int(time.time()), int(time.time()), ad_id, start_date, end_date)
    # print(url)
    rsp = requests.get(url)
    # print(rsp)
    # print(rsp.headers)
    rsp_data = rsp.json()
    # rsp_data = response.json()
    return rsp_data


def data_to_mysql(fandian_dict, market_cursor, aid, start_date, data):
    data_list = data['data']['list']
    show = 0
    click = 0
    cost = 0.0
    for i in data_list:
        show = int(i['impression'])
        click = int(i['click'])
        cost = round((float(i['cost']) / 100) / (1 + float(fandian_dict[int(aid)])), 2)
    if cost > 0.0:
        sql = "insert into t_ad_data (second_name, aid, show_num, click_num, cost, ad_date) values('广点通', '{0}', " \
          "{1}, {2}, {3}, '{4}')".format(aid, show, click, cost, start_date)
        market_cursor.execute(sql)
        print('插入数据成功')
    else:
        print('cost : ' + str(cost))
        pass


def main(fandian_dict, market_cursor, ad_id, start_date, end_date):
    # ad_id = 10896792
    token = str(get_token_from_redis(ad_id))
    # token = "ffd0d03b3f93ca8e61934872d143998b"
    data = get_data(token, ad_id, start_date, end_date)
    # print(data)
    # while data['code'] != 0:
    #     data = get_data(token, ad_id, start_date, end_date)
    if data['code'] != 0:
        flag = True
        while flag:
            data = get_data(token, ad_id, start_date, end_date)
            if data['code'] != 0:
                flag = True
            else:
                flag = False
            time.sleep(1)
        data_to_mysql(fandian_dict, market_cursor, ad_id, start_date, data)
        # print(data)
    else:
        data_to_mysql(fandian_dict, market_cursor, ad_id, start_date, data)
        # print(data)
        # data_to_mysql(ad_id, data)


if __name__ == '__main__':
    fandian_dict = {
        10799290: 0.08,
        10799316: 0.08,
        8865454: 0.08,
        10753003: 0.08,
        10896768: 0.07,
        10896792: 0.07,
        11318002: 0.08,
        10829922: 0.08
    }
    db_market = MySQLdb.connect(mysql_host, mysql_user, mysql_passwd, mysql_db, charset='utf8')
    market_cursor = db_market.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    id_list = [10799316, 10799290, 10753003, 8865454, 10896768, 11318002, 10829922]
    # id_list = [10896792]
    start_date_tmp = '2019-02-28'
    for i in range(1000):
        start_date = (datetime.strptime(start_date_tmp, '%Y-%m-%d') + timedelta(days=i)).strftime('%Y-%m-%d')
        if str(start_date) == str(date.today()):
            print(start_date)
            exit(0)
        print(start_date)
        end_date = start_date
        for ad_id in id_list:
            time.sleep(1)
            main(fandian_dict, market_cursor, ad_id, start_date, end_date)
            db_market.commit()
