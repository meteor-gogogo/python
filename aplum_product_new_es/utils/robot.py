#!/usr/bin/env python
# coding=utf-8

import requests
import json

def alarmDD(msg):
    url = 'https://oapi.dingtalk.com/robot/send?access_token=b5176622f55d3ed61e5d78527bb58478a8fb4e0519bb14f1b0652d2d4cb07916'
    headers = {'Content-Type': 'application/json'}
    payload = {
        "msgtype": "text", 
        "text": {"content": msg}
    }
    r = requests.post(url, data = json.dumps(payload), headers = headers)
    print(r.text)


if __name__ == '__main__':
    alarmDD('这是来自data01的测试')

