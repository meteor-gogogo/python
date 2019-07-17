#!/usr/bin/env python
# coding=utf-8

from datetime import timedelta, date
import configparser
import os
from loaddata import loaddata


class UserProRun(object):
    def __init__(self, before_days):
        self.before_days = before_days

    def run(self):
        today = date.today()
        # yesterday = today + timedelta(days=-1)
        before_day = today + timedelta(days=-int(self.before_days))
        config = configparser.ConfigParser()
        usr_pro_path = os.path.abspath(os.path.dirname(__file__))
        config_path = os.path.abspath(usr_pro_path + '/config/config.ini')
        config.read(config_path)
        loaddata.load_data(config, before_day, int(self.before_days))
