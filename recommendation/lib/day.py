from datetime import datetime,timedelta
import time

def startDate(Days):
    return (datetime.now() - timedelta(days=Days)).strftime('%Y-%m-%d')

def endDate():
    return datetime.now().strftime('%Y-%m-%d')

def startHourMinite(Days):
    return (datetime.now() - timedelta(days=Days)).strftime('%Y-%m-%d %H:%M:%S')

def endHourMinite():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def unixtime(date):
    return int(time.mktime((datetime.strptime(date, "%Y-%m-%d %H:%M:%S")).timetuple()))

def unix2date(t):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t)) 

def startUnixtime(Days):
    date = startHourMinite(Days)
    return unixtime(date)

def endUnixtime():
    date = endHourMinite()
    return unixtime(date)

def str2Timestamp(st):
    d = datetime.strptime(st, "%Y-%m-%d %H:%M:%S.%f")
    return int(time.mktime(d.timetuple()))
