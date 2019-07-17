from datetime import datetime, date, timedelta
import time


if __name__ == '__main__':
    date_time = datetime.strptime('2019-07-16 00:15:00', '%Y-%m-%d %H:%M:%S')
    print(date_time.timestamp())
    hour = (date_time - timedelta(hours=1)).hour
    print((date_time - timedelta(hours=1)).hour)
    begin_time = int(time.mktime((date.today() + timedelta(days=-1)).timetuple()) + 3600 * hour)
    print(begin_time)
    print(datetime.strptime('2019-07-16 00:15:00', '%Y-%m-%d %H:%M:%S'))
    hour = (datetime.now() - timedelta(hours=+1)).hour
    begin_time = int(time.mktime(date.today().timetuple()) + 3600 * (hour - 1))
    end_time = int(time.mktime(date.today().timetuple()) + 3600 * hour)
    print(hour)
    print(begin_time)
    print(end_time)
    print(date.today().timetuple())
    print(datetime.today())
    print(datetime.timestamp(datetime.today()))