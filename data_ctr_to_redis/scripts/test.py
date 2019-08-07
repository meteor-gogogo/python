import geoip2.database
import redis
import time


if __name__ == "__main__":
    tic = lambda: 'at %1.1f seconds' % (time.time() - start)
    r = redis.Redis(host='dev02.aplum-inc.com', port='6379', password='', db=2, decode_responses=True, charset='utf-8')
    start = time.time()
    print(r.get("gbdt:1:2842391:pv"))
    print(tic())