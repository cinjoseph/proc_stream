# -*- coding:utf-8 -*-
import re
import redis
import time
import threading
from functools import partial

def parse_redis_uri(uri):
    pattern = r"redis://((?P<passwd>\S+)@)?"
    pattern += r"(?P<host>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(?P<port>\d{1,5})"
    pattern += r"/(?P<db>[a-zA-Z0-9_]+)(\?expire=(?P<expire>[0-9]+))?"

    reg = re.compile(pattern)
    match = reg.match(uri)
    if match == None:
        return None

    return match.groupdict()

class RedisReader():

    def __init__(self, conf):
        self.lname = conf['list_name']
        self._poll_timeout = conf.get('poll_timeout', 0.5)
        url = conf['url']
        cache_conf = parse_redis_uri(url)
        host = cache_conf['host']
        port = cache_conf['port']
        passwd = cache_conf['passwd']
        db = cache_conf['db']

        self.rds = redis.Redis(host=host, port=port, db=db, password=passwd)
        self._need_stop = threading.Event()

    def start(self, recv_callback):
        while not self._need_stop.is_set():
            while True:
                data = self.rds.lpop(self.lname)
                if data == None:
                    break
                else:
                    recv_callback(data, None, None)
            time.sleep(self._poll_timeout)

    def stop(self):
        self._need_stop.set()
