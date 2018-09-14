# -*- coding:utf-8 -*-
import re
import redis
import time
import threading
from stream.trigger_node import Trigger


def parse_redis_uri(uri):
    pattern = r"redis://((?P<passwd>\S+)@)?"
    pattern += r"(?P<host>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(?P<port>\d{1,5})"
    pattern += r"/(?P<db>[a-zA-Z0-9_]+)(\?expire=(?P<expire>[0-9]+))?"
    reg = re.compile(pattern)
    match = reg.match(uri)
    if match == None:
        return None
    return match.groupdict()


class RedisTrigger(Trigger):

    def _init(self, conf):
        self.lname = conf['list_name']
        self._poll_timeout = conf.get('poll_timeout', 0.5)
        self._need_stop = threading.Event()
        rds_conf = parse_redis_uri(conf['url'])
        self.rds = redis.Redis(host=rds_conf['host'],
                               port=rds_conf['port'],
                               db=rds_conf['db'],
                               password=rds_conf['passwd'])

    def _fini(self):
        self.rds.close()

    def start(self, recv_callback):
        while not self._need_stop.is_set():
            while True:
                data = self.rds.lpop(self.lname)
                if data == None:
                    break
                else:
                    recv_callback(data, None, None)
            time.sleep(self._poll_timeout)

    def _stop(self):
        self._need_stop.set()
