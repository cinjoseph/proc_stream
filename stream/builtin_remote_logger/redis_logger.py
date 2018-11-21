# -*- coding:utf-8 -*-
### Warning CustomRemoteLogger 禁止直接使用log 的相关函数，否则可能会造成死循环
### 若要使用, 请先使用 current_local_logger 获取本地logger后输出
import re
import redis

from stream.logger import CustomRemoteLogger

def parse_redis_uri(uri):
    pattern = r"redis://((?P<passwd>\S+)@)?"
    pattern += r"(?P<host>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(?P<port>\d{1,5})"
    pattern += r"/(?P<db>[a-zA-Z0-9_]+)(\?expire=(?P<expire>[0-9]+))?"
    reg = re.compile(pattern)
    match = reg.match(uri)
    if match == None:
        return None
    return match.groupdict()


class RedisLogger(CustomRemoteLogger):

    def _init(self, conf):
        self.lname = conf['list_name']
        rds_conf = parse_redis_uri(conf['url'])
        self.rds = redis.Redis(host=rds_conf['host'],
                               port=rds_conf['port'],
                               db=rds_conf['db'],
                               password=rds_conf['passwd'])

    def log(self, ts, name, level, body):
        body = {'ts': ts, 'logger_name': name, 'level': level, 'body': body}
        self.rds.lpush(self.lname, body)

