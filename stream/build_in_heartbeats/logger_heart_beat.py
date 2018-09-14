# -*- coding:utf-8 -*-
import pprint
from stream.stream import StreamHeartBeat
from stream.log import get_logger


class LoggerHeartBeat(StreamHeartBeat):

    def _init(self, args):
        self.logger = get_logger()

    def heart_beat(self, infos):
        s = pprint.pformat(infos)
        for s in s.replace('\r', "").split('\n'):
            self.logger.info("Server Runtime Info:\n%s" % s)




