# -*- coding:utf-8 -*-
import pprint
from stream.stream import StreamHeartBeat
from stream.log import get_logger

logger = get_logger()


class LoggerHeartBeat(StreamHeartBeat):

    def heartbeat(self, infos):
        s = pprint.pformat(infos)
        logger.info("Server Runtime Info:\n%s" % s)




