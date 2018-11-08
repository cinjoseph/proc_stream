# -*- coding:utf-8 -*-
import pprint
from stream.stream import StreamHeartBeat
from stream.log import get_logger

logger = get_logger()


class LoggerHeartBeat(StreamHeartBeat):

    def heart_beat(self, controller):
        infos = controller.runtime_info
        s = pprint.pformat(infos)
        logger.info("Server Runtime Info:\n%s" % s)




