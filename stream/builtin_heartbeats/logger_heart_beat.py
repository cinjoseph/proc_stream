# -*- coding:utf-8 -*-
import pprint
import stream.logger as logger
from stream.stream import StreamHeartBeat

count = 0

class LoggerHeartBeat(StreamHeartBeat):

    def heart_beat(self, controller):
        infos = controller.runtime_info
        s = pprint.pformat(infos)
        logger.info("Server Runtime Info:\n%s" % s)

        controller.restart_stream('TestStream')




