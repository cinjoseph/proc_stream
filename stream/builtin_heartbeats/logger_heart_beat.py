# -*- coding:utf-8 -*-
import json
import pprint
import stream.logger as logger
from stream.stream import StreamHeartBeat

count = 0

class LoggerHeartBeat(StreamHeartBeat):

    def heart_beat(self, controller):
        infos = controller.runtime_info
        s = json.dumps(infos)
        logger.info("Server Runtime Info:\n%s" % s)





