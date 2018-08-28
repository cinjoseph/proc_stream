# -*- coding: utf-8 -*-

# standard library modules

import json
from stream.process_node import HandlerProcessNode
from stream.log import get_logger

logger = get_logger()


class PrintNode(HandlerProcessNode):

    def _init(self, node_conf):
        self.conf = node_conf

    def proc(self, data):
        logger.info("%s recv data: %s" % (self.name , json.dumps(data)))
        self.emit(str(data) + '.')
