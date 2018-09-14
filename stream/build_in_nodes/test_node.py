# -*- coding: utf-8 -*-

# standard library modules

import json
from stream.process_node import HandlerProcessNode, OutputProcessNode
from stream.log import get_logger

logger = get_logger()


class PrintNode(OutputProcessNode):

    def init(self, node_conf):
        self.conf = node_conf

    def fini(self):
        pass

    def proc(self, data):
        logger.debug("%s recv data: %s" % (self.name , data) )
        logger.info("%s : %s" % (self.name , data) )


class AddTailNode(HandlerProcessNode):

    def init(self, node_conf):
        self.conf = node_conf

    def fini(self):
        pass

    def proc(self, data):
        logger.debug("%s recv data: %s" % (self.name , data) )
        self.emit(str(data) + ' +')
