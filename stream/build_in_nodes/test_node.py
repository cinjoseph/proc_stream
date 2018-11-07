# -*- coding: utf-8 -*-

# standard library modules

import json
from stream.process_node import HandlerProcessNode, OutputProcessNode
from stream.log import get_logger

logger = get_logger()


class PrintNode(OutputProcessNode):

    def _init(self, node_conf):
        pass

    def _fini(self):
        pass

    def proc(self, data):
        if type(data) == str:
            data = data.decode('utf-8')
        logger.debug("%s recv data: %s" % (self.name, data))
        # print data
        # logger.info("%s : %s" % (self.name , data) )
        pass


class DelayNode(HandlerProcessNode):

    def _init(self, args):
        self.loop = args.get('loop', 5)

    def proc(self, data):
        count = 0
        # 200 / 1s 10000-5
        for i in xrange(10000):
            for j in xrange(self.loop):
                count += 1
        self.emit(data)


class AddTailNode(HandlerProcessNode):

    def _init(self, node_conf):
        pass

    def _fini(self):
        pass

    def proc(self, data):
        logger.debug("%s recv data: %s" % (self.name, data))
        self.emit(str(data) + ' +')
