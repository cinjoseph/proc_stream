# -*- coding: utf-8 -*-

# standard library modules

from stream.proc_node import PROC_STOP, PROC_CONTINUE, PROC_UPDATE
from stream.log import get_logger

logger = get_logger()


class SyncOutput():

    _type = "_sync"
    _node_type = "output"

    def __init__(self, conf):
        self.msg = conf.get('msg', "no msg")

    def proc(self, data):
        data += " (sync_output msg: " + self.msg + ")"
        logger.info(data)
        return PROC_CONTINUE, None


class AsyncOutput():

    _type = "_sync"
    _node_type = "output"

    def __init__(self, conf):
        self.msg = conf.get('msg', "no msg")

    def proc(self, data):
        data += " (async_output msg: " + self.msg + ")"
        logger.info(data)
        return PROC_CONTINUE, None


class SyncHandler():

    _type = "_sync"
    _node_type = "output"

    def __init__(self, conf):
        self.msg = conf.get('msg', "no msg")

    def proc(self, data):
        data += " (sync_handler msg: " + self.msg + ")"
        logger.info(data)
        return PROC_CONTINUE, None

class AsyncHandler():

    _type = "_sync"
    _node_type = "output"

    def __init__(self, conf):
        self.msg = conf.get('msg', "no msg")

    def proc(self, data):
        data += " (async_handler msg: " + self.msg + ")"
        logger.info(data)
        return PROC_CONTINUE, None
