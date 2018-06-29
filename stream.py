# -*- coding: utf-8 -*-

# standard library modules
import re
import sys
import time
import json
import datetime
import traceback
import threading
from pprint import pprint

from read_node import ReaderNode
from proc_node import ProcNode
from stream_node import ProcStream
from utils import print_traceback

import log
logger = log.get_logger()

class Stream:

    def __init__(self, name, reader_name, reader_conf, stream_conf,
           node_template):

        self.name = name

        self._stream = ProcStream(node_template, stream_conf)
        self._reader = ReaderNode(reader_name, reader_conf)

        # binding reader to stream
        self._reader.register_output(self.__reader_input_cb)

    def __reader_input_cb(self, raw, private_data):

        def exception_cb(exc_info, private_data):
            logger.error("Get Some Exception:")
            #traceback.print_exception(*exc_info)
            print_traceback(logger)
            reader_event = private_data
            reader_event.msg_finish_ack()
            logger.error("Raw is:\n%s" % reader_event.raw)

        def output_cb(result, private_data):
            reader_event = private_data
            reader_event.msg_finish_ack()

        self._stream.input_request(raw, 
                handle_result=output_cb,
                handle_exception=exception_cb, 
                private_data=private_data)

    def get_info(self):
        info = {}
        summary, detail = self._reader.runtime_info()
        info['reader'] = {
                "summary": summary, 
                "detail": detail}
        summary, detail = self._stream.runtime_info()
        info['stream'] = {
                "summary": summary, 
                "detail": detail}
        return info

    def start(self):
        logger.info("Stream Main Start ~")
        self._stream.start()
        self._reader.start()

    def stop(self):
        self._reader.stop()
        self._stream.stop()
        logger.info("Stream Main Stop ~")

