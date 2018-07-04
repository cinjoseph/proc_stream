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

from trigger_node import ReaderNode
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
        info = {'name': self.name}
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


class StreamController(object):

    def __init__(self, conf, hb_cb=None):
        """

        :param conf: 流事件的配置
        :param hb_cb: 心跳处理函数
        """
        self.loop_stop  = threading.Event()
        self.conf       = conf
        self.hb_cb      = hb_cb

    def start(self):
        node_template = self.conf['NodeTemplate']
        reader_template = self.conf['ReaderTemplate']
        streams_conf = self.conf['Streams']

        streams = []

        logger.info("--------------------------------")
        logger.info("All Streams Start !!!")

        for k, v in streams_conf.items():
            logger.info("Init stream %s" % k)
            stream_conf = v['stream']
            reader_name = v['reader']
            reader_conf = reader_template[reader_name]
            s = Stream(k, reader_name, reader_conf,
                    stream_conf, node_template)
            streams.append(s)

        # start streams
        for s in streams:
            logger.info("Start stream %s !!!" % k)
            s.start()

        count = 0
        while not self.loop_stop.is_set():
            if count == 5 and self.hb_cb:
                count =0
                self.hb_cb([ s.get_info() for s in streams ])
                logger.info("Do Heart Beat, need exit now? %s" % (self.loop_stop.is_set()))
            count += 1
            time.sleep(1)

        # stop streams
        for s in streams:
            s.stop()
            logger.info("stop stream %s !!!" % k)

        logger.info("All Streams Stop !!!")

    def stop(self):
        self.loop_stop.set()



