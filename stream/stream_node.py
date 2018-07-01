# -*- coding: utf-8 -*-

__author__ = "chenjianing"

# standard library modules
import sys
import json
import time
import Queue
import threading
import traceback
import logging

from proc_node import ProcNode, PoolNotReady
from proc_node import PROC_STOP, PROC_CONTINUE, PROC_UPDATE
from utils import get_module_class

import log
logger = log.get_logger()

class NodeFactory:

    def __init__(self, conf):
        self.conf = conf

    def new(self, node_name):
        conf = self.conf.get(node_name)
        module_path = conf.get('module', None)
        if not module_path:
            raise Exception("%s conf doesn't has module conf" % node_name)
        node_cls = get_module_class(module_path)
        node_args = conf.get("args", {})
        pool_size = conf.get('pool_size', 1) 
        poll_timeout = conf.get('poll_timeout', 1)

        current_t_name = threading.current_thread().getName()
        node_name = current_t_name + "." + node_name
        node = ProcNode(node_name, node_cls, node_args,
                pool_size=pool_size, poll_timeout=poll_timeout)

        return node


class ProcStreamEvent:

    def __init__(self, msg, private_data):
        self.id = id(self)
        self.msg = msg
        self.pos = -1
        self.private_data = private_data


class ProcStream:

    def __init__(self, node_conf, stream_conf):

        node_factory = NodeFactory(node_conf)
        self.stream = []
        for s in stream_conf:
            node = node_factory.new(s)
            self.stream.append(node)

        self._start_success = threading.Event()

        self._total_msg_count = 0
        self._total_msg_count_lock = threading.Lock()

        self._finished_msg_count = 0
        self._finished_msg_count_lock = threading.Lock()

    #############################################################
    # finished msg count operation
    ############################################################# 
    def _inc_finished_msg_count(self):
        if self._finished_msg_count_lock.acquire():
            self._finished_msg_count += 1
        self._finished_msg_count_lock.release()

    def get_finished_msg_count(self):
        count = None
        if self._finished_msg_count_lock.acquire():
            count = self._finished_msg_count
        self._finished_msg_count_lock.release()
        return count

    #############################################################
    # total msg count opreation
    ############################################################# 
    def _inc_total_msg_count(self):
        if self._total_msg_count_lock.acquire():
            self._total_msg_count += 1
        self._total_msg_count_lock.release()

    def get_total_msg_count(self):
        count = None
        if self._total_msg_count_lock.acquire():
            count = self._total_msg_count
        self._total_msg_count_lock.release()
        return count

    def runtime_info(self):
        detail = []
        total = self.get_total_msg_count()
        in_progress = total - self.get_finished_msg_count()

        for node in self.stream:
            node_summary, node_detail = node.runtime_info()
            detail.append({node.name: {"summary": node_summary, "detail": node_detail}})
        summary = {"total": total, "in_progress": in_progress}
        return summary, detail

    def put_2_next_node(self, event):
        while True:
            event.pos += 1
            # 工作流遍历到最后一个，结束
            if event.pos >= len(self.stream): 
                return None

            node = self.stream[event.pos]
            # node.input_request(event.msg, self.handler_result_cb,
            #                    self.handler_exception_cb, event)

            if node.type == "handler":
                node.input_request(event.msg, self.handler_result_cb,
                            self.handler_exception_cb, event)
            elif node.type == "output":
                try:
                    node.input_request(event.msg, self.handler_output_result_cb,
                                       self.handler_output_exception_cb, None)
                    self.handler_result_cb(event, PROC_CONTINUE, None)
                except:
                    self.handler_exception_cb(event, sys.exc_info())
            else:
                raise Exception("Unknow node type %s" % node.type)
            return node

    def send_result(self, result, async_context):
        handle_result = async_context['handle_result']
        private_data = async_context['private_data']
        handle_result(result, private_data)
        self._inc_finished_msg_count()

    def handler_output_exception_cb(self, private_data, exc_info):
        logger.error(str(exc_info))
        pass

    def handler_output_result_cb(self, private_data, *result):
        pass

    def handler_exception_cb(self, private_data, exc_info):
        event = private_data
        async_context = event.private_data
        handle_exception = async_context['handle_exception']
        private_data = async_context['private_data']
        handle_exception((exc_info), private_data)
        self._inc_finished_msg_count()

    def handler_result_cb(self, private_data, ret, info):
        event = private_data
        async_context = event.private_data

        node_name = self.stream[event.pos].name

        if ret == PROC_CONTINUE:
            logger.debug("%s Proc event result: PROC_CONTINUE, reason: %s" %
                    (node_name, str(info)))
            node = self.put_2_next_node(event)
            if not node:
                self.send_result((True, event.msg), async_context)
        elif ret == PROC_UPDATE:
            logger.debug("%s Proc event result: PROC_UPDATE" %
                    (node_name,))
            event.msg = info
            node = self.put_2_next_node(event)
            if not node:
                self.send_result((True, event.msg), async_context)
        elif ret == PROC_STOP:
            logger.debug("%s Proc event result: PROC_STOP, reason: %s" %
                    (node_name, str(info)))
            self.send_result((False, "Stream Proc Stop! reason: %s" % info),
                    async_context)

    def input_request(self, msg, handle_result, handle_exception,
            private_data):
        self._inc_total_msg_count()
        async_context = {
                "handle_result": handle_result,
                "handle_exception": handle_exception,
                "private_data": private_data
            }
        event = ProcStreamEvent(msg, async_context)
        node = self.put_2_next_node(event)
        if not node:
            # Stream Proc finish, do callback
            handle_result((True, msg))

    def start(self):
        for node in self.stream:
            node.start()

    def stop(self):
        for node in self.stream:
            node.stop()


