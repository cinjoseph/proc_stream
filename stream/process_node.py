# -*- coding: utf-8 -*-

__author__ = "cinojseph"

# standard library modules
import time
import Queue
import threading

from utils import print_traceback

import log
logger = log.get_logger()


class PoolNotReady(Exception):
    pass


class NodeNotImplement(Exception):
    pass


class UnknownNodeType(Exception):
    pass

class ProcThreadInitError(Exception):

    def __init__(self, err):
        Exception.__init__(self, err)


class OutputProcessNode(object):

    def __init__(self, name, conf, emit=None):
        self.name = name
        self.conf = conf
        self.is_init = False

    def initialize(self):
        try:
            self._init(self.conf)
            self.is_init = True
        except NodeNotImplement:
            pass

    def finish(self):
        try:
            if self.is_init:
                self._fini()
        except NodeNotImplement:
            pass

    def _init(self, conf):
        raise NodeNotImplement

    def _fini(self):
        raise NodeNotImplement

    def proc(self, data):
        raise NodeNotImplement



class HandlerProcessNode(object):

    def __init__(self, name, conf, emit):
        self.name = name
        self.emit = emit
        self.conf = conf
        self.is_init = False

    def initialize(self):
        try:
            self._init(self.conf)
            self.is_init = True
        except NodeNotImplement:
            pass

    def finish(self):
        try:
            if self.is_init:
                self._fini()
        except NodeNotImplement:
            pass

    def emit(self, data):
        self.emit(data)

    def _init(self, conf):
        raise NodeNotImplement

    def _fini(self):
        raise NodeNotImplement

    def proc(self, data):
        raise NodeNotImplement



class ProcessNodeCoroutine:

    def __init__(self, cls, args, controller_emit_cb, poll_timeout=1, name=None):
        if name:
            self.name = name
        self._dismissed = threading.Event()
        self._start_success = threading.Event()
        self.processer = cls(self.name, args, controller_emit_cb)

    def input(self, event):
        if None == self.processer:
            raise PoolNotReady
        try:
            self.processer.proc(event)
        except:
            print_traceback(logger)

    def start(self):
        self.processer.initialize()
        self._start_success.set()

    def stop(self):
        self.processer.finish()
        self._dismissed.set()



class ProcessNodeThread():

    def __init__(self, cls, args, controller_emit_cb, poll_timeout=1, name=None):
        self.name = name if name else self.name
        self._poll_timeout = poll_timeout
        self._event_queue = Queue.Queue()
        self._dismissed = threading.Event()
        self._start_success = threading.Event()
        self.thread = threading.Thread(target=self.thread_run)
        self.processer = cls(self.name, args, controller_emit_cb)

    def input(self, event):
        self._event_queue.put(event)

    def thread_run(self):
        self.processer.initialize()
        self._start_success.set()
        while True:
            try:
                event = self._event_queue.get(True, self._poll_timeout)
            except Queue.Empty:
                if self._dismissed.is_set():
                    break
                continue
            else:
                try:
                    self.processer.proc(event)
                except:
                    print_traceback(logger)
        self.processer.finish()

    def start(self):
        self.thread.start()

    def stop(self):
        while True:
            logger.info( "%s _wait_for_all_msg_finish, unfinished: %s" % (self.name, self._event_queue.qsize()))
            time.sleep(self._poll_timeout)
            if 0 == self._event_queue.qsize():
                break

        if not self._dismissed.is_set():
            self._dismissed.set()
            self.thread.join()


class ProcessNodeProcess():
    # TODO: 实现进程节点
    pass



class ProcNodeController:

    def __init__(self, name, node_cls, node_args, emit=None, pool_size=1, poll_timeout=1, mode='single'):
        # arguement check
        if not (issubclass(node_cls, OutputProcessNode) or issubclass(node_cls, HandlerProcessNode)):
            raise UnknownNodeType

        # basic arguement
        self.name = name
        self._emit = emit
        self._emit_lock = threading.Lock()
        self._pool = []
        self._pool_index = 0
        self._poll_timeout = poll_timeout
        self._is_output = True if issubclass(node_cls, OutputProcessNode) else False

        self._emit_count = 0

        self._recv_count = 0

        node_mode_set = {
            'single'    : (ProcessNodeCoroutine, 1),
            'thread'    : (ProcessNodeThread, pool_size),
            'process'   : (None, pool_size)
        }
        proc_node_cls, pool_size = node_mode_set.get(mode, (None, None))
        if not proc_node_cls:
            raise Exception("Node mode %s is not supprot yet!" % mode)

        for i in range(pool_size):
            name = self.name + "-unit" + str(i + 1) + "[%s]" %  mode[0]
            node = proc_node_cls(node_cls, node_args, self.controller_emit_callback,
                                 poll_timeout=poll_timeout, name=name)
            self._pool.append(node)

    def controller_emit_callback(self, event):
        if self._emit:
            if self._emit_lock.acquire():
                self._emit(event)
                self._emit_count += 1
            self._emit_lock.release()
        else:
            # logger.debug("%s next emit is None, Finished Process" % self.name)
            pass

    def input(self, event):
        if 0 == len(self._pool):
            raise PoolNotReady
        # TODO： 目前根据到达的数据包平分event，后续改成根据负载均分
        self._pool[self._pool_index].input(event)
        self._pool_index += 1
        if self._pool_index > len(self._pool) - 1:
            self._pool_index = 0
        self._recv_count += 1

        # 如果是输出节点，直接返回将Event返回
        if self._is_output:
            self.controller_emit_callback(event)

    def start(self):
        logger.debug("Start Proc Node %s" % self.name)
        for process_node in self._pool:
            process_node.start()
            logger.debug("  |- Start Proc Unit %s id:%s" % (process_node.name, id(process_node)))
            if not process_node._start_success.wait(10):
                raise ProcThreadInitError("proc thread %s init timeout" % process_node.name)

    def stop(self):
        logger.debug("Stop Proc Node %s" % self.name)
        for process_node in self._pool:
            process_node.stop()
            logger.debug("  |- Stop Proc Unit %s id:%s" % (process_node.name, id(process_node)))

    def runtime_info(self):
        return self._recv_count, self._emit_count

