# -*- coding: utf-8 -*-

__author__ = "chenjianing"

# standard library modules
import Queue
import threading

from utils import print_traceback

import log
logger = log.get_logger()


class PoolNotReady(Exception):
    pass


class NodeNotImplement(Exception):
    pass


class ProcThreadInitError(Exception):

    def __init__(self, err):
        Exception.__init__(self, err)


class OutputProcessNode(object):

    def __init__(self, name, conf):
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

    def _init(self, conf):
        raise NodeNotImplement

    def _fini(self):
        raise NodeNotImplement

    def proc(self, data):
        raise NodeNotImplement

    def emit(self, data):
        self.emit(data)


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


class ProcessNodeThread(threading.Thread):

    def __init__(self, cls, args, controller_emit_cb, poll_timeout=1, name=None):
        threading.Thread.__init__(self)
        self.setDaemon(1)
        if name:
            self.name = name
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
        self._dismissed.set()
        self.thread.join()


class ProcNodeController:

    def __init__(self, name, node_cls, node_args, emit=None, pool_size=1, poll_timeout=1, mutli_type='single'):
        # basic arguement
        self.name = name
        self._poll_timeout = poll_timeout
        self._emit = emit
        self._emit_lock = threading.Lock()

        self.pool_index = 0

        self.pool = []
        proc_node_cls = None
        if mutli_type == 'single':
            pool_size = 1
            logger.warn('Node %s mutli_type is `single`, force set pool_size=1 !' % (self.name))
            proc_node_cls = ProcessNodeCoroutine
        elif 'thread' == mutli_type:
            proc_node_cls = ProcessNodeThread
        elif 'process' == mutli_type:
            raise Exception("mutli_type process is not supprot yet!")

        for i in range(pool_size):
            name = self.name + "-unit" + str(i + 1) + "[%s]" %  mutli_type
            t = proc_node_cls(node_cls, node_args, self.controller_emit_callback,
                               poll_timeout=poll_timeout, name=name)
            self.pool.append(t)

    def controller_emit_callback(self, data):
        if self._emit:
            if self._emit_lock.acquire():
                self._emit(data)
            self._emit_lock.release()
        else:
            logger.debug("%s next emit is None, Finished Process" % self.name)

    def input(self, event):
        if 0 == len(self.pool):
            raise PoolNotReady
        # TODO： 目前根据到达的数据包平分event，后续改成根据负载均分
        self.pool[self.pool_index].input(event)
        self.pool_index += 1
        if self.pool_index > len(self.pool) - 1:
            self.pool_index = 0

    def start(self):
        logger.debug("Start Proc Node %s" % self.name)
        for process_node in self.pool:
            process_node.start()
            logger.debug("  |- Start Proc Unit %s id:%s" % (process_node.name, id(process_node)))
            if not process_node._start_success.wait(10):
                raise ProcThreadInitError("proc thread %s init timeout" % process_node.name)

    def stop(self):
        logger.debug("Stop Proc Node %s" % self.name)
        for process_node in self.pool:
            process_node.stop()
            logger.debug("  |- Stop Proc Unit %s id:%s" % (process_node.name, id(process_node)))

