# -*- coding: utf-8 -*-

__author__ = "cinojseph"

# standard library modules
import ujson
import time
import Queue
import hashlib
import threading

from utils import print_traceback

from mini_ruler.ruler import Ruler, RulerNoMatch

import logger


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

    def __init__(self, name, cls, args, controller_emit_cb, poll_timeout=1, pool_size=1):
        self.name = name + "-unit[c]"
        self.processer = cls(self.name, args, controller_emit_cb)

    def get_event_pending_count(self):
        return 0

    def get_pool(self):
        return [self]

    def input(self, event):
        if None == self.processer:
            raise PoolNotReady
        try:
            self.processer.proc(event)
        except:
            print_traceback(logger)

    def start(self):
        logger.debug("  |- Start Proc Node %s" % self.name)
        self.processer.initialize()
        logger.debug("  |  |- Start Proc Unit %s id:%s" % (self.name, id(self)))

    def stop(self):
        logger.debug("  |- Stop Proc Node %s" % self.name)
        self.processer.finish()
        logger.debug("  |  |- Stop Proc Unit %s id:%s" % (self.name, id(self)))


class ProcessNodeThread:

    def __init__(self, name, cls, args, controller_emit_cb, poll_timeout=1, pool_size=1):
        self.name = name
        self._poll_timeout = poll_timeout
        self._event_queue = Queue.Queue()

        self._pool = []
        for i in range(pool_size):
            name = self.name + "-unit." + str(i + 1) + "[t]"
            processer = cls(self.name, args, controller_emit_cb)
            dismissed = threading.Event()
            start_success = threading.Event()
            thread = threading.Thread(target=self.thread_run, name=name,
                                      args=(processer, dismissed, start_success))

            self._pool.append((thread, dismissed, start_success))

    def get_event_pending_count(self):
        return self._event_queue.qsize()

    def get_pool(self):
        return [node[0] for node in self._pool]

    def input(self, event):
        self._event_queue.put(event)

    def thread_run(self, processer, dismissed, start_success):
        try:
            processer.initialize()
        except:
            print_traceback(logger)

        start_success.set()
        while True:
            try:
                event = self._event_queue.get(True, self._poll_timeout)
            except Queue.Empty:
                if dismissed.is_set():
                    break
                continue
            else:
                try:
                    processer.proc(event)
                except:
                    print_traceback(logger)
        processer.finish()

    def start(self):
        logger.debug("  |-Start Proc Node %s" % self.name)
        for t, _, start_success in self._pool:
            t.start()
            if not start_success.wait(10):
                # TODO: 是否需要强行杀死该线程？
                raise ProcThreadInitError("proc thread %s init timeout" % t.name)
            logger.debug("  |  |- Start Proc Unit %s id:%s" % (t.name, id(t)))

    def stop(self):

        logger.debug("  |- Stop Proc Node %s" % self.name)
        for _, dismissed, _ in self._pool:
            if not dismissed.is_set():
                dismissed.set()

        while True:
            logger.info("%s _wait_for_all_msg_finish, unfinished: %s" % (self.name, self._event_queue.qsize()))
            time.sleep(self._poll_timeout)
            if 0 == self._event_queue.qsize():
                break

        for t, _, _ in self._pool:
            t.join()
            logger.debug("  |  |- Stop Proc Unit %s id:%s" % (t.name, id(t)))


class ProcessNodeProcess():
    # TODO: 实现进程节点, 日后再说
    pass


class ProcNodeController:

    def __init__(self, name, node_cls, node_args, pool_size=1, poll_timeout=1, mode='single', filter=None):
        # arguement check
        if not (issubclass(node_cls, OutputProcessNode) or issubclass(node_cls, HandlerProcessNode)):
            raise UnknownNodeType

        self.filter = Ruler()
        self.filter.register_action('CONTINUE', 0)
        self.filter.register_action('ACCEPT', 1)
        self.filter.register_action('DROP', -1)
        _filter = filter if type(filter) == list else []
        self.filter.register_rule_set('local', _filter)

        # basic arguement
        self.name = name
        self._emit = None
        self._emit_lock = threading.Lock()
        self._poll_timeout = poll_timeout
        self._is_output = True if issubclass(node_cls, OutputProcessNode) else False

        self._recv_count = 0
        self._emit_count = 0
        self._drop_count = 0
        self._mode = mode

        node_mode_set = {
            'single': (ProcessNodeCoroutine, 1),
            'thread': (ProcessNodeThread, pool_size),
            'process': (None, pool_size)
        }
        if mode not in node_mode_set:
            raise Exception("Node mode %s doesn't exist!" % mode)

        proc_node_cls, pool_size = node_mode_set.get(mode)
        self.node = proc_node_cls(name, node_cls, node_args, self.controller_emit_callback,
                                  poll_timeout=poll_timeout, pool_size=pool_size)

    def register_emit(self, emit):
        self._emit = emit

    def controller_emit_callback(self, event):
        self._emit_count += 1
        if self._emit:
            # node间数据json格式流转。发送Event前先反序列化
            event = ujson.dumps(event)
            if self._emit_lock.acquire():
                self._emit(event)
            self._emit_lock.release()
        else:
            logger.debug("%s next emit is None, Finished Process" % self.name)
            pass

    def input(self, raw):
        # node间数据json格式流转。使用收到Event时先反序列化
        event = ujson.loads(raw)
        self._recv_count += 1
        # 检查Filter
        try:
            filter_result = self.filter.entry('local', event)
        except RulerNoMatch:
            filter_result = (None, 'no rule match')
        except(Exception) as e:
            logger.error("%s's Filter Error:%s !" % (self.name, str(e)))
            filter_result = -1, "Filter Error:%s" % str(e)

        (filter_result, rule) = (filter_result[0], filter_result[1]) if filter_result else (None, 'no rule match')

        if filter_result == 0:  # CONTINUE: 匹配中 CONTINUE  直接发送至下一个节点
            self.controller_emit_callback(event)
            logger.debug("%s's filter send event to next node! reason: '%s', raw:\n%s" % (self.name, rule, str(raw)))
        elif filter_result == -1:  # DROP: 匹配中 Drop 丢弃该event
            self._drop_count += 1
            logger.debug("%s's filter drop event! reason: '%s', raw:\n%s" % (self.name, rule, str(raw)))
        elif filter_result == 1 or filter_result is None:  # ACCEPT: 匹配中 ACCEPT 或未匹配中 接受 Event
            logger.debug("%s's filter accept event! reason: '%s', raw:\n%s" % (self.name, rule, str(raw)))
            self.node.input(event)
            if self._is_output:  # 如果是输出节点，直接返回将Event返回
                self.controller_emit_callback(event)
            pass
        else:
            raise Exception("Error filter result %s" % filter_result)

    def start(self):
        self.node.start()

    def stop(self):
        self.node.stop()

    def runtime_info(self):
        return self._recv_count, self._emit_count, self._drop_count, self._mode, self.node.get_event_pending_count()
