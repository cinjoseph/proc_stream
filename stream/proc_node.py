# -*- coding: utf-8 -*-

__author__ = "chenjianing"

# standard library modules
import sys
import time
import Queue
import threading

from utils import print_traceback

PROC_EXCEPTION  = -2 # 处理中发生预期外的异常
PROC_STOP       = -1 # 处理异常停止

PROC_CONTINUE   =  0 # 处理完毕，无任何变动
PROC_UPDATE     =  1 # 处理有更新

PROC_BREAK      =  2 # 处理进入异步阶段
PROC_FINISH     =  3 # 处理正常完成

import log
logger = log.get_logger()

class PoolNotReady(Exception):
    pass


class ProcThreadInitError(Exception):

    def __init__(self, err):
        Exception.__init__(self, err)


class ProcNodeRequest:

    def __init__(self, args=None, kwds=None, 
            callback=None, exc_callback=None, 
            private_data=None):

        self.id = hash(id(self))
        self.callback = callback
        self.exc_callback = exc_callback
        self.args = args or []
        self.kwds = kwds or {}
        self.private_data = private_data


class AsyncWorkerContext:

    def __init__(self, request, callback_hook=None, exc_callback_hook=None):
        self._request = request
        self._callback_hook = callback_hook
        self._exc_callback_hook = exc_callback_hook

    def async_callback(self, result):
        if self._callback_hook:
            self._callback_hook(self._request, result)
        if not isinstance(result, tuple):
            result = (result,)
        self._request.callback(self._request.private_data, *result)

    def async_exc_callback(self, exc_info):
        if self._exc_callback_hook:
            self._exc_callback_hook(self._request, exc_info)
        if not isinstance(exc_info, tuple):
            exc_info = (exc_info,)
        self._request.exc_callback(self._request.private_data, exc_info)


class NodeThread(threading.Thread):

    def __init__(self, cls, args, request_queue, poll_timeout=1,
             name=None,):
        threading.Thread.__init__(self)
        self.setDaemon(1)
        if name:
            self.name = name
        self._requests_queue = request_queue
        self._dismissed = threading.Event()
        self._poll_timeout = poll_timeout
        self._cls = cls
        self._args = args

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

    def async_callback_hook(self, request, result):
        self._inc_finished_msg_count()

    def async_exc_callback_hook(self, request, exc_info):
        self._inc_finished_msg_count()
  
    def run(self):
        try:
            worker = self._cls(self._args)
        except :
            print_traceback(logger)
            exit()

        _type = worker._type

        if callable(getattr(worker, 'initialize', None)):
            worker.initialize()

        self._start_success.set()

        while True:
            try:
                request = self._requests_queue.get(True, self._poll_timeout)
                self._inc_total_msg_count()
            except Queue.Empty:
                if self._dismissed.isSet():
                    self_unfinished = self.get_total_msg_count() - self.get_finished_msg_count()
                    if self_unfinished == 0:
                        break
                continue
            else:
                if "_sync" == _type:
                    try:
                        result = worker.proc(*request.args, **request.kwds)
                        self._inc_finished_msg_count()
                        try:
                            request.callback(request.private_data, *result)
                        except:
                            print_traceback(logger)
                    except:
                        self._inc_finished_msg_count()
                        request.exc_callback(request.private_data, sys.exc_info())
                elif "_async" == _type:
                    try:
                        async_context = AsyncWorkerContext(request,
                                self.async_callback_hook, 
                                self.async_exc_callback_hook)
                        worker.proc(async_context, *request.args, **request.kwds)
                        self._inc_finished_msg_count()
                    except:
                        self._inc_finished_msg_count()
                        request.exc_callback(request.private_data, sys.exc_info())

        if callable(getattr(worker, 'finish', None)):
            worker.finish()

    def stop(self):
        self._dismissed.set()


class ProcNode:

    def __init__(self, name, node_cls, node_args, pool_size=1, 
            poll_timeout=1):
        self.name = name
        self.stop_event = threading.Event()
        self._requests_queue = Queue.Queue()

        self.type = node_cls._node_type

        self.pool = []
        for i in range(pool_size):
            name = self.name + "-unit" + str(i+1)
            t = NodeThread(node_cls, node_args, self._requests_queue,
                    poll_timeout=poll_timeout, name=name)
            self.pool.append(t)

        self._poll_timeout = poll_timeout

        self._total_msg_count = 0
        self._total_msg_count_lock = threading.Lock()

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
        detail = {}
        total = 0
        finished = 0

        for unit in self.pool:
            one_total_count = unit.get_total_msg_count()
            one_finished_count = unit.get_finished_msg_count()

            detail[unit.name] = {
                    "total": one_total_count, 
                    "finished": one_finished_count}

            total += one_total_count
            finished += one_finished_count

        summary = {"total": self.get_total_msg_count(),
                   "subrecv": total,
                   "finished": finished}

        return summary, detail

    def input_request(self, req, handle_result=None, handle_exception=None, private_data=None):
        request = self.make_request(req, handle_result, handle_exception, private_data)
        self.put_request(request)
        return request

    def make_request(self, req, handle_result=None, handle_exception=None,
            private_data=None):
        request = ProcNodeRequest([req], None, handle_result, handle_exception,
                private_data)
        return request

    def put_request(self, request):
        if self.pool == None:
            raise PoolNotReady
        self._inc_total_msg_count()
        self._requests_queue.put(request)

    def start(self):
        logger.debug("Start Proc Node %s" % self.name)
        for proc_thread in self.pool:
            proc_thread.start()
            logger.debug("  |- Start Proc Unit %s" % (proc_thread.name,))
            if not proc_thread._start_success.wait(3):
                raise ProcThreadInitError("proc thread %s init exception" % proc_thread.name)

    def dismiss_all_threads(self):
        if len(self.pool) < 1:
            raise Exception("Pool is Null!")
        elif len(self.pool) == 1:
            while self._requests_queue.qsize():
                time.sleep(0.5)

        logger.debug("Stop Proc Node %s" % self.name)
        for proc_thread in self.pool:
            proc_thread.stop()
            logger.debug("  |- Stop Proc Unit %s" % proc_thread.name)

        logger.debug("Join Proc Node %s" % self.name)
        for proc_thread in self.pool:
            proc_thread.join()
            logger.debug("  |- Join Proc Unit %s" % proc_thread.name)

    def stop(self):
        while True:
            summary, detail = self.runtime_info()
            total = summary['total']
            finished = summary['finished']
            logger.info( "%s _wait_for_all_msg_finish, unfinished: %s" % 
                    (self.name, total-finished))
            if total - finished == 0:
                break
            time.sleep(self._poll_timeout/2)

        for i in range(len(self.pool)):
            self.dismiss_all_threads()


