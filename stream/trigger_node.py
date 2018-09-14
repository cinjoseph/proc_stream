# -*- coding:utf-8 -*-
import threading
from utils import print_traceback

from log import get_logger

logger = get_logger()


class ReaderOutputAlreadyExist(Exception):
    pass


class TriggerInitError(Exception):

    def __init__(self, err):
        Exception.__init__(self, err)


class TriggerNotImplement(Exception):
    pass


class Trigger(object):

    def __init__(self, emit):
        self.emit = emit
        self._stop_signal = threading.Event()

    def initialize(self, trigger_conf):
        raise TriggerNotImplement

    def start(self):
        raise TriggerNotImplement

    def stop(self):
        self._stop_signal.set()

    def emit(self, data):
        self.emit(data)



class TriggerThread(threading.Thread):

    def __init__(self, trigger_cls, trigger_conf, controller_emit_callback, name=None):
        threading.Thread.__init__(self)
        self.setDaemon(1)
        self.name = name
        self._trigger_cls = trigger_cls
        self._trigger_conf = trigger_conf
        self._start_success = threading.Event()
        self._trigger = None
        self._controller_emit_callback = controller_emit_callback

    def trigger_emit_callback(self, raw):
        self._controller_emit_callback(raw)

    def run(self):
        try:
            self._trigger = self._trigger_cls(self.trigger_emit_callback)
        except:
            print_traceback(logger)
            exit()
        self._trigger.initialize(self._trigger_conf)
        self._start_success.set()
        self._trigger.start()

    def stop(self):
        if self._trigger:
            self._trigger.stop()



class TriggerNodeController:

    def __init__(self, name, emit, trigger_cls, trigger_conf, pool_size=1):
        self.name = name

        self._pool = []
        for i in range(pool_size):
            name = self.name + "-unit" + str(i+1)
            t = TriggerThread(trigger_cls, trigger_conf, self.controller_emit_callback, name=name)
            self._pool.append(t)

        self._emit = emit
        self._emit_lock = threading.Lock()

    def controller_emit_callback(self, raw):
        # 多个trigger thread 只能有一个同时对外输出
        if self._emit_lock.acquire():
            self._emit(raw)
        self._emit_lock.release()

    def start(self):
        logger.debug("Start Trigger %s" % self.name)
        for trigger in self._pool:
            trigger.start()
            logger.debug("  |- Start Trigger %s" % trigger.name)
            if not trigger._start_success.wait(10):
                raise TriggerInitError("Trigger %s init timeout" % trigger.name)
        logger.debug(" --- " )

    def stop(self):
        for r in self._pool:
            r.stop()
        for r in self._pool:
            r.join()

