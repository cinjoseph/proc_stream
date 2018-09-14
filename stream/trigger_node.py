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

    def __init__(self, name, conf, emit):
        self.name = name
        self.conf = conf
        self.emit = emit

    def initialize(self):
        try:
            self._init(self.conf)
        except TriggerNotImplement:
            pass

    def finish(self):
        try:
            self._fini()
        except TriggerNotImplement:
            pass

    def stop(self):
        try:
            self._stop()
        except TriggerNotImplement:
            pass

    def emit(self, data):
        self.emit(data)

    def start(self):
        raise TriggerNotImplement

    def _init(self, conf):
        raise TriggerNotImplement

    def _fini(self):
        raise TriggerNotImplement

    def _stop(self):
        raise TriggerNotImplement



class TriggerThread:

    def __init__(self, cls, args, controller_emit_callback, name=None):
        self.name = name if name else self.name
        self._trigger = cls(name, args, self.trigger_emit_callback)
        self._start_success = threading.Event()
        self._controller_emit_callback = controller_emit_callback
        self.thread = threading.Thread(target=self.thread_run)

    def trigger_emit_callback(self, raw):
        self._controller_emit_callback(raw)

    def thread_run(self):
        self._trigger.initialize()
        self._start_success.set()
        self._trigger.start()
        self._trigger.finish()

    def start(self):
        self.thread.start()

    def stop(self):
        if self._start_success.is_set():
            self._trigger.stop()
            self.thread.join()



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

        self._emit_count = 0

    def controller_emit_callback(self, raw):
        # 多个trigger thread 只能有一个同时对外输出
        if self._emit_lock.acquire():
            self._emit_count += 1
            self._emit(raw)
        self._emit_lock.release()

    def start(self):
        logger.info("Start Trigger %s" % self.name)
        for trigger in self._pool:
            trigger.start()
            logger.info("  |- Start Trigger Unit %s id:%s" % (trigger.name, id(trigger)))
            if not trigger._start_success.wait(5):
                raise TriggerInitError("Trigger %s init timeout" % trigger.name)
        logger.info(" --- " )

    def stop(self):
        logger.info("Stop Trigger %s" % self.name)
        for trigger in self._pool:
            trigger.stop()
            logger.info("  |- Stop Trigger Unit %s id:%s" % (trigger.name, id(trigger)))

    def runtime_info(self):
        return self._emit_count

