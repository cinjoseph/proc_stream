# -*- coding: utf-8 -*-

# standard library modules


import os
import time
import signal
import threading
import multiprocessing
from collections import OrderedDict
from Queue import Empty

from process_node import ProcNodeController
from trigger_node import TriggerNodeController
from utils import get_module_class, exception_catcher

import logger


class StreamHeartBeatNotImplement(Exception):
    pass


class StreamHeartBeat(object):

    def __init__(self, name, interval, controller, args):
        self.name = name
        self.args = args
        self.interval = interval
        self.controller = controller
        self.timer = None
        self.stop = False

    def cancel(self):
        if self.timer:
            self.timer.cancel()
            logger.info("Cancle HeartBeat %s !!!" % self.name)
        self.stop = True

    def start(self):
        self.timer = threading.Timer(self.interval, self.do_heart_beat)
        self.timer.start()

    def do_heart_beat(self):
        self.heart_beat(self.controller)
        if not self.stop:
            self.start()

    def intialize(self):
        self._init(*self.args)

    def _init(self, args):
        raise StreamHeartBeatNotImplement

    @exception_catcher
    def heart_beat(self, controller):
        raise StreamHeartBeatNotImplement

    def _fini(self):
        raise StreamHeartBeatNotImplement


def create_trigger_node(stream_name, name, conf):
    trigger_name = name
    module_path = conf.get('module', None)
    if not module_path:
        raise Exception("%s conf doesn't has module conf" % trigger_name)
    try:
        trigger_cls = get_module_class(module_path)
    except ImportError, e:
        raise Exception("Load Trigger %s's module: %s Error: %s" % (trigger_name, module_path, str(e)))
    trigger_conf = conf.get('args', {})
    pool_size = conf.get('pool_size', 1)

    trigger_name = stream_name + "." + trigger_name
    trigger_controller = \
        TriggerNodeController(trigger_name, trigger_cls, trigger_conf, pool_size=pool_size)

    return trigger_controller


def create_processer_node(stream_name, name, conf):
    node_name = name
    module_path = conf.get('module', None)
    if not module_path:
        raise Exception("%s conf doesn't has module conf" % node_name)
    try:
        node_cls = get_module_class(module_path)
    except ImportError, e:
        raise Exception("Load node %s's module: %s Error: %s" % (node_name, module_path, str(e)))
    node_args = conf.get("args", {})
    pool_size = conf.get('pool_size', 1)
    poll_timeout = conf.get('poll_timeout', 1)
    mode = conf.get('mode', 'single')

    _filter = conf.get('filter', None)
    _filter = [] if _filter is None else _filter
    if type(_filter) != list:
        raise Exception("Error filter type")

    node_name = stream_name + "." + node_name
    node = ProcNodeController(node_name, node_cls, node_args,
                              pool_size=pool_size, poll_timeout=poll_timeout, mode=mode, filter=_filter)
    return node


def init(stream_name, config):
    processers = []
    triggers = []
    emit = None
    for name, cfg in config['processer'].items()[::-1]:
        proc_node = create_processer_node(stream_name, name, cfg)
        proc_node.register_emit(emit)
        emit = proc_node.input
        processers.append(proc_node)
        logger.info("Processer register %s emit func as %s" % (proc_node.name, emit))
    processers.reverse()

    logger.info("init stream process finish: %s " % [p.name for p in processers])

    emit = None
    if len(processers) > 0:
        emit = processers[0].input

    for name, cfg in config['trigger'].items():
        trigger = create_trigger_node(stream_name, name, cfg)
        trigger.register_emit(emit)
        logger.info("Trigger register %s emit func as %s" % (trigger.name, emit))
        triggers.append(trigger)
    return triggers, processers


def get_stream_runtime_info(triggers, processers):
    info = {'trigger': {}, 'process': {}}
    for trigger in triggers:
        info['trigger'][trigger.name] = {'emit': trigger.runtime_info()}
    for process in processers[::-1]:
        recv_count, emit_count, drop_count = process.runtime_info()
        info['process'][process.name] = {'emit': emit_count, 'recv': recv_count, 'drop': drop_count}
    return info


def start_stream(triggers, processers):
    # 从最后一个开始启动
    for process in processers[::-1]:
        process.start()
    for trigger in triggers:
        trigger.start()
    logger.info(" ----- ")


def stop_stream(triggers, processers):
    for trigger in triggers:
        trigger.stop()
        # 从第一个开始停止
    for process in processers:
        process.stop()


@exception_catcher(logger.error)
def stream_main(name, config, start_event, stop_event, notify_queue):
    logger.init(name)
    stream_obj = init(name, config)
    logger.info("Start Stream %s, pid=%s" % (name, os.getpid()))
    start_stream(*stream_obj)
    start_event.set()
    while True:
        if stop_event.is_set():
            break
        runtime_info = get_stream_runtime_info(*stream_obj)
        notify_queue.put({name: runtime_info})
        time.sleep(1)
    logger.info("Stop Stream %s, pid=%s" % (name, os.getpid()))
    stop_stream(*stream_obj)
    logger.fini()


class Stream:

    def __init__(self, name, config, notify_queue):
        self.name = name
        self.config = config
        self.start_event = multiprocessing.Event()
        self.stop_event = multiprocessing.Event()
        args = (self.name, self.config, self.start_event, self.stop_event, notify_queue)
        self.process = multiprocessing.Process(target=stream_main, name=name, args=args)

    def kill(self, wait_time, loop_time=0.5):
        # self.process.terminate()
        os.kill(self.process.pid, signal.SIGKILL)
        time_count = 0
        while self.process.is_alive():
            time.sleep(loop_time)
            time_count += loop_time
            if time_count > wait_time:
                return False
        return True

    def start(self):
        self.process.start()
        if self.start_event.wait(15):
            return True
        logger.error("start child timeout, terminate it ")
        if self.kill(15):
            return False
        logger.error("kill child failed, ignore it ")
        return False

    def stop(self):
        if not self.stop_event.is_set():
            self.stop_event.set()
        self.process.join(15)
        if not self.process.is_alive():
            return True
        logger.error("join child failed, terminate it ")
        if self.kill(15):
            return True
        logger.error("kill child failed, ignore it ")
        return False


class StreamController(object):

    def __init__(self, conf, poll_time=1):
        self.notify_queue = multiprocessing.Queue()
        self.loop_stop = threading.Event()
        self.conf = conf
        self.poll_time = poll_time
        self.runtime_info = {}  # dict 线程安全
        self.streams = {}
        self.heartbeats = {}
        self.oper_lock = threading.Lock()

    def acquire_oper_lock(func):
        def wapper(*args):
            controller = args[0]
            if controller.oper_lock.acquire(10):
                func(*args)
                controller.oper_lock.release()
            else:
                logger.error("acquire oper_lock timeout, %s do not exec" % func)

        return wapper

    @acquire_oper_lock
    def start_stream(self, name):
        if name not in self.conf['Streams']:
            raise Exception('start_stream Error, Stream %s does not exsit' % name)

        stream_cfg = {}
        trigger_name = self.conf['Streams'][name][0]
        processer_name_list = self.conf['Streams'][name][1:]

        stream_cfg['trigger'] = OrderedDict()
        if trigger_name in self.conf['TriggerTemplate']:
            stream_cfg['trigger'][trigger_name] = self.conf['TriggerTemplate'][trigger_name]
        else:
            raise Exception('start_stream Error, Trigger %s does not exsit' % trigger_name)

        stream_cfg['processer'] = OrderedDict()
        for processer_name in processer_name_list:
            if processer_name in self.conf['NodeTemplate']:
                stream_cfg['processer'][processer_name] = self.conf['NodeTemplate'][processer_name]
            else:
                raise Exception('start_stream Error, ProcNode %s does not exsit' % processer_name)

        if name in self.streams:
            raise Exception('start_stream Error, Stream %s already exist' % name)
        stream = Stream(name, stream_cfg, self.notify_queue)

        if not stream.start():
            logger.error("Start stream %s Failed!!!" % name)
            return

        self.streams[name] = stream
        logger.info("Start stream %s Success!!!" % name)

    @acquire_oper_lock
    def stop_stream(self, name):
        logger.info("Ready to Stop stream %s !!!" % name)
        if name not in self.streams.keys():
            raise Exception('stop_stream Error, Stream %s does not exist' % name)
        self.streams[name].stop()
        del self.streams[name]
        logger.info("Stop stream %s Success !!!" % name)

    def restart_stream(self, name):
        if name in self.streams:
            logger.info("Restart Stream %s" % name)
            self.stop_stream(name)
            self.start_stream(name)
        else:
            logger.error("Restart Stream Error, Strean %s does not exist" % name)

    def init_heartbeat(self, name, cfg):
        logger.info("Init HeartBeat %s" % name)
        cls = get_module_class(cfg['module'])
        if not cls:
            raise Exception("HeartBeat Module %s doesn't exist")
        if not issubclass(cls, StreamHeartBeat):
            raise Exception("Init Heartbeat Error, error class type %s" % cls)
        interval = cfg.get('interval', 5)
        args = cfg.get('args', {})
        return cls(name, interval, self, args)

    @exception_catcher(logger.error)
    def start(self):
        # 初始化logger
        logger.info("--------------------------------")

        # 启动所有Stream
        for name in self.conf['Streams']:
            self.start_stream(name)

        # 启动所有HeratBeat
        for name, cfg in self.conf['HeartBeat'].items():
            self.heartbeats[name] = self.init_heartbeat(name, cfg)
            self.heartbeats[name].start()

        while not self.loop_stop.is_set():
            try:
                ret = self.notify_queue.get(timeout=self.poll_time)
                for k, v in ret.items():
                    self.runtime_info[k] = v
            except Empty:
                pass

        for name, hb in self.heartbeats.items():
            hb.cancel()
        logger.info("All HeartBeat cancled !!!")

        stream_name_list = self.streams.keys()
        for name in stream_name_list:
            self.stop_stream(name)
        logger.info("All Stoped !!!")

    def stop(self):
        if not self.loop_stop.is_set():
            self.loop_stop.set()
