# -*- coding: utf-8 -*-

# standard library modules


import os
import time
import threading
import multiprocessing
from collections import OrderedDict
from Queue import Empty

from process_node import ProcNodeController
from trigger_node import TriggerNodeController
from utils import get_module_class, exception_catcher

import log

logger = log.get_logger()


class StreamHeartBeatNotImplement(Exception):
    pass


class StreamHeartBeat(object):

    def __init__(self, name, interval, controller, args):
        self.name = name
        self.args = args
        self.interval = interval
        self.controller = controller
        self.timer = None

    def cancel(self):
        if self.timer:
            self.timer.cancel()

    def start(self):
        self.timer = threading.Timer(self.interval, self.do_heart_beat)
        self.timer.start()

    def do_heart_beat(self):
        self.heart_beat(self.controller)
        self.start()

    def intialize(self):
        self._init(*self.args)

    def _init(self, args):
        raise StreamHeartBeatNotImplement

    def heart_beat(self, controller):
        raise StreamHeartBeatNotImplement

    def _fini(self):
        raise StreamHeartBeatNotImplement


def create_trigger_node(stream_name, name, conf):
    trigger_name = name
    module_path = conf.get('module', None)
    if not module_path:
        raise Exception("%s conf doesn't has module conf" % trigger_name)

    trigger_cls = get_module_class(module_path)
    if not trigger_cls:
        raise Exception("Can not get class %s %s" % (trigger_name, module_path))

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
        raise Exception("Load moudle %s.%s Error: %s" % (module_path, node_name, str(e)))
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
    for name, cfg in config['processer'].items():
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


class Stream:

    def __init__(self, name, config, notify_queue):
        self.name = name
        self.start_event = multiprocessing.Event()
        self.stop_event = multiprocessing.Event()
        args = (self.name, config, self.start_event, self.stop_event, notify_queue)
        self.process = multiprocessing.Process(target=stream_main, name=name, args=args)

    def terminate(self, wait_time, loop_time=0.5):
        self.process.terminate()
        time_count = 0
        while self.process.is_alive():
            time.sleep(loop_time)
            time_count += loop_time
            if time_count > wait_time:
                return False
        return True

    def start(self):
        self.process.start()
        if self.start_event.wait(3):
            return True
        logger.error("start child timeout, terminate it ")
        if self.terminate(3):
            return False
        logger.error("terminate child failed, ignore it ")
        return False

    def stop(self):
        if not self.stop_event.is_set():
            self.stop_event.set()
        if self.process.join(15):
            return True
        logger.error("join child failed, terminate it ")
        if self.terminate(10):
            return True
        logger.error("terminate child failed, ignore it ")
        return False


class StreamController(object):

    def __init__(self, conf, poll_time=1):
        self.queue = multiprocessing.Queue()
        self.loop_stop = threading.Event()
        self.conf = conf
        self.poll_time = poll_time

        self.runtime_info = {}  # dict 线程安全

        self.streams = {}
        self.heartbeats = {}

    def init_heartbeat(self, name, cfg):
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
        heart_beat_conf = self.conf['HeartBeat']
        node_template = self.conf['NodeTemplate']
        reader_template = self.conf['TriggerTemplate']

        stream_cfg = {}
        for name, stream in self.conf['Streams'].items():
            stream_cfg[name] = {}
            stream_cfg[name]['trigger'] = OrderedDict()
            stream_cfg[name]['trigger'][stream[0]] = reader_template[stream[0]]
            stream_cfg[name]['processer'] = OrderedDict()
            for pname in stream[1:][::-1]:
                stream_cfg[name]['processer'][pname] = node_template[pname]

        logger.info("--------------------------------")
        # logger.info("All Streams Start !!!")

        # 初始化Stream实例列表
        for name, cfg in stream_cfg.items():
            logger.info("Init stream %s" % name)
            self.streams[name] = Stream(name, cfg, self.queue)

        # 初始化所有心跳回调
        for name, cfg in heart_beat_conf.items():
            logger.info("Init HeartBeat %s" % name)
            self.heartbeats[name] = self.init_heartbeat(name, cfg)

        # 启动所有Stream
        for name, s in self.streams.items():
            if not s.start():
                logger.error("Start stream %s Failed!!!" % s.name)
            else:
                logger.error("Start stream %s Success!!!" % s.name)

        # 启动所有HeratBeat
        for name, hb in self.heartbeats.items():
            hb.start()

        while not self.loop_stop.is_set():
            try:
                ret = self.queue.get(timeout=self.poll_time)
                for k, v in ret.items():
                    self.runtime_info[k] = v
            except Empty:
                pass

        for name, hb in self.heartbeats.items():
            hb.cancel()
            logger.info("Cancle HeartBeat %s !!!" % name)
        logger.info("All HeartBeat cancled !!!")

        for name, stream in self.streams.items():
            stream.stop()
            logger.info("stop stream %s !!!" % name)
        logger.info("All Streams Stop !!!")

    def stop(self):
        self.loop_stop.set()
