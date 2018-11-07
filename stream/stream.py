# -*- coding: utf-8 -*-

# standard library modules
import time
import Queue
import threading
from process_node import ProcNodeController
from trigger_node import TriggerNodeController
from utils import get_module_class
import log

logger = log.get_logger()


class StreamHeartBeatNotImplement(Exception):
    pass


class StreamHeartBeat(object):

    def __init__(self, args):
        self.args = args

    def intialize(self):
        self._init(self.args)

    def _init(self, args):
        raise StreamHeartBeatNotImplement

    def heart_beat(self, infos):
        raise StreamHeartBeatNotImplement

    def _fini(self):
        raise StreamHeartBeatNotImplement


class TriggerControllerFactory:

    def __init__(self, conf):
        self.conf = conf

    def new(self, trigger_name, stream_name):
        conf = self.conf.get(trigger_name, None)
        if not conf:
            return None
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


class ProcNodeFactory:

    def __init__(self, conf):
        self.conf = conf

    def new(self, node_name, stream_name):
        conf = self.conf[node_name]
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


class StreamEvent:

    def __init__(self, data, stream_id, pos=-1):
        self.data = data
        self.stream_id = stream_id
        self.pos = pos


class Stream:

    def __init__(self, name, stream_conf, trigger_template, node_template):
        self.name = name
        emit = None
        self.stream_process = []
        process_controller_factory = ProcNodeFactory(node_template)
        for node_name in stream_conf['process'][::-1]:
            proc_node = process_controller_factory.new(node_name, self.name)
            proc_node.register_emit(emit)
            emit = proc_node.input
            self.stream_process.append(proc_node)
            logger.info("register %s emit func as %s" % (proc_node.name, emit))
        self.stream_process.reverse()

        logger.info("init stream process finish: %s " % [p.name for p in self.stream_process])

        emit = None
        if len(self.stream_process) > 0:
            emit = self.stream_process[0].input
        self.stream_trigger = []

        trigger_controller_factory = TriggerControllerFactory(trigger_template)
        for trigger_name in stream_conf['trigger']:
            trigger = trigger_controller_factory.new(trigger_name, self.name)
            trigger.register_emit(emit)
            if not trigger:
                raise Exception("Stream init Error: trigger `%s` doesn't exist" % trigger_name)
            self.stream_trigger.append(trigger)

    def start(self):
        # 从最后一个开始启动
        for process in self.stream_process[::-1]:
            process.start()
        for trigger in self.stream_trigger:
            trigger.start()

    def stop(self):
        for trigger in self.stream_trigger:
            trigger.stop()
        # 从第一个开始停止
        for process in self.stream_process:
            process.stop()

    def runtime_info(self):
        info = {'trigger': {}, 'process': {}}
        for trigger in self.stream_trigger:
            info['trigger'][trigger.name] = {'emit': trigger.runtime_info()}
        for process in self.stream_process[::-1]:
            recv_count, emit_count, drop_count = process.runtime_info()
            info['process'][process.name] = {'emit': emit_count, 'recv': recv_count, 'drop': drop_count}
        return info


class StreamController(object):

    def __init__(self, conf, poll_time=1, hb_inter=5):
        self.loop_stop = threading.Event()
        self.conf = conf
        self.poll_time = poll_time
        self.heart_beat_poll_count = int(float(hb_inter) / float(poll_time))

    def start(self):
        node_template = self.conf['NodeTemplate']
        reader_template = self.conf['TriggerTemplate']
        streams_conf = self.conf['Streams']
        heart_beat_conf = self.conf['HeartBeat']

        streams = []
        heart_beats = []

        logger.info("--------------------------------")
        logger.info("All Streams Start !!!")

        # 初始化Stream实例列表
        for stream_name, stream_conf in streams_conf.items():
            logger.info("Init stream %s" % stream_name)
            s = Stream(stream_name, stream_conf, reader_template, node_template)
            streams.append(s)

        # 初始化所有心跳回调
        for hb_name, hb_conf in heart_beat_conf.items():
            logger.info("Init Heart %s" % hb_name)
            cls = get_module_class(hb_conf['module'])
            if not cls:
                raise Exception("HeartBeat Module %s doesn't exist")
            hb_instance = cls(hb_conf.get('args', {}))
            heart_beats.append(hb_instance)

        # 启动所有Stream
        try:
            for s in streams:
                logger.info("Start stream %s !!!" % stream_name)
                s.start()
        except Exception, e:
            logger.error("Can not start stream '%s', reason: %s" % (stream_name, e))
            for s in streams:
                s.stop()
                logger.info("stop stream %s !!!" % stream_name)
            logger.info("All Streams Stop !!!")
            return

        count = 0
        while not self.loop_stop.is_set():
            time.sleep(self.poll_time)
            if count == self.heart_beat_poll_count:
                count = 0
                info = [s.runtime_info() for s in streams]
                for hb in heart_beats:
                    hb.heartbeat(info)
                logger.info("Do Heart Beat, need exit now? %s" % (self.loop_stop.is_set()))
            count += 1

        # stop streams
        for s in streams:
            s.stop()
            logger.info("stop stream %s !!!" % stream_name)
        logger.info("All Streams Stop !!!")

    def stop(self):
        self.loop_stop.set()
