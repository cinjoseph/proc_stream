# -*- coding: utf-8 -*-

# standard library modules
import time
import Queue
import threading
import importlib
from process_node import ProcNodeController
from trigger_node import TriggerNodeController
from utils import print_traceback

import log
logger = log.get_logger()


def get_module_class(module_path):
    module = module_path.rsplit('.', 1)
    class_name = module[1]
    module = importlib.import_module(module[0])
    aClass = getattr(module, class_name,  None)
    return aClass


class TriggerControllerFactory:

    def __init__(self, conf):
        self.conf = conf

    def new(self, trigger_name, stream_name, emit):
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

        trigger_name =  stream_name + "." + trigger_name
        trigger_controller = \
            TriggerNodeController(trigger_name, emit, trigger_cls, trigger_conf, pool_size=pool_size)

        return trigger_controller


class ProcNodeFactory:

    def __init__(self, conf):
        self.conf = conf

    def new(self, node_name, stream_name, emit):
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
        mutli_type = conf.get('mutli_type', 'single')

        node_name = stream_name + "." +node_name
        node = ProcNodeController(node_name, node_cls, node_args, emit,
                pool_size=pool_size, poll_timeout=poll_timeout, mutli_type=mutli_type)
        return node


class StreamEvent:

    def __init__(self, data, stream_id, pos=-1):
        self.data = data
        self.stream_id = stream_id
        self.pos = pos


class Stream():

    def __init__(self, name, stream_conf, trigger_template, node_template):
        self.name = name

        self._event_queue = Queue.Queue()
        self._stream_queue_lock = threading.Lock()

        self._stop_signal = threading.Event()

        emit = None
        self.stream_process = []
        process_controller_factory = ProcNodeFactory(node_template)
        for node_name in stream_conf['process'][::-1]:
            proc_node = process_controller_factory.new(node_name, self.name, emit)
            logger.info("register %s emit func as %s" % (proc_node.name, emit))

            emit = proc_node.input
            self.stream_process.append(proc_node)
        self.stream_process.reverse()

        logger.info("init stream process finish: %s " % [p.name for p in self.stream_process])

        emit = None
        if len(self.stream_process) > 0:
            emit = self.stream_process[0].input
        self.stream_trigger = []

        trigger_controller_factory = TriggerControllerFactory(trigger_template)
        for trigger_name in stream_conf['trigger']:
            trigger = trigger_controller_factory.new(trigger_name, self.name, emit)
            if not trigger:
                raise Exception("Stream init Error: trigger `%s` doesn't exist" % trigger_name)
            self.stream_trigger.append(trigger)

    def start(self):
        for process in self.stream_process[::-1]:
            process.start()
        for trigger in self.stream_trigger:
            trigger.start()

    def stop(self):
        for trigger in self.stream_trigger:
            trigger.stop()
        for process in self.stream_process[::-1]:
            process.stop()
        self._stop_signal.set()


class StreamController(object):

    def __init__(self, conf, hb_inter=5, hb_cb=None):
        """

        :param conf: 流事件的配置
        :param hb_cb: 心跳处理函数
        """
        self.loop_stop  = threading.Event()
        self.conf       = conf
        self.hb_cb      = hb_cb
        self.hb_inter   = hb_inter

    def start(self):
        node_template = self.conf['NodeTemplate']
        reader_template = self.conf['TriggerTemplate']
        streams_conf = self.conf['Streams']

        streams = []

        logger.info("--------------------------------")
        logger.info("All Streams Start !!!")

        for stream_name, stream_conf in streams_conf.items():
            logger.info("Init stream %s" % stream_name)
            s = Stream(stream_name, stream_conf, reader_template, node_template)
            streams.append(s)

        # start streams
        for s in streams:
            logger.info("Start stream %s !!!" % stream_name)
            s.start()

        count = 0
        while not self.loop_stop.is_set():
            if count == self.hb_inter and self.hb_cb:
                count = 0
                # self.hb_cb([ s.get_info() for s in streams ])
                logger.info("Do Heart Beat, need exit now? %s" % (self.loop_stop.is_set()))
            count += 1
            time.sleep(1)

        # stop streams
        for s in streams:
            s.stop()
            logger.info("stop stream %s !!!" % stream_name)

        logger.info("All Streams Stop !!!")

    def stop(self):
        self.loop_stop.set()



