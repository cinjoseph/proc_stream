# -*- coding: utf-8 -*-

__author__ = "cinjoseph"

# standard library modules
import datetime
import threading
import Queue
import logging
import logging.handlers

from utils import print_traceback, get_module_class

g_logger_level = "debug"
g_logger_name = "main"
g_logger_path = "/var/log/"
g_console_log = False


g_local_logger = None
g_remote_logger = None
g_loggers = {}

g_remote_logger_template = {}

__log_level__ = {
    "debug": 0,
    "info": 1,
    "error": 2,
    "warning": 3,
    "critical": 4,
}


def remote_logger_pending_count():
    global g_remote_logger
    return g_remote_logger.get_pending_count()


class LoggerNotImplement(Exception):
    pass


class UnknowLoggerType(Exception):
    pass


class LocalLogger:
    format_str = "%(asctime)s - %(levelname)-8s --> %(message)s"

    def __init__(self, name, level, path, console_print=False):
        self.level = self.get_level(level)
        self.name = name
        self.path = path + "/%s.log" % name
        self.console_print = console_print
        self.logger = self.init()

    def get_level(self, level_str):
        if level_str == "debug":
            LOG_LEVEL = logging.DEBUG
        elif level_str == "info":
            LOG_LEVEL = logging.INFO
        elif level_str == "error":
            LOG_LEVEL = logging.ERROR
        elif level_str == "warning":
            LOG_LEVEL = logging.WARNING
        elif level_str == "critical":
            LOG_LEVEL = logging.CRITICAL
        else:
            raise Exception("Unknow level_str %s" % level_str)
        return LOG_LEVEL

    def local_logger_handler_exist(self, logger, name):
        for handler in logger.handlers:
            h_name = handler.get_name()
            if name == h_name:
                return True
        return False

    def init(self):
        logger = logging.getLogger(self.name)
        logger.setLevel(logging.DEBUG)

        file_log = logging.handlers.RotatingFileHandler(self.path, mode='a', maxBytes=20 * 1024 * 1024, backupCount=100)
        file_log.setLevel(self.level)
        file_log.setFormatter(logging.Formatter(self.format_str))
        logger.addHandler(file_log)

        if self.console_print:
            console_log = logging.StreamHandler()
            console_log.setLevel(self.level)
            console_log.setFormatter(logging.Formatter(self.format_str))
            logger.addHandler(console_log)
        return logger

    def debug(self, body):
        self.log('debug', body)

    def info(self, body):
        self.log('info', body)

    def warning(self, body):
        self.log('warning', body)

    def error(self, body):
        self.log('error', body)

    def critical(self, body):
        self.log('critical', body)

    def log(self, level, body):
        if __log_level__[level] < __log_level__[g_logger_level]:
            return
        self.logger.log(self.get_level(level), body)


class CustomRemoteLogger(object):

    def __init__(self, name, conf, level, enable):
        self.name = name
        self.conf = conf
        self.level = level
        self.enable = enable
        self.is_init = False

    def initialize(self):
        try:
            self._init(self.conf)
            self.is_init = True
        except LoggerNotImplement:
            pass

    def finish(self):
        try:
            if self.is_init:
                self._fini()
        except LoggerNotImplement:
            pass

    def _init(self, conf):
        raise LoggerNotImplement

    def _fini(self):
        raise LoggerNotImplement

    def log(self, ts, name, level, body):
        raise LoggerNotImplement


class RemoteLogger(threading.Thread):

    def __init__(self, name, level):
        threading.Thread.__init__(self)
        self.logger_queue = Queue.Queue()
        self.stop_event = threading.Event()
        self.logger_name = name
        self.logger_level = level
        self.loggers = []

    def get_pending_count(self):
        return self.logger_queue.qsize()

    def register_custom_logger(self, logger):
        self.loggers.append(logger)

    def log(self, level, body):
        if len(self.loggers) == 0:
            return
        log_data = {
            "timestamp": datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f+0800'),
            "name": self.logger_name,
            "level": level,
            "body": body,
        }
        self.logger_queue.put(log_data)

    def run(self):
        for logger in self.loggers:
            logger.initialize()

        while True:
            try:
                data = self.logger_queue.get(True, 1)
            except Queue.Empty:
                if self.stop_event.is_set():
                    break
                continue
            else:
                ts = data["timestamp"]
                name = data["name"]
                level = data["level"]
                body = data["body"]
                for logger in self.loggers:
                    try:
                        if not logger.enable:
                            continue
                        if __log_level__[level] < __log_level__[logger.level]:
                            continue
                        logger.log(ts, name, level, body)
                        g_local_logger.log("debug", "send log `%s` to remote logger %s" % (data, logger.name))
                    except:
                        print_traceback(g_local_logger)

        for logger in self.loggers:
            logger.finish()

    def stop(self):
        self.stop_event.set()


def init(name=None):
    global g_local_logger
    global g_remote_logger
    global g_logger_name
    global g_logger_level
    global g_logger_path
    global g_console_log

    if not name:
        name = g_logger_name
    g_logger_name = name
    local_logger = LocalLogger(g_logger_name, g_logger_level, g_logger_path, g_console_log)
    g_local_logger = local_logger

    remote_logger = RemoteLogger(g_logger_name, g_logger_level)
    for name, conf in g_remote_logger_template.items():
        cls = get_module_class(conf['module'])
        args = conf['args']
        level = conf.get('level', 'debug')
        enable = conf.get('enable', True)
        if not issubclass(cls, CustomRemoteLogger):
            raise UnknowLoggerType
        remote_logger.register_custom_logger(cls(name, args, level, enable))
    remote_logger.start()
    g_local_logger.log("info", "Process %s's custom logger start" % (g_logger_name))

    g_remote_logger = remote_logger


def fini():
    # g_local_logger.log("info", "Process %s stop remote logger %s" % (g_logger_name, g_logger_name))
    g_remote_logger.stop()
    g_remote_logger.join()
    g_local_logger.log("info", "Process %s's custom logger stoped" % (g_logger_name))


"""
同时 getlogger 在用logger输出 10w条的时间数据
0:00:09.100858 
0:00:09.016348
0:00:09.235863

先 getlogger 在用logger输出 10w条的时间数据
0:00:08.828259
0:00:09.327315
0:00:08.880530

下面的情况稍微快一点
"""


def log(level, body):
    global g_local_logger
    global g_remote_logger
    g_local_logger.log(level, body)
    g_remote_logger.log(level, body)


def debug(body):
    log('debug', body)


def info(body):
    log('info', body)


def warning(body):
    log('warning', body)


def error(body):
    log('error', body)


def critical(body):
    log('critical', body)
