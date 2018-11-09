# -*- coding: utf-8 -*-

__author__ = "cinjoseph"

# standard library modules
import logging
import logging.handlers

logger_level = "debug"

logger_name = "main"

logger_path = "/var/log/"

console_log = False


def get_level(level_str):
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


def logger_handler_exist(logger, name):
    for handler in logger.handlers:
        h_name = handler.get_name()
        if name == h_name:
            return True
    return False


def init_rotating_file_handler(name, level, format_str, path):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    file_log = logging.handlers.RotatingFileHandler(path, mode='a',
                                                    maxBytes=20 * 1024 * 1024, backupCount=100)
    file_log.setLevel(level)
    file_log.setFormatter(logging.Formatter(format_str))
    logger.addHandler(file_log)
    return logger


def init_stream_handler(name, level, format_str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    console_log = logging.StreamHandler()
    console_log.setLevel(level)
    console_log.setFormatter(logging.Formatter(format_str))
    logger.addHandler(console_log)
    return logger


def init(name=None):
    global logger_name
    if name:
        logger_name = name
    name = logger_name
    format_str = "%(asctime)s - %(levelname)-8s --> %(message)s"
    level = get_level(logger_level)
    path = logger_path + "/%s.log" % name
    logger = init_rotating_file_handler(name, level, format_str, path)
    if console_log:
        logger = init_stream_handler(name, level, format_str)
    return logger


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


def debug(s):
    logger = logging.getLogger(logger_name)
    logger.debug(s)


def info(s):
    logger = logging.getLogger(logger_name)
    logger.info(s)


def warning(s):
    logger = logging.getLogger(logger_name)
    logger.warning(s)


def error(s):
    logger = logging.getLogger(logger_name)
    logger.error(s)


def critical(s):
    logger = logging.getLogger(logger_name)
    logger.critical(s)
