
# -*- coding: utf-8 -*-

__author__ = "cinjoseph"

# standard library modules
import logging
import logging.handlers


logger_name = "stream_proc"


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
            maxBytes=20*1024*1024, backupCount=100)
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


def init_logger(level, path, console_log=False, name=None):
    if not name:
        name = logger_name
    format_str = "%(asctime)s - %(levelname)-8s --> %(message)s"
    level = get_level(level)
    logger = init_rotating_file_handler(name, level, format_str, path)
    if console_log:
        logger = init_stream_handler(name, level, format_str)
    return logger


def get_logger(name=None):
    if not name:
        name = logger_name
    return logging.getLogger(name)


