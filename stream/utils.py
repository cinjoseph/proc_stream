# -*- coding: utf-8 -*- 

import os
import traceback
import threading
import importlib
from functools import wraps


def print_traceback(logger=None):
    bt = traceback.format_exc()
    bt = bt.split('\n')
    for s in bt:
        if logger == None:
            print str(s)
        else:
            logger.error(str(s))


def get_module_class(module_path):
    module = module_path.rsplit('.', 1)
    module_path = module[0]
    class_name = module[1]
    module = importlib.import_module(module_path)
    aClass = getattr(module, class_name, None)
    if aClass is None:
        raise Exception("No class `%s` in module `%s`" % (class_name, module_path))
    return aClass


def exception_catcher(err_printer):
    def func_wapper(func):
        @wraps(func)
        def wapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                curr_t = threading.currentThread()
                pid = os.getpid()
                err_printer("Error Occur in pid:%s thread:%s" % (pid, curr_t.name))
                [err_printer(str(s)) for s in traceback.format_exc().split('\n')]

        return wapper

    return func_wapper
