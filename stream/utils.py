# -*- coding: utf-8 -*- 

import traceback
def print_traceback(logger=None):
    bt = traceback.format_exc()
    bt = bt.split('\n')
    for s in bt:
        if logger == None:
            print str(s)
        else:
            logger.error(str(s))

import importlib
def get_module_class(module_path):
    module = module_path.rsplit('.', 1)
    class_name = module[1]
    module = importlib.import_module(module[0])
    aClass = getattr(module, class_name,  None)
    return aClass


