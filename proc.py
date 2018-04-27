# -*- coding: utf-8 -*-

__author__ = "chenjianing"
__version__ = "0.1.0"

# standard library modules
import os
import threading
import multiprocessing
import queue
import time
from abc import ABCMeta, abstractmethod

from utils import print_traceback

DROP = -1
CONTINUE = 0
UPDATE = 1


class ParasiteClass:
    __metaclass__ = ABCMeta

    @abstractmethod
    def structure(self):
        pass

    @abstractmethod
    def process(self, msg):
        pass

    @abstractmethod
    def destructure(self):
        pass


class Parasite(threading.Thread):

    def __init__(self, parasite_cls, parasite_args, in_queue, out_queue,
                 in_queue_timeout=3, out_queue_timeout=3, **kwds):
        threading.Thread.__init__(self, **kwds)
        self.parasite_cls = parasite_cls
        self.parasite_args = parasite_args
        self.parasite = parasite_cls(parasite_args)
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.in_queue_timeout = in_queue_timeout
        self.out_queue = out_queue
        self.out_queue_timeout = out_queue_timeout
        self.is_start = threading.Event()
        self.need_stop = threading.Event()

    def run(self):
        parasite = self.parasite_cls(self.parasite_args)
        parasite.structure()
        while not self.need_stop.is_set():
            try:
                msg = self.in_queue.get(timeout=self.in_queue_timeout)
            except queue.Empty:
                continue

            action, newmsg = parasite.process(msg)
            print("%s process result %s, %s"% (self.name, action, newmsg))
            if action == DROP:
                pass
            elif action == CONTINUE:
                self.out_queue.put(msg, timeout=self.out_queue_timeout)
            elif action == UPDATE:
                self.out_queue.put(newmsg, timeout=self.out_queue_timeout)
            else:
                raise Exception("Unknow action")

        parasite.destructure()

    def stop(self):
        print("%s stop" % self.name)
        self.need_stop.set()


def _proc_node_exc_handler(exc_info):
    print("This is _proc_node_exc_handler %s" % exc_info)


# 单个处理节点
class ProcNode:

    def __init__(self, proc_cls, proc_args, name, parasite_num=3):
        self.id = id(self)
        self.inQueue = multiprocessing.Queue(maxsize=0)
        self.outQueue = multiprocessing.Queue(maxsize=0)
        self.start_success = multiprocessing.Event()
        self.start_success.clear()
        self.need_stop = multiprocessing.Event()
        self.need_stop.clear()
        self.process = None
        self.parasite_cls = proc_cls
        self.parasite_args = proc_args

        self.nodeName = name
        self.parasite_num = parasite_num
        self.parasite_list = []

    def read_out(self, timeout=3):
        read_buf = self.outQueue.get(timeout=timeout)
        return read_buf

    def write_in(self, write_buf):
        """
            writeBuff format: json  {"index": _id, "body": body}
        """
        self.inQueue.put(write_buf)

    def wait_parasite_join(self):
        while not self.need_stop.is_set():
            time.sleep(0.5)

        for parasite in self.parasite_list:
            parasite.stop()
        for parasite in self.parasite_list:
            parasite.join()

    def _main_loop(self):
        try:
            for i in range(self.parasite_num):
                parasiq te_name = "%s.parasite-%s" % (self.nodeName, i)
                parasite = Parasite(self.parasite_cls,
                                    self.parasite_args,
                                    self.inQueue, self.outQueue,
                                    in_queue_timeout=3, out_queue_timeout=3,
                                    name=parasite_name)
                self.parasite_list.append(parasite)
            for parasite in self.parasite_list:
                parasite.start()
        except Exception:
            print_traceback()
            for parasite in self.parasite_list:
                parasite.stop()
        else:
            self.start_success.set()
        finally:
            self.wait_parasite_join()

    def start(self):
        self.process = multiprocessing.Process(target=self._main_loop)
        self.process.start()
        if self.start_success.wait(2):
            self.start_success.clear()

    def stop(self):
        self.need_stop.set()
        self.process.join()


class TestClass(ParasiteClass):

    def __init__(self, attr):
        self.name = attr['name']

    def structure(self):
        print("Struct func")
        pass

    def process(self, msg):
        msg += "!"
        return UPDATE, msg

    def destructure(self):
        pass


if __name__ == "__main__":
    print("father is %s" % os.getpid())
    proc_cls = TestClass
    proc_args = {"name": 'lalala'}
    name = "testNode"
    node = ProcNode(proc_cls, proc_args, name="testNode")
    node.start()
    node.write_in("Hello world ... ")
    count = 0
    while True:
        time.sleep(1)
        print("")
        try:
            out_buf = node.read_out(2)
            node.write_in(out_buf)
        except queue.Empty:
            print("Node father read outQueue : None")\

        if count < 3:
            break
        count += 1

    node.stop()


