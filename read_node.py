# -*- coding:utf-8 -*-
import time
import importlib
import threading
from utils import print_traceback

from ..stream_proc.log import get_logger

logger = get_logger()

class ReaderEvent:

    def __init__(self, raw, finish_callback, private_data):
        self.raw = raw
        self.private_data = private_data
        self.finish_callback = finish_callback

    def get_raw(self):
        return self.raw

    def msg_finish_ack(self):
        self.finish_callback(self.private_data)


def get_module_class(module_path):
    module = module_path.rsplit('.', 1)
    class_name = module[1]
    module = importlib.import_module(module[0])
    aClass = getattr(module, class_name,  None)
    return aClass


class ReaderOutputAlreadyExist(Exception):
    pass

class ReaderNode:

    def __init__(self, name, conf):
        self.name = name

        module_path = conf['module'] # requisite
        reader_cls = get_module_class(module_path)
        if reader_cls == None:
            raise Exception("No module %s" % module_path)
        reader_args = conf.get("args")

        self.pool = []
        pool_size = conf.get('pool_size', 1)
        for i in range(pool_size):
            name = self.name + ".unit%s" % (i + 1)
            reader = reader_cls(reader_args)
            t = ReaderThread(reader, self.reader_callback, name)
            self.pool.append(t)

        self.reader_output = None

    def runtime_info(self):
        summary = {}
        detail = {}
        total = 0
        in_progress = 0

        for r in self.pool:
            one_total_count = r.get_total_msg_count()
            one_in_progress_count = r.get_in_progress_msg_count()
            one_progress_speed = r.get_progress_speed()

            detail[r.name] = {
                    "total": one_total_count, 
                    "in_progress": one_in_progress_count,
                    "speed": one_progress_speed}

            total += one_total_count
            in_progress += one_in_progress_count

        summary = {"total": total, "in_progress": in_progress}

        return summary, detail

    def total_msg_count(self):
        total = 0
        detail = {}
        for r in self.pool:
            count = r.get_total_msg_count()
            detail[r.name] = count
            total += count
        return total, detail

    def in_progress_msg_count(self):
        total = 0
        detail = {}
        for r in self.pool:
            count = r.get_in_progress_msg_count()
            detail[r.name] = count
            total += count
        return total, detail

    def reader_callback(self, raw, private_data):
        self.reader_output(raw, private_data)

    def register_output(self, callback):
        if self.reader_output:
            raise ReaderOutputAlreadyExist

        self.reader_output = callback

    def unregister_output(self, callback):
        self.reader_output = None

    def start(self):
        for r in self.pool:
            r.start()

    def stop(self):
        for r in self.pool:
            r.stop()
        for r in self.pool:
            r.join()

class ReaderThread(threading.Thread):

    def __init__(self, reader, read_callback, name=None):
        threading.Thread.__init__(self)
        self.setDaemon(1)
        self.name = name
        self._reader = reader

        self._read_callback = read_callback

        self._total_msg_count = 0
        self._total_msg_count_lock = threading.Lock()

        self._finished_msg_count = 0
        self._finished_msg_count_lock = threading.Lock()

        self._in_progress_msg_count = 0
        self._in_progress_msg_count_lock = threading.Lock()

        self._progress_speed = 0
        self._progress_speed_lock = threading.Lock()

        self._end = threading.Event()
        self._pause = threading.Event()
        self._pause.clear()

        self._poll_time = 1
        self._monitor_t = threading.Thread(target=self._monitor_thread)
        self._monitor_t_stop = threading.Event()

    def _monitor_thread(self):
        """
            monitor thread
        """
        last_finished_count = 0
        while True:
            time.sleep(self._poll_time)

            if self._monitor_t_stop.isSet():
                break

            current_finished_count = self.get_finished_msg_count()
            speed_count = current_finished_count - last_finished_count
            speed = speed_count / self._poll_time
            self._set_progress_speed(speed)
            last_finished_count = current_finished_count

    #############################################################
    # progress info
    ############################################################# 
    def _set_progress_speed(self, speed):
        if self._progress_speed_lock.acquire():
            self._progress_speed = speed
        self._progress_speed_lock.release()

    def get_progress_speed(self):
        speed = 0
        if self._progress_speed_lock.acquire():
            speed = self._progress_speed
        self._progress_speed_lock.release()
        return speed

    def get_in_progress_msg_count(self):
        total = self.get_total_msg_count()
        finished = self.get_finished_msg_count()
        return total - finished

    #############################################################
    # finished msg count operation
    ############################################################# 
    def _inc_finished_msg_count(self):
        if self._finished_msg_count_lock.acquire():
            self._finished_msg_count += 1
        self._finished_msg_count_lock.release()

    def get_finished_msg_count(self):
        count = None
        if self._finished_msg_count_lock.acquire():
            count = self._finished_msg_count
        self._finished_msg_count_lock.release()
        return count

    #############################################################
    # total msg count opreation
    ############################################################# 
    def _inc_total_msg_count(self):
        if self._total_msg_count_lock.acquire():
            self._total_msg_count += 1
        self._total_msg_count_lock.release()

    def get_total_msg_count(self):
        count = None
        if self._total_msg_count_lock.acquire():
            count = self._total_msg_count
        self._total_msg_count_lock.release()
        return count
    
    #############################################################
    # 等待所有消息finish
    ############################################################# 
    def _wait_for_all_msg_finish(self):
        while True:
            count = self.get_in_progress_msg_count()
            logger.info("%s _wait_for_all_msg_finish, unfinished: %s" %
                    (self.name, count))
            if 0 == count:
                break
            time.sleep(self._poll_time)

    #############################################################
    # main functions
    ############################################################# 
    def msg_finish_ack(self, private_data):
        finish_ack_cb = private_data['finish_ack_cb']
        private_data = private_data['private_data']

        try:
            finish_ack_cb(private_data)
        except:
            print_traceback()

        self._inc_finished_msg_count()
        
    def msg_recv_callback(self, raw, finish_ack_cb, private_data):
        try:
            self._inc_total_msg_count()

            private_data = {
                    "finish_ack_cb": finish_ack_cb,
                    "private_data": private_data
                }
            event = ReaderEvent(raw, self.msg_finish_ack, private_data)
            self._read_callback(raw, event)
        except Exception, e:
            print_traceback()

    def run(self):
        self._monitor_t.start()

        if callable(getattr(self._reader, 'initialize', None)):                                                                                                                                                   
            self._reader.initialize()

        self._reader.start(self.msg_recv_callback)
        self._wait_for_all_msg_finish()

        if callable(getattr(self._reader, 'finish', None)):                                                                                                                                                   
            self._reader.finish()


    def stop(self):
        self._end.set()
        self._reader.stop()
        self._monitor_t_stop.set()
        self._monitor_t.join()

