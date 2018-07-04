
import time
import threading

class TestReader():

    def __init__(self, conf):
        self._need_stop = threading.Event()

    def start(self, recv_callback):
        count = 0
        while not self._need_stop.is_set():
            count +=1
            recv_callback("msg.%s" % count, None, None)
            time.sleep(1)

    def stop(self):
        self._need_stop.set()
