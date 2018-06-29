
import time
import threading

class TestReader():

    def __init__(self, conf):
        self._need_stop = threading.Event()

    def msg_finish_ack(self, private_data):
        pass

    def start(self, recv_callback):
        count = 0
        while not self._need_stop.is_set():
            count +=1
            recv_callback("msg.%s" % count, self.msg_finish_ack, None)
            time.sleep(1)
		

    def stop(self):
        self._need_stop.set()
