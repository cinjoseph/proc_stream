import time
import threading
from stream.trigger_node import Trigger


class TimerTrigger(Trigger):

    def _init(self, conf):
        self.poll_time = conf.get('poll_time', 1)
        self._stop_event = threading.Event()
        self._stop_event.clear()

    def start(self):
        count = 0
        while not self._stop_event.is_set():
            count += 1
            self.emit(str(count))
            time.sleep(self.poll_time)

    def _stop(self):
        self._stop_event.set()
