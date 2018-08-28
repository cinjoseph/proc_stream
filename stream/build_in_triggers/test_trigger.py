import time
from stream.trigger_node import Trigger

class TimerTrigger(Trigger):

    def initialize(self, conf):
        self.sleep_time = conf.get('sleep_time', 1)

    def start(self):
        count = 0
        while not self._stop_signal.is_set():
            count +=1
            self.emit(count)
            time.sleep(self.sleep_time)