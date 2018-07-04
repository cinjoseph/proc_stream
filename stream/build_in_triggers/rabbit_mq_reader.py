# -*- coding:utf-8 -*-
import pika
import time
import threading
from functools import partial


class RabbitMQReader():

    def __init__(self, conf):
        self.url, self.queue = tuple(conf['url'].rsplit('.', 1))
        self._need_ack = conf.get('need_ack', False)
        self._mode = conf.get('mode', "normal")# normal / fullspeed

        self._need_stop = threading.Event()

        self._channel_lock = threading.Lock()
        self._connection = None
        self._channel = None

    #############################################################
    # initialize & finish
    ############################################################# 
    def initialize(self):
        self._connection = pika.BlockingConnection(pika.URLParameters(self.url))
        self._channel = self._connection.channel()

    def finish(self):
        self._channel.close()
        self._connection.close()

    #############################################################
    # recv_callback & ack_callback
    ############################################################# 
    def msg_finish_ack(self, private_data):
        delivery_tag = private_data
        if self._need_ack:
            if "normal" == self._mode:
                self.normal_mode_msg_finish_ack(delivery_tag)
            elif "fullspeed" == self._mode:
                self.fullspeed_mode_msg_finish_ack(delivery_tag)

    def msg_recv_handler(self, recv_callback, channel, method, header, body):
        private_data = method.delivery_tag
        recv_callback(body, self.msg_finish_ack, private_data)

    #############################################################
    # full speed mode 处理函数 
    ############################################################# 
    def fullspeed_mode_msg_finish_ack(self, delivery_tag):
        def ack_callback(delivery_tag):
            self._channel.basic_ack(delivery_tag)
        self._connection.add_callback_threadsafe(
                partial(ack_callback, delivery_tag))

    def fullspeed_mode_stop(self):
        # TODO: 如何在停止时消费完当前pika队列里面的数据?
        def stop_callback():
            self._channel.stop_consuming()
        self._connection.add_callback_threadsafe(stop_callback)

    def fullspeed_mode_start(self, recv_callback):
        cb = partial(self.msg_recv_handler, recv_callback)
        self._consume_tag = self._channel.basic_consume(cb,
                self.queue, no_ack=not self._need_ack)
        self._channel.start_consuming()

    #############################################################
    # normal mode 处理函数 
    ############################################################# 
    def normal_mode_msg_finish_ack(self, delivery_tag):
        if self._channel_lock.acquire():
            self._channel.basic_ack(delivery_tag)
        self._channel_lock.release()

    def normal_mode_stop(self):
        self._need_stop.set()

    def normal_mode_start(self, recv_callback):
        self._need_stop.clear()
        while not self._need_stop.isSet():
            if self._channel_lock.acquire():
                method_frame, header_frame, body = self._channel.basic_get(
                        self.queue, no_ack=not self._need_ack)
            self._channel_lock.release()

            if method_frame:
                self.msg_recv_handler(recv_callback, self._channel, 
                        method_frame, header_frame, body)
            else:
                time.sleep(0.5)

    def start(self, recv_callback):
        if self._mode == "normal":
            self.normal_mode_start(recv_callback)
        if self._mode == "fullspeed":
            self.fullspeed_mode_start(recv_callback)

    def stop(self):
        if "normal" == self._mode:
            self.normal_mode_stop()
        elif "fullspeed" == self._mode:
            self.fullspeed_mode_stop()

