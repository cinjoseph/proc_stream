# -*- coding:utf-8 -*-

import pika
import json

from stream.process_node import PROC_STOP, PROC_CONTINUE, PROC_UPDATE
from stream.log import get_logger

logger = get_logger()


class RabbitMqWriter:

    _type = "_sync"
    _node_type = "output"

    def __init__(self, conf):
        self.url, self.exchange = tuple(conf['url'].rsplit('.', 1))

    def initialize(self):
        self._connection = pika.BlockingConnection(pika.URLParameters(self.url))
        self._channel = self._connection.channel()

    def send2mq(self, data):
        try:
            self._channel.basic_publish(exchange=self.exchange, routing_key='', body=data)
        except pika.exceptions.ConnectionClosed:
            logger.error('The AMQP connection was lost, reconnect ...')
            self.initialize()
            self._channel.basic_publish(exchange=self.exchange, routing_key='', body=data)

    def proc(self, data):
        data = json.dumps(data)
        self.send2mq(data)
        logger.info("send msg to mq")

        return PROC_CONTINUE, None

    def finish(self):
        self._connection.close()
