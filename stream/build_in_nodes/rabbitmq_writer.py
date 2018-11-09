# -*- coding:utf-8 -*-

import pika
import json

from stream.process_node import HandlerProcessNode

import stream.logger as logger


class RabbitMqWriter(HandlerProcessNode):

    def init(self, node_conf):
        self.url, self.exchange = tuple(node_conf['url'].rsplit('.', 1))
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
        self.emit(data)
        data = json.dumps(data)
        self.send2mq(data)
        logger.info("send msg to mq")

    def fini(self):
        self._connection.close()
