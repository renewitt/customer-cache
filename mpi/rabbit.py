"""
These methods are mostly replaced or implemented by mrabbit.
"""
import amqp

class Rabbit:
    """ Replaces mrabbit main class. """
    def __init__(
            self, host, user, password, consumer_bindings):
        """ Initialise RabbitMQ client. """
        self.host = host
        self.user = user
        self.password = password

        # AMQP connections/channels
        self.conn = None
        self.consume_channel = None

        # service config
        self.message_callback = None
        self.consumer_bindings = consumer_bindings

    def _connect(self):
        """ Create a connection to RabbitMQ. """
        conn = amqp.Connection(
            host=self.host,
            userid=self.user,
            password=self.password)
        conn.connect()
        return conn

    def init_consumer(self):
        """ Initialise consume connection. """
        if self.conn is None or not self.conn.connected:
            self.conn = self._connect()

        if self.consume_channel is None:
            self.consume_channel = self.conn.channel()

        self._declare_exchange()
        self._declare_queue_bind()

    def _declare_exchange(self):
        exchange = self.consumer_bindings['exchange']
        args = {
            "alternate-exchange": "dead-letter"
        }

        self.consume_channel.exchange_declare(
            exchange,       # exchange name
            "direct",       # exchange type,
            durable=True,
            auto_delete=False,
            arguments=args,
        )

    def _declare_queue_bind(self):
        """ Declare queue for consuming. """
        if self.consume_channel is None:
            self.init_consumer()

        keys = self.consumer_bindings['keys']
        exchange = self.consumer_bindings['exchange']
        queue_name = self.consumer_bindings['input_queue']
        max_queue_length = self.consumer_bindings['queue_size']

        self.consume_channel.queue_declare(
            queue_name, durable=True, arguments={
            "x-message-ttl": 60000,
            "x-dead-letter-exchange": "dead-letter",
            "x-max-length": max_queue_length,
        })
        for key in keys:
            self.consume_channel.queue_bind(queue_name, exchange=exchange, routing_key=key)

    def stop(self):
        if self.consume_channel is not None:
            self.consume_channel.close()

        if self.conn is not None:
            self.conn.close()

    def consume(self, callback):
        """
        Get messages from the queue.

        :param callback: the method to call when a message is received
        """
        if self.consume_channel is None:
            self.init_consumer()

        self.consume_channel.basic_consume(
            queue=self.consumer_bindings['input_queue'],
            callback=callback)

        while True:
            self.conn.drain_events()
