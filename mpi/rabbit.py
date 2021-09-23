"""
These methods are mostly replaced or implemented by mrabbit.
"""
import amqp
import logging
import datetime

# Having this set to 1 second is easiest for development as it
# makes the logs easier to follow, and is more than low enough to
# ensure we publish on time. In production, this value could be
# below 1 second.
TIMEOUT = 1

class Timer:
    """ Defines a timer. """
    def __init__(self, name, interval, method):
        """
        Sets initial timer properties.

        :param name: name of this timer
        :param interval: number of seconds between each method call
        :param method: the function to call when this timer expires
        """
        self.name = name
        self.interval = datetime.timedelta(seconds=interval)
        self.last_run = datetime.datetime.now()
        self.method = method

    def run(self):
        """
        Checks if the timer is due, and if it is, calls the method belonging to the
        timer, and updates the last_run time. Errors raised by the timer method are
        raised and should be handled by the client.
        """
        # we could also set a window here, so if we're within range of the timer,
        # just run it. This could be proportional to the interval to ensure it scales
        # nicely.
        timer_due = self.last_run + self.interval
        if timer_due <= datetime.datetime.now():
            self.method()
            self.last_run = datetime.datetime.now()

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
        self.logger = logging.getLogger("MRABBIT")
        self.message_callback = None
        self.consumer_bindings = consumer_bindings

        # These are used both for clients to add custom timer events, and
        # to track the internal state of magicrabbit, allowing us to manage
        # publishing mux data, and set a max duration for consume.
        self.timers = {}

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
            try:
                # This connection times out after the configured time, allowing us to
                # exit the blocking loop and do something else.
                self.conn.drain_events(TIMEOUT)
            except OSError:
                # We catch the OSError/socket.timeout so we can check the timers,
                # before continuing.
                self.check_timers()

    def check_timers(self):
        """
        This functions runs all the timers. The Timer class will only run the attached timer
        method if the timer is due.
        """
        # we use list around the items(), to create a shallow, view copy of the timer dict,
        # and iterate over that instead of the dict itself. Due to the way the timers are
        # called, its possible we could be deleting a timer at the same time we were iterating
        # over this dict, causing it to change size, and resulting in a RuntimeError.
        for name, timer in list(self.timers.items()):
            timer.run()

    def add_timer(self, name, interval, method):
        """
        Add a timer instance to our tracked timers. If a timer already exists with this name,
        it will be overwritten.

        :param name: the name for this timer
        :param interval: number of seconds this timer runs for, before calling the function
        :param method: the function to call when this timer expires
        """
        self.timers[name] = Timer(name, interval, method)
        self.logger.info(f'Added new timer {name}.')

    def delete_timer(self, name):
        """
        Delete the associated timer from the list of tracked timers.
        This function should be safe to call, even if the named timer
        does not exist.

        :param name: name of the timer instance to remove.
        """
        if name in self.timers.keys():
            timer = self.timers.pop(name)
            self.logger.info(f'Deleted tracked timer {name}: {timer}')

    def reject_message(self, error_log, delivery_tag):
        """
        Reject a message and send it to the dead-letter exchange.

        :param error_log: The content of the error log message
        :param delivery_tag: The delivery tag which identifies this message
        """
        self.consume_channel.basic_reject(delivery_tag, requeue=False)
        self.logger.error(error_log)