""" Initalises the service class and storage cache. """
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class Pi:
    def __init__(
                self, rabbit, redis, order_freq, order_size, cooldown_time, expiry_time):
        """
        Initialise and configure the instance.

        :param rabbit: connected rabbit instance
        :param redis: connected redis handle
        :param order_freq: how many seconds to wait between publishes
        :param order_size: maximum amount of orders allowed in a set
        :param cooldown_time: seconds the customer must remain in cooldown
        :param expiry_time: how many seconds each start is valid for
        """
        self.rabbit = rabbit
        self.redis = redis

        self.order_freq = order_freq
        self.order_size = order_size
        self.cooldown_time = cooldown_time
        self.expiry_time = expiry_time

        self.logger = logging.getLogger('PI')

    def run(self):
        """ Connect to RabbitMQ and Redis. """
        self.logger.info("Starting up PI")
        self.rabbit.init_consumer()

        # Add a timer to our RabbitMQ consumer. When the timer expires,
        # the method passed into this function will be called.
        self.rabbit.add_timer('pi_manifest', self.order_freq, self.publish_manifest)
        self.rabbit.consume(self.message_callback)

    def stop(self):
        """ Shut down PI. """
        self.logger.info("Stopping PI")
        self.rabbit.stop()

    def message_callback(self, message):
        """ Callback to pass to RabbitMQ consume. """
        headers = message.application_headers
        routing_key = message.delivery_info['routing_key']
        self.logger.info(f"Message received: {routing_key}, {headers}")

    def publish_manifest(self):
        """
        This method exports and publishes the current manifest.
        """
        self.logger.info(f'Publishing manifest')