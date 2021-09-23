""" Initalises the service class and storage cache. """
import time
import json
import logging

import mpi.dbapi as dbapi

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

START = 'start'
STOP = 'stop'
TIMER = 'pi_manifest'

class CacheError(Exception):
    """ Exception raised by PI when an error occurs when manipulating the cache. """
    pass

class Pi:
    def __init__(
                self, rabbit, refresh_time, manifest_size, cooldown_time, active_time):
        """
        Initialise and configure the instance.

        :param rabbit: connected rabbit instance
        :param refresh_time: how many seconds to wait between publishes
        :param manifest_size: maximum amount of orders allowed in a set
        :param cooldown_time: seconds the customer must remain in cooldown
        :param expiry_time: how many seconds each start is valid for
        """
        self.rabbit = rabbit

        self.refresh_time = refresh_time
        self.manifest_size = manifest_size
        self.cooldown_time = cooldown_time
        self.active_time = active_time

        self.logger = logging.getLogger("PI")
        self.conn = dbapi.connect()

    def run(self):
        """ Connect to RabbitMQ and set timers. """
        self.logger.info("Starting up PI")

        # Create the data cache
        dbapi.create_cache(self.conn)

        self.rabbit.init_consumer()
        # Add a timer to our RabbitMQ consumer. When the timer expires,
        # the method passed into this function will be called.
        self.rabbit.add_timer(TIMER, self.refresh_time, self.publish_manifest)
        self.rabbit.consume(self.message_callback)

    def stop(self):
        """ Shut down PI. """
        self.logger.info("Stopping PI")
        # Clean up the timer we added before closing the RabbitMQ connection
        self.rabbit.delete_timer(TIMER)
        self.rabbit.stop()
        self.conn.close()

    def message_callback(self, message):
        """ Callback to pass to RabbitMQ consume. """
        headers = message.application_headers
        routing_key = message.delivery_info['routing_key']

        # we can only process start or stop beyond this point, so reject other messages
        if routing_key not in [START, STOP]:
            self.rabbit.reject_message(
                f'Message has unexpected routing key {routing_key}, rejecting message',
                message.delivery_tag)

        phone = headers['phone']
        ip_addr = headers['ip_addr']
        desc = headers['description']
        region = headers['region']
        guid = headers['guid']

        if routing_key == START:
            self.on_start(phone, ip_addr, desc, region, guid)
            self.rabbit.consume_channel.basic_ack(message.delivery_tag)
            return

        if routing_key == STOP:
            self.on_stop(phone)
            self.rabbit.consume_channel.basic_ack(message.delivery_tag)
            return

    def publish_manifest(self):
        """
        This method exports and publishes the current manifest. This function is
        called by a timer instance attached to MRabbit.

        TODO: Add an actual publish here instead of writing out to a file.
        """
        manifest = self.generate_manifest()
        self.logger.info(f'Publishing manifest with {len(manifest)} records.')
        with open(f'output/manifest_{time.time()}.json', 'w') as f:
            f.write(json.dumps(manifest, indent=2))

    def generate_manifest(self):
        """ Generates the manifest to be published. """
        # prune cache by removing expired records and sending eligible customers
        # to cooldown
        self._prune_cache()

        # Check how many active records remain after pruning. If we still have too many, we
        # enforce a cooldown time.
        self._send_to_cooldown()

        # If after pruning the cooldown check, we still have too many active records, we trim
        # the list. The records are ordered from most recent date_created, so we select
        # from the beginning of the list to the record max. This is because we assume the newest
        # records are the most valuable, and the old ones may be near to expiry.

        # We use date_created instead of last_active to avoid a situation where a customer
        # we regular receive starts for always appears to be new. Using last_active for
        # cooldown and expiry, and date_created for trimming the manifest, is the most
        # optimised way to keep the manifest as full as possible.
        with self.conn as conn:
            records = dbapi.select_manifest_records(conn, self.active_time)
            if len(records) > self.manifest_size:
                over_count = len(records) - self.manifest_size
                self.logger.warning(
                    f'Cache is still oversized after pruning. Ignoring {over_count} oldest records.')
                # We don't need to do anything with the excluded records. Depending on their `tasked`
                # status, they will either be deleted or sent to cooldown in the next prune.
                records = records[:self.manifest_size]

            # mark all these records as being being tasked
            dbapi.update_to_tasked(conn, records)
            return records

    def _prune_cache(self):
        """
        We can prune the cache by deleting records which are past the allowed active window,
        or customers who have completed their cooldown.
        """
        with self.conn as conn:
            expired = dbapi.delete_expired_records(conn, self.active_time)
            self.logger.info(f'Pruned {expired} expired records from the cache.')

            cooldown_finished = dbapi.delete_finished_cooldown(conn)
            self.logger.info(f'Pruned {cooldown_finished} records who have completed cooldown.')

    def _send_to_cooldown(self):
        """
        By counting the amount of records which are NOT in cooldown and NOT expired, we can
        ascertain the total number of eligible records in the cache. If we are below the
        allowed limit, we can skip cooldown. If we are over the limit, customers who have
        already been tasked are eligible to be put into cooldown.

        TODO: Figure out a solution in cases where we end up with a tiny manifest and loads
        of people in cooldown. If this happens, we should release the cooldown early.
        """
        with self.conn as conn:
            eligible_records = dbapi.select_manifest_records(conn, self.active_time)
            if len(eligible_records) <= self.manifest_size:
                return

            self.logger.info(f'Cache is oversized. Sending customers to cooldown.')
            # TODO: Implement better cooldown. This is not optimised in any way and could
            # result in 90% of the cache being sent to cooldown. A better way to do this
            # would be to ascertain how many less records we need, and send them to cooldown.
            dbapi.update_records_with_cooldown(conn, self.cooldown_time)

    def on_start(self, phone, ip_addr, region, desc, guid):
        """
        Called when a message is received with a `start` routing key.

        :param phone: Customer's phone number.
        :param ip_addr: Customer's IP address
        :param region: Region this customer is in
        :param guid: the unique identifier for this customer session (BH)
        :param desc: Customer description
        """
        with self.conn as conn:
            record = dbapi.select_record(conn, phone)
            # if the customer is not already in the cache, we need to add them
            if record is None:
                self.logger.info(f'Inserted new record for customer phone={phone}')
                dbapi.insert_record(conn, phone, ip_addr, region, guid, desc)
                return

            # if they are, we just want to update their last_active time.
            self.logger.info(f'New start for customer in cache. Updated last_active for phone={phone}')
            dbapi.update_active_time(conn, phone)

    def on_stop(self, phone):
        """
        When we receive a stop message, we need to remove the customer from the cache. Customer
        records will only be deleted if they are not in cooldown. Records in cooldown are never
        included as part of a published manifest.

        :param phone: the phone number which identifies this customer
        """
        with self.conn as conn:
            deleted_records = dbapi.delete_record(conn, phone)
            if deleted_records != 1:
                self.logger.warning(f'Received stop for customer not in cache, phone={phone}')
                return

        self.logger.info(f'Deleted record for customer phone={phone}')
