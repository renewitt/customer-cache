""" Initalises the service class and storage cache. """
import json
import logging
import datetime

import mpi.dbapi as dbapi

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

START = 'start'
STOP = 'stop'
TIMER = 'pi_manifest'

class Pi:
    def __init__(
                self, rabbit, refresh_time, manifest_size, cooldown_time, active_time,
                publish_exchange, publish_key):
        """
        Initialise and configure the instance.

        :param rabbit: connected rabbit instance
        :param refresh_time: how many seconds to wait between publishes
        :param manifest_size: maximum amount of orders allowed in a set
        :param cooldown_time: seconds the customer must remain in cooldown
        :param expiry_time: how many seconds each start is valid for
        :param publish_exchange: exchange for published MPI data
        :param publish_key: routing key used to publish manifests
        """
        self.rabbit = rabbit

        self.refresh_time = refresh_time
        self.manifest_size = manifest_size
        self.cooldown_time = cooldown_time
        self.active_time = active_time

        self.publish_exchange = publish_exchange
        self.publish_key = publish_key

        self.logger = logging.getLogger("PI")
        self.conn = dbapi.connect()

    def run(self):
        """ Connect to RabbitMQ and set timers. """
        self.logger.info("Starting up PI")

        try:
            dbapi.create_cache(self.conn)

            self.rabbit.init_consumer()
            self.rabbit.init_publisher(self.publish_exchange)
            # Add a timer to our RabbitMQ consumer. Whenever the timer expires,
            # the method passed into this function will be called.
            self.rabbit.add_timer(TIMER, self.refresh_time, self.publish_manifest)
            self.rabbit.consume(self.message_callback)
        finally:
            self.stop()

    def stop(self):
        """ Shut down PI. """
        self.logger.info("Stopping PI")
        self.rabbit.delete_timer(TIMER)
        self.rabbit.stop()
        self.conn.close()

    def message_callback(self, message):
        """
        Callback to pass to RabbitMQ consume.

        Any error raised in here that isn't to do with RabbitMQ data should cause the
        application to crash. If something weird is happening with the cache, we should
        err on the side of caution, crash and allow the end-client to fall back to disaster
        recovery instead of attempting to recover and potentially sending bad data. The
        service should be on auto-restart, so the result of the crash is the cache getting
        dumped on crash, service restarting and us starting again.

        NOTE: We might have to think about what that would mean if it happened repeatedly.
        """
        headers = message.application_headers
        routing_key = message.delivery_info['routing_key']
        tag = message.delivery_tag

        # we can only process start or stop beyond this point, so reject other messages
        if routing_key not in [START, STOP]:
            self.rabbit.reject_message(
                f"Message has unexpected routing key '{routing_key}', rejecting message", tag)
            return

        try:
            phone = headers['phone']
            ip_addr = headers['ip_addr']
            # We've been bitten by empty descriptions in the past
            desc = headers['description'] or 'UNKNOWN'
            region = headers['region']
            guid = headers['guid']
        except KeyError:
            # If this message has bad headers, reject it and carry on
            self.rabbit.reject_message(
                f'Message headers were improperly formed {headers}', tag)
            return

        if routing_key == START:
            self.on_start(phone, ip_addr, desc, region, guid)
            self.rabbit.consume_channel.basic_ack(tag)
            return

        if routing_key == STOP:
            self.on_stop(phone)
            self.rabbit.consume_channel.basic_ack(tag)
            return

    def publish_manifest(self):
        """
        This method exports and publishes the current manifest. This function is
        called by a timer instance attached to MRabbit.
        """
        manifest = self.generate_manifest()
        data = json.dumps(manifest, indent=2)
        headers = {
            'source': 'mpi',
            'published_at': datetime.datetime.isoformat(datetime.datetime.now()),
            'records': len(manifest)}
        self.rabbit.publish(self.publish_exchange, headers, data, self.publish_key)
        self.logger.info(f'Published manifest with {len(manifest)} records.')

    def generate_manifest(self):
        """ Generates the manifest to be published. """
        # prune cache by removing records who are past their allowed active time, or who
        # have completed their cooldown
        self._prune_cache()

        # Check whether we need to enforce cooldown due to an oversized cache, or free some
        # customers from cooldown if the cache is undersized.
        self._send_to_cooldown()

        # If we still have too many active records, we trim the list. The records are ordered
        # from most recent date_created, so we select from the beginning of the list to the
        # record max. This is because we assume the newest records are the most valuable, and
        # the old ones may be near to expiry.

        # We use date_created instead of last_active to avoid a situation where a customer
        # we regular receive starts for always appears to be new. Using last_active for
        # cooldown and expiry, and date_created for trimming the manifest, is the most
        # optimised way to keep the manifest as full as possible.
        with self.conn as conn:
            records = dbapi.select_manifest_records(conn, self.active_time)
            if len(records) > self.manifest_size:
                over_count = len(records) - self.manifest_size
                self.logger.warning(
                    f'Cache is still oversized after pruning and enforcing cooldown. '
                    f'Ignoring {over_count} oldest records.')
                # We don't need to do anything with the excluded records, as they will either
                # be expired or sent to cooldown next time this is run
                records = records[:self.manifest_size]

            # mark all these records as being being tasked. If these have been previously tasked,
            # the `tasked_time` will be overwritten.
            dbapi.update_to_tasked(conn, records)
            return records

    def _prune_cache(self):
        """
        We can prune the cache by deleting records which are past the allowed active window,
        or customers who have completed their cooldown.
        """
        with self.conn as conn:
            expired = dbapi.delete_expired_records(conn, self.active_time)
            self.logger.debug(f'Pruned {expired} expired records from the cache.')

            cooldown_finished = dbapi.delete_finished_cooldown(conn)
            self.logger.debug(f'Pruned {cooldown_finished} records who have completed cooldown.')

    def _send_to_cooldown(self):
        """
        By counting the amount of records which are NOT in cooldown and NOT expired, we can
        ascertain the total number of eligible records in the cache. If we are below the
        allowed limit, we can skip cooldown. If we are over the limit, customers who have
        already been tasked are eligible to be put into cooldown.

        This method first prunes an oversized cache by sending all previously tasked customers
        to cooldown, and then attempts to recover if the cache is undersize, by freeing recently
        active customers from cooldown.
        """
        with self.conn as conn:
            eligible_records = dbapi.select_manifest_records(conn, self.active_time)
            if len(eligible_records) > self.manifest_size:
                self.logger.info(f'Cache is oversized. Sending customers to cooldown.')
                # There is potential optimisations to be had here. Currently this could
                # result in 90% of the cache being sent to cooldown. We could cooldown
                # only the amount of records we need to drop below the manifest size,
                # but these rules would be fairly arbitrary, so instead we just force them
                # all into cooldown and release them if we have space AND we have received
                # a start for them within the active window.
                dbapi.update_to_cooldown(conn, self.cooldown_time)

            # We reselect the records again, because if we hit the previous conditional,
            # the eligible records have now changed and we want to run this to re-fill
            # the cache if we were too heavy-handed with cooldown.
            eligible_records = dbapi.select_manifest_records(conn, self.active_time)
            if len(eligible_records) < self.manifest_size:
                # Because we keep track of the `last_active` time for customers in the cache,
                # we can tell which, if any, are currently active but in enforced cooldown.
                # If our cache is below the allowed size, we can free these customers from
                # cooldown in order to fill more slots. We don't really care if we free too
                # many because we might just ignore a few of the oldest ones if we're still
                # over size.
                self.logger.debug(
                    f'Cache undersized. Freeing any recently seen customers from cooldown.')
                dbapi.update_free_cooldown(conn, self.active_time)

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
