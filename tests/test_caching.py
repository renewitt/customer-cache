"""
These are more faux integration tests, than unit tests, but they are fairly contained
because the cache is just SQLite in memory. They mainly exist to test areas of the code
which rely on multiple queries being executed in sequence.
"""
import logging

from uuid import uuid4

import mpi.service as svc
import mpi.dbapi as dbapi

import tests.conftest as helpers

def init_mpi(mocker):
    rabbit = mocker.Mock()
    refresh_time = 20
    manifest_size = 5
    cooldown_time = 300
    active_time = 60
    exchange = 'exchange'
    key = 'key'
    mpi = svc.Pi(
        rabbit, refresh_time, manifest_size, cooldown_time, active_time, exchange, key)
    return mpi

def test_adding_and_removing_records(conn, mocker, caplog):
    """ Send a start message, then another one for the same customer, then a stop. """
    mpi = init_mpi(mocker)
    mpi.conn = conn

    message = mocker.Mock()
    message.application_headers = helpers.gen_headers()

    # run the callback with our first start
    message.delivery_info = {'routing_key': 'start'}
    mpi.message_callback(message)
    assert helpers.count_records(conn) == 1

    # send an update for the same customer
    message.delivery_info = {'routing_key': 'start'}
    mpi.message_callback(message)
    # record should have been updated
    assert helpers.count_records(conn) == 1

    # record has not been published so should be cleanly pruned
    message.delivery_info = {'routing_key': 'stop'}
    mpi.message_callback(message)
    assert helpers.count_records(conn) == 0

    # log is generated if we try to stop the same customer, as they
    # are no longer in the cache
    message.delivery_info = {'routing_key': 'stop'}
    with caplog.at_level(logging.WARNING):
        mpi.message_callback(message)
        assert 'not in cache' in caplog.text
    assert helpers.count_records(conn) == 0

def test_cache_prune(conn, mocker):
    """
    Test expired records and records which have completed cooldown are the only ones
    removed when the cache is pruned.
    """
    mpi = init_mpi(mocker)
    mpi.conn = conn

    with conn as cur:
        dbapi.insert_record(cur, '1111', '127.0.0.1', 'hogwarts', f'{uuid4()}', 'wizard')
        dbapi.insert_record(cur, '2222', '8.8.8.8', 'diagon alley', f'{uuid4()}', 'witch')
    assert helpers.count_records(conn) == 2
    # force a prune. Nothing should happen because we are well within the expiry time
    mpi._prune_cache()
    assert helpers.count_records(conn) == 2
