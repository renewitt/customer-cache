""" Test the SQLite queries return the expected data. """
import pytest
import sqlite3

from uuid import uuid4

import mpi.dbapi as dbapi
import tests.conftest as helpers

def test_duplicate_customer(conn):
    """
    The customer phone number is the PRIMARY KEY for the cache, so trying
    to insert the same record twice should fail. We use this to maintain
    data integrity in the event we see two start messages for the same
    customer in quick succession.
    """
    with conn as cur:
        dbapi.insert_record(cur, '1234', '127.0.0.1', 'hogwarts', f'{uuid4()}', 'wizard')
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint"):
            dbapi.insert_record(cur, '1234', '127.0.0.1', 'hogwarts', f'{uuid4()}', 'wizard')

def test_delete_record(conn):
    """
    Test deleting records that do and don't exist. The delete record method
    returns the count of modified rows and we use this value to check what
    to do next.
    """
    with conn as cur:
        count = dbapi.delete_record(conn, '7890')
        assert count == 0

        dbapi.insert_record(cur, '1234', '127.0.0.1', 'hogwarts', f'{uuid4()}', 'wizard')
        count = dbapi.delete_record(cur, '1234')
        assert count == 1

def test_get_record(conn):
    """ Test getting records that do and don't exist. """
    with conn as cur:
        dbapi.insert_record(cur, '1234', '127.0.0.1', 'hogwarts', f'{uuid4()}', 'wizard')
        # record is returned as a dictionary with expected keys
        out = dbapi.select_record(cur, '1234')
        assert {'phone', 'ip_addr', 'cooldown_expiry', 'tasked_time', 'region'} <= out.keys()

        # a record that doesn't exist returns None
        out = dbapi.select_record(cur, '7890')
        assert out == None

def test_record_ordering(conn):
    """
    Its important that the records are returned in the correct order, as we may trim
    the manifest if the cache is oversized.
    """
    with conn as cur:
        dbapi.insert_record(cur, '1111', '127.0.0.1', 'hogwarts', f'{uuid4()}', 'wizard')
        dbapi.insert_record(cur, '2222', '127.0.0.1', 'diagon alley', f'{uuid4()}', 'wizard')
        dbapi.insert_record(cur, '3333', '127.0.0.1', 'azkaban', f'{uuid4()}', 'wizard')
        records = dbapi.select_manifest_records(cur, 60)
        assert records[0]['phone'] == '3333'
        assert records[1]['phone'] == '2222'
        assert records[2]['phone'] == '1111'

def test_marking_as_tasked(conn):
    """ Test records being marked as tasked. """
    with conn as cur:
        dbapi.insert_record(cur, '1111', '127.0.0.1', 'hogwarts', f'{uuid4()}', 'wizard')
        dbapi.insert_record(cur, '2222', '192.168.1.1', 'azkaban', f'{uuid4()}', 'wizard')
        assert helpers.count_records(cur) == 2

        # Update only the record 1111 to tasked
        dbapi.update_to_tasked(cur, [{'phone': '1111'}])
        records = cur.execute('SELECT phone, tasked_time FROM record;').fetchall()
        for r in records:
            if r['phone'] == '1111':
                assert r['tasked_time'] is not None
            if r['phone'] == '2222':
                assert r['tasked_time'] is None

def test_delete_expired(conn):
    """
    Test inserting records and running the expiry prune to ensure they are
    correctly removed.
    """
    with conn as cur:
        # allowed expiry time is 0 seconds after last updated, record should be deleted
        dbapi.insert_record(cur, '1234', '127.0.0.1', 'hogwarts', f'{uuid4()}', 'wizard')
        removed_count = dbapi.delete_expired_records(conn, 0)
        assert removed_count == 1
        assert helpers.count_records(cur) == 0

        dbapi.insert_record(cur, '1234', '127.0.0.1', 'hogwarts', f'{uuid4()}', 'wizard')
        # allowed expiry time is 100 seconds after last updated, no records should be deleted
        removed_count = dbapi.delete_expired_records(conn, 3600)
        assert removed_count == 0
        assert helpers.count_records(conn) == 1

