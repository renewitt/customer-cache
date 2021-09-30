""" Pytest Fixtures and default data. """
import uuid
import time

import faker
import pytest

import mpi.dbapi as dbapi

@pytest.fixture(scope='function')
def conn():
    """
    Initialise the data cache for storing records. The in memory DB persists only as
    long as the connection is open, ensuring each test starts with a clean connection.

    :returns: SQLite3 connection handle
    """
    conn = dbapi.connect()
    dbapi.create_cache(conn)
    yield conn
    conn.close()

def gen_headers():
    """
    This mimics the headers containing customer metadata we would get from RabbitMQ when we
    receive a message.
    """
    fake = faker.Faker()
    headers = {
        "region": fake.city(),
        "description": fake.company(),
        "phone": fake.phone_number(),
        "ip_address": fake.ipv4(),
        "guid": f"{uuid.uuid4()}"
    }
    return headers

def insert_records(conn, num=1):
    """ Insert the provided number of records into the cache. """
    records = []
    for i in range(0, num):
        record = gen_headers()
        record['date_created'] = time.time()
        record['last_active'] = time.time()
        records.append(record)

    with conn:
        c = conn.executemany("""
        INSERT INTO record (
            phone,
            ip_addr,
            region,
            guid,
            date_created,
            last_active,
            description
        ) VALUES (
            :phone,
            :ip_addr,
            :region,
            :guid,
            :date_created,
            :last_active,
            :description
        )""", records)
        assert c.rowcount == num

def count_records(conn):
    """ Count the total records in the cache. """
    with conn:
        c = conn.execute("""
            SELECT COUNT (phone)
            FROM record
        """)
        # This is a bit weird because of the dict cursor
        return c.fetchone()['COUNT (phone)']
