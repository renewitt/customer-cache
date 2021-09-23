""" SQLite3 API for PI. """
import time
import sqlite3

MEMORY = ':memory:'

def dict_cursor(conn, row):
    """
    This method is attached the connection so queries to the cache return us a dictionary. The
    names of the columns become the dict keys. The SQLite3 default is to return rows as tuples,
    which is really annoying when named fields are a thing.

    There is also a more efficient way to do this, which returns views of rows using sqlite3.Row,
    and allows you to access the Row object with dict keys. It does require an extra data transform
    using dict() if you want to view the actual data. Using this other method is advised for maximum
    performance or very large data sets.

    :param conn: SQLite3 connection handle
    :param row: SQLite Row object.

    :returns: a dictionary view of this row, with the column names as keys
    """
    d = {}
    for idx, col in enumerate(conn.description):
        d[col[0]] = row[idx]
    return d

def connect():
    """
    Connect to SQLite3 in memory.

    :returns: SQLite3 connection handle
    """
    conn = sqlite3.connect(MEMORY)
    # set up the dictionary cursor
    conn.row_factory = dict_cursor
    return conn

def create_cache(conn):
    """
    Create the table which will be used to store the customer records. SQLite
    stores booleans as integers, and supports UNIX timestamps.

    :param conn: SQLite connection handle
    """
    conn.execute("""
    CREATE TABLE record (
        phone TEXT PRIMARY KEY,
        ip_addr TEXT NOT NULL,
        region TEXT NOT NULL,
        guid TEXT NOT NULL,
        description TEXT NOT NULL,
        date_created INTEGER NOT NULL,
        last_active INTEGER NOT NULL,
        cooldown_expiry INTEGER,
        tasked_time INTEGER
    )""")

def select_record(conn, phone):
    """
    Get the record associated with a phone number.

    :param conn: SQLite connection handle
    :param phone: the customers phone number

    :returns: the associated record, or None
    """
    c = conn.execute("""
        SELECT
            phone,
            ip_addr,
            region,
            guid,
            description,
            last_active,
            date_created,
            cooldown_expiry,
            tasked_time
        FROM record
        WHERE phone=:phone
    """, {'phone': phone})
    return c.fetchone()

def insert_record(conn, phone, ip_addr, region, guid, desc):
    """
    Insert a record into the cache. Customers are uniquely identified by their
    phone number.

    :param conn: SQLite connection handle
    :param phone: Customer's phone number.
    :param ip_addr: Customer's IP address
    :param region: Region this customer is in
    :param guid: the unique identifier for this customer session (BH)
    :param desc: Customer description

    :returns: the number of modified rows
    """
    c = conn.execute("""
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
    )""", {
        'phone': phone, 'ip_addr': ip_addr, 'region': region, 'guid': guid,
        'date_created': time.time(), 'last_active': time.time(), 'description': desc
    })
    return c.rowcount

def delete_finished_cooldown(conn):
    """
    Delete records who have completed their cooldown time.

    :param conn: SQLite connection handle
    :returns: the amount of rows which were deleted
    """
    c = conn.execute("""
        DELETE
        FROM record
        WHERE cooldown_expiry <= :now
    """, {'now': time.time()})
    return c.rowcount

def delete_expired_records(conn, active_time):
    """
    Delete records which are past their expiry time, and are not in cooldown.

    NOTE: We should probably add some safety window in here. If they have under
    5 seconds left, they should probably be regarded as expired, as by the time
    the manifest is actually sent, that time is likely to have elapsed.

    :param conn: SQLite connection handle
    :param active_time: the amount of time records are active after they were last updated
    :returns: the amount of rows which were deleted
    """
    c = conn.execute("""
        DELETE
        FROM record
        WHERE cooldown_expiry IS NULL
        AND (last_active + :active_time) <= :now
    """, {'active_time': active_time, 'now': time.time()})
    return c.rowcount

def update_active_time(conn, phone):
    """
    Update the active time on a record. This occurs when a customer already in the cache
    gets a new start message.
    """
    conn.execute("""
        UPDATE record
        SET last_active=:last_active
        WHERE phone=:phone
    """, {'phone': phone, 'last_active': time.time()})

def delete_record(conn, phone):
    """
    If we receive a stop message for a customer, they should be deleted from the cache.
    If they are in cooldown, they are ignored as deleting them will circumvent the cooldown
    logic.

    :param conn: SQLite connection handle
    :param phone: the customers phone number

    :returns: the count of the rows modified
    """
    c = conn.execute("""
        DELETE FROM
            record
        WHERE phone=:phone
        AND cooldown_expiry IS NULL
    """, {'phone': phone})
    return c.rowcount

def select_manifest_records(conn, active_time):
    """
    Select all the records which are eligible to be included in a manifest.

    :param conn: SQLite connection handle
    :param active_time: seconds records are considered active for, after they were last updated

    :returns: a list of dictionaries, or an empty list
    """
    c = conn.execute("""
        SELECT
            phone,
            ip_addr,
            region,
            guid,
            description,
            last_active,
            date_created,
            cooldown_expiry,
            tasked_time
        FROM record
        WHERE cooldown_expiry IS NULL
        AND (last_active + :active_time) >= :now
        ORDER BY date_created DESC
    """, {'active_time': active_time, 'now': time.time()})
    return c.fetchall()

def update_to_tasked(conn, records):
    """
    Mark all these records as tasked so we can keep track of which records have been published.

    :param conn: SQLite connection handle
    :param records: a list of dictionaries with customer datas
    """
    conn.executemany("""
        UPDATE record
        SET tasked_time = strftime('%s','now')
        WHERE phone = :phone
    """, records)

def update_to_cooldown(conn, cooldown_time):
    """
    Update all the records which have a tasked_time, and send them to cooldown. Cooldown means they
    cannot be included as part of a manifest, and expires when the cooldown time has elapsed, or they
    are released.

    :param conn: SQLite connection handle
    :param cooldown_time: seconds customer records must remain in cooldown
    """
    conn.execute("""
        UPDATE record
        SET cooldown_expiry = :cooldown_expiry
        WHERE tasked_time IS NOT NULL
    """, {'cooldown_expiry': time.time() + cooldown_time})

def update_free_cooldown(conn, active_time):
    """
    Free customers from cooldown if they have a `cooldown_expiry` set and their active
    window (`last_active` + active_time) is later than now.

    :param conn: SQLite connection handle
    :param active_time: seconds records are considered active for, after they were last updated
    """
    conn.execute("""
        UPDATE record
        SET cooldown_expiry = NULL
        WHERE cooldown_expiry IS NOT NULL
        AND last_active + :active_time > :now
    """, {'active_time': active_time, 'now': time.time()})