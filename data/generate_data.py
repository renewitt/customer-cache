#!/usr/bin/env python
import amqp
import faker
import random
import uuid
import time

START = 'start'
STOP = 'stop'

def generate_customer_data():
    """ Generate customer data to use when publishing. """
    customers = []
    fake = faker.Faker()

    for idx in range(1000):
        customers.append({
            "region": fake.city(),
            "description": fake.company(),
            "phone": fake.phone_number(),
            "ip_addr": fake.ipv4()
        })
    return customers

def publish(channel, headers, key):
    print(f'{key}: {headers}')
    msg = amqp.Message(application_headers=headers)
    channel.basic_publish(msg, exchange="mpi", routing_key=key)

def run():
    """ Run the main loop. """
    customer_data = generate_customer_data()
    started = []

    with amqp.Connection('rabbitmq:5672') as c:
        ch = c.channel()
        args = {
            "alternate-exchange": "dead-letter"
        }
        ch.exchange_declare(
            "mpi",       # exchange name
            "direct",    # exchange type,
            durable=True,
            auto_delete=False,
            arguments=args,
        )

        while True:
            headers = random.choice(customer_data)
            headers["guid"] = f"{uuid.uuid4()}"

            publish(ch, headers, START)
            # append these to our list of started records
            started.append(headers)

            # We don't get orders that often.
            time.sleep(random.randint(1, 20))

            stop = random.choice([True, False])
            if stop:
                # randomly choose which stop to publish
                idx = random.randrange(0, len(started))
                customer = started.pop(idx)
                publish(ch, customer, STOP)

if __name__ == "__main__":
    run()