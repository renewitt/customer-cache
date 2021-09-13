import amqp
import faker
import random
import uuid
import time

def generate_customer_data():
    """ Generate customer data to use when publishing. """
    customers = []
    fake = faker.Faker()

    for idx in range(1000):
        customers.append({
            "region": fake.city(),
            "company": fake.company(),
            "phone": fake.phone_number(),
            "ip_address": fake.ipv4()
        })
    return customers

def publish_order(channel, headers):
    """
    Choose a random custom from the generated data and publish
    the details to RabbitMQ.
    """
    key = random.choice(["start", "stop"])
    print(f'Publishing message. Key: {key},  Headers: {headers}')
    msg = amqp.Message(application_headers=headers)
    channel.basic_publish(msg, exchange="pi", routing_key=key)

def publish_start(channel, headers):
    print(f'Resending previous message.  Key: start, Headers: {headers}')
    msg = amqp.Message(application_headers=headers)
    channel.basic_publish(msg, exchange="pi", routing_key="start")

def run():
    """ Run the main loop. """
    customer_data = generate_customer_data()

    with amqp.Connection('localhost:5672') as c:
        ch = c.channel()
        args = {
            "alternate-exchange": "dead-letter"
        }
        ch.exchange_declare(
            "pi",       # exchange name
            "direct",   # exchange type,
            durable=True,
            auto_delete=False,
            arguments=args,
        )

        while True:
            headers = random.choice(customer_data)
            headers["guid"] = f"{uuid.uuid4()}"
            publish_order(ch, headers)

            # We don't get orders that often.
            time.sleep(random.randint(1, 8))

            # randomly publish some already published ones so
            # we can test cooldown works. This generates a lot of
            # re-sends, which is fine, because a lot of them might
            # have previously been stops.
            if '5' in headers["guid"]:
                publish_start(ch, headers)

if __name__ == "__main__":
    run()