from confluent_kafka import Producer
from datetime import datetime
import json
import time
import random

p = Producer({'bootstrap.servers': 'localhost:9092'})

transaction_card_type_list = ["Visa", "MasterCard", "Maestro"]

message = None
some_data_source=[]
for i in range(10):
    i = i + 1
    message = {}
    print("Sending message to Kafka topic: " + str(i))
    event_datetime = datetime.now()
    message["transaction_id"] = str(i)
    message["transaction_card_type"] = random.choice(transaction_card_type_list)
    message["transaction_amount"] = round(random.uniform(5.5, 555.5), 2)
    message["transaction_datetime"] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")

    print("Message to be sent: ", message)
    some_data_source.append(message)


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

for data in some_data_source:
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    event_datetime = datetime.now()

    p.produce('input-topic', json.dumps(data), callback=delivery_report)
    # p.produce('input-topic', key="", value=data, on_delivery=delivery_report)
    # p.poll() serves delivery reports (on_delivery)
    # from previous produce() calls.
    p.poll(0)
    # time.sleep(1)

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()