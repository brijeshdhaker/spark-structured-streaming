#
#
#
import io
import json
import fastavro
from uuid import uuid4
from confluent_kafka import Producer
from datetime import datetime
from utils.load_avro_schema_from_file import load_avro_schema_from_file, load_avro_schema_as_str
import random


class Transaction(object):
    """
    Transaction record
    Args:
        transaction_id (int): Transaction's id
        transaction_card_type (str): Transaction's card_type
        transaction_amount (str): Transaction's amount
        transaction_datetime (str): Transaction's datetime
    """

    def __init__(self, transaction_id, transaction_card_type, transaction_amount, transaction_datetime):
        self.transaction_id = transaction_id
        self.transaction_card_type = transaction_card_type
        self.transaction_amount = transaction_amount
        self.transaction_datetime = transaction_datetime


def trans_to_dict(transaction):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """
    # User._address must not be serialized; omit from dict
    return dict(transaction_id=transaction.transaction_id,
                transaction_card_type=transaction.transaction_card_type,
                transaction_amount=transaction.transaction_amount,
                transaction_datetime=transaction.transaction_datetime)


#
#
#
BASE_DIR = "/home/brijeshdhaker/PycharmProjects/spark-structured-streaming/kafka-producer"
key_schema_str, value_schema_str = load_avro_schema_as_str('transaction_record.avsc')
#
value_schema_dict = json.loads(value_schema_str)


def fastavro_encode(msg_value):
    bytes_writer = io.BytesIO()
    fastavro.writer(bytes_writer, fastavro.parse_schema(value_schema_dict), [msg_value])
    raw_bytes = bytes_writer.getvalue()
    bytes_writer.flush()
    # return base64.b64encode(raw_bytes)
    return raw_bytes


transaction_card_type_list = ["Visa", "MasterCard", "Maestro"]
message = None
some_data_source = []
for i in range(10):
    i = i + 1
    message = {}
    print("Sending message to Kafka topic: " + str(i))
    event_datetime = datetime.now()
    message["transaction_id"] = i
    message["transaction_card_type"] = random.choice(transaction_card_type_list)
    message["transaction_amount"] = round(random.uniform(5.5, 555.5), 2)
    message["transaction_datetime"] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")

    transaction = Transaction(transaction_id=i,
                              transaction_card_type=random.choice(transaction_card_type_list),
                              transaction_amount=round(random.uniform(5.5, 555.5), 2),
                              transaction_datetime=event_datetime.strftime("%Y-%m-%d %H:%M:%S"))

    some_data_source.append(transaction)


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print("Delivery failed for Transaction record {}: {}".format(msg.key(), err))
    else:
        print('Transaction record {} successfully produced to {} [{}] at offset {}'.format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()))


# Create Producer instance
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
}
producer = Producer(producer_conf)

for data in some_data_source:
    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.

    event_datetime = datetime.now()
    key = str(uuid4())
    value = fastavro_encode(trans_to_dict(data))
    # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)
    try:
        # t
        producer.produce(topic='input-avro-topic', key=key, value=value, on_delivery=delivery_report)
        # time.sleep(1)
    except ValueError:
        print("Invalid input, discarding record...")
        continue
    except Exception as e:
        print("Exception while producing record value - {} to topic - input-avro-topic : {}".format(value, e))
        continue
    else:
        print("Successfully producing record value - {} to topic - input-avro-topic ".format(value))

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
print("\nFlushing Records...")
producer.flush()
