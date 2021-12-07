#
#
#
import base64
import io
import avro.schema
import fastavro
from confluent_kafka import Consumer
from confluent_kafka.avro.serializer import SerializerError
import json


class Transaction(object):
    """
    Transaction record
    Args:
        transaction_id (int): Transaction's name
        transaction_card_type (str): Transaction's card_type
        transaction_amount (str): Transaction's amount
        transaction_datetime (str): Transaction's datetime
    """
    def __init__(self, transaction_id=None, transaction_card_type=None, transaction_amount=None, transaction_datetime=None):
        self.transaction_id = transaction_id
        self.transaction_card_type = transaction_card_type
        self.transaction_amount = transaction_amount
        self.transaction_datetime = transaction_datetime


def dict_to_transaction(obj, ctx):
    """
    Converts object literal(dict) to a User instance.
    Args:
        obj (dict): Object literal(dict)
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    """
    if obj is None:
        return None

    return Transaction(transaction_id=obj['transaction_id'],
                       transaction_card_type=obj['transaction_card_type'],
                       transaction_amount=obj['transaction_amount'],
                       transaction_datetime=obj['transaction_datetime'])

def dict_to_key(obj, ctx):
    """
    Converts object literal(dict) to a User instance.
    Args:
        obj (dict): Object literal(dict)
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    """
    if obj is None:
        return None

    return {"name": obj['name']}


def fastavro_decode(raw_bytes):
    bytes_reader = io.BytesIO(raw_bytes)
    # bytes_reader.seek(5)
    event = {}
    for record in fastavro.reader(bytes_reader):
        event = record
        #print("{}".format(record['id']))
    return event

# Report malformed record, discard results, continue polling
consumer = Consumer({
    'bootstrap.servers': 'thinkpad:9092',
    'group.id': 'stream-avro-cgroup',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['input-avro-topic'])

while True:

    try:
        msg = consumer.poll(10)

    except SerializerError as e:
        print("Message deserialization failed for {}: {}".format(msg, e))
        break

    except Exception as e:
        print("Exception while trying to poll messages - {e}".format(e))

    else:

        if msg is None:
            continue

        if msg:
            raw_bytes = msg.value()
            t_dict = fastavro_decode(raw_bytes)
            print("Successfully poll a record from kafka topic: {}, partition: {}, offset: {}".format(msg.topic(), msg.partition(), msg.offset()))
            print("Message key : {} payload : {}".format(msg.key(), json.dumps(t_dict)))
            consumer.commit()
        else:
            print("No new messages at this point. Try again later.")

consumer.close()

