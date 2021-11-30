from confluent_kafka.avro import AvroConsumer
from confluent_kafka import DeserializingConsumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka import avro

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



value_schema_str = """
{
  "fields": [
    {
      "name": "transaction_id",
      "type": "int"
    },
    {
      "name": "transaction_card_type",
      "type": "string"
    },
    {
      "name": "transaction_amount",
      "type": "float"
    },
    {
      "name": "transaction_datetime",
      "type": "string"
    }
  ],
  "name": "Transaction",
  "namespace": "com.test.avro",
  "type": "record"
}
"""


#
#
#
key_schema_str = """
{
  "fields": [
    {
      "name": "name",
      "type": "string"
    }
  ],
  "name": "key",
  "namespace": "com.test.avro",
  "type": "record"
}
"""

value_schema = avro.loads(value_schema_str)
key_schema = avro.loads(key_schema_str)


consumer = AvroConsumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'stream-avro-cgroup',
    'auto.offset.reset': 'earliest',
    'schema.registry.url': 'http://127.0.0.1:8081'
}, reader_key_schema=key_schema, reader_value_schema=value_schema)

consumer.subscribe(['input-avro-topic'])

# c.subscribe(['input-avro-topic'])

while True:
    try:
        msg = consumer.poll(10)

    except SerializerError as e:
        print("Message deserialization failed for {}: {}".format(msg, e))
        break

    if msg is None:
        continue

    if msg.error():
        print("AvroConsumer error: {}".format(msg.error()))
        continue

    print(msg.value())

consumer.close()
