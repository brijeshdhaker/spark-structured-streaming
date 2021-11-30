from confluent_kafka.avro import AvroConsumer
from confluent_kafka import DeserializingConsumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
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
    # Transaction._address must not be serialized; omit from dict
    return dict(transaction_id=transaction.transaction_id,
                transaction_card_type=transaction.transaction_card_type,
                transaction_amount=transaction.transaction_amount,
                transaction_datetime=transaction.transaction_datetime)

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



#
#
#
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


"""
c = AvroConsumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'stream-avro-cgroup',
    'auto.offset.reset': 'earliest',
    'schema.registry.url': 'http://127.0.0.1:8081'
})
"""

#
#
#
sr_conf = {'url': 'http://127.0.0.1:8081'}
schema_registry_client = SchemaRegistryClient(sr_conf)

key_avro_deserializer = AvroDeserializer(key_schema_str, schema_registry_client, dict_to_key)
value_avro_deserializer = AvroDeserializer(value_schema_str, schema_registry_client, dict_to_transaction)

string_deserializer = StringDeserializer('utf_8')

consumer_conf = {'bootstrap.servers': 'localhost:9092',
                 'group.id': 'stream-avro-cgroup',
                 'auto.offset.reset': 'earliest',
                 'key.deserializer': string_deserializer,
                 'value.deserializer': value_avro_deserializer}

consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe(['input-avro-topic'])

# c.subscribe(['input-avro-topic'])

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

        t_obj = msg.value()
        if t_obj is not None:
            t_dict = trans_to_dict(t_obj)
            print("Successfully poll a record from kafka topic: {}, partition: {}, offset: {}".format(msg.topic(), msg.partition(), msg.offset()))
            print("Message key : {} payload : {}".format(msg.key(), json.dumps(t_dict)))
            consumer.commit()
        else:
            print("No new messages at this point. Try again later.")

#
consumer.close()
