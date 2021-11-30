#
#
#
from uuid import uuid4
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from datetime import datetime
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

# schema_registry_conf = {'url': 'http://localhost:8081'}
# schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# value_avro_serializer = AvroSerializer(value_schema_str, schema_registry_client, trans_to_dict)
# producer_conf = {'bootstrap.servers': 'localhost:9092', 'key.serializer': StringSerializer('utf_8'), 'value.serializer': value_avro_serializer}
# avroProducer = SerializingProducer(producer_conf)

transaction_card_type_list = ["Visa", "MasterCard", "Maestro"]
message = None
some_data_source=[]
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


avroProducer = AvroProducer({
    'bootstrap.servers': 'localhost:9092',
    'on_delivery': delivery_report,
    'schema.registry.url': 'http://localhost:8081'
}, default_key_schema=key_schema, default_value_schema=value_schema)

for data in some_data_source:
    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.

    event_datetime = datetime.now()
    value = trans_to_dict(data)
    key = {"name": str(uuid4())}
    # Serve on_delivery callbacks from previous calls to produce()
    avroProducer.poll(0.0)
    try:
        #t
        avroProducer.produce(topic='input-avro-topic', key=key, value=value)
        # time.sleep(1)
    except ValueError:
        print("Invalid input, discarding record...")
        continue

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
print("\nFlushing Records...")
avroProducer.flush()
