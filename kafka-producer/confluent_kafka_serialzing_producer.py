#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# This is a simple example of the SerializingProducer using Avro.
#
import argparse
import random
from uuid import uuid4

from six.moves import input

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from datetime import datetime

from utils.load_avro_schema_from_file import load_avro_schema_as_str
from utils.parse_command_line_args import parse_command_line_args

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


def trans_to_dict(transaction, ctx):
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


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """
    if err is not None:
        print("Delivery failed for Transaction record {}: {}".format(msg.key(), err))
        return
    print('Transaction record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(args):
    topic = 'input-avro-topic'

    key_schema, value_schema = load_avro_schema_as_str("transaction_record.avsc")
    schema_registry_conf = {'url': 'http://localhost:8081'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    value_avro_serializer = AvroSerializer(value_schema,
                                           schema_registry_client,
                                           trans_to_dict)

    producer_conf = {'bootstrap.servers': 'localhost:9092',
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': value_avro_serializer}

    producer = SerializingProducer(producer_conf)

    print("Producing transaction records to topic {} ".format(topic))
    transaction_card_type_list = ["Visa", "MasterCard", "Maestro"]

    while True:
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        try:

            id = random.randint(1001, 2000)
            print("Sending message to Kafka topic: " + str(id))
            event_datetime = datetime.now()
            transaction = Transaction(transaction_id=id,
                                             transaction_card_type=random.choice(transaction_card_type_list),
                                             transaction_amount=round(random.uniform(5.5, 555.5), 2),
                                             transaction_datetime=event_datetime.strftime("%Y-%m-%d %H:%M:%S"))

            producer.produce(topic=topic, key=str(uuid4()), value=transaction, on_delivery=delivery_report)

        except ValueError:
            print("Invalid input, discarding record...")
            continue

print("\nFlushing records...")
producer.flush()


if __name__ == '__main__':
    """
    parser = argparse.ArgumentParser(description="SerializingProducer Example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")
    """
    # args = parse_command_line_args()
    main({"topic": "input-avro-topic"})
