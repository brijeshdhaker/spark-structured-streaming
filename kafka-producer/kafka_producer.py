from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random

KAFKA_INPUT_TOPIC_NAME = "input-topic"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))

    transaction_card_type_list = ["Visa", "MasterCard", "Maestro"]

    message = None
    while True:

        # Serve on_delivery callbacks from previous calls to produce()
        # kafka_producer_obj.poll(0.0)
        try:

            message = {}
            id = random.randint(1001, 2000)
            print("Sending message to Kafka topic: " + str(id))
            event_datetime = datetime.now()
            message["transaction_id"] = str(id)
            message["transaction_card_type"] = random.choice(transaction_card_type_list)
            message["transaction_amount"] = round(random.uniform(5.5, 555.5), 2)
            message["transaction_datetime"] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")
            #
            kafka_producer_obj.send(KAFKA_INPUT_TOPIC_NAME, message)

            # time.sleep(1)

        except ValueError:
            print("Invalid input, discarding record...")
            continue
