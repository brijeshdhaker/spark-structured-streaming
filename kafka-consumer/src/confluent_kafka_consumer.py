from confluent_kafka import Consumer


c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'spark-stream-cgroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['input-topic'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: Key :{} Value :{}'.format(msg.key(), msg.value().decode('utf-8')))

c.close()