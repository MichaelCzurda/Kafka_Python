from confluent_kafka import Consumer, KafkaError

settings = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'my-python_3-application',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'earliest'}
        }

topic = "first_topic";

c = Consumer(settings)

c.subscribe([topic])

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            print('Key: {0}, Received message: {1}, Offset: {2}'.format(msg.key(), msg.value(), msg.offset()))
            print()
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            print('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass

finally:
    c.close()