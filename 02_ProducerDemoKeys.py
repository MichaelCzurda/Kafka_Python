from confluent_kafka import Producer


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {0}: {1}"
              .format(msg.value(), err.str()))
    else:
        print("Message produced: {0}".format(msg.value()))
        print("message.topic={}".format(msg.topic()))
        print("message.timestamp={}".format(msg.timestamp()))
        print("message.key={}".format(msg.key()))
        print("message.partition={}".format(msg.partition()))
        print("message.offset={}".format(msg.offset()))
        print()


p = Producer({'bootstrap.servers': '127.0.0.1:9092'})

try:
    for val in range(0, 10):
        topic = "first_topic"
        value = "hello from python #{}".format(val)
        key = "key_{}".format(val)
        p.produce(topic=topic, key=key, value=value, callback=acked)
        p.poll(0.5)

        # Every Key goes to some partition if you rerun the code
        # key_0  part 2
        # key_1  part 0
        # key_2  part 1
        # key_3  part 2
        # key_4  part 1
        # key_5  part 2
        # key_6  part 0
        # key_7  part 0
        # key_8  part 1
        # key_9  part 0

except KeyboardInterrupt:
    pass

p.produce('first_topic', key=None, value='first from python')
p.flush(10)
