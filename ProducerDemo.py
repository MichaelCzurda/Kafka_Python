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


p = Producer({'bootstrap.servers': '127.0.0.1:9092'})

try:
    for val in range(1, 1000):
        p.produce('first_topic', 'python_value #{0}'
                  .format(val), callback=acked)
        p.poll(0.5)

except KeyboardInterrupt:
    pass

p.produce('first_topic', key=None, value='first from python')
p.flush(10)
