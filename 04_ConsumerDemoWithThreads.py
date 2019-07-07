from confluent_kafka import Consumer, KafkaError
import threading
import logging


class ConsumerRunnable:
    def __init__(self, bootstrapServers, groupid, topic, client_id):
        self.settings = {
            'bootstrap.servers': bootstrapServers,
            'group.id': groupid,
            'client.id': client_id,
            'enable.auto.commit': True,
            'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'earliest'}
        }
        self.logger = logging.getLogger('ConsumerRunnable')
        self.topic = topic
        self.client_id = client_id
        self.c = Consumer(self.settings)
        self.c.subscribe([topic])

    def run(self):
        print('hello')
        try:
            while True:
                msg = self.c.poll(0.1)
                print(msg)
                if msg is None:
                    continue
                elif not msg.error():
                    self.logger.info('Consumer: {}'.format(self.client_id))
                    self.logger.info('Key: {0}, Received message: {1}'.format(msg.key(), msg.value()))
                    self.logger.info('Partition: {0}, Offset: {1}'.format(msg.partition, msg.offset()))
                elif msg.error().code() == KafkaError._PARTITION_EOF:
                    self.logger.info('End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
                else:
                    self.logger.info('Error occured: {0}'.format(msg.error().str()))

        except KeyboardInterrupt:
            pass

        finally:
            self.c.close()


if __name__ == '__main__':
    topic = "first_topic"
    bootstrapServers = '127.0.0.1:9092'
    groupid = 'my-python-thread_4-application'

    logger = logging.getLogger(__name__)

    consumerThreads = list()
    for index in range(3):
        logger.info("Creating the consumer thread for consumer #{}".format(index))
        client_id = 'Consumer_{}'.format(index)
        consumer = ConsumerRunnable(bootstrapServers, groupid, topic, client_id)
        x = threading.Thread(target=consumer.run)
        consumerThreads.append(x)
        x.start()

    for index, thread in enumerate(consumerThreads):
        logging.info("Main    : before joining thread %d.", index)
        thread.join()
        logging.info("Main    : thread %d done", index)

