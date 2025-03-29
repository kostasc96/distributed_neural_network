import time
from confluent_kafka import Producer
from confluent_kafka import Consumer, KafkaError, KafkaException
from queue import Empty
from pcomp.fast_queue import FastQueue
import threading

class KafkaProducerHandler:
    def __init__(self, server, topic):
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': server,
            'acks': 'all',
            'retries': 5,
            'linger.ms': 100,
            'batch.size': 131072,
            'compression.type': 'lz4',
            'socket.send.buffer.bytes': 1048576
        })

    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')

    def send(self, message, key=None):
        value = message.encode('utf-8')
        key_encoded = key.encode('utf-8') if key else None
        self.producer.poll(0)
        try:
            self.producer.produce(self.topic, value=value, key=key_encoded)
        except BufferError:
            self.producer.poll(0.1)
            self.producer.produce(self.topic, value=value, key=key_encoded)

    def close(self):
        self.producer.flush()


class KafkaConsumerHandler:
    def __init__(self, topic, servers, group_id='default_group'):
        self.topic = topic
        self.consumer = Consumer({
            'bootstrap.servers': servers,
            'group.id': group_id,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000,
            'fetch.min.bytes': 1048576,
            'fetch.wait.max.ms': 100,
            'max.partition.fetch.bytes': 10485760,
            'socket.receive.buffer.bytes': 1048576
        })
        self.consumer.subscribe([topic])
        self.msg_queue = FastQueue(maxsize=10000)
        self.running = True
        self.poll_thread = threading.Thread(target=self._poll_messages, daemon=True)
        self.poll_thread.start()

    def _poll_messages(self, poll_timeout=0.5):
        while self.running:
            msg = self.consumer.poll(poll_timeout)
            if msg is None:
                continue
            if msg.error():
                continue
            else:
                key = msg.key().decode("utf-8") if msg.key() else None
                value = msg.value().decode("utf-8")
                self.msg_queue.put((key, value))

    def consume(self, break_after=20, key_filter=None):
        last_message_time = time.time()
        while True:
            try:
                message = self.msg_queue.get(timeout=0.5)
            except Empty:  # explicitly catch Empty
                if time.time() - last_message_time >= break_after:
                    break
                continue

            last_message_time = time.time()
            key, value = message
            if key_filter is None or key == key_filter:
                yield key, value

    def close(self):
        self.running = False
        self.poll_thread.join()
        if self.consumer is not None:
            self.consumer.close()
            self.consumer = None