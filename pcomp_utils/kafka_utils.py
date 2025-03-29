import time
from confluent_kafka import Producer
from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_BEGINNING, OFFSET_END, KafkaException
from queue import Queue, Empty
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
            'compression.type': 'lz4'  ,
            'socket.send.buffer.bytes': 1048576
        })
    

    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
    

    def send(self, message, partition=None):
        value = message.encode('utf-8')
        # Poll to serve delivery callbacks and free up queue space
        self.producer.poll(0)
        try:
            if partition is not None:
                self.producer.produce(self.topic, value=value, partition=partition)
            else:
                self.producer.produce(self.topic, value=value)
        except BufferError:
            # If the queue is still full, poll briefly to free up space and retry
            self.producer.poll(0.1)
            if partition is not None:
                self.producer.produce(self.topic, value=value, partition=partition)
            else:
                self.producer.produce(self.topic, value=value)

    def close(self):
        self.producer.flush()



class KafkaConsumerHandler:
    def __init__(self, topic, servers, group_id='default_group', partition=None):
        self.topic = topic
        self.consumer = Consumer({
            'bootstrap.servers': servers,
            'group.id': group_id,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False,
            'fetch.min.bytes': 1048576,
            'fetch.wait.max.ms': 100,
            'max.partition.fetch.bytes': 10485760,
            'socket.receive.buffer.bytes': 1048576
        })
        if partition is not None:
            tp = TopicPartition(topic, partition, OFFSET_END)
            self.consumer.assign([tp])
        else:
            self.consumer.subscribe([topic])
        self.msg_queue = FastQueue(maxsize=10000)
        self.running = True
        self.poll_thread = threading.Thread(target=self._poll_messages, daemon=True)
        self.poll_thread.start()
    
    def _poll_messages(self, poll_timeout=0.5):
        messages_since_commit = 0
        commit_interval = 100

        while self.running:
            msg = self.consumer.poll(poll_timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError.OFFSET_OUT_OF_RANGE:
                    metadata = self.consumer.list_topics(self.topic)
                    partitions = [p.id for p in metadata.topics[self.topic].partitions.values()]
                    topic_partitions = [TopicPartition(self.topic, partition) for partition in partitions]
                    self.consumer.assign(topic_partitions)
                    self.consumer.seek_to_end()
                    end_offsets = self.consumer.position(topic_partitions)
                    self.consumer.commit(offsets=end_offsets, asynchronous=False)
                else:
                    raise KafkaException(msg.error())
            else:
                message_str = msg.value().decode("utf-8")
                self.msg_queue.put(message_str)
                messages_since_commit += 1

                if messages_since_commit >= commit_interval:
                    self.consumer.commit(asynchronous=True)
                    messages_since_commit = 0

    def consume(self, break_after=20):
        last_message_time = time.time()
        while True:
            try:
                message = self.msg_queue.get(timeout=0.5)
                last_message_time = time.time()
                yield message
            except Empty:
                if time.time() - last_message_time >= break_after:
                    break
    
    def commit(self):
        self.consumer.commit()

    def close(self):
        self.running = False
        self.poll_thread.join()
        if self.consumer is not None:
            self.consumer.close()
            self.consumer = None