import time
from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition
from queue import Empty
from pcomp.fast_queue import FastQueue
import threading

class KafkaProducerHandler:
    def __init__(self, server, topic):
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': server,
            'linger.ms': 100,
            'batch.size': 131072,
            'compression.type': 'lz4'  ,
            'socket.send.buffer.bytes': 1048576,
            'enable.idempotence': True
        })
    

    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
    

    def send(self, message):
        value = message.encode('utf-8')
        self.producer.poll(0)
        try:
            self.producer.produce(self.topic, value=value)
        except BufferError:
            # If the queue is still full, poll briefly to free up space and retry
            self.producer.poll(0.1)
            self.producer.produce(self.topic, value=value)

    def send_neuron(self, message):
        value = message
        self.producer.poll(0)
        try:
            self.producer.produce(self.topic, value=value)
        except BufferError:
            # If the queue is still full, poll briefly to free up space and retry
            self.producer.poll(0.1)
            self.producer.produce(self.topic, value=value)

    def close(self):
        self.producer.flush()



class KafkaConsumerHandler:
    def __init__(self, topic, servers, group_id='default_group'):
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
            if msg.error() and msg.error().code() == KafkaError.OFFSET_OUT_OF_RANGE:
                print("Offset out of range, resetting...")
                # Get currently assigned partitions
                partitions = self.consumer.assignment()
                if partitions:
                    new_assignments = []
                    for p in partitions:
                        low, high = self.consumer.get_watermark_offsets(p)
                        # Set the partition's offset to the high watermark
                        new_assignments.append(TopicPartition(p.topic, p.partition, high))
                    # Reassign partitions with new valid offsets
                    self.consumer.assign(new_assignments)
                    # Optional: wait briefly to allow state update
                    time.sleep(0.1)
                else:
                    print("No partitions assigned; cannot reset offset.")
                continue  # Skip processing this message
            else:
                self.msg_queue.put(msg.value().decode("utf-8"))
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


class KafkaConsumerHandlerNeuron:
    def __init__(self, topic, servers, group_id='default_group'):
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
            if msg.error() and msg.error().code() == KafkaError.OFFSET_OUT_OF_RANGE:
                print("Offset out of range, resetting...")
                # Get currently assigned partitions
                partitions = self.consumer.assignment()
                if partitions:
                    new_assignments = []
                    for p in partitions:
                        low, high = self.consumer.get_watermark_offsets(p)
                        # Set the partition's offset to the high watermark
                        new_assignments.append(TopicPartition(p.topic, p.partition, high))
                    # Reassign partitions with new valid offsets
                    self.consumer.assign(new_assignments)
                    # Optional: wait briefly to allow state update
                    time.sleep(0.1)
                else:
                    print("No partitions assigned; cannot reset offset.")
                continue  # Skip processing this message
            else:
                self.msg_queue.put(msg)
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