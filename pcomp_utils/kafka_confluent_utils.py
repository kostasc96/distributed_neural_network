import time
from confluent_kafka import Producer
from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_BEGINNING, OFFSET_END, KafkaException

class KafkaProducerHandler:
    def __init__(self, server):
        self.producer = Producer({
            'bootstrap.servers': server,
            'acks': 'all',
            'retries': 5,
            'linger.ms': 50,
            'batch.size': 65536,
            'compression.type': 'lz4'  
        })
    

    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
    

    def send(self, topic, message, partition=None):
        value = message.encode('utf-8')
        if partition is not None:
            self.producer.produce(topic, value=value, partition=partition)
        else:
            self.producer.produce(topic, value=value)

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
            'max.partition.fetch.bytes': 10485760
        })
        if partition is not None:
            tp = TopicPartition(topic, partition, OFFSET_END)
            self.consumer.assign([tp])
        else:
            self.consumer.subscribe([topic])
    
    def consume(self, poll_timeout=0.5, break_after=20):
        last_message_time = time.time()
        while True:
            msg = self.consumer.poll(poll_timeout)
            if msg is None:
                # Check if 20 seconds have passed since the last message
                if time.time() - last_message_time >= break_after:
                    break
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
            # Reset timer on receiving a message
            last_message_time = time.time()
            yield msg.value().decode("utf-8")
            self.consumer.commit(msg)
    
    def commit(self):
        self.consumer.commit()

    def close(self):
        if self.consumer is not None:
            self.consumer.close()
            self.consumer = None