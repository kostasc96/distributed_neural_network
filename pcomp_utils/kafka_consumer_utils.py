from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from json import loads

class KafkaConsumerHandler:
    def __init__(self, topic, servers, partition=None):
        self.consumer = KafkaConsumer(
            bootstrap_servers=servers,
            value_deserializer=lambda m: loads(m.decode('utf-8')),
            #group_id='dummy_group',
            auto_offset_reset='latest',
            enable_auto_commit=False,
            consumer_timeout_ms=60000
        )
        if partition is not None:
            tp = TopicPartition(topic, partition)
            self.consumer.assign([tp])
        else:
            self.consumer.subscribe([topic])

    def consume(self):
        for message in self.consumer:
            yield message
    
    def poll(self, seconds=0.5):
        return self.consumer.poll(seconds)
    
    def commit(self, topic, partition, offset):
        tp = TopicPartition(topic, partition)
        self.consumer.commit({tp: OffsetAndMetadata(offset, None)})

    def close(self):
        self.consumer.close()