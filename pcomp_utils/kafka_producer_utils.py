from kafka import KafkaProducer
from json import dumps

class KafkaProducerHandler:
    def __init__(self, server):
        self.producer = KafkaProducer(
            bootstrap_servers=server,
            value_serializer=lambda v: dumps(v).encode('utf-8'),
            acks='all',
            retries=5,
            linger_ms=50,      # Delay to allow batching
            batch_size=16384   # Increase batch size as needed
        )

    def send(self, topic, message, partition=None):
        if partition is not None:
            self.producer.send(topic, value=message, partition=partition)
        else:
            self.producer.send(topic, value=message)
        self.producer.flush()

    def close(self):
        self.producer.flush()
        self.producer.close()
