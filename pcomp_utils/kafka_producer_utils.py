from kafka import KafkaProducer
from json import dumps

class KafkaProducerHandler:
    def __init__(self, servers, retries, timeout, value_serializer=lambda v: dumps(v).encode('utf-8')):
        self.producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=value_serializer,
            retries=retries,
            request_timeout_ms=timeout
        )

    def send_message(self, topic, value, key=None, partition=None):
        try:
            future = self.producer.send(
                topic=topic,
                key=key.encode() if isinstance(key, str) else key,
                value=value,
                partition=partition
            )
            result = future.get(timeout=10)  # Ensures message was sent
            print(f"Message sent to {result.topic} partition {result.partition} with offset {result.offset}")
        except Exception as e:
            print(f"Error sending message to topic {topic}: {e}")

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.close()
