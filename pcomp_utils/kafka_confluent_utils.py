from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition, OFFSET_BEGINNING
import json
import time


class KafkaProducerHandler:
    def __init__(self, server):
        self.producer = Producer({
            'bootstrap.servers': server,
            'acks': 'all',
            'retries': 5,
            'linger.ms': 50,
            'batch.size': 16384
        })

    def send(self, topic, message, partition=None):
        value = json.dumps(message).encode('utf-8')
        if partition is not None:
            self.producer.produce(topic, value=value, partition=partition)
        else:
            self.producer.produce(topic, value=value)

    def close(self):
        self.producer.flush()


class KafkaConsumerHandler:
    def __init__(self, topic, servers, group_id='default_group', partition=None):
        self.consumer = Consumer({
            'bootstrap.servers': servers,
            'group.id': group_id,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True
        })
        if partition is not None:
            tp = TopicPartition(topic, partition, OFFSET_BEGINNING)
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
            # Reset timer on receiving a message
            last_message_time = time.time()
            yield json.loads(msg.value().decode("utf-8"))


    # def consume(self):
    #     while True:
    #         msg = self.consumer.poll(0.5)
    #         if msg is None:
    #             continue
    #         if msg.error():
    #             if msg.error().code() == KafkaError._PARTITION_EOF:
    #                 continue
    #             else:
    #                 print(msg.error())
    #                 break
    #         yield json.loads(msg.value().decode("utf-8"))

    def close(self):
        self.consumer.close()
