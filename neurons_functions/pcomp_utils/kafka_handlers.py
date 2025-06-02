from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition, OFFSET_END

class KafkaProducerHandler:
    def __init__(self, server, topic):
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': server,
            'linger.ms': 5,
            'batch.size': 256000,
            'queue.buffering.max.ms': 5,
            'queue.buffering.max.messages': 1000000,
            'compression.type': 'lz4',
            'enable.idempotence': True,
            'acks': 'all',
            'delivery.timeout.ms': 300000,
            'message.send.max.retries': 5,
            'retry.backoff.ms': 100,
            'socket.keepalive.enable': True,
            'socket.nagle.disable': True
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
    
    def send_with_key(self, key, message):
        key = key.encode('utf-8')
        value = message.encode('utf-8')
        self.producer.poll(0)
        try:
            self.producer.produce(self.topic, key=key, value=value)
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
    def __init__(self, topic, servers, batch_size=500, group_id='default_group'):
        self.topic = topic
        self.batch_size = batch_size
        self.consumer = Consumer({
            'bootstrap.servers': servers,
            'group.id': group_id,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000,
            'fetch.min.bytes': 262144,
            'fetch.max.bytes': 134217728,
            'fetch.wait.max.ms': 25,
            'max.partition.fetch.bytes': 67108864,
            'socket.receive.buffer.bytes': 8388608
        })
        self.consumer.subscribe([topic])

    
    def error_handling(self, msg):
        if msg.error().code() == KafkaError.OFFSET_OUT_OF_RANGE:
            print("Offset out of range, resetting...")
            current_assignments = self.consumer.assignment() or [
                TopicPartition(msg.topic(), msg.partition())
            ]
            new_assignments = [
                TopicPartition(tp.topic, tp.partition, OFFSET_END)
                for tp in current_assignments
            ]
            self.consumer.assign(new_assignments)
    
    def get_consumer(self):
        return self.consumer
    
    def commit(self):
        self.consumer.commit()

    def close(self):
        self.consumer.close()
        self.consumer = None