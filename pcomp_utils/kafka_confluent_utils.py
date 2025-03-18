import flatbuffers
import time
import json
from confluent_kafka import Producer
from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_BEGINNING
from pcomp_utils.NeuronMessage import NeuronMessage
from pcomp_utils.LayerMessage import LayerMessage

class KafkaProducerHandler:
    def __init__(self, server):
        self.producer = Producer({
            'bootstrap.servers': server,
            'acks': 'all',
            'retries': 5,
            'linger.ms': 50,
            'batch.size': 16384
        })
    
    def send_neuron_message(self, topic, neuron_id, image_id, output_hex, partition):
        # Build a Neuron message using FlatBuffers
        builder = flatbuffers.Builder(1024)
        output_offset = builder.CreateString(output_hex)
        NeuronMessage.NeuronMessageStart(builder)
        NeuronMessage.NeuronMessageAddNeuronId(builder, neuron_id)
        NeuronMessage.NeuronMessageAddImageId(builder, image_id)
        NeuronMessage.NeuronMessageAddOutput(builder, output_offset)
        neuron_msg = NeuronMessage.NeuronMessageEnd(builder)
        builder.Finish(neuron_msg)
        buf = bytes(builder.Output())
        self.producer.produce(topic, value=buf, partition=partition)
        self.producer.flush()

    
    def send_layer_message(self, topic, layer, image_id, partition):
        # Build a Layer message using FlatBuffers
        builder = flatbuffers.Builder(1024)
        layer_offset = builder.CreateString(layer)
        LayerMessage.LayerMessageStart(builder)
        LayerMessage.LayerMessageAddLayer(builder, layer_offset)
        LayerMessage.LayerMessageAddImageId(builder, image_id)
        layer_msg = LayerMessage.LayerMessageEnd(builder)
        builder.Finish(layer_msg)
        buf = bytes(builder.Output())
        self.producer.produce(topic, value=buf, partition=partition)
        self.producer.flush()
    

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
    
    def consume_neuron_messages(self, poll_timeout=0.5, break_after=20):
        last_message_time = time.time()
        while True:
            msg = self.consumer.poll(poll_timeout)
            if msg is None:
                # Exit if no messages have been received for break_after seconds.
                if time.time() - last_message_time >= break_after:
                    break
                continue
            last_message_time = time.time()
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            buf = msg.value()
            # Parse the binary data as a NeuronMessage
            neuron = NeuronMessage.GetRootAsNeuronMessage(buf, 0)
            yield neuron

    def consume_layer_messages(self, poll_timeout=0.5, break_after=20):
        last_message_time = time.time()
        while True:
            msg = self.consumer.poll(poll_timeout)
            if msg is None:
                if time.time() - last_message_time >= break_after:
                    break
                continue
            last_message_time = time.time()
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            buf = msg.value()
            # Parse the binary data as a LayerMessage
            layer = LayerMessage.GetRootAsLayerMessage(buf, 0)
            yield layer
    
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

    def close(self):
        self.consumer.close()