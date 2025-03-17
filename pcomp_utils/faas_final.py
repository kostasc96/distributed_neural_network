import json
import numpy as np
import threading
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from pcomp_utils.kafka_producer_utils import KafkaProducerHandler
from pcomp_utils.kafka_consumer_utils import KafkaConsumerHandler
from pcomp_utils.activation_functions import ACTIVATIONS, relu, softmax
from pcomp_utils.redis_utils import RedisHandler

# Kafka Configuration
KAFKA_BROKER = 'kafka:9092'

class Neuron(threading.Thread):
    def __init__(self, layer_id, neuron_id, weights, bias, activation, is_final_layer):
        threading.Thread.__init__(self)
        self.layer_id = layer_id
        self.layer_id_num = int(self.layer_id.replace("layer_", ""))
        self.neuron_id = neuron_id
        self.weights = np.array(weights)
        self.bias = np.array(bias)
        self.activation_func = ACTIVATIONS.get(activation, relu)
        self.is_final_layer = is_final_layer
        self.consumer = KafkaConsumerHandler(f'layer-{self.layer_id_num}', KAFKA_BROKER, partition=self.neuron_id)
        self.producer = KafkaProducerHandler(KAFKA_BROKER)
        self.redis_handler = RedisHandler('host.docker.internal', 6379, 0)
        self.executor = ThreadPoolExecutor(max_workers=4)

    def fetch_input(self, image_id):
        key = f"initial_data:{image_id}" if self.layer_id == 'layer_0' else f"layer_{int(self.layer_id[-1]) - 1}_{image_id}"
        data = self.redis_handler.get(key)
        while True:
            data = self.redis_handler.get(key)
            if data is not None:
                return data

    def process_data(self, inputs):
        z = np.dot(inputs, self.weights) + self.bias
        return z if self.is_final_layer else self.activation_func(z)

    def process_and_send(self, image_id, input_data):
        # thread pool.
        output = self.process_data(input_data)
        msg = {
            'neuron_id': self.neuron_id,
            'image_id': image_id,
            'output': output.astype(np.float64).tobytes().hex()
        }
        self.producer.send(f'layer-{self.layer_id_num}-complete', msg, self.layer_id_num)

    def run(self):
        last_msg_time = time.time()
        while True:
            got_message = False
            for message in self.consumer.consume():
                got_message = True
                last_msg_time = time.time()
                if message.value.get('layer') == self.layer_id:
                    image_id = message.value.get('image_id')
                    redis_key = f"{self.layer_id}:neuron_{self.neuron_id}:{image_id}"
    
                    input_data = self.fetch_input(image_id)
                    if input_data is not None:
                        self.executor.submit(self.process_and_send, image_id, input_data)
            if not got_message:
                if time.time() - last_msg_time > 10:
                    self.consumer.close()
                    self.producer.close()
                    break
            time.sleep(0.1)


class LayerCoordinator(threading.Thread):
    def __init__(self, layer_id, neuron_count, is_final_layer=False):
        threading.Thread.__init__(self)
        self.layer_id = layer_id
        self.layer_id_num = int(self.layer_id.replace("layer_", ""))
        self.neuron_count = neuron_count
        self.is_final_layer = is_final_layer
        self.completed_neurons = {}  # TODO: make a dictionary with image_id as the keys
        self.outputs = {}
        self.consumer = KafkaConsumerHandler(f'layer-{self.layer_id_num}-complete', KAFKA_BROKER, self.layer_id_num)
        self.producer = KafkaProducerHandler(KAFKA_BROKER)
        self.redis_handler = RedisHandler('host.docker.internal', 6379, 0)
        self.executor = ThreadPoolExecutor(max_workers=4)

    def run(self):
        last_msg_time = time.time()
        while True:
            got_message = False
            for message in self.consumer.consume():
                got_message = True
                last_msg_time = time.time()
                image_id = message.value.get('image_id')
                neuron_id = message.value.get('neuron_id')
                output_hex = message.value.get('output')
                output = np.frombuffer(bytes.fromhex(output_hex), dtype=np.float64)[0]
                if image_id not in self.completed_neurons:
                    self.completed_neurons[image_id] = set()
                    self.outputs[image_id] = {}
                self.completed_neurons[image_id].add(neuron_id)
                self.outputs[image_id][neuron_id] = output

                if len(self.completed_neurons[image_id]) == self.neuron_count:
                    local_outputs = self.outputs[image_id].copy()
                    self.executor.submit(self.aggregate_neuron_outputs, image_id, local_outputs)
                    del self.completed_neurons[image_id]
                    del self.outputs[image_id]
                #self.consumer.commit(message.topic, message.partition, message.offset + 1)
            if not got_message:
                if time.time() - last_msg_time > 10:
                    self.consumer.close()
                    self.producer.close()
                    break
            time.sleep(0.1)

    def aggregate_neuron_outputs(self, image_id, local_outputs):
        if not self.is_final_layer:
            self.activate_next_layer(image_id)
        outputs = np.array([local_outputs.get(neuron_id) for neuron_id in range(self.neuron_count)])
    
        # Store the aggregated result in Redis
        self.redis_handler.set(f"{self.layer_id}_{image_id}", outputs)
    
        if self.is_final_layer:
            prediction = int(np.argmax(outputs))
            self.redis_handler.hset('predictions', image_id, prediction)
            self.redis_handler.delete(f"initial_data:{image_id}")
    
    def activate_next_layer(self, image_id):
        next_layer = f'layer_{self.layer_id_num + 1}'
        self.producer.send('activate-layer', {'layer': next_layer, 'image_id': image_id}, self.layer_id_num + 1)
            

class Layer(threading.Thread):
    def __init__(self, layer_id, neuron_count):
        threading.Thread.__init__(self)
        self.layer_id = layer_id
        self.neuron_count = neuron_count
        self.producer = KafkaProducerHandler(KAFKA_BROKER)
        self.layer_id_num = int(self.layer_id.replace("layer_", ""))
        self.consumer = KafkaConsumerHandler('activate-layer', KAFKA_BROKER, self.layer_id_num)
        self.executor = ThreadPoolExecutor(max_workers=8)

    def activate_neurons(self, image_id):
        # Offload each send to the executor.
        for neuron_id in range(self.neuron_count):
            self.executor.submit(self.send_activation, neuron_id, image_id)

    def send_activation(self, neuron_id, image_id):
        self.producer.send(f'layer-{self.layer_id_num}', {'layer': self.layer_id, 'image_id': image_id}, neuron_id)

    def run(self):
        last_msg_time = time.time()
        while True:
            got_message = False
            for message in self.consumer.consume():
                got_message = True
                last_msg_time = time.time()
                if message.value.get('layer') == self.layer_id:
                    image_id = message.value.get('image_id')
                    self.activate_neurons(image_id)
            if not got_message:
                if time.time() - last_msg_time > 10:
                    self.consumer.close()
                    self.producer.close()
                    break
            time.sleep(0.1)

def store_initial_input_data(image_np, image_id):
    redis_handler = RedisHandler('host.docker.internal', 6379, 0)
    key = f"initial_data:{image_id}"
    redis_handler.set(key, image_np)

def activate_network(image_id):
    producer = KafkaProducerHandler(KAFKA_BROKER)
    producer.send('activate-layer', {'layer': 'layer_0', 'image_id': image_id}, 0)
    producer.close()

# Load network and dataset
data = json.load(open("node_based_model.json"))
df = pd.read_csv('data/mnist.csv').head(10)

neurons = []
layers = []
coordinators = []

for layer_name, layer_info in data.items():
    neurons += [Neuron(layer_id=layer_name, neuron_id=i, weights=node['weights'], bias=node['biases'], activation=node['activation'], is_final_layer=(layer_name == list(data.keys())[-1])) for i, node in enumerate(layer_info['nodes'])]
    layers.append(Layer(layer_id=layer_name, neuron_count=len(layer_info['nodes'])))
    coordinators.append(LayerCoordinator(layer_id=layer_name, neuron_count=len(layer_info['nodes']), is_final_layer=(layer_name == list(data.keys())[-1])))

# Start all threads
for thread in neurons + layers + coordinators:
    thread.start()


print("Threads started")


# Wait for all threads to complete
for thread in neurons + layers + coordinators:
    thread.join()

print("Threads finished")