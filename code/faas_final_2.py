import json
import numpy as np
import threading
import time
import pandas as pd
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

    def fetch_input(self, image_id):
        key = f"initial_data:{image_id}" if self.layer_id == 'layer_0' else f"layer_{int(self.layer_id[-1]) - 1}_{image_id}"
        data = self.redis_handler.get(key)
        while True:
            data = self.redis_handler.get(key)
            if data is not None:
                return data
            print(f"⏳ Neuron {self.neuron_id} waiting for input data for key: {key}")

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
                        output = self.process_data(input_data)
                        self.redis_handler.set(f"{self.layer_id}:{self.neuron_id}:{image_id}", str(output), False)
                        self.producer.send(f'layer-{self.layer_id_num}-complete', {'neuron_id': self.neuron_id, 'image_id': image_id}, self.layer_id_num)
                #self.consumer.commit(message.topic, message.partition, message.offset + 1)
                #break
            if not got_message:
                if time.time() - last_msg_time > 10:
                    print(f"No messages for 10 seconds in Neuron {self.neuron_id}; stopping thread.")
                    break
            time.sleep(0.5)

    def process_data(self, inputs):
        z = np.dot(inputs, self.weights) + self.bias
        return z if self.is_final_layer else self.activation_func(z)

class LayerCoordinator(threading.Thread):
    def __init__(self, layer_id, neuron_count, is_final_layer=False):
        threading.Thread.__init__(self)
        self.layer_id = layer_id
        self.layer_id_num = int(self.layer_id.replace("layer_", ""))
        self.neuron_count = neuron_count
        self.is_final_layer = is_final_layer
        self.completed_neurons = {}  # TODO: make a dictionary with image_id as the keys
        self.consumer = KafkaConsumerHandler(f'layer-{self.layer_id_num}-complete', KAFKA_BROKER, self.layer_id_num)
        self.producer = KafkaProducerHandler(KAFKA_BROKER)
        self.redis_handler = RedisHandler('host.docker.internal', 6379, 0)

    def run(self):
        last_msg_time = time.time()
        while True:
            got_message = False
            for message in self.consumer.consume():
                got_message = True
                last_msg_time = time.time()
                image_id = message.value.get('image_id')
                neuron_id = message.value.get('neuron_id')
                if image_id not in self.completed_neurons:
                    self.completed_neurons[image_id] = set()
                self.completed_neurons[image_id].add(neuron_id)

                if len(self.completed_neurons[image_id]) == self.neuron_count:
                    print(f"🟢 Aggregating outputs for image {image_id} on layer {self.layer_id}")
                    self.aggregate_neuron_outputs(image_id)
                    del self.completed_neurons[image_id]
                #self.consumer.commit(message.topic, message.partition, message.offset + 1)
            if not got_message:
                if time.time() - last_msg_time > 10:
                    print(f"No messages for 10 seconds in Neuron {neuron_id}; stopping thread.")
                    break
            time.sleep(0.5)

    def aggregate_neuron_outputs(self, image_id):
        if not self.is_final_layer:
            self.activate_next_layer(image_id)
        outputs = np.array([np.float64(self.redis_handler.get(f"{self.layer_id}:{neuron_id}:{image_id}", False)) for neuron_id in range(self.neuron_count)])
        #print(f"✅ Aggregated outputs for image {image_id}: {outputs}")
    
        # Store the aggregated result in Redis
        self.redis_handler.set(f"{self.layer_id}_{image_id}", outputs)
    
        if self.is_final_layer:
            prediction = int(np.argmax(outputs))
            self.redis_handler.hset('predictions', image_id, prediction)
            print(f"🎯 Final Prediction for image {image_id}: {prediction}")
    
    def activate_next_layer(self, image_id):
        next_layer = f'layer_{self.layer_id_num + 1}'
        self.producer.send('activate-layer', {'layer': next_layer, 'image_id': image_id}, self.layer_id_num + 1)
        print(f"🚀 Activating next layer: {next_layer} for image {image_id}")
            

class Layer(threading.Thread):
    def __init__(self, layer_id, neuron_count):
        threading.Thread.__init__(self)
        self.layer_id = layer_id
        self.neuron_count = neuron_count
        self.producer = KafkaProducerHandler(KAFKA_BROKER)
        self.layer_id_num = int(self.layer_id.replace("layer_", ""))
        self.consumer = KafkaConsumerHandler('activate-layer', KAFKA_BROKER, self.layer_id_num)

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
                    print(f"No messages for 10 seconds in Neuron {self.neuron_id}; stopping thread.")
                    break
            time.sleep(0.5)

    def activate_neurons(self, image_id):
        for neuron_id in range(self.neuron_count):
            self.producer.send(f'layer-{self.layer_id_num}', {'layer': self.layer_id, 'image_id': image_id}, neuron_id)
            print(f"✅ Activated Neuron {neuron_id} in {self.layer_id} for image {image_id}")

def store_initial_input_data(image_np, image_id):
    redis_handler = RedisHandler('host.docker.internal', 6379, 0)
    key = f"initial_data:{image_id}"
    redis_handler.set(key, image_np)
    print(f"📥 Initial input data stored in Redis.")

def activate_network(image_id):
    producer = KafkaProducerHandler(KAFKA_BROKER)
    producer.send('activate-layer', {'layer': 'layer_0', 'image_id': image_id}, 0)
    print(f"🚀 Initial activation sent to activate-layer for layer_0 and image {image_id}")
    producer.close()

# Load network and dataset
data = json.load(open("node_based_model.json"))
df = pd.read_csv('data/mnist.csv').head(2)

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



# Send images one by one with a 40-second delay
for idx, row in df.iterrows():
    image_np = row.iloc[:-1].values.astype(np.float64)
    store_initial_input_data(image_np, idx)
    activate_network(idx)
    #time.sleep(3)

# Wait for all threads to complete
for thread in neurons + layers + coordinators:
    thread.join()



from pcomp_utils.redis_utils import RedisHandler
import pandas as pd

def calculate_accuracy(file_path, limit=10):
    redis_handler = RedisHandler('host.docker.internal', 6379, 0)
    df = pd.read_csv(file_path).head(limit)
    predictions = redis_handler.hgetall('predictions')

    correct = 0
    total = 0

    for idx, row in df.iterrows():
        label = int(row['label'])
        prediction = predictions.get(str(idx).encode())  # Decode Redis key lookup

        if prediction is not None:
            prediction = int(prediction.decode())  # Decode the Redis stored value
            print(f"✅ Prediction for Image {idx}: {prediction}, Actual Label: {label}")

            if prediction == label:
                correct += 1
            total += 1
        else:
            print(f"⚠️ No prediction found for Image ID: {idx}")

    if total == 0:
        print("⚠️ No valid predictions to calculate accuracy.")
        return

    accuracy = correct / total
    print(f"🎯 Test Accuracy (First {limit} Images): {accuracy * 100:.2f}%")


calculate_accuracy('data/mnist.csv', 2)
