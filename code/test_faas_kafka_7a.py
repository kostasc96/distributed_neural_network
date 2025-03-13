import json
import numpy as np
from kafka import KafkaProducer, KafkaConsumer
import redis
import threading
import time
from torchvision import datasets, transforms

# Redis setup
redis_client = redis.Redis(host='host.docker.internal', port=6379, db=0)

# Activation functions
def relu(x):
    return np.maximum(0, x)

def softmax(x):
    e_x = np.exp(x - np.max(x))
    return e_x / e_x.sum()

ACTIVATIONS = {
    "relu": relu,
    "softmax": softmax
}

class Neuron(threading.Thread):
    def __init__(self, layer_id, neuron_id, weights, bias, activation, is_final_layer=False):
        threading.Thread.__init__(self)
        self.layer_id = layer_id
        self.neuron_id = neuron_id
        self.weights = np.array(weights)
        self.bias = np.array(bias)
        self.activation_func = None if is_final_layer else ACTIVATIONS.get(activation, relu)
        self.is_final_layer = is_final_layer
        self.output = None

        # Kafka consumer for activation messages
        self.consumer = KafkaConsumer(
            f'layer-{self.layer_id[-1]}',
            bootstrap_servers='kafka:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            consumer_timeout_ms=60000
        )

    def fetch_input(self):
        if self.layer_id == 'layer_0':
            data = redis_client.get("initial_data")
        else:
            previous_layer = f'layer_{int(self.layer_id[-1]) - 1}'
            data = redis_client.get(previous_layer)
        if data:
            return np.frombuffer(data, dtype=np.float32)
        return None

    def run(self):
        print(f"🕒 Neuron {self.neuron_id} in {self.layer_id} waiting for activation...")

        for message in self.consumer:
            if message.value.get('layer') == self.layer_id:
                print(f"✅ Neuron {self.neuron_id} in {self.layer_id} received activation.")

                # Fetch input data after activation
                input_data = self.fetch_input()
                if input_data is None:
                    print(f"⚠️ Neuron {self.neuron_id} in {self.layer_id} did not find input data.")
                    break

                # Process data and return output
                self.output = self.process_data(input_data)
                break

        self.consumer.close()

    def process_data(self, inputs):
        z = np.dot(inputs, self.weights) + self.bias
        return z if self.is_final_layer else self.activation_func(z)

class Layer:
    def __init__(self, layer_id, neuron_configs, is_final_layer=False):
        self.layer_id = layer_id
        self.neuron_configs = neuron_configs
        self.is_final_layer = is_final_layer
        self.neurons = []

    def initialize_neurons(self):
        self.neurons = [
            Neuron(
                layer_id=self.layer_id,
                neuron_id=idx,
                weights=neuron['weights'],
                bias=neuron['biases'],
                activation=neuron['activation'],
                is_final_layer=self.is_final_layer
            )
            for idx, neuron in enumerate(self.neuron_configs)
        ]

    def forward(self, image_id):
        self.initialize_neurons()

        # Start neuron threads
        for neuron in self.neurons:
            neuron.start()

        time.sleep(2)

        # Send activation message to neurons
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        activation_message = {'layer': self.layer_id}

        for neuron_id in range(len(self.neurons)):
            producer.send(f'layer-{self.layer_id[-1]}', key=str(neuron_id).encode(), value=activation_message)
            print(f"✅ Layer {self.layer_id} sent activation to Neuron {neuron_id}")

        producer.flush()
        producer.close()

        # Wait for all neuron threads to complete
        for neuron in self.neurons:
            neuron.join()

        # Aggregate and store neuron outputs
        outputs = np.array([neuron.output for neuron in self.neurons])
        redis_client.set(self.layer_id, outputs.astype(np.float32).tobytes())
        print(f"📝 Layer {self.layer_id} stored aggregated data in Redis.")

        if self.is_final_layer:
            prediction = int(np.argmax(outputs))
            redis_client.hset('predictions', image_id, prediction)
            print(f"🎯 Prediction for Image {image_id}: {prediction}")

        if not self.is_final_layer:
            self.activate_next_layer()

    def activate_next_layer(self):
        producer = KafkaProducer(bootstrap_servers='kafka:9092',
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        next_layer = f'layer_{int(self.layer_id[-1]) + 1}'
        print(f"🚀 Activating next layer: {next_layer}")
        producer.send('activate-layer', {'layer': next_layer})
        producer.flush()
        producer.close()

def store_initial_input_data(input_data):
    redis_client.set("initial_data", input_data.astype(np.float32).tobytes())
    print("📥 Initial input data stored in Redis under 'initial_data' key.")

def calculate_accuracy(mnist_test):
    predictions = redis_client.hgetall('predictions')
    correct = sum(int(predictions[k]) == mnist_test[int(k)][1] for k in predictions)
    accuracy = correct / len(predictions)
    print(f"🎯 Test Accuracy: {accuracy * 100:.2f}%")

def load_network(filename):
    with open(filename, 'r') as f:
        return json.load(f)

def build_network(json_data):
    layers = []
    sorted_layers = sorted(json_data.keys(), key=lambda x: int(x.split('_')[-1]))
    for i, layer_name in enumerate(sorted_layers):
        layer_info = json_data[layer_name]
        neuron_configs = layer_info['nodes']
        layers.append(Layer(layer_id=layer_name, neuron_configs=neuron_configs, is_final_layer=(i == len(sorted_layers) - 1)))
    return layers

def forward_pass(layers, image_np, image_id):
    store_initial_input_data(image_np)
    for layer in layers:
        layer.forward(image_id)

# Load network
data = load_network("node_based_model.json")
network = build_network(data)

# Load MNIST dataset
transform = transforms.Compose([
    transforms.ToTensor(),
    transforms.Normalize((0.5,), (0.5,))
])
mnist_test = datasets.MNIST(root="./data", train=False, transform=transform, download=True)

# Process first 10 images
for i in range(10):
    image, label = mnist_test[i]
    image_np = image.view(-1).numpy()
    forward_pass(network, image_np, i)

# Calculate and print accuracy
calculate_accuracy(mnist_test)
