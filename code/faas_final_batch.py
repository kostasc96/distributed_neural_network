import json
import numpy as np
import threading
import time
import io
from concurrent.futures import ThreadPoolExecutor
from pcomp_utils.kafka_confluent_utils import KafkaProducerHandler, KafkaConsumerHandler
from pcomp_utils.activation_functions import ACTIVATIONS, relu, softmax
from pcomp_utils.redis_utils import RedisHandler
from pcomp_utils.minio_utils import MinioClient
from pcomp_utils.utils import batch_generator


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
        self.redis_handler = RedisHandler('host.docker.internal', 6379, 0)
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.producer = None

    def fetch_input(self, batch_id, batch_size, columns_size):
        key = f"batch:{batch_id}:initial_data" if self.layer_id == 'layer_0' else f"batch:{batch_id}:layer_{int(self.layer_id[-1]) - 1}"
        # Poll Redis until the data is available.
        while True:
            data = np.frombuffer(self.redis_handler.get(key), dtype=np.float64).reshape(-1, int(columns_size))
            if data is not None:
                return data
            print(f"⏳ Neuron {self.neuron_id} waiting for input data for key: {key}")

    def process_and_send(self, batch_id, batch_size, columns_size):
        input_data = self.fetch_input(batch_id, batch_size, columns_size)
        z = np.dot(input_data, self.weights) + self.bias
        output = z if self.is_final_layer else self.activation_func(z)
        self.redis_handler.set(f"batch:{batch_id}:n_{self.layer_id_num}_{self.neuron_id}", output, True, 1000)
        msg = f"{self.neuron_id}|{batch_id}|{batch_size}|{columns_size}"
        self.producer.send(f'layer-{self.layer_id_num}-complete', msg, self.layer_id_num)
        #self.producer.send(f'requests-responses', 'www.neuron.example')

    def run(self):
        # Instantiate Kafka consumer and producer inside the thread.
        consumer = KafkaConsumerHandler(f'layer-{self.layer_id_num}', KAFKA_BROKER, partition=self.neuron_id)
        self.producer = KafkaProducerHandler(KAFKA_BROKER)
        last_msg_time = time.time()
        while True:
            got_message = False
            for message in consumer.consume():
                got_message = True
                last_msg_time = time.time()
                layer, batch_id_str, batch_size, columns_size = message.split('|')
                if layer == self.layer_id:
                    batch_id = int(batch_id_str)
                    self.executor.submit(self.process_and_send, batch_id, batch_size, columns_size)
            if not got_message and (time.time() - last_msg_time > 10):
                consumer.commit()
                consumer.close()
                self.producer.close()
                break
            time.sleep(0.05)


class LayerCoordinator(threading.Thread):
    def __init__(self, layer_id, neuron_count, is_final_layer=False):
        threading.Thread.__init__(self)
        self.layer_id = layer_id
        self.layer_id_num = int(self.layer_id.replace("layer_", ""))
        self.neuron_count = neuron_count
        self.is_final_layer = is_final_layer
        self.completed_neurons = {}
        self.redis_handler = RedisHandler('host.docker.internal', 6379, 0)
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.producer = None

    def run(self):
        consumer = KafkaConsumerHandler(f'layer-{self.layer_id_num}-complete', KAFKA_BROKER, self.layer_id_num)
        self.producer = KafkaProducerHandler(KAFKA_BROKER)
        last_msg_time = time.time()
        while True:
            got_message = False
            for message in consumer.consume():
                got_message = True
                last_msg_time = time.time()
                neuron_id, batch_id, batch_size, columns_size = message.split('|')
                neuron_id = int(neuron_id)
                batch_id = int(batch_id)
                if batch_id not in self.completed_neurons:
                    self.completed_neurons[batch_id] = set()
                self.completed_neurons[batch_id].add(neuron_id)

                if len(self.completed_neurons[batch_id]) == self.neuron_count:
                    batch_size = int(batch_size)
                    columns_size = int(columns_size)
                    self.aggregate_neuron_outputs(batch_id, batch_size, columns_size)
                    #self.executor.submit(self.aggregate_neuron_outputs, batch_id, batch_size, columns_size)
                    del self.completed_neurons[batch_id]
            if not got_message and (time.time() - last_msg_time > 10):
                consumer.commit()
                consumer.close()
                self.producer.close()
                break
            time.sleep(0.05)

    def aggregate_neuron_outputs(self, batch_id, batch_size, columns_size):
        keys = [f"batch:{batch_id}:n_{self.layer_id_num}_{neuron}" for neuron in range(self.neuron_count)]
        outputs = self.redis_handler.get_batch_multi(keys, batch_size)
        #outputs = np.squeeze(np.stack([self.redis_handler.get_batch(f"batch:n_{self.layer_id_num}_{neuron}_{batch_id}", batch_size, 1) for neuron in range(self.neuron_count)], axis=1))
        # Store the aggregated result in Redis.
        if not self.is_final_layer:
            self.redis_handler.set(f"batch:{batch_id}:{self.layer_id}", outputs, True, 1000)
            self.activate_next_layer(batch_id, batch_size, columns_size)
        else:
            preds = np.argmax(outputs, axis=1)
            pipe = self.redis_handler.pipeline()
            cnt = batch_id * int(batch_size)
            for idx, prediction in enumerate(preds):
                pipe.hset("batch:predictions", idx + cnt, int(prediction))
            pipe.execute()
            self.redis_handler.delete_batch_keys(batch_id)

    def activate_next_layer(self, batch_id, batch_size, columns_size):
        next_layer = f'layer_{self.layer_id_num + 1}'
        self.producer.send('activate-layer', f"{next_layer}|{batch_id}|{batch_size}|{self.neuron_count}", self.layer_id_num + 1)
        #self.producer.send(f'requests-responses', 'www.layercoordinator.example')
            

class Layer(threading.Thread):
    def __init__(self, layer_id, neuron_count):
        threading.Thread.__init__(self)
        self.layer_id = layer_id
        self.neuron_count = neuron_count
        self.layer_id_num = int(self.layer_id.replace("layer_", ""))
        self.executor = ThreadPoolExecutor(max_workers=8)
        self.producer = None

    def activate_neurons(self, batch_id, batch_size, columns_size):
        for neuron_id in range(self.neuron_count):
            self.executor.submit(self.send_activation, neuron_id, batch_id, batch_size, columns_size)

    def send_activation(self, neuron_id, batch_id, batch_size, columns_size):
        self.producer.send(f'layer-{self.layer_id_num}', f"{self.layer_id}|{batch_id}|{batch_size}|{columns_size}", neuron_id)
        #self.producer.send(f'requests-responses', 'www.layer.example')

    def run(self):
        consumer = KafkaConsumerHandler('activate-layer', KAFKA_BROKER, self.layer_id_num)
        self.producer = KafkaProducerHandler(KAFKA_BROKER)
        last_msg_time = time.time()
        while True:
            got_message = False
            for message in consumer.consume():
                got_message = True
                last_msg_time = time.time()
                layer, batch_id_str, batch_size, columns_size = message.split('|')
                if layer == self.layer_id:
                    batch_id = int(batch_id_str)
                    self.activate_neurons(batch_id, batch_size, columns_size)
            if not got_message and (time.time() - last_msg_time > 10):
                consumer.commit()
                consumer.close()
                self.producer.close()
                break
            time.sleep(0.05)

def predict_data():
    batch_size = 300
    producer = KafkaProducerHandler(KAFKA_BROKER)
    redis_handler = RedisHandler('host.docker.internal', 6379, 0)
    file = MinioClient("host.docker.internal:9000", "admin", "admin123").get_object("my-bucket", "mnist.csv")
    data = np.genfromtxt(io.StringIO(file.read().decode('utf-8')), delimiter=',', skip_header=1)
    features = data[:, :-1][:300]
    for idx, batch in enumerate(batch_generator(features, batch_size), start=0):
        redis_handler.set(f"batch:{idx}:initial_data", batch, True, 1000)
        producer.send('activate-layer', f"layer_0|{idx}|{batch_size}|784", 0)
    producer.close()

# Load network and dataset
data = json.load(open("node_based_model.json"))
#df = pd.read_csv('data/mnist.csv').head(10)

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

predict_data()
