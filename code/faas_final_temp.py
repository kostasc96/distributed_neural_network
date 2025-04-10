import json
import numpy as np
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pcomp.kafka_handlers import KafkaProducerHandler, KafkaConsumerHandler, KafkaConsumerHandlerNeuron
from pcomp.activation_functions import ACTIVATIONS, relu, softmax
from pcomp.redis_utils import RedisHandler
from pcomp.parser import parse_layer_coordinator_message, parse_layer_message
from pcomp.avro_utils import avro_serialize, avro_deserialize
from pcomp.neurons_accumulator import NeuronsAccumulator

# Kafka Configuration
KAFKA_BROKER = 'kafka:29092'


class Neuron(threading.Thread):
    def __init__(self, layer_id, neuron_id, weights, bias, activation, is_final_layer):
        threading.Thread.__init__(self)
        self.layer_id = layer_id
        self.layer_id_num = int(self.layer_id.replace("layer_", ""))
        self.neuron_id = neuron_id
        self.weights = np.array(weights)
        self.bias = np.array(bias)
        self.activation = activation
        self.activation_func = ACTIVATIONS.get(activation, relu)
        self.is_final_layer = is_final_layer
        self.redis_handler = RedisHandler('host.docker.internal', 6379, 0)
        self.producer = None
        self.executor = ThreadPoolExecutor(max_workers=4)
        

    def fetch_input(self, image_id):
        key = f"streams:{image_id}:initial_data" if self.layer_id == 'layer_0' else f"streams:{image_id}:{self.layer_id_num - 1}"
        # Poll Redis until the data is available.
        while True:
            data = np.frombuffer(self.redis_handler.get(key), dtype=np.float64)
            if data is not None:
                return data
            print(f"⏳ Neuron {self.neuron_id} waiting for input data for key: {key}")
            time.sleep(0.5)

    def process_and_send(self, image_id, input_data):
        z = np.dot(input_data, self.weights) + self.bias
        output = z if self.is_final_layer else self.activation_func(z)
        #key = f"outputs:{image_id}:{self.layer_id_num}"
        #self.redis_handler.store_neuron_output(key, self.neuron_id, output)
        msg = f"{image_id}|{self.neuron_id}|{format(output, '.17g')}"
        self.producer.send(msg)
        #self.producer.send(f'requests-responses', 'www.neuron.example')

    def run(self):
        # Instantiate Kafka consumer and producer inside the thread.
        consumer = KafkaConsumerHandler(f'layer-{self.layer_id_num}', KAFKA_BROKER, group_id=f"{self.neuron_id}_{self.layer_id_num}_group")
        self.producer = KafkaProducerHandler(KAFKA_BROKER, f'layer-{self.layer_id_num}-complete')
        last_msg_time = time.time()
        while True:
            got_message = False
            for message in consumer.consume():
                got_message = True
                last_msg_time = time.time()
                image_id_str = message
                image_id = int(image_id_str)
                input_data = self.fetch_input(image_id)
                self.process_and_send(image_id, input_data)
            if not got_message and (time.time() - last_msg_time > 10):
                consumer.commit()
                consumer.close()
                self.producer.close()
                break


class LayerCoordinator(threading.Thread):
    def __init__(self, layer_id, neuron_count, is_final_layer=False):
        threading.Thread.__init__(self)
        self.layer_id = layer_id
        self.layer_id_num = int(self.layer_id.replace("layer_", ""))
        self.neuron_count = neuron_count
        self.is_final_layer = is_final_layer
        self.redis_handler = RedisHandler('host.docker.internal', 6379, 0)
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.producer = None

    def run(self):
        consumer = KafkaConsumerHandler(f'layer-{self.layer_id_num}-complete', KAFKA_BROKER, group_id=f"{self.layer_id_num}_coord_group")
        if not self.is_final_layer:
            self.producer = KafkaProducerHandler(KAFKA_BROKER, f"layer-{self.layer_id_num + 1}")
        last_msg_time = time.time()
        while True:
            got_message = False
            for message in consumer.consume():
                got_message = True
                last_msg_time = time.time()
                image_id_str, neuron_id_str, output = message.split("|")
                image_id = int(image_id_str)
                cnt = self.redis_handler.store_neuron_result(f"outputs:{image_id}:{self.layer_id_num}", f"streams:{image_id}:cnt:{self.layer_id_num}", neuron_id_str, output)
                if cnt == self.neuron_count:
                    key = f"outputs:{image_id}:{self.layer_id_num}"
                    self.aggregate_neuron_outputs(image_id, key)
            if not got_message and (time.time() - last_msg_time > 10):
                consumer.commit()
                consumer.close()
                if self.producer:
                    self.producer.close()
                break

    def aggregate_neuron_outputs(self, image_id, key):
        outputs = self.redis_handler.get_output_vector(key, self.neuron_count)
        if not self.is_final_layer:
            self.redis_handler.set(f"streams:{image_id}:{self.layer_id_num}", outputs, True, 60)
            msg = f"{image_id}"
            self.producer.send(msg)
        else:
            prediction = int(np.argmax(outputs))
            self.redis_handler.hset('streams:predictions', image_id, prediction)
            self.redis_handler.delete_streams_keys(image_id)

# Load network and dataset
data = json.load(open("node_based_model.json"))
#df = pd.read_csv('data/mnist.csv').head(10)

neurons = []
layers = []
coordinators = []

for layer_name, layer_info in data.items():
    neurons += [Neuron(layer_id=layer_name, neuron_id=i, weights=node['weights'], bias=node['biases'], activation=node['activation'], is_final_layer=(layer_name == list(data.keys())[-1])) for i, node in enumerate(layer_info['nodes'])]
    coordinators.append(LayerCoordinator(layer_id=layer_name, neuron_count=len(layer_info['nodes']), is_final_layer=(layer_name == list(data.keys())[-1])))

# Start all threads
for thread in neurons + coordinators:
    thread.start()

print("Threads started")