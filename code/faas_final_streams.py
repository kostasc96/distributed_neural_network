import json
import numpy as np
import threading
import time
from base64 import b64encode, b64decode
from concurrent.futures import ThreadPoolExecutor
from pcomp.kafka_handlers import KafkaProducerHandler, KafkaConsumerHandler
from pcomp.activation_functions import ACTIVATIONS, relu, softmax
from pcomp.redis_utils import RedisHandler
from pcomp.parser import parse_layer_coordinator_message, parse_layer_message

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
        self.activation = activation
        self.activation_func = ACTIVATIONS.get(activation, relu)
        self.is_final_layer = is_final_layer
        self.producer = None
        self.executor = ThreadPoolExecutor(max_workers=4)
        

    def process_data(self, inputs):
        z = np.dot(inputs, self.weights) + self.bias
        return z if self.is_final_layer else self.activation_func(z)

    def process_and_send(self, image_id, input_data):
        z = np.dot(input_data, self.weights) + self.bias
        output = z if self.is_final_layer else self.activation_func(z)
        output_str = format(output, '.17g')
        msg = f"{self.neuron_id}|{image_id}|{output_str}"
        self.producer.send(msg)
        #self.producer.send(f'requests-responses', 'www.neuron.example')

    def run(self):
        # Instantiate Kafka consumer and producer inside the thread.
        consumer = KafkaConsumerHandlerNeuron(f'layer-{self.layer_id_num}', KAFKA_BROKER, group_id=f"{self.neuron_id}_{self.layer_id_num}_group")
        self.producer = KafkaProducerHandler(KAFKA_BROKER, f'layer-{self.layer_id_num}-complete')
        last_msg_time = time.time()
        while True:
            got_message = False
            for message in consumer.consume():
                got_message = True
                last_msg_time = time.time()
                message_dict = avro_deserialize(message.value())
                layer = message_dict["layer_id"]
                image_id = message_dict["image_id"]
                data_bytes = message_dict["data"]
                input_data = np.frombuffer(data_bytes, dtype=np.float64)
                if layer == self.layer_id:
                    self.executor.submit(self.process_and_send, image_id, input_data)
                    #self.process_and_send(image_id, input_data)
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
        self.accumulators = {}
        self.redis_handler = RedisHandler('host.docker.internal', 6379, 0)
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.producer = None

    def run(self):
        consumer = KafkaConsumerHandler(f'layer-{self.layer_id_num}-complete', KAFKA_BROKER, group_id=f"{self.layer_id_num}_coord_group")
        self.producer = KafkaProducerHandler(KAFKA_BROKER, 'activate-layer')
        last_msg_time = time.time()
        while True:
            got_message = False
            for message in consumer.consume():
                got_message = True
                last_msg_time = time.time()
                #neuron_id, image_id, output_str = message.split('|')
                msg = parse_layer_coordinator_message(message)
                neuron_id = msg.neuron_id
                image_id = msg.image_id
                output = msg.output
                try:
                    acc = self.accumulators[image_id]
                except KeyError:
                    acc = self.accumulators[image_id] = NeuronsAccumulator(self.neuron_count)
                if acc.outputs[neuron_id] is None:
                    acc.outputs[neuron_id] = output
                    acc.completed += 1
                if acc.completed == self.neuron_count:
                    self.executor.submit(self.aggregate_neuron_outputs, image_id, acc.outputs.copy())
                    del self.accumulators[image_id]
            if not got_message and (time.time() - last_msg_time > 10):
                consumer.commit()
                consumer.close()
                self.producer.close()
                break

    def aggregate_neuron_outputs(self, image_id, local_outputs):
        outputs = np.array(local_outputs, dtype=np.float64)
        if not self.is_final_layer:
            self.redis_handler.set(f"streams:{image_id}:{self.layer_id_num}", outputs)
            self.activate_next_layer(image_id)
        else:
            prediction = int(np.argmax(outputs))
            self.redis_handler.hset('streams:predictions', image_id, prediction)
            self.redis_handler.delete_streams_keys(image_id)

    def activate_next_layer(self, image_id):
        next_layer = f'layer_{self.layer_id_num + 1}'
        self.producer.send(f"{next_layer}|{image_id}")
        #self.producer.send(f'requests-responses', 'www.layercoordinator.example')
            

class Layer(threading.Thread):
    def __init__(self, layer_id, neuron_count):
        threading.Thread.__init__(self)
        self.layer_id = layer_id
        self.neuron_count = neuron_count
        self.layer_id_num = int(self.layer_id.replace("layer_", ""))
        self.producer = None
        self.redis_handler = RedisHandler('host.docker.internal', 6379, 0)

    def fetch_input(self, image_id):
        key = f"streams:{image_id}:initial_data" if self.layer_id == 'layer_0' else f"streams:{image_id}:{self.layer_id_num - 1}"
        # Poll Redis until the data is available.
        while True:
            data = self.redis_handler.get(key)
            if data is not None:
                return data

    def activate_neurons(self, image_id):
        input_data = self.fetch_input(image_id).tobytes()
        message_dict = {
            "layer_id": self.layer_id,
            "image_id": image_id,
            "data": input_data
        }
        msg = avro_serialize(message_dict)
        self.producer.send_neuron(msg)

    def run(self):
        consumer = KafkaConsumerHandler('activate-layer', KAFKA_BROKER, group_id=f"{self.layer_id_num}_group")
        self.producer = KafkaProducerHandler(KAFKA_BROKER, f'layer-{self.layer_id_num}')
        last_msg_time = time.time()
        while True:
            got_message = False
            for message in consumer.consume():
                got_message = True
                last_msg_time = time.time()
                #layer, image_id_str = message.split('|')
                msg = parse_layer_message(message)
                layer = msg.layer
                image_id= msg.image_id
                if layer == self.layer_id:
                    #image_id = int(image_id_str)
                    self.activate_neurons(image_id)
            if not got_message and (time.time() - last_msg_time > 10):
                consumer.commit()
                consumer.close()
                self.producer.close()
                break

def store_initial_input_data(image_np, image_id):
    redis_handler = RedisHandler('host.docker.internal', 6379, 0)
    key = f"initial_data:{image_id}"
    redis_handler.set(key, image_np)
    print(f"📥 Initial input data stored in Redis.")

def activate_network(image_id):
    producer = KafkaProducerHandler(KAFKA_BROKER)
    producer.send('activate-layer', {'layer': 'layer_0', 'image_id': image_id}, 0)
    #self.producer.send(f'requests-responses', 'www.layer.example')
    print(f"🚀 Initial activation sent to activate-layer for layer_0 and image {image_id}")
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