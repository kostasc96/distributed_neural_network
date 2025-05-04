import json
import numpy as np
import threading
import time
from pcomp.kafka_handlers import KafkaProducerHandler, KafkaConsumerHandler
from pcomp.activation_functions import ACTIVATIONS, relu, softmax
from pcomp.redis_utils import RedisHandler


class Neuron(threading.Thread):
    IDLE_TIMEOUT = 60  # seconds

    def __init__(self, parameters, kafka_broker, redis_url, batch_size=50):
        threading.Thread.__init__(self)
        params = json.loads(parameters)
        self.layer_id = params["layer_id"]
        self.layer_id_num = int(params["layer_id_num"])
        self.neuron_id = int(params["neuron_id"])
        self.is_final_layer = params.get("is_final_layer") != "0"
        flat_weights = json.loads(params["weights"])
        len_flat_weights = len(flat_weights)
        self.neuron_count = int(params["neuron_count"])
        self.weights = np.ascontiguousarray(flat_weights, dtype=np.float64)
        self.bias = np.ascontiguousarray(float(params["bias"]), dtype=np.float64)
        self.activation = None if self.is_final_layer else params.get("activation")
        self.activation_func = None if self.is_final_layer else ACTIVATIONS.get(self.activation, relu)
        self.kafka_broker = kafka_broker
        host, port = redis_url.split(":")
        self.redis_handler = RedisHandler(host, int(port), 0, 2)
        self.producer = None
        self.batch_size = batch_size
        self._input_buf = np.empty((self.batch_size, len_flat_weights), dtype=np.float64)
        self._z = np.empty(self.batch_size, dtype=np.float64)

    def fetch_input(self, image_ids, input_size):
        pipe = self.redis_handler.pipeline()
        prefix = (f"initial_data_" if self.layer_id_num == 0
                  else f"{self.layer_id_num - 1}_")
        for img_id in image_ids:
            pipe.get(f"{prefix}{img_id}")
        raws = pipe.execute()
        buf = self._input_buf[:input_size]
        for i, raw in enumerate(raws):
            buf[i, :] = np.frombuffer(raw or b'', dtype=np.float64)
        return buf

    def process_and_send(self, image_ids, inputs, input_size):
        z = self._z[:input_size]
        np.dot(inputs, self.weights, out=z)
        z += self.bias
        outputs = z if self.is_final_layer else self.activation_func(z)
        for img_id, val in zip(image_ids, outputs):
            self.producer.send_with_key(
                str(img_id), f"{self.neuron_id}|{format(val, '.17g')}"
            )

    def run(self):
        # set up consumer and producer
        consumer = KafkaConsumerHandler(
            f'layer-{self.layer_id_num}',
            self.kafka_broker,
            group_id=f"{self.neuron_id}_{self.layer_id_num}_group"
        ).get_consumer()
        self.producer = KafkaProducerHandler(
            self.kafka_broker,
            f'layer-{self.layer_id_num}-streams'
        )

        last_msg_time = time.time()
        while True:
            # consume a batch of messages
            msgs = consumer.consume(self.batch_size, timeout=0.2)
            if not msgs:
                # check for idle timeout
                if time.time() - last_msg_time > self.IDLE_TIMEOUT:
                    consumer.commit()
                    consumer.close()
                    self.producer.close()
                    self.redis_handler.close()
                    break
                continue
            last_msg_time = time.time()
            image_ids = [int(msg.value().decode('utf-8')) for msg in msgs]
            input_size = len(image_ids)
            # fetch inputs
            inputs = self.fetch_input(image_ids, input_size)
            # batch compute and send
            self.process_and_send(image_ids, inputs, input_size)