import pandas as pd
import numpy as np
import redis
import boto3
import io
import time
from confluent_kafka import Producer

# ---------- CONFIGURATION ----------
REDIS_URL = os.environ.get('REDIS_URL')
redis_host, redis_port_str = REDIS_URL.split(":")
redis_port = int(redis_port_str)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC = 'layer-0'

s3_endpoint = os.environ.get('S3_URL')
s3_access_key = os.environ.get('S3_ACCESS_KEY')
s3_secret_key = os.environ.get('S3_SECRET_KEY')
bucket_name = os.environ.get('S3_BUCKET')
region = os.environ.get('S3_REGION')
S3_KEY = 'mnist.csv'

MAX_ROWS = 10000
SLEEP_MS = 50

# ---------- SETUP ----------

# Redis
r = redis.Redis(host=redis_host, port=redis_port, db=0)

# Kafka (local Confluent)
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

# MinIO/S3
s3 = boto3.client(
    's3',
    endpoint_url=s3_endpoint,
    aws_access_key_id=s3_access_key,
    aws_secret_access_key=s3_secret_key,
    region_name=region
)

# ---------- LOAD CSV ----------
response = s3.get_object(Bucket=bucket_name, Key=S3_KEY)
csv_content = response['Body'].read().decode('utf-8')
df = pd.read_csv(io.StringIO(csv_content))
features = df.iloc[:, :-1]  # exclude label

# ---------- PROCESS ----------
for idx, row in features.head(MAX_ROWS).iterrows():
    data = row.to_numpy(dtype=np.float64)
    byte_data = data.tobytes()

    # Store in Redis with expiration
    r.setex(f"initial_data_{idx}", 700, byte_data)

    # Send index to Kafka topic as string
    producer.produce(KAFKA_TOPIC, value=str(idx))
    producer.poll(0)

# Ensure all messages sent
producer.flush()
producer.close()
r.close()
print("Streaming complete.")
