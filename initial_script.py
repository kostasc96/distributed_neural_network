import pandas as pd
import numpy as np
import redis
import boto3
import io
import time
from confluent_kafka import Producer

# ---------- CONFIGURATION ----------
REDIS_HOST = 'localhost'
REDIS_PORT = 6379

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'layer-0'

S3_ENDPOINT = 'http://localhost:9000'
S3_BUCKET = 'my-bucket'
S3_KEY = 'mnist.csv'
AWS_ACCESS_KEY = 'admin'
AWS_SECRET_KEY = 'admin123'

MAX_ROWS = 10000
SLEEP_MS = 50

# ---------- SETUP ----------

# Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

# Kafka (local Confluent)
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

# MinIO/S3
s3 = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name='us-east-1'
)

# ---------- LOAD CSV ----------
response = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
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
