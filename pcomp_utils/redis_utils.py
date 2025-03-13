import redis
import numpy as np

class RedisHandler:
    def __init__(self, host, port, db):
        self.client = redis.Redis(host, port, db)

    def set(self, key, value):
        self.client.set(key, value.astype(np.float32).tobytes())

    def get(self, key):
        data = self.client.get(key)
        if data:
            return np.frombuffer(data, dtype=np.float32)
        print(f"⚠️ No data found in Redis for key: {key}")
        return None

    def hset(self, key, field, value):
        self.client.hset(key, field, value)

    def hgetall(self, key):
        return self.client.hgetall(key)