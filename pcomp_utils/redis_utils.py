import redis
import numpy as np

class RedisHandler:
    def __init__(self, host, port, db):
        self.client = redis.Redis(host, port, db)

    def set(self, key, value, to_bytes=True):
        if to_bytes:
            self.client.set(key, value.astype(np.float32).tobytes())
        else:
            self.client.set(key, value)

    def get(self, key, from_bytes=True):
        data = self.client.get(key)
        if data:
            if from_bytes:
                return np.frombuffer(data, dtype=np.float32)
            else:
                return data
        print(f"⚠️ No data found in Redis for key: {key}")
        return None

    def hset(self, key, field, value):
        self.client.hset(key, field, value)

    def hgetall(self, key):
        return self.client.hgetall(key)