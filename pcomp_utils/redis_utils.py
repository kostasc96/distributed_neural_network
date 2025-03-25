import redis
import numpy as np

class RedisHandler:
    def __init__(self, host, port, db):
        self.client = redis.Redis(host, port, db)

    def set(self, key, value, to_bytes=True, seconds=15):
        if to_bytes:
            self.client.set(key, value.astype(np.float64).tobytes())
        else:
            self.client.set(key, value)
        self.client.expire(key, seconds)

    def get(self, key, from_bytes=True):
        data = self.client.get(key)
        if data:
            if from_bytes:
                return np.frombuffer(data, dtype=np.float64)
            else:
                return data
        print(f"⚠️ No data found in Redis for key: {key}")
        return None
    
    def get_batch(self, key, batch_size, columns_size):
        data = self.client.get(key)
        if data:
            return np.frombuffer(data, dtype=np.float64).reshape(batch_size, columns_size)
        print(f"⚠️ No data found in Redis for key: {key}")
        return None
    
    def get_batch_multi(self, keys, batch_size, columns_size=1):
        pipe = self.client.pipeline()
        for key in keys:
            pipe.get(key)
        blobs = pipe.execute()
        arrays = []
        for blob in blobs:
            if blob:
                arrays.append(np.frombuffer(blob, dtype=np.float64).reshape(batch_size, columns_size))
            else:
                arrays.append(np.zeros((batch_size, columns_size)))  # fallback
        del_pipe = self.client.pipeline()
        for key in keys:
            del_pipe.unlink(key)
        del_pipe.execute()
        return np.squeeze(np.stack(arrays, axis=1))

        
    def hset(self, key, field, value):
        self.client.hset(key, field, value)

    def hgetall(self, key):
        return self.client.hgetall(key)
    
    def sadd(self, key, value):
        self.client.sadd(key, value)
    
    def scard(self, key):
        return self.client.scard(key)
    
    def pipeline(self):
        return self.client.pipeline()
    
    def delete(self, key):
        self.client.delete(key)
    
    def expire(self, key, seconds=10):
        self.client.expire(key, seconds)