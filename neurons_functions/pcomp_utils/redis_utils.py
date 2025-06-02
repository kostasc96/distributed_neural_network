import redis
import numpy as np

class RedisHandler:
    def __init__(self, host, port, db, max_con=5):
        pool = redis.ConnectionPool(host=host, port=port, db=db, max_connections=max_con)
        self.client = redis.Redis(connection_pool=pool)
    

    def set(self, key, value, to_bytes=True, ttl=15):
        if to_bytes:
            self.client.set(key, value.astype(np.float64).tobytes())
        else:
            self.client.set(key, value)
        self.client.expire(key, ttl)

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
    
    def get_batch_multi(self, keys, batch_size):
        results = self.client.mget(keys)
        arrays = []
        for res in results:
            if res is not None:
                arr = np.frombuffer(res, dtype=np.float64).reshape(batch_size, 1)
                arrays.append(arr)
            else:
                arrays.append(np.zeros((batch_size, 1), dtype=np.float64))
        return np.squeeze(np.stack(arrays, axis=1))

        
    def hset(self, key, field, value):
        formatted = format(value, '.17g') if isinstance(value, float) else str(value)
        self.client.hset(key, field, formatted)
    
    def hset_expiry(self, key, field, value, ttl):
        formatted = format(value, '.17g') if isinstance(value, float) else str(value)
        self.client.hset(key, field, formatted)
        self.client.expire(key, ttl)
    
    def hset_bulk(self, key, values):
        self.client.hset(key, mapping=values)

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
    
    def delete_batch_keys(self, batch_id):
        pattern = f"batch:{batch_id}:*"
        cursor = 0
        while True:
            cursor, keys = self.client.scan(cursor=cursor, match=pattern, count=1000)
            if keys:
                self.client.delete(*keys)
            if cursor == 0:
                break
    
    def delete_streams_keys(self, image_id):
        patterns = [
            f"streams:{image_id}:*",
            f"outputs:{image_id}:*"
        ]
        for pattern in patterns:
            cursor = 0
            while True:
                cursor, keys = self.client.scan(cursor=cursor, match=pattern, count=1000)
                if keys:
                    self.client.delete(*keys)
                if cursor == 0:
                    break
    
    def hdel(self, h, f):
        self.client.delete(h, f)
    
    def expire(self, key, seconds=10):
        self.client.expire(key, seconds)

    def store_neuron_output(self, key, neuron_id, output):
        formatted = format(output, '.17g') if isinstance(output, float) else str(output)
        self.client.hset(key, str(neuron_id), formatted)

    def hlen(self, key):
        return self.client.hlen(key)

        
    def exists(self, key):
        return self.client.exists(key)
    
    def incr(self, key):
        return self.client.incr(key)
    
    def pubsub(self):
        return self.client.pubsub()
    
    def close(self):
        self.client.close()