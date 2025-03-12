import redis

class RedisHandler:
    def __init__(self, host: str, port: int, db):
        self.client = redis.Redis(host=host, port=port, db=db)

    def get_client(self):
        return self.client

    def set(self, key, value):
        self.client.set(key, value)

    def hset(self, hash_name, key, value):
        self.client.hset(hash_name, key, value)

    def get_all(self, key):
        return self.client.hgetall(key)

    def hgetall(self, hash_name):
        return self.client.hgetall(hash_name)

    def delete(self, key):
        self.client.delete(key)
