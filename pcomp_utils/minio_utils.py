from minio import Minio

class MinioClient():
    def __init__(self, host, access_key, access_secret, is_secure=False):
        self.client = Minio(
            host,
            access_key=access_key,
            secret_key=access_secret,
            secure=is_secure
        )
    
    def get_object(self, bucket_name, file_name):
        return self.client.get_object(bucket_name, file_name)