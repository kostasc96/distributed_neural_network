from io import BytesIO
from boto3 import client
from botocore.client import Config
from boto3.s3.transfer import TransferConfig

class S3Client:
    def __init__(self, endpoint, aws_access_key_id, aws_secret_access_key, is_secure=False, region_name="eu-west-1"):
        scheme = "https" if is_secure else "http"
        self.s3 = client(
            's3',
            endpoint_url=f"{scheme}://{endpoint}",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            config=Config(signature_version='s3v4'),
            region_name=region_name
        )
    
    def download_fileobj(self, bucket_name, file_name):
        buffer = BytesIO()
        config = TransferConfig(
            multipart_threshold=5 * 1024 * 1024,
            max_concurrency=20,
            multipart_chunksize=5* 1024 * 1024,
            use_threads=True
        )
        self.s3.download_fileobj(Bucket=bucket_name, Key=file_name, Fileobj=buffer, Config=config)
        buffer.seek(0)
        return buffer