import boto3
import pandas as pd
import os
import io

def main():
    # Environment variables
    s3_endpoint = os.environ.get('S3_URL')
    s3_access_key = os.environ.get('S3_ACCESS_KEY')
    s3_secret_key = os.environ.get('S3_SECRET_KEY')
    bucket_name = os.environ.get('S3_BUCKET')
    region = os.environ.get('S3_REGION')

    # Create S3 client
    s3 = boto3.client(
        's3',
        endpoint_url=s3_endpoint,
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
        region_name=region
    )

    # 1. Load the ground-truth labels
    response = s3.get_object(Bucket=bucket_name, Key='labels.csv')
    labels_data = response['Body'].read()
    labels = pd.read_csv(io.BytesIO(labels_data))

    # 2. List and read all prediction CSVs
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix='predictions/')
    
    dfs = []
    for page in page_iterator:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv'):
                resp = s3.get_object(Bucket=bucket_name, Key=key)
                data = resp['Body'].read()
                df = pd.read_csv(io.BytesIO(data))
                dfs.append(df)

    # 3. Combine all predictions
    predictions = pd.concat(dfs, ignore_index=True)

    # 4. Merge and compute accuracy
    merged = labels.merge(predictions, on='image_id')
    accuracy = (merged['label'] == merged['prediction']).mean()

    # 5. Output result
    print(f'Overall accuracy: {accuracy:.2%}')

if __name__ == '__main__':
    main()
