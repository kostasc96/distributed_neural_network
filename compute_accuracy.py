from minio import Minio
import pandas as pd

def main():
    # 1. Connect to your local MinIO server
    client = Minio(
        'localhost:9000',
        access_key='admin',
        secret_key='admin123',
        secure=False
    )

    # 2. Load the ground-truth labels
    response = client.get_object('faasnn-bucket', 'labels.csv')
    labels = pd.read_csv(response)

    # 3. Read and concatenate all CSVs in 'predictions/' folder
    objects = client.list_objects('my-bucket', prefix='predictions/')
    dfs = []
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            resp = client.get_object('my-bucket', obj.object_name)
            dfs.append(pd.read_csv(resp))
    predictions = pd.concat(dfs, ignore_index=True)

    # 4. Merge and compute accuracy
    merged = labels.merge(predictions, on='image_id')
    accuracy = (merged['label'] == merged['prediction']).mean()

    # 5. Output result
    print(f'Overall accuracy: {accuracy:.2%}')

if __name__ == '__main__':
    main()
