from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import boto3

def download_lidar_from_s3(bucket_name, object_key, local_path):
    s3 = boto3.client('s3')
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3.download_file(bucket_name, object_key, local_path)
    print(f"Downloaded {object_key} to {local_path}")

def dummy_pdal_clean(path):
    # Placeholder for PDAL pipeline call
    print(f"Pretending to clean LiDAR file at: {path}")

def upload_processed_to_s3(local_path, bucket_name, object_key):
    s3 = boto3.client('s3')
    s3.upload_file(local_path, bucket_name, object_key)
    print(f"Uploaded {local_path} to s3://{bucket_name}/{object_key}")

with DAG(
    dag_id="lidar_etl_pipeline",
    start_date=datetime(2025, 3, 29),
    schedule_interval=None,
    catchup=False,
    tags=["lidar", "etl"]
) as dag:

    download = PythonOperator(
        task_id="download_lidar",
        python_callable=download_lidar_from_s3,
        op_kwargs={
            "bucket_name": "lidarprocessing-bucket",
            "object_key": "raw/sample.laz",
            "local_path": "/tmp/sample.laz"
        }
    )

    clean = PythonOperator(
        task_id="clean_lidar",
        python_callable=dummy_pdal_clean,
        op_kwargs={"path": "/tmp/sample.laz"}
    )

    upload = PythonOperator(
        task_id="upload_processed",
        python_callable=upload_processed_to_s3,
        op_kwargs={
            "local_path": "/tmp/sample.laz",
            "bucket_name": "lidarprocessing-bucket",
            "object_key": "processed/sample.laz"
        }
    )

    download >> clean >> upload