from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

def upload_to_s3(file_path: str,bucket_name: str,aws_conn_id: str = "aws_default"):
    today=datetime.now().strftime("%Y-%m-%d")
    hook = S3Hook(aws_conn_id=aws_conn_id)
    
    hook.load_file(
        filename=file_path,
        bucket_name=bucket_name,
        key=f"raw/{today}/crypto_data.parquet",
        replace=True
    )
    
    print(f"File uploaded to s3://{bucket_name}/raw/{today}/crypto_data.parquet")
