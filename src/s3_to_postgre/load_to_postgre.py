from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import pandas as pd
import tempfile


def load_parquet_s3_to_postgres(
    s3_bucket: str,
    s3_key: str,
    postgres_conn_id: str,
    table_name: str
):

    s3_hook = S3Hook(aws_conn_id="aws_default")
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    obj=s3_hook.get_key(key=s3_key, bucket_name=s3_bucket)
    file_path=obj.get()['Body'].read()
    with tempfile.NamedTemporaryFile(suffix=".parquet") as parquet_file:
      parquet_file.write(file_path)
      parquet_file.flush()

      df = pd.read_parquet(parquet_file.name)
      df["created_at"] = datetime.now()
      with tempfile.NamedTemporaryFile(suffix=".csv") as csv_file:
        df.to_csv(csv_file.name, index=False)

        print("Converted to CSV")

        pg_hook.copy_expert(
            sql=f"""
                COPY {table_name}(
                    id,
                    symbol,
                    name,
                    current_price,
                    market_cap,
                    market_cap_rank,
                    total_volume,
                    created_at
                )
                FROM STDIN
                WITH CSV HEADER
            """,
            filename=csv_file.name
        )

        print("Data successfully loaded using COPY")