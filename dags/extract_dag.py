from airflow.sdk import dag, task
import pendulum
from datetime import datetime
from extract.extract_coin import extract_coin
from transform.transform_coin import transform_coin
from load.load_coin import load_coin
from load_to_s3.load_s3 import upload_to_s3
from s3_to_postgre.load_to_postgre import load_parquet_s3_to_postgres


@dag(
    start_date=pendulum.datetime(2026, 2, 15, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False
)
def extract_dag():

    @task
    def extract():
        return extract_coin()

    @task
    def transform(coin_data):
        return transform_coin(coin_data)

    @task
    def load(transformed_data):
        load_coin(transformed_data)

    @task
    def upload():
        file_path = "/opt/airflow/data/coin_data.parquet"
        bucket_name = "crypto-coin-data-project"
        upload_to_s3(file_path, bucket_name)

    @task
    def load_to_postgre():
        today = datetime.now().strftime("%Y-%m-%d")

        s3_bucket = "crypto-coin-data-project"
        s3_key = f"raw/{today}/crypto_data.parquet"
        postgres_conn_id = "postgres_default"
        table_name = "staging.stg_coin_data"

        load_parquet_s3_to_postgres(
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            postgres_conn_id=postgres_conn_id,
            table_name=table_name
        )
        
    @task.bash
    def bronze_layer():
        return """
             cd /opt/airflow/coin_data_project &&
            dbt clean &&
            dbt deps &&
            dbt run --target docker --select bronze_coin
            """
            
    @task.bash
    def silver_layer():
        return """
             cd /opt/airflow/coin_data_project &&
            dbt clean &&
            dbt deps &&
            dbt run --target docker --select silver_coin
            """
    @task.bash
    def gold_layer():
        return """
             cd /opt/airflow/coin_data_project &&
            dbt clean &&
            dbt deps &&
            dbt run --target docker --select gold
            """

    coin_data = extract()
    transformed_data = transform(coin_data)
    load_task = load(transformed_data)
    upload_task = upload()
    load_to_postgre_task = load_to_postgre()
    bronze_layer_task = bronze_layer()
    silver_layer_task = silver_layer()
    gold_layer_task = gold_layer()

    load_task >> upload_task >> load_to_postgre_task >> bronze_layer_task >> silver_layer_task >> gold_layer_task


extract_dag()