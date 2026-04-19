FROM apache/airflow:3.1.6

USER airflow

RUN pip install --no-cache-dir \
    dbt-postgres==1.10.0 \
    pyarrow \
    boto3 \
    apache-airflow-providers-amazon \
    apache-airflow-providers-postgres