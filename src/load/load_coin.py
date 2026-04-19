import os

def load_coin(df, filename="coin_data.parquet"):
    if df is None or df.empty:
        print("No data to save.")
        return
    
    data_folder = "/opt/airflow/data"
    os.makedirs(data_folder, exist_ok=True)
    file_path = os.path.join(data_folder, filename)

    df.to_parquet(file_path, index=False, engine='pyarrow')
    print(f"✅ Data saved to: {file_path}")
