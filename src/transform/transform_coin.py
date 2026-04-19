import pandas as pd

def transform_coin(data):
    if not data:
        print("No data to transform.")
        return None
    df=pd.DataFrame(data)
    df=df[["id","symbol","name","current_price","market_cap","market_cap_rank","total_volume"]]
    return df