import requests
import pandas as pd
import os

url="https://api.coingecko.com/api/v3/coins/markets"
params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 100,
    "page": 1,
    "sparkline": False
}

def extract_coin():
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    print(f"Error: {response.status_code}")
    return None
    
