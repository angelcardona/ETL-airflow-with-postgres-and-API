import requests
import pandas as pd
import os
import datetime
from sqlalchemy import create_engine, text
import psycopg2

# Variables directamente en el código
API_KEY = 'VOOUGD0S2L1WCGGE'
DB_USER = 'miguel'
DB_PASSWORD = 'simple123'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'finanzas'

def get_last_date():
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT MAX(date) FROM spx_data")).fetchone()
        return result[0] if result[0] else None

def fetch_data():
    last_date = get_last_date()

    if last_date:
        print(f"Ultima fecha para SPX {last_date}")
        output_size = "compact"
    else:
        print("No hay datos para SPY, descargando historico")
        output_size = "full"

    url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': 'SPY',
        'outputsize': output_size,
        'apikey': API_KEY
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"Error al conectar con la API, error {response.status_code}")

    data = response.json()
    df = pd.DataFrame.from_dict(data["Time Series (Daily)"], orient="index")

    df.reset_index(inplace=True)
    df.rename(columns={"index": "date"}, inplace=True)
    df['date'] = pd.to_datetime(df['date'])

    if last_date:
        df = df[df['date'] > pd.to_datetime(last_date)]

    return df

if __name__ == "__main__":
    df = fetch_data()
    print(df.head())
