import pandas as pd
from scripts.extract import fetch_data

def transform_data(df):
    try:
        # Renombrar columnas
        df = df.rename(columns={
            '1. open': 'open',
            '2. high': 'high',
            '3. low': 'low',
            '4. close': 'close',
            '5. volume': 'volume'
        })

        # Convertir columnas al tipo correcto
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(int)

        # Formato de fecha
        df['date'] = pd.to_datetime(df['date'])

    except Exception as e:
        print(f"Error durante la transformación: {e}")
        raise

    return df

if __name__ == '__main__':
    df = fetch_data()
    df_clean = transform_data(df)
    print(df_clean.head())
