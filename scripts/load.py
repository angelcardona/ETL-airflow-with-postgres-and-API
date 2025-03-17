from scripts.extract import fetch_data
from scripts.transform import transform_data
import pandas as pd
from sqlalchemy import create_engine
import psycopg2




DB_USER = 'miguel'
DB_PASSWORD = 'simple123'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'finanzas'


engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def load_data(df):
    try:
        with engine.connect() as conn:
            df.to_sql('spx_data', con=conn, if_exists='append', index=False)
            print("Datos guardados exitosamente en PostgreSQL")
    except Exception as e:
        print(f"Error al guardar los datos: {e}")


if __name__ == '__main__':
    df = fetch_data()
    df_clean = transform_data(df)
    load_data(df_clean)
