
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.extract import fetch_data
from scripts.transform import transform_data
from scripts.load import load_data

default_args = {
    'owner': 'miguel',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='etl_spy_postgres',
    default_args=default_args,
    description='ETL para SPY directo a PostgreSQL',
    schedule_interval='0 9 * * 1-5',  # Lunes a viernes a las 9 AM
    start_date=datetime(2024, 3, 17),
    catchup=False,
    tags=['spy', 'alpha_vantage', 'postgres']
) as dag:

    def extract_task(**context):
        df = fetch_data()
        context['ti'].xcom_push(key='extracted_data', value=df.to_json())
    
    def transform_task(**context):
        df_json = context['ti'].xcom_pull(key='extracted_data')
        df = pd.read_json(df_json)
        df_clean = transform_data(df)
        context['ti'].xcom_push(key='transformed_data', value=df_clean.to_json())

    def load_task(**context):
        df_json = context['ti'].xcom_pull(key='transformed_data')
        df_clean = pd.read_json(df_json)
        load_data(df_clean)

    extract = PythonOperator(
        task_id='extract_spy_data',
        python_callable=extract_task,
        provide_context=True
    )

    transform = PythonOperator(
        task_id='transform_spy_data',
        python_callable=transform_task,
        provide_context=True
    )

    load = PythonOperator(
        task_id='load_spy_data',
        python_callable=load_task,
        provide_context=True
    )

    extract >> transform >> load
