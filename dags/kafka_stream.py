from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.weather import live_weather

def call_live_weather():
    # This function will call live_weather with the specified arguments
    live_weather(start_date='2024-07-18', end_date='2024-07-20')

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 7, 17, 20, 00)
}

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    weather_task = PythonOperator(
        task_id='weather_stream',
        python_callable=call_live_weather
    )

# weather_stream >> clean_data >> transform_data >> store_data