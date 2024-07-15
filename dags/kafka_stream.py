from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.weather import live_weather
from scripts.user import users_stream_data

def call_live_weather():
    # This function will call live_weather with the specified arguments
    live_weather(start_date='2024-07-13', end_date='2024-07-14')

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 7, 9, 20, 00)
}

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    #
    # user_task = PythonOperator(
    #     task_id='stream_data_from_api',
    #     python_callable=users_stream_data
    # )

    weather_task = PythonOperator(
        task_id='weather_stream',
        python_callable=call_live_weather
    )

# [weather_task, user_task]