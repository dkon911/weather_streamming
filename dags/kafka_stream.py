from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.weather import live_weather


def call_live_weather():
    # Calculate start_date and end_date dynamically
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)

    # Format dates as strings if required by live_weather
    formatted_start_date = start_date.strftime('%Y-%m-%d')
    formatted_end_date = end_date.strftime('%Y-%m-%d')

    live_weather(start_date=formatted_start_date, end_date=formatted_start_date)


default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 7, 22, 20, 00)
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