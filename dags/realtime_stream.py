from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.current_weather import realtime_weather


def call_live_weather_realtime():
    realtime_weather()

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 8, 1, 20, 00),
}

with DAG('realtime_weather_stream',
        default_args=default_args,
         schedule_interval='*/15 * * * *',
        catchup=False) as dag:

    weather_task = PythonOperator(
        task_id='realtime_stream',
        python_callable=call_live_weather_realtime
    )
