from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.historical_weather import historical_weather


def fetch_historical_weather():
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)

    yesterday = start_date.strftime('%Y-%m-%d')
    formatted_end_date = end_date.strftime('%Y-%m-%d')

    historical_weather(start_date=yesterday, end_date=yesterday)


default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 8, 1, 20, 00),
    'timezone': 'Asia/Ho_Chi_Minh'
}

with DAG('weather_automation',
        default_args=default_args,
         schedule_interval='15 0 * * *',
        catchup=False) as dag:

    weather_task = PythonOperator(
        task_id='historical_stream',
        python_callable=fetch_historical_weather
    )

# weather_task >> clean_data>> transform_data >> store_data
