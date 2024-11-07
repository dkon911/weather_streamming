from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.historical_weather import historical_weather


def fetch_historical_weather():
    get_date = datetime.now() - timedelta(days=1)

    yesterday = get_date.strftime('%Y-%m-%d')

    historical_weather(start_date=yesterday, end_date=yesterday)
    # historical_weather(start_date='2024-10-24', end_date='2024-10-25')


default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 8, 1, 20, 00),
    'timezone': 'Asia/Ho_Chi_Minh'
}

with DAG('historical_weather_stream',
        default_args=default_args,
         schedule_interval='15 0 * * *',
        catchup=False) as dag:

    weather_task = PythonOperator(
        task_id='historical_stream',
        python_callable=fetch_historical_weather
    )

# weather_task >> clean_data>> transform_data >> store_data
