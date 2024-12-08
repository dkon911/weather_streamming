from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from scripts.historical_weather import historical_weather

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 8, 1),
    'timezone': 'Asia/Ho_Chi_Minh'
}

def fetch_historical_weather():
    get_date = datetime.now() - timedelta(days=1)
    yesterday = get_date.strftime('%Y-%m-%d')
    historical_weather(start_date=yesterday, end_date=yesterday)

load_cassandra_command = 'spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.postgresql:postgresql:42.7.4 load_to_DW.py'

# Define the DAG
with DAG('historical_weather_stream',
         default_args=default_args,
         schedule_interval='15 0 * * *',
         catchup=False) as dag:

    weather_task = PythonOperator(
        task_id='historical_stream',
        python_callable=fetch_historical_weather
    )

    # load_cassandra_task = BashOperator(
    #     task_id='load_cassandra_to_postgres',
    #     bash_command=load_cassandra_command
    # )

    # weather_task >> load_cassandra_task
