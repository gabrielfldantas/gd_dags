from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime
from utils import load_cities, dict_cities

with DAG(
    schedule_interval='*/15 * * * *', 
    start_date=datetime(2025, 7, 20),
    dag_id='weather_etl',
    catchup=False,
    description='ETL pipeline to extract weather data from Europe cities.',
    tags=['weather']
) as dag:

    get_cities = PythonOperator(
        task_id="get_cities",
        python_callable=load_cities
    )

    build_dict_cities = PythonOperator(
        task_id="dict_cities",
        python_callable=dict_cities
    )

    get_cities >> build_dict_cities

