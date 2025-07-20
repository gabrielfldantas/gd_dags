from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime
import requests as req

def load_cities() -> list:
    """
    Call an URL to load a JSON file with city data.
    """

    url = "https://www.jsonkeeper.com/b/L3YT"

    try:
        response = req.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except req.RequestException as e:
        print(f"Error fetching cities: {e}")
        return []

def dict_cities(ti) -> dict:
    """
    Convert a list of city dictionaries into a dictionary indexed by city ID.
    """
    cities = ti.xcom_pull(task_ids='get_cities')
    
    d_cities = {}
    for city in cities:
        d_cities[city["id"]] = city
    return d_cities

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

