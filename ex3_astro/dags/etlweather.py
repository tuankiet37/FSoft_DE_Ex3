from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import pandas as pd
import requests
import json
from airflow.datasets import Dataset


POSTGRES_CONN_ID='postgres_default'
API_CONN_ID='open_meteo_api'

default_args={
    'owner':'airflow',
    'start_date':days_ago(1)
}

def data_vn_city():
    df = pd.read_csv('./data/worldcities.csv')
    df = df[df['iso2'] == 'VN'][['city_ascii', 'lat', 'lng']]
    df.rename(columns={'city_ascii': 'city', 'lng' : 'long'}, inplace=True)
    return df


with DAG(dag_id='weather_etl_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dags:
    

    @task()
    def extract_weather_data():

        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        cities_df = data_vn_city()
        all_weather_data = []
        
        for _, city in cities_df.iterrows():
            endpoint = f'/v1/forecast?latitude={city["lat"]}&longitude={city["long"]}&current_weather=true'
            response = http_hook.run(endpoint)
            
            if response.status_code == 200:
                weather_data = response.json()
                weather_data['city'] = city['city']
                all_weather_data.append(weather_data)
            else:
                raise Exception(f"Failed to fetch weather data: {response.status_code}")
                
        return all_weather_data

    @task()
    def transform_weather_data(weather_data_list):
        """Transform the extracted weather data for all cities."""
        transformed_data = []
        for weather_data in weather_data_list:
            current_weather = weather_data['current_weather']
            transformed_data.append({
                'city': weather_data['city'],
                'latitude': weather_data['latitude'],
                'longitude': weather_data['longitude'],
                'temperature': current_weather['temperature'],
                'windspeed': current_weather['windspeed'],
                'winddirection': current_weather['winddirection'],
                'weathercode': current_weather['weathercode']
            })
        return transformed_data

    @task()
    def load_weather_data(transformed_data_list):
        """Load transformed data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            city VARCHAR(100),
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        for data in transformed_data_list:
            cursor.execute("""
            INSERT INTO weather_data (city, latitude, longitude, temperature, windspeed, winddirection, weathercode)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                data['city'],
                data['latitude'],
                data['longitude'],
                data['temperature'],
                data['windspeed'],
                data['winddirection'],
                data['weathercode']
            ))

        conn.commit()
        cursor.close()

    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)