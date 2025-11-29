from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import pendulum

# Config
LATITUDE = 51.5074
LONGITUDE = -0.1278
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.now("UTC").subtract(days=1)
}

with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    schedule='@daily',  # instead of schedule_interval
    catchup=False,
    tags=['etl', 'weather']
) as dag:

    @task()
    def extract_weather_data():
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')

        ## Build the API endpoint
        ## https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")

    @task()
    def transform_weather_data(weather_data: dict):
        current = weather_data.get('current_weather', {})
        return {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current.get('temperature'),
            'windspeed': current.get('windspeed'),
            'winddirection': current.get('winddirection'),
            'weathercode': current.get('weathercode')
        }

    @task()
    def load_weather_data(data: dict):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            data['latitude'],
            data['longitude'],
            data['temperature'],
            data['windspeed'],
            data['winddirection'],
            data['weathercode']
        ))

        conn.commit()
        cursor.close()
        conn.close()

    # DAG flow
    weather = extract_weather_data()
    transformed = transform_weather_data(weather)
    load_weather_data(transformed)
