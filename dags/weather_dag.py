from airflow import DAG
from datetime import datetime, timezone, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
import pandas as pd
from airflow.operators.python import PythonOperator
import os
from dotenv import load_dotenv
import boto3
from io import StringIO
import pendulum

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
env_path = os.path.join(project_root, ".env")
load_dotenv(env_path)

OPEN_WEATHER_API_KEY = os.getenv("OPEN_WEATHER_API_KEY")

kh_time = pendulum.timezone("Asia/Phnom_Penh")

def setup_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    description = data["weather"][0]["description"]
    temp = data["main"]["temp"]
    temp_feel_like = data["main"]["feels_like"]
    temp_min = data["main"]["temp_min"]
    temp_max = data["main"]["temp_max"]
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_record = datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(kh_time)
    time_sunrise = datetime.utcfromtimestamp(data["sys"]["sunrise"]).replace(tzinfo=timezone.utc).astimezone(kh_time)
    time_sunset = datetime.utcfromtimestamp(data["sys"]["sunset"]).replace(tzinfo=timezone.utc).astimezone(kh_time)


    # convert to time

    time_record_str = time_record.strftime("%Y-%m-%d %H:%M:%S")
    time_sunrise_str = time_sunrise.strftime("%Y-%m-%d %H:%M:%S")
    time_sunset_str = time_sunset.strftime("%Y-%m-%d %H:%M:%S")
    tranformed_data = {
        "City": city,
        "Description": description,
        "Temp (C)": temp,
        "Temp_feel_like (C)": temp_feel_like,
        "Temp_min (C)": temp_min,
        "Temp_max (C)": temp_max,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind_speed": wind_speed,
        "Time_record": time_record_str,
        "Time_sunrise": time_sunrise_str,
        "Time_sunset": time_sunset_str,
    }
    df_data = pd.DataFrame([tranformed_data])

    csv_buffer = StringIO()
    df_data.to_csv(csv_buffer, index=False)

    # Create S3 client using env credentials
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN")  # optional
    )

    dt_string = datetime.now().strftime("%d%m%Y%H%M%S")
    file_name = f"current_weather_data_phnompenh_{dt_string}.csv"

    s3_client.put_object(
        Bucket="openweatherapiairflowdata",
        Key=file_name,
        Body=csv_buffer.getvalue()
    )

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


with DAG('weather_dag',
        default_args=default_args,
        schedule_interval='0 10 * * *', 
        catchup=False) as dag:

        is_weather_api_ready = HttpSensor(
        task_id ='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint=f'/data/2.5/weather?lat=11.5564&lon=104.9282&units=metric&appid={OPEN_WEATHER_API_KEY}'

        )

        extract_weather_data = SimpleHttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'weathermap_api',
        endpoint=f'/data/2.5/weather?lat=11.5564&lon=104.9282&units=metric&appid={OPEN_WEATHER_API_KEY}',
        method = 'GET',
        response_filter= lambda data: json.loads(data.text),
        log_response=True
        )

        transform_load_weather_data = PythonOperator(
        task_id= 'setup_load_weather_data',
        python_callable=setup_load_data
        )

        is_weather_api_ready >> extract_weather_data >> transform_load_weather_data