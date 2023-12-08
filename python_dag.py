from datetime import datetime
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

import json


locs = ["Lviv", "Kyiv", "Kharkiv", "Odesa", "Zhmerynka"]
coords = [("49.841952", "24.0315921"), ("50.4500336", "30.5241361"), ("49.9923181", "36.2310146"),
          ("46.4843023", "30.7322878"), ("49.0354593", "28.1147317")]


def _process_weather(**kwargs):
    info = kwargs["ti"].xcom_pull(kwargs["task_id"])
    timestamp = info["current"]["dt"]
    temp = info["current"]["temp"]
    humidity = info["current"]["humidity"]
    clouds = info["current"]["clouds"]
    wind_speed = info["current"]["wind_speed"]

    return kwargs["loc"], timestamp, temp, humidity, clouds, wind_speed


def date_macros(date):
    format_d = "%Y%m%dT%H%M%S"
    datetime_object = datetime.strptime(date, format_d)
    return int(datetime_object.timestamp())


with DAG(dag_id="weather_python_dag",
         schedule_interval="@daily",
         start_date=datetime(2023, 11, 30),
         catchup=True,
         user_defined_macros={
             'date_macros': date_macros
         }
         ) as dag:
    b_create = SqliteOperator(
        task_id="create_table_sqlite",
        sqlite_conn_id="airflow_conn",
        sql="""
     CREATE TABLE IF NOT EXISTS measures
     (
    loc TEXT, 
    timestamp TIMESTAMP,
    temp FLOAT,
    humidity FLOAT,
    clouds FLOAT,
    wind_speed FLOAT
     );"""
    )

    check_api = HttpSensor(
        task_id="check_api",
        http_conn_id="weather_conn",
        endpoint="data/3.0/onecall",
        request_params={"appid": Variable.get("key"),
                        "lat": coords[0][0],
                        "lon": coords[0][1]}
    )

    for loc_ind in range(len(locs)):


        extract_data = SimpleHttpOperator(
            task_id=f"extract_data_{loc_ind}",
            http_conn_id="weather_conn",
            endpoint="data/3.0/onecall",
            data={"appid": Variable.get("key"),
                  "lat": coords[loc_ind][0],
                  "lon": coords[loc_ind][1],
                  "dt": "{{ date_macros(ts_nodash) }}"},
            method="GET",
            response_filter=lambda x: json.loads(x.text),
            log_response=True
        )

        process_data = PythonOperator(
            task_id=f"process_data_{loc_ind}",
            python_callable=_process_weather,
            provide_context=True,
            op_kwargs={"loc": locs[loc_ind], "task_id": f"extract_data_{loc_ind}"},
        )

        inject_data = SqliteOperator(
            task_id=f"inject_data_{loc_ind}",
            sqlite_conn_id="airflow_conn",
            sql=f"""
            INSERT INTO measures (loc, timestamp,  temp, humidity, clouds, wind_speed) VALUES
            ('{{{{ ti.xcom_pull(task_ids='process_data_{loc_ind}')[0] }}}}', 
            {{{{ ti.xcom_pull(task_ids='process_data_{loc_ind}')[1] }}}}, 
            {{{{ ti.xcom_pull(task_ids='process_data_{loc_ind}')[2] }}}}, 
            {{{{ ti.xcom_pull(task_ids='process_data_{loc_ind}')[3] }}}}, 
            {{{{ ti.xcom_pull(task_ids='process_data_{loc_ind}')[4] }}}}, 
            {{{{ ti.xcom_pull(task_ids='process_data_{loc_ind}')[5] }}}});
            """
        )

        b_create >> check_api >> extract_data >> process_data >> inject_data
