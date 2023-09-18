import requests
import json
import psycopg2
from psycopg2 import OperationalError
import datetime
from datetime import date
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pendulum
from airflow.decorators import dag, task

pg_hook = PostgresHook(
        postgres_conn_id='PG_WAREHOUSE_CONNECTION'
    )
conn = pg_hook.get_conn()

def import_couriers():
    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists stg.couriers(
            id serial primary key,
            object_value text
            );
            DELETE FROM stg.couriers;
            """
            )
    sort_field = 'id'
    sort_direction = 'asc'
    limit = 50
    offset = 0
    api_key = '25c27781-8fde-4b30-a22e-524044a7580f'
    while 1==1:
        url = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field={sort_field}\
        &sort_direction={sort_direction}&limit={limit}&offset={offset}'
        headers = {'X-Nickname': 'aleksei_savrasov',
                  'X-Cohort': '15',
                  'X-API-KEY': api_key}
        response = requests.get(url, headers=headers)
            
        if len(response.json()) > 0:
            for i in response.json():
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO stg.couriers(object_value
                        )
                        VALUES('{json.dumps(i, ensure_ascii=False).encode('utf8').decode()}')
                        ;"""
                    )
            offset+=50
        else:
            break
            
def import_deliveries():
    start_date = pd.Timestamp(date.today() - datetime.timedelta(days=7))
    end_date = pd.Timestamp(date.today())
    cursor = conn.cursor()
    #Берем из таблицы уникальные заказы
    orders_list_in_db = []
    with conn.cursor() as cur:
        df = pd.read_sql("""
                SELECT DISTINCT object_value FROM stg.deliveries;""", conn)
        for i in df['object_value']:
            i = json.loads(i)
            orders_list_in_db.append(i)
        if orders_list_in_db is None:
            orders_list_in_db = []
        else:
            pass
    #Забираем из внешней БД список заказов за последние 7 дней, кладем в таблицу stg
    sort_field = 'id'
    sort_direction = 'asc'
    limit = 50
    offset = 0
    api_key = '25c27781-8fde-4b30-a22e-524044a7580f'

    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists stg.deliveries(
            id serial primary key,
            object_value text
            );"""
            )
    while 1==1:
        url = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}&from={start_date}&to={end_date}'        
        headers = {'X-Nickname': 'aleksei_savrasov',
                  'X-Cohort': '15',
                  'X-API-KEY': api_key}

        response = requests.get(url, headers=headers)
        
        if len(response.json()) > 0:
            for i in response.json():
                if i not in orders_list_in_db:
                    cursor.execute(
                    f"""
                    INSERT INTO stg.deliveries(object_value
                    )
                    VALUES('{json.dumps(i, ensure_ascii=False).encode('utf8').decode()}')
                    ;""")
                elif i in orders_list_in_db:
                    pass
            offset+=50
        else:
            break            
        


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'project', 'yandex-cloud'],
    is_paused_upon_creation=False
)
def project_couriers_and_deliveries_dag():
    @task()
    def couriers_task():
        import_couriers()
    couriers_api = couriers_task()
    couriers_api  # type: ignore
    
    @task()
    def deliveries_task():
        import_deliveries()
    deliveries_api = deliveries_task()
    deliveries_api #type: ignore

project_couriers_and_deliveries_dag = project_couriers_and_deliveries_dag()
