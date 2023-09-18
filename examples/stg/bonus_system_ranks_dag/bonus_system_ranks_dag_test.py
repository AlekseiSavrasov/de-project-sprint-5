import logging
import pandas as pd

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)


def get_ranks_data():
    sql = "SELECT * FROM public.ranks;"
    pg_hook = PostgresHook(
        postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION'
    )
    pg_conn = pg_hook.get_conn()
    df = pd.read_sql(sql, pg_conn)
    print(df)
    return df
def process_ranks_data():
    df = get_ranks_data()
    pg_hook = PostgresHook(
        postgres_conn_id='PG_WAREHOUSE_CONNECTION'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    for index, row in df.iterrows():
        print(row['name'])
        cursor.execute("INSERT INTO stg.bonussystem_ranks (id, name, bonus_percent, min_payment_threshold) VALUES (" + str(row.id) + ",'" + str(row['name']) + "'," + str(row.bonus_percent) + "," + str(row.min_payment_threshold) + ")")
    pg_conn.commit()
def get_users_data():
    sql = "SELECT * FROM public.users;"
    pg_hook = PostgresHook(
        postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION'
    )
    pg_conn = pg_hook.get_conn()
    df = pd.read_sql(sql, pg_conn)
    print(df)
    return df
def process_users_data():
    df = get_users_data()
    pg_hook = PostgresHook(
        postgres_conn_id='PG_WAREHOUSE_CONNECTION'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    for index, row in df.iterrows():
        print(row['order_user_id'])
        cursor.execute("INSERT INTO stg.bonussystem_users (id, order_user_id) VALUES (" + str(row.id) + ",'" + str(row['order_user_id']) + "')")
    pg_conn.commit()

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def insert_ranks_and_users_data_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="load_ranks_and_users")
    def load_ranks_and_users():  # Вызываем функцию, которая перельет данные.
        process_ranks_data()
        process_users_data()
    
    # Инициализируем объявленные таски.
    ranks_and_users_dict = load_ranks_and_users()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    ranks_and_users_dict  # type: ignore

stg_ranks_and_users_dag = insert_ranks_and_users_data_dag()
