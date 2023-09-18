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

def cdm_insert():
    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists cdm.dm_courier_ledger(
                    id serial primary key,
                    courier_id text not null,
                    courier_name text,
                    settlement_year		int4, 
                    settlement_month	int4, 
                    orders_count		int4,
                    orders_total_sum	numeric(19,5),
                    rate_avg			numeric(4,3),
                    order_processing_fee numeric(19,5),
                    couriers_order_sum	numeric(19,5),
                    couriers_tips_sum	numeric(19,5),
                    couriers_reward_sum	numeric(19,5)
            );
            delete from cdm.dm_courier_ledger;
            insert into cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month,
                    orders_count, orders_total_sum, rate_avg, order_processing_fee, couriers_order_sum, couriers_tips_sum,
                    couriers_reward_sum)
            select 
                    oc.courier_id,
                    courier_name,
                    settlement_year,
                    settlement_month,
                    count(order_id) as orders_count,
                    sum(order_sum) as orders_total_sum,
                    rate_avg,
                    sum(order_sum)*0.25 as order_processing_fee,
                    sum(courier_order_sum) as courier_order_sum,
                    sum(courier_tips_sum) as courier_tips_sum,
                    sum(courier_reward_sum) as courier_reward_sum
            from dds.orders_couriers oc
            left join dds.couriers c
                    on c.courier_id = oc.courier_id 
            left join dds.couriers_rates cr
                    on cr.courier_id = oc.courier_id 
            group by
                    oc.courier_id,
                    courier_name,
                    settlement_year,
                    settlement_month,
                    rate_avg
            """
            )

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'project', 'yandex-cloud'],
    is_paused_upon_creation=False
)
def project_tables_to_cdm_dag():
    @task()
    def cdm_task():
        cdm_insert()
    cdm_result = cdm_task()

project_tables_to_cdm_dag = project_tables_to_cdm_dag()
