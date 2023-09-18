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

def dds_couriers_insert():
    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists dds.couriers(
                    id serial primary key,
                    courier_id text not null,
                    courier_name text not null
            );
            delete from dds.couriers;
            insert into dds.couriers(courier_id, courier_name)
            select 
            object_value::json->>'_id',
            object_value::json->>'name'
            from stg.couriers;
            """
            )
def dds_couriers_rates_insert():
    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists dds.couriers_rates(
                    courier_id text not null,
                    year int4 not null,
                    month int4 not null,
                    rate_avg numeric(4,3) not null
            );
            delete from dds.couriers_rates;
            insert into dds.couriers_rates(month, year, courier_id, rate_avg)
            select 
                    month,
                    year,
                    courier_id,
                    round(avg(rate)::numeric, 2) as rate_avg
            from
                    (select 
                            extract(month from cast(object_value::json->>'order_ts' as date)) as month,
                            extract(year from cast(object_value::json->>'order_ts' as date)) as year,
                            object_value::json->>'courier_id' as courier_id,
                            (object_value::json->>'rate')::numeric as rate
                    from stg.deliveries d 
                    ) data_for_table
            group by 
                    month,
                    year,
                    courier_id;
            """
            )
def dds_orders_couriers():
    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists dds.orders_couriers(
                    order_id			text,
                    settlement_year		int4, 
                    settlement_month	int4, 
                    order_sum			numeric(19,5), 
                    courier_id			text,
                    courier_order_sum	numeric(19,5),
                    courier_tips_sum	numeric(19,5),
                    courier_reward_sum	numeric(19,5)
                    );
            delete from dds.orders_couriers;
            insert into dds.orders_couriers
            select 
                    order_id,
                    settlement_year,
                    settlement_month,
                    order_sum,
                    d.courier_id,
                    case 
                            when rate_avg < 4 then greatest(0.05*order_sum, 100)
                            when rate_avg < 4.5 then greatest(0.07*order_sum, 150)
                            when rate_avg < 4.9 then greatest(0.08*order_sum, 175)
                            when rate_avg <= 5 then greatest(0.1*order_sum, 200)
                    end as courier_order_sum,
                    courier_tips_sum,
                    ((case 
                            when rate_avg < 4 then greatest(0.05*order_sum, 100)
                            when rate_avg < 4.5 then greatest(0.07*order_sum, 150)
                            when rate_avg < 4.9 then greatest(0.08*order_sum, 175)
                            when rate_avg <= 5 then greatest(0.1*order_sum, 200)
                    end)+courier_tips_sum)*0.95 as reward_sum
            from
                    (select 
                            object_value::json->>'order_id'	as order_id,
                            extract(year from cast(object_value::json->>'order_ts' as date)) as settlement_year,
                            extract(month from cast(object_value::json->>'order_ts' as date)) settlement_month,
                            (object_value::json->>'sum')::numeric	as order_sum,
                            object_value::json->>'courier_id'	as courier_id,
                            (object_value::json->>'tip_sum')::numeric	as courier_tips_sum
                    from stg.deliveries
                    ) d
            left join dds.couriers_rates cr
                    on d.settlement_year = cr.year and
                    d.settlement_month = cr.month and
                    d.courier_id = cr.courier_id;
            """
            )

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'project', 'yandex-cloud'],
    is_paused_upon_creation=False
)
def project_tables_to_dds_dag():
    @task()
    def couriers_task():
        dds_couriers_insert()
    couriers_dds = couriers_task()
    # type: ignore
    
    @task()
    def rates_task():
        dds_couriers_rates_insert()
    rates_dds = rates_task() #type: ignore

    @task()
    def orders_couriers_task():
        dds_orders_couriers()
    orders_couriers_dds = dds_orders_couriers() #type: ignore

#[couriers_dds, rates_dds] >> orders_couriers_dds

project_tables_to_dds_dag = project_tables_to_dds_dag()
