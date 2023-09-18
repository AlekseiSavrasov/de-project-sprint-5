#Задаем структуру витрины
```SQL
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
```

#Проектируем структуру dds слоя
```SQL
create table if not exists dds.couriers(
        id serial primary key,
        courier_id text not null,
        courier_name text not null
);

create table if not exists dds.couriers_rates(
        courier_id text not null,
        year int4 not null,
        month int4 not null,
        rate_avg numeric(4,3) not null
);

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
```

#Проектируем структуру STG-слоя
```SQL
create table if not exists stg.couriers(
id serial primary key,
object_value text
);

create table if not exists stg.deliveries(
id serial primary key,
object_value text
);
```
