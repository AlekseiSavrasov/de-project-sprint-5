# 1. Список необходимых полей

***id*** - автоинкремент, serial4  
***courier_id*** - /couriers (API)  
***courier_name*** - /couriers (API)

### Следующие поля сведем на основе объединения таблиц /couriers (API) и /deliveries (API): 
***settlement_year***  
***settlement_month***  
***orders_count***  
***rate_avg***

###Далее те, для которых нам понадобится таблица с полями (*order\_id*, *order\_sum*), которую мы можем создать на основе *fct\_product\_sales* из БД:
***orders\_total\_sum***  
***order\_processing\_fee***

Идея следующая: агрегируем таблицу фактов по полю order_id, получаем сумму каждого отдельного заказа, далее сделаем по ней join к основной табличке нашей витрины.

### И финальное - сколько мы должны заплатить курьеру?

***courier\_order\_sum*** - поле зависит от поля rate\_avg; важно, что для каждого отдельного заказа сумма будет считаться отдельно (т.е. нам понадобится сначала построить таблицу выплат за каждый заказ, а уже потом агрегировать ее и присоединить к основной витрине данных)  
***courier\_tips\_sum*** - чаевые /deliveries  
***courier\_reward\_sum*** - посчитаем по предыдущим полям


# 2. Список таблиц слоя DDS, необходимых для витрины

1. **couriers** *(id, courier\_id, courier\_name)* - напрямую из API
2. **couriers_rates** *(courier\_id, year, month, rate\_avg)*  - нужна /deliveries (API) (order\_id, courier\_id, delivery\_ts, rate)
3. **orders_couriers** *(order\_id, settlement\_year, settlement\_month, order\_sum, courier\_id, courier\_order\_sum, courier\_tips\_sum, courier\_reward\_sum)* - на основе объединения **/deliveries** (order\_id, courier\_id, delivery\_ts, rate, tip_sum) и **fct\_product\_sales** (order\_id, sum(total\_sum) as order\_sum); поле *courier\_order\_sum* здесь считается после присоеднинения таблицы **couriers\_rates** и проверки условий выплат по ней


# 3. Список сущностей и полей для загрузки из API
1. **couriers** *(id, courier\_id, courier\_name)*  (/couriers)
2. **deliveries** *(order\_id, courier\_id, delivery\_ts, rate, tip_sum)* (/deliveries)
