# Проект 5-го спринта

### Описание
К основному хранилищу, которое мы строили в заданиях спринта в рамках проекта добавлены:  
1) Файл **tables_DDL.md** с DDL-запросами для таблиц  
2) Файл **api_entities.md** с описанием всех таблиц и полей, которые нам необходимы в проекте (+ в этом же файле можно проследить логику построения витрины и сопутствующих таблиц)  
3) Даги к проекту в папке *dags/projects_dags/*, разбитые на **stg**, **dds** и **cdm** слои   
***
  
P.s. обратите внимание, что в файле **api_entities.md** теоретическое описание таблиц сделано под случай, описанный в задании страницы практикума (с полями таблиц, указанными там); в таком случае нам пришлось бы адресоваться к таблицам из созданной ранее БД.  
На практике реализация проще, т.к. в таблицах, которые мы тянем по API присутствует большее число полей (в частности, суммы и соответствия заказ-курьер). Я воспользовался этими дополнительными полями, когда создавал DAGs.