-- Databricks notebook source
insert into auto_sales values (10, 10, 'wheel', 300, current_date());
insert into auto_sales values (11, 11, 'tire', 100, current_date());
insert into auto_sales values (10, 10, 'wheel', 300, current_date());
insert into auto_sales values (10, 10, 'wheel', 300, current_date());
insert into auto_sales values (10, 10, 'wheel', 300, current_date());
insert into auto_sales values (10, 10, 'wheel', 300, current_date());
insert into auto_sales values (10, 10, 'wheel', 300, current_date());
insert into auto_sales values (10, 10, 'wheel', 300, current_date());

-- COMMAND ----------

select * from auto_sales

-- COMMAND ----------

describe history auto_sales;

-- COMMAND ----------

delete from auto_sales where ord_no = 10
