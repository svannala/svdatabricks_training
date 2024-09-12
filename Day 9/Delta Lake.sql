-- Databricks notebook source
create table auto_sales(ord_no integer, part_no  integer, part_desc string, quantity integer, order_date date) --location '/mnt/training/auto_sales';

-- COMMAND ----------

select * from auto_sales;

-- COMMAND ----------

describe history auto_sales;

-- COMMAND ----------

--insert into auto_sales values(1, 1, 'motor', 3, current_date());
insert into auto_sales values(2, 2, 'brake pad', 10, current_date());
insert into auto_sales values(3, 3, 'disc', 8, current_date());
insert into auto_sales values(4, 4, 'cutter', 3, current_date());


-- COMMAND ----------

delete from auto_sales where ord_no = 3;

-- COMMAND ----------

select * from auto_sales version as of 5
