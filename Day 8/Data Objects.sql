-- Databricks notebook source
use catalog teqdata;
use suresh_schema;

-- COMMAND ----------

create table teqdata.suresh_schema.department(dno integer, dname string);

-- COMMAND ----------

insert into department values(1,'Finance'), (2, 'Marketing'), (3, 'Information Technology')

-- COMMAND ----------

select * from department

-- COMMAND ----------

-- DBTITLE 1,Standard View
create view dept_std_view as select * from teqdata.suresh_schema.department where dno=1;

-- COMMAND ----------

select * from dept_std_view;

-- COMMAND ----------

-- DBTITLE 1,Temp View
create temp view dept_temp_view as select * from department where dno = 3;

-- COMMAND ----------

select * from dept_temp_view;

-- COMMAND ----------

show views

-- COMMAND ----------

select * from teqdata.naval_schema.emp_temp_view;
