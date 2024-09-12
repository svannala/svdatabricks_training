# Databricks notebook source
Schema

1. Str (simple)
2. pyspark types (complex)

# COMMAND ----------

data=[{"id":1,"name":"naval","mo":123}]
schema="id int, name string, mo int"
df=spark.createDataFrame(data,schema)

# COMMAND ----------

df.display()

# COMMAND ----------

data1=[{"id":1,"name":"naval","mo":[123,7700]}]
schema1="id int, name string, mo array"
df1=spark.createDataFrame(data1,schema1)

# COMMAND ----------

pyspark data types
1. Struct Type
2. Array Type
3. Map Type

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

data1=[{"id":1,"name":"naval","mo":[123,7700]}]
schema_pyspark=StructType([StructField("id",IntegerType()),
                           StructField("name",StringType()),
                           StructField("mo",ArrayType(IntegerType()))
])
df1=spark.createDataFrame(data1,schema_pyspark)

# COMMAND ----------

display(df1)

# COMMAND ----------

import datetime
users=[{"id":1, 
       "first_name":'Naval',
       "last_name":'Yemul',
        "mail":'navalyemul@mail.com',
        "phone_numbers": ["+1 234 567 789 ", "+91 123987645"],
        "is_customer":True,
        "amount_paid":1000.2,
        "customer_from":datetime.date(2002,12,31),
        "last_update_ts":datetime.date(2022,1,1)
       }, 
       {"id":2, 
       "first_name":'John',
       "last_name":'Players',
        "mail":'john@mail.com',
        "phone_numbers": ["+1 789 567 234 ", "+91 124137645"],
        "is_customer":True,
        "amount_paid":2000.4,
        "customer_from":datetime.date(2004,5,30),
        "last_update_ts":datetime.date(2022,11,29)
       },
       {"id":3, 
       "first_name":'Killer',
       "last_name":'Spykar',
        "mail":'killer@mail.com',
        "phone_numbers": [" ", "+91 124137645"],
        "is_customer":True,
        "amount_paid":3000.0,
        "customer_from":datetime.date(2016,3,18),
        "last_update_ts":datetime.date(2020,3,24)
       },
       {"id":4, 
       "first_name":'Levis',
       "last_name":'Jeans',
        "mail":'jeans@mail.com',
        "phone_numbers": ["+1 789 567 234 ", " "],
        "is_customer":True,
        "amount_paid":1500.5,
        "customer_from":datetime.date(1990,12,3),
        "last_update_ts":datetime.date(2022,3,2)
       },
       {"id":5, 
       "first_name":'Puma',
       "last_name":'Adidas',
        "mail":'puma@mail.com',
        "phone_numbers": [" ", " "],
        "is_customer":True,
        "amount_paid":500.5,
        "customer_from":datetime.date(1995,2,19),
        "last_update_ts":datetime.date(2022,1,21)
       }
       
]

# COMMAND ----------

schema_pyspark_json=StructType([StructField("id",IntegerType()),
                           StructField("first_name",StringType()),
                           StructField("last_name",StringType()),
                           StructField("mail",StringType()),
                           StructField("phone_numbers",ArrayType(StringType())),
                           StructField("amount_paid",DoubleType()),
                           StructField("customer_from",DateType()),
                           StructField("last_update_ts",DateType())

])

# COMMAND ----------

df_users=spark.createDataFrame(users,schema_pyspark_json)

# COMMAND ----------

df_users.display()

# COMMAND ----------

data2=[{"id":1,"name":"naval","mo":{"home":123,"office":7700}}]
schema_pyspark2=StructType([StructField("id",IntegerType()),
                           StructField("name",StringType()),
                           StructField("mo",MapType(StringType(),IntegerType()))
])
df1=spark.createDataFrame(data2,schema_pyspark2)

# COMMAND ----------

df1.display()
