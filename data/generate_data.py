# Databricks notebook source
# MAGIC %pip install dbldatagen

# COMMAND ----------

from random import randrange, choice

import dbldatagen as dg

import pyspark.sql.functions as F 
from pyspark.sql.types import IntegerType

# COMMAND ----------

data_rows = 100_000
partitions_requested = 4
unique_orders = 100_000
unique_customers = 10_000
unique_items = 100

base_data_spec = (
    dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
    .withColumn("data_id", IntegerType(), uniqueValues=unique_orders)
    .withColumn("customer_id", IntegerType(), uniqueValues=unique_customers)
    .withColumn("order_id", IntegerType(), uniqueValues=unique_items)
    .withColumn("quantity", IntegerType(), minValue=1, maxValue=5_000)
    .withColumn("order_date", "timestamp", expr="now()")
    .withColumn("_change_timestamp", "timestamp", expr="now()")
    .withColumn("_cdc", expr="'i'")
    )

df_base_data = base_data_spec.build()

# COMMAND ----------

# write the data

df_base_data.withColumnRenamed('data_id', 'id').write.format('delta').mode('overwrite').save('/mnt/datalake/chat_gpt_delta/data/')

# COMMAND ----------

#  inserts
for i in range(0,10):
    start_of_new_ids = df_base_data.select(F.max('customer_id')+1).collect()[0][0]

    print(start_of_new_ids)

    df_inserts = (
        base_data_spec.clone()
            .option("startingId", start_of_new_ids)
            .withRowCount(10 * 1000)
            .build()
            .withColumn("_cdc", F.lit("i"))
            .withColumn("customer_id", F.expr(f"customer_id + {start_of_new_ids}"))
            .withColumnRenamed('data_id', 'id')
    )

    df_inserts.write.format('delta').mode('append').save('/mnt/datalake/chat_gpt_delta/data/')
    
    df_base_data = df_base_data.union(df_inserts)

# COMMAND ----------

# updates
for i in range(24):
    interval = choice(["DAY", "MONTH", "HOUR"])

    df_updates = (df_base_data.sample(False, 0.1)
        .limit(500)
        .withColumn("quantity", F.lit(randrange(start=1, stop=5_000)))
        .withColumn("order_date", F.col("order_date") + F.expr(f"INTERVAL {i} {interval}"))
        .withColumn("_change_timestamp", F.col("_change_timestamp") + F.expr(f"INTERVAL {i} {interval}"))
        .withColumn("_cdc", F.lit("u"))
        .withColumnRenamed('data_id', 'id')
                 )

    df_updates.write.format('delta').mode('append').save('/mnt/datalake/chat_gpt_delta/data/')

# COMMAND ----------

# deletes
df_all = spark.read.format('delta').load('/mnt/datalake/chat_gpt_delta/data/')

# COMMAND ----------

df_all.createOrReplaceTempView('df_all')

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO delta.`/mnt/datalake/chat_gpt_delta/data/`
# MAGIC WITH max_change_timestamp AS (
# MAGIC   SELECT
# MAGIC     max(_change_timestamp) as max__change_timestamp
# MAGIC   FROM df_all
# MAGIC )
# MAGIC SELECT 
# MAGIC   id, 
# MAGIC   customer_id, 
# MAGIC   order_id,
# MAGIC   quantity,
# MAGIC   order_date,
# MAGIC   current_timestamp() + interval 3 years as _change_timestamp,
# MAGIC   'd' as _cdc
# MAGIC FROM df_all
# MAGIC WHERE _change_timestamp = (
# MAGIC   SELECT *
# MAGIC   FROM max_change_timestamp
# MAGIC )
# MAGIC LIMIT 100

# COMMAND ----------

# creating a lot of csv files by setting a high repartition
df_csv = spark.read.format('delta').load("/mnt/datalake/chat_gpt_delta/data/").repartition(10_000)

# COMMAND ----------

df_csv.write.format('csv').options(header='True', delimiter='|').save('/mnt/datalake/chat_gpt_delta/csv/')
