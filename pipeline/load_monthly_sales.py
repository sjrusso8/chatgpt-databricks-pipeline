# Databricks notebook source
from pyspark.sql.functions import month, year, desc, col, concat_ws

# 1. Read the "all_sales" Delta table using Structured Streaming
all_sales_input_path = "/mnt/datalake/chat_gpt_delta/all_sales/delta_output"

all_sales_stream = spark.readStream \
    .format("delta") \
    .load(all_sales_input_path)

# 2. Process the data to create a new column with just the month and year from the "order_date" column
all_sales_processed = all_sales_stream \
    .withColumn("month_year", concat_ws("-",year(col("order_date")).cast("string"), month(col("order_date")).cast("string")))

# 3. Group by "customer_id" and the new month column
# 4. Sum the "quantity" column
monthly_sales_agg = all_sales_processed \
    .groupBy("customer_id", "month_year") \
    .sum("quantity") \
    .withColumnRenamed("sum(quantity)", "total_quantity")

# 5. Order by the new month column descending and the total quantity descending
monthly_sales_ordered = monthly_sales_agg \
    .orderBy(desc("month_year"), desc("total_quantity"))

# 6. Write the aggregated data to a new Delta table named "monthly_sales"
# monthly_sales_output_path = "abfss://{0}@{1}.dfs.core.windows.net/monthly_sales".format(adls_container_name, adls_account_name)
# checkpoint_location = "abfss://{0}@{1}.dfs.core.windows.net/checkpoints_monthly_sales".format(adls_container_name, adls_account_name)
monthly_sales_output_path = "/mnt/datalake/chat_gpt_delta/monthly_sales/delta_output"
checkpoint_location = "/mnt/datalake/chat_gpt_delta/monthly_sales/checkpoints_monthly_sales"

monthly_sales_query = monthly_sales_ordered.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", checkpoint_location) \
    .trigger(availableNow=True) \
    .start(monthly_sales_output_path)

# 7. Start the stream
monthly_sales_query.awaitTermination()

