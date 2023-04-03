# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import row_number, desc
from pyspark.sql.window import Window

# 1. Read the output Delta table using Structured Streaming
# delta_input_path = "abfss://{0}@{1}.dfs.core.windows.net/delta_output".format(adls_container_name, adls_account_name)

delta_input_path = "/mnt/datalake/chat_gpt_delta/raw_data/delta_output"

delta_stream = spark.readStream \
    .format("delta") \
    .load(delta_input_path)

# 2. Define a function to process the data based on the "action" column and order it by "_change_timestamp"
def process_data(batch_df: DataFrame, batch_id: int):
    # all_sales_path = "abfss://{0}@{1}.dfs.core.windows.net/all_sales".format(adls_container_name, adls_account_name)
    all_sales_path = "/mnt/datalake/chat_gpt_delta/all_sales/delta_output"

    if batch_df.count() > 0:
        # Define the window function for deduplication
        deduplication_window = Window.partitionBy("id").orderBy(desc("_change_timestamp"))

        # Deduplicate the data based on the id and _change_timestamp descending
        deduplicated_batch_df = batch_df \
            .withColumn("row_number", row_number().over(deduplication_window)) \
            .filter("row_number = 1") \
            .drop("row_number")

        # Register deduplicated_batch_df as a temporary view
        deduplicated_batch_df.createOrReplaceTempView("updates")

        # Initialize the all_sales DeltaTable, or create a new one if it doesn't exist
        try:
            all_sales = DeltaTable.forPath(spark, all_sales_path)
        except:
            batch_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(all_sales_path)
            all_sales = DeltaTable.forPath(spark, all_sales_path)
            
        batch_df = spark.table("updates")

        # 3. Apply the function using foreachBatch
        # Merge the updates based on the "action" column and order by "_change_timestamp"
        all_sales.alias("t") \
            .merge(
                batch_df.alias("s"),
                "t.id = s.id") \
            .whenMatchedUpdate(
                condition="s.action = 'update'",
                set={"customer_id": "s.customer_id", "item_id": "s.item_id", "quantity": "s.quantity",
                     "order_date": "s.order_date", "_cdc": "s._cdc", "_change_timestamp": "s._change_timestamp"}) \
            .whenMatchedDelete(condition="s.action = 'delete'") \
            .whenNotMatchedInsert(
                values={"id": "s.id", "customer_id": "s.customer_id", "item_id": "s.item_id", "quantity": "s.quantity",
                        "order_date": "s.order_date", "_cdc": "s._cdc", "_change_timestamp": "s._change_timestamp"}) \
            .execute()

# 4. Write the updated data to a new Delta table named "all_sales"
# checkpoint_location = "abfss://{0}@{1}.dfs.core.windows.net/checkpoints_all_sales".format(adls_container_name, adls_account_name)
checkpoint_location = "/mnt/datalake/chat_gpt_delta/all_sales/checkpoints_all_sales"

query = delta_stream \
    .writeStream \
    .foreachBatch(process_data) \
    .outputMode("update") \
    .option("checkpointLocation", checkpoint_location) \
    .trigger(availableNow=True) \
    .start()

# 5. Start the stream
query.awaitTermination()

