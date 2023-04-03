# Databricks notebook source
# 1. Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# 2. Set up the configurations for ADLS
# adls_account_name = "<your_adls_account_name>"
# adls_container_name = "<your_adls_container_name>"
# adls_folder_path = "<your_adls_folder_path>"
# adls_sas_token = "<your_adls_sas_token>"

# conf = {
#     "fs.azure.sas.{0}.{1}.dfs.core.windows.net".format(adls_container_name, adls_account_name): adls_sas_token
# }
# 
# data_location = "abfss://{0}@{1}.dfs.core.windows.net/{2}".format(adls_container_name, adls_account_name, adls_folder_path)

data_location = '/mnt/datalake/chat_gpt_delta/csv/'

# 3. Define the schema for the CSV files
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("_change_timestamp", TimestampType(), True),
    StructField("_cdc", StringType(), True)
])

# 4. Read the CSV files using Structured Streaming
# spark = SparkSession.builder.config(conf=conf).getOrCreate()
# I modified the maxFilesPerTrigger to the default 1000

streaming_data = spark.readStream \
    .format("csv") \
    .option("sep", "|") \
    .schema(schema) \
    .option("header", "true") \
    .option("maxFilesPerTrigger", "1000") \
    .load(data_location)

# 5. Process the data based on the '_cdc' column
processed_data = streaming_data \
    .withColumn("action",
                when(col("_cdc") == "i", "insert")
                .when(col("_cdc") == "u", "update")
                .when(col("_cdc") == "d", "delete"))

# 6. Write the data to Delta format with append mode
# delta_output_path = "abfss://{0}@{1}.dfs.core.windows.net/delta_output".format(adls_container_name, adls_account_name)
# checkpoint_location = "abfss://{0}@{1}.dfs.core.windows.net/checkpoints".format(adls_container_name, adls_account_name)
delta_output_path = "/mnt/datalake/chat_gpt_delta/raw_data/delta_output"
checkpoint_location = "/mnt/datalake/chat_gpt_delta/raw_data/checkpoints"

query = processed_data.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_location) \
    .trigger(availableNow=True) \
    .start(delta_output_path)

# 7. Start the stream
query.awaitTermination()

