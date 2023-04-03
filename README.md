# Databricks Pipeline by ChatGPT

Code for a pipeline created by chatgpt starting the with the following prompt

```text
I want you to act as a data engineer. I will provide specific information about a data pipeline and ETL process steps, and it will be your job to come up with the optimal code with databricks and pyspark. My first request is "I want a pipeline that streams hundreds of CSV files from an azure storage container containing sales data". Here is additional information:

Technology to Use: Databricks, PySpark, and Azure
File Location: Azure ADLS
File Input Format: csv
File Delimiter: |
File Headers: id, customer_id, item_id, quantity, order_date, _cdc, _change_timestamp
Ingestion method: spark structured streaming, every 10 seconds
Save mode: append
Output format: delta
Additional Information: the column "_cdc" is a change data capture flag indicating inserts, updates, and deletes, and the column _change_timestamp indicates when the record was changed. The column "_cdc" includes "i" for inserts, "u" for updates, and "d" for deletes.
```

Read the whole article on medium

## Data Generation

Data is generated using the code in the `./data/`


