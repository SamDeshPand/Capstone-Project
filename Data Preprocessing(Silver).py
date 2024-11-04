# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog capstone_data

# COMMAND ----------

df_sales=spark.table("bronze.sales")
df_sales_silver=df_sales.dropna(subset=["order_id","customer_id","transaction_id","product_id"]).dropDuplicates()
df_sales_silver.write.mode("overwrite").saveAsTable("silver.sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.products

# COMMAND ----------

from pyspark.sql.functions import current_date

df_products = spark.table("bronze.products")
df_products_today = df_products.filter(df_products.ingestion_date == current_date()).drop("ingestion_date")
df_products_today.write.mode("overwrite").saveAsTable("temp")

# COMMAND ----------

# MAGIC %md
# MAGIC The following snippet is written on assumption that if the product price for one item is updated two times in a single data file then the price which comes later in the table is considered

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE table if NOT EXISTS silver.products(product_category string,
# MAGIC    product_id bigint,
# MAGIC    product_name string,
# MAGIC    product_price double);
# MAGIC
# MAGIC MERGE INTO silver.products AS target
# MAGIC USING (
# MAGIC   WITH RankedProducts AS (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY (SELECT NULL)) AS rn
# MAGIC     FROM temp
# MAGIC   ),
# MAGIC   MaxRankPerProduct AS (
# MAGIC     SELECT product_id, MAX(rn) AS max_rn
# MAGIC     FROM RankedProducts
# MAGIC     GROUP BY product_id
# MAGIC   )
# MAGIC   SELECT rp.*
# MAGIC   FROM RankedProducts rp
# MAGIC   JOIN MaxRankPerProduct mrpp ON rp.product_id = mrpp.product_id AND rp.rn = mrpp.max_rn
# MAGIC ) AS source
# MAGIC ON target.product_id = source.product_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.product_category = source.product_category,
# MAGIC     target.product_name = source.product_name,
# MAGIC     target.product_price = source.product_price
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC    product_category,
# MAGIC    product_id,
# MAGIC    product_name,
# MAGIC    product_price
# MAGIC   )
# MAGIC   VALUES (
# MAGIC    product_category,
# MAGIC    product_id,
# MAGIC    product_name,
# MAGIC    product_price
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.products

# COMMAND ----------

df_customers=spark.table("bronze.customers")
df_customers_silver=df_customers.dropna(subset="customer_id").dropDuplicates()
df_customers_silver.display()

# COMMAND ----------

df_dates=spark.table("bronze.order_dates")
df_dates_silver=df_dates.dropDuplicates()
df_dates_silver.display()

# COMMAND ----------

df_customers_silver.write.mode("overwrite").saveAsTable("silver.customers")

# COMMAND ----------

df_dates_silver.write.mode("overwrite").saveAsTable("silver.dates")
