# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog capstone_data

# COMMAND ----------

df_sales_silver=spark.table("silver.sales")
df_customers_silver=spark.table("silver.customers")
df_products_silver=spark.table("silver.products")
df_dates_silver=spark.table("silver.dates")
df_joined=(df_sales_silver
           .join(df_customers_silver, on="customer_id", how="inner")
           .join(df_products_silver,on="product_id",how="inner")
           .join(df_dates_silver,how="inner",on="order_date")
           )
df_joined.display()
df_joined.write.mode("overwrite").saveAsTable("gold.final")

# COMMAND ----------


