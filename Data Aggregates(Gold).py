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

df_joined=spark.table("capstone_data.gold.final")
df_joined.createOrReplaceTempView("total_revenue")
total_revenue_df = spark.sql("select sum(total_amount) as total_revenue from total_revenue")
total_revenue_df.display()

# COMMAND ----------

df_joined.createOrReplaceTempView("total_discount")

total_discount_df = spark.sql("select sum(discount_amount) as total_discount from total_discount")
total_discount_df.display()

# COMMAND ----------

df_joined.createOrReplaceTempView("avg_order_value")

avg_order_value_df = spark.sql("select avg(total_amount) as average_order_value from avg_order_value")
avg_order_value_df.display()

# COMMAND ----------

df_joined.createOrReplaceTempView("no_of_orders")


no_of_orders_df = spark.sql("select count(distinct order_id) as number_of_orders from no_of_orders")
no_of_orders_df.display()


# COMMAND ----------

df_joined.createOrReplaceTempView("top5_products_by_sales")

top5_products_by_sales_df = spark.sql("select product_name, sum(total_amount) as total_sales from top5_products_by_sales group by product_name order by total_sales desc limit 5")
top5_products_by_sales_df.display()

# COMMAND ----------

df_joined.createOrReplaceTempView("revenue_by_customer_loc")

revenue_by_customer_loc_df = spark.sql("select customer_city,customer_state, sum(total_amount) as total_revenue from revenue_by_customer_loc group by customer_city,customer_state order by total_revenue desc")
revenue_by_customer_loc_df.display()

# COMMAND ----------

df_joined.createOrReplaceTempView("frequency")


frequency_df = spark.sql("select order_date,count(distinct order_id) as number_of_orders from frequency group by order_date order by order_date")
frequency_df.display()


# COMMAND ----------

df_joined.createOrReplaceTempView("trends")

# COMMAND ----------

trends_df = spark.sql("select order_date, sum(total_amount) as total_revenue from trends group by order_date order by order_date")
trends_df.display()

# COMMAND ----------

total_revenue_df.write.mode("overwrite").saveAsTable("capstone_data.gold.total_revenue")
total_discount_df.write.mode("overwrite").saveAsTable("capstone_data.gold.total_discount")
avg_order_value_df.write.mode("overwrite").saveAsTable("capstone_data.gold.avg_order_value")
no_of_orders_df.write.mode("overwrite").saveAsTable("capstone_data.gold.no_of_orders")
top5_products_by_sales_df.write.mode("overwrite").saveAsTable("capstone_data.gold.top5_products_by_sales")
revenue_by_customer_loc_df.write.mode("overwrite").saveAsTable("capstone_data.gold.revenue_by_customer_loc")
frequency_df.write.mode("overwrite").saveAsTable("capstone_data.gold.frequency")
trends_df.write.mode("overwrite").saveAsTable("capstone_data.gold.trends")

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace
masked_trends_df = trends_df.withColumn("total_revenue",
                                        regexp_replace(col("total_revenue").cast("string"), ".", "*"))
 
masked_trends_df.display()

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace
frequency_df = spark.table("gold.frequency")
masked_frequency_df = frequency_df.withColumn("number_of_orders",
                                              regexp_replace(col("number_of_orders").cast("string"), ".", "*"))
display(masked_frequency_df)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace
top5_products_by_sales_df=spark.table("gold.top5_products_by_sales")
masked_top5products_df = top5_products_by_sales_df.withColumn("product_name",
                                                    regexp_replace(col("product_name"), ".", "*")) \
                                        .withColumn("total_sales",
                                                    regexp_replace(col("total_sales").cast("string"), ".", "*"))
 
# Display the masked DataFrame to verify the masking
display(masked_top5products_df)
