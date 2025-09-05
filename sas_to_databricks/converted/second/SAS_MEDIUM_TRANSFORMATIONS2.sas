import pyspark.sql.functions as F
from pyspark.sql import SparkSession

jdbc_url = "jdbc:postgresql://172.190.194.46:5432/northwind"
connection_properties = {
    "user": "root",
    "password": "Systech123",
    "driver": "org.postgresql.Driver"
}

df_customers_from_pg = spark.read.jdbc(url=jdbc_url, table="public.customers", properties=connection_properties)
df_sorted_customers = df_customers_from_pg.select("customer_id", "country")

df_orders_from_pg = spark.read.jdbc(url=jdbc_url, table="public.orders", properties=connection_properties)
df_sorted_orders = df_orders_from_pg.select("customer_id", "order_id", "order_date")

df_customer_orders = df_sorted_customers.join(df_sorted_orders, on="customer_id", how="inner")
df_customer_orders = df_customer_orders.select("country", "order_id", "order_date")

df_sorted_customer_orders = df_customer_orders

df_order_details_from_pg = spark.read.jdbc(url=jdbc_url, table="public.order_details", properties=connection_properties)
df_sorted_order_details = df_order_details_from_pg.select("order_id", "unit_price", "quantity", "discount")

df_full_transactions = df_sorted_customer_orders.join(df_sorted_order_details, on="order_id", how="inner")
df_full_transactions = df_full_transactions.withColumn("year", F.year(F.col("order_date")))
df_full_transactions = df_full_transactions.withColumn("revenue", F.col("unit_price") * F.col("quantity") * (1 - F.col("discount")))
df_full_transactions = df_full_transactions.select("country", "year", "revenue")

df_revenue_summary_long = df_full_transactions.filter(F.col("year").isin([1996, 1997, 1998])) \
    .groupBy("country", "year") \
    .agg(F.sum("revenue").alias("total_revenue"))

df_revenue_summary_long_rounded = df_revenue_summary_long.withColumn("total_revenue", F.round(F.col("total_revenue"), 2))

df_revenue_wide = df_revenue_summary_long_rounded.groupBy("country") \
    .pivot("year") \
    .agg(F.first("total_revenue"))

df_revenue_wide = df_revenue_wide.withColumnRenamed("1996", "y1996") \
                                 .withColumnRenamed("1997", "y1997") \
                                 .withColumnRenamed("1998", "y1998")

df_final_report_unsorted = df_revenue_wide.filter(
    (F.col("y1996").isNotNull()) &
    (F.col("y1997").isNotNull()) &
    (F.col("y1998").isNotNull())
)

df_revenue_by_country_final = df_final_report_unsorted.withColumnRenamed("y1996", "rev1996") \
    .orderBy(F.col("rev1996"), F.col("y1997"), F.col("y1998"))

print("Revenue by Country for 1996, 1997, and 1998")

df_final_print_view = df_revenue_by_country_final.select(
    F.col("country").alias("Country"),
    F.col("y1997"),
    F.col("y1998"),
    F.col("rev1996").alias("1996")
)

df_final_print_view.show()

#End-DBShift