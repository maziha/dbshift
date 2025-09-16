import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SAS to PySpark Conversion").getOrCreate()

jdbc_url = "jdbc:postgresql://172.190.194.46:5432/northwind"
connection_properties = {
    "user": "root",
    "password": "Systech123",
    "driver": "org.postgresql.Driver"
}
pg_schema = "public"

df_customers_from_pg = spark.read.jdbc(url=jdbc_url, table=f"{pg_schema}.customers", properties=connection_properties)
df_sorted_customers = df_customers_from_pg.select("customer_id", "country")

df_orders_from_pg = spark.read.jdbc(url=jdbc_url, table=f"{pg_schema}.orders", properties=connection_properties)
df_sorted_orders = df_orders_from_pg.select("customer_id", "order_id", "order_date")

df_customer_orders = df_sorted_customers.join(df_sorted_orders, on="customer_id", how="inner")
df_customer_orders = df_customer_orders.select("country", "order_id", "order_date")

df_sorted_customer_orders = df_customer_orders

df_order_details_from_pg = spark.read.jdbc(url=jdbc_url, table=f"{pg_schema}.order_details", properties=connection_properties)
df_sorted_order_details = df_order_details_from_pg.select("order_id", "unit_price", "quantity", "discount")

df_full_transactions = df_sorted_customer_orders.join(df_sorted_order_details, on="order_id", how="inner")
df_full_transactions = df_full_transactions.withColumn("year", F.year(F.col("order_date")))
df_full_transactions = df_full_transactions.withColumn("revenue", F.col("unit_price") * F.col("quantity") * (1 - F.col("discount")))
df_full_transactions = df_full_transactions.select("country", "year", "revenue")

df_revenue_summary_long = df_full_transactions.filter(F.col("year").isin([1996, 1997, 1998]))
df_revenue_summary_long = df_revenue_summary_long.groupBy("country", "year").agg(F.sum("revenue").alias("total_revenue"))

df_revenue_summary_long_rounded = df_revenue_summary_long.withColumn("total_revenue", F.round(F.col("total_revenue"), 2))

df_with_prefix_for_pivot = df_revenue_summary_long_rounded.withColumn("year_prefixed", F.concat(F.lit("y"), F.col("year")))
df_revenue_wide = df_with_prefix_for_pivot.groupBy("country").pivot("year_prefixed").agg(F.first("total_revenue"))

df_final_report_unsorted = df_revenue_wide.filter(
    (F.col("y1996").isNotNull()) &
    (F.col("y1997").isNotNull()) &
    (F.col("y1998").isNotNull())
)

df_sorted_for_rename = df_final_report_unsorted.orderBy("y1996", "y1997", "y1998")
df_revenue_by_country_final = df_sorted_for_rename.withColumnRenamed("y1996", "rev1996")

print("Revenue by Country for 1996, 1997, and 1998")

df_for_print = df_revenue_by_country_final.select(
    F.col("Country"),
    F.col("y1997"),
    F.col("y1998"),
    F.col("rev1996")
)

df_with_labels = df_for_print.withColumnRenamed("Country", "Country") \
                             .withColumnRenamed("y1997", "y1997") \
                             .withColumnRenamed("y1998", "y1998") \
                             .withColumnRenamed("rev1996", "1996")

df_formatted = df_with_labels.withColumn("y1997", F.format_number(F.col("y1997"), 2)) \
                             .withColumn("y1998", F.format_number(F.col("y1998"), 2)) \
                             .withColumn("1996", F.format_number(F.col("1996"), 2))

df_formatted.show()

#End-DBShift