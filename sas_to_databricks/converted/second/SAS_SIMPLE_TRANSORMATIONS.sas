import pyspark.sql.functions as F
from pyspark.sql.window import Window

jdbc_url = "jdbc:postgresql://172.190.194.46:5432/northwind"
connection_properties = {
    "user": "root",
    "password": "Systech123",
    "driver": "org.postgresql.Driver"
}

df_region = spark.read.jdbc(url=jdbc_url, table="public.region", properties=connection_properties)
df_territories = spark.read.jdbc(url=jdbc_url, table="public.territories", properties=connection_properties)
df_employee_territories = spark.read.jdbc(url=jdbc_url, table="public.employee_territories", properties=connection_properties)
df_orders = spark.read.jdbc(url=jdbc_url, table="public.orders", properties=connection_properties)
df_order_details = spark.read.jdbc(url=jdbc_url, table="public.order_details", properties=connection_properties)
df_employees = spark.read.jdbc(url=jdbc_url, table="public.employees", properties=connection_properties)

df_region_territory = df_region.join(df_territories, on="region_id", how="inner")
df_region_territory = df_region_territory.select("territory_id", "region_description")

df_territory_employee = df_region_territory.join(df_employee_territories, on="territory_id", how="inner")
df_territory_employee = df_territory_employee.select("employee_id", "region_description")

df_orders_kept = df_orders.select("order_id", "employee_id")
df_order_revenue = df_orders_kept.join(df_order_details, on="order_id", how="inner")
df_order_revenue = df_order_revenue.withColumn("revenue", F.col("unit_price") * F.col("quantity") * (1 - F.col("discount")))
df_order_revenue = df_order_revenue.select("employee_id", "revenue")

df_employees_kept = df_employees.select("employee_id")
df_final_transactions = df_order_revenue.join(df_employees_kept, on="employee_id", how="inner")

df_lookup_with_duplicates = df_territory_employee
df_sorted_final_transactions = df_final_transactions

df_flat_file_for_summary = df_sorted_final_transactions.join(df_lookup_with_duplicates, on="employee_id", how="inner")
df_flat_file_for_summary = df_flat_file_for_summary.select("region_description", "revenue")

df_revenue_by_region_unsorted = df_flat_file_for_summary.groupBy("region_description").agg(
    F.sum("revenue").alias("revenue")
).withColumnRenamed("region_description", "region_name")

df_revenue_by_region = df_revenue_by_region_unsorted.orderBy(F.col("revenue").desc())

print("Revenue by Region")
df_revenue_by_region_formatted = df_revenue_by_region.withColumn("revenue", F.format_number(F.col("revenue"), 2))
df_revenue_by_region_formatted.show(truncate=False)

#End-DBShift