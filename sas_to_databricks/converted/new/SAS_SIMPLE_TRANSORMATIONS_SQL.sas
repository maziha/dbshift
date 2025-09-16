import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# This script assumes a SparkSession is available as 'spark'.
# In a Databricks notebook, this is automatically created.

# Define JDBC connection properties for PostgreSQL
jdbc_url = "jdbc:postgresql://172.190.194.46:5432/northwind"
connection_properties = {
    "user": "root",
    "password": "Systech123",
    "driver": "org.postgresql.Driver"
}

# Translation of 'PROC CONTENTS DATA=pglib._all_ NODS;'
# This step is to test the connection by inspecting the metadata of all tables.
# A practical PySpark equivalent is to read one of the tables and display its schema to verify the connection.
print("--- Connection Test: Schema for 'pglib.region' ---")
df_region_test = spark.read.jdbc(url=jdbc_url, table="public.region", properties=connection_properties)
df_region_test.printSchema()

# Read all source tables from the PostgreSQL database
df_order_details = spark.read.jdbc(url=jdbc_url, table="public.order_details", properties=connection_properties)
df_orders = spark.read.jdbc(url=jdbc_url, table="public.orders", properties=connection_properties)
df_employees = spark.read.jdbc(url=jdbc_url, table="public.employees", properties=connection_properties)
df_employee_territories = spark.read.jdbc(url=jdbc_url, table="public.employee_territories", properties=connection_properties)
df_territories = spark.read.jdbc(url=jdbc_url, table="public.territories", properties=connection_properties)
df_region = spark.read.jdbc(url=jdbc_url, table="public.region", properties=connection_properties)

# Register DataFrames as temporary views for use in spark.sql()
df_order_details.createOrReplaceTempView("order_details")
df_orders.createOrReplaceTempView("orders")
df_employees.createOrReplaceTempView("employees")
df_employee_territories.createOrReplaceTempView("employee_territories")
df_territories.createOrReplaceTempView("territories")
df_region.createOrReplaceTempView("region")

# Translation of 'PROC SQL;'
df_revenue_by_region = spark.sql("""
    SELECT 
        r.region_description AS region_name,
        round(sum(od.unit_price * od.quantity * (1 - od.discount)), 2) AS revenue
    FROM order_details od
    INNER JOIN orders o ON od.order_id = o.order_id
    INNER JOIN employees e ON o.employee_id = e.employee_id
    INNER JOIN employee_territories et ON e.employee_id = et.employee_id
    INNER JOIN territories t ON et.territory_id = t.territory_id
    INNER JOIN region r ON t.region_id = r.region_id
    GROUP BY r.region_id, r.region_description
    ORDER BY revenue DESC
""")

# Translation of 'PROC PRINT DATA=work.revenue_by_region;'
print("Revenue by Region - Northwind Database")
df_revenue_by_region.select("region_name", "revenue").show(truncate=False)

#End-DBShift