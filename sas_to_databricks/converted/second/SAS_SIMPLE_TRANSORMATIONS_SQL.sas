from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("SAS_to_PySpark_Conversion").getOrCreate()

jdbc_url = "jdbc:postgresql://172.190.194.46:5432/northwind"
connection_properties = {
    "user": "root",
    "password": "Systech123",
    "driver": "org.postgresql.Driver",
    "UseServerSidePrepare": "1"
}

df_order_details = spark.read.jdbc(url=jdbc_url, table="public.order_details", properties=connection_properties)
df_orders = spark.read.jdbc(url=jdbc_url, table="public.orders", properties=connection_properties)
df_employees = spark.read.jdbc(url=jdbc_url, table="public.employees", properties=connection_properties)
df_employee_territories = spark.read.jdbc(url=jdbc_url, table="public.employee_territories", properties=connection_properties)
df_territories = spark.read.jdbc(url=jdbc_url, table="public.territories", properties=connection_properties)
df_region = spark.read.jdbc(url=jdbc_url, table="public.region", properties=connection_properties)

print("--- Schema for 'region' table (from PROC CONTENTS) ---")
df_region.printSchema()
print("\n--- Data Sample for 'region' table (from PROC CONTENTS) ---")
df_region.show(5, truncate=False)

df_order_details.createOrReplaceTempView("order_details")
df_orders.createOrReplaceTempView("orders")
df_employees.createOrReplaceTempView("employees")
df_employee_territories.createOrReplaceTempView("employee_territories")
df_territories.createOrReplaceTempView("territories")
df_region.createOrReplaceTempView("region")

sql_query = """
    select
        r.region_description as region_name,
        round(sum(od.unit_price * od.quantity * (1 - od.discount)), 2) as revenue
    from order_details od
        inner join orders o on od.order_id = o.order_id
        inner join employees e on o.employee_id = e.employee_id
        inner join employee_territories et on e.employee_id = et.employee_id
        inner join territories t on et.territory_id = t.territory_id
        inner join region r on t.region_id = r.region_id
    group by r.region_id, r.region_description
    order by revenue desc
"""
df_revenue_by_region = spark.sql(sql_query)

print("Revenue by Region - Northwind Database")
df_revenue_by_region.select("region_name", "revenue").show(truncate=False)

#End-DBShift