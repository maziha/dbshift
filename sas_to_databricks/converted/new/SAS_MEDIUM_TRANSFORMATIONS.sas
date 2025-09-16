import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import time
from datetime import datetime

spark = SparkSession.builder.appName("SAS_to_PySpark_Conversion").getOrCreate()

year1 = 1997
year2 = 1998
category_name = "Beverages"
min_spending = 100
debug_mode = "Y"

jdbc_url = "jdbc:postgresql://172.190.194.46:5432/northwind"
connection_properties = {
    "user": "root",
    "password": "Systech123",
    "driver": "org.postgresql.Driver",
    "schema": "public"
}

def logger(message, level="INFO"):
    if debug_mode.upper() == "Y":
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{level}: {timestamp} - {message}")

def db_connect_and_test():
    logger(message="Connecting to PostgreSQL database")
    try:
        df_test = spark.read.jdbc(url=jdbc_url, table="customers", properties=connection_properties)
        conn_test = df_test.count()
        if conn_test > 0:
            logger(message=f"Connection successful - Found {conn_test} customers")
            return True
        else:
            logger(message="Connection failed", level="ERROR")
            return False
    except Exception as e:
        logger(message=f"Connection failed with exception: {e}", level="ERROR")
        return False

def extract_category_sales(category, start_year, end_year):
    logger(message=f"Extracting {category} sales data for {start_year}-{end_year}")

    df_customers = spark.read.jdbc(url=jdbc_url, table="public.customers", properties=connection_properties)
    df_orders = spark.read.jdbc(url=jdbc_url, table="public.orders", properties=connection_properties)
    df_order_details = spark.read.jdbc(url=jdbc_url, table="public.order_details", properties=connection_properties)
    df_products = spark.read.jdbc(url=jdbc_url, table="public.products", properties=connection_properties)
    df_categories = spark.read.jdbc(url=jdbc_url, table="public.categories", properties=connection_properties)

    df_customers.createOrReplaceTempView("customers")
    df_orders.createOrReplaceTempView("orders")
    df_order_details.createOrReplaceTempView("order_details")
    df_products.createOrReplaceTempView("products")
    df_categories.createOrReplaceTempView("categories")

    df_category_sales = spark.sql(f"""
        SELECT
            c.customer_id,
            c.company_name,
            c.country,
            c.city,
            o.order_id,
            o.order_date,
            YEAR(o.order_date) as sales_year,
            p.product_name,
            od.unit_price,
            od.quantity,
            od.discount,
            od.unit_price * od.quantity * (1 - od.discount) as net_amount
        FROM customers c
        INNER JOIN orders o ON c.customer_id = o.customer_id
        INNER JOIN order_details od ON o.order_id = od.order_id
        INNER JOIN products p ON od.product_id = p.product_id
        INNER JOIN categories cat ON p.category_id = cat.category_id
        WHERE cat.category_name = '{category}'
          AND YEAR(o.order_date) BETWEEN {start_year} AND {end_year}
        ORDER BY c.customer_id, o.order_date
    """)

    record_count = df_category_sales.count()
    logger(message=f"Extracted {record_count} records for {category} category")
    return df_category_sales

def create_yearly_summary(input_df, min_spending_filter):
    logger(message=f"Creating yearly summary from input DataFrame")
    input_df.createOrReplaceTempView("category_sales")

    df_yearly_summary = spark.sql(f"""
        SELECT
            customer_id,
            company_name,
            country,
            sales_year,
            count(distinct order_id) as yearly_orders,
            sum(net_amount) as yearly_spending,
            avg(net_amount) as avg_order_value,
            count(distinct product_name) as unique_products
        FROM category_sales
        GROUP BY customer_id, company_name, country, sales_year
        HAVING sum(net_amount) > {min_spending_filter}
        ORDER BY customer_id, sales_year
    """)

    logger(message="Yearly summary created successfully")
    return df_yearly_summary

def pivot_years(input_df, yr1, yr2):
    logger(message=f"Pivoting data to compare {yr1} vs {yr2}")
    input_df.createOrReplaceTempView("yearly_summary")

    sql_query = f"""
    WITH pivoted_base AS (
        SELECT
            customer_id,
            company_name,
            country,
            SUM(CASE WHEN sales_year = {yr1} THEN yearly_spending ELSE 0 END) as spending_{yr1},
            SUM(CASE WHEN sales_year = {yr1} THEN yearly_orders ELSE 0 END) as orders_{yr1},
            SUM(CASE WHEN sales_year = {yr1} THEN unique_products ELSE 0 END) as products_{yr1},
            SUM(CASE WHEN sales_year = {yr2} THEN yearly_spending ELSE 0 END) as spending_{yr2},
            SUM(CASE WHEN sales_year = {yr2} THEN yearly_orders ELSE 0 END) as orders_{yr2},
            SUM(CASE WHEN sales_year = {yr2} THEN unique_products ELSE 0 END) as products_{yr2}
        FROM yearly_summary
        GROUP BY customer_id, company_name, country
    )
    SELECT
        customer_id,
        company_name,
        country,
        spending_{yr1},
        orders_{yr1},
        products_{yr1},
        spending_{yr2},
        orders_{yr2},
        products_{yr2},
        spending_{yr2} - spending_{yr1} AS spending_change,
        CASE
            WHEN spending_{yr1} > 0 THEN
                ROUND(((spending_{yr2} - spending_{yr1}) / spending_{yr1} * 100), 2)
            ELSE 0
        END AS growth_percentage,
        orders_{yr1} + orders_{yr2} AS total_orders
    FROM pivoted_base
    WHERE spending_{yr1} > 0 AND spending_{yr2} > 0
    ORDER BY spending_change DESC
    """
    df_year_comparison = spark.sql(sql_query)
    logger(message="Year comparison table created")
    return df_year_comparison

def classify_customers(input_df):
    logger(message="Classifying customer performance")
    
    df = input_df.withColumn("growth_category",
        F.when(F.col("growth_percentage") >= 50, 'HIGH_GROWTH')
         .when(F.col("growth_percentage") >= 20, 'MODERATE_GROWTH')
         .when(F.col("growth_percentage") >= 0, 'SLOW_GROWTH')
         .when(F.col("growth_percentage") >= -20, 'DECLINING')
         .otherwise('MAJOR_DECLINE')
    )
    
    df = df.withColumn("total_spending", F.col(f"spending_{year1}") + F.col(f"spending_{year2}"))
    
    df = df.withColumn("spending_tier",
        F.when(F.col("total_spending") >= 1000, 'PREMIUM')
         .when(F.col("total_spending") >= 500, 'STANDARD')
         .otherwise('BASIC')
    )
    
    df = df.withColumn("customer_value_score",
        (F.col("total_spending") * 0.6) + (F.col("total_orders") * 10) + (F.col("growth_percentage") * 2)
    )
    
    logger(message="Customer classification completed")
    return df

def generate_report(input_df, category):
    logger(message="Generating analysis report")
    
    print("\n" + "="*80)
    print(f"Northwind Traders - {category} Category Analysis")
    print(f"Customer Performance Comparison: {year1} vs {year2}")
    print("="*80)
    
    input_df.createOrReplaceTempView("final_analysis")
    
    print("\n--- Executive Summary ---")
    df_summary_stats = spark.sql("""
        SELECT 'Total Customers Analyzed' AS metric, CAST(COUNT(*) AS STRING) AS value FROM final_analysis
        UNION ALL
        SELECT 'Average Growth Rate' AS metric, CAST(AVG(growth_percentage) AS STRING) AS value FROM final_analysis
        UNION ALL
        SELECT 'Total Revenue Change' AS metric, CAST(SUM(spending_change) AS STRING) AS value FROM final_analysis
    """)
    df_summary_stats.select(
        F.col("metric"),
        F.when(F.col("metric") == "Total Customers Analyzed", F.col("value"))
         .when(F.col("metric") == "Average Growth Rate", F.concat(F.round(F.col("value"), 1).cast("string"), F.lit("%")))
         .when(F.col("metric") == "Total Revenue Change", F.concat(F.lit("$"), F.format_number(F.col("value"), 2)))
         .alias("value")
    ).show(truncate=False)

    print("\n--- Top 10 Growing Customers ---")
    input_df.select(
        "company_name", "country", f"spending_{year1}", f"spending_{year2}", "spending_change", "growth_percentage", "growth_category"
    ).limit(20).show(truncate=False)

    print("\n--- Customer Growth Distribution ---")
    df_freq = input_df.groupBy("growth_category").count()
    df_freq.show()

    print("\n--- Performance by Spending Tier ---")
    df_means = input_df.groupBy("spending_tier").agg(
        F.mean("spending_change").alias("spending_change"),
        F.mean("growth_percentage").alias("growth_percentage"),
        F.sum("total_orders").alias("total_orders")
    )
    df_means.select(
        "spending_tier",
        F.format_number("spending_change", 2).alias("spending_change"),
        "growth_percentage",
        "total_orders"
    ).show()
    
    logger(message="Report generation completed")

def main_process():
    start_time_sec = time.time()
    logger(message=f"Starting {category_name} analysis process")
    
    if not db_connect_and_test():
        logger(message="Aborting due to database connection failure.", level="ERROR")
        return None
    
    df_category_sales = extract_category_sales(
        category=category_name,
        start_year=year1,
        end_year=year2
    )
    
    df_yearly_summary = create_yearly_summary(
        input_df=df_category_sales,
        min_spending_filter=min_spending
    )
    
    df_year_comparison = pivot_years(
        input_df=df_yearly_summary,
        yr1=year1,
        yr2=year2
    )
    
    df_final_analysis = classify_customers(
        input_df=df_year_comparison
    )
    
    generate_report(
        input_df=df_final_analysis,
        category=category_name
    )
    
    end_time_sec = time.time()
    duration = end_time_sec - start_time_sec
    
    logger(message=f"Analysis completed in {time.strftime('%H:%M:%S', time.gmtime(duration))} seconds")
    
    return df_final_analysis

df_final_analysis_result = main_process()

if df_final_analysis_result:
    print("\n--- Final Dataset Schema ---")
    df_final_analysis_result.printSchema()
    print("\n--- Final Dataset Sample ---")
    df_final_analysis_result.show(5, truncate=False)

#End-DBShift