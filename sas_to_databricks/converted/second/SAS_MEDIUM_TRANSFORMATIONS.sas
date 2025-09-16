import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import datetime

spark = SparkSession.builder.appName("CustomerBeverageAnalysis").getOrCreate()

year1 = 1997
year2 = 1998
category_name = "Beverages"
min_spending = 100
debug_mode = "Y"

jdbc_url = "jdbc:postgresql://172.190.194.46:5432/northwind"
connection_properties = {
    "user": "root",
    "password": "Systech123",
    "driver": "org.postgresql.Driver"
}

def logger(message, level="INFO"):
    if debug_mode.upper() == "Y":
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{level}: {timestamp} - {message}")

def extract_category_sales(category, start_year, end_year):
    logger(f"Extracting {category} sales data for {start_year}-{end_year}")

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

    query = f"""
    SELECT 
        c.customer_id,
        c.company_name,
        c.country,
        c.city,
        o.order_id,
        o.order_date,
        year(o.order_date) as sales_year,
        p.product_name,
        od.unit_price,
        od.quantity,
        od.discount,
        od.unit_price * od.quantity * (1 - od.discount) as net_amount
    FROM customers c
    INNER JOIN orders o on c.customer_id = o.customer_id
    INNER JOIN order_details od on o.order_id = od.order_id
    INNER JOIN products p on od.product_id = p.product_id
    INNER JOIN categories cat on p.category_id = cat.category_id
    WHERE cat.category_name = '{category}'
      AND year(o.order_date) BETWEEN {start_year} AND {end_year}
    ORDER BY c.customer_id, o.order_date
    """
    
    df_category_sales = spark.sql(query)
    
    record_count = df_category_sales.count()
    logger(f"Extracted {record_count} records for {category} category")
    
    return df_category_sales

def create_yearly_summary(df_input, min_spend_amount):
    logger(f"Creating yearly summary from input DataFrame")
    
    df_input.createOrReplaceTempView("category_sales")
    
    query = f"""
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
    HAVING sum(net_amount) > {min_spend_amount}
    ORDER BY customer_id, sales_year
    """
    
    df_yearly_summary = spark.sql(query)
    logger("Yearly summary created successfully")
    return df_yearly_summary

def pivot_years(df_input, yr1, yr2):
    logger(f"Pivoting data to compare {yr1} vs {yr2}")
    
    df_input.createOrReplaceTempView("yearly_summary")
    
    query = f"""
    SELECT 
        customer_id,
        company_name,
        country,
        
        sum(case when sales_year = {yr1} then yearly_spending else 0 end) as spending_{yr1},
        sum(case when sales_year = {yr1} then yearly_orders else 0 end) as orders_{yr1},
        sum(case when sales_year = {yr1} then unique_products else 0 end) as products_{yr1},
        
        sum(case when sales_year = {yr2} then yearly_spending else 0 end) as spending_{yr2},
        sum(case when sales_year = {yr2} then yearly_orders else 0 end) as orders_{yr2},
        sum(case when sales_year = {yr2} then unique_products else 0 end) as products_{yr2},
        
        (sum(case when sales_year = {yr2} then yearly_spending else 0 end)) - (sum(case when sales_year = {yr1} then yearly_spending else 0 end)) as spending_change,
        
        case 
            when (sum(case when sales_year = {yr1} then yearly_spending else 0 end)) > 0 then 
                round((((sum(case when sales_year = {yr2} then yearly_spending else 0 end)) - (sum(case when sales_year = {yr1} then yearly_spending else 0 end))) / (sum(case when sales_year = {yr1} then yearly_spending else 0 end)) * 100), 2)
            else 0 
        end as growth_percentage,
        
        (sum(case when sales_year = {yr1} then yearly_orders else 0 end)) + (sum(case when sales_year = {yr2} then yearly_orders else 0 end)) as total_orders
        
    FROM yearly_summary
    GROUP BY customer_id, company_name, country
    
    HAVING sum(case when sales_year = {yr1} then yearly_spending else 0 end) > 0 
       AND sum(case when sales_year = {yr2} then yearly_spending else 0 end) > 0
    
    ORDER BY spending_change desc
    """
    
    df_year_comparison = spark.sql(query)
    logger("Year comparison table created")
    return df_year_comparison

def classify_customers(df_input, yr1, yr2):
    logger("Classifying customer performance")
    
    df_classified = df_input.withColumn("growth_category",
        F.when(F.col("growth_percentage") >= 50, 'HIGH_GROWTH')
         .when(F.col("growth_percentage") >= 20, 'MODERATE_GROWTH')
         .when(F.col("growth_percentage") >= 0, 'SLOW_GROWTH')
         .when(F.col("growth_percentage") >= -20, 'DECLINING')
         .otherwise('MAJOR_DECLINE')
    )
    
    df_classified = df_classified.withColumn("total_spending", F.col(f"spending_{yr1}") + F.col(f"spending_{yr2}"))
    
    df_classified = df_classified.withColumn("spending_tier",
        F.when(F.col("total_spending") >= 1000, 'PREMIUM')
         .when(F.col("total_spending") >= 500, 'STANDARD')
         .otherwise('BASIC')
    )
    
    df_classified = df_classified.withColumn("customer_value_score",
        (F.col("total_spending") * 0.6) + (F.col("total_orders") * 10) + (F.col("growth_percentage") * 2)
    )
    
    logger("Customer classification completed")
    return df_classified

def generate_report(df_input, category, yr1, yr2):
    logger("Generating analysis report")
    
    print(f"\n--- Northwind Traders - {category} Category Analysis ---")
    print(f"--- Customer Performance Comparison: {yr1} vs {yr2} ---")
    
    df_input.createOrReplaceTempView("final_analysis")
    
    print("\n--- Executive Summary ---")
    df_summary_stats = spark.sql("""
        SELECT 
            'Total Customers Analyzed' as metric,
            FORMAT_NUMBER(count(*), 0) as value
        FROM final_analysis
        UNION ALL
        SELECT 
            'Average Growth Rate' as metric,
            CONCAT(FORMAT_NUMBER(mean(growth_percentage), 1), '%') as value
        FROM final_analysis
        UNION ALL
        SELECT 
            'Total Revenue Change' as metric,
            FORMAT_STRING('%,.0f', sum(spending_change)) as value
        FROM final_analysis
    """)
    df_summary_stats.show(truncate=False)
    
    print("\n--- Top 10 Growing Customers ---")
    df_input.select(
        "company_name", "country", f"spending_{yr1}", f"spending_{yr2}", 
        "spending_change", "growth_percentage", "growth_category"
    ).show(20, truncate=False)
    
    print("\n--- Customer Growth Distribution ---")
    df_freq = df_input.groupBy("growth_category").count().orderBy(F.col("count").desc())
    df_freq.show(truncate=False)

    print("\n--- Performance by Spending Tier ---")
    df_means = df_input.groupBy("spending_tier").agg(
        F.mean("spending_change").alias("mean_spending_change"),
        F.sum("spending_change").alias("sum_spending_change"),
        F.mean("growth_percentage").alias("mean_growth_percentage"),
        F.sum("growth_percentage").alias("sum_growth_percentage"),
        F.mean("total_orders").alias("mean_total_orders"),
        F.sum("total_orders").alias("sum_total_orders")
    )
    df_means.select(
        "spending_tier",
        F.format_number(F.col("mean_spending_change"), 2).alias("spending_change_mean"),
        F.format_number(F.col("sum_spending_change"), 2).alias("spending_change_sum"),
        F.format_number(F.col("mean_growth_percentage"), 1).alias("growth_percentage_mean"),
        F.format_number(F.col("sum_growth_percentage"), 1).alias("growth_percentage_sum"),
        F.format_number(F.col("mean_total_orders"), 1).alias("total_orders_mean"),
        F.format_number(F.col("sum_total_orders"), 0).alias("total_orders_sum")
    ).show(truncate=False)
    
    logger("Report generation completed")

def main_process():
    start_time = datetime.datetime.now()
    logger(f"Starting {category_name} analysis process")
    
    logger("Database connection configured")
    
    df_category_sales = extract_category_sales(
        category=category_name,
        start_year=year1,
        end_year=year2
    )
    
    df_yearly_summary = create_yearly_summary(
        df_input=df_category_sales,
        min_spend_amount=min_spending
    )

    df_year_comparison = pivot_years(
        df_input=df_yearly_summary,
        yr1=year1,
        yr2=year2
    )

    df_final_analysis = classify_customers(
        df_input=df_year_comparison,
        yr1=year1,
        yr2=year2
    )
    
    generate_report(
        df_input=df_final_analysis,
        category=category_name,
        yr1=year1,
        yr2=year2
    )
    
    end_time = datetime.datetime.now()
    duration = end_time - start_time
    
    logger(f"Analysis completed in {duration}")
    
    print("\n--- Schema (from .printSchema()) ---")
    df_final_analysis.printSchema()
    
    print("\n--- Data Sample (from .show()) ---")
    df_final_analysis.show(5, truncate=False)

main_process()

#End-DBShift