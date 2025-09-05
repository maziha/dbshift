import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, StructType, StructField
import datetime
import time
import sys

spark = SparkSession.builder.appName("AdvancedCustomerIntelligence").getOrCreate()

start_year = 1997
end_year = 1998
min_revenue_threshold = 500
debug_mode = 'Y'
output_path = '/tmp'
mask_sensitive_data = 'Y'
enable_quality_checks = 'Y'

def advanced_logger(message, level="INFO", location="MAIN", include_timing='N'):
    if debug_mode.upper() == 'Y':
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if include_timing.upper() == 'Y':
            # NOTE: Direct memory usage like SAS's getoption(memsize) is not applicable in a distributed environment.
            # This part of the original macro is intentionally omitted.
            print(f"{level}: [{timestamp}] [{location}] {message}")
        else:
            print(f"{level}: [{timestamp}] [{location}] {message}")

def connect_northwind(max_retries=3):
    retry_count = 0
    connection_status = "FAILED"
    
    advanced_logger(message="Initiating Northwind database connection", level="INFO", location="CONNECT_NORTHWIND")
    
    jdbc_properties = {
        "driver": "org.postgresql.Driver",
        "user": "root",
        "password": "Systech123"
    }
    jdbc_url = "jdbc:postgresql://172.190.194.46:5432/northwind?schema=public"
    
    while retry_count < max_retries and connection_status == "FAILED":
        retry_count += 1
        try:
            conn_test_df = spark.read.jdbc(url=jdbc_url, table="customers", properties=jdbc_properties)
            conn_test = conn_test_df.count()
            
            if conn_test > 0:
                connection_status = "SUCCESS"
                advanced_logger(message=f"Connection successful on attempt {retry_count} - Found {conn_test} customers",
                               level="INFO", location="CONNECT_NORTHWIND")
                
                tables_to_load = ["customers", "orders", "order_details", "products", "categories", "employees", "shippers"]
                for table_name in tables_to_load:
                    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties)
                    df.createOrReplaceTempView(table_name)
                return True
            else:
                advanced_logger(message=f"Connection attempt {retry_count} failed, data not found", level="WARNING", location="CONNECT_NORTHWIND")
        except Exception as e:
            advanced_logger(message=f"Connection attempt {retry_count} failed with error: {e}", level="WARNING", location="CONNECT_NORTHWIND")

        if connection_status == "FAILED" and retry_count < max_retries:
            advanced_logger(message="Retrying connection in 2 seconds", level="INFO", location="CONNECT_NORTHWIND")
            time.sleep(2)

    if connection_status == "FAILED":
        advanced_logger(message="All connection attempts failed", level="ERROR", location="CONNECT_NORTHWIND")
        sys.exit("ABORT: Could not connect to the database.")
    
    return False

def data_quality_assessment(input_df):
    advanced_logger(message="Starting data quality assessment", location="DATA_QUALITY_ASSESSMENT")

    total_records = input_df.count()
    unique_records = input_df.select(F.concat_ws('|', "customer_id", "order_id", "product_name")).distinct().count()
    duplicate_count = total_records - unique_records

    if duplicate_count > 0:
        advanced_logger(message=f"Found {duplicate_count} duplicate records", level="WARNING", location="DATA_QUALITY_ASSESSMENT")

    missing_customers = input_df.filter(F.col("customer_id").isNull()).count()
    missing_amounts = input_df.filter(F.col("net_amount").isNull() | (F.col("net_amount") <= 0)).count()
    missing_dates = input_df.filter(F.col("order_date").isNull()).count()

    advanced_logger(message=f"Quality Check - Missing customers: {missing_customers}. Missing amounts: {missing_amounts}. Missing dates: {missing_dates}.",
                   level="INFO", location="DATA_QUALITY_ASSESSMENT")

def extract_base_data():
    advanced_logger(message="Starting comprehensive data extraction", location="EXTRACT_BASE_DATA", include_timing='Y')
    
    sql_query = f"""
    SELECT 
        c.customer_id,
        c.company_name,
        c.contact_name,
        c.country,
        c.city,
        c.phone,
        o.order_id,
        o.order_date,
        o.employee_id,
        CONCAT_WS(' ', e.first_name, e.last_name) as employee_full_name,
        od.unit_price,
        od.quantity,
        od.discount,
        p.product_name,
        cat.category_name,
        s.company_name as shipper_name,
        od.unit_price * od.quantity as gross_amount,
        od.unit_price * od.quantity * (1 - od.discount) as net_amount,
        od.unit_price * od.quantity * od.discount as discount_amount,
        YEAR(o.order_date) as order_year,
        QUARTER(o.order_date) as order_quarter,
        MONTH(o.order_date) as order_month,
        CASE 
            WHEN c.country IN ('USA', 'Canada') THEN 'North America'
            WHEN c.country IN ('Germany', 'France', 'UK', 'Italy', 'Spain', 'Sweden', 
                              'Norway', 'Denmark', 'Finland', 'Austria', 'Belgium', 
                              'Switzerland', 'Ireland', 'Portugal') THEN 'Europe'
            WHEN c.country IN ('Brazil', 'Argentina', 'Venezuela', 'Mexico') THEN 'Latin America'
            ELSE 'Other'
        END as geographic_region,
        CASE 
            WHEN (od.unit_price * od.quantity * (1 - od.discount)) >= 1000 THEN 'Premium'
            WHEN (od.unit_price * od.quantity * (1 - od.discount)) >= 500 THEN 'Standard'
            ELSE 'Basic'
        END as transaction_tier
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    INNER JOIN order_details od ON o.order_id = od.order_id
    INNER JOIN products p ON od.product_id = p.product_id
    INNER JOIN categories cat ON p.category_id = cat.category_id
    LEFT JOIN employees e ON o.employee_id = e.employee_id
    LEFT JOIN shippers s ON o.ship_via = s.shipper_id
    WHERE YEAR(o.order_date) BETWEEN {start_year} AND {end_year}
    """
    
    df_base_customer_data = spark.sql(sql_query).orderBy("customer_id", "order_date")
    
    if enable_quality_checks.upper() == 'Y':
        data_quality_assessment(input_df=df_base_customer_data)
        
    advanced_logger(message="Base data extraction completed", location="EXTRACT_BASE_DATA")
    return df_base_customer_data

def remove_duplicates_advanced(input_df, key_vars):
    advanced_logger(message="Removing duplicates using advanced logic", location="REMOVE_DUPLICATES_ADVANCED")
    
    window_spec = Window.partitionBy(*key_vars).orderBy(F.col("net_amount").desc())
    
    df_with_rank = input_df.withColumn("dup_rank", F.row_number().over(window_spec))
    df_output = df_with_rank.filter(F.col("dup_rank") == 1).drop("dup_rank")
    
    final_count = df_output.count()
    advanced_logger(message=f"Duplicate removal completed - Final record count: {final_count}", location="REMOVE_DUPLICATES_ADVANCED")
    return df_output

def split_contact_names(input_df):
    advanced_logger(message="Splitting and standardizing contact names", location="SPLIT_CONTACT_NAMES")
    
    df = input_df.withColumn("name_parts", F.split(F.col("contact_name"), " "))
    df = df.withColumn("name_word_count", F.size(F.col("name_parts")))
    
    df = df.withColumn("first_name_raw", F.when(F.col("contact_name").isNotNull(), F.col("name_parts")[0]).otherwise(""))
    df = df.withColumn("last_name_raw", 
        F.when((F.col("contact_name").isNotNull()) & (F.col("name_word_count") >= 2), F.col("name_parts")[F.col("name_word_count")-1]).otherwise(""))
    
    df = df.withColumn("middle_name_raw",
        F.when((F.col("contact_name").isNotNull()) & (F.col("name_word_count") > 2),
            F.array_join(F.slice(F.col("name_parts"), 2, F.col("name_word_count") - 2), " ")
        ).otherwise(""))
    
    df_output = df.withColumn("first_name", F.initcap(F.col("first_name_raw"))) \
                  .withColumn("last_name", F.initcap(F.col("last_name_raw"))) \
                  .withColumn("middle_name", F.initcap(F.col("middle_name_raw"))) \
                  .drop("name_parts", "name_word_count", "first_name_raw", "last_name_raw", "middle_name_raw")

    advanced_logger(message="Contact name parsing completed", location="SPLIT_CONTACT_NAMES")
    return df_output

def calculate_customer_metrics(input_df):
    advanced_logger(message="Calculating comprehensive customer metrics", location="CALCULATE_CUSTOMER_METRICS")
    
    input_df.createOrReplaceTempView("parsed_customer_data_view")
    
    sql_query = f"""
    SELECT 
        customer_id,
        company_name,
        first_name,
        last_name,
        middle_name,
        country,
        city,
        phone,
        geographic_region,
        COUNT(DISTINCT order_id) as total_orders,
        COUNT(DISTINCT category_name) as categories_purchased,
        SUM(net_amount) as total_revenue,
        AVG(net_amount) as avg_order_value,
        SUM(discount_amount) as total_discounts_received,
        MIN(order_date) as first_order_date,
        MAX(order_date) as last_order_date,
        SUM(net_amount) * 0.4 + COUNT(DISTINCT order_id) * 50 + COUNT(DISTINCT category_name) * 25 as customer_score,
        CAST(MAX(order_date) AS INT) - CAST(MIN(order_date) AS INT) as customer_lifespan_days,
        SUM(CASE WHEN transaction_tier = 'Premium' THEN 1 ELSE 0 END) as premium_transactions,
        SUM(CASE WHEN transaction_tier = 'Standard' THEN 1 ELSE 0 END) as standard_transactions,
        SUM(CASE WHEN transaction_tier = 'Basic' THEN 1 ELSE 0 END) as basic_transactions,
        STDDEV_SAMP(net_amount) as revenue_volatility,
        MAX(net_amount) as highest_order_value,
        MIN(net_amount) as lowest_order_value
    FROM parsed_customer_data_view
    GROUP BY customer_id, company_name, first_name, last_name, middle_name, 
             country, city, phone, geographic_region
    HAVING SUM(net_amount) > {min_revenue_threshold}
    ORDER BY total_revenue DESC
    """
    
    df_customer_performance = spark.sql(sql_query)
    
    advanced_logger(message="Customer metrics calculation completed", location="CALCULATE_CUSTOMER_METRICS")
    return df_customer_performance

def rank_customers_advanced(input_df):
    advanced_logger(message="Applying advanced customer ranking algorithms", location="RANK_CUSTOMERS_ADVANCED")
    
    percentiles = input_df.approxQuantile("total_revenue", [0.2, 0.4, 0.6, 0.8], 0.0)
    p20, p40, p60, p80 = percentiles[0], percentiles[1], percentiles[2], percentiles[3]
    
    df_classified = input_df.withColumn("performance_category",
        F.when(F.col("total_revenue") >= p80, 'Top Performer')
         .when(F.col("total_revenue") >= p60, 'High Performer')
         .when(F.col("total_revenue") >= p40, 'Average Performer')
         .otherwise('Below Average')
    ).withColumn("loyalty_segment",
        F.when((F.col("customer_lifespan_days") >= 300) & (F.col("total_orders") >= 10), 'Loyal Champion')
         .when((F.col("customer_lifespan_days") >= 200) & (F.col("total_orders") >= 5), 'Engaged Customer')
         .when(F.col("total_orders") >= 3, 'Regular Customer')
         .otherwise('New Customer')
    )
    
    df_with_dummy = df_classified.withColumn("dummy_partition", F.lit(1))
    
    window_overall = Window.partitionBy("dummy_partition").orderBy(F.col("total_revenue").desc(), F.col("customer_score").desc())
    df_ranked1 = df_with_dummy.withColumn("revenue_rank", F.rank().over(Window.partitionBy("dummy_partition").orderBy(F.col("total_revenue").desc()))) \
                              .withColumn("overall_rank", F.rank().over(Window.partitionBy("dummy_partition").orderBy(F.col("customer_score").desc())))

    window_regional = Window.partitionBy("geographic_region").orderBy(F.col("total_revenue").desc())
    df_output = df_ranked1.withColumn("regional_rank", F.rank().over(window_regional)).drop("dummy_partition")

    advanced_logger(message="Customer ranking completed", location="RANK_CUSTOMERS_ADVANCED")
    return df_output

def mask_sensitive_data_func(input_df):
    advanced_logger(message="Applying data masking for privacy protection", location="MASK_SENSITIVE_DATA")
    
    if mask_sensitive_data.upper() == 'Y':
        df_output = input_df.withColumn("masked_phone",
            F.when((F.col("phone").isNotNull()) & (F.length(F.trim(F.col("phone"))) >= 6),
                F.concat(F.substring(F.col("phone"), 1, 3), F.lit("XXX"), F.expr("substring(phone, length(trim(phone))-2)"))
            ).otherwise(F.col("phone"))
        ).withColumn("masked_customer_id",
            F.concat(F.lit("CUST_"), F.md5(F.col("customer_id")))
        ).withColumn("display_company_name",
            F.when(F.length(F.col("company_name")) > 20,
                F.concat(F.substring(F.col("company_name"), 1, 17), F.lit("..."))
            ).otherwise(F.col("company_name"))
        )
    else:
        df_output = input_df.withColumn("masked_phone", F.col("phone")) \
                              .withColumn("masked_customer_id", F.col("customer_id")) \
                              .withColumn("display_company_name", F.col("company_name"))

    advanced_logger(message="Data masking completed", location="MASK_SENSITIVE_DATA")
    return df_output

def create_stacked_metrics(input_df):
    advanced_logger(message="Creating stacked metrics for trend analysis", location="CREATE_STACKED_METRICS")
    
    base_cols = ["customer_id", "company_name", "geographic_region", "performance_category"]
    
    df_revenue = input_df.select(*base_cols, F.lit("Revenue").alias("metric_type"), F.col("total_revenue").alias("metric_value"))
    df_orders = input_df.select(*base_cols, F.lit("Orders").alias("metric_type"), F.col("total_orders").alias("metric_value"))
    df_categories = input_df.select(*base_cols, F.lit("Categories").alias("metric_type"), F.col("categories_purchased").alias("metric_value"))
    df_score = input_df.select(*base_cols, F.lit("Customer Score").alias("metric_type"), F.col("customer_score").alias("metric_value"))
    
    df_output = df_revenue.unionByName(df_orders).unionByName(df_categories).unionByName(df_score)
    
    advanced_logger(message="Stacked metrics creation completed", location="CREATE_STACKED_METRICS")
    return df_output

def generate_intelligence_reports(input_df):
    advanced_logger(message="Generating comprehensive intelligence reports", location="GENERATE_INTELLIGENCE_REPORTS")
    
    print("\n--- Customer Intelligence Platform - Executive Dashboard ---")
    print("--- Advanced Analytics and Performance Insights ---\n")
    
    input_df.createOrReplaceTempView("final_intel_view")
    
    # Executive Summary
    sql_summary = """
    SELECT 'Total Customers Analyzed' as metric, CAST(COUNT(DISTINCT customer_id) AS STRING) as value, 'COUNT' as metric_type FROM final_intel_view
    UNION ALL
    SELECT 'Total Revenue Generated' as metric, FORMAT_STRING('$%,.2f', SUM(total_revenue)) as value, 'CURRENCY' as metric_type FROM final_intel_view
    UNION ALL
    SELECT 'Average Customer Value' as metric, FORMAT_STRING('$%,.2f', AVG(total_revenue)) as value, 'CURRENCY' as metric_type FROM final_intel_view
    UNION ALL
    SELECT 'Top Performer Revenue Threshold' as metric, FORMAT_STRING('$%,.2f', MIN(CASE WHEN performance_category = 'Top Performer' THEN total_revenue END)) as value, 'CURRENCY' as metric_type FROM final_intel_view
    """
    df_executive_summary = spark.sql(sql_summary)
    print("\n--- Executive Summary - Key Performance Indicators ---")
    df_executive_summary.show(truncate=False)

    # Performance Category Analysis
    print("\n--- Customer Performance Distribution ---")
    df_freq = input_df.stat.crosstab("performance_category", "loyalty_segment")
    df_freq.show(truncate=False)

    # Geographic Performance Analysis
    print("\n--- Geographic Performance Analysis ---")
    df_tabulate = input_df.groupBy("geographic_region").pivot("performance_category").agg(
        F.count("total_revenue").alias("n_revenue"),
        F.sum("total_revenue").alias("sum_revenue"),
        F.mean("total_revenue").alias("mean_revenue"),
        F.sum("total_orders").alias("sum_orders"),
        F.mean("total_orders").alias("mean_orders")
    )
    df_tabulate.show(truncate=False)
    
    # Top Performers Detail
    print("\n--- Top 20 Customer Champions ---")
    df_top_performers = input_df.filter(F.col("performance_category") == 'Top Performer').orderBy(F.col("overall_rank")).limit(20)
    df_top_performers.select(
        "display_company_name", "geographic_region", 
        F.format_string("$%,.2f", F.col("total_revenue")).alias("total_revenue"), 
        "total_orders", "loyalty_segment", "overall_rank", "regional_rank"
    ).show(truncate=False)

    advanced_logger(message="Intelligence reports generated successfully", location="GENERATE_INTELLIGENCE_REPORTS")

def main_intelligence_pipeline():
    pipeline_start_time = time.time()
    
    advanced_logger(message="Starting Customer Intelligence Platform Pipeline", 
                   level="INFO", location="MAIN_INTELLIGENCE_PIPELINE", include_timing='Y')
    
    # Step 1: Database Connection
    connect_northwind(max_retries=3)
    
    # Step 2: Extract Base Data
    df_raw_customer_data = extract_base_data()
    
    # Step 3: Remove Duplicates
    df_clean_customer_data = remove_duplicates_advanced(
        input_df=df_raw_customer_data,
        key_vars=["customer_id", "order_id", "product_name"]
    )
    
    # Step 4: Split Contact Names
    df_parsed_customer_data = split_contact_names(
        input_df=df_clean_customer_data
    )
    
    # Step 5: Calculate Customer Metrics
    df_customer_performance = calculate_customer_metrics(
        input_df=df_parsed_customer_data
    )
    
    # Step 6: Rank Customers
    df_ranked_customers = rank_customers_advanced(
        input_df=df_customer_performance
    )
    
    # Step 7: Mask Sensitive Data
    df_final_customer_intelligence = mask_sensitive_data_func(
        input_df=df_ranked_customers
    )
    df_final_customer_intelligence.cache() # Caching for use in reports and metrics
    
    # Step 8: Create Stacked Metrics
    df_stacked_metrics = create_stacked_metrics(
        input_df=df_final_customer_intelligence
    )
    
    # Step 9: Generate Reports
    generate_intelligence_reports(input_table=df_final_customer_intelligence)
    
    pipeline_end_time = time.time()
    pipeline_duration = pipeline_end_time - pipeline_start_time
    
    # NOTE: The SAS time12. format is complex. Showing seconds is a direct equivalent.
    advanced_logger(message=f"Customer Intelligence Pipeline completed successfully in {pipeline_duration:.2f} seconds", 
                   level="INFO", location="MAIN_INTELLIGENCE_PIPELINE", include_timing='Y')
    
    return df_final_customer_intelligence, df_stacked_metrics

if __name__ == "__main__":
    df_final_customer_intelligence, df_stacked_metrics = main_intelligence_pipeline()

    print("\n--- Final Dataset Summary: final_customer_intelligence ---")
    print("Schema:")
    df_final_customer_intelligence.printSchema()
    print("Sample Data:")
    df_final_customer_intelligence.show(5, truncate=False)

    print("\n--- Final Dataset Summary: stacked_metrics ---")
    print("Schema:")
    df_stacked_metrics.printSchema()
    print("Sample Data:")
    df_stacked_metrics.show(5, truncate=False)

#End-DBShift