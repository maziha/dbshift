import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
import time
import datetime

spark = SparkSession.builder.appName("AdvancedCustomerIntelligence").getOrCreate()

start_year = 1997
end_year = 1998
min_revenue_threshold = 500
debug_mode = 'Y'
output_path = '/tmp'
mask_sensitive_data = 'Y'
enable_quality_checks = 'Y'

def advanced_logger(message="", level="INFO", location="MAIN", include_timing=False):
    if debug_mode.upper() == 'Y':
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"{level}: [{timestamp}] [{location}] {message}"
        
        # NOTE: Memory information like in SAS getoption(memsize) is not directly available per-operation.
        # This part of the logic is omitted as it has no direct, simple equivalent in PySpark.
        print(log_message)

def connect_northwind(max_retries=3):
    advanced_logger(message="Initiating Northwind database connection", level="INFO", location="CONNECT_NORTHWIND")
    
    retry_count = 0
    connection_status = "FAILED"
    conn_test = 0
    
    jdbc_properties = {
        "user": "root",
        "password": "Systech123",
        "driver": "org.postgresql.Driver"
    }
    jdbc_url = "jdbc:postgresql://172.190.194.46:5432/northwind"

    while retry_count < max_retries and connection_status == "FAILED":
        retry_count += 1
        try:
            # Test connection by trying to read the customers table
            df_test = spark.read.jdbc(url=jdbc_url, table="public.customers", properties=jdbc_properties)
            conn_test = df_test.count()
            
            if conn_test > 0:
                connection_status = "SUCCESS"
                advanced_logger(message=f"Connection successful on attempt {retry_count} - Found {conn_test} customers",
                               level="INFO", location="CONNECT_NORTHWIND")
            else:
                raise Exception("No records found in customers table.")
        except Exception as e:
            advanced_logger(message=f"Connection attempt {retry_count} failed: {e}", level="WARNING", location="CONNECT_NORTHWIND")
            if retry_count < max_retries:
                advanced_logger(message="Retrying connection in 2 seconds", level="INFO", location="CONNECT_NORTHWIND")
                time.sleep(2)

    if connection_status == "FAILED":
        advanced_logger(message="All connection attempts failed", level="ERROR", location="CONNECT_NORTHWIND")
        raise ConnectionError("Failed to connect to the Northwind database.")
    
    return jdbc_url, jdbc_properties

def data_quality_assessment(df_input):
    advanced_logger(message="Starting data quality assessment", location="DATA_QUALITY_ASSESSMENT")

    total_records = df_input.count()
    unique_records = df_input.select(F.concat_ws('|', "customer_id", "order_id", "product_name")).distinct().count()
    duplicate_count = total_records - unique_records

    if duplicate_count > 0:
        advanced_logger(message=f"Found {duplicate_count} duplicate records", level="WARNING", location="DATA_QUALITY_ASSESSMENT")

    missing_customers = df_input.filter(F.col("customer_id").isNull()).count()
    missing_amounts = df_input.filter(F.col("net_amount").isNull() | (F.col("net_amount") <= 0)).count()
    missing_dates = df_input.filter(F.col("order_date").isNull()).count()

    advanced_logger(message=f"Quality Check - Missing customers: {missing_customers}. Missing amounts: {missing_amounts}. Missing dates: {missing_dates}.",
                   level="INFO", location="DATA_QUALITY_ASSESSMENT")

def extract_base_data(jdbc_url, jdbc_properties):
    advanced_logger(message="Starting comprehensive data extraction", location="EXTRACT_BASE_DATA", include_timing=True)
    
    # Read all required tables from the database
    df_customers = spark.read.jdbc(url=jdbc_url, table="public.customers", properties=jdbc_properties)
    df_orders = spark.read.jdbc(url=jdbc_url, table="public.orders", properties=jdbc_properties)
    df_order_details = spark.read.jdbc(url=jdbc_url, table="public.order_details", properties=jdbc_properties)
    df_products = spark.read.jdbc(url=jdbc_url, table="public.products", properties=jdbc_properties)
    df_categories = spark.read.jdbc(url=jdbc_url, table="public.categories", properties=jdbc_properties)
    df_employees = spark.read.jdbc(url=jdbc_url, table="public.employees", properties=jdbc_properties)
    df_shippers = spark.read.jdbc(url=jdbc_url, table="public.shippers", properties=jdbc_properties)

    # Register tables as temporary views to use in spark.sql
    df_customers.createOrReplaceTempView("customers")
    df_orders.createOrReplaceTempView("orders")
    df_order_details.createOrReplaceTempView("order_details")
    df_products.createOrReplaceTempView("products")
    df_categories.createOrReplaceTempView("categories")
    df_employees.createOrReplaceTempView("employees")
    df_shippers.createOrReplaceTempView("shippers")
    
    df_base_customer_data = spark.sql(f"""
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
    """).orderBy("customer_id", "order_date")
    
    if enable_quality_checks.upper() == 'Y':
        data_quality_assessment(df_base_customer_data)
    
    advanced_logger(message="Base data extraction completed", location="EXTRACT_BASE_DATA")
    return df_base_customer_data

def remove_duplicates_advanced(df_input, key_vars=["customer_id", "order_id", "product_name"]):
    advanced_logger(message="Removing duplicates using advanced logic", location="REMOVE_DUPLICATES_ADVANCED")
    
    window_spec = Window.partitionBy(*key_vars).orderBy(F.col("net_amount").desc())
    
    df_output = df_input.withColumn("row_num", F.row_number().over(window_spec)) \
                        .filter(F.col("row_num") == 1) \
                        .drop("row_num")
    
    final_count = df_output.count()
    advanced_logger(message=f"Duplicate removal completed - Final record count: {final_count}", location="REMOVE_DUPLICATES_ADVANCED")
    return df_output

def split_contact_names(df_input):
    advanced_logger(message="Splitting and standardizing contact names", location="SPLIT_CONTACT_NAMES")

    df_with_split = df_input.withColumn("contact_name_arr", F.split(F.col("contact_name"), " "))
    df_with_counts = df_with_split.withColumn("name_word_count", F.size(F.col("contact_name_arr")))

    df_output = df_with_counts.withColumn(
        "first_name",
        F.when(F.col("contact_name").isNotNull(), F.initcap(F.col("contact_name_arr").getItem(0))).otherwise(F.lit(""))
    ).withColumn(
        "last_name",
        F.when(F.col("name_word_count") >= 2, F.initcap(F.col("contact_name_arr").getItem(F.col("name_word_count") - 1))).otherwise(F.lit(""))
    ).withColumn(
        "middle_name",
        F.when(
            F.col("name_word_count") > 2,
            F.initcap(F.expr("array_join(slice(contact_name_arr, 2, name_word_count - 2), ' ')"))
        ).otherwise(F.lit(""))
    ).drop("contact_name_arr", "name_word_count")
    
    advanced_logger(message="Contact name parsing completed", location="SPLIT_CONTACT_NAMES")
    return df_output

def calculate_customer_metrics(df_input):
    advanced_logger(message="Calculating comprehensive customer metrics", location="CALCULATE_CUSTOMER_METRICS")
    
    df_input.createOrReplaceTempView("parsed_customer_data")

    df_customer_performance = spark.sql(f"""
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
            DATEDIFF(MAX(order_date), MIN(order_date)) as customer_lifespan_days,
            SUM(CASE WHEN transaction_tier = 'Premium' THEN 1 ELSE 0 END) as premium_transactions,
            SUM(CASE WHEN transaction_tier = 'Standard' THEN 1 ELSE 0 END) as standard_transactions,
            SUM(CASE WHEN transaction_tier = 'Basic' THEN 1 ELSE 0 END) as basic_transactions,
            STDDEV_SAMP(net_amount) as revenue_volatility,
            MAX(net_amount) as highest_order_value,
            MIN(net_amount) as lowest_order_value
        FROM parsed_customer_data
        GROUP BY customer_id, company_name, first_name, last_name, middle_name,
                 country, city, phone, geographic_region
        HAVING SUM(net_amount) > {min_revenue_threshold}
        ORDER BY total_revenue DESC
    """)
    
    advanced_logger(message="Customer metrics calculation completed", location="CALCULATE_CUSTOMER_METRICS")
    return df_customer_performance

def rank_customers_advanced(df_input):
    advanced_logger(message="Applying advanced customer ranking algorithms", location="RANK_CUSTOMERS_ADVANCED")

    percentiles = df_input.approxQuantile("total_revenue", [0.2, 0.4, 0.6, 0.8], 0.0)
    p20, p40, p60, p80 = percentiles[0], percentiles[1], percentiles[2], percentiles[3]

    df_classified = df_input.withColumn("performance_category",
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

    # Add overall rankings (No BY statement in SAS PROC RANK)
    # Per rules, create a dummy partition to avoid performance issues
    df_with_dummy_partition = df_classified.withColumn("dummy_partition", F.lit(1))
    window_overall = Window.partitionBy("dummy_partition").orderBy(F.col("total_revenue").desc())
    window_score = Window.partitionBy("dummy_partition").orderBy(F.col("customer_score").desc())
    
    df_ranked_overall = df_with_dummy_partition.withColumn("revenue_rank", F.rank().over(window_overall)) \
                                               .withColumn("overall_rank", F.rank().over(window_score)) \
                                               .drop("dummy_partition")

    # Add regional rankings (BY geographic_region)
    window_regional = Window.partitionBy("geographic_region").orderBy(F.col("total_revenue").desc())
    df_ranked_final = df_ranked_overall.withColumn("regional_rank", F.rank().over(window_regional))
    
    advanced_logger(message="Customer ranking completed", location="RANK_CUSTOMERS_ADVANCED")
    return df_ranked_final

def mask_sensitive_data_func(df_input):
    advanced_logger(message="Applying data masking for privacy protection", location="MASK_SENSITIVE_DATA")
    
    df_output = df_input
    
    if mask_sensitive_data.upper() == 'Y':
        df_output = df_output.withColumn("masked_phone",
            F.when(
                (F.col("phone").isNotNull()) & (F.length(F.trim(F.col("phone"))) >= 6),
                F.concat(F.substring(F.col("phone"), 1, 3), F.lit('XXX'), F.substring(F.col("phone"), F.length(F.trim(F.col("phone"))) - 2, 3))
            ).otherwise(F.col("phone"))
        ).withColumn("masked_customer_id",
            F.concat(F.lit('CUST_'), F.md5(F.col("customer_id")))
        ).withColumn("display_company_name",
            F.when(
                F.length(F.col("company_name")) > 20,
                F.concat(F.substring(F.col("company_name"), 1, 17), F.lit('...'))
            ).otherwise(F.col("company_name"))
        )
    else:
        df_output = df_output.withColumn("masked_phone", F.col("phone")) \
                             .withColumn("masked_customer_id", F.col("customer_id")) \
                             .withColumn("display_company_name", F.col("company_name"))
                             
    advanced_logger(message="Data masking completed", location="MASK_SENSITIVE_DATA")
    return df_output

def create_stacked_metrics(df_input):
    advanced_logger(message="Creating stacked metrics for trend analysis", location= "CREATE_STACKED_METRICS")

    # This uses spark.sql.functions.expr("stack(...)") for a direct translation of the DATA step logic
    stack_expr = "stack(4, 'Revenue', total_revenue, 'Orders', total_orders, 'Categories', categories_purchased, 'Customer Score', customer_score) as (metric_type, metric_value)"

    df_stacked = df_input.select(
        "customer_id",
        "company_name",
        "geographic_region",
        "performance_category",
        F.expr(stack_expr)
    )

    advanced_logger(message="Stacked metrics creation completed", location="CREATE_STACKED_METRICS")
    return df_stacked
    
def generate_intelligence_reports(df_input):
    advanced_logger(message="Generating comprehensive intelligence reports", location="GENERATE_INTELLIGENCE_REPORTS")
    
    print("\n--- Customer Intelligence Platform - Executive Dashboard ---")
    print("--- Advanced Analytics and Performance Insights ---\n")
    
    df_input.createOrReplaceTempView("final_intelligence")

    # Executive Summary
    df_executive_summary = spark.sql("""
        SELECT 'Total Customers Analyzed' as metric, CAST(COUNT(DISTINCT customer_id) AS STRING) as value, 'COUNT' as metric_type FROM final_intelligence
        UNION ALL
        SELECT 'Total Revenue Generated' as metric, FORMAT_STRING('%,.0f', SUM(total_revenue)) as value, 'CURRENCY' as metric_type FROM final_intelligence
        UNION ALL
        SELECT 'Average Customer Value' as metric, FORMAT_STRING('%,.0f', AVG(total_revenue)) as value, 'CURRENCY' as metric_type FROM final_intelligence
        UNION ALL
        SELECT 'Top Performer Revenue Threshold' as metric, FORMAT_STRING('%,.0f', MIN(CASE WHEN performance_category = 'Top Performer' THEN total_revenue END)) as value, 'CURRENCY' as metric_type FROM final_intelligence
    """)
    
    print("--- Executive Summary - Key Performance Indicators ---")
    df_executive_summary.select("metric", "value").show(truncate=False)

    # Performance Category Analysis
    print("--- Customer Performance Distribution ---")
    df_freq_analysis = df_input.stat.crosstab("performance_category", "loyalty_segment")
    df_freq_analysis.show()

    # Geographic Performance Analysis
    print("--- Geographic Performance Analysis ---")
    df_geo_analysis = df_input.groupBy("geographic_region", "performance_category").agg(
        F.count("total_revenue").alias("n"),
        F.format_string("$%,.2f", F.sum("total_revenue")).alias("sum_total_revenue"),
        F.format_string("$%,.2f", F.mean("total_revenue")).alias("mean_total_revenue"),
        F.format_number(F.sum("total_orders"), "##,###,##0").alias("sum_total_orders"),
        F.format_number(F.mean("total_orders"), "##,###,##0").alias("mean_total_orders")
    ).orderBy("geographic_region", "performance_category")
    df_geo_analysis.show(truncate=False)
    
    # Top Performers Detail
    print("--- Top 20 Customer Champions ---")
    df_top_performers = df_input.filter(F.col("performance_category") == 'Top Performer') \
                                .select(
                                    "display_company_name", "geographic_region",
                                    F.format_string("$%,.2f", F.col("total_revenue")).alias("total_revenue"),
                                    "total_orders", "loyalty_segment", "overall_rank", "regional_rank"
                                )
    df_top_performers.show(20, truncate=False)

    advanced_logger(message="Intelligence reports generated successfully", location="GENERATE_INTELLIGENCE_REPORTS")

def main_intelligence_pipeline():
    pipeline_start_time = time.time()
    advanced_logger(message="Starting Customer Intelligence Platform Pipeline", 
                   level="INFO", location="MAIN_INTELLIGENCE_PIPELINE", include_timing=True)
    
    # Step 1: Database Connection
    jdbc_url, jdbc_properties = connect_northwind(max_retries=3)
    
    # Step 2: Extract Base Data
    df_raw_customer_data = extract_base_data(jdbc_url, jdbc_properties)
    
    # Step 3: Remove Duplicates
    df_clean_customer_data = remove_duplicates_advanced(
        df_input=df_raw_customer_data,
        key_vars=["customer_id", "order_id", "product_name"]
    )
    
    # Step 4: Split Contact Names
    df_parsed_customer_data = split_contact_names(
        df_input=df_clean_customer_data
    )
    
    # Step 5: Calculate Customer Metrics
    df_customer_performance = calculate_customer_metrics(
        df_input=df_parsed_customer_data
    )
    
    # Step 6: Rank Customers
    df_ranked_customers = rank_customers_advanced(
        df_input=df_customer_performance
    )
    
    # Step 7: Mask Sensitive Data
    df_final_customer_intelligence = mask_sensitive_data_func(
        df_input=df_ranked_customers
    )
    
    # Step 8: Create Stacked Metrics
    df_stacked_metrics = create_stacked_metrics(
        df_input=df_final_customer_intelligence
    )
    
    # Step 9: Generate Reports
    generate_intelligence_reports(df_input=df_final_customer_intelligence)
    
    pipeline_end_time = time.time()
    pipeline_duration = pipeline_end_time - pipeline_start_time
    
    duration_formatted = time.strftime('%H:%M:%S', time.gmtime(pipeline_duration))
    advanced_logger(message=f"Customer Intelligence Pipeline completed successfully in {duration_formatted}", 
                   level="INFO", location="MAIN_INTELLIGENCE_PIPELINE", include_timing=True)
    
    # Expose final dataframes for potential further use in the notebook
    # Display final datasets information
    print("\n--- Final Dataset Summary: final_customer_intelligence ---")
    df_final_customer_intelligence.printSchema()
    
    print("\n--- Final Dataset Summary: stacked_metrics ---")
    df_stacked_metrics.printSchema()

    # Saving final tables to the metastore (as an example of persistence)
    df_final_customer_intelligence.write.format("delta").mode("overwrite").saveAsTable("work.final_customer_intelligence")
    df_stacked_metrics.write.format("delta").mode("overwrite").saveAsTable("work.stacked_metrics")

if __name__ == "__main__":
    main_intelligence_pipeline()
#End-DBShift