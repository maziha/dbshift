import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# This script assumes that 'spark' is an available SparkSession, as is standard in Databricks.
# The following variables must be configured to match the target environment.
L6_id = "YOUR_L6_ID_VALUE"  # Example: '123'
td_connection_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.teradata.jdbc.TeraDriver"
}
td_url = "jdbc:teradata://your_teradata_host/DATABASE=your_db"


def pids_model_sort(L6):
    
    # This block translates the SAS PROC SQL passthrough query.
    # The inner query is executed via JDBC, and the outer SELECT/ORDER BY logic is applied to the resulting DataFrame.
    sql_query_from_sas = f"""
    (sel * from dl_cntl_adva.fcst_p_upc_inf
    where mjr_mds_are_id = {L6})
    """
    
    df_from_teradata = spark.read.jdbc(
        url=td_url,
        table=sql_query_from_sas,
        properties=td_connection_properties
    )
    
    df_distinct_info = df_from_teradata.select("mjr_p_cls_id", "L2").distinct().orderBy(F.col("L2").asc())
    
    # Collect the results to iterate over them, similar to how the SAS macro loops through the created macro variables.
    # This is appropriate for a list of tables/IDs, which is expected to be manageable in size.
    info_list = df_distinct_info.collect()
    nL2 = len(info_list)
    
    # This loop translates the SAS %DO loop.
    for j, row in enumerate(info_list, 1):
        
        # These assignments translate the %LET statements with the %SCAN function.
        L2_id = row["mjr_p_cls_id"]
        string2 = row["L2"]

        # The 'proc printto' is a SAS environment command to redirect logs and has no direct equivalent in PySpark.
        # The following 'print' statements translate the '%PUT' statements.
        print(j)
        print(string2)
        
        # This translates the %if %sysfunc(exist(...)) check.
        table_name = f"promo.pid_{string2}"
        if spark.catalog.tableExists(table_name):
            
            # This block translates the PROC SQL query against dictionary.tables to check the filesize.
            # The PySpark equivalent is to use DESCRIBE DETAIL and check the sizeInBytes property.
            normsort = 0
            try:
                details_df = spark.sql(f"DESCRIBE DETAIL {table_name}")
                size_in_bytes_row = details_df.select("sizeInBytes").first()
                if size_in_bytes_row and size_in_bytes_row[0] is not None:
                    if size_in_bytes_row[0] > 15742926848:
                        normsort = 1
            except Exception:
                # If size cannot be determined, normsort remains 0, matching the 'else' path.
                pass

            # This translates the %if &normsort. ge 1 %then %do; ... %else %do; logic.
            # In SAS, this decided between a normal sort and a tagsort.
            # In Spark, both translate to .orderBy(), but the logic is preserved for 1:1 translation.
            if normsort >= 1:
                # Translates: proc sort data=...; by...;
                df_to_sort = spark.table(table_name)
                df_sorted = df_to_sort.orderBy("p_id", "ut_id", "cldr_day_of_wk_id")
                df_sorted.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
            else:
                # Translates: proc sort tagsort data=...; by...;
                df_to_sort = spark.table(table_name)
                df_sorted = df_to_sort.orderBy("p_id", "ut_id", "cldr_day_of_wk_id")
                df_sorted.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

# This call translates the execution of the %pids_model_sort macro.
pids_model_sort(L6=L6_id)
#End-DBShift