import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def pids_model_sort(L6, jdbc_url, user, password):
    
    inner_teradata_query = f"(sel * from dl_cntl_adva.fcst_p_upc_inf where mjr_mds_are_id = {L6}) as t"

    df_from_td = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", inner_teradata_query) \
        .option("user", user) \
        .option("password", password) \
        .load()

    df_pairs = df_from_td.select("mjr_p_cls_id", "L2").distinct().orderBy(F.desc("L2"))

    pairs_to_process = df_pairs.collect()

    for j, row in enumerate(pairs_to_process):
        L2_id = row['mjr_p_cls_id']
        string2 = row['L2']

        print(f"{j + 1}")
        print(f"{string2}")

        table_to_sort = f"promo.pid_{string2}"
        
        if spark.catalog.tableExists(table_to_sort):
            
            df_to_sort = spark.table(table_to_sort)
            
            df_sorted = df_to_sort.orderBy("p_id", "ut_id", "cldr_day_of_wk_id")
            
            df_sorted.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_to_sort)

# It is assumed that the following variables are defined in the execution environment
# (e.g., via Databricks widgets or configuration files):
# L6_id, jdbc_url, user, password
pids_model_sort(L6=L6_id, jdbc_url=jdbc_url, user=user, password=password)

#End-DBShift