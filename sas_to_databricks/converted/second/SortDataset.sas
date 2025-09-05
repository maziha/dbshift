import pyspark.sql.functions as F

L6_id = 'some_value'

jdbc_url = "jdbc:teradata://<host>/DATABASE=<db>"
jdbc_user = "your_user"
jdbc_password = "your_password"
jdbc_driver = "com.teradata.jdbc.TeraDriver"

connection_properties = {
  "user": jdbc_user,
  "password": jdbc_password,
  "driver": jdbc_driver
}

def pids_model_sort(L6):
    teradata_query = f"(select * from dl_cntl_adva.fcst_p_upc_inf where mjr_mds_are_id = {L6}) as t"

    df_from_teradata = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", teradata_query) \
        .options(**connection_properties) \
        .load()

    df_from_teradata.createOrReplaceTempView("fcst_p_upc_inf_view")

    df_distinct_l2_info = spark.sql("""
        SELECT DISTINCT
            mjr_p_cls_id,
            L2
        FROM fcst_p_upc_inf_view
        ORDER BY L2 ASC
    """)

    l2_rows = df_distinct_l2_info.collect()

    l2_ids = [row.mjr_p_cls_id for row in l2_rows]
    l2_tables = [row.L2 for row in l2_rows]
    nl2 = len(l2_rows)

    for j in range(nl2):
        l2_id = l2_ids[j]
        string2 = l2_tables[j]

        print(f"Redirecting log for sort_{string2}_s1.txt")

        print(j + 1)
        print(string2)

        table_name = f"promo.pid_{string2}"

        if spark.catalog.tableExists(table_name):
            df_to_sort = spark.table(table_name)

            df_sorted = df_to_sort.orderBy("p_id", "ut_id", "cldr_day_of_wk_id")

            df_sorted.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

pids_model_sort(L6=L6_id)
#End-DBShift