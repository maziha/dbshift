import pyspark.sql.functions as F

# This corresponds to the %LET L6_id=... that would be defined before the macro call.
# In a real Databricks environment, this value would likely be passed via a widget.
# For example: L6_id = dbutils.widgets.get("L6_id")
L6_id = 'your_l6_id_here'

# This section translates the `%macro pids_model_sort` logic.

# The first PROC SQL step uses a Teradata passthrough query to fetch metadata.
# SAS: proc sql; &tdcon.; select... from connection to teradata(...)
# The inner query from the `connection to teradata` block is used for the JDBC read.
teradata_inner_query = f"""(
SELECT * FROM dl_cntl_adva.fcst_p_upc_inf
WHERE mjr_mds_are_id = '{L6_id}'
/*and mjr_p_cls_id in ('L2-010076',  'L2-010051',    'L2-010073')*/
)"""

# In a real environment, provide actual JDBC connection details.
# These should be stored securely, for example, in Databricks secrets.
jdbc_url = "jdbc:teradata://your_teradata_host/your_db"
jdbc_user = "your_username"
jdbc_password = "your_password" # e.g., dbutils.secrets.get(scope="your_scope", key="teradata_password")

# Execute the query to get the base data from Teradata.
# This corresponds to the `from connection to teradata(...)` part.
df_from_teradata = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", teradata_inner_query) \
    .option("user", jdbc_user) \
    .option("password", jdbc_password) \
    .load()

# The outer part of the SAS PROC SQL performs a distinct select and orders the result.
# This prepares the list of tables to be sorted.
# SAS: select distinct mjr_p_cls_id, L2 ... order by L2 desc;
df_tables_to_process = df_from_teradata.select("mjr_p_cls_id", "L2").distinct().orderBy(F.desc("L2"))

# Collect the results into a Python list to iterate over. This mimics the SAS %DO loop
# that uses macro variables populated by the `INTO:` clause.
tables_list = df_tables_to_process.collect()

# The SAS log redirections (`proc printto`) are environment-specific and are not translated.
# Databricks captures `print` statements and Spark logs automatically.

# This corresponds to the `%do j=1 %to &nL2.;` loop.
for i, row in enumerate(tables_list):
    # Corresponds to %let L2_id=%scan(...) and %let string2=%scan(...)
    L2_id = row["mjr_p_cls_id"]
    string2 = row["L2"]

    # Corresponds to %put &j.; and %put &string2.;
    print(i + 1)
    print(string2)

    table_to_sort_name = f"promo.pid_{string2}"

    # Corresponds to %if %sysfunc(exist(promo.pid_&string2.)) %then %do;
    if spark.catalog.tableExists(table_to_sort_name):
        
        # The inner logic in SAS checks table filesize to decide between 'sort' and 'tagsort'.
        # This is a SAS-specific performance optimization. Spark's distributed sort mechanism
        # handles large datasets automatically, so this conditional logic is not applicable.
        # The core action in both SAS branches is to sort the table, which is what is translated.

        # Read the table that needs to be sorted.
        df_to_sort = spark.table(table_to_sort_name)

        # Sort the table - corresponds to `proc sort data=...; by p_id ut_id cldr_day_of_wk_id;`
        df_sorted = df_to_sort.orderBy("p_id", "ut_id", "cldr_day_of_wk_id")

        # Overwrite the original table with the sorted data. This is the default behavior
        # of `proc sort` when an `OUT=` clause is not specified.
        df_sorted.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_to_sort_name)

# The final `run;` in SAS is a statement terminator and is not needed in Python.

#End-DBShift