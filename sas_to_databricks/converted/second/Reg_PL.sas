import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, DateType, IntegerType
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from datetime import datetime, date, timedelta

spark = SparkSession.builder.appName("RegSPD").getOrCreate()

# These variables are assumed to be pre-defined in the Databricks environment
# Example values are provided for context.
fcst_log = "/tmp/fcst_log"
fcst_coef = "/tmp/fcst_coef"
archive = "/tmp/archive"
L6_id = "some_l6_id"
LastPreWeek = "2023-12-31"
ValidWks = 4
train = "N"
Update = "N"
mo_dt = "2023-01-01"

# JDBC connection properties for Teradata would be defined here
# jdbc_url = "jdbc:teradata://..."
# connection_properties = {"user": "...", "password": "...", "driver": "com.teradata.jdbc.TeraDriver"}

ModelType = "Reg_PL"
type = "Reg_PL"
tdate = datetime.now().strftime("%m%d%y")

md = datetime.now().day
print(md)
print(f"today month day is {md}")


def pids_model_reg_pl(L6, LastPrWk, ValidWeeks, retrain, Update_M, mo_date):
    # proc printto is for logging, not directly translated in PySpark data processing.

    if Update_M == 'Y':
        train_m_db = f"{fcst_coef.replace('/', '_')}_{type}"
        train_a_db = f"{archive.replace('/', '_')}_{type}"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {train_m_db}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {train_a_db}")
        ValidWeeks = 0
    else:
        train_m_db = f"{fcst_coef.replace('/', '_')}_{type}_Train"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {train_m_db}")

    now = date.today()
    print(now)
    d = now.day
    print(d)
    
    # PROC SQL to create l2_data_coef
    # This logic reconstructs SAS's dictionary.tables by inspecting the Spark catalog.
    tables_in_db = spark.catalog.listTables(train_m_db)
    l2_data_list = []
    for table in tables_in_db:
        if table.name.startswith("pid_") and table.name.endswith("_coef"):
            try:
                history_df = spark.sql(f"DESCRIBE HISTORY {train_m_db}.{table.name}")
                modate = history_df.orderBy(F.col("version").desc()).select("timestamp").first()[0]
                l2 = table.name[4:-5][:9] # substr(substr(memname,5,-1),1,9) logic
                l2_data_list.append((l2, modate))
            except Exception as e:
                # Table might not be a delta table or has no history
                print(f"Could not get history for {table.name}: {e}")
                continue

    if l2_data_list:
        l2_data_coef_schema = StructType([
            StructField("l2", StringType(), True),
            StructField("modate", TimestampType(), True)
        ])
        df_l2_data_coef = spark.createDataFrame(l2_data_list, schema=l2_data_coef_schema)
        df_l2_data_coef = df_l2_data_coef.orderBy(F.col("modate"))
        df_l2_data_coef.createOrReplaceTempView("l2_data_coef")
    else:
        # Create an empty view if no tables are found to avoid SQL errors
        spark.createDataFrame([], StructType([StructField("l2", StringType(), True), StructField("modate", TimestampType(), True)])).createOrReplaceTempView("l2_data_coef")


    # PROC SQL to select into macro variables
    spark.read.table("mtest.fcst_p_upc_inf").createOrReplaceTempView("fcst_p_upc_inf")
    spark.read.table("mtest.fcst_seasonal_index").createOrReplaceTempView("fcst_seasonal_index")
    spark.read.table("mtest.model_upd_track_train").createOrReplaceTempView("model_upd_track_train")

    sql_query_for_l2s = f"""
        SELECT
            a.mjr_p_cls_id,
            a.L2,
            SUBSTR(a.mjr_mds_are_nm_tx, 1, 2) AS L6_nm,
            b.seasonal_indx
        FROM fcst_p_upc_inf AS a
        LEFT JOIN fcst_seasonal_index AS b ON a.mjr_p_cls_id = b.mjr_p_cls_id
        LEFT JOIN l2_data_coef AS c ON a.l2 = c.l2
        INNER JOIN model_upd_track_train AS m ON a.l2 = m.l2
        WHERE a.mjr_mds_are_id = {L6}
        AND a.mjr_p_cls_id NOT IN ('L2-010051')
        AND (m.newdate < '{mo_date}' OR CAST(m.bytes AS DOUBLE) <= 131072)
        AND m.model = '{ModelType}'
        ORDER BY c.modate ASC
    """
    df_l2_info = spark.sql(sql_query_for_l2s)
    
    l2_info_collected = df_l2_info.agg(
        F.count(F.lit(1)).alias("nL2"),
        F.collect_list("mjr_p_cls_id").alias("L2_ids"),
        F.collect_list("L2").alias("L2_table"),
        F.collect_list("L6_nm").alias("L6_nm"),
        F.collect_list("seasonal_indx").alias("seasonal_indx")
    ).first()

    if l2_info_collected and l2_info_collected['nL2'] > 0:
        nL2 = l2_info_collected['nL2']
        L2_ids_list = l2_info_collected['L2_ids']
        L2_table_list = l2_info_collected['L2_table']
        L6_nm_list = l2_info_collected['L6_nm'] # This assumes one L6_nm per group, taking the first
        L6_nm = L6_nm_list[0] if L6_nm_list else ""
        seasonal_indx_list = l2_info_collected['seasonal_indx']
    else:
        nL2 = 0

    for j in range(nL2):
        L2_id = L2_ids_list[j]
        string2 = L2_table_list[j]
        L2_indx = seasonal_indx_list[j]

        # proc printto is for logging, not directly translated.

        exist = 0
        nobs = 0
        coef_table_name = f"pid_{string2}_coef"
        
        if spark.catalog.tableExists(train_m_db, coef_table_name):
            try:
                history_df = spark.sql(f"DESCRIBE HISTORY {train_m_db}.{coef_table_name}")
                latest_modate = history_df.orderBy(F.col("version").desc()).select("timestamp").first()[0]
                
                # SAS time 72660.739 is 20:11:00.739
                mo_datetime_check = datetime.strptime(mo_date, '%Y-%m-%d').replace(hour=20, minute=11, second=0, microsecond=739000)
                mo_date_check = datetime.strptime(mo_date, '%Y-%m-%d').date()

                if latest_modate.date() > mo_date_check or \
                   (latest_modate.date() == mo_date_check and latest_modate > mo_datetime_check):
                    exist = 1
                
                nobs = spark.table(f"{train_m_db}.{coef_table_name}").count()
            except Exception as e:
                print(f"Could not process history/count for {train_m_db}.{coef_table_name}: {e}")
                exist = 0
                nobs = 0

        print(exist)
        print(nobs)

        promo_table_exists = spark.catalog.tableExists("promo", f"pid_{string2}")

        if promo_table_exists and (exist != 1 or nobs < 10 or retrain == 'Y'):
            if Update_M == 'Y':
                archive_table_name = f"{train_a_db}.{coef_table_name}"
                source_table_name = f"{train_m_db}.{coef_table_name}"
                spark.sql(f"DROP TABLE IF EXISTS {archive_table_name}")
                if spark.catalog.tableExists(train_m_db, coef_table_name):
                    df_to_copy = spark.table(source_table_name)
                    df_to_copy.write.format("delta").mode("overwrite").saveAsTable(archive_table_name)
            
            # Get the training start date from Teradata
            # The SAS code executes the inner SQL on Teradata, then filters in SAS.
            # We will read the results and then apply the MAX.
            inner_sql = f"SELECT CASE WHEN mjr_p_cls_id = '{L2_id}' THEN min_date ELSE DATE'2010-01-01' END AS start_date FROM dl_cntl_adva.training_date GROUP BY 1"
            # df_from_td = spark.read.format("jdbc") \
            #    .option("url", jdbc_url) \
            #    .option("dbtable", f"({inner_sql}) as t") \
            #    .options(**connection_properties) \
            #    .load()
            # train_dt = df_from_td.agg(F.max("start_date")).collect()[0][0]
            
            # Mocking the JDBC call since connection details are not available
            train_dt = date(2010, 1, 1)

            # Create temp table
            df_promo = spark.table(f"promo.pid_{string2}")
            df_st_inf_flg = spark.table("mtest.st_inf_flg")
            df_temp_data_unfiltered = df_promo.join(df_st_inf_flg, "ut_id", "inner")
            
            # PROC REG data filtering
            filter_cond = (
                ~F.col("day_dt").isin([date(2014, 12, 25), date(2015, 12, 25), date(2016, 12, 25), date(2017, 12, 25), date(2018, 12, 25), date(2019, 12, 25), date(2020, 12, 25), date(2021, 12, 25), date(2022, 12, 25)]) &
                (F.col("ut_id").between(19, 399)) &
                (F.col("day_dt") > F.lit(train_dt))
            )

            if L2_indx == 1 and Update_M == 'N':
                last_pr_wk_date = datetime.strptime(LastPrWk, "%Y-%m-%d").date()
                filter_cond = filter_cond & (
                    ((F.col("p_sold_first_dt") <= last_pr_wk_date - timedelta(days=724)) & (F.col("day_dt") <= last_pr_wk_date - timedelta(days=364))) |
                    ((F.col("p_sold_first_dt") > last_pr_wk_date - timedelta(days=724)) & (F.col("day_dt") <= last_pr_wk_date - timedelta(days=(ValidWeeks*7))))
                )
            else:
                last_pr_wk_date = datetime.strptime(LastPrWk, "%Y-%m-%d").date()
                filter_cond = filter_cond & (F.col("day_dt") <= last_pr_wk_date - timedelta(days=(ValidWeeks*7)))
            
            df_temp_data = df_temp_data_unfiltered.filter(filter_cond)

            # PROC REG BY p_id -> Implemented using pandas_udf for grouped regression
            label_col = "ntl_log_mkt_bskt_ut_qt"
            
            feature_cols = [
                "ntl_log_bse_pr_am", "ntl_log_bse_promo_dct_pr_am", "fcl_per_of_yr_id",
                "adv_circ_flg", "adv_mid_wk_flg", "adv_super_evnt_flg", "adv_dgtl_circ_flg",
                "unadv_flg", "p_ten_for_ten_flg",
                "NY", "SuperBowlSat", "DayB4Valentine", "Valentine", "Easter", "EasterWk",
                "MomSat", "Mom", "Memorial", "MemWkend", "DadSat", "Dad", "JulyFour", "Labor",
                "LaborWkEnd", "LaborFri", "LaborSat", "LaborSun", "Columbus", "MCC_BLK_OUT",
                "PreHalo_Fri", "PreHalo_Sat", "PreHalo_Sun",
                "HalloweenEve", "HalloweenFri", "Halloween", "Veterans",
                "FridayB4Turkey", "SaturdayB4Turkey", "TurkeyWed", "Turkey", "BlackFri",
                "RedSat", "DecTwoThree", "DecTwoSix", "SunPreXmas", "XMASEVE", "SUNPNY", "SATPNY", "NYEVE"
            ]
            feature_cols.extend([f"wk{i}" for i in range(2, 53)])
            feature_cols.extend([f"dow{i}" for i in range(2, 8)])
            feature_cols.extend([f"ut{i}" for i in range(19, 325)])
            
            all_cols_for_udf = ["p_id", label_col] + feature_cols
            
            # Define output schema for the pandas UDF
            result_schema_fields = [
                StructField("p_id", StringType(), True),
                StructField("_MODEL_", StringType(), True),
                StructField("_TYPE_", StringType(), True),
                StructField("_DEPVAR_", StringType(), True),
                StructField("_RMSE_", DoubleType(), True),
                StructField("_ADJR_", DoubleType(), True),
                StructField("Intercept", DoubleType(), True)
            ]
            for col in feature_cols:
                result_schema_fields.append(StructField(col, DoubleType(), True))
            result_schema = StructType(result_schema_fields)

            # Define the regression function to be applied to each group
            def ols_on_pandas(pdf):
                import pandas as pd
                import statsmodels.api as sm
                import numpy as np

                p_id_val = pdf['p_id'].iloc[0]
                
                # Check for sufficient data
                if pdf.shape[0] < (len(feature_cols) + 2):
                    return pd.DataFrame() # Return empty if not enough data to fit

                y = pdf[label_col]
                X = pdf[feature_cols]
                X = sm.add_constant(X) # for intercept
                
                # Handle potential perfect collinearity or other issues
                try:
                    model = sm.OLS(y, X, missing='drop').fit()
                except Exception:
                    return pd.DataFrame()

                params = model.params
                
                results_dict = {
                    "p_id": p_id_val,
                    "_MODEL_": "RegDaily",
                    "_TYPE_": "PARMS",
                    "_DEPVAR_": label_col,
                    "_RMSE_": np.sqrt(model.mse_resid),
                    "_ADJR_": model.rsquared_adj,
                    "Intercept": params.get('const', None)
                }
                for col in feature_cols:
                    results_dict[col] = params.get(col, None)
                    
                return pd.DataFrame([results_dict])

            # Run the grouped regression
            df_model_data = df_temp_data.select(all_cols_for_udf)
            df_coef_temp = df_model_data.groupBy("p_id").applyInPandas(ols_on_pandas, schema=result_schema)

            coef_temp_table_name = f"{train_m_db}.{coef_table_name}_temp"
            df_coef_temp.write.format("delta").mode("overwrite").saveAsTable(coef_temp_table_name)
            
            coef_cnt = 0
            if spark.catalog.tableExists(train_m_db, f"{coef_table_name}_temp"):
                coef_cnt = spark.table(coef_temp_table_name).count()
            
            print(coef_cnt)

            if coef_cnt > 0:
                final_coef_table = f"{train_m_db}.{coef_table_name}"
                spark.sql(f"DROP TABLE IF EXISTS {final_coef_table}")
                spark.sql(f"ALTER TABLE {coef_temp_table_name} RENAME TO {final_coef_table}")

                if L2_indx == 0 and Update_M == 'N':
                    train_c_db = f"{fcst_coef.replace('/', '_')}_{type}"
                    spark.sql(f"CREATE DATABASE IF NOT EXISTS {train_c_db}")
                    target_table = f"{train_c_db}.{coef_table_name}"
                    spark.sql(f"DROP TABLE IF EXISTS {target_table}")
                    df_to_copy = spark.table(final_coef_table)
                    df_to_copy.write.format("delta").mode("overwrite").saveAsTable(target_table)

                if Update_M == 'Y':
                    train_c_db = f"{fcst_coef.replace('/', '_')}_{type}_Train"
                    spark.sql(f"CREATE DATABASE IF NOT EXISTS {train_c_db}")
                    target_table = f"{train_c_db}.{coef_table_name}"
                    spark.sql(f"DROP TABLE IF EXISTS {target_table}")
                    df_to_copy = spark.table(final_coef_table)
                    df_to_copy.write.format("delta").mode("overwrite").saveAsTable(target_table)

# Execute the main logic
pids_model_reg_pl(L6=L6_id, LastPrWk=LastPreWeek, ValidWeeks=ValidWks, retrain=train, Update_M=Update, mo_date=mo_dt)

#End-DBShift