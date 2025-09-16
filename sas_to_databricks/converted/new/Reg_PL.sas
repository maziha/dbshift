import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType, TimestampType
from datetime import datetime
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import numpy as np

# This script assumes the following variables are defined in the Databricks environment
# fcst_log, fcst_coef, archive, L6_id, LastPreWeek, ValidWks, train, Update, mo_dt
# jdbc_url, user, password for Teradata connection

ModelType = "Reg_PL"
type = "Reg_PL"
tdate = datetime.now().strftime("%m%d%Y")
md = datetime.now().day
print(md)
print(f"today month day is {md}")

def pids_model_reg_pl(L6, LastPrWk, ValidWeeks, retrain, Update_M, mo_date):

    tdcon_properties = {"user": user, "password": password, "driver": "com.teradata.jdbc.TeraDriver"}

    if Update_M == 'Y':
        db_path_train_m = f"{fcst_coef}/{type}"
        db_path_train_a = f"{archive}/{type}"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS train_m LOCATION '{db_path_train_m}'")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS train_a LOCATION '{db_path_train_a}'")
        db_train_m = "train_m"
        db_train_a = "train_a"
        ValidWeeks = 0
    else:
        db_path_train_m = f"{fcst_coef}/{type}/Train"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS train_m_train LOCATION '{db_path_train_m}'")
        db_train_m = "train_m_train"

    now_val = datetime.now().date()
    print(now_val)
    d = now_val.day
    print(d)

    try:
        tables_in_db = spark.catalog.listTables(db_train_m)
        df_catalog_tables = spark.createDataFrame(tables_in_db)
        
        df_l2_data_coef = df_catalog_tables.withColumn(
            "l2", F.substring(F.substring(F.col("name"), 5, 255), 1, 9)
        ).select(
            F.col("l2"),
            F.col("lastModified").alias("modate")
        ).orderBy("modate")
    except Exception as e:
        print(f"Could not list tables for {db_train_m}: {e}")
        schema = StructType([
            StructField("l2", StringType(), True),
            StructField("modate", TimestampType(), True)
        ])
        df_l2_data_coef = spark.createDataFrame([], schema)

    df_l2_data_coef.createOrReplaceTempView("l2_data_coef")
    
    sql_details_query = f"""
        SELECT DISTINCT
            a.mjr_p_cls_id,
            a.L2,
            TRIM(SUBSTRING(a.mjr_mds_are_nm_tx, 1, 2)) as L6_nm,
            b.seasonal_indx,
            c.modate
        FROM mtest.fcst_p_upc_inf AS a
        LEFT JOIN mtest.fcst_seasonal_index AS b ON a.mjr_p_cls_id = b.mjr_p_cls_id
        LEFT JOIN l2_data_coef AS c ON a.l2 = c.l2
        INNER JOIN mtest.model_upd_track_train AS m ON a.l2 = m.l2
        WHERE a.mjr_mds_are_id = {L6}
          AND a.mjr_p_cls_id NOT IN ('L2-010051')
          AND (m.newdate < '{mo_date}' OR CAST(m.bytes AS DOUBLE) <= 131072)
          AND m.model = '{ModelType}'
    """

    sql_aggs_query = f"""
        SELECT
            COUNT(DISTINCT a.mjr_p_cls_id),
            ROUND(COUNT(DISTINCT a.mjr_p_cls_id) / 2, 1)
        FROM mtest.fcst_p_upc_inf AS a
        INNER JOIN mtest.model_upd_track_train AS m ON a.l2 = m.l2
        WHERE a.mjr_mds_are_id = {L6}
          AND a.mjr_p_cls_id NOT IN ('L2-010051')
          AND (m.newdate < '{mo_date}' OR CAST(m.bytes AS DOUBLE) <= 131072)
          AND m.model = '{ModelType}'
    """
    
    df_details = spark.sql(sql_details_query).orderBy(F.asc_nulls_last("modate"))
    agg_results_row = spark.sql(sql_aggs_query).first()
    collected_details = df_details.collect()

    if not collected_details:
        print("No L2 IDs found to process. Exiting function.")
        return

    L2_ids = "|".join([row.mjr_p_cls_id for row in collected_details])
    L2_table = "|".join([row.L2 for row in collected_details])
    seasonal_indx_str_list = [str(row.seasonal_indx) if row.seasonal_indx is not None else "0" for row in collected_details]
    seasonal_indx = "|".join(seasonal_indx_str_list)
    
    nL2 = agg_results_row[0] if agg_results_row else 0
    stL2 = agg_results_row[1] if agg_results_row and len(agg_results_row) > 1 else 0
    L6_nm = collected_details[0].L6_nm if collected_details else ""

    L2_ids_list = L2_ids.split('|')
    L2_table_list = L2_table.split('|')
    seasonal_indx_list = seasonal_indx.split('|')

    for j in range(len(L2_ids_list)):
        L2_id = L2_ids_list[j]
        string2 = L2_table_list[j]
        L2_indx = seasonal_indx_list[j]
        
        exist_flag = False
        nobs = 0
        target_table_name = f"pid_{string2}_coef"

        try:
            if spark.catalog.tableExists(f"{db_train_m}.{target_table_name}"):
                detail_df = spark.sql(f"DESCRIBE DETAIL {db_train_m}.{target_table_name}")
                details = detail_df.first()
                mod_datetime = details.lastModified
                mod_date = mod_datetime.date()
                mod_time = mod_datetime.time()
                sas_time = mod_time.hour * 3600 + mod_time.minute * 60 + mod_time.second
                mo_date_dt = datetime.strptime(mo_date, '%Y-%m-%d').date()
                
                if mod_date > mo_date_dt:
                    exist_flag = True
                elif mod_date == mo_date_dt and sas_time > 72660.739:
                    exist_flag = True
                
                nobs = int(details.numOutputRows)
        except Exception as e:
            print(f"Could not get details for {db_train_m}.{target_table_name}: {e}")
            exist_flag = False
            nobs = 0

        print(exist_flag)
        print(nobs)

        promo_table_exists = spark.catalog.tableExists("promo", f"pid_{string2}")

        if promo_table_exists and (not exist_flag or nobs < 10 or retrain == 'Y'):
            if Update_M == 'Y':
                source_table = f"{db_train_m}.{target_table_name}"
                archive_table = f"{db_train_a}.{target_table_name}"
                if spark.catalog.tableExists(source_table):
                    spark.sql(f"DROP TABLE IF EXISTS {archive_table}")
                    df_to_copy = spark.table(source_table)
                    df_to_copy.write.format("delta").mode("overwrite").saveAsTable(archive_table)

            sql_query_teradata = f"""
            SELECT
                CASE WHEN mjr_p_cls_id = '{L2_id}' THEN min_date ELSE CAST('2010-01-01' AS DATE) END AS start_date
            FROM  dl_cntl_adva.training_date
            GROUP BY 1
            """
            
            df_dates = spark.read.jdbc(url=jdbc_url, table=f"({sql_query_teradata}) as query", properties=tdcon_properties)
            train_dt_row = df_dates.agg(F.max("start_date")).first()
            train_dt = train_dt_row[0] if train_dt_row and train_dt_row[0] else datetime.strptime('2010-01-01', '%Y-%m-%d').date()
            train_dt_str = train_dt.strftime('%Y-%m-%d')

            df_temp_table = spark.table(f"promo.pid_{string2}").alias("a").join(
                spark.table("mtest.st_inf_flg").alias("b"),
                on="ut_id",
                how="inner"
            )
            
            filter_expr = (
                ~F.col("day_dt").isin(
                    '2014-12-25', '2015-12-25', '2016-12-25', '2017-12-25', 
                    '2018-12-25', '2019-12-25', '2020-12-25', '2021-12-25', '2022-12-25'
                ) &
                (F.col("ut_id").between(19, 399)) &
                (F.col("day_dt") > F.lit(train_dt_str))
            )

            if L2_indx == '1' and Update_M == 'N':
                filter_expr = filter_expr & (
                    ((F.col("p_sold_first_dt") <= F.expr(f"date '{LastPrWk}' - interval 724 days")) &
                     (F.col("day_dt") <= F.expr(f"date '{LastPrWk}' - interval 364 days"))) |
                    ((F.col("p_sold_first_dt") > F.expr(f"date '{LastPrWk}' - interval 724 days")) &
                     (F.col("day_dt") <= F.expr(f"date '{LastPrWk}' - interval {ValidWeeks * 7} days")))
                )
            else:
                filter_expr = filter_expr & (F.col("day_dt") <= F.expr(f"date '{LastPrWk}' - interval {ValidWeeks * 7} days"))

            df_filtered_for_reg = df_temp_table.filter(filter_expr)
            
            feature_list = [
                'ntl_log_bse_pr_am', 'ntl_log_bse_promo_dct_pr_am', 'fcl_per_of_yr_id', 'adv_circ_flg', 
                'adv_mid_wk_flg', 'adv_super_evnt_flg', 'adv_dgtl_circ_flg', 'unadv_flg', 'p_ten_for_ten_flg',
                'NY', 'SuperBowlSat', 'DayB4Valentine', 'Valentine', 'Easter', 'EasterWk', 'MomSat', 'Mom',
                'Memorial', 'MemWkend', 'DadSat', 'Dad', 'JulyFour', 'Labor', 'LaborWkEnd', 'LaborFri', 
                'LaborSat', 'LaborSun', 'Columbus', 'MCC_BLK_OUT', 'PreHalo_Fri', 'PreHalo_Sat', 'PreHalo_Sun',
                'HalloweenEve', 'HalloweenFri', 'Halloween', 'Veterans', 'FridayB4Turkey', 'SaturdayB4Turkey',
                'TurkeyWed', 'Turkey', 'BlackFri', 'RedSat', 'DecTwoThree', 'DecTwoSix', 'SunPreXmas', 'XMASEVE',
                'SUNPNY', 'SATPNY', 'NYEVE'
            ]
            feature_list.extend([f'wk{i}' for i in range(2, 53)])
            feature_list.extend([f'dow{i}' for i in range(2, 8)])
            
            available_cols = df_filtered_for_reg.columns
            ut_cols = [f'ut{i}' for i in range(19, 325) if f'ut{i}' in available_cols]
            feature_list.extend(ut_cols)
            
            label_column = 'ntl_log_mkt_bskt_ut_qt'

            schema_fields = [
                StructField("p_id", StringType(), True), StructField("_MODEL_", StringType(), True),
                StructField("_TYPE_", StringType(), True), StructField("_DEPVAR_", StringType(), True),
                StructField("_RMSE_", DoubleType(), True), StructField("Intercept", DoubleType(), True)
            ]
            schema_fields.extend([StructField(f, DoubleType(), True) for f in feature_list])
            pandas_udf_output_schema = StructType(schema_fields)

            def run_regression_by_group(pdf: pd.DataFrame) -> pd.DataFrame:
                for col in feature_list:
                    if col not in pdf.columns:
                        pdf[col] = 0
                
                pdf = pdf.dropna(subset=[label_column] + feature_list).reset_index(drop=True)

                if pdf.empty or len(pdf) <= len(feature_list):
                    return pd.DataFrame(columns=[f.name for f in pandas_udf_output_schema.fields])

                X = pdf[feature_list]
                y = pdf[label_column]
                p_id_val = pdf['p_id'].iloc[0]

                try:
                    model = LinearRegression()
                    model.fit(X, y)
                    y_pred = model.predict(X)
                    rmse = np.sqrt(mean_squared_error(y, y_pred))
                    coefs = {name: coef for name, coef in zip(feature_list, model.coef_)}
                    result_row = {
                        "p_id": p_id_val, "_MODEL_": "RegDaily", "_TYPE_": "PARMS",
                        "_DEPVAR_": label_column, "_RMSE_": rmse, "Intercept": model.intercept_, **coefs
                    }
                    return pd.DataFrame([result_row])
                except Exception:
                    return pd.DataFrame(columns=[f.name for f in pandas_udf_output_schema.fields])

            cols_for_reg = ["p_id", label_column] + list(set(feature_list) & set(available_cols))
            df_for_pandas_udf = df_filtered_for_reg.select(*cols_for_reg)

            df_coef_temp = df_for_pandas_udf.groupBy("p_id").applyInPandas(run_regression_by_group, schema=pandas_udf_output_schema)
            df_coef_temp.persist()

            coef_cnt = df_coef_temp.count()
            print(coef_cnt)

            if coef_cnt > 0:
                final_coef_table = f"{db_train_m}.pid_{string2}_coef"
                spark.sql(f"DROP TABLE IF EXISTS {final_coef_table}")
                df_coef_temp.write.format("delta").mode("overwrite").saveAsTable(final_coef_table)

                if L2_indx == '0' and Update_M == 'N':
                    db_train_c_path = f"{fcst_coef}/{type}"
                    spark.sql(f"CREATE DATABASE IF NOT EXISTS train_c LOCATION '{db_train_c_path}'")
                    dest_table = f"train_c.pid_{string2}_coef"
                    spark.sql(f"DROP TABLE IF EXISTS {dest_table}")
                    df_coef_temp.write.format("delta").mode("overwrite").saveAsTable(dest_table)

                if Update_M == 'Y':
                    db_train_c_train_path = f"{fcst_coef}/{type}/Train"
                    spark.sql(f"CREATE DATABASE IF NOT EXISTS train_c_train LOCATION '{db_train_c_train_path}'")
                    dest_table = f"train_c_train.pid_{string2}_coef"
                    spark.sql(f"DROP TABLE IF EXISTS {dest_table}")
                    df_coef_temp.write.format("delta").mode("overwrite").saveAsTable(dest_table)
            
            df_coef_temp.unpersist()

pids_model_reg_pl(
    L6=L6_id, 
    LastPrWk=LastPreWeek, 
    ValidWeeks=ValidWks, 
    retrain=train, 
    Update_M=Update, 
    mo_date=mo_dt
)
#End-DBShift