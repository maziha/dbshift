import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, DateType, LongType, IntegerType, DoubleType
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("Eval_PL_Conversion").getOrCreate()

ModelType = "Reg_PL"
type_var = "Reg_PL"
tdate = datetime.now().strftime("%m%d%y") + "N"

l6_id = "YOUR_L6_ID"
Forecast = "Y"
fcst_log = "/path/to/fcst_log"
eval_db_path = "/path/to/eval"
pred_promo_db_path = "/path/to/pred_promo"
pred_base_db_path = "/path/to/pred_base"
fcst_promo_db_path = "/path/to/fcst_promo"
fcst_base_db_path = "/path/to/fcst_base"
dr_db = "dr"
mtest_db = "mtest"


def pids_eval_reg_pl(ModelType, l6, Fcst):
    if Fcst == "Y":
        log_path = f"{fcst_log}/Update/DF_Eval/library_setup_eval_{Fcst}_{ModelType}{l6}.txt"
        print(f"INFO: SAS log would be written to: {log_path}")
    else:
        log_path = f"{fcst_log}/{ModelType}/library_setup_eval_{Fcst}{l6}.txt"
        print(f"INFO: SAS log would be written to: {log_path}")

    if Fcst == "Y":
        eval_type = "fcst"
        eval_db = f"{eval_db_path.replace('/', '_')}_{ModelType}"
        pred_p_db = f"{pred_promo_db_path.replace('/', '_')}_{ModelType}"
        pred_b_db = f"{pred_base_db_path.replace('/', '_')}_{ModelType}"
    else:
        eval_type = "train"
        eval_db = f"{eval_db_path.replace('/', '_')}_{ModelType}_Train"
        pred_p_db = f"{fcst_promo_db_path.replace('/', '_')}_{ModelType}"
        pred_b_db = f"{fcst_base_db_path.replace('/', '_')}_{ModelType}"

    df_fcst_p_upc_inf = spark.table(f"{mtest_db}.fcst_p_upc_inf")

    df_loop_params = df_fcst_p_upc_inf \
        .filter(F.col("mjr_mds_are_id") == l6) \
        .withColumn("L6NM_val", F.trim(F.substring(F.col("mjr_mds_are_nm_tx"), 1, 2))) \
        .select(
            "mjr_p_cls_id",
            "L2",
            "L6NM_val"
        ).distinct().orderBy(F.asc("L2"))

    collected_rows = df_loop_params.collect()
    
    if not collected_rows:
        print(f"WARNING: No data found for mjr_mds_are_id = {l6}. Terminating process.")
        return

    L2_ids = [row.mjr_p_cls_id for row in collected_rows]
    L2_table = [row.L2 for row in collected_rows]
    nL2 = len(L2_ids)
    L6NM = collected_rows[0].L6NM_val
    
    df_base_accumulator = None
    df_total_accumulator = None

    for j in range(nL2):
        L2_id = L2_ids[j]
        string2 = L2_table[j]

        if j == 0:
            schema = StructType([
                StructField("day_dt", DateType(), True),
                StructField("p_id", LongType(), True),
                StructField("ut_id", IntegerType(), True),
                StructField("RegDaily", DoubleType(), True),
                StructField("Pred", DoubleType(), True)
            ])
            df_base_accumulator = spark.createDataFrame([], schema)
            df_total_accumulator = spark.createDataFrame([], schema)

        print(string2)
        
        base_table_name = f"pid_{string2}_{ModelType}_base"
        base_table_fqn = f"{pred_b_db}.{base_table_name}"
        if spark.catalog.tableExists(base_table_fqn):
            
            # This logic corresponds to the drop/data step followed by insert
            # The steps are combined into a single DataFrame transformation flow
            
            df_temp_base_source = spark.table(base_table_fqn)

            df_temp_base_processed = df_temp_base_source \
                .select("day_dt", "p_id", "ut_id", "RegDaily") \
                .withColumn("Pred", F.exp(F.col("RegDaily")) - 1) \
                .filter(F.col("regdaily") < 10)

            df_to_append_base = df_temp_base_processed \
                .select("day_dt", "p_id", "ut_id", "RegDaily", "pred") \
                .distinct()
            
            df_base_accumulator = df_base_accumulator.unionByName(df_to_append_base)

        promo_table_name = f"pid_{string2}_{ModelType}_promo"
        promo_table_fqn = f"{pred_p_db}.{promo_table_name}"
        if spark.catalog.tableExists(promo_table_fqn):

            df_temp_total_source = spark.table(promo_table_fqn)

            df_temp_total_processed = df_temp_total_source \
                .select("day_dt", "p_id", "ut_id", "RegDaily") \
                .withColumn("Pred", F.exp(F.col("RegDaily")) - 1) \
                .filter(F.col("regdaily") < 10)

            df_to_append_total = df_temp_total_processed \
                .select("day_dt", "p_id", "ut_id", "RegDaily", "pred") \
                .distinct()

            df_total_accumulator = df_total_accumulator.unionByName(df_to_append_total)

    # Final write operations
    base_output_table = f"{dr_db}.fcst_{L6NM}_base_{eval_type}_{ModelType}"
    total_output_table = f"{dr_db}.fcst_{L6NM}_total_{eval_type}_{ModelType}"

    spark.sql(f"DROP TABLE IF EXISTS {base_output_table}")
    spark.sql(f"DROP TABLE IF EXISTS {total_output_table}")

    if df_base_accumulator is not None:
        df_base_accumulator.write.format("delta").mode("overwrite").saveAsTable(base_output_table)

    if df_total_accumulator is not None:
        df_total_accumulator.write.format("delta").mode("overwrite").saveAsTable(total_output_table)

pids_eval_reg_pl(ModelType=ModelType, l6=l6_id, Fcst=Forecast)
#End-DBShift