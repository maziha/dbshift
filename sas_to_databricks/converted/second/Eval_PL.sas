import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, DateType, NumericType
from datetime import datetime

# Assume SparkSession is available from the Databricks environment
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("pids_eval_reg_pl_translation").getOrCreate()

# Global variables translated from SAS %LET statements
ModelType = "Reg_PL"
type_var = "Reg_PL"
tdate = datetime.now().strftime("%m%d%Y")

def pids_eval_reg_pl(ModelType, l6, Fcst):
    """
    This function translates the pids_eval_reg_pl SAS macro.
    """
    
    # proc printto is for SAS log redirection, which is not directly applicable.
    # Logging in Databricks notebooks is handled by cell output.

    # libname logic is handled by defining database prefixes.
    if Fcst == 'Y':
        eval_type = 'fcst'
        eval_db = "eval"
        pred_p_db = "pred_p"
        pred_b_db = "pred_b"
    else:
        eval_type = 'train'
        eval_db = "eval" 
        pred_p_db = "fcst_promo"
        pred_b_db = "fcst_base"

    # Define database paths based on Fcst
    if Fcst == 'Y':
        eval_path = f"{eval_db}.{ModelType}"
        pred_p_path = f"{pred_p_db}.{ModelType}"
        pred_b_path = f"{pred_b_db}.{ModelType}"
    else:
        eval_path = f"{eval_db}.{ModelType}.Train"
        pred_p_path = f"{pred_p_db}.{ModelType}"
        pred_b_path = f"{pred_b_db}.{ModelType}"
        
    # PROC SQL to get loop parameters
    df_params_source = spark.table("mtest.fcst_p_upc_inf").filter(F.col("mjr_mds_are_id") == l6)

    df_for_params = df_params_source.select(
        F.col("mjr_p_cls_id"),
        F.col("L2"),
        F.trim(F.substring(F.col("mjr_mds_are_nm_tx"), 1, 2)).alias("l6nm_val")
    ).distinct().orderBy(F.col("L2").asc())
    
    collected_params = df_for_params.collect()

    if not collected_params:
        print(f"Warning: No data found for mjr_mds_are_id = {l6}")
        return

    nL2 = df_params_source.select(F.col("mjr_p_cls_id")).distinct().count()
    L2_ids = [row.mjr_p_cls_id for row in collected_params]
    L2_table = [row.L2 for row in collected_params]
    L6NM = collected_params[0].l6nm_val

    # Loop setup
    # Initial table creation, mirroring the `%if &j.=1` block
    base_table_name = f"{eval_db}.{L6NM}_{eval_type}_base_{ModelType}"
    total_table_name = f"{eval_db}.{L6NM}_{eval_type}_total_{ModelType}"

    schema = StructType([
        StructField("day_dt", DateType(), True),
        StructField("p_id", DoubleType(), True),
        StructField("ut_id", DoubleType(), True),
        StructField("RegDaily", DoubleType(), True),
        StructField("Pred", DoubleType(), True)
    ])
    
    empty_df = spark.createDataFrame([], schema)
    empty_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(base_table_name)
    empty_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(total_table_name)

    # Begin %do loop translation
    for j in range(len(L2_ids)):
        L2_id = L2_ids[j]
        string2 = L2_table[j]
        
        print(string2) # Corresponds to SAS %put

        # Block for pred_b
        source_base_table = f"{pred_b_db}.pid_{string2}_{ModelType}_base"
        if spark.catalog.tableExists(source_base_table):
            
            # The SAS logic of drop/data/insert is convoluted.
            # It creates a temporary table and appends its distinct contents.
            
            # proc sql; drop table eval.&l6nm._base_&eval_type._&ModelType.;
            # In Spark, we'll create a temporary DataFrame instead of a physical table.
            
            # data eval.&l6nm._base_&eval_type._&ModelType.;
            df_temp_base = spark.table(source_base_table).select(
                "day_dt", "p_id", "ut_id", "RegDaily"
            )
            df_temp_base = df_temp_base.withColumn("Pred", F.exp(F.col("RegDaily")) - 1) \
                                       .filter(F.col("regdaily") < 10)

            # proc sql; insert into eval.&l6nm._&eval_type._base_&ModelType. select distinct ...
            # The SAS code inserts from the table it just created into the main accumulator.
            df_to_append_base = df_temp_base.distinct()
            df_to_append_base.write.format("delta").mode("append").saveAsTable(base_table_name)

        # Block for pred_p
        source_promo_table = f"{pred_p_db}.pid_{string2}_{ModelType}_promo"
        if spark.catalog.tableExists(source_promo_table):
            
            # proc sql; drop table eval.&l6nm._total_&eval_type._&ModelType.;
            # Again, we use a temporary DataFrame.
            
            # data eval.&l6nm._total_&eval_type._&ModelType.;
            df_temp_total = spark.table(source_promo_table).select(
                "day_dt", "p_id", "ut_id", "RegDaily"
            )
            df_temp_total = df_temp_total.withColumn("Pred", F.exp(F.col("RegDaily")) - 1) \
                                         .filter(F.col("regdaily") < 10)
            
            # proc sql; insert into eval.&l6nm._&eval_type._total_&ModelType. select distinct ...
            df_to_append_total = df_temp_total.distinct()
            df_to_append_total.write.format("delta").mode("append").saveAsTable(total_table_name)
    
    # Final copy to 'dr' database
    dr_db = "dr"
    
    final_base_table = f"{dr_db}.fcst_{L6NM}_base_{eval_type}_{ModelType}"
    final_total_table = f"{dr_db}.fcst_{L6NM}_total_{eval_type}_{ModelType}"
    
    spark.sql(f"DROP TABLE IF EXISTS {final_base_table}")
    spark.sql(f"DROP TABLE IF EXISTS {final_total_table}")
    
    # data dr.fcst_..._base_...; set eval...;
    df_eval_base = spark.table(base_table_name)
    df_eval_base.write.format("delta").mode("overwrite").saveAsTable(final_base_table)

    # data dr.fcst_..._total_...; set eval...;
    df_eval_total = spark.table(total_table_name)
    df_eval_total.write.format("delta").mode("overwrite").saveAsTable(final_total_table)


# --- Main Execution Block ---
# These parameters would be passed from a Databricks widget or job configuration.
l6_id_param = "YOUR_L6_ID"  # Placeholder value
Forecast_param = "Y"        # Placeholder value

pids_eval_reg_pl(ModelType=ModelType, l6=l6_id_param, Fcst=Forecast_param)
#End-DBShift