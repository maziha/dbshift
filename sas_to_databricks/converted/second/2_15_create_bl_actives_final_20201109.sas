import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

# This corresponds to the %let rename_list statement
rename_list = {
    'caregiving_em_attend': 'CAREGIVE_ATT_EM',
    'driver_safety_tek_dm': 'DRVSAFE_TEK_DM',
    'driver_safety_tek_em': 'DRVSAFE_TEK_EM',
    'medicare_em_attend': 'MEDICARE_ATT_EM',
    'work_jobs_attendee_em': 'WORKJOB_ATT_EM'
}

# This corresponds to the %let additional_vars statement
additional_vars = [
    'relationship_seg',
    'hid_key',
    'mid_key',
    'ch_acq',
    'donfnd12',
    'donadv12',
    'petadv12',
    'cur_term',
    'marital',
    'category',
    'GOI',
    'rpm_score',
    'old_score1_vigintile',
    'old_score2_vigintile',
    'old_score3_vigintile',
    'old_score6_vigintile',
    'old_score8_vigintile',
    'old_score9_vigintile',
    'old_score11_vigintile',
    'old_score14_vigintile',
    'old_score16_vigintile',
    'old_score20_vigintile',
    'old_score21_vigintile',
    'old_score22_vigintile',
    'old_score26_vigintile',
    'old_score29_vigintile',
    'old_score30_vigintile',
    'old_score31_vigintile',
    'old_score33_vigintile',
    'old_score34_vigintile',
    'old_score35_vigintile',
    'old_score38_vigintile',
    'old_score39_vigintile',
    'FNDN_AARPPRO_DM_score_b4_190819',
    'FNDN_AARPPRO_DM_b4_190819'
]

# This corresponds to the proc import step
# The path should be updated to a valid DBFS path.
df_models_list_raw = spark.read.option("header", "true").csv("dbfs:/vg04/twalters/Models_List.csv")

# This corresponds to the proc sql step to create the models_list macro variable
df_filtered_models = df_models_list_raw.filter(~F.col("modelname").contains("NMAS"))
models_list = [row.modelname for row in df_filtered_models.select("modelname").collect()]

# This corresponds to the %put &models_list statement
print(f"Models List: {models_list}")

# This corresponds to the DATA step creating WORK.BDE_MODELS
bde_models_schema = [
    "mid_key",
    "DAPM_Catalist_voteprop_mod_2020",
    "DAPM_Catalist_Mail_Readership",
    "DAPM_Catalist_Partisanship_Model",
    "AIAN_Percentile",
    "AIAN_Binary_Score",
    "AIAN_Event_Percentile",
    "ready_travel",
    "ready_cruise",
    "ready_dine",
    "online_banking"
]

# Reading the CSV file. The path should be updated to a valid DBFS path.
df_bde_models_raw = spark.read.option("header", "false").option("delimiter", ",").csv("dbfs:/vg01/aarp_sas/ftp/incoming/all_model_scores.csv", schema=",".join([f"`{c}` STRING" for c in bde_models_schema]))

# Cast columns and apply logic
df_bde_models = df_bde_models_raw

# Ensure mid_key is numeric
df_bde_models = df_bde_models.withColumn("mid_key", F.col("mid_key").cast(DoubleType()))

# Apply transformations
df_bde_models = df_bde_models.withColumn("DAPM_Catalist_Mail_Readership", 
    F.when(F.col("DAPM_Catalist_Mail_Readership").cast(DoubleType()).isNull() | (F.col("DAPM_Catalist_Mail_Readership").cast(DoubleType()) > 99), 99)
    .otherwise(F.col("DAPM_Catalist_Mail_Readership").cast(DoubleType())))

df_bde_models = df_bde_models.withColumn("DAPM_Catalist_voteprop_mod_2020", 
    F.when(F.col("DAPM_Catalist_voteprop_mod_2020").cast(DoubleType()).isNull() | (F.col("DAPM_Catalist_voteprop_mod_2020").cast(DoubleType()) > 99), 99)
    .otherwise(F.col("DAPM_Catalist_voteprop_mod_2020").cast(DoubleType())))

df_bde_models = df_bde_models.withColumn("DAPM_Catalist_Partisanship_Model", 
    F.when(F.col("DAPM_Catalist_Partisanship_Model").cast(DoubleType()).isNull() | (F.col("DAPM_Catalist_Partisanship_Model").cast(DoubleType()) > 99), 99)
    .otherwise(F.col("DAPM_Catalist_Partisanship_Model").cast(DoubleType())))

df_bde_models = df_bde_models.withColumn("AIAN_Percentile", 
    F.when(F.col("AIAN_Percentile").cast(DoubleType()).isNull() | (F.col("AIAN_Percentile").cast(DoubleType()) > 99), 99)
    .otherwise(F.col("AIAN_Percentile").cast(DoubleType())))

df_bde_models = df_bde_models.withColumn("AIAN_Binary_Score", 
    F.when(F.col("AIAN_Binary_Score").cast(DoubleType()).isNull() | (F.col("AIAN_Binary_Score").cast(DoubleType()) == 0), 99)
    .otherwise(F.col("AIAN_Binary_Score").cast(DoubleType())))

df_bde_models = df_bde_models.withColumn("AIAN_Event_Percentile", 
    F.when(F.col("AIAN_Event_Percentile").cast(DoubleType()).isNull() | (F.col("AIAN_Event_Percentile").cast(DoubleType()) > 99), 99)
    .otherwise(F.col("AIAN_Event_Percentile").cast(DoubleType())))

df_bde_models = df_bde_models.withColumn("ready_travel", 
    F.when(F.col("ready_travel").cast(DoubleType()).isNull() | (F.col("ready_travel").cast(DoubleType()) > 99), 99)
    .otherwise(F.col("ready_travel").cast(DoubleType())))

df_bde_models = df_bde_models.withColumn("ready_cruise", 
    F.when(F.col("ready_cruise").cast(DoubleType()).isNull() | (F.col("ready_cruise").cast(DoubleType()) > 99), 99)
    .otherwise(F.col("ready_cruise").cast(DoubleType())))

df_bde_models = df_bde_models.withColumn("ready_dine", 
    F.when(F.col("ready_dine").cast(DoubleType()).isNull() | (F.col("ready_dine").cast(DoubleType()) > 99), 99)
    .otherwise(F.col("ready_dine").cast(DoubleType())))

df_bde_models = df_bde_models.withColumn("online_banking", 
    F.when(F.col("online_banking").cast(DoubleType()).isNull() | (F.col("online_banking").cast(DoubleType()) > 99), 99)
    .otherwise(F.col("online_banking").cast(DoubleType())))

# This corresponds to the proc sql step to create the true_bl_vars macro variable
df_bonus_layout_for_schema = spark.table("intermed.bonus_layout")
true_bl_vars = df_bonus_layout_for_schema.columns

# This corresponds to the %put &true_bl_vars statement
print(f"True BL Vars: {true_bl_vars}")

def build_datasets(runtype, muldate):
    # This corresponds to the %if/%then logic for the filename
    if runtype == 'Weekly':
        filename = f"wkly_bl_actives_{muldate}"
    if runtype == 'Monthly':
        filename = f"bl_actives_{muldate}"

    # Prepare tables and views for PROC SQL translation
    df_bonus_layout = spark.table("intermed.bonus_layout")
    df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
    
    # Rename columns in geo_appends_rpm DataFrame before creating view
    df_geo_appends_rpm_renamed = df_geo_appends_rpm
    for old_name, new_name in rename_list.items():
        df_geo_appends_rpm_renamed = df_geo_appends_rpm_renamed.withColumnRenamed(old_name, new_name)

    # Register views to be used in spark.sql
    df_bonus_layout.createOrReplaceTempView("bonus_layout")
    df_geo_appends_rpm_renamed.createOrReplaceTempView("geo_appends_rpm_renamed")
    df_bde_models.createOrReplaceTempView("bde_models_view")

    # This corresponds to the first proc sql step to create temp_bl
    sql_temp_bl = """
    SELECT *
    FROM (SELECT * FROM bonus_layout WHERE memstatus = '0') AS a
    LEFT JOIN (SELECT * FROM geo_appends_rpm_renamed WHERE memstatus = '0') AS b
    ON a.memacctnum = b.memacctnum
    """
    df_temp_bl_unfiltered_cols = spark.sql(sql_temp_bl)
    
    # This corresponds to the KEEP= statement
    new_score_cols = [c for c in df_temp_bl_unfiltered_cols.columns if c.startswith('new_score')]
    keep_list_for_temp_bl = ['memacctnum', 'memstatus'] + additional_vars + models_list + new_score_cols + true_bl_vars
    
    final_cols_for_temp_bl = []
    for col in keep_list_for_temp_bl:
        if col in df_temp_bl_unfiltered_cols.columns and col not in final_cols_for_temp_bl:
            final_cols_for_temp_bl.append(col)
            
    df_temp_bl = df_temp_bl_unfiltered_cols.select(final_cols_for_temp_bl)
    df_temp_bl.createOrReplaceTempView("temp_bl")
    
    # This corresponds to the second proc sql step to create aarpdata.&filename
    sql_final_actives = f"""
    SELECT a.*, bde.*
    FROM temp_bl a
    LEFT JOIN (SELECT * FROM bde_models_view WHERE mid_key IS NOT NULL AND mid_key NOT IN (0, 1)) bde
    ON a.merkleid = TRIM(CAST(bde.mid_key AS STRING))
    """
    df_actives_with_dups = spark.sql(sql_final_actives)
    
    # This corresponds to the proc sort nodupkey step
    df_actives_final = df_actives_with_dups.dropDuplicates(['memacctnum'])
    
    # Write the final table
    df_actives_final.write.format("delta").mode("overwrite").saveAsTable(f"aarpdata.{filename}")

    # This corresponds to the %if &runtype=Monthly %then %do block
    if runtype == 'Monthly':
        # This corresponds to creating bl_expires
        sql_expires = """
        SELECT a.*, b.*, bde.*
        FROM (SELECT * FROM bonus_layout WHERE memstatus = '5') AS a
        LEFT JOIN (SELECT * FROM geo_appends_rpm_renamed WHERE memstatus = '5') AS b
        ON a.memacctnum = b.memacctnum
        LEFT JOIN (SELECT * FROM bde_models_view WHERE mid_key IS NOT NULL AND mid_key NOT IN (0, 1)) bde
        ON a.merkleid = TRIM(CAST(bde.mid_key AS STRING))
        """
        df_expires_unfiltered_cols = spark.sql(sql_expires)
        
        # This corresponds to the KEEP= statement
        new_score_cols_expires = [c for c in df_expires_unfiltered_cols.columns if c.startswith('new_score')]
        keep_list_for_expires = ['memacctnum', 'memstatus'] + additional_vars + models_list + new_score_cols_expires + true_bl_vars
        
        final_cols_for_expires = []
        for col in keep_list_for_expires:
            if col in df_expires_unfiltered_cols.columns and col not in final_cols_for_expires:
                final_cols_for_expires.append(col)
                
        df_expires_with_dups = df_expires_unfiltered_cols.select(final_cols_for_expires)

        # This corresponds to the proc sort nodupkey step
        df_expires_final = df_expires_with_dups.dropDuplicates(['memacctnum'])

        # Write the final table
        df_expires_final.write.format("delta").mode("overwrite").saveAsTable(f"aarpdata.bl_expires_{muldate}")

        # This corresponds to creating emu file
        sql_emu = """
        SELECT a.*, b.*, bde.*
        FROM (SELECT * FROM bonus_layout WHERE memstatus = 'E') AS a
        LEFT JOIN (SELECT * FROM geo_appends_rpm_renamed WHERE memstatus = 'E') AS b
        ON CAST(a.merkleid AS BIGINT) = b.mid_key
        LEFT JOIN (SELECT * FROM bde_models_view WHERE mid_key IS NOT NULL AND mid_key NOT IN (0, 1)) bde
        ON a.merkleid = TRIM(CAST(bde.mid_key AS STRING))
        """
        df_emu_unfiltered_cols = spark.sql(sql_emu)

        # This corresponds to the KEEP= statement
        new_score_cols_emu = [c for c in df_emu_unfiltered_cols.columns if c.startswith('new_score')]
        keep_list_for_emu = ['memacctnum', 'memstatus'] + additional_vars + models_list + new_score_cols_emu + true_bl_vars

        final_cols_for_emu = []
        for col in keep_list_for_emu:
            if col in df_emu_unfiltered_cols.columns and col not in final_cols_for_emu:
                final_cols_for_emu.append(col)
        
        df_emu_with_dups = df_emu_unfiltered_cols.select(final_cols_for_emu)

        # This corresponds to the proc sort nodupkey step
        df_emu_final = df_emu_with_dups.dropDuplicates(['mid_key'])

        # Write the final table
        df_emu_final.write.format("delta").mode("overwrite").saveAsTable(f"aarpdata.emu_{muldate}")

# This corresponds to the call to the macro %build_datasets
# The user must define these variables before running the script.
# Example:
# runtype = "Monthly"
# muldate = "20170628"
# build_datasets(runtype, muldate)

# Final %put statements for debugging
print(f"Final Models List: {models_list}")
print(f"Final True BL Vars used: {true_bl_vars}")
#End-DBShift