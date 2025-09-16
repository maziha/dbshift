import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# This script assumes the following variables are set, for example:
# runtype = "Monthly"
# muldate = "20230131"

# Mimicking the %include directive by defining the variables from the SAS program
# The SAS script dynamically gets bl_vars, so we will do the same later.

rename_list = {
    "caregiving_em_attend": "CAREGIVE_ATT_EM",
    "driver_safety_tek_dm": "DRVSAFE_TEK_DM",
    "driver_safety_tek_em": "DRVSAFE_TEK_EM",
    "medicare_em_attend": "MEDICARE_ATT_EM",
    "work_jobs_attendee_em": "WORKJOB_ATT_EM"
}

additional_vars = [
    "relationship_seg",
    "hid_key",
    "mid_key",
    "ch_acq",
    "donfnd12",
    "donadv12",
    "petadv12",
    "cur_term",
    "marital",
    "category",
    "GOI",
    "rpm_score",
    "old_score1_vigintile",
    "old_score2_vigintile",
    "old_score3_vigintile",
    "old_score6_vigintile",
    "old_score8_vigintile",
    "old_score9_vigintile",
    "old_score11_vigintile",
    "old_score14_vigintile",
    "old_score16_vigintile",
    "old_score20_vigintile",
    "old_score21_vigintile",
    "old_score22_vigintile",
    "old_score26_vigintile",
    "old_score29_vigintile",
    "old_score30_vigintile",
    "old_score31_vigintile",
    "old_score33_vigintile",
    "old_score34_vigintile",
    "old_score35_vigintile",
    "old_score38_vigintile",
    "old_score39_vigintile",
    "FNDN_AARPPRO_DM_score_b4_190819",
    "FNDN_AARPPRO_DM_b4_190819"
]

# In a real Databricks environment, these paths would point to DBFS or a mounted location
models_list_path = "/vg04/twalters/Models_List.csv"
bde_models_path = "/vg01/aarp_sas/ftp/incoming/all_model_scores.csv"

# proc import out=models_list
df_models_list_raw = spark.read.csv(models_list_path, header=True, inferSchema=True)

# proc sql noprint to create macro variable
df_models_list_filtered = df_models_list_raw.filter(~F.col("modelname").contains("NMAS"))
models_list_rows = df_models_list_filtered.select("modelname").collect()
models_list = " ".join([row.modelname for row in models_list_rows if row.modelname is not None])
models_list_py = models_list.split()

print(models_list)

# data WORK.BDE_MODELS
bde_schema = StructType([
    StructField("mid_key", DoubleType(), True),
    StructField("DAPM_Catalist_voteprop_mod_2020", DoubleType(), True),
    StructField("DAPM_Catalist_Mail_Readership", DoubleType(), True),
    StructField("DAPM_Catalist_Partisanship_Model", DoubleType(), True),
    StructField("AIAN_Percentile", DoubleType(), True),
    StructField("AIAN_Binary_Score", DoubleType(), True),
    StructField("AIAN_Event_Percentile", DoubleType(), True),
    StructField("ready_travel", DoubleType(), True),
    StructField("ready_cruise", DoubleType(), True),
    StructField("ready_dine", DoubleType(), True),
    StructField("online_banking", DoubleType(), True)
])

df_bde_models = spark.read.csv(bde_models_path, header=True, schema=bde_schema, emptyValue=None)

df_bde_models = df_bde_models.withColumn("DAPM_Catalist_Mail_Readership", F.when((F.col("DAPM_Catalist_Mail_Readership").isNull()) | (F.col("DAPM_Catalist_Mail_Readership") > 99), 99).otherwise(F.col("DAPM_Catalist_Mail_Readership")))
df_bde_models = df_bde_models.withColumn("DAPM_Catalist_voteprop_mod_2020", F.when((F.col("DAPM_Catalist_voteprop_mod_2020").isNull()) | (F.col("DAPM_Catalist_voteprop_mod_2020") > 99), 99).otherwise(F.col("DAPM_Catalist_voteprop_mod_2020")))
df_bde_models = df_bde_models.withColumn("DAPM_Catalist_Partisanship_Model", F.when((F.col("DAPM_Catalist_Partisanship_Model").isNull()) | (F.col("DAPM_Catalist_Partisanship_Model") > 99), 99).otherwise(F.col("DAPM_Catalist_Partisanship_Model")))
df_bde_models = df_bde_models.withColumn("AIAN_Percentile", F.when((F.col("AIAN_Percentile").isNull()) | (F.col("AIAN_Percentile") > 99), 99).otherwise(F.col("AIAN_Percentile")))
df_bde_models = df_bde_models.withColumn("AIAN_Binary_Score", F.when((F.col("AIAN_Binary_Score").isNull()) | (F.col("AIAN_Binary_Score") == 0), 99).otherwise(F.col("AIAN_Binary_Score")))
df_bde_models = df_bde_models.withColumn("AIAN_Event_Percentile", F.when((F.col("AIAN_Event_Percentile").isNull()) | (F.col("AIAN_Event_Percentile") > 99), 99).otherwise(F.col("AIAN_Event_Percentile")))
df_bde_models = df_bde_models.withColumn("ready_travel", F.when((F.col("ready_travel").isNull()) | (F.col("ready_travel") > 99), 99).otherwise(F.col("ready_travel")))
df_bde_models = df_bde_models.withColumn("ready_cruise", F.when((F.col("ready_cruise").isNull()) | (F.col("ready_cruise") > 99), 99).otherwise(F.col("ready_cruise")))
df_bde_models = df_bde_models.withColumn("ready_dine", F.when((F.col("ready_dine").isNull()) | (F.col("ready_dine") > 99), 99).otherwise(F.col("ready_dine")))
df_bde_models = df_bde_models.withColumn("online_banking", F.when((F.col("online_banking").isNull()) | (F.col("online_banking") > 99), 99).otherwise(F.col("online_banking")))

# proc sql to get varnames
df_bonus_layout_schema_source = spark.table("intermed.bonus_layout")
true_bl_vars = df_bonus_layout_schema_source.columns
print(f"True BL Vars: {true_bl_vars}")

# Begin macro logic translation
# Hardcoding these for demonstration, as they are macro variables in SAS
runtype = "Monthly"  # or "Weekly"
muldate = "20240101" # example date

filename = ""
if runtype == "Weekly":
    filename = f"wkly_bl_actives_{muldate}"
if runtype == "Monthly":
    filename = f"bl_actives_{muldate}"

# Prepare DataFrames for PROC SQL
df_bonus_layout_actives_pre = spark.table("intermed.bonus_layout").filter(F.col("memstatus") == '0')
df_geo_appends_rpm_actives_pre = spark.table("intermed.geo_appends_rpm").filter(F.col("memstatus") == '0')

# Apply renames to geo_appends_rpm
df_geo_appends_rpm_actives_renamed = df_geo_appends_rpm_actives_pre
for old_name, new_name in rename_list.items():
    if old_name in df_geo_appends_rpm_actives_renamed.columns:
        df_geo_appends_rpm_actives_renamed = df_geo_appends_rpm_actives_renamed.withColumnRenamed(old_name, new_name)

# Register views for spark.sql()
df_bonus_layout_actives_pre.createOrReplaceTempView("bonus_layout_view")
df_geo_appends_rpm_actives_renamed.createOrReplaceTempView("geo_appends_rpm_view")

# PROC SQL to create temp_bl
# Joining first to get the combined schema
df_temp_bl_unkept = spark.sql("""
    SELECT *
    FROM bonus_layout_view AS a
    LEFT JOIN geo_appends_rpm_view AS b
    ON a.memacctnum = b.memacctnum
""")

# Build the keep list
keep_cols = ["memacctnum", "memstatus"] + additional_vars + models_list_py
new_score_cols = [c for c in df_temp_bl_unkept.columns if c.startswith('new_score')]
final_keep_cols_set = set(keep_cols + new_score_cols + true_bl_vars)
final_keep_cols = [c for c in df_temp_bl_unkept.columns if c in final_keep_cols_set]
df_temp_bl = df_temp_bl_unkept.select(*final_keep_cols)

# Prepare for the next join
df_bde_models_filtered = df_bde_models.filter(F.col("mid_key").isNotNull() & ~F.col("mid_key").isin([0, 1]))
df_temp_bl.createOrReplaceTempView("temp_bl_view")
df_bde_models_filtered.createOrReplaceTempView("bde_models_view")

# PROC SQL to create aarpdata.&filename
df_actives_final_unselected = spark.sql(f"""
    SELECT a.*, bde.*
    FROM temp_bl_view a
    LEFT JOIN bde_models_view bde
    ON TRIM(a.merkleid) = TRIM(CAST(bde.mid_key AS STRING))
""")

# Resolve conflicting columns from the bde join before writing
# Assuming we want to keep the columns from the left side (temp_bl_view)
cols_to_select_after_bde_join = df_temp_bl.columns + [F.col(f"bde.{c}").alias(c) for c in df_bde_models_filtered.columns if c not in df_temp_bl.columns]
df_actives_final = df_actives_final_unselected.select(*cols_to_select_after_bde_join)

df_actives_final.write.format("delta").mode("overwrite").saveAsTable(f"aarpdata.{filename}")

# proc sort nodupkey
df_actives_deduped = spark.table(f"aarpdata.{filename}").dropDuplicates(["memacctnum"])
df_actives_deduped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"aarpdata.{filename}")

if runtype == "Monthly":
    # create bl_expires
    df_bonus_layout_expires_pre = spark.table("intermed.bonus_layout").filter(F.col("memstatus") == '5')
    df_geo_appends_rpm_expires_pre = spark.table("intermed.geo_appends_rpm").filter(F.col("memstatus") == '5')

    df_geo_appends_rpm_expires_renamed = df_geo_appends_rpm_expires_pre
    for old_name, new_name in rename_list.items():
        if old_name in df_geo_appends_rpm_expires_renamed.columns:
            df_geo_appends_rpm_expires_renamed = df_geo_appends_rpm_expires_renamed.withColumnRenamed(old_name, new_name)

    df_bonus_layout_expires_pre.createOrReplaceTempView("bonus_layout_expires_view")
    df_geo_appends_rpm_expires_renamed.createOrReplaceTempView("geo_appends_rpm_expires_view")

    df_expires_unkept = spark.sql("""
        SELECT *
        FROM bonus_layout_expires_view a
        LEFT JOIN geo_appends_rpm_expires_view b
        ON a.memacctnum = b.memacctnum
    """)

    df_expires_temp = df_expires_unkept.select(*final_keep_cols)
    df_expires_temp.createOrReplaceTempView("expires_temp_view")

    df_expires_final_unselected = spark.sql("""
        SELECT a.*, bde.*
        FROM expires_temp_view a
        LEFT JOIN bde_models_view bde
        ON TRIM(a.merkleid) = TRIM(CAST(bde.mid_key AS STRING))
    """)
    
    cols_to_select_after_bde_join_exp = df_expires_temp.columns + [F.col(f"bde.{c}").alias(c) for c in df_bde_models_filtered.columns if c not in df_expires_temp.columns]
    df_expires_final = df_expires_final_unselected.select(*cols_to_select_after_bde_join_exp)

    df_expires_final.write.format("delta").mode("overwrite").saveAsTable(f"aarpdata.bl_expires_{muldate}")

    # create emu file
    df_bonus_layout_emu_pre = spark.table("intermed.bonus_layout").filter(F.col("memstatus") == 'E')
    df_geo_appends_rpm_emu_pre = spark.table("intermed.geo_appends_rpm").filter(F.col("memstatus") == 'E')

    df_geo_appends_rpm_emu_renamed = df_geo_appends_rpm_emu_pre
    for old_name, new_name in rename_list.items():
        if old_name in df_geo_appends_rpm_emu_renamed.columns:
            df_geo_appends_rpm_emu_renamed = df_geo_appends_rpm_emu_renamed.withColumnRenamed(old_name, new_name)
    
    # Registering views for the more complex EMU join
    df_bonus_layout_emu_pre.createOrReplaceTempView("bonus_layout_emu_view")
    df_geo_appends_rpm_emu_renamed.createOrReplaceTempView("geo_appends_rpm_emu_view")

    df_emu_join1_unkept = spark.sql("""
        SELECT a.*, b.*
        FROM bonus_layout_emu_view a
        LEFT JOIN geo_appends_rpm_emu_view b
        ON CAST(a.merkleid AS BIGINT) = b.mid_key
    """)
    
    # Resolve conflicting columns from first EMU join before the keep operation
    emu_join1_cols = df_bonus_layout_emu_pre.columns + [F.col(f"b.{c}").alias(c) for c in df_geo_appends_rpm_emu_renamed.columns if c not in df_bonus_layout_emu_pre.columns]
    df_emu_join1_temp = df_emu_join1_unkept.select(*emu_join1_cols)
    
    # Apply keep logic
    final_keep_cols_emu = [c for c in df_emu_join1_temp.columns if c in final_keep_cols_set]
    df_emu_temp = df_emu_join1_temp.select(*final_keep_cols_emu)
    
    df_emu_temp.createOrReplaceTempView("emu_temp_view")
    
    df_emu_final_unselected = spark.sql("""
        SELECT a.*, bde.*
        FROM emu_temp_view a
        LEFT JOIN bde_models_view bde
        ON TRIM(a.merkleid) = TRIM(CAST(bde.mid_key AS STRING))
    """)
    
    cols_to_select_after_bde_join_emu = df_emu_temp.columns + [F.col(f"bde.{c}").alias(c) for c in df_bde_models_filtered.columns if c not in df_emu_temp.columns]
    df_emu_final = df_emu_final_unselected.select(*cols_to_select_after_bde_join_emu)
    
    df_emu_final.write.format("delta").mode("overwrite").saveAsTable(f"aarpdata.emu_{muldate}")

    # proc sort for expires
    df_expires_deduped = spark.table(f"aarpdata.bl_expires_{muldate}").dropDuplicates(["memacctnum"])
    df_expires_deduped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"aarpdata.bl_expires_{muldate}")

    # proc sort for emu
    df_emu_deduped = spark.table(f"aarpdata.emu_{muldate}").dropDuplicates(["mid_key"])
    df_emu_deduped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"aarpdata.emu_{muldate}")

# The chmod commands are filesystem-level and not applicable in Databricks.
# Permissions are managed via Unity Catalog or Table ACLs using GRANT statements.

# Final put statements
print(models_list)
# The original bl_vars was from the include file but the script then gets true_bl_vars from metadata.
# We will print true_bl_vars as it's the one used in the logic.
print(" ".join(true_bl_vars))

#End-DBShift