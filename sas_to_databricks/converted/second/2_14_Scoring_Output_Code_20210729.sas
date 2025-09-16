import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql import Window
from itertools import chain

# Placeholder for SAS macro variables. These should be set by the execution environment.
runtype = "Monthly"
MULDATE = "20230101" # Example date
create_dt = MULDATE

# Initialize Spark Session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Scoring_Output_Code").getOrCreate()

# Clean out work datasets
spark.sql("DROP TABLE IF EXISTS work.all_")
spark.sql("DROP TABLE IF EXISTS work.bl_rank")
spark.sql("DROP TABLE IF EXISTS work.bymailerid")
spark.sql("DROP TABLE IF EXISTS work.click_")
spark.sql("DROP TABLE IF EXISTS work.combo")
spark.sql("DROP TABLE IF EXISTS work.contact")
spark.sql("DROP TABLE IF EXISTS work.email")
spark.sql("DROP TABLE IF EXISTS work.lo_aca_scores")
spark.sql("DROP TABLE IF EXISTS work.mailer")
spark.sql("DROP TABLE IF EXISTS work.medicaid_score")
spark.sql("DROP TABLE IF EXISTS work.runner")
spark.sql("DROP TABLE IF EXISTS work.score_")
spark.sql("DROP TABLE IF EXISTS work.sent_")
spark.sql("DROP TABLE IF EXISTS work.vt")
spark.sql("DROP TABLE IF EXISTS work.web_visit")
spark.sql("DROP TABLE IF EXISTS work.rest_rr")
spark.sql("DROP TABLE IF EXISTS work.rr2")

df_new_masters2012_formats = spark.table("scoring.New_masters2012_VIGINTILES")

# In PySpark, formats are applied via transformations, not a central registry.
# The 'new_masters2012_formats' data is loaded but not explicitly used later in the SAS code.
# The primary format used is '$scoreout', which is defined below as a dictionary.

scoreout_map_dict = {
    'NEW_SCORE1_CENTILE':'NMAS_1_HLTH_DCL',
    'NEW_SCORE2_CENTILE':'NMAS_2_HLTH_SEC',
    'NEW_SCORE3_CENTILE':'NMAS_3_REN_HLTH',
    'NEW_SCORE4_CENTILE':'NMAS_4_PARENT',
    'NEW_SCORE5_CENTILE':'NMAS_5_LIFE_INS',
    'NEW_SCORE6_CENTILE':'NMAS_6_FIN_SEC',
    'NEW_SCORE7_CENTILE':'NMAS_7_CONSUME',
    'NEW_SCORE8_CENTILE':'NMAS_8_EMPLOY',
    'NEW_SCORE9_CENTILE':'NMAS_9_INVEST',
    'NEW_SCORE10_CENTILE':'NMAS_10_CREDIT',
    'NEW_SCORE11_CENTILE':'NMAS_11_RETIRE',
    'NEW_SCORE12_CENTILE':'NMAS_12_HME_INS',
    'NEW_SCORE13_CENTILE':'NMAS_13_CAR_INS',
    'NEW_SCORE14_CENTILE':'NMAS_14_COM_INV',
    'NEW_SCORE15_CENTILE':'NMAS_15_LIV_COM',
    'NEW_SCORE16_CENTILE':'NMAS_16_LOCAL',
    'NEW_SCORE17_CENTILE':'NMAS_17_CHARITY',
    'NEW_SCORE18_CENTILE':'NMAS_18_ADVOCY',
    'NEW_SCORE19_CENTILE':'NMAS_19_VOLUNT',
    'NEW_SCORE20_CENTILE':'NMAS_20_TRAVEL',
    'NEW_SCORE21_CENTILE':'NMAS_21_LEISURE',
    'NEW_SCORE22_CENTILE':'NMAS_22_CHILDRN',
    'NEW_SCORE23_CENTILE':'NMAS_23_MOTORNG',
    'NEW_SCORE24_CENTILE':'NMAS_24_PROGSVC',
    'NEW_SCORE25_CENTILE':'NMAS_25_ATTITDE',
    'NEW_SCORE26_CENTILE':'NMAS_26_WEBUSE',
    'NEW_SCORE27_CENTILE':'NMAS_27_PUBS',
    'NEW_SCORE29_CENTILE':'NMAS_29_LIFE',
    'NEW_SCORE30_CENTILE':'NMAS_30_ONLINE',
    'NEW_SCORE31_CENTILE':'NMAS_31_DRVS',
    'NEW_SCORE32_CENTILE':'NMAS_32_JOB_SEC',
    'NEW_SCORE33_CENTILE':'NMAS_33_MEDICAR',
    'NEW_SCORE34_CENTILE':'NMAS_34_BIZNES',
    'NEW_SCORE35_CENTILE':'NMAS_35_RENEWAL',
    'NEW_SCORE36_CENTILE':'NMAS_36_UTILITY',
    'NEW_SCORE37_CENTILE':'NMAS_37_REN_OT',
    'NEW_SCORE38_CENTILE':'NMAS_38_DEALS',
    'NEW_SCORE39_CENTILE':'NMAS_39_SS',
    'NEW_SCORE40_CENTILE':'NMAS_40_LTC',
    'NEW_SCORE41_CENTILE':'NMAS_41_MAIL',
    'NEW_SCORE42_CENTILE':'NMAS_42_EMAIL',
    'NEW_SCORE43_CENTILE':'NMAS_43_PHONE',
    'NEW_SCORE44_CENTILE':'NMAS_44_CELL',
    'NEW_SCORE45_CENTILE':'NMAS_45_WEB',
    'NEW_SCORE46_CENTILE':'NMAS_46_TECH',
    'NEW_SCORE47_CENTILE':'NMAS_47_LOCLENT',
    'NEW_SCORE48_CENTILE':'NMAS_48_SOCIAL',
    'NEW_SCORE49_CENTILE':'NMAS_49_CONNECT',
    'NEW_SCORE50_CENTILE':'NMAS_50_FINANCE',
    'RPM_SCORE':'RPM_SCORE',
    'fndnothr_score':'FNDNOTHR_SCORE',
    'advo_dm_50_64':'ADVO_DM_50_64',
    'advo_dm_65plus':'ADVO_DM_65PLUS',
    'fndn_pro_em':'FNDN_PRO_EM',
    'drvsafe_pro_em':'DRVSAFE_PRO_EM',
    'ACA_TTH':'ACA_TTH',
    'WorkNSaveINT_PH':'WORKNSAVEINT_PH',
    'WorkNSaveACT_PH':'WORKNSAVEACT_PH',
    'CAREGIVER_PH':'CAREGIVER_PH',
    'live_answer_am': 'live_answer_am',
    'live_answer_aft':'live_answer_aft',
    'live_answer_pm':'live_answer_pm',
    'FNDN_Housing':'FNDN_HOUSING',
    'Work_Jobs_EM':'WORK_JOBS_EM',
    'Auto_Buying_EM':'AUTO_BUYING_EM',
    'Soc_Sec_LO':'SOC_SEC_LO',
    'TAS_Volunteer':'TAS_VOLUNTEER',
    'Drvsafe_pro_dm':'DRVSAFE_PRO_DM',
    'Fraudwatch_lo':'FRAUDWATCH_LO',
    'advo_pro_em':'ADVO_PRO_EM',
    'FNDN_AARPPRO_DM':'FNDN_AARPPRO_DM',
    'CPD_CARE_ATTEND':'CPD_CARE_ATTEND',
    'CPD_JOBS_ATTEND':'CPD_JOBS_ATTEND',
    'CPD_TEK_ATTEND':'CPD_TEK_ATTEND',
    'EM_CLICKRATE':'EM_CLICKRATE',
    'LO_ACA':'LO_ACA',
    'LO_MEDICAID':'LO_MEDICAID',
    'LO_MEDICARE':'LO_MEDICARE',
    'NPS_DETRACTOR':'NPS_DETRACTOR',
    'SAVE_PLAN':'SAVE_PLAN',
    'CAREGIVING_EM':'CAREGIVING_EM',
    'DRIVER_SAFETY_TEK_DM':'DRVSAFE_TEK_DM',
    'DRIVER_SAFETY_TEK_EM':'DRVSAFE_TEK_EM',
    'MEDICARE_EM_ATTEND':'MEDICARE_ATT_EM',
    'WORK_JOBS_ATTENDEE_EM':'WORKJOB_ATT_EM',
    'CAREGIVING_EM_ATTEND':'CAREGIVE_ATT_EM',
    'MEDICARE_TTH':'MEDICARE_TTH',
    'SOC_SEC_EM_ATT':'SOC_SEC_EM_ATT',
    'SOC_SEC_EM':'SOC_SEC_EM',
    'rx_advo_65plus':'RX_ADVO_65PLUS',
    'rx_advo_50_64':'RX_ADVO_50_64',
    'rpm2019_centile':'RPM2019_CENTILE',
    'rpm2019_prob':'RPM2019_PROB',
    'fndn_em_proseng':'FNDN_EM_PROSENG',
    'fraud_click_em':'FRAUD_CLICK_EM',
    'ipl_care_dm_reg':'IPL_CARE_DM_REG',
    'ipl_job_dm_reg':'IPL_JOB_DM_REG',
    'ipl_tech_dm_reg':'IPL_TECH_DM_REG',
    'ipl_care_em':'IPL_CARE_EM',
    'ipl_job_em':'IPL_JOB_EM',
    'ipl_tech_em':'IPL_TECH_EM',
    'rx_lo':'RX_LO',
    'fraud_tth':'FRAUD_TTH',
    'covid19_tth':'COVID19_TTH',
    'lgbtq_ally':'LGBTQ_ALLY',
    'Speakers_Bureau_Vol':'SPEAKERS_BUREAU_VOL',
    'Volunteer_Leaders':'VOLUNTEER_LEADERS',
    'DAPM_Catalist_Ideology':'DAPM_CATALIST_IDEOLOGY',
    'Major_Gifts':'MAJOR_GIFTS',
    'ready_travel':'READY_TRAVEL',
    'ready_cruise':'READY_CRUISE',
    'finra_pckt':'FINRA_PCKT',
    'DAPM_Catalist_Mail_Readership':'DAPM_CATALIST_MAIL_READERSHIP',
    'DAPM_Catalist_voteprop_mod_2020':'DAPM_CATALIST_VOTERPROP_MOD_2020',
    'ttar_dm':'TTAR_DM',
    'ttar_em':'TTAR_EM',
    'ready_dine':'READY_DINE',
    'AIAN_Percentile':'AIAN_PERCENTILE',
    'AIAN_Binary_Score':'AIAN_BINARY_SCORE',
    'DAPM_Catalist_Partisanship_Model':'DAPM_CATALIST_PARTISANSHIP_MODEL',
    'covid_vacc_tth':'COVID_VACC_TTH',
    'AIAN_Event_Percentile':'AIAN_EVENT_PERCENTILE',
    'fnf_dance':'FNF_DANCE',
    'MFG_Virtual_Screenings':'MFG_VIRTUAL_SCREENINGS',
    'veterans_attendee':'VETERANS_ATTENDEE',
    'online_banking':'ONLINE_BANKING'
}

bde_models_schema = StructType([
    StructField("mid_key", LongType(), True),
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

df_bde_models_raw = spark.read.csv(
    "/vg01/aarp_sas/ftp/incoming/all_model_scores.csv",
    schema=bde_models_schema,
    header=True,
    delimiter=","
)

df_bde_models = df_bde_models_raw.withColumn(
    "DAPM_Catalist_Mail_Readership",
    F.when(F.col("DAPM_Catalist_Mail_Readership").isNull() | (F.col("DAPM_Catalist_Mail_Readership") > 99), 99)
    .otherwise(F.col("DAPM_Catalist_Mail_Readership"))
).withColumn(
    "DAPM_Catalist_voteprop_mod_2020",
    F.when(F.col("DAPM_Catalist_voteprop_mod_2020").isNull() | (F.col("DAPM_Catalist_voteprop_mod_2020") > 99), 99)
    .otherwise(F.col("DAPM_Catalist_voteprop_mod_2020"))
).withColumn(
    "DAPM_Catalist_Partisanship_Model",
    F.when(F.col("DAPM_Catalist_Partisanship_Model").isNull() | (F.col("DAPM_Catalist_Partisanship_Model") > 99), 99)
    .otherwise(F.col("DAPM_Catalist_Partisanship_Model"))
).withColumn(
    "AIAN_Percentile",
    F.when(F.col("AIAN_Percentile").isNull() | (F.col("AIAN_Percentile") > 99), 99)
    .otherwise(F.col("AIAN_Percentile"))
).withColumn(
    "AIAN_Binary_Score",
    F.when(F.col("AIAN_Binary_Score").isNull() | (F.col("AIAN_Binary_Score") == 0), 99)
    .otherwise(F.col("AIAN_Binary_Score"))
).withColumn(
    "AIAN_Event_Percentile",
    F.when(F.col("AIAN_Event_Percentile").isNull() | (F.col("AIAN_Event_Percentile") > 99), 99)
    .otherwise(F.col("AIAN_Event_Percentile"))
).withColumn(
    "ready_travel",
    F.when(F.col("ready_travel").isNull() | (F.col("ready_travel") > 99), 99)
    .otherwise(F.col("ready_travel"))
).withColumn(
    "ready_cruise",
    F.when(F.col("ready_cruise").isNull() | (F.col("ready_cruise") > 99), 99)
    .otherwise(F.col("ready_cruise"))
).withColumn(
    "ready_dine",
    F.when(F.col("ready_dine").isNull() | (F.col("ready_dine") > 99), 99)
    .otherwise(F.col("ready_dine"))
).withColumn(
    "online_banking",
    F.when(F.col("online_banking").isNull() | (F.col("online_banking") > 99), 99)
    .otherwise(F.col("online_banking"))
)

df_bde_models = df_bde_models.dropDuplicates(["mid_key"])

columns_to_keep = [
    "new_score1_centile", "new_score2_centile", "new_score3_centile", "new_score4_centile", "new_score5_centile",
    "new_score6_centile", "new_score7_centile", "new_score8_centile", "new_score9_centile", "new_score10_centile",
    "new_score11_centile", "new_score12_centile", "new_score13_centile", "new_score14_centile", "new_score15_centile",
    "new_score16_centile", "new_score17_centile", "new_score18_centile", "new_score19_centile", "new_score20_centile",
    "new_score21_centile", "new_score22_centile", "new_score23_centile", "new_score24_centile", "new_score25_centile",
    "new_score26_centile", "new_score27_centile", "new_score29_centile", "new_score30_centile",
    "new_score31_centile", "new_score32_centile", "new_score33_centile", "new_score34_centile", "new_score35_centile",
    "new_score36_centile", "new_score37_centile", "new_score38_centile", "new_score39_centile", "new_score40_centile",
    "new_score41_centile", "new_score42_centile", "new_score43_centile", "new_score44_centile", "new_score45_centile",
    "new_score46_centile", "new_score47_centile", "new_score48_centile", "new_score49_centile", "new_score50_centile",
    "FNDNOTHR_SCORE", "advo_dm_50_64", "advo_dm_65plus", "fndn_pro_em", "drvsafe_pro_em", "aca_tth", "worknsaveint_ph",
    "worknsaveact_ph", "caregiver_ph", "merkleid", "live_answer_am", "live_answer_aft", "live_answer_pm", "FNDN_Housing",
    "Work_Jobs_EM", "Auto_Buying_EM", "Soc_Sec_LO", "TAS_Volunteer", "Drvsafe_pro_dm", "Fraudwatch_lo", "advo_pro_em",
    "FNDN_AARPPRO_DM", "CPD_CARE_ATTEND", "CPD_JOBS_ATTEND", "CPD_TEK_ATTEND", "EM_CLICKRATE", "LO_ACA", "LO_MEDICAID",
    "LO_MEDICARE", "nps_detractor", "caregiving_em", "save_plan", "driver_safety_tek_dm", "driver_safety_tek_em",
    "WORK_JOBS_ATTENDEE_EM", "caregiving_em_attend", "Medicare_em_attend", "medicare_tth", "SOC_SEC_EM_ATT", "SOC_SEC_EM",
    "rx_advo_65plus", "rx_advo_50_64", "rpm2019_centile", "rpm2019_prob", "fndn_em_proseng", "fraud_click_em",
    "ipl_care_dm_reg", "ipl_job_dm_reg", "ipl_tech_dm_reg", "ipl_care_em", "ipl_job_em", "ipl_tech_em", "rx_lo", "fraud_tth",
    "covid19_tth", "lgbtq_ally", "Speakers_Bureau_Vol", "Volunteer_Leaders", "DAPM_Catalist_Ideology", "Major_Gifts", "Finra_pckt",
    "ttar_em", "ttar_dm", "covid_vacc_tth", "fnf_dance", "MFG_Virtual_Screenings", "veterans_attendee"
]

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm").select(*columns_to_keep)

df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_bde_models.createOrReplaceTempView("bde_models")

df_current_scores2 = spark.sql("""
    SELECT
        a.*,
        CASE
            WHEN b.ready_travel IS NULL OR b.ready_travel > 99 THEN 99
            ELSE b.ready_travel
        END AS ready_travel,
        CASE
            WHEN b.ready_cruise IS NULL OR b.ready_cruise > 99 THEN 99
            ELSE b.ready_cruise
        END AS ready_cruise,
        CASE
            WHEN b.ready_dine IS NULL OR b.ready_dine > 99 THEN 99
            ELSE b.ready_dine
        END AS ready_dine,
        CASE
            WHEN b.DAPM_Catalist_Mail_Readership IS NULL OR b.DAPM_Catalist_Mail_Readership > 99 THEN 99
            ELSE b.DAPM_Catalist_Mail_Readership
        END AS DAPM_Catalist_Mail_Readership,
        CASE
            WHEN b.DAPM_Catalist_voteprop_mod_2020 IS NULL OR b.DAPM_Catalist_voteprop_mod_2020 > 99 THEN 99
            ELSE b.DAPM_Catalist_voteprop_mod_2020
        END AS DAPM_Catalist_voteprop_mod_2020,
        CASE
            WHEN b.DAPM_Catalist_Partisanship_Model IS NULL OR b.DAPM_Catalist_Partisanship_Model > 99 THEN 99
            ELSE b.DAPM_Catalist_Partisanship_Model
        END AS DAPM_Catalist_Partisanship_Model,
        CASE
            WHEN b.AIAN_Percentile IS NULL OR b.AIAN_percentile > 99 THEN 99
            ELSE b.AIAN_Percentile
        END AS AIAN_Percentile,
        CASE
            WHEN b.AIAN_Binary_Score IS NULL OR b.AIAN_Binary_Score = 0 THEN 99
            ELSE b.AIAN_Binary_Score
        END AS AIAN_Binary_Score,
        CASE
            WHEN b.AIAN_Event_percentile IS NULL OR b.AIAN_Event_percentile > 99 THEN 99
            ELSE b.AIAN_Event_percentile
        END AS AIAN_Event_percentile,
        CASE
            WHEN b.online_banking IS NULL OR b.online_banking > 99 THEN 99
            ELSE b.online_banking
        END AS online_banking
    FROM
        geo_appends_rpm a
    LEFT JOIN
        bde_models b ON a.merkleid = CAST(b.mid_key AS STRING)
""")

spark.sql("DROP TABLE IF EXISTS work.bde_models")

cols_to_unpivot = [c for c in df_current_scores2.columns if c.upper() != "MERKLEID"]
stack_expressions = []
for col_name in cols_to_unpivot:
    stack_expressions.append(f"'{col_name}', `{col_name}`")

stack_expr_str = f"stack({len(cols_to_unpivot)}, {', '.join(stack_expressions)}) as (score_variable, score_value)"

df_long = df_current_scores2.select(F.col("merkleid"), F.expr(stack_expr_str))

mapping_expr = F.create_map([F.lit(x) for x in chain(*scoreout_map_dict.items())])

df_temp1 = df_long.withColumn(
    "scorename",
    F.concat_ws(
        "|",
        F.col("merkleid"),
        mapping_expr[F.upper(F.col("score_variable"))],
        F.col("score_value"),
        F.lit(create_dt)
    )
).select("scorename")

spark.catalog.dropTempView("geo_appends_rpm")

if runtype == "Weekly":
    run_prefix = "wkly"
if runtype == "Monthly":
    run_prefix = "mnthly"

df_temp1.select("scorename").write.format("text").mode("overwrite").save(f"/vg01/aarp_sas/ftp/temp/{run_prefix}_SCORES_{MULDATE}.txt")

df_temp1.limit(200).select("scorename").write.format("text").mode("overwrite").save(f"/vg01/aarp_sas/ftp/temp/{run_prefix}_SCORES_200_{MULDATE}.txt")

df_parsed = df_temp1.withColumn("scorename_part", F.split(F.col("scorename"), "\\|").getItem(1)) \
                    .withColumn("score_part", F.split(F.col("scorename"), "\\|").getItem(2))

df_scorename = df_parsed.groupBy("scorename_part").agg(
    F.count("scorename_part").alias("freq")
).withColumn(
    "percent",
    (F.col("freq") / df_temp1.count())
).withColumnRenamed("scorename_part", "scorename")

# Create formatted output for the MNCNT file
df_scorename_output = df_scorename.select(
    F.format_string("%-32s%8d", F.col("scorename"), F.col("freq")).alias("formatted_line")
)
df_scorename_output.write.format("text").mode("overwrite").save(f"/vg01/aarp_sas/ftp/temp/{run_prefix}_SCORES_{MULDATE}.MNCNT")

df_temp2_sumry = df_scorename.agg(F.sum("freq").alias("total_records"))
df_temp2_sumry.select("total_records").write.format("text").mode("overwrite").save(f"/vg01/aarp_sas/ftp/temp/{run_prefix}_SCORES_{MULDATE}.CNT")

print("Scoring Diagnostic Report")
print(f"Data as of {MULDATE} {runtype}")

df_scorename.select("scorename", "freq", F.format_number("percent", 4).alias("percent")).show(truncate=False)

print(f"{runtype} Frequencies - {MULDATE}")

df_temp_score_base = df_parsed.select(
    F.col("scorename_part").alias("scorename"),
    F.col("score_part").cast("double").alias("score")
)

df_temp_score = df_temp_score_base.groupBy("scorename", "score").agg(F.count("score").alias("freq"))

df_scorename_totals = df_scorename.select(
    F.col("scorename"),
    F.col("freq").alias("score_tot")
)

df_final_report = df_temp_score.join(df_scorename_totals, "scorename", "inner")
df_final_report = df_final_report.withColumn("percent", F.col("freq") / F.col("score_tot"))

df_final_report.select(
    "scorename",
    "score",
    "freq",
    F.format_number("percent", 4).alias("percent")
).orderBy("scorename", "score").show(truncate=False)

print("MDSS - FOR INTERNAL USE ONLY")

#End-DBShift