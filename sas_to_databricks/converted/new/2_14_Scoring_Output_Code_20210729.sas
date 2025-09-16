from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, DoubleType
import datetime
from itertools import chain

spark = SparkSession.builder.appName("Scoring_Output_Code").getOrCreate()

runtype = "Monthly"
muldate = datetime.datetime.now().strftime('%Y%m%d')
create_dt = muldate

tables_to_delete = [
    "all_", "bl_rank", "bymailerid", "click_", "combo", "contact", "email", "lo_aca_scores",
    "mailer", "medicaid_score", "runner", "score_", "sent_", "vt", "web_visit", "rest_rr", "rr2"
]
for table_name in tables_to_delete:
    spark.sql(f"DROP TABLE IF EXISTS work.{table_name}")

df_new_masters2012_formats = spark.table("scoring.New_masters2012_VIGINTILES")

# The PROC FORMAT from CNTLIN=new_masters2012_formats is represented by having the above DataFrame available.
# The format itself is not directly used in the subsequent SAS code provided.

scoreout_map = {
    'NEW_SCORE1_CENTILE': 'NMAS_1_HLTH_DCL', 'NEW_SCORE2_CENTILE': 'NMAS_2_HLTH_SEC', 'NEW_SCORE3_CENTILE': 'NMAS_3_REN_HLTH',
    'NEW_SCORE4_CENTILE': 'NMAS_4_PARENT', 'NEW_SCORE5_CENTILE': 'NMAS_5_LIFE_INS', 'NEW_SCORE6_CENTILE': 'NMAS_6_FIN_SEC',
    'NEW_SCORE7_CENTILE': 'NMAS_7_CONSUME', 'NEW_SCORE8_CENTILE': 'NMAS_8_EMPLOY', 'NEW_SCORE9_CENTILE': 'NMAS_9_INVEST',
    'NEW_SCORE10_CENTILE': 'NMAS_10_CREDIT', 'NEW_SCORE11_CENTILE': 'NMAS_11_RETIRE', 'NEW_SCORE12_CENTILE': 'NMAS_12_HME_INS',
    'NEW_SCORE13_CENTILE': 'NMAS_13_CAR_INS', 'NEW_SCORE14_CENTILE': 'NMAS_14_COM_INV', 'NEW_SCORE15_CENTILE': 'NMAS_15_LIV_COM',
    'NEW_SCORE16_CENTILE': 'NMAS_16_LOCAL', 'NEW_SCORE17_CENTILE': 'NMAS_17_CHARITY', 'NEW_SCORE18_CENTILE': 'NMAS_18_ADVOCY',
    'NEW_SCORE19_CENTILE': 'NMAS_19_VOLUNT', 'NEW_SCORE20_CENTILE': 'NMAS_20_TRAVEL', 'NEW_SCORE21_CENTILE': 'NMAS_21_LEISURE',
    'NEW_SCORE22_CENTILE': 'NMAS_22_CHILDRN', 'NEW_SCORE23_CENTILE': 'NMAS_23_MOTORNG', 'NEW_SCORE24_CENTILE': 'NMAS_24_PROGSVC',
    'NEW_SCORE25_CENTILE': 'NMAS_25_ATTITDE', 'NEW_SCORE26_CENTILE': 'NMAS_26_WEBUSE', 'NEW_SCORE27_CENTILE': 'NMAS_27_PUBS',
    'NEW_SCORE29_CENTILE': 'NMAS_29_LIFE', 'NEW_SCORE30_CENTILE': 'NMAS_30_ONLINE', 'NEW_SCORE31_CENTILE': 'NMAS_31_DRVS',
    'NEW_SCORE32_CENTILE': 'NMAS_32_JOB_SEC', 'NEW_SCORE33_CENTILE': 'NMAS_33_MEDICAR', 'NEW_SCORE34_CENTILE': 'NMAS_34_BIZNES',
    'NEW_SCORE35_CENTILE': 'NMAS_35_RENEWAL', 'NEW_SCORE36_CENTILE': 'NMAS_36_UTILITY', 'NEW_SCORE37_CENTILE': 'NMAS_37_REN_OT',
    'NEW_SCORE38_CENTILE': 'NMAS_38_DEALS', 'NEW_SCORE39_CENTILE': 'NMAS_39_SS', 'NEW_SCORE40_CENTILE': 'NMAS_40_LTC',
    'NEW_SCORE41_CENTILE': 'NMAS_41_MAIL', 'NEW_SCORE42_CENTILE': 'NMAS_42_EMAIL', 'NEW_SCORE43_CENTILE': 'NMAS_43_PHONE',
    'NEW_SCORE44_CENTILE': 'NMAS_44_CELL', 'NEW_SCORE45_CENTILE': 'NMAS_45_WEB', 'NEW_SCORE46_CENTILE': 'NMAS_46_TECH',
    'NEW_SCORE47_CENTILE': 'NMAS_47_LOCLENT', 'NEW_SCORE48_CENTILE': 'NMAS_48_SOCIAL', 'NEW_SCORE49_CENTILE': 'NMAS_49_CONNECT',
    'NEW_SCORE50_CENTILE': 'NMAS_50_FINANCE', 'RPM_SCORE': 'RPM_SCORE', 'fndnothr_score': 'FNDNOTHR_SCORE',
    'advo_dm_50_64': 'ADVO_DM_50_64', 'advo_dm_65plus': 'ADVO_DM_65PLUS', 'fndn_pro_em': 'FNDN_PRO_EM',
    'drvsafe_pro_em': 'DRVSAFE_PRO_EM', 'ACA_TTH': 'ACA_TTH', 'WorkNSaveINT_PH': 'WORKNSAVEINT_PH',
    'WorkNSaveACT_PH': 'WORKNSAVEACT_PH', 'CAREGIVER_PH': 'CAREGIVER_PH', 'live_answer_am': 'live_answer_am',
    'live_answer_aft': 'live_answer_aft', 'live_answer_pm': 'live_answer_pm', 'FNDN_Housing': 'FNDN_HOUSING',
    'Work_Jobs_EM': 'WORK_JOBS_EM', 'Auto_Buying_EM': 'AUTO_BUYING_EM', 'Soc_Sec_LO': 'SOC_SEC_LO',
    'TAS_Volunteer': 'TAS_VOLUNTEER', 'Drvsafe_pro_dm': 'DRVSAFE_PRO_DM', 'Fraudwatch_lo': 'FRAUDWATCH_LO',
    'advo_pro_em': 'ADVO_PRO_EM', 'FNDN_AARPPRO_DM': 'FNDN_AARPPRO_DM', 'CPD_CARE_ATTEND': 'CPD_CARE_ATTEND',
    'CPD_JOBS_ATTEND': 'CPD_JOBS_ATTEND', 'CPD_TEK_ATTEND': 'CPD_TEK_ATTEND', 'EM_CLICKRATE': 'EM_CLICKRATE',
    'LO_ACA': 'LO_ACA', 'LO_MEDICAID': 'LO_MEDICAID', 'LO_MEDICARE': 'LO_MEDICARE', 'NPS_DETRACTOR': 'NPS_DETRACTOR',
    'SAVE_PLAN': 'SAVE_PLAN', 'CAREGIVING_EM': 'CAREGIVING_EM', 'DRIVER_SAFETY_TEK_DM': 'DRVSAFE_TEK_DM',
    'DRIVER_SAFETY_TEK_EM': 'DRVSAFE_TEK_EM', 'MEDICARE_EM_ATTEND': 'MEDICARE_ATT_EM', 'WORK_JOBS_ATTENDEE_EM': 'WORKJOB_ATT_EM',
    'CAREGIVING_EM_ATTEND': 'CAREGIVE_ATT_EM', 'MEDICARE_TTH': 'MEDICARE_TTH', 'SOC_SEC_EM_ATT': 'SOC_SEC_EM_ATT',
    'SOC_SEC_EM': 'SOC_SEC_EM', 'rx_advo_65plus': 'RX_ADVO_65PLUS', 'rx_advo_50_64': 'RX_ADVO_50_64',
    'rpm2019_centile': 'RPM2019_CENTILE', 'rpm2019_prob': 'RPM2019_PROB', 'fndn_em_proseng': 'FNDN_EM_PROSENG',
    'fraud_click_em': 'FRAUD_CLICK_EM', 'ipl_care_dm_reg': 'IPL_CARE_DM_REG', 'ipl_job_dm_reg': 'IPL_JOB_DM_REG',
    'ipl_tech_dm_reg': 'IPL_TECH_DM_REG', 'ipl_care_em': 'IPL_CARE_EM', 'ipl_job_em': 'IPL_JOB_EM',
    'ipl_tech_em': 'IPL_TECH_EM', 'rx_lo': 'RX_LO', 'fraud_tth': 'FRAUD_TTH', 'covid19_tth': 'COVID19_TTH',
    'lgbtq_ally': 'LGBTQ_ALLY', 'Speakers_Bureau_Vol': 'SPEAKERS_BUREAU_VOL', 'Volunteer_Leaders': 'VOLUNTEER_LEADERS',
    'DAPM_Catalist_Ideology': 'DAPM_CATALIST_IDEOLOGY', 'Major_Gifts': 'MAJOR_GIFTS', 'ready_travel': 'READY_TRAVEL',
    'ready_cruise': 'READY_CRUISE', 'finra_pckt': 'FINRA_PCKT', 'DAPM_Catalist_Mail_Readership': 'DAPM_CATALIST_MAIL_READERSHIP',
    'DAPM_Catalist_voteprop_mod_2020': 'DAPM_CATALIST_VOTERPROP_MOD_2020', 'ttar_dm': 'TTAR_DM', 'ttar_em': 'TTAR_EM',
    'ready_dine': 'READY_DINE', 'AIAN_Percentile': 'AIAN_PERCENTILE', 'AIAN_Binary_Score': 'AIAN_BINARY_SCORE',
    'DAPM_Catalist_Partisanship_Model': 'DAPM_CATALIST_PARTISANSHIP_MODEL', 'covid_vacc_tth': 'COVID_VACC_TTH',
    'AIAN_Event_Percentile': 'AIAN_EVENT_PERCENTILE', 'fnf_dance': 'FNF_DANCE', 'MFG_Virtual_Screenings': 'MFG_VIRTUAL_SCREENINGS',
    'veterans_attendee': 'VETERANS_ATTENDEE', 'online_banking': 'ONLINE_BANKING'
}

bde_models_csv_path = "/vg01/aarp_sas/ftp/incoming/all_model_scores.csv"

schema_bde_models = StructType([
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

df_bde_models_raw = spark.read.csv(
    bde_models_csv_path, header=True, schema=schema_bde_models, nullValue='.', emptyValue='.'
)

df_bde_models_cleaned = df_bde_models_raw.withColumn(
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

window_spec_dups = Window.partitionBy("mid_key").orderBy(F.lit(1))
df_with_rownum = df_bde_models_cleaned.withColumn("row_num", F.row_number().over(window_spec_dups))

df_bde_models_dups = df_with_rownum.filter(F.col("row_num") > 1).drop("row_num")
df_bde_models = df_with_rownum.filter(F.col("row_num") == 1).drop("row_num")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_bde_models.createOrReplaceTempView("bde_models")

df_current_scores2 = spark.sql("""
    SELECT
        a.new_score1_centile, a.new_score2_centile, a.new_score3_centile, a.new_score4_centile, a.new_score5_centile, 
        a.new_score6_centile, a.new_score7_centile, a.new_score8_centile, a.new_score9_centile, a.new_score10_centile, 
        a.new_score11_centile, a.new_score12_centile, a.new_score13_centile, a.new_score14_centile, a.new_score15_centile, 
        a.new_score16_centile, a.new_score17_centile, a.new_score18_centile, a.new_score19_centile, a.new_score20_centile, 
        a.new_score21_centile, a.new_score22_centile, a.new_score23_centile, a.new_score24_centile, a.new_score25_centile, 
        a.new_score26_centile, a.new_score27_centile, a.new_score28_centile, a.new_score29_centile, a.new_score30_centile, 
        a.new_score31_centile, a.new_score32_centile, a.new_score33_centile, a.new_score34_centile, a.new_score35_centile, 
        a.new_score36_centile, a.new_score37_centile, a.new_score38_centile, a.new_score39_centile, a.new_score40_centile, 
        a.new_score41_centile, a.new_score42_centile, a.new_score43_centile, a.new_score44_centile, a.new_score45_centile, 
        a.new_score46_centile, a.new_score47_centile, a.new_score48_centile, a.new_score49_centile, a.new_score50_centile, 
        a.FNDNOTHR_SCORE, a.advo_dm_50_64, a.advo_dm_65plus, a.fndn_pro_em, a.drvsafe_pro_em, a.aca_tth, 
        a.worknsaveint_ph, a.worknsaveact_ph, a.caregiver_ph, a.merkleid, a.live_answer_am, a.live_answer_aft, 
        a.live_answer_pm, a.FNDN_Housing, a.Work_Jobs_EM, a.Auto_Buying_EM, a.Soc_Sec_LO, a.TAS_Volunteer, 
        a.Drvsafe_pro_dm, a.Fraudwatch_lo, a.advo_pro_em, a.FNDN_AARPPRO_DM, a.CPD_CARE_ATTEND, a.CPD_JOBS_ATTEND, 
        a.CPD_TEK_ATTEND, a.EM_CLICKRATE, a.LO_ACA, a.LO_MEDICAID, a.LO_MEDICARE, a.nps_detractor, a.caregiving_em, 
        a.save_plan, a.driver_safety_tek_dm, a.driver_safety_tek_em, a.WORK_JOBS_ATTENDEE_EM, a.caregiving_em_attend, 
        a.Medicare_em_attend, a.medicare_tth, a.SOC_SEC_EM_ATT, a.SOC_SEC_EM, a.rx_advo_65plus, a.rx_advo_50_64, 
        a.rpm2019_centile, a.rpm2019_prob, a.fndn_em_proseng, a.fraud_click_em, a.ipl_care_dm_reg, a.ipl_job_dm_reg, 
        a.ipl_tech_dm_reg, a.ipl_care_em, a.ipl_job_em, a.ipl_tech_em, a.rx_lo, a.fraud_tth, a.covid19_tth, 
        a.lgbtq_ally, a.Speakers_Bureau_Vol, a.Volunteer_Leaders, a.DAPM_Catalist_Ideology, a.Major_Gifts, a.Finra_pckt, 
        a.ttar_em, a.ttar_dm, a.covid_vacc_tth, a.fnf_dance, a.MFG_Virtual_Screenings, a.veterans_attendee,
        CASE WHEN b.ready_travel IS NULL OR b.ready_travel > 99 THEN 99 ELSE b.ready_travel END AS ready_travel,
        CASE WHEN b.ready_cruise IS NULL OR b.ready_cruise > 99 THEN 99 ELSE b.ready_cruise END AS ready_cruise,
        CASE WHEN b.ready_dine IS NULL OR b.ready_dine > 99 THEN 99 ELSE b.ready_dine END AS ready_dine,
        CASE WHEN b.DAPM_Catalist_Mail_Readership IS NULL OR b.DAPM_Catalist_Mail_Readership > 99 THEN 99 ELSE b.DAPM_Catalist_Mail_Readership END AS DAPM_Catalist_Mail_Readership,
        CASE WHEN b.DAPM_Catalist_voteprop_mod_2020 IS NULL OR b.DAPM_Catalist_voteprop_mod_2020 > 99 THEN 99 ELSE b.DAPM_Catalist_voteprop_mod_2020 END AS DAPM_Catalist_voteprop_mod_2020,
        CASE WHEN b.DAPM_Catalist_Partisanship_Model IS NULL OR b.DAPM_Catalist_Partisanship_Model > 99 THEN 99 ELSE b.DAPM_Catalist_Partisanship_Model END AS DAPM_Catalist_Partisanship_Model,
        CASE WHEN b.AIAN_Percentile IS NULL OR b.AIAN_percentile > 99 THEN 99 ELSE b.AIAN_Percentile END AS AIAN_Percentile,
        CASE WHEN b.AIAN_Binary_Score IS NULL OR b.AIAN_Binary_Score = 0 THEN 99 ELSE b.AIAN_Binary_Score END AS AIAN_Binary_Score,
        CASE WHEN b.AIAN_Event_percentile IS NULL OR b.AIAN_Event_percentile > 99 THEN 99 ELSE b.AIAN_Event_percentile END AS AIAN_Event_percentile,
        CASE WHEN b.online_banking IS NULL OR b.online_banking > 99 THEN 99 ELSE b.online_banking END AS online_banking
    FROM geo_appends_rpm AS a
    LEFT JOIN bde_models AS b ON a.merkleid = CAST(b.mid_key AS STRING)
""")

spark.catalog.dropTempView("bde_models")

all_columns = df_current_scores2.columns
print("Variables found for processing:")
print(all_columns)

score_columns = [c for c in all_columns if c.upper() != "MERKLEID"]
num_vars = len(score_columns)

stack_expr_parts = []
for col_name in score_columns:
    stack_expr_parts.append(f"'{col_name}'")
    stack_expr_parts.append(f"`{col_name}`")

stack_expression = f"stack({num_vars}, {', '.join(stack_expr_parts)}) AS (score_variable, score_value)"
df_unpivoted = df_current_scores2.select("merkleid", F.expr(stack_expression))

mapping_expr = F.create_map([F.lit(x) for x in chain(*scoreout_map.items())])

df_temp1 = df_unpivoted.withColumn(
    "score_name_formatted",
    mapping_expr[F.upper(F.col("score_variable"))]
).withColumn(
    "scorename",
    F.concat_ws(
        "|",
        F.col("merkleid"),
        F.col("score_name_formatted"),
        F.col("score_value"),
        F.lit(create_dt)
    )
).select("scorename")

if runtype == "Weekly":
    run_prefix = "wkly"
elif runtype == "Monthly":
    run_prefix = "mnthly"
else:
    run_prefix = ""

output_path_main = f"/vg01/aarp_sas/ftp/temp/{run_prefix}_SCORES_{muldate}.txt"
output_path_200 = f"/vg01/aarp_sas/ftp/temp/{run_prefix}_SCORES_200_{muldate}.txt"

df_temp1.select("scorename").write.mode("overwrite").text(output_path_main)
df_temp1.limit(200).select("scorename").write.mode("overwrite").text(output_path_200)

df_temp1.createOrReplaceTempView("temp1")
df_scorename = spark.sql("""
    SELECT
        scorename,
        COUNT(scorename) as freq,
        COUNT(scorename) / (SELECT COUNT(*) FROM temp1) as percent
    FROM (
        SELECT
            SPLIT(scorename, '\\\\|')[1] as scorename,
            SPLIT(scorename, '\\\\|')[2] as score
        FROM temp1
    )
    GROUP BY scorename
""")

df_mncnt_output = df_scorename.withColumnRenamed("freq", "count").withColumn(
    "output_line",
    F.format_string("%-32s%8d", F.col("scorename"), F.col("count"))
)
mncnt_path = f"/vg01/aarp_sas/ftp/temp/{run_prefix}_SCORES_{muldate}.MNCNT"
df_mncnt_output.select("output_line").write.mode("overwrite").text(mncnt_path)

df_scorename.createOrReplaceTempView("scorename_view_freq")
df_temp2_sumry = spark.sql("""
    SELECT SUM(freq) as total_records FROM scorename_view_freq
""")

cnt_path = f"/vg01/aarp_sas/ftp/temp/{run_prefix}_SCORES_{muldate}.CNT"
df_temp2_sumry.select(F.col("total_records").cast("string")).write.mode("overwrite").text(cnt_path)

print("ODS PDF FILE='/vg01/aarp_sas/aarp_output/Scoring_Output/Updated_scores_diagnostics_{runtype}_{muldate}.PDF'")
print("Scoring Diagnostic Report")
print(f"Data as of {muldate} {runtype}")

df_scorename.show(n=df_scorename.count(), truncate=False)

print(f"{runtype} Frequencies - {muldate}")

df_scorename_renamed = df_scorename.select(
    F.col("scorename"),
    F.col("freq").alias("score_tot")
)
df_scorename_renamed.createOrReplaceTempView("scorename_view_total")
df_temp1.createOrReplaceTempView("temp1")

df_final_report = spark.sql("""
    WITH temp_score AS (
        SELECT
            SPLIT(scorename, '\\\\|')[1] AS scorename,
            CAST(SPLIT(scorename, '\\\\|')[2] AS DOUBLE) AS score
        FROM temp1
    ),
    grouped_score AS (
        SELECT
            scorename,
            score,
            COUNT(score) as freq
        FROM temp_score
        GROUP BY scorename, score
    )
    SELECT
        a.scorename,
        a.score,
        a.freq,
        a.freq / b.score_tot AS percent
    FROM grouped_score a
    JOIN scorename_view_total b ON a.scorename = b.scorename
""")
df_final_report.show(n=df_final_report.count(), truncate=False)

#End-DBShift