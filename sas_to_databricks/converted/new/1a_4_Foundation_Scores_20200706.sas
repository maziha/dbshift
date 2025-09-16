import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType, IntegerType, DateType
from functools import reduce

spark = SparkSession.builder.appName("Foundation_Scores").getOrCreate()

# Placeholder variables for SAS macro variables
# These should be set by the execution environment (e.g., Databricks widgets)
MULDATE = "20230101" # Example date
conn = "your_connection_alias"
dsn = "your_dsn"
usern = "your_username"
passw = "your_password"
jdbc_url = "jdbc:your_db_url" # Example JDBC URL
ref2 = "your_schema"
fndncurr = "'val1', 'val2'" # Example values
fndnminus1 = "'val3', 'val4'"
fndnminus2 = "'val5', 'val6'"
runtype = "PROD"

# PROC FORMAT is translated by reading the format definition table.
# This DataFrame will be used later to apply the format.
df_new_fndnothr_percentiles = spark.table("scoring.new_fndnothr_percentiles")

# PROC SQL: create table mul_demo_cont
df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_fndnmodelcontacthistfinal = spark.table("fndn.fndnmodelcontacthistfinal")

df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_fndnmodelcontacthistfinal.createOrReplaceTempView("fndnmodelcontacthistfinal")

df_mul_demo_cont = spark.sql("""
    SELECT
        a.mid_key,
        a.memacctnum,
        a.NbrTimesSelEmailedInd,
        a.MemXRenew,
        a.mempaiddate,
        a.curr_order_create_dt,
        a.PartyAffiliation,
        a.Past3MoTouchCt_Financial,
        a.Past12MoTouchCt_Health,
        a.Zip,
        a.ZipPlus4,
        a.mail_order_donor_categories,
        a.MEMBER_FL_AGG_IND,
        a.IBX_HOME_ASSESSED_VALUE_RANGES,
        a.IBX_MOVIE_MUSIC_GROUPING,
        a.IBX_RACE_CD_INPUT_INDIVIDUAL_PRE,
        a.IBX_TOTAL_ONLINE_PURCHASES,
        a.IBX_PC_OWNER_PREMIER,
        a.IBX_WEEKS_SINCE_LAST_ONLINE_ORDE,
        a.IBX_RETAIL_PURCHASES_MOST_FREQUE,
        a.Advo_Last_Petition_Dt_Agg_Ind,
        a.ch_acq,
        a.donadv12,
        a.Globally_opted_in,
        a.Past3MoTouchCt_Overall,
        a.IBX_COMMUNITY_CHARITIES_AGG_HHD,
        a.IBX_ADULT_AGE_65_74_AGG_HHD,
        a.IBX_MAIL_BUYER_CAT_HEALTH_AGG_HH,
        a.IBX_EDUCATION,
        a.gender_agg_ind,
        a.life_stage,
        a.IBX_DONATION_CONTRIBUTION,
        a.IBX_INFERRED_HOUSEHOLD_RANK,
        a.LapsMail,
        a.orders_online,
        a.orders_altmedia,
        b.Acknow AS acknow_fndn
    FROM geo_appends_rpm AS a
    LEFT JOIN fndnmodelcontacthistfinal AS b
        ON a.mid_key = b.mid_key
""")

# PROC SQL: create table mul_div_temp
df_new_geocodes = spark.table("aarpdata.new_geocodes")
df_mul_demo_cont.createOrReplaceTempView("mul_demo_cont")
df_new_geocodes.createOrReplaceTempView("new_geocodes")

df_mul_div_temp = spark.sql("""
    SELECT a.*, b.geocode AS geocode_new
    FROM mul_demo_cont a
    LEFT JOIN new_geocodes b
    ON a.zip = b.zip AND a.ZipPlus4 = b.zip4
""")

# PROC SQL: create table New_FNDN_Model_Data
df_geodata = spark.table("aarpdata.geodata")
df_mul_div_temp.createOrReplaceTempView("mul_div_temp")
df_geodata.createOrReplaceTempView("geodata")

df_New_FNDN_Model_Data = spark.sql("""
    SELECT a.*,
           b.HH_pct_Male_HOH_Fam_W_per_LT18,
           b.pct_Other_Relative_in_Family_HH,
           b.HU_pct_Occupied,
           b.Inc_HH_Med_Inc_HHer_Age_65_74,
           b.Avg_Trav_Time_to_Work,
           b.OOHU_pct_Home_Value_125_149K,
           b.HH_pct_HOH_NonHispHINatOthPIOnly,
           b.pct_in_Households,
           b.Rntl_pct_Cash_550_599
    FROM mul_div_temp AS a
    LEFT JOIN geodata AS b
    ON a.geocode_new = b.geocode
""")

# PROC SORT NODUPKEY
df_New_FNDN_Model_Data = df_New_FNDN_Model_Data.dropDuplicates(["mid_key"])

# DATA New_fndn_model
df_new_fndn_model_base = df_New_FNDN_Model_Data.select(
    "mid_key", "memacctnum", "PartyAffiliation", "curr_order_create_dt", "Past3MoTouchCt_Financial",
    "NbrTimesSelEmailedInd", "MemXRenew", "Past12MoTouchCt_Health", "mempaiddate",
    "HH_pct_Male_HOH_Fam_W_per_LT18", "pct_Other_Relative_in_Family_HH", "HU_pct_Occupied",
    "Inc_HH_Med_Inc_HHer_Age_65_74", "Avg_Trav_Time_to_Work", "OOHU_pct_Home_Value_125_149K",
    "HH_pct_HOH_NonHispHINatOthPIOnly", "pct_in_Households", "Rntl_pct_Cash_550_599",
    "mail_order_donor_categories", "acknow_fndn"
).withColumnRenamed("Acknow_fndn", "nAcknow") \
 .withColumnRenamed("PartyAffiliation", "nPartyAffiliation") \
 .withColumnRenamed("Past3MoTouchCt_Financial", "nPast3MoTouchCt_Financial") \
 .withColumnRenamed("NbrTimesSelEmailedInd", "nNbrTimesSelEmailedInd") \
 .withColumnRenamed("mail_order_donor_categories", "nmail_order_donor_categories") \
 .withColumnRenamed("Past12MoTouchCt_Health", "nPast12MoTouchCt_Health")

df_transformed = df_new_fndn_model_base.withColumn("Past3MoTouchCt_Financial", F.when(F.col("nPast3MoTouchCt_Financial") == "", None).otherwise(F.col("nPast3MoTouchCt_Financial")).cast(DoubleType())) \
    .withColumn("NbrTimesSelEmailedInd", F.when(F.col("nNbrTimesSelEmailedInd") == "", None).otherwise(F.col("nNbrTimesSelEmailedInd")).cast(DoubleType())) \
    .withColumn("Past12MoTouchCt_Health", F.when(F.col("nPast12MoTouchCt_Health") == "", None).otherwise(F.col("nPast12MoTouchCt_Health")).cast(DoubleType())) \
    .withColumn("Acknow", F.regexp_replace(F.col("nAcknow").cast(StringType()), " ", "")) \
    .withColumn("PartyAffiliation", F.regexp_replace(F.col("nPartyAffiliation").cast(StringType()), " ", "")) \
    .withColumn("mail_order_donor_categories", F.regexp_replace(F.col("nmail_order_donor_categories").cast(StringType()), " ", ""))

# Translating the commented-out (but only logical) block for MailOrderDonorSum
sum_expr = F.lit(0)
for i in range(1, 14):
    sum_expr = sum_expr + F.when(F.substring(F.col("mail_order_donor_categories"), i, 1) == '1', 1).otherwise(0)
df_transformed = df_transformed.withColumn("MailOrderDonorSum", sum_expr)

# Date calculations
df_transformed = df_transformed.withColumn("muldate_dt", F.to_date(F.lit(MULDATE), "yyyyMMdd")) \
    .withColumn("day", F.dayofmonth(F.col("muldate_dt")))

df_transformed = df_transformed.withColumn("mempaiddate_new", F.to_date(F.col("mempaiddate").cast(StringType()), "yyyyMMdd")) \
    .withColumn("curr_order_create_dt_new", F.to_date(F.col("curr_order_create_dt").cast(StringType()), "yyyyMMdd"))

df_transformed = df_transformed.withColumn("new_MonthstoExpire", F.months_between(F.col("mempaiddate_new"), F.col("muldate_dt"))) \
    .withColumn("MonthsSinceLastOrder", F.months_between(F.col("muldate_dt"), F.col("curr_order_create_dt_new")))

# Imputations
df_transformed = df_transformed.withColumn("Acknow_X", F.when(F.col("Acknow").isNull() | (F.col("Acknow") == '.'), '0').otherwise(F.col("Acknow"))) \
    .withColumn("Past3MoTouchCt_Financial_X", F.coalesce(F.col("Past3MoTouchCt_Financial"), F.lit(0))) \
    .withColumn("Past12MoTouchCt_Health_X", F.coalesce(F.col("Past12MoTouchCt_Health"), F.lit(0))) \
    .withColumn("PartyAffiliation", F.when(F.col("PartyAffiliation").isin('', ' '), 'BLANK').otherwise(F.col("PartyAffiliation")))

# Transformations (binned variables)
df_binned = df_transformed \
    .withColumn("new_MonthstoExpire_binned4_7", F.when((F.col("new_MonthstoExpire") > 4.43333) & (F.col("new_MonthstoExpire") <= 7.5), 1).otherwise(0)) \
    .withColumn("new_MonthstoExpire_binned7_9", F.when((F.col("new_MonthstoExpire") > 7.5) & (F.col("new_MonthstoExpire") <= 9.53333), 1).otherwise(0)) \
    .withColumn("new_MonthstoExpire_binned9_15", F.when((F.col("new_MonthstoExpire") > 9.53333) & (F.col("new_MonthstoExpire") <= 15.6), 1).otherwise(0)) \
    .withColumn("new_MonthstoExpire_binned15_26", F.when((F.col("new_MonthstoExpire") > 15.6) & (F.col("new_MonthstoExpire") <= 26.73333), 1).otherwise(0)) \
    .withColumn("new_MonthstoExpire_binned26_44", F.when((F.col("new_MonthstoExpire") > 26.73333) & (F.col("new_MonthstoExpire") <= 44.0667), 1).otherwise(0)) \
    .withColumn("new_MonthstoExpire_binned44", F.when(F.col("new_MonthstoExpire") > 44.0667, 1).otherwise(0)) \
    .withColumn("MonthsSinceLastOrder_binned5_14", F.when((F.col("MonthsSinceLastOrder") > 5.6333) & (F.col("MonthsSinceLastOrder") <= 14.2667), 1).otherwise(0)) \
    .withColumn("MonthsSinceLastOrder_binned14_20", F.when((F.col("MonthsSinceLastOrder") > 14.2667) & (F.col("MonthsSinceLastOrder") <= 20.3667), 1).otherwise(0)) \
    .withColumn("MonthsSinceLastOrder_binned20", F.when(F.col("MonthsSinceLastOrder") > 20.3667, 1).otherwise(0)) \
    .withColumn("Acknow_X_binnedGROUP2", F.when(F.col("Acknow_X") == '1', 1).otherwise(0)) \
    .withColumn("Acknow_X_binnedGROUP3", F.when(F.col("Acknow_X") == '2', 1).otherwise(0)) \
    .withColumn("Past3MoTouchCt_Fin_X_binned0_2", F.when((F.col("Past3MoTouchCt_Financial_X") > 0) & (F.col("Past3MoTouchCt_Financial_X") <= 2), 1).otherwise(0)) \
    .withColumn("Past3MoTouchCt_Fin_X_binned2", F.when(F.col("Past3MoTouchCt_Financial_X") > 2, 1).otherwise(0)) \
    .withColumn("NbrTimesSelEmailed_binned0", F.when(F.col("NbrTimesSelEmailedInd") > 0, 1).otherwise(0)) \
    .withColumn("HH_pct_Male_per_LT18_binned9_20", F.when((F.col("HH_pct_Male_HOH_Fam_W_per_LT18") > 9) & (F.col("HH_pct_Male_HOH_Fam_W_per_LT18") <= 20), 1).otherwise(0)) \
    .withColumn("HH_pct_Male_per_LT18_binned20", F.when(F.col("HH_pct_Male_HOH_Fam_W_per_LT18") > 20, 1).otherwise(0)) \
    .withColumn("HH_pct_Male_per_LT18_binnedMISS", F.when(F.col("HH_pct_Male_HOH_Fam_W_per_LT18").isNull(), 1).otherwise(0)) \
    .withColumn("MailOrderDonorSum_binned2_3", F.when((F.col("MailOrderDonorSum") > 2) & (F.col("MailOrderDonorSum") <= 3), 1).otherwise(0)) \
    .withColumn("MailOrderDonorSum_binned3", F.when(F.col("MailOrderDonorSum") > 3, 1).otherwise(0)) \
    .withColumn("PartyAffiliation_binnedGROUP2", F.when(F.col("PartyAffiliation").isin('DEM','DTS','LIB','GRE'), 1).otherwise(0)) \
    .withColumn("pct_Other_Relative_HH_binned14", F.when(F.col("pct_Other_Relative_in_Family_HH") > 14, 1).otherwise(0)) \
    .withColumn("HU_pct_Occupied_binned914_952", F.when((F.col("HU_pct_Occupied") > 914) & (F.col("HU_pct_Occupied") <= 952), 1).otherwise(0)) \
    .withColumn("HU_pct_Occupied_binned952_962", F.when((F.col("HU_pct_Occupied") > 952) & (F.col("HU_pct_Occupied") <= 962), 1).otherwise(0)) \
    .withColumn("HU_pct_Occupied_binned962", F.when(F.col("HU_pct_Occupied") > 962, 1).otherwise(0)) \
    .withColumn("Inc_Med_Age_65_74_binned2_4", F.when((F.col("Inc_HH_Med_Inc_HHer_Age_65_74") > 23528) & (F.col("Inc_HH_Med_Inc_HHer_Age_65_74") <= 44749), 1).otherwise(0)) \
    .withColumn("Inc_Med_Age_65_74_binned4_5", F.when((F.col("Inc_HH_Med_Inc_HHer_Age_65_74") > 44749) & (F.col("Inc_HH_Med_Inc_HHer_Age_65_74") <= 50999), 1).otherwise(0)) \
    .withColumn("Inc_Med_Age_65_74_binned5_6", F.when((F.col("Inc_HH_Med_Inc_HHer_Age_65_74") > 50999) & (F.col("Inc_HH_Med_Inc_HHer_Age_65_74") <= 67499), 1).otherwise(0)) \
    .withColumn("Inc_Med_Age_65_74_binned67499", F.when(F.col("Inc_HH_Med_Inc_HHer_Age_65_74") > 67499, 1).otherwise(0)) \
    .withColumn("MemXRenew_binned0", F.when(F.col("MemXRenew") <= 0, 1).otherwise(0)) \
    .withColumn("MemXRenew_binned0_4", F.when((F.col("MemXRenew") > 0) & (F.col("MemXRenew") <= 4), 1).otherwise(0)) \
    .withColumn("MemXRenew_binned4", F.when(F.col("MemXRenew") > 4, 1).otherwise(0)) \
    .withColumn("Avg_Trav_Time_Work_binned330_370", F.when((F.col("Avg_Trav_Time_to_Work") > 330) & (F.col("Avg_Trav_Time_to_Work") <= 370), 1).otherwise(0)) \
    .withColumn("Avg_Trav_Time_Work_binned370", F.when(F.col("Avg_Trav_Time_to_Work") > 370, 1).otherwise(0)) \
    .withColumn("OOHU_Home_Value_binned75_132", F.when((F.col("OOHU_pct_Home_Value_125_149K") > 75) & (F.col("OOHU_pct_Home_Value_125_149K") <= 132), 1).otherwise(0)) \
    .withColumn("OOHU_Home_Value_binned132_186", F.when((F.col("OOHU_pct_Home_Value_125_149K") > 132) & (F.col("OOHU_pct_Home_Value_125_149K") <= 186), 1).otherwise(0)) \
    .withColumn("OOHU_Home_Value_binned186", F.when(F.col("OOHU_pct_Home_Value_125_149K") > 186, 1).otherwise(0)) \
    .withColumn("HH_NonHispHINatOthPIOnly_binned2", F.when(F.col("HH_pct_HOH_NonHispHINatOthPIOnly") > 2, 1).otherwise(0)) \
    .withColumn("Past12MoTouchCt_Health_binned11", F.when(F.col("Past12MoTouchCt_Health_X") > 11, 1).otherwise(0)) \
    .withColumn("pct_in_Households_binned963_998", F.when((F.col("pct_in_Households") > 963) & (F.col("pct_in_Households") <= 998), 1).otherwise(0)) \
    .withColumn("pct_in_Households_binned998", F.when(F.col("pct_in_Households") > 998, 1).otherwise(0)) \
    .withColumn("Rntl_pct_Cash_550_599_binned150", F.when(F.col("Rntl_pct_Cash_550_599") > 150, 1).otherwise(0))

# Model Equation
df_scored = df_binned.withColumn("pred",
    -6.48408 +
    (F.col("new_MonthstoExpire_binned4_7") * 0.45278) +
    (F.col("new_MonthstoExpire_binned7_9") * 1.39285) +
    (F.col("new_MonthstoExpire_binned9_15") * 1.22922) +
    (F.col("new_MonthstoExpire_binned15_26") * 1.43327) +
    (F.col("new_MonthstoExpire_binned26_44") * 1.41868) +
    (F.col("new_MonthstoExpire_binned44") * 1.61487) +
    (F.col("MonthsSinceLastOrder_binned5_14") * 0.65109) +
    (F.col("MonthsSinceLastOrder_binned14_20") * 0.71139) +
    (F.col("MonthsSinceLastOrder_binned20") * 0.52577) +
    (F.col("Acknow_X_binnedGROUP2") * 2.19829) +
    (F.col("Acknow_X_binnedGROUP3") * -3.67442) +
    (F.col("Past3MoTouchCt_Fin_X_binned0_2") * -0.11274) +
    (F.col("Past3MoTouchCt_Fin_X_binned2") * -0.32020) +
    (F.col("NbrTimesSelEmailed_binned0") * -0.26740) +
    (F.col("HH_pct_Male_per_LT18_binned9_20") * -0.14048) +
    (F.col("HH_pct_Male_per_LT18_binned20") * 0.06014) +
    (F.col("HH_pct_Male_per_LT18_binnedMISS") * 0.80730) +
    (F.col("MailOrderDonorSum_binned2_3") * 0.11250) +
    (F.col("MailOrderDonorSum_binned3") * 0.21005) +
    (F.col("PartyAffiliation_binnedGROUP2") * 0.17347) +
    (F.col("pct_Other_Relative_HH_binned14") * 0.14495) +
    (F.col("HU_pct_Occupied_binned914_952") * 0.17639) +
    (F.col("HU_pct_Occupied_binned952_962") * 0.01949) +
    (F.col("HU_pct_Occupied_binned962") * 0.22427) +
    (F.col("Inc_Med_Age_65_74_binned2_4") * -0.16207) +
    (F.col("Inc_Med_Age_65_74_binned4_5") * -0.30629) +
    (F.col("Inc_Med_Age_65_74_binned5_6") * -0.16164) +
    (F.col("Inc_Med_Age_65_74_binned67499") * -0.35706) +
    (F.col("MemXRenew_binned0_4") * 0.17238) +
    (F.col("MemXRenew_binned4") * 0.25146) +
    (F.col("Avg_Trav_Time_Work_binned330_370") * 0.11174) +
    (F.col("Avg_Trav_Time_Work_binned370") * -0.16862) +
    (F.col("OOHU_Home_Value_binned75_132") * -0.12321) +
    (F.col("OOHU_Home_Value_binned132_186") * 0.08564) +
    (F.col("OOHU_Home_Value_binned186") * -0.13025) +
    (F.col("HH_NonHispHINatOthPIOnly_binned2") * 0.13961) +
    (F.col("Past12MoTouchCt_Health_binned11") * -0.13675) +
    (F.col("pct_in_Households_binned963_998") * 0.11910) +
    (F.col("pct_in_Households_binned998") * 0.02027) +
    (F.col("Rntl_pct_Cash_550_599_binned150") * 0.10235)
)

df_scored = df_scored.withColumn("p_score", F.exp(F.col("pred")) / (1 + F.exp(F.col("pred")))) \
    .withColumn("score", F.round(F.col("p_score"), 6))

# Applying the format using a join with the percentiles table
# Assuming the format table has 'START', 'END', and 'LABEL' columns
df_formatted = df_scored.join(
    df_new_fndnothr_percentiles.alias("fmt"),
    (df_scored.score >= F.col("fmt.START")) & (df_scored.score <= F.col("fmt.END")),
    "left"
).select(df_scored["*"], F.col("fmt.LABEL").alias("fndnothr_score_fmt"))

df_formatted = df_formatted.withColumn("fndnothr_score", F.when(F.col("fndnothr_score_fmt") == 100, 99).otherwise(F.col("fndnothr_score_fmt")))

df_New_fndn_model = df_formatted.select("mid_key", "fndnothr_score")

# PROC SQL with CONNECT TO
sql_query = f"""
    select mid_key, flowchart_run_id
    from {ref2}.F_CONTACT_HISTORY_ANALYTIC A
    join {ref2}.d_campaign_analytic CA
    on A.D_CAMPAIGN_KEY = CA.D_CAMPAIGN_KEY
    where (flowchart_run_id in ({fndncurr}) or flowchart_run_id in ({fndnminus1}) or flowchart_run_id in ({fndnminus2}))
"""
df_campaigns_sent = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", sql_query) \
    .option("user", usern) \
    .option("password", passw) \
    .load()

# PROC SQL to create campaigns_mid
df_campaigns_sent.createOrReplaceTempView("campaigns_sent")
df_campaigns_mid = spark.sql(f"""
    SELECT
        mid_key,
        SUM(CASE WHEN flowchart_run_id IN ({fndnminus2}) THEN 1 ELSE 0 END) as monthminus2,
        SUM(CASE WHEN flowchart_run_id IN ({fndnminus1}) THEN 1 ELSE 0 END) as monthminus1,
        SUM(CASE WHEN flowchart_run_id IN ({fndncurr}) THEN 1 ELSE 0 END) as month0
    FROM campaigns_sent
    GROUP BY mid_key
""")

# DATA fndn_data_prep
df_fndn_data_prep_base = df_New_FNDN_Model_Data.drop(
    "PartyAffiliation", "Past3MoTouchCt_Financial", "Past12MoTouchCt_Health", "Zip", "ZipPlus4",
    "mail_order_donor_categories", "acknow_fndn", "HH_pct_Male_HOH_Fam_W_per_LT18",
    "pct_Other_Relative_in_Family_HH", "HU_pct_Occupied", "Inc_HH_Med_Inc_HHer_Age_65_74",
    "Avg_Trav_Time_to_Work", "OOHU_pct_Home_Value_125_149K", "HH_pct_HOH_NonHispHINatOthPIOnly",
    "pct_in_Households", "Rntl_pct_Cash_550_599"
)

df_fndn_data_prep_transformed = df_fndn_data_prep_base.withColumn("muldate_dt", F.to_date(F.lit(MULDATE), "yyyyMMdd")) \
    .withColumn("mempaiddate_new", F.to_date(F.col("mempaiddate").cast(StringType()), "yyyyMMdd")) \
    .withColumn("MonthstoPaidDate", F.months_between(F.col("mempaiddate_new"), F.col("muldate_dt")))

df_fndn_data_prep = df_fndn_data_prep_transformed.withColumn("MonthstoPaidDate",
    F.when(F.col("MonthstoPaidDate") < 0, 0)
     .when((F.col("MonthstoPaidDate") >= 0) & (F.col("MonthstoPaidDate") <= 121), F.col("MonthstoPaidDate"))
     .when(F.col("MonthstoPaidDate") > 121, 121)
     .otherwise(F.col("MonthstoPaidDate"))
)

# PROC SQL create pml_alldata
df_fndn_data_prep.createOrReplaceTempView("fndn_data_prep")
df_campaigns_mid.createOrReplaceTempView("campaigns_mid")
df_pml_alldata_joined = spark.sql("""
    SELECT a.*, h.monthminus2, h.monthminus1, h.month0
    FROM fndn_data_prep AS a
    LEFT JOIN campaigns_mid AS h
    ON a.mid_key = h.mid_key
""")

# DATA pml_alldata
df_pml_alldata_features = df_pml_alldata_joined \
    .withColumn("Advo_Last_Petition_Dt_Agg_Ind_c", F.coalesce(F.col("Advo_Last_Petition_Dt_Agg_Ind"), F.lit(20119990.37))) \
    .withColumn("chacq_u_dum", F.when(F.col("ch_acq") == 'U', 1).otherwise(0)) \
    .withColumn("community_charity_dum", F.when(F.col("IBX_COMMUNITY_CHARITIES_AGG_HHD") == 1, 1).otherwise(0)) \
    .withColumn("donadv12_c", F.coalesce(F.col("donadv12"), F.lit(0.0153967))) \
    .withColumn("education_3_dum", F.when(F.col("IBX_EDUCATION") == '3', 1).otherwise(0)) \
    .withColumn("female_dum", F.when(F.col("gender_agg_ind") == 'F', 1).otherwise(0)) \
    .withColumn("goi_missing_dum", F.when(F.col("Globally_opted_in") == '', 1).otherwise(0)) \
    .withColumn("homerange_d", F.when(F.col("IBX_HOME_ASSESSED_VALUE_RANGES") == 'D', 1).otherwise(0)) \
    .withColumn("IBX_ADULT_AGE_65_74_AGG_HHD_num", F.when(F.col("IBX_ADULT_AGE_65_74_AGG_HHD") == '', 0).otherwise(F.col("IBX_ADULT_AGE_65_74_AGG_HHD").cast(DoubleType()))) \
    .withColumn("ibx_donation", F.when(F.col("IBX_DONATION_CONTRIBUTION"), 1).otherwise(0)) \
    .withColumn("IBX_INFERRED_HOUSEHOLD_RANK_num", F.when(F.col("IBX_INFERRED_HOUSEHOLD_RANK") == '', 0).otherwise(F.col("IBX_INFERRED_HOUSEHOLD_RANK").cast(DoubleType()))) \
    .withColumn("IBX_MOVIE_MUSIC_GROUPING_num", F.when(F.col("IBX_MOVIE_MUSIC_GROUPING") == '', 0).otherwise(F.col("IBX_MOVIE_MUSIC_GROUPING").cast(DoubleType()))) \
    .withColumn("ibx_race_o", F.when(F.col("IBX_RACE_CD_INPUT_INDIVIDUAL_PRE") == 'O', 1).otherwise(0)) \
    .withColumn("IBX_TOTAL_ONLINE_PURCHASES_num", F.when(F.col("IBX_TOTAL_ONLINE_PURCHASES") == '', 0).otherwise(F.col("IBX_TOTAL_ONLINE_PURCHASES").cast(DoubleType()))) \
    .withColumn("IBX_WEEKS_SINCE_LAST_ONLINE__num", F.when(F.col("IBX_WEEKS_SINCE_LAST_ONLINE_ORDE") == '', 0).otherwise(F.col("IBX_WEEKS_SINCE_LAST_ONLINE_ORDE").cast(DoubleType()))) \
    .withColumn("LapsMail_c", F.coalesce(F.col("LapsMail"), F.lit(0.303591))) \
    .withColumn("lifestage_5", F.when(F.col("LIFE_STAGE") == '5', 1).otherwise(0)) \
    .withColumn("mail_health_dum", F.when(F.col("IBX_MAIL_BUYER_CAT_HEALTH_AGG_HH") == '1', 1).otherwise(0)) \
    .withColumn("member_secondary", F.when(F.col("MEMBER_FL_AGG_IND") == 'S', 1).otherwise(0)) \
    .withColumn("orders_altmedia_c", F.coalesce(F.col("orders_altmedia"), F.lit(1.3500962))) \
    .withColumn("orders_online_c", F.coalesce(F.col("orders_online"), F.lit(1.4392047))) \
    .withColumn("past3touch_0_dum", F.when(F.col("Past3MoTouchCt_Overall") == '', 1).otherwise(0)) \
    .withColumn("pcowner", F.when(F.col("IBX_PC_OWNER_PREMIER") == 'Y', 1).otherwise(0)) \
    .withColumn("retail_a1", F.when(F.col("IBX_RETAIL_PURCHASES_MOST_FREQUE") == 'A1', 1).otherwise(0)) \
    .withColumn("timeleft_3mo_dum", F.when(F.col("MonthstoPaidDate") <= 3, 1).otherwise(0)) \
    .withColumn("timeleft_6moto3mo_dum", F.when((F.col("MonthstoPaidDate") > 3) & (F.col("MonthstoPaidDate") <= 6), 1).otherwise(0)) \
    .withColumn("timeleft_gt12mo_dum", F.when(F.col("MonthstoPaidDate") > 12, 1).otherwise(0)) \
    .withColumn("cadence_past3_dum0", F.when((F.coalesce(F.col("month0"), F.lit(0)) + F.coalesce(F.col("monthminus1"), F.lit(0)) + F.coalesce(F.col("monthminus2"), F.lit(0))) == 0, 1).otherwise(0))

df_pml_alldata = df_pml_alldata_features.withColumn("logit_FNDN_AARPPRO_DM_score",
    -141.9 +
    0.000006829 * F.col("Advo_Last_Petition_Dt_Agg_Ind_c") +
    -0.2007 * F.col("cadence_past3_dum0") +
    -0.3694 * F.col("chacq_u_dum") +
    0.065 * F.col("community_charity_dum") +
    0.8625 * F.col("donadv12_c") +
    -0.1138 * F.col("education_3_dum") +
    -0.126 * F.col("female_dum") +
    0.1779 * F.col("goi_missing_dum") +
    -0.0983 * F.col("homerange_d") +
    -0.174 * F.col("IBX_ADULT_AGE_65_74_AGG_HHD_num") +
    0.3415 * F.col("ibx_donation") +
    0.0339 * F.col("IBX_INFERRED_HOUSEHOLD_RANK_num") +
    0.0582 * F.col("IBX_MOVIE_MUSIC_GROUPING_num") +
    -0.0791 * F.col("ibx_race_o") +
    -0.0126 * F.col("IBX_TOTAL_ONLINE_PURCHASES_num") +
    -0.00076 * F.col("IBX_WEEKS_SINCE_LAST_ONLINE__num") +
    -0.0215 * F.col("LapsMail_c") +
    -0.316 * F.col("lifestage_5") +
    0.1264 * F.col("mail_health_dum") +
    0.9568 * F.col("member_secondary") +
    0.0972 * F.col("orders_altmedia_c") +
    -0.2698 * F.col("orders_online_c") +
    0.1288 * F.col("past3touch_0_dum") +
    -0.1839 * F.col("pcowner") +
    -0.5452 * F.col("retail_a1") +
    -0.7199 * F.col("timeleft_3mo_dum") +
    -0.486 * F.col("timeleft_6moto3mo_dum") +
    0.1544 * F.col("timeleft_gt12mo_dum")
).withColumn("FNDN_AARPPRO_DM_score", F.exp(F.col("logit_FNDN_AARPPRO_DM_score")) / (1 + F.exp(F.col("logit_FNDN_AARPPRO_DM_score"))))

# PROC UNIVARIATE
print("--- Summary statistics for FNDN_AARPPRO_DM_score ---")
df_pml_alldata.select("FNDN_AARPPRO_DM_score").summary().show()

# PROC RANK
df_with_dummy = df_pml_alldata.withColumn("dummy", F.lit(1))
window_spec = Window.partitionBy("dummy").orderBy(F.col("FNDN_AARPPRO_DM_score").desc())
df_centiles = df_with_dummy.withColumn("FNDN_AARPPRO_DM", F.ntile(99).over(window_spec)).drop("dummy")

# PROC SQL to create final table
df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_New_fndn_model.createOrReplaceTempView("New_fndn_model")
df_centiles.createOrReplaceTempView("centiles")

df_geo_appends_rpm_final = spark.sql("""
    SELECT 
        a.*,
        c.fndnothr_score,
        d.FNDN_AARPPRO_DM + 1 AS FNDN_AARPPRO_DM,
        d.FNDN_AARPPRO_DM_score
    FROM geo_appends_rpm AS a
    LEFT JOIN New_fndn_model AS c ON CAST(a.merkleid AS BIGINT) = c.mid_key
    LEFT JOIN centiles AS d ON CAST(a.merkleid AS BIGINT) = d.mid_key
""")

df_geo_appends_rpm_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

# PROC FREQ
print(f"--- Frequency Distribution for Foundation Model Scores: {MULDATE} {runtype} ---")
print("--- AARP Foundation Scoring Diagnostic Report ---")
print(f"--- Data as of {MULDATE} {runtype} ---")

print("\n--- Frequency for fndnothr_score ---")
df_geo_appends_rpm_final.groupBy("fndnothr_score").count().orderBy("fndnothr_score").show(100)

print("\n--- Frequency for FNDN_AARPPRO_DM ---")
df_geo_appends_rpm_final.groupBy("FNDN_AARPPRO_DM").count().orderBy("FNDN_AARPPRO_DM").show(100)
#End-DBShift