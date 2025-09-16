import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType

spark = SparkSession.builder.appName("Foundation_Scores_Translation").getOrCreate()

# Mocked SAS macro variables
MULDATE = "20230101"
conn = "your_connection_alias"
dsn = "your_dsn"
usern = "your_user"
passw = "your_password"
ref2 = "your_db_schema"
fndncurr = "'val1', 'val2'"
fndnminus1 = "'val3', 'val4'"
fndnminus2 = "'val5', 'val6'"
runtype = "monthly"

# PROC FORMAT is handled by reading the format table and joining later.
# We will read this table when it's needed in the data step translation.
# df_format_source = spark.table("scoring.new_fndnothr_percentiles")

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
    FROM geo_appends_rpm a
    LEFT JOIN fndnmodelcontacthistfinal b ON a.mid_key = b.mid_key
""")

# PROC SQL: create table mul_div_temp
df_new_geocodes = spark.table("aarpdata.new_geocodes")
df_mul_demo_cont.createOrReplaceTempView("mul_demo_cont")
df_new_geocodes.createOrReplaceTempView("new_geocodes")

df_mul_div_temp = spark.sql("""
    SELECT a.*, b.geocode AS geocode_new
    FROM mul_demo_cont a
    LEFT JOIN new_geocodes b ON a.zip = b.zip AND a.ZipPlus4 = b.zip4
""")

# PROC SQL: create table New_FNDN_Model_Data
df_geodata = spark.table("aarpdata.geodata")
df_mul_div_temp.createOrReplaceTempView("mul_div_temp")
df_geodata.createOrReplaceTempView("geodata")

df_New_FNDN_Model_Data = spark.sql("""
    SELECT a.*, b.HH_pct_Male_HOH_Fam_W_per_LT18, b.pct_Other_Relative_in_Family_HH , b.HU_pct_Occupied , b.Inc_HH_Med_Inc_HHer_Age_65_74
    , b.Avg_Trav_Time_to_Work , b.OOHU_pct_Home_Value_125_149K , b.HH_pct_HOH_NonHispHINatOthPIOnly , b.pct_in_Households ,
    b.Rntl_pct_Cash_550_599
    FROM mul_div_temp AS a
    LEFT JOIN geodata AS b
    ON a.geocode_new=b.geocode
""")

# PROC SORT NODUPKEY
df_New_FNDN_Model_Data = df_New_FNDN_Model_Data.dropDuplicates(["mid_key"])

# DATA STEP: New_fndn_model
df_temp_new_fndn_model = df_New_FNDN_Model_Data.select(
    "mid_key",
    "memacctnum",
    F.col("PartyAffiliation").alias("nPartyAffiliation"),
    "curr_order_create_dt",
    F.col("Past3MoTouchCt_Financial").alias("nPast3MoTouchCt_Financial"),
    F.col("NbrTimesSelEmailedInd").alias("nNbrTimesSelEmailedInd"),
    "MemXRenew",
    F.col("Past12MoTouchCt_Health").alias("nPast12MoTouchCt_Health"),
    "mempaiddate",
    "HH_pct_Male_HOH_Fam_W_per_LT18",
    "pct_Other_Relative_in_Family_HH",
    "HU_pct_Occupied",
    "Inc_HH_Med_Inc_HHer_Age_65_74",
    "Avg_Trav_Time_to_Work",
    "OOHU_pct_Home_Value_125_149K",
    "HH_pct_HOH_NonHispHINatOthPIOnly",
    "pct_in_Households",
    "Rntl_pct_Cash_550_599",
    F.col("mail_order_donor_categories").alias("nmail_order_donor_categories"),
    F.col("acknow_fndn").alias("nAcknow")
)

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn(
    "Past3MoTouchCt_Financial",
    F.when(F.col("nPast3MoTouchCt_Financial") == "", None).otherwise(F.col("nPast3MoTouchCt_Financial")).cast(DoubleType())
)
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn(
    "NbrTimesSelEmailedInd",
    F.when(F.col("nNbrTimesSelEmailedInd") == "", None).otherwise(F.col("nNbrTimesSelEmailedInd")).cast(DoubleType())
)
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn(
    "Past12MoTouchCt_Health",
    F.when(F.col("nPast12MoTouchCt_Health") == "", None).otherwise(F.col("nPast12MoTouchCt_Health")).cast(DoubleType())
)
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("Acknow", F.regexp_replace(F.col("nAcknow").cast(StringType()), " ", ""))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("PartyAffiliation", F.regexp_replace(F.col("nPartyAffiliation").cast(StringType()), " ", ""))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("mail_order_donor_categories", F.regexp_replace(F.col("nmail_order_donor_categories").cast(StringType()), " ", ""))

def sum_string_digits(s):
    if s is None:
        return None
    return sum(int(digit) for digit in s if digit.isdigit())

sum_string_digits_udf = F.udf(sum_string_digits, IntegerType())

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("MailOrderDonorSum", sum_string_digits_udf(F.col("mail_order_donor_categories")))

muldate_dt = F.to_date(F.lit(MULDATE), "yyyyMMdd")
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("E", muldate_dt)
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn(
    "mempaiddate_str",
    F.concat(F.trim(F.col("mempaiddate")), F.lpad(F.dayofmonth(muldate_dt), 2, '0'))
)
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("A", F.to_date(F.col("mempaiddate_str"), "yyyyMMdd"))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("new_MonthstoExpire", F.months_between(F.col("A"), F.col("E")))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("B", F.to_date(F.col("curr_order_create_dt").cast(StringType()), "yyyyMMdd"))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("MonthsSinceLastOrder", F.months_between(F.col("E"), F.col("B")))

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn(
    "Acknow_X",
    F.when(F.col("Acknow").isNull() | (F.trim(F.col("Acknow")) == "."), "0").otherwise(F.col("Acknow"))
)
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn(
    "Past3MoTouchCt_Financial_X",
    F.coalesce(F.col("Past3MoTouchCt_Financial"), F.lit(0))
)
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn(
    "Past12MoTouchCt_Health_X",
    F.coalesce(F.col("Past12MoTouchCt_Health"), F.lit(0))
)
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn(
    "PartyAffiliation",
    F.when(F.col("PartyAffiliation").isin("", " "), "BLANK").otherwise(F.col("PartyAffiliation"))
)

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("new_MonthstoExpire_binned4_7", F.when((F.col("new_MonthstoExpire") > 4.43333) & (F.col("new_MonthstoExpire") <= 7.5), 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("new_MonthstoExpire_binned7_9", F.when((F.col("new_MonthstoExpire") > 7.5) & (F.col("new_MonthstoExpire") <= 9.53333), 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("new_MonthstoExpire_binned9_15", F.when((F.col("new_MonthstoExpire") > 9.53333) & (F.col("new_MonthstoExpire") <= 15.6), 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("new_MonthstoExpire_binned15_26", F.when((F.col("new_MonthstoExpire") > 15.6) & (F.col("new_MonthstoExpire") <= 26.73333), 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("new_MonthstoExpire_binned26_44", F.when((F.col("new_MonthstoExpire") > 26.73333) & (F.col("new_MonthstoExpire") <= 44.0667), 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("new_MonthstoExpire_binned44", F.when(F.col("new_MonthstoExpire") > 44.0667, 1).otherwise(0))

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("MonthsSinceLastOrder_binned5_14", F.when((F.col("MonthsSinceLastOrder") > 5.6333) & (F.col("MonthsSinceLastOrder") <= 14.2667), 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("MonthsSinceLastOrder_binned14_20", F.when((F.col("MonthsSinceLastOrder") > 14.2667) & (F.col("MonthsSinceLastOrder") <= 20.3667), 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("MonthsSinceLastOrder_binned20", F.when(F.col("MonthsSinceLastOrder") > 20.3667, 1).otherwise(0))

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("Acknow_X_binnedGROUP2", F.when(F.col("Acknow_X") == '1', 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("Acknow_X_binnedGROUP3", F.when(F.col("Acknow_X") == '2', 1).otherwise(0))

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("Past3MoTouchCt_Fin_X_binned0_2", F.when((F.col("Past3MoTouchCt_Financial_X") > 0) & (F.col("Past3MoTouchCt_Financial_X") <= 2), 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("Past3MoTouchCt_Fin_X_binned2", F.when(F.col("Past3MoTouchCt_Financial_X") > 2, 1).otherwise(0))

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("NbrTimesSelEmailed_binned0", F.when(F.col("NbrTimesSelEmailedInd") > 0, 1).otherwise(0))

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("HH_pct_Male_per_LT18_binned9_20", F.when((F.col("HH_pct_Male_HOH_Fam_W_per_LT18") > 9) & (F.col("HH_pct_Male_HOH_Fam_W_per_LT18") <= 20), 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("HH_pct_Male_per_LT18_binned20", F.when(F.col("HH_pct_Male_HOH_Fam_W_per_LT18") > 20, 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("HH_pct_Male_per_LT18_binnedMISS", F.when(F.col("HH_pct_Male_HOH_Fam_W_per_LT18").isNull(), 1).otherwise(0))

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("MailOrderDonorSum_binned2_3", F.when((F.col("MailOrderDonorSum") > 2) & (F.col("MailOrderDonorSum") <= 3), 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("MailOrderDonorSum_binned3", F.when(F.col("MailOrderDonorSum") > 3, 1).otherwise(0))

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("PartyAffiliation_binnedGROUP2", F.when(F.col("PartyAffiliation").isin('DEM','DTS','LIB','GRE'), 1).otherwise(0))

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("pct_Other_Relative_HH_binned14", F.when(F.col("pct_Other_Relative_in_Family_HH") > 14, 1).otherwise(0))

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("HU_pct_Occupied_binned914_952", F.when((F.col("HU_pct_Occupied") > 914) & (F.col("HU_pct_Occupied") <= 952), 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("HU_pct_Occupied_binned952_962", F.when((F.col("HU_pct_Occupied") > 952) & (F.col("HU_pct_Occupied") <= 962), 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("HU_pct_Occupied_binned962", F.when(F.col("HU_pct_Occupied") > 962, 1).otherwise(0))

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("Inc_Med_Age_65_74_binned2_4", F.when((F.col("Inc_HH_Med_Inc_HHer_Age_65_74") > 23528) & (F.col("Inc_HH_Med_Inc_HHer_Age_65_74") <= 44749), 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("Inc_Med_Age_65_74_binned4_5", F.when((F.col("Inc_HH_Med_Inc_HHer_Age_65_74") > 44749) & (F.col("Inc_HH_Med_Inc_HHer_Age_65_74") <= 50999), 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("Inc_Med_Age_65_74_binned5_6", F.when((F.col("Inc_HH_Med_Inc_HHer_Age_65_74") > 50999) & (F.col("Inc_HH_Med_Inc_HHer_Age_65_74") <= 67499), 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("Inc_Med_Age_65_74_binned67499", F.when(F.col("Inc_HH_Med_Inc_HHer_Age_65_74") > 67499, 1).otherwise(0))

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("MemXRenew_binned0", F.when(F.col("MemXRenew") <= 0, 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("MemXRenew_binned0_4", F.when((F.col("MemXRenew") > 0) & (F.col("MemXRenew") <= 4), 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("MemXRenew_binned4", F.when(F.col("MemXRenew") > 4, 1).otherwise(0))

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("Avg_Trav_Time_Work_binned330_370", F.when((F.col("Avg_Trav_Time_to_Work") > 330) & (F.col("Avg_Trav_Time_to_Work") <= 370), 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("Avg_Trav_Time_Work_binned370", F.when(F.col("Avg_Trav_Time_to_Work") > 370, 1).otherwise(0))

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("OOHU_Home_Value_binned75_132", F.when((F.col("OOHU_pct_Home_Value_125_149K") > 75) & (F.col("OOHU_pct_Home_Value_125_149K") <= 132), 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("OOHU_Home_Value_binned132_186", F.when((F.col("OOHU_pct_Home_Value_125_149K") > 132) & (F.col("OOHU_pct_Home_Value_125_149K") <= 186), 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("OOHU_Home_Value_binned186", F.when(F.col("OOHU_pct_Home_Value_125_149K") > 186, 1).otherwise(0))

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("HH_NonHispHINatOthPIOnly_binned2", F.when(F.col("HH_pct_HOH_NonHispHINatOthPIOnly") > 2, 1).otherwise(0))

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("Past12MoTouchCt_Health_binned11", F.when(F.col("Past12MoTouchCt_Health_X") > 11, 1).otherwise(0))

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("pct_in_Households_binned963_998", F.when((F.col("pct_in_Households") > 963) & (F.col("pct_in_Households") <= 998), 1).otherwise(0))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("pct_in_Households_binned998", F.when(F.col("pct_in_Households") > 998, 1).otherwise(0))

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("Rntl_pct_Cash_550_599_binned150", F.when(F.col("Rntl_pct_Cash_550_599") > 150, 1).otherwise(0))

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("pred",
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

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("p_score", F.exp(F.col("pred")) / (1 + F.exp(F.col("pred"))))
df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn("score", F.round(F.col("p_score"), 6))

df_format_source = spark.table("scoring.new_fndnothr_percentiles")
df_format_lookup = df_format_source.filter(F.upper(F.col("FMTNAME")) == "NEW_FNDNOTHR_PERCENTIL").select(
    F.col("START").cast(DoubleType()), F.col("END").cast(DoubleType()), F.col("LABEL")
)

df_temp_new_fndn_model = df_temp_new_fndn_model.join(
    df_format_lookup,
    df_temp_new_fndn_model["score"].between(df_format_lookup["START"], df_format_lookup["END"]),
    "left"
).withColumn("fndnothr_score", F.col("LABEL").cast(IntegerType())).drop("START", "END", "LABEL")

df_temp_new_fndn_model = df_temp_new_fndn_model.withColumn(
    "fndnothr_score",
    F.when(F.col("fndnothr_score") == 100, 99).otherwise(F.col("fndnothr_score"))
)

df_new_fndn_model = df_temp_new_fndn_model.select("mid_key", "fndnothr_score")

# PROC SQL: Database Passthrough
sql_query = f"""
    select mid_key, flowchart_run_id
    from {ref2}.F_CONTACT_HISTORY_ANALYTIC A
    join {ref2}.d_campaign_analytic CA
    on A.D_CAMPAIGN_KEY = CA.D_CAMPAIGN_KEY
    where (flowchart_run_id in ({fndncurr}) or flowchart_run_id in ({fndnminus1}) or flowchart_run_id in ({fndnminus2}))
"""
# jdbc_url would be constructed based on conn, dsn etc.
# df_campaigns_sent = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", sql_query).option("user", usern).option("password", passw).load()
# As we cannot connect, creating a placeholder dataframe
schema_campaigns_sent = "mid_key long, flowchart_run_id string"
df_campaigns_sent = spark.createDataFrame([], schema=schema_campaigns_sent)

# PROC SQL: create campaigns_mid
df_campaigns_sent.createOrReplaceTempView("campaigns_sent")
sql_query_mid = f"""
    SELECT
        mid_key,
        SUM(CASE WHEN flowchart_run_id IN ({fndnminus2}) THEN 1 ELSE 0 END) AS monthminus2,
        SUM(CASE WHEN flowchart_run_id IN ({fndnminus1}) THEN 1 ELSE 0 END) AS monthminus1,
        SUM(CASE WHEN flowchart_run_id IN ({fndncurr}) THEN 1 ELSE 0 END) AS month0
    FROM campaigns_sent
    GROUP BY mid_key
"""
df_campaigns_mid = spark.sql(sql_query_mid)

# DATA STEP: fndn_data_prep
df_fndn_data_prep = df_New_FNDN_Model_Data.drop(
    "PartyAffiliation", "Past3MoTouchCt_Financial", "Past12MoTouchCt_Health", "Zip", "ZipPlus4",
    "mail_order_donor_categories", "acknow_fndn", "HH_pct_Male_HOH_Fam_W_per_LT18", "pct_Other_Relative_in_Family_HH",
    "HU_pct_Occupied", "Inc_HH_Med_Inc_HHer_Age_65_74", "Avg_Trav_Time_to_Work", "OOHU_pct_Home_Value_125_149K",
    "HH_pct_HOH_NonHispHINatOthPIOnly", "pct_in_Households", "Rntl_pct_Cash_550_599"
)

df_fndn_data_prep = df_fndn_data_prep.withColumn("E", muldate_dt)
df_fndn_data_prep = df_fndn_data_prep.withColumn(
    "mempaiddate_str",
    F.concat(F.trim(F.col("mempaiddate")), F.lpad(F.dayofmonth(muldate_dt), 2, '0'))
)
df_fndn_data_prep = df_fndn_data_prep.withColumn("A", F.to_date(F.col("mempaiddate_str"), "yyyyMMdd"))
df_fndn_data_prep = df_fndn_data_prep.withColumn("MonthstoPaidDate", F.months_between(F.col("A"), F.col("E")))

df_fndn_data_prep = df_fndn_data_prep.withColumn(
    "MonthstoPaidDate",
    F.when(F.col("MonthstoPaidDate") < 0, 0)
     .when(F.col("MonthstoPaidDate") > 121, 121)
     .otherwise(F.col("MonthstoPaidDate"))
)

# PROC SQL: create table pml_alldata
df_fndn_data_prep.createOrReplaceTempView("fndn_data_prep")
df_campaigns_mid.createOrReplaceTempView("campaigns_mid")
df_pml_alldata = spark.sql("""
    SELECT 
        a.MonthstoPaidDate, a.IBX_RETAIL_PURCHASES_MOST_FREQUE, a.IBX_PC_OWNER_PREMIER, a.Past3MoTouchCt_Overall, 
        a.MEMBER_FL_AGG_IND, a.LIFE_STAGE, a.LapsMail, a.IBX_WEEKS_SINCE_LAST_ONLINE_ORDE, a.IBX_TOTAL_ONLINE_PURCHASES, 
        a.IBX_RACE_CD_INPUT_INDIVIDUAL_PRE, a.IBX_MOVIE_MUSIC_GROUPING, a.IBX_INFERRED_HOUSEHOLD_RANK, a.IBX_DONATION_CONTRIBUTION,
        a.IBX_ADULT_AGE_65_74_AGG_HHD, a.IBX_HOME_ASSESSED_VALUE_RANGES, a.Globally_opted_in, a.gender_agg_ind, a.IBX_EDUCATION, a.donadv12, 
        a.ch_acq, a.Advo_Last_Petition_Dt_Agg_Ind, a.IBX_MAIL_BUYER_CAT_HEALTH_AGG_HH, a.IBX_COMMUNITY_CHARITIES_AGG_HHD,
        a.orders_altmedia, a.orders_online, 
        a.memacctnum, a.mid_key,
        h.monthminus2, h.monthminus1, h.month0
    FROM fndn_data_prep AS a
    LEFT JOIN campaigns_mid AS h ON a.mid_key = h.mid_key
""")

# DATA STEP: pml_alldata transformations
df_pml_alldata = df_pml_alldata.withColumn("Advo_Last_Petition_Dt_Agg_Ind_c", F.coalesce(F.col("Advo_Last_Petition_Dt_Agg_Ind"), F.lit(20119990.37)))
df_pml_alldata = df_pml_alldata.withColumn("chacq_u_dum", F.when(F.col("ch_acq") == 'U', 1).otherwise(0))
df_pml_alldata = df_pml_alldata.withColumn("community_charity_dum", F.when(F.col("IBX_COMMUNITY_CHARITIES_AGG_HHD") == 1, 1).otherwise(0))
df_pml_alldata = df_pml_alldata.withColumn("donadv12_c", F.coalesce(F.col("donadv12"), F.lit(0.0153967)))
df_pml_alldata = df_pml_alldata.withColumn("education_3_dum", F.when(F.col("IBX_EDUCATION") == '3', 1).otherwise(0))
df_pml_alldata = df_pml_alldata.withColumn("female_dum", F.when(F.col("gender_agg_ind") == 'F', 1).otherwise(0))
df_pml_alldata = df_pml_alldata.withColumn("goi_missing_dum", F.when(F.col("Globally_opted_in") == '', 1).otherwise(0))
df_pml_alldata = df_pml_alldata.withColumn("homerange_d", F.when(F.col("IBX_HOME_ASSESSED_VALUE_RANGES") == 'D', 1).otherwise(0))
df_pml_alldata = df_pml_alldata.withColumn("IBX_ADULT_AGE_65_74_AGG_HHD_num", F.when((F.col("IBX_ADULT_AGE_65_74_AGG_HHD") == '') | F.col("IBX_ADULT_AGE_65_74_AGG_HHD").isNull(), 0).otherwise(F.col("IBX_ADULT_AGE_65_74_AGG_HHD").cast(DoubleType())))
df_pml_alldata = df_pml_alldata.withColumn("ibx_donation", F.when(F.col("IBX_DONATION_CONTRIBUTION"), 1).otherwise(0))
df_pml_alldata = df_pml_alldata.withColumn("IBX_INFERRED_HOUSEHOLD_RANK_num", F.when((F.col("IBX_INFERRED_HOUSEHOLD_RANK") == '') | F.col("IBX_INFERRED_HOUSEHOLD_RANK").isNull(), 0).otherwise(F.col("IBX_INFERRED_HOUSEHOLD_RANK").cast(DoubleType())))
df_pml_alldata = df_pml_alldata.withColumn("IBX_MOVIE_MUSIC_GROUPING_num", F.when((F.col("IBX_MOVIE_MUSIC_GROUPING") == '') | F.col("IBX_MOVIE_MUSIC_GROUPING").isNull(), 0).otherwise(F.col("IBX_MOVIE_MUSIC_GROUPING").cast(DoubleType())))
df_pml_alldata = df_pml_alldata.withColumn("ibx_race_o", F.when(F.col("IBX_RACE_CD_INPUT_INDIVIDUAL_PRE") == 'O', 1).otherwise(0))
df_pml_alldata = df_pml_alldata.withColumn("IBX_TOTAL_ONLINE_PURCHASES_num", F.when((F.col("IBX_TOTAL_ONLINE_PURCHASES") == '') | F.col("IBX_TOTAL_ONLINE_PURCHASES").isNull(), 0).otherwise(F.col("IBX_TOTAL_ONLINE_PURCHASES").cast(DoubleType())))
df_pml_alldata = df_pml_alldata.withColumn("IBX_WEEKS_SINCE_LAST_ONLINE__num", F.when((F.col("IBX_WEEKS_SINCE_LAST_ONLINE_ORDE") == '') | F.col("IBX_WEEKS_SINCE_LAST_ONLINE_ORDE").isNull(), 0).otherwise(F.col("IBX_WEEKS_SINCE_LAST_ONLINE_ORDE").cast(DoubleType())))
df_pml_alldata = df_pml_alldata.withColumn("LapsMail_c", F.coalesce(F.col("LapsMail"), F.lit(0.303591)))
df_pml_alldata = df_pml_alldata.withColumn("lifestage_5", F.when(F.col("LIFE_STAGE") == '5', 1).otherwise(0))
df_pml_alldata = df_pml_alldata.withColumn("mail_health_dum", F.when(F.col("IBX_MAIL_BUYER_CAT_HEALTH_AGG_HH") == '1', 1).otherwise(0))
df_pml_alldata = df_pml_alldata.withColumn("member_secondary", F.when(F.col("MEMBER_FL_AGG_IND") == 'S', 1).otherwise(0))
df_pml_alldata = df_pml_alldata.withColumn("orders_altmedia_c", F.coalesce(F.col("orders_altmedia"), F.lit(1.3500962)))
df_pml_alldata = df_pml_alldata.withColumn("orders_online_c", F.coalesce(F.col("orders_online"), F.lit(1.4392047)))
df_pml_alldata = df_pml_alldata.withColumn("past3touch_0_dum", F.when(F.col("Past3MoTouchCt_Overall") == '', 1).otherwise(0))
df_pml_alldata = df_pml_alldata.withColumn("pcowner", F.when(F.col("IBX_PC_OWNER_PREMIER") == 'Y', 1).otherwise(0))
df_pml_alldata = df_pml_alldata.withColumn("retail_a1", F.when(F.col("IBX_RETAIL_PURCHASES_MOST_FREQUE") == 'A1', 1).otherwise(0))
df_pml_alldata = df_pml_alldata.withColumn("timeleft_3mo_dum", F.when(F.col("MonthstoPaidDate") <= 3, 1).otherwise(0))
df_pml_alldata = df_pml_alldata.withColumn("timeleft_6moto3mo_dum", F.when((F.col("MonthstoPaidDate") > 3) & (F.col("MonthstoPaidDate") <= 6), 1).otherwise(0))
df_pml_alldata = df_pml_alldata.withColumn("timeleft_gt12mo_dum", F.when(F.col("MonthstoPaidDate") > 12, 1).otherwise(0))
df_pml_alldata = df_pml_alldata.withColumn("cadence_past3_dum0", F.when(F.coalesce(F.col("month0"), F.lit(0)) + F.coalesce(F.col("monthminus1"), F.lit(0)) + F.coalesce(F.col("monthminus2"), F.lit(0)) == 0, 1).otherwise(0))

df_pml_alldata = df_pml_alldata.withColumn("logit_FNDN_AARPPRO_DM_score",
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
)
df_pml_alldata = df_pml_alldata.withColumn("FNDN_AARPPRO_DM_score", F.exp(F.col("logit_FNDN_AARPPRO_DM_score")) / (1 + F.exp(F.col("logit_FNDN_AARPPRO_DM_score"))))

# PROC UNIVARIATE
df_pml_alldata.select("FNDN_AARPPRO_DM_score").summary().show()

# PROC RANK
df_pml_alldata_with_dummy = df_pml_alldata.withColumn("dummy", F.lit(1))
window_spec = Window.partitionBy("dummy").orderBy(F.col("FNDN_AARPPRO_DM_score").desc())
df_centiles = df_pml_alldata_with_dummy.withColumn("FNDN_AARPPRO_DM", (F.ntile(99).over(window_spec) - 1))

# PROC SQL: create intermed.geo_appends_rpm
df_geo_appends_rpm_source = spark.table("intermed.geo_appends_rpm")
df_geo_appends_rpm_source.createOrReplaceTempView("geo_appends_rpm_source")
df_new_fndn_model.createOrReplaceTempView("new_fndn_model_source")
df_centiles.createOrReplaceTempView("centiles_source")

df_geo_appends_rpm_final = spark.sql("""
    SELECT
        a.*,
        c.fndnothr_score,
        d.FNDN_AARPPRO_DM + 1 AS FNDN_AARPPRO_DM,
        d.FNDN_AARPPRO_DM_score
    FROM
        geo_appends_rpm_source AS a
    LEFT JOIN
        new_fndn_model_source AS c ON CAST(a.merkleid AS BIGINT) = c.mid_key
    LEFT JOIN
        centiles_source AS d ON CAST(a.merkleid AS BIGINT) = d.mid_key
""")

df_geo_appends_rpm_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

# PROC FREQ
df_final_report = spark.table("intermed.geo_appends_rpm")

print("Frequency distribution for fndnothr_score:")
df_final_report.groupBy("fndnothr_score").count().orderBy("fndnothr_score").show()

print("Frequency distribution for FNDN_AARPPRO_DM:")
df_final_report.groupBy("FNDN_AARPPRO_DM").count().orderBy("FNDN_AARPPRO_DM").show()
#End-DBShift