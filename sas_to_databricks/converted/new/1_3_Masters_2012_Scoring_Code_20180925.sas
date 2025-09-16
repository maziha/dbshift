import pyspark.sql.functions as F
from pyspark.sql.types import *
import math

# Let's define the macro variables as Python variables
# The user should set these values before running the script.
MULDATE = "20240101" # Example date, should be set to the actual run date
runtype = "PROD" # Example runtype

# Step 1: Read the formats table
df_new_masters2012_formats = spark.table("scoring.New_masters2012_VIGINTILES")

# Step 2: Read and prepare the main input data
df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")

# Keep and Rename columns as per the SAS DATA step
df_old_masters = df_geo_appends_rpm.select(
    "merkleid", "ACEV_Num", "Advo_Last_Amt", "Advo_TTD_Amt", "Advo_TTD_Num", "age_agg_ind", "Chase_Num_Active_Particpnts", "Chase_Num_InActive_Particpnts", "CurrentPartCt_Overall",
    "Fndn_Last_Amt", "Fndn_TTD_Amt", "Fndn_TTD_Num", "Foremost_Num_Active_Particpnts", "GE_Num_Active_Particpnts",
    "Gender", "Hartford_Num_Active_Particpnts", "HistPartCt_Overall", "MaritalStatus", "MemXRenew",
    "Advo_Last_Dt", "Fndn_Last_Dt", "MemPaidDate", "curr_order_create_dt", "NYL_Num_Active_Particpnts", "NYL_Num_InActive_Particpnts", "NbrTimesSelEmailedInd",
    "OriginCode", "Overall_Historic_SP_Reltshps", "PartyMix", "Past12MoTouchCt_AARP", "Past12MoTouchCt_Financial",
    "Past12MoTouchCt_Health", "Past12MoTouchCt_Priv", "Past3MoTouchCt_AARP", "Past3MoTouchCt_Financial", "Past3MoTouchCt_Health", "Past3MoTouchCt_Priv",
    "State", "vtm_vol_flag_act", "vtm_num_assignments_act", "VoterCount", "VoterStatus",
    "memorigindate", "Age_HH_pct_with_HHer_55_64", "Age_HH_pct_with_HHer_65_74", "Age_HH_pct_with_HHer_75_84", "Age_HH_pct_with_HHer_85p",
    "CENS_LANG_HH_PERCENT_SPANISH_SPE", "CENS_HOMVAL_HOME_VALUE_CBSA_INDE", "CENS_INC_HH_MEDIAN_HOUSEHOLD_INC", "OCCHU_Median_Length_of_Residence",
    "CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL", "Pop_pct_Asian_Only_Hisp", "Pop_pct_Asian_Only_", "Pop_pct_Black_Only_Hisp", "CENS_ETHNIC_POP_PERCENT_BLACK_ON",
    "IBX_ADULTS_NUM_AGG_HHD", "IBX_EDUCATION", "ibx_home_market_value_premier", "IBX_TELECOM_CELLULAR_AGG_HHD",
    "IBX_TELECOM_INTERNET_AGG_HHD", "IBX_NUM_OF_LINES_OF_CREDIT", "IBX_VEHICLE_TRUCK_MC_RV_AGG_HHD",
    "voter_party_input", "pc_prdct_buyer", "Home_purch_yr", "channel_acquired"
).withColumnRenamed("ACEV_Num", "nACEV_Num") \
 .withColumnRenamed("CurrentPartCt_Overall", "nCurrentPartCt_Overall") \
 .withColumnRenamed("HistPartCt_Overall", "nHistPartCt_Overall") \
 .withColumnRenamed("home_purch_yr", "nhome_purch_yr") \
 .withColumnRenamed("NbrTimesSelEmailedInd", "nNbrTimesSelEmailedInd") \
 .withColumnRenamed("OriginCode", "nOriginCode") \
 .withColumnRenamed("Overall_Historic_SP_Reltshps", "nOverall_Historic_SP_Reltshps") \
 .withColumnRenamed("PartyMix", "nPartyMix") \
 .withColumnRenamed("Past12MoTouchCt_AARP", "nPast12MoTouchCt_AARP") \
 .withColumnRenamed("Past12MoTouchCt_Financial", "nPast12MoTouchCt_Financial") \
 .withColumnRenamed("Past12MoTouchCt_Health", "nPast12MoTouchCt_Health") \
 .withColumnRenamed("Past12MoTouchCt_Priv", "nPast12MoTouchCt_Priv") \
 .withColumnRenamed("Past3MoTouchCt_AARP", "nPast3MoTouchCt_AARP") \
 .withColumnRenamed("Past3MoTouchCt_Financial", "nPast3MoTouchCt_Financial") \
 .withColumnRenamed("Past3MoTouchCt_Health", "nPast3MoTouchCt_Health") \
 .withColumnRenamed("Past3MoTouchCt_Priv", "nPast3MoTouchCt_Priv") \
 .withColumnRenamed("VoterStatus", "nVoterStatus") \
 .withColumnRenamed("IBX_ADULTS_NUM_AGG_HHD", "nIBX_ADULTS_NUM_AGG_HHD") \
 .withColumnRenamed("IBX_TELECOM_INTERNET_AGG_HHD", "ninfb_trend_telecom_internet_use") \
 .withColumnRenamed("IBX_TELECOM_CELLULAR_AGG_HHD", "ninfb_trend_telecom_cellular_use") \
 .withColumnRenamed("IBX_NUM_OF_LINES_OF_CREDIT", "nnumber_of_lines_of_credit") \
 .withColumnRenamed("IBX_VEHICLE_TRUCK_MC_RV_AGG_HHD", "nvehicle_truck_motorcycle_rv") \
 .withColumnRenamed("ibx_home_market_value_premier", "nhome_market_value") \
 .withColumnRenamed("voter_party_input", "nvoter_party_input")

# Start of DATA Step transformations
df_old_masters = df_old_masters.withColumn("memdum_1986", F.when(F.col("memorigindate") <= 19860000, 1).otherwise(0))
df_old_masters = df_old_masters.withColumn("memdum_1991", F.when((F.col("memorigindate") <= 19910000) & (F.col("memorigindate") > 19860000), 1).otherwise(0))
df_old_masters = df_old_masters.withColumn("memdum_2001", F.when((F.col("memorigindate") <= 20010000) & (F.col("memorigindate") > 19910000), 1).otherwise(0))
df_old_masters = df_old_masters.withColumn("new_UHG_model", -0.173 + 0.27837 * F.col("memdum_1986") + 0.19285 * F.col("memdum_1991") + 0.03494 * F.col("memdum_2001") + .00284 * F.col("age_agg_ind") + .00574 * F.col("nACEV_Num"))
df_old_masters = df_old_masters.withColumn("nNo_of_Historic_Particpnts_UHG", F.when(F.col("new_UHG_model") >= 0.27, 1).otherwise(0))

# Type conversions and string manipulations
df_old_masters = df_old_masters.withColumn("ACEV_Num", F.when(F.col("nACEV_Num") == "", None).otherwise(F.col("nACEV_Num")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("CurrentPartCt_Overall", F.when(F.col("nCurrentPartCt_Overall") == "", None).otherwise(F.col("nCurrentPartCt_Overall")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("HistPartCt_Overall", F.when(F.col("nHistPartCt_Overall") == "", None).otherwise(F.col("nHistPartCt_Overall")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("home_purch_yr", F.when(F.col("nhome_purch_yr") == "", None).otherwise(F.col("nhome_purch_yr")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("NbrTimesSelEmailedInd", F.when(F.col("nNbrTimesSelEmailedInd") == "", None).otherwise(F.col("nNbrTimesSelEmailedInd")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("No_of_Historic_Particpnts_UHG", F.when(F.col("nNo_of_Historic_Particpnts_UHG") == "", None).otherwise(F.col("nNo_of_Historic_Particpnts_UHG")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("OriginCode", F.regexp_replace(F.col("nOriginCode").cast(StringType()), " ", ""))
df_old_masters = df_old_masters.withColumn("Overall_Historic_SP_Reltshps", F.when(F.col("nOverall_Historic_SP_Reltshps") == "", None).otherwise(F.col("nOverall_Historic_SP_Reltshps")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("PartyMix", F.col("nPartyMix").cast(StringType()))
df_old_masters = df_old_masters.withColumn("Past12MoTouchCt_AARP", F.when(F.col("nPast12MoTouchCt_AARP") == "", None).otherwise(F.col("nPast12MoTouchCt_AARP")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("Past12MoTouchCt_Financial", F.when(F.col("nPast12MoTouchCt_Financial") == "", None).otherwise(F.col("nPast12MoTouchCt_Financial")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("Past12MoTouchCt_Health", F.when(F.col("nPast12MoTouchCt_Health") == "", None).otherwise(F.col("nPast12MoTouchCt_Health")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("Past12MoTouchCt_Priv", F.when(F.col("nPast12MoTouchCt_Priv") == "", None).otherwise(F.col("nPast12MoTouchCt_Priv")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("Past3MoTouchCt_AARP", F.when(F.col("nPast3MoTouchCt_AARP") == "", None).otherwise(F.col("nPast3MoTouchCt_AARP")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("Past3MoTouchCt_Financial", F.when(F.col("nPast3MoTouchCt_Financial") == "", None).otherwise(F.col("nPast3MoTouchCt_Financial")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("Past3MoTouchCt_Health", F.when(F.col("nPast3MoTouchCt_Health") == "", None).otherwise(F.col("nPast3MoTouchCt_Health")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("Past3MoTouchCt_Priv", F.when(F.col("nPast3MoTouchCt_Priv") == "", None).otherwise(F.col("nPast3MoTouchCt_Priv")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("Voterstatus", F.col("nVoterStatus").cast(StringType()))
df_old_masters = df_old_masters.withColumn("vmis_flag", F.when(F.col("vtm_vol_flag_act") == 'N', '0').when(F.col("vtm_vol_flag_act") == 'Y', '1').otherwise(' '))
df_old_masters = df_old_masters.withColumn("VMIS_Num_Act", F.when(F.col("vtm_num_assignments_act") == "", None).otherwise(F.col("vtm_num_assignments_act")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("IBX_ADULTS_NUM_AGG_HHD", F.when(F.col("nIBX_ADULTS_NUM_AGG_HHD") == "", None).otherwise(F.col("nIBX_ADULTS_NUM_AGG_HHD")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("infb_trend_telecom_internet_user", F.when(F.col("ninfb_trend_telecom_internet_use") == "", None).otherwise(F.col("ninfb_trend_telecom_internet_use")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("infb_trend_telecom_cellular_user", F.when(F.col("ninfb_trend_telecom_cellular_use") == "", None).otherwise(F.col("ninfb_trend_telecom_cellular_use")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("number_of_lines_of_credit", F.when(F.col("nnumber_of_lines_of_credit") == "", None).otherwise(F.col("nnumber_of_lines_of_credit")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("vehicle_truck_motorcycle_rv", F.when(F.col("nvehicle_truck_motorcycle_rv") == "", None).otherwise(F.col("nvehicle_truck_motorcycle_rv")).cast(DoubleType()))
df_old_masters = df_old_masters.withColumn("home_market_value", F.col("nhome_market_value").cast(StringType()))
df_old_masters = df_old_masters.withColumn("voter_party_input", F.col("nvoter_party_input").cast(StringType()))

# Create VMIS_Num_Act_Flag variable
df_old_masters = df_old_masters.withColumn("VMIS_Num_Act_Flag", F.when(F.col("vtm_num_assignments_act").cast(IntegerType()) > 0, '1').otherwise('0'))

# Date calculations
muldate_col = F.to_date(F.lit(MULDATE), "yyyyMMdd")
df_old_masters = df_old_masters.withColumn("mempaiddate_new", F.to_date(F.concat(F.substring(F.col("MemPaidDate").cast("string"), 1, 6), F.lpad(F.dayofmonth(muldate_col), 2, '0')), "yyyyMMdd"))
df_old_masters = df_old_masters.withColumn("MonthstoPaidDate", F.datediff(F.col("mempaiddate_new"), muldate_col) / 30.0)
df_old_masters = df_old_masters.withColumn("MonthsSinceLastOrder", F.datediff(muldate_col, F.to_date(F.col("curr_order_create_dt").cast("string"), "yyyyMMdd")) / 30.0)
df_old_masters = df_old_masters.withColumn("MonthsSinceLastAdvoContrib", F.datediff(muldate_col, F.to_date(F.col("Advo_Last_Dt").cast("string"), "yyyyMMdd")) / 30.0)
df_old_masters = df_old_masters.withColumn("MonthsSinceLastFndnContrib", F.datediff(muldate_col, F.to_date(F.col("Fndn_Last_Dt").cast("string"), "yyyyMMdd")) / 30.0)

# Replace missing character values
df_old_masters = df_old_masters.withColumn("OriginCode", F.when(F.col("OriginCode").isNull(), '').otherwise(F.col("OriginCode")))
df_old_masters = df_old_masters.withColumn("VMIS_Flag", F.when(F.col("vmis_flag").isNull(), '').otherwise(F.col("vmis_flag")))

# Replace blanks with 'BLANK'
char_cols_to_fill = ["Voterstatus", "Gender", "MaritalStatus", "OriginCode", "PartyMix", "State", "channel_acquired", "home_market_value", "voter_party_input", "NYL_Num_Active_Particpnts", "NYL_Num_InActive_Particpnts", "IBX_EDUCATION", "VMIS_Flag", "VMIS_Num_Act_Flag", "Chase_Num_Active_Particpnts", "Chase_Num_InActive_Particpnts", "Foremost_Num_Active_Particpnts", "GE_Num_Active_Particpnts", "Hartford_Num_Active_Particpnts", "pc_prdct_buyer"]
for col_name in char_cols_to_fill:
    df_old_masters = df_old_masters.withColumn(col_name, F.when((F.trim(F.col(col_name)) == '') | F.col(col_name).isNull(), 'BLANK').otherwise(F.col(col_name)))

# Replace missing numeric values with zeroes
num_cols_to_fill_zero = ["ACEV_Num", "Advo_Last_Amt", "Advo_TTD_Amt", "Advo_TTD_Num", "CurrentPartCt_Overall", "Fndn_Last_Amt", "Fndn_TTD_Amt", "Fndn_TTD_Num", "HistPartCt_Overall", "MonthsSinceLastAdvoContrib", "MonthsSinceLastFndnContrib", "Past12MoTouchCt_AARP", "Past12MoTouchCt_Financial", "Past12MoTouchCt_Health", "Past12MoTouchCt_Priv", "Past3MoTouchCt_AARP", "Past3MoTouchCt_Financial", "Past3MoTouchCt_Health", "Past3MoTouchCt_Priv", "number_of_lines_of_credit"]
for col_name in num_cols_to_fill_zero:
    df_old_masters = df_old_masters.withColumn(f"{col_name}_X", F.when(F.col(col_name).isNull(), 0).otherwise(F.col(col_name)))

# Cap values
df_old_masters = df_old_masters.withColumn("MonthstoPaidDate", F.when(F.col("MonthstoPaidDate") < 0, 0).when(F.col("MonthstoPaidDate") > 121, 121).otherwise(F.col("MonthstoPaidDate")))
df_old_masters = df_old_masters.withColumn("MonthsSinceLastOrder", F.when(F.col("MonthsSinceLastOrder") < 0, 0).when(F.col("MonthsSinceLastOrder") > 121, 121).otherwise(F.col("MonthsSinceLastOrder")))
df_old_masters = df_old_masters.withColumn("MonthsSinceLastAdvoContrib", F.when(F.col("MonthsSinceLastAdvoContrib_X") < 0, 0).when(F.col("MonthsSinceLastAdvoContrib_X") > 121, 121).otherwise(F.col("MonthsSinceLastAdvoContrib_X")))
df_old_masters = df_old_masters.withColumn("MonthsSinceLastFndnContrib", F.when(F.col("MonthsSinceLastFndnContrib_X") < 0, 0).when(F.col("MonthsSinceLastFndnContrib_X") > 121, 121).otherwise(F.col("MonthsSinceLastFndnContrib_X")))

# MODEL 1
df_old_masters = df_old_masters.na.fill({
    "ACEV_Num_X": 0.19040788156007, "age_agg_ind": 69.171197303718, "CENS_LANG_HH_PERCENT_SPANISH_SPE": 65.267686476242,
    "HistPartCt_Overall_X": 0.5212601835061, "infb_trend_telecom_cellular_user": 5.6865280642105, "infb_trend_telecom_internet_user": 6.6462850454267,
    "MonthsSinceLastAdvoContrib_X": 7.6036746339351, "MonthsToPaidDate": 31.676732368428, "number_of_lines_of_credit_X": 1.4485073212981,
    "CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL": 205919.20879924, "Overall_Historic_SP_Reltshps": 0.48890359220357, "Past12MoTouchCt_Financial_X": 10.476493355347,
    "Past12MoTouchCt_Health_X": 3.7184886458982, "Past12MoTouchCt_Priv_X": 0.67039272471378, "Past3MoTouchCt_Financial_X": 1.2420223563483,
    "Past3MoTouchCt_Health_X": 0.37962486805424, "CENS_ETHNIC_POP_PERCENT_BLACK_ON": 86.233897387302
})
df_old_masters = df_old_masters.withColumn("ACEV_Num_X_log", F.log(F.col("ACEV_Num_X") + 1))
df_old_masters = df_old_masters.withColumn("Age_normalized", (F.col("age_agg_ind") - 69.154478135827) / 9.9271257966606)
df_old_masters = df_old_masters.withColumn("HH_pct_Spanish_Speaking_log", F.log(F.col("CENS_LANG_HH_PERCENT_SPANISH_SPE") + 1))
df_old_masters = df_old_masters.withColumn("HistPartCt_Overall_X_log", F.log(F.col("HistPartCt_Overall_X") + 1))
df_old_masters = df_old_masters.withColumn("infb_trend_telecom_cellular__nor", (F.col("infb_trend_telecom_cellular_user") - 5.6867752993541) / 2.8312492840162)
df_old_masters = df_old_masters.withColumn("infb_trend_telecom_internet__nor", (F.col("infb_trend_telecom_internet_user") - 6.6484323255784) / 2.5483400494176)
df_old_masters = df_old_masters.withColumn("MonthsSinceLastAdvoContrib_X_log", F.log(F.col("MonthsSinceLastAdvoContrib_X") + 1.3667))
df_old_masters = df_old_masters.withColumn("MonthsToPaidDate_normalized", (F.col("MonthsToPaidDate") - 23.385650165871) / 19.408971946364)
df_old_masters = df_old_masters.withColumn("number_of_lines_of_credit_X_norm", (F.col("number_of_lines_of_credit_X") - 1.4423441991647) / 1.7403056898452)
df_old_masters = df_old_masters.withColumn("OOHU_Median_Home_Value_normalize", (F.col("CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL") - 204959.61692536) / 148180.86709533)
df_old_masters = df_old_masters.withColumn("Overall_Historic_SP_Reltshps_nor", (F.col("Overall_Historic_SP_Reltshps") - 0.48474591910683) / 0.80221371654579)
df_old_masters = df_old_masters.withColumn("Past12MoTouchCt_Financial_X_norm", (F.col("Past12MoTouchCt_Financial_X") - 10.453924174215) / 7.7967360321777)
df_old_masters = df_old_masters.withColumn("Past12MoTouchCt_Health_X_normali", (F.col("Past12MoTouchCt_Health_X") - 3.6997613494603) / 4.2478888779226)
df_old_masters = df_old_masters.withColumn("Past12MoTouchCt_Priv_X_normalize", (F.col("Past12MoTouchCt_Priv_X") - 0.66808591419428) / 0.85229351691637)
df_old_masters = df_old_masters.withColumn("Past3MoTouchCt_Financial_X_norma", (F.col("Past3MoTouchCt_Financial_X") - 1.2336605738461) / 1.2798310951166)
df_old_masters = df_old_masters.withColumn("Past3MoTouchCt_Health_X_normaliz", (F.col("Past3MoTouchCt_Health_X") - 0.3760373162662) / 0.77850928781589)
df_old_masters = df_old_masters.withColumn("Pop_pct_Black_Only__log", F.log(F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON") + 1))
df_old_masters = df_old_masters.withColumn("NYL_Num_Active_Particpnts_binned", F.when(F.col("NYL_Num_Active_Particpnts").isin('0','2'), 'GROUP1').when(F.col("NYL_Num_Active_Particpnts").isin('1','BLANK','3'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("VoterStatus_binned", F.when(F.col("Voterstatus").isin('unmatchedMember','active','dropped'), 'GROUP1').when(F.col("Voterstatus").isin('BLANK','unregistered','multipleAppearances'), 'GROUP2').when(F.col("Voterstatus") == 'inactive', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("pred1", 
    -1.9811936 +
    (F.col("Age_normalized") * 0.1992814) +
    (F.col("infb_trend_telecom_cellular__nor") * 0.0771699) +
    (F.col("Overall_Historic_SP_Reltshps_nor") * 0.1430353) +
    (F.col("Past12MoTouchCt_Priv_X_normalize") * 0.0676579) +
    (F.col("Past3MoTouchCt_Health_X_normaliz") * 0.0750837) +
    (F.when(F.col("VoterStatus_binned") == 'GROUP3', 1).otherwise(0) * 0.3672312) +
    (F.when(F.col("VoterStatus_binned") == 'GROUP2', 1).otherwise(0) * 0.1463402) +
    (F.col("MonthsSinceLastAdvoContrib_X_log") * 0.5418499) +
    (F.pow(F.col("MonthsSinceLastAdvoContrib_X_log"), 2) * -0.2192023) +
    (F.col("HistPartCt_Overall_X_log") * -0.1517417) +
    (F.pow(F.col("Past12MoTouchCt_Financial_X_norm"), 2) * 0.0344664) +
    (F.col("OOHU_Median_Home_Value_normalize") * -0.055091) +
    (F.col("Past12MoTouchCt_Health_X_normali") * -0.0380973) +
    (F.col("HH_pct_Spanish_Speaking_log") * 0.0366854) +
    (F.pow(F.col("Age_normalized"), 4) * -0.0101061) +
    (F.pow(F.col("Age_normalized"), 2) * 0.0865619) +
    (F.col("infb_trend_telecom_internet__nor") * 0.0620481) +
    (F.col("number_of_lines_of_credit_X_norm") * -0.0325748) +
    (F.pow(F.col("Pop_pct_Black_Only__log"), 4) * -8.37E-5) +
    (F.when(F.col("NYL_Num_Active_Particpnts_binned") == 'GROUP2', 1).otherwise(0) * 0.1695201) +
    (F.col("Past3MoTouchCt_Financial_X_norma") * 0.0330935) +
    (F.pow(F.col("MonthsToPaidDate_normalized"), 2) * 0.0197996) +
    (F.col("MonthsToPaidDate_normalized") * -0.0680692) +
    (F.pow(F.col("MonthsToPaidDate_normalized"), 3) * -3.98E-5) +
    (F.col("ACEV_Num_X_log") * -0.1058474) +
    (F.pow(F.col("MonthsSinceLastAdvoContrib_X_log"), 3) * 0.0250932))
df_old_masters = df_old_masters.withColumn("p_score1", F.exp(F.col("pred1")) / (1 + F.exp(F.col("pred1"))))

# MODEL 2
df_old_masters = df_old_masters.na.fill({
    "ACEV_Num_X": 0.19040788156007, "Advo_Last_Amt_X": 2.43321514602, "Advo_TTD_Amt_X": 7.0002165263756,
    "age_agg_ind": 69.171197303718, "CurrentPartCt_Overall_X": 0.27918369556392, "Fndn_TTD_Amt_X": 5.5316098736027,
    "MonthsSinceLastAdvoContrib_X": 7.6036746339351, "MonthsSinceLastOrder": 25.69275392992, "No_of_Historic_Particpnts_UHG": 0.11253549472059,
    "Past3MoTouchCt_Financial_X": 1.2420223563483, "CENS_ETHNIC_POP_PERCENT_BLACK_ON": 86.233897387302, "vehicle_truck_motorcycle_rv": 29.974820739107
})
df_old_masters = df_old_masters.withColumn("ACEV_Num_X_log", F.log(F.col("ACEV_Num_X") + 1))
df_old_masters = df_old_masters.withColumn("Advo_Last_Amt_X_log", F.log(F.col("Advo_Last_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("Advo_TTD_Amt_X_log", F.log(F.col("Advo_TTD_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("CurrentPartCt_Overall_X_normaliz", (F.col("CurrentPartCt_Overall_X") - 0.27542441828931) / 0.60379632322646)
df_old_masters = df_old_masters.withColumn("Fndn_TTD_Amt_X_log", F.log(F.col("Fndn_TTD_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("No_of_Historic_Particpnts_UH_log", F.log(F.col("No_of_Historic_Particpnts_UHG") + 1))
df_old_masters = df_old_masters.withColumn("IBX_EDUCATION_binned", F.when(F.col("IBX_EDUCATION") == '3', 'GROUP1').when(F.col("IBX_EDUCATION").isin('2','1','4','BLANK'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("home_market_value_binned", F.when(F.col("home_market_value").isin('S','R','O','K','P','L','Q','M','I','H','J','N','A'), 'GROUP1').when(F.col("home_market_value").isin('D','F','G','B'), 'GROUP2').when(F.col("home_market_value").isin('C','E','BLANK'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("OriginCode_binned", F.when(F.col("OriginCode").isin('13','16','22','5','9','0','2','BLANK'), 'GROUP1').when(F.col("OriginCode").isin('1','3','6','7','20','12','8'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("PartyMix_binned", F.when(F.col("PartyMix").isin('4','1'), 'GROUP1').when(F.col("PartyMix").isin('3','6','5','0'), 'GROUP2').when(F.col("PartyMix") == 'U', 'GROUP3').when(F.col("PartyMix").isin('2','BLANK','7'), 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("VMIS_Flag_binned", F.when(F.col("VMIS_Flag") == '1', 'GROUP1').when(F.col("VMIS_Flag").isin('0','BLANK'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("VMIS_Num_Act_Flag_binned", F.when(F.col("VMIS_Num_Act_Flag") == '0', 'GROUP1').when(F.col("VMIS_Num_Act_Flag") == '1', 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("voter_party_input_binned", F.when(F.col("voter_party_input").isin('I','R','V'), 'GROUP1').when(F.col("voter_party_input") == 'BLANK', 'GROUP2').when(F.col("voter_party_input") == 'D', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Age_binned", F.when(F.col("age_agg_ind") <= 57, '<=57').when(F.col("age_agg_ind") <= 60, '57:60').when(F.col("age_agg_ind") <= 63, '60:63').when(F.col("age_agg_ind") <= 65, '63:65').when(F.col("age_agg_ind") <= 68, '65:68').when(F.col("age_agg_ind") <= 71, '68:71').when(F.col("age_agg_ind") <= 74, '71:74').when(F.col("age_agg_ind") <= 78, '74:78').when(F.col("age_agg_ind") <= 83, '78:83').otherwise('>83'))
df_old_masters = df_old_masters.withColumn("MonthsSinceLastAdvoContrib_X_bin", F.when(F.col("MonthsSinceLastAdvoContrib_X") <= 0, '<=0').when(F.col("MonthsSinceLastAdvoContrib_X") <= 7.7333, '0:7.7333').when(F.col("MonthsSinceLastAdvoContrib_X") <= 20.34666, '7.7333:20.3467').otherwise('>20.3467'))
df_old_masters = df_old_masters.withColumn("MonthsSinceLastOrder_binned", F.when(F.col("MonthsSinceLastOrder") <= 5.2, '<=5.2').when(F.col("MonthsSinceLastOrder") <= 7.9667, '5.2:7.9667').when(F.col("MonthsSinceLastOrder") <= 10.9667, '7.9667:10.9667').when(F.col("MonthsSinceLastOrder") <= 14.3333, '10.9667:14.3333').when(F.col("MonthsSinceLastOrder") <= 21.0667, '14.3333:21.0667').when(F.col("MonthsSinceLastOrder") <= 27.9, '21.0667:27.9').when(F.col("MonthsSinceLastOrder") <= 36.5, '27.9:36.5').when(F.col("MonthsSinceLastOrder") <= 44.5333, '36.5:44.5333').when(F.col("MonthsSinceLastOrder") <= 53.5, '44.5333:53.5').otherwise('>53.5'))
df_old_masters = df_old_masters.withColumn("Past3MoTouchCt_Financial_X_binne", F.when(F.col("Past3MoTouchCt_Financial_X") <= 0, '<=0').when(F.col("Past3MoTouchCt_Financial_X") <= 1, '0:1').when(F.col("Past3MoTouchCt_Financial_X") <= 2, '1:2').when(F.col("Past3MoTouchCt_Financial_X") <= 3, '2:3').otherwise('>3'))
df_old_masters = df_old_masters.withColumn("Pop_pct_Black_Only__binned", F.when(F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON") <= 2, '<=2').when(F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON") <= 4, '2:4').when(F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON") <= 6, '4:6').when(F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON") <= 10, '6:10').when(F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON") <= 16, '10:16').when(F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON") <= 27, '16:27').when(F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON") <= 46, '27:46').when(F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON") <= 91, '46:91').when(F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON") <= 239, '91:239').otherwise('>239'))
df_old_masters = df_old_masters.withColumn("vehicle_truck_motorcycle_rv_binn", F.when(F.col("vehicle_truck_motorcycle_rv") <= 0, '<=0').when(F.col("vehicle_truck_motorcycle_rv") <= 29.974820739107, '0:29.9748').when(F.col("vehicle_truck_motorcycle_rv") <= 100, '29.9748:100').when(F.col("vehicle_truck_motorcycle_rv") <= 101, '100:101').otherwise('>101'))
df_old_masters = df_old_masters.withColumn("pred2", 
    -0.9595038 +
    (F.when(F.col("Age_binned") == '57:60', 1).otherwise(0) * 0.0803815) + (F.when(F.col("Age_binned") == '60:63', 1).otherwise(0) * 0.1078774) + (F.when(F.col("Age_binned") == '63:65', 1).otherwise(0) * 0.0248527) + (F.when(F.col("Age_binned") == '65:68', 1).otherwise(0) * -0.1129511) + (F.when(F.col("Age_binned") == '68:71', 1).otherwise(0) * -0.1275182) + (F.when(F.col("Age_binned") == '71:74', 1).otherwise(0) * -0.1098926) + (F.when(F.col("Age_binned") == '74:78', 1).otherwise(0) * -0.3476477) + (F.when(F.col("Age_binned") == '78:83', 1).otherwise(0) * -0.3681065) + (F.when(F.col("Age_binned") == '>83', 1).otherwise(0) * -0.5721249) +
    (F.when(F.col("PartyMix_binned") == 'GROUP1', 1).otherwise(0) * -0.0809185) + (F.when(F.col("PartyMix_binned") == 'GROUP4', 1).otherwise(0) * 0.0843425) + (F.when(F.col("PartyMix_binned") == 'GROUP3', 1).otherwise(0) * 0.0097182) +
    (F.when(F.col("MonthsSinceLastOrder_binned") == '5.2:7.9667', 1).otherwise(0) * 0.0069807) + (F.when(F.col("MonthsSinceLastOrder_binned") == '7.9667:10.9667', 1).otherwise(0) * 0.0843926) + (F.when(F.col("MonthsSinceLastOrder_binned") == '10.9667:14.3333', 1).otherwise(0) * -0.0262486) + (F.when(F.col("MonthsSinceLastOrder_binned") == '14.3333:21.0667', 1).otherwise(0) * -0.0161095) + (F.when(F.col("MonthsSinceLastOrder_binned") == '21.0667:27.9', 1).otherwise(0) * -0.1524582) + (F.when(F.col("MonthsSinceLastOrder_binned") == '27.9:36.5', 1).otherwise(0) * -0.1139196) + (F.when(F.col("MonthsSinceLastOrder_binned") == '36.5:44.5333', 1).otherwise(0) * -0.1160795) + (F.when(F.col("MonthsSinceLastOrder_binned") == '44.5333:53.5', 1).otherwise(0) * -0.0966531) + (F.when(F.col("MonthsSinceLastOrder_binned") == '>53.5', 1).otherwise(0) * -0.1966621) +
    (F.when(F.col("home_market_value_binned") == 'GROUP2', 1).otherwise(0) * 0.0534966) + (F.when(F.col("home_market_value_binned") == 'GROUP3', 1).otherwise(0) * 0.1265562) +
    (F.col("No_of_Historic_Particpnts_UH_log") * 0.2198331) + (F.pow(F.col("Fndn_TTD_Amt_X_log"), 3) * -0.0030175) +
    (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '0:7.7333', 1).otherwise(0) * 0.1961472) + (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '7.7333:20.3467', 1).otherwise(0) * 0.2186528) + (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '>20.3467', 1).otherwise(0) * 0.1675804) +
    (F.when(F.col("Pop_pct_Black_Only__binned") == '2:4', 1).otherwise(0) * 0.07698) + (F.when(F.col("Pop_pct_Black_Only__binned") == '4:6', 1).otherwise(0) * 0.0426714) + (F.when(F.col("Pop_pct_Black_Only__binned") == '6:10', 1).otherwise(0) * 0.0344581) + (F.when(F.col("Pop_pct_Black_Only__binned") == '10:16', 1).otherwise(0) * 0.0686341) + (F.when(F.col("Pop_pct_Black_Only__binned") == '16:27', 1).otherwise(0) * 0.0748604) + (F.when(F.col("Pop_pct_Black_Only__binned") == '27:46', 1).otherwise(0) * 0.1239) + (F.when(F.col("Pop_pct_Black_Only__binned") == '46:91', 1).otherwise(0) * 0.1417121) + (F.when(F.col("Pop_pct_Black_Only__binned") == '91:239', 1).otherwise(0) * 0.1551112) + (F.when(F.col("Pop_pct_Black_Only__binned") == '>239', 1).otherwise(0) * 0.2547488) +
    (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP1', 1).otherwise(0) * -0.1016414) + (F.pow(F.col("Advo_Last_Amt_X_log"), 2) * -0.0590733) + (F.col("Advo_TTD_Amt_X_log") * 0.1032375) +
    (F.when(F.col("voter_party_input_binned") == 'GROUP1', 1).otherwise(0) * -0.1097517) + (F.when(F.col("voter_party_input_binned") == 'GROUP2', 1).otherwise(0) * -0.0549847) +
    (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '0:1', 1).otherwise(0) * -0.0010381) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '1:2', 1).otherwise(0) * 0.0936676) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '2:3', 1).otherwise(0) * 0.1991667) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '>3', 1).otherwise(0) * 0.0918454) +
    (F.when(F.col("vehicle_truck_motorcycle_rv_binn") == '0:29.9748', 1).otherwise(0) * 0.1039323) + (F.when(F.col("vehicle_truck_motorcycle_rv_binn") == '29.9748:100', 1).otherwise(0) * -0.1197557) + (F.when(F.col("vehicle_truck_motorcycle_rv_binn") == '100:101', 1).otherwise(0) * 0.0073355) + (F.when(F.col("vehicle_truck_motorcycle_rv_binn") == '>101', 1).otherwise(0) * 0.0017054) +
    (F.col("CurrentPartCt_Overall_X_normaliz") * 0.0332771) + (F.col("ACEV_Num_X_log") * -0.0898511) + (F.when(F.col("OriginCode_binned") == 'GROUP2', 1).otherwise(0) * 0.1486352) +
    (F.when(F.col("VMIS_Num_Act_Flag_binned") == 'GROUP2', 1).otherwise(0) * -1.4072936) + (F.when(F.col("VMIS_Flag_binned") == 'GROUP1', 1).otherwise(0) * 1.245579))
df_old_masters = df_old_masters.withColumn("p_score2", F.exp(F.col("pred2")) / (1 + F.exp(F.col("pred2"))))

# MODEL 3
df_old_masters = df_old_masters.na.fill({
    "ACEV_Num_X": 0.19040788156007, "Advo_TTD_Num_X": 0.46499039164208, "age_agg_ind": 69.171197303718,
    "CurrentPartCt_Overall_X": 0.27918369556392, "Home_purch_yr": 1996.3816060398, "No_of_Historic_Particpnts_UHG": 0.11253549472059,
    "Overall_Historic_SP_Reltshps": 0.48890359220357, "Past12MoTouchCt_Health_X": 3.7184886458982, "Past3MoTouchCt_Health_X": 0.37962486805424,
    "CENS_ETHNIC_POP_PERCENT_BLACK_ON": 86.233897387302, "VoterCount": 8.4994624100571
})
df_old_masters = df_old_masters.withColumn("ACEV_Num_X_log", F.log(F.col("ACEV_Num_X") + 1))
df_old_masters = df_old_masters.withColumn("Advo_TTD_Num_X_log", F.log(F.col("Advo_TTD_Num_X") + 1))
df_old_masters = df_old_masters.withColumn("CurrentPartCt_Overall_X_normaliz", (F.col("CurrentPartCt_Overall_X") - 0.27542441828931) / 0.60379632322646)
df_old_masters = df_old_masters.withColumn("No_of_Historic_Particpnts_UH_log", F.log(F.col("No_of_Historic_Particpnts_UHG") + 1))
df_old_masters = df_old_masters.withColumn("IBX_EDUCATION_binned", F.when(F.col("IBX_EDUCATION").isin('3','4'), 'GROUP1').when(F.col("IBX_EDUCATION").isin('2','1'), 'GROUP2').when(F.col("IBX_EDUCATION") == 'BLANK', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("home_market_value_binned", F.when(F.col("home_market_value").isin('P','O','S','L','N','M','K','R','I'), 'GROUP1').when(F.col("home_market_value").isin('E','G','J','D','C','Q','F','B','A','H'), 'GROUP2').when(F.col("home_market_value") == 'BLANK', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("OriginCode_binned", F.when(F.col("OriginCode").isin('13','16','22','7','0','3'), 'GROUP1').when(F.col("OriginCode").isin('BLANK','1','2','20','9','6','5','12','8'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("State_binned", F.when(F.col("State").isin('MN','SD','ND','NE','IA','MT','AR'), 'GROUP1').when(F.col("State").isin('HI','KS','MI','AK','MO','UT','OR','OH','WI','ID','TN','PA','AL','IL','MA','NH'), 'GROUP2').when(F.col("State").isin('CO','OK','CA','NV','FL','AZ','IN','NC','NY','TX','NM','SC','VA','DC','WA','KY','ME','RI','CT','MD','GA','DE','WV','BLANK'), 'GROUP3').when(F.col("State").isin('WY','LA','VT','NJ','MS'), 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("VMIS_Flag_binned", F.when(F.col("VMIS_Flag") == '0', 'GROUP1').when(F.col("VMIS_Flag") == '1', 'GROUP2').when(F.col("VMIS_Flag") == 'BLANK', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Home_purch_yr_binned", F.when(F.col("Home_purch_yr") <= 1986, '<=1986').when(F.col("Home_purch_yr") <= 1991, '1986:1991').when(F.col("Home_purch_yr") <= 1995, '1991:1995').when(F.col("Home_purch_yr") <= 1996.3815960398, '1995:1996.3816').when(F.col("Home_purch_yr") <= 1996.3816060398, '1996.3816:1996.3816').when(F.col("Home_purch_yr") <= 2000, '1996.3816:2000').when(F.col("Home_purch_yr") <= 2004, '2000:2004').when(F.col("Home_purch_yr") <= 2008, '2004:2008').otherwise('>2008'))
df_old_masters = df_old_masters.withColumn("Overall_Historic_SP_Reltshps_bin", F.when(F.col("Overall_Historic_SP_Reltshps") <= 0, '<=0').when(F.col("Overall_Historic_SP_Reltshps") <= 1, '0:1').when(F.col("Overall_Historic_SP_Reltshps") <= 2, '1:2').otherwise('>2'))
df_old_masters = df_old_masters.withColumn("Past12MoTouchCt_Health_X_binned", F.when(F.col("Past12MoTouchCt_Health_X") <= 0, '<=0').when(F.col("Past12MoTouchCt_Health_X") <= 1, '0:1').when(F.col("Past12MoTouchCt_Health_X") <= 2, '1:2').when(F.col("Past12MoTouchCt_Health_X") <= 4, '2:4').when(F.col("Past12MoTouchCt_Health_X") <= 6, '4:6').when(F.col("Past12MoTouchCt_Health_X") <= 7, '6:7').when(F.col("Past12MoTouchCt_Health_X") <= 10, '7:10').otherwise('>10'))
df_old_masters = df_old_masters.withColumn("Past3MoTouchCt_Health_X_binned", F.when(F.col("Past3MoTouchCt_Health_X") <= 0, '<=0').when(F.col("Past3MoTouchCt_Health_X") <= 1, '0:1').when(F.col("Past3MoTouchCt_Health_X") <= 2, '1:2').otherwise('>2'))
df_old_masters = df_old_masters.withColumn("VoterCount_binned", F.when(F.col("VoterCount") <= 0, '<=0').when(F.col("VoterCount") <= 1, '0:1').when(F.col("VoterCount") <= 3, '1:3').when(F.col("VoterCount") <= 5, '3:5').when(F.col("VoterCount") <= 8, '5:8').when(F.col("VoterCount") <= 12, '8:12').when(F.col("VoterCount") <= 16, '12:16').when(F.col("VoterCount") <= 23, '16:23').otherwise('>23'))
df_old_masters = df_old_masters.withColumn("pred3", 
    -1.7275204 +
    (F.when(F.col("State_binned") == 'GROUP1', 1).otherwise(0) * -0.3828717) + (F.when(F.col("State_binned") == 'GROUP3', 1).otherwise(0) * 0.2624253) + (F.when(F.col("State_binned") == 'GROUP4', 1).otherwise(0) * 0.360171) +
    (F.when(F.col("VoterCount_binned") == '0:1', 1).otherwise(0) * -0.0120357) + (F.when(F.col("VoterCount_binned") == '1:3', 1).otherwise(0) * -0.1206242) + (F.when(F.col("VoterCount_binned") == '3:5', 1).otherwise(0) * -0.1369562) + (F.when(F.col("VoterCount_binned") == '5:8', 1).otherwise(0) * -0.1602619) + (F.when(F.col("VoterCount_binned") == '8:12', 1).otherwise(0) * -0.3180332) + (F.when(F.col("VoterCount_binned") == '12:16', 1).otherwise(0) * -0.365567) + (F.when(F.col("VoterCount_binned") == '16:23', 1).otherwise(0) * -0.2963233) + (F.when(F.col("VoterCount_binned") == '>23', 1).otherwise(0) * -0.4072972) +
    (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '0:1', 1).otherwise(0) * -0.2274241) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '1:2', 1).otherwise(0) * -0.2847312) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '2:4', 1).otherwise(0) * -0.3945845) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '4:6', 1).otherwise(0) * -0.5643841) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '6:7', 1).otherwise(0) * -0.5548656) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '7:10', 1).otherwise(0) * -0.6012042) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '>10', 1).otherwise(0) * -0.9501645) +
    (F.when(F.col("Past3MoTouchCt_Health_X_binned") == '0:1', 1).otherwise(0) * 0.2360563) + (F.when(F.col("Past3MoTouchCt_Health_X_binned") == '1:2', 1).otherwise(0) * 0.4192192) + (F.when(F.col("Past3MoTouchCt_Health_X_binned") == '>2', 1).otherwise(0) * 0.4798688) +
    (F.when(F.col("Pop_pct_Black_Only__binned") == '2:4', 1).otherwise(0) * 0.023463) + (F.when(F.col("Pop_pct_Black_Only__binned") == '4:6', 1).otherwise(0) * -0.0395877) + (F.when(F.col("Pop_pct_Black_Only__binned") == '6:10', 1).otherwise(0) * 0.0464498) + (F.when(F.col("Pop_pct_Black_Only__binned") == '10:16', 1).otherwise(0) * 0.0841376) + (F.when(F.col("Pop_pct_Black_Only__binned") == '16:27', 1).otherwise(0) * -0.0955677) + (F.when(F.col("Pop_pct_Black_Only__binned") == '27:46', 1).otherwise(0) * 0.0623886) + (F.when(F.col("Pop_pct_Black_Only__binned") == '46:91', 1).otherwise(0) * 0.0996247) + (F.when(F.col("Pop_pct_Black_Only__binned") == '91:239', 1).otherwise(0) * 0.1642817) + (F.when(F.col("Pop_pct_Black_Only__binned") == '>239', 1).otherwise(0) * 0.3680339) +
    (F.when(F.col("Overall_Historic_SP_Reltshps_bin") == '0:1', 1).otherwise(0) * 0.0314069) + (F.when(F.col("Overall_Historic_SP_Reltshps_bin") == '1:2', 1).otherwise(0) * 0.2694009) + (F.when(F.col("Overall_Historic_SP_Reltshps_bin") == '>2', 1).otherwise(0) * 0.2244553) +
    (F.when(F.col("Age_binned") == '57:60', 1).otherwise(0) * 0.0113872) + (F.when(F.col("Age_binned") == '60:63', 1).otherwise(0) * -0.0114796) + (F.when(F.col("Age_binned") == '63:65', 1).otherwise(0) * 0.2006286) + (F.when(F.col("Age_binned") == '65:68', 1).otherwise(0) * 0.3263485) + (F.when(F.col("Age_binned") == '68:71', 1).otherwise(0) * 0.200642) + (F.when(F.col("Age_binned") == '71:74', 1).otherwise(0) * 0.064117) + (F.when(F.col("Age_binned") == '74:78', 1).otherwise(0) * -0.1080165) + (F.when(F.col("Age_binned") == '78:83', 1).otherwise(0) * -0.1176143) + (F.when(F.col("Age_binned") == '>83', 1).otherwise(0) * -0.2799934) +
    (F.col("ACEV_Num_X_log") * -0.2683002) + (F.col("No_of_Historic_Particpnts_UH_log") * 0.3413873) +
    (F.when(F.col("Home_purch_yr_binned") == '1986:1991', 1).otherwise(0) * 0.033921) + (F.when(F.col("Home_purch_yr_binned") == '1991:1995', 1).otherwise(0) * 0.0638099) + (F.when(F.col("Home_purch_yr_binned") == '1995:1996.3816', 1).otherwise(0) * 0.121285) + (F.when(F.col("Home_purch_yr_binned") == '1996.3816:1996.3816', 1).otherwise(0) * 0.2136074) + (F.when(F.col("Home_purch_yr_binned") == '1996.3816:2000', 1).otherwise(0) * 0.0701595) + (F.when(F.col("Home_purch_yr_binned") == '2000:2004', 1).otherwise(0) * 0.0683835) + (F.when(F.col("Home_purch_yr_binned") == '2004:2008', 1).otherwise(0) * -0.0464326) + (F.when(F.col("Home_purch_yr_binned") == '>2008', 1).otherwise(0) * 0.223042) +
    (F.col("CurrentPartCt_Overall_X_normaliz") * 0.0938408) + (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP1', 1).otherwise(0) * -0.1376604) + (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP3', 1).otherwise(0) * 0.0442019) +
    (F.col("Advo_TTD_Num_X_log") * 0.0851027) + (F.when(F.col("VMIS_Flag_binned") == 'GROUP2', 1).otherwise(0) * -0.492002) + (F.when(F.col("VMIS_Flag_binned") == 'GROUP3', 1).otherwise(0) * -0.5953255) +
    (F.when(F.col("OriginCode_binned") == 'GROUP2', 1).otherwise(0) * 0.252799) + (F.when(F.col("home_market_value_binned") == 'GROUP1', 1).otherwise(0) * -0.1030898) + (F.when(F.col("home_market_value_binned") == 'GROUP3', 1).otherwise(0) * 0.052097) +
    (F.pow(F.col("CurrentPartCt_Overall_X_normaliz"), 2) * -0.016362))
df_old_masters = df_old_masters.withColumn("p_score3", F.exp(F.col("pred3")) / (1 + F.exp(F.col("pred3"))))

# MODEL 6
df_old_masters = df_old_masters.na.fill({
    "ACEV_Num_X": 0.19040788156007, "age_agg_ind": 69.171197303718, "CurrentPartCt_Overall_X": 0.27918369556392,
    "infb_trend_telecom_internet_user": 6.6462850454267, "NbrTimesSelEmailedInd": 29.982907396686, "Past3MoTouchCt_Financial_X": 1.2420223563483,
    "CENS_ETHNIC_POP_PERCENT_BLACK_ON": 86.233897387302
})
df_old_masters = df_old_masters.withColumn("ACEV_Num_X_log", F.log(F.col("ACEV_Num_X") + 1))
df_old_masters = df_old_masters.withColumn("CurrentPartCt_Overall_X_normaliz", (F.col("CurrentPartCt_Overall_X") - 0.27542441828931) / 0.60379632322646)
df_old_masters = df_old_masters.withColumn("home_market_value_binned", F.when(F.col("home_market_value").isin('S','K','R','O','I','A','Q','F','P','M','J','L'), 'GROUP1').when(F.col("home_market_value").isin('C','N','B','D','E','G','H'), 'GROUP2').when(F.col("home_market_value") == 'BLANK', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("MaritalStatus_binned", F.when(F.col("MaritalStatus").isin('W','M'), 'GROUP1').when(F.col("MaritalStatus").isin('S','I','B','BLANK'), 'GROUP2').when(F.col("MaritalStatus").isin('U','D','A'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("OriginCode_binned", F.when(F.col("OriginCode").isin('12','13','16','7','5','0'), 'GROUP1').when(F.col("OriginCode").isin('BLANK','3','9','1','20','2','6','22','8'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Past3MoTouchCt_Priv_X_binned", F.when(F.col("Past12MoTouchCt_Priv_X") == '0', 'GROUP1').when(F.col("Past12MoTouchCt_Priv_X") == '1', 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("State_binned", F.when(F.col("State").isin('KS','ID','OR','MN','UT','MI','WV','NH','NE','AR','DE','HI','FL','AZ','KY','RI','IA','VT','WI','ME','MT'), 'GROUP1').when(F.col("State").isin('CA','CO','AL','TX','WA','WY','OK','IN','MO','NV','OH','DC','ND','CT','AK','SD','MA','NY','NJ','NM','VA','SC','GA','NC','MD','TN','PA','BLANK'), 'GROUP2').when(F.col("State").isin('IL','LA'), 'GROUP3').when(F.col("State") == 'MS', 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("VMIS_Num_Act_Flag_binned", F.when(F.col("VMIS_Num_Act_Flag") == '0', 'GROUP1').when(F.col("VMIS_Num_Act_Flag") == '1', 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("infb_trend_telecom_internet__bin", F.when(F.col("infb_trend_telecom_internet_user") <= 3, '<=3').when(F.col("infb_trend_telecom_internet_user") <= 4, '3:4').when(F.col("infb_trend_telecom_internet_user") <= 5, '4:5').when(F.col("infb_trend_telecom_internet_user") <= 6, '5:6').when(F.col("infb_trend_telecom_internet_user") <= 7, '6:7').when(F.col("infb_trend_telecom_internet_user") <= 8, '7:8').when(F.col("infb_trend_telecom_internet_user") <= 9, '8:9').when(F.col("infb_trend_telecom_internet_user") <= 10, '9:10').otherwise('>10'))
df_old_masters = df_old_masters.withColumn("NbrTimesSelEmailedInd_binned", F.when(F.col("NbrTimesSelEmailedInd") <= 0, '<=0').when(F.col("NbrTimesSelEmailedInd") <= 1, '0:1').when(F.col("NbrTimesSelEmailedInd") <= 10, '1:10').when(F.col("NbrTimesSelEmailedInd") <= 40, '10:40').when(F.col("NbrTimesSelEmailedInd") <= 139, '40:139').otherwise('>139'))
df_old_masters = df_old_masters.withColumn("pred6", 
    -0.370576 +
    (F.when(F.col("Age_binned") == '57:60', 1).otherwise(0) * -0.2182036) + (F.when(F.col("Age_binned") == '60:63', 1).otherwise(0) * -0.5758955) + (F.when(F.col("Age_binned") == '63:65', 1).otherwise(0) * -0.8041636) + (F.when(F.col("Age_binned") == '65:68', 1).otherwise(0) * -1.2445665) + (F.when(F.col("Age_binned") == '68:71', 1).otherwise(0) * -1.4319844) + (F.when(F.col("Age_binned") == '71:74', 1).otherwise(0) * -1.6119254) + (F.when(F.col("Age_binned") == '74:78', 1).otherwise(0) * -1.9407308) + (F.when(F.col("Age_binned") == '78:83', 1).otherwise(0) * -2.0584909) + (F.when(F.col("Age_binned") == '>83', 1).otherwise(0) * -2.1731451) +
    (F.when(F.col("State_binned") == 'GROUP1', 1).otherwise(0) * -0.1340537) + (F.when(F.col("State_binned") == 'GROUP3', 1).otherwise(0) * 0.1337757) + (F.when(F.col("State_binned") == 'GROUP4', 1).otherwise(0) * 0.246268) +
    (F.when(F.col("OriginCode_binned") == 'GROUP2', 1).otherwise(0) * 0.3303544) +
    (F.when(F.col("Pop_pct_Black_Only__binned") == '2:4', 1).otherwise(0) * 0.0334578) + (F.when(F.col("Pop_pct_Black_Only__binned") == '4:6', 1).otherwise(0) * -0.0767479) + (F.when(F.col("Pop_pct_Black_Only__binned") == '6:10', 1).otherwise(0) * 0.0088886) + (F.when(F.col("Pop_pct_Black_Only__binned") == '10:16', 1).otherwise(0) * -0.0045605) + (F.when(F.col("Pop_pct_Black_Only__binned") == '16:27', 1).otherwise(0) * -0.0021196) + (F.when(F.col("Pop_pct_Black_Only__binned") == '27:46', 1).otherwise(0) * 0.0614115) + (F.when(F.col("Pop_pct_Black_Only__binned") == '46:91', 1).otherwise(0) * 0.063531) + (F.when(F.col("Pop_pct_Black_Only__binned") == '91:239', 1).otherwise(0) * 0.0756657) + (F.when(F.col("Pop_pct_Black_Only__binned") == '>239', 1).otherwise(0) * 0.2936015) +
    (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '0:1', 1).otherwise(0) * 0.0794921) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '1:2', 1).otherwise(0) * 0.1453445) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '2:3', 1).otherwise(0) * 0.2650515) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '>3', 1).otherwise(0) * 0.1772538) +
    (F.when(F.col("home_market_value_binned") == 'GROUP2', 1).otherwise(0) * 0.0914699) + (F.when(F.col("home_market_value_binned") == 'GROUP3', 1).otherwise(0) * 0.1848819) +
    (F.when(F.col("infb_trend_telecom_internet__bin") == '3:4', 1).otherwise(0) * -0.0521437) + (F.when(F.col("infb_trend_telecom_internet__bin") == '4:5', 1).otherwise(0) * -0.0959584) + (F.when(F.col("infb_trend_telecom_internet__bin") == '5:6', 1).otherwise(0) * -0.0241745) + (F.when(F.col("infb_trend_telecom_internet__bin") == '6:7', 1).otherwise(0) * -0.1282906) + (F.when(F.col("infb_trend_telecom_internet__bin") == '7:8', 1).otherwise(0) * -0.0475076) + (F.when(F.col("infb_trend_telecom_internet__bin") == '8:9', 1).otherwise(0) * -0.1530242) + (F.when(F.col("infb_trend_telecom_internet__bin") == '9:10', 1).otherwise(0) * -0.2757026) +
    (F.when(F.col("MaritalStatus_binned") == 'GROUP2', 1).otherwise(0) * -0.0039571) + (F.when(F.col("MaritalStatus_binned") == 'GROUP1', 1).otherwise(0) * -0.108712) +
    (F.when(F.col("VMIS_Num_Act_Flag_binned") == 'GROUP2', 1).otherwise(0) * -0.4609781) +
    (F.when(F.col("NbrTimesSelEmailedInd_binned") == '0:1', 1).otherwise(0) * -0.0435196) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '1:10', 1).otherwise(0) * 0.0840493) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '10:40', 1).otherwise(0) * 0.1336167) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '40:139', 1).otherwise(0) * 0.0932083) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '>139', 1).otherwise(0) * 0.1020572) +
    (F.pow(F.col("CurrentPartCt_Overall_X_normaliz"), 2) * -0.0260748) + (F.col("CurrentPartCt_Overall_X_normaliz") * 0.060219) + (F.col("ACEV_Num_X_log") * -0.1143661) +
    (F.when(F.col("Past3MoTouchCt_Priv_X_binned") == 'GROUP2', 1).otherwise(0) * -0.4432301))
df_old_masters = df_old_masters.withColumn("p_score6", F.exp(F.col("pred6")) / (1 + F.exp(F.col("pred6"))))

# MODEL 8
df_old_masters = df_old_masters.na.fill({
    "ACEV_Num_X": 0.19040788156007, "age_agg_ind": 69.171197303718, "Age_HH_pct_with_HHer_65_74": 138.22547718966,
    "MonthsSinceLastFndnContrib_X": 2.8837758031775, "MonthsSinceLastOrder": 25.69275392992, "number_of_lines_of_credit_X": 1.4485073212981,
    "Overall_Historic_SP_Reltshps": 0.48890359220357, "Past12MoTouchCt_Financial_X": 10.476493355347, "CENS_ETHNIC_POP_PERCENT_BLACK_ON": 86.233897387302
})
df_old_masters = df_old_masters.withColumn("ACEV_Num_X_log", F.log(F.col("ACEV_Num_X") + 1))
df_old_masters = df_old_masters.withColumn("MonthsSinceLastFndnContrib_X_log", F.log(F.col("MonthsSinceLastFndnContrib_X") + 1.3667))
df_old_masters = df_old_masters.withColumn("IBX_EDUCATION_binned", F.when(F.col("IBX_EDUCATION") == '3', 'GROUP1').when(F.col("IBX_EDUCATION").isin('1','2','4'), 'GROUP2').when(F.col("IBX_EDUCATION") == 'BLANK', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Gender_binned", F.when(F.col("Gender") == 'M', 'GROUP1').when(F.col("Gender").isin('F','BLANK'), 'GROUP2').when(F.col("Gender") == 'U', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("home_market_value_binned", F.when(F.col("home_market_value").isin('S','L','O','Q','J','P','R'), 'GROUP1').when(F.col("home_market_value").isin('I','M','K','A','N','G','F','H'), 'GROUP2').when(F.col("home_market_value").isin('D','E','C','B'), 'GROUP3').when(F.col("home_market_value") == 'BLANK', 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("NYL_Num_Active_Particpnts_binned", F.when(F.col("NYL_Num_Active_Particpnts") == '0', 'GROUP1').when(F.col("NYL_Num_Active_Particpnts") == 'BLANK', 'GROUP2').when(F.col("NYL_Num_Active_Particpnts").isin('1','2','3'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("OriginCode_binned", F.when(F.col("OriginCode").isin('12','13','7','5','22','0'), 'GROUP1').when(F.col("OriginCode").isin('BLANK','2','16','6','3'), 'GROUP2').when(F.col("OriginCode").isin('1','20','9','8'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("PartyMix_binned", F.when(F.col("PartyMix").isin('1','7'), 'GROUP1').when(F.col("PartyMix").isin('3','5','4'), 'GROUP2').when(F.col("PartyMix").isin('0','2','6'), 'GROUP3').when(F.col("PartyMix").isin('U','BLANK'), 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("VoterStatus_binned", F.when(F.col("Voterstatus").isin('active','multipleAppearances'), 'GROUP1').when(F.col("Voterstatus").isin('unregistered','dropped','BLANK','inactive','unmatchedMember'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("voter_party_input_binned", F.when(F.col("voter_party_input") == 'R', 'GROUP1').when(F.col("voter_party_input").isin('V','I'), 'GROUP2').when(F.col("voter_party_input") == 'BLANK', 'GROUP3').when(F.col("voter_party_input") == 'D', 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Age_HH_pct_with_HHer_65_74_binne", F.when(F.col("Age_HH_pct_with_HHer_65_74") <= 80, '<=80').when(F.col("Age_HH_pct_with_HHer_65_74") <= 98, '80:98').when(F.col("Age_HH_pct_with_HHer_65_74") <= 112, '98:112').when(F.col("Age_HH_pct_with_HHer_65_74") <= 123, '112:123').when(F.col("Age_HH_pct_with_HHer_65_74") <= 133, '123:133').when(F.col("Age_HH_pct_with_HHer_65_74") <= 144, '133:144').when(F.col("Age_HH_pct_with_HHer_65_74") <= 156, '144:156').when(F.col("Age_HH_pct_with_HHer_65_74") <= 170, '156:170').when(F.col("Age_HH_pct_with_HHer_65_74") <= 196, '170:196').otherwise('>196'))
df_old_masters = df_old_masters.withColumn("number_of_lines_of_credit_X_binn", F.when(F.col("number_of_lines_of_credit_X") <= 0, '<=0').when(F.col("number_of_lines_of_credit_X") <= 1, '0:1').when(F.col("number_of_lines_of_credit_X") <= 2, '1:2').when(F.col("number_of_lines_of_credit_X") <= 4, '2:4').otherwise('>4'))
df_old_masters = df_old_masters.withColumn("Past12MoTouchCt_Financial_X_binn", F.when(F.col("Past12MoTouchCt_Financial_X") <= 0, '<=0').when(F.col("Past12MoTouchCt_Financial_X") <= 2, '0:2').when(F.col("Past12MoTouchCt_Financial_X") <= 5, '2:5').when(F.col("Past12MoTouchCt_Financial_X") <= 8, '5:8').when(F.col("Past12MoTouchCt_Financial_X") <= 10, '8:10').when(F.col("Past12MoTouchCt_Financial_X") <= 12, '10:12').when(F.col("Past12MoTouchCt_Financial_X") <= 15, '12:15').when(F.col("Past12MoTouchCt_Financial_X") <= 17, '15:17').when(F.col("Past12MoTouchCt_Financial_X") <= 21, '17:21').otherwise('>21'))
df_old_masters = df_old_masters.withColumn("pred8", 
    -0.0189932 +
    (F.when(F.col("Age_binned") == '57:60', 1).otherwise(0) * -0.182467) + (F.when(F.col("Age_binned") == '60:63', 1).otherwise(0) * -0.4749108) + (F.when(F.col("Age_binned") == '63:65', 1).otherwise(0) * -0.7450024) + (F.when(F.col("Age_binned") == '65:68', 1).otherwise(0) * -0.9584629) + (F.when(F.col("Age_binned") == '68:71', 1).otherwise(0) * -1.2242573) + (F.when(F.col("Age_binned") == '71:74', 1).otherwise(0) * -1.3101676) + (F.when(F.col("Age_binned") == '74:78', 1).otherwise(0) * -1.4779776) + (F.when(F.col("Age_binned") == '78:83', 1).otherwise(0) * -1.7520216) + (F.when(F.col("Age_binned") == '>83', 1).otherwise(0) * -1.9814246) +
    (F.when(F.col("Pop_pct_Black_Only__binned") == '2:4', 1).otherwise(0) * 0.010307) + (F.when(F.col("Pop_pct_Black_Only__binned") == '4:6', 1).otherwise(0) * 0.0762153) + (F.when(F.col("Pop_pct_Black_Only__binned") == '6:10', 1).otherwise(0) * 0.1116906) + (F.when(F.col("Pop_pct_Black_Only__binned") == '10:16', 1).otherwise(0) * 0.1412189) + (F.when(F.col("Pop_pct_Black_Only__binned") == '16:27', 1).otherwise(0) * 0.0880692) + (F.when(F.col("Pop_pct_Black_Only__binned") == '27:46', 1).otherwise(0) * 0.1607306) + (F.when(F.col("Pop_pct_Black_Only__binned") == '46:91', 1).otherwise(0) * 0.1944005) + (F.when(F.col("Pop_pct_Black_Only__binned") == '91:239', 1).otherwise(0) * 0.266389) + (F.when(F.col("Pop_pct_Black_Only__binned") == '>239', 1).otherwise(0) * 0.5115414) +
    (F.when(F.col("Gender_binned") == 'GROUP1', 1).otherwise(0) * -0.1628493) + (F.when(F.col("Gender_binned") == 'GROUP3', 1).otherwise(0) * -0.069886) +
    (F.when(F.col("home_market_value_binned") == 'GROUP3', 1).otherwise(0) * 0.0720238) + (F.when(F.col("home_market_value_binned") == 'GROUP1', 1).otherwise(0) * -0.1340705) + (F.when(F.col("home_market_value_binned") == 'GROUP4', 1).otherwise(0) * 0.115576) +
    (F.when(F.col("voter_party_input_binned") == 'GROUP2', 1).otherwise(0) * -0.1350375) + (F.when(F.col("voter_party_input_binned") == 'GROUP1', 1).otherwise(0) * -0.184536) + (F.when(F.col("voter_party_input_binned") == 'GROUP3', 1).otherwise(0) * -0.0955817) +
    (F.when(F.col("MonthsSinceLastOrder_binned") == '5.2:7.9667', 1).otherwise(0) * -0.1094125) + (F.when(F.col("MonthsSinceLastOrder_binned") == '7.9667:10.9667', 1).otherwise(0) * 0.0162154) + (F.when(F.col("MonthsSinceLastOrder_binned") == '10.9667:14.3333', 1).otherwise(0) * -0.0855302) + (F.when(F.col("MonthsSinceLastOrder_binned") == '14.3333:21.0667', 1).otherwise(0) * -0.1451393) + (F.when(F.col("MonthsSinceLastOrder_binned") == '21.0667:27.9', 1).otherwise(0) * -0.1992852) + (F.when(F.col("MonthsSinceLastOrder_binned") == '27.9:36.5', 1).otherwise(0) * -0.1518718) + (F.when(F.col("MonthsSinceLastOrder_binned") == '36.5:44.5333', 1).otherwise(0) * -0.2462482) + (F.when(F.col("MonthsSinceLastOrder_binned") == '44.5333:53.5', 1).otherwise(0) * -0.188467) + (F.when(F.col("MonthsSinceLastOrder_binned") == '>53.5', 1).otherwise(0) * -0.2778368) +
    (F.when(F.col("OriginCode_binned") == 'GROUP3', 1).otherwise(0) * 0.3409312) + (F.when(F.col("OriginCode_binned") == 'GROUP2', 1).otherwise(0) * 0.1876203) +
    (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '0:2', 1).otherwise(0) * 0.0532926) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '2:5', 1).otherwise(0) * 0.104023) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '5:8', 1).otherwise(0) * 0.1515453) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '8:10', 1).otherwise(0) * 0.218499) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '10:12', 1).otherwise(0) * 0.2049991) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '12:15', 1).otherwise(0) * 0.2851353) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '15:17', 1).otherwise(0) * 0.2909454) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '17:21', 1).otherwise(0) * 0.3220622) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '>21', 1).otherwise(0) * 0.3122538) +
    (F.when(F.col("Age_HH_pct_with_HHer_65_74_binne") == '80:98', 1).otherwise(0) * -0.1058882) + (F.when(F.col("Age_HH_pct_with_HHer_65_74_binne") == '98:112', 1).otherwise(0) * -0.1022258) + (F.when(F.col("Age_HH_pct_with_HHer_65_74_binne") == '112:123', 1).otherwise(0) * -0.1207602) + (F.when(F.col("Age_HH_pct_with_HHer_65_74_binne") == '123:133', 1).otherwise(0) * -0.0893778) + (F.when(F.col("Age_HH_pct_with_HHer_65_74_binne") == '133:144', 1).otherwise(0) * -0.1090655) + (F.when(F.col("Age_HH_pct_with_HHer_65_74_binne") == '144:156', 1).otherwise(0) * -0.0782349) + (F.when(F.col("Age_HH_pct_with_HHer_65_74_binne") == '156:170', 1).otherwise(0) * -0.2467342) + (F.when(F.col("Age_HH_pct_with_HHer_65_74_binne") == '170:196', 1).otherwise(0) * -0.069184) + (F.when(F.col("Age_HH_pct_with_HHer_65_74_binne") == '>196', 1).otherwise(0) * -0.313954) +
    (F.when(F.col("NYL_Num_Active_Particpnts_binned") == 'GROUP3', 1).otherwise(0) * 0.1536275) + (F.when(F.col("NYL_Num_Active_Particpnts_binned") == 'GROUP2', 1).otherwise(0) * 0.2782553) +
    (F.when(F.col("PartyMix_binned") == 'GROUP1', 1).otherwise(0) * -0.2012263) + (F.when(F.col("PartyMix_binned") == 'GROUP2', 1).otherwise(0) * -0.0968826) + (F.when(F.col("PartyMix_binned") == 'GROUP4', 1).otherwise(0) * -0.0573075) +
    (F.when(F.col("VoterStatus_binned") == 'GROUP2', 1).otherwise(0) * 0.106141) + (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP1', 1).otherwise(0) * -0.1233488) + (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP3', 1).otherwise(0) * -0.0095714) +
    (F.when(F.col("Overall_Historic_SP_Reltshps_bin") == '0:1', 1).otherwise(0) * 0.0474878) + (F.when(F.col("Overall_Historic_SP_Reltshps_bin") == '1:2', 1).otherwise(0) * 0.1937679) + (F.when(F.col("Overall_Historic_SP_Reltshps_bin") == '>2', 1).otherwise(0) * 0.1673497) +
    (F.when(F.col("number_of_lines_of_credit_X_binn") == '0:1', 1).otherwise(0) * -0.0063814) + (F.when(F.col("number_of_lines_of_credit_X_binn") == '1:2', 1).otherwise(0) * -0.0130475) + (F.when(F.col("number_of_lines_of_credit_X_binn") == '2:4', 1).otherwise(0) * -0.0444342) + (F.when(F.col("number_of_lines_of_credit_X_binn") == '>4', 1).otherwise(0) * -0.2025337) +
    (F.col("ACEV_Num_X_log") * -0.0969169) + (F.col("MonthsSinceLastFndnContrib_X_log") * 0.0312881))
df_old_masters = df_old_masters.withColumn("p_score8", F.exp(F.col("pred8")) / (1 + F.exp(F.col("pred8"))))

# MODEL 9
df_old_masters = df_old_masters.na.fill({
    "ACEV_Num_X": 0.19040788156007, "age_agg_ind": 69.171197303718, "Age_HH_pct_with_HHer_85p": 38.56732096927,
    "MemXRenew": 4.8998428583244, "NbrTimesSelEmailedInd": 29.982907396686, "OCCHU_Median_Length_of_Residence": 875.16989305537,
    "Past12MoTouchCt_AARP_X": 13.66313909113, "Past12MoTouchCt_Financial_X": 10.476493355347, "Past12MoTouchCt_Health_X": 3.7184886458982,
    "Pop_pct_Asian_Only_Hisp": 0.50965209151212, "CENS_ETHNIC_POP_PERCENT_BLACK_ON": 86.233897387302, "Pop_pct_Black_Only_Hisp": 2.0458372817111, "VoterCount": 8.4994624100571
})
df_old_masters = df_old_masters.withColumn("ACEV_Num_X_log", F.log(F.col("ACEV_Num_X") + 1))
df_old_masters = df_old_masters.withColumn("Age_normalized", (F.col("age_agg_ind") - 69.154478135827) / 9.9271257966606)
df_old_masters = df_old_masters.withColumn("Age_HH_pct_with_HHer_85p_log", F.log(F.col("Age_HH_pct_with_HHer_85p") + 1))
df_old_masters = df_old_masters.withColumn("MemXRenew_normalized", (F.col("MemXRenew") - 4.9005123959025) / 3.0470451785475)
df_old_masters = df_old_masters.withColumn("NbrTimesSelEmailedInd_normalized", (F.col("NbrTimesSelEmailedInd") - 29.809119693696) / 57.49682884759)
df_old_masters = df_old_masters.withColumn("OCCHU_Median_Length_of_Resid_nor", (F.col("OCCHU_Median_Length_of_Residence") - 873.71866460695) / 312.45049877184)
df_old_masters = df_old_masters.withColumn("Past12MoTouchCt_AARP_X_normalize", (F.col("Past12MoTouchCt_AARP_X") - 13.608857189347) / 12.020958594713)
df_old_masters = df_old_masters.withColumn("Past12MoTouchCt_Financial_X_norm", (F.col("Past12MoTouchCt_Financial_X") - 10.453924174215) / 7.7967360321777)
df_old_masters = df_old_masters.withColumn("Past12MoTouchCt_Health_X_normali", (F.col("Past12MoTouchCt_Health_X") - 3.6997613494603) / 4.2478888779226)
df_old_masters = df_old_masters.withColumn("Pop_pct_Asian_Only_Hisp_log", F.log(F.col("Pop_pct_Asian_Only_Hisp") + 1))
df_old_masters = df_old_masters.withColumn("Pop_pct_Black_Only__log", F.log(F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON") + 1))
df_old_masters = df_old_masters.withColumn("Pop_pct_Black_Only_Hisp_log", F.log(F.col("Pop_pct_Black_Only_Hisp") + 1))
df_old_masters = df_old_masters.withColumn("VoterCount_normalized", (F.col("VoterCount") - 8.4641383539724) / 9.37359914137)
df_old_masters = df_old_masters.withColumn("Foremost_Num_Active_Particpn_bin", F.when(F.col("Foremost_Num_Active_Particpnts") == '1', 'GROUP1').when(F.col("Foremost_Num_Active_Particpnts") == '0', 'GROUP2').when(F.col("Foremost_Num_Active_Particpnts").isin('BLANK','2'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Gender_binned", F.when(F.col("Gender") == 'M', 'GROUP1').when(F.col("Gender").isin('F','BLANK'), 'GROUP2').when(F.col("Gender") == 'U', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("GE_Num_Active_Particpnts_binned", F.when(F.col("GE_Num_Active_Particpnts").isin('2','0'), 'GROUP1').when(F.col("GE_Num_Active_Particpnts").isin('1','BLANK','3','4'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("home_market_value_binned", F.when(F.col("home_market_value").isin('S','L','I','R','O','J','K','F','A','M','B','Q','D','P','G'), 'GROUP1').when(F.col("home_market_value").isin('E','C','N','H'), 'GROUP2').when(F.col("home_market_value") == 'BLANK', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("MaritalStatus_binned", F.when(F.col("MaritalStatus").isin('W','M','I'), 'GROUP1').when(F.col("MaritalStatus").isin('S','BLANK'), 'GROUP2').when(F.col("MaritalStatus").isin('U','B','D','A'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("NYL_Num_Active_Particpnts_binned", F.when(F.col("NYL_Num_Active_Particpnts").isin('0','3'), 'GROUP1').when(F.col("NYL_Num_Active_Particpnts").isin('BLANK','2'), 'GROUP2').when(F.col("NYL_Num_Active_Particpnts") == '1', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("OriginCode_binned", F.when(F.col("OriginCode").isin('12','22','0'), 'GROUP1').when(F.col("OriginCode").isin('BLANK','2','5','1','16','7','3','20','6','9','13','8'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("PartyMix_binned", F.when(F.col("PartyMix") == '1', 'GROUP1').when(F.col("PartyMix").isin('5','4','3'), 'GROUP2').when(F.col("PartyMix").isin('0','7'), 'GROUP3').when(F.col("PartyMix") == '2', 'GROUP4').when(F.col("PartyMix").isin('U','6','BLANK'), 'GROUP5').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("voter_party_input_binned", F.when(F.col("voter_party_input") == 'R', 'GROUP1').when(F.col("voter_party_input") == 'V', 'GROUP2').when(F.col("voter_party_input") == 'BLANK', 'GROUP3').when(F.col("voter_party_input").isin('D','I'), 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("pred9", 
    -1.3094467 +
    (F.col("Age_normalized") * -0.3895476) + (F.col("Pop_pct_Black_Only__log") * 0.0962757) + (F.pow(F.col("Age_normalized"), 2) * 0.1109861) +
    (F.when(F.col("PartyMix_binned") == 'GROUP1', 1).otherwise(0) * -0.0869844) + (F.when(F.col("PartyMix_binned") == 'GROUP4', 1).otherwise(0) * 0.0779877) + (F.when(F.col("PartyMix_binned") == 'GROUP2', 1).otherwise(0) * -0.0703338) + (F.when(F.col("PartyMix_binned") == 'GROUP5', 1).otherwise(0) * 0.0523822) +
    (F.when(F.col("OriginCode_binned") == 'GROUP2', 1).otherwise(0) * 0.3169913) + (F.col("VoterCount_normalized") * -0.0810585) + (F.col("OCCHU_Median_Length_of_Resid_nor") * 0.0707752) +
    (F.col("Past12MoTouchCt_AARP_X_normalize") * 0.0461686) + (F.pow(F.col("Age_normalized"), 3) * 0.0396487) + (F.pow(F.col("Age_normalized"), 4) * -0.009577) +
    (F.when(F.col("NYL_Num_Active_Particpnts_binned") == 'GROUP3', 1).otherwise(0) * 0.2238364) + (F.when(F.col("NYL_Num_Active_Particpnts_binned") == 'GROUP2', 1).otherwise(0) * 0.0601908) +
    (F.when(F.col("MaritalStatus_binned") == 'GROUP1', 1).otherwise(0) * -0.1872371) + (F.when(F.col("MaritalStatus_binned") == 'GROUP2', 1).otherwise(0) * -0.147187) +
    (F.col("Past12MoTouchCt_Financial_X_norm") * 0.044669) + (F.pow(F.col("Pop_pct_Asian_Only_Hisp_log"), 3) * 0.0178794) + (F.col("ACEV_Num_X_log") * -0.1347537) +
    (F.when(F.col("Gender_binned") == 'GROUP1', 1).otherwise(0) * -0.0830697) + (F.when(F.col("Gender_binned") == 'GROUP3', 1).otherwise(0) * -0.1200727) +
    (F.when(F.col("GE_Num_Active_Particpnts_binned") == 'GROUP2', 1).otherwise(0) * 0.1494638) +
    (F.when(F.col("voter_party_input_binned") == 'GROUP1', 1).otherwise(0) * -0.1245948) + (F.when(F.col("voter_party_input_binned") == 'GROUP2', 1).otherwise(0) * -0.061598) + (F.when(F.col("voter_party_input_binned") == 'GROUP3', 1).otherwise(0) * -0.0322821) +
    (F.when(F.col("home_market_value_binned") == 'GROUP2', 1).otherwise(0) * 0.0811608) + (F.when(F.col("home_market_value_binned") == 'GROUP3', 1).otherwise(0) * 0.0102084) +
    (F.pow(F.col("Pop_pct_Black_Only_Hisp_log"), 2) * 0.0135126) + (F.pow(F.col("MemXRenew_normalized"), 2) * 0.0323305) + (F.pow(F.col("Age_HH_pct_with_HHer_85p_log"), 2) * -0.0048928) +
    (F.pow(F.col("Past12MoTouchCt_Health_X_normali"), 2) * -0.0137911) + (F.col("NbrTimesSelEmailedInd_normalized") * 0.1717541) + (F.pow(F.col("NbrTimesSelEmailedInd_normalized"), 2) * -0.189487) +
    (F.pow(F.col("NbrTimesSelEmailedInd_normalized"), 3) * 0.0413171) +
    (F.when(F.col("Foremost_Num_Active_Particpn_bin") == 'GROUP1', 1).otherwise(0) * -0.3281193) + (F.when(F.col("Foremost_Num_Active_Particpn_bin") == 'GROUP3', 1).otherwise(0) * -0.2402952) +
    (F.pow(F.col("VoterCount_normalized"), 2) * 0.0188084))
df_old_masters = df_old_masters.withColumn("p_score9", F.exp(F.col("pred9")) / (1 + F.exp(F.col("pred9"))))

# MODEL 11
df_old_masters = df_old_masters.na.fill({
    "age_agg_ind": 69.171197303718, "CENS_INC_HH_MEDIAN_HOUSEHOLD_INC": 58864.123162312, "MonthsSinceLastAdvoContrib_X": 7.6036746339351,
    "NbrTimesSelEmailedInd": 29.982907396686, "No_of_Historic_Particpnts_UHG": 0.11253549472059, "Past12MoTouchCt_Priv_X": 0.67039272471378,
    "Past3MoTouchCt_Financial_X": 1.2420223563483, "CENS_ETHNIC_POP_PERCENT_BLACK_ON": 86.233897387302
})
df_old_masters = df_old_masters.withColumn("No_of_Historic_Particpnts_UH_log", F.log(F.col("No_of_Historic_Particpnts_UHG") + 1))
df_old_masters = df_old_masters.withColumn("IBX_EDUCATION_binned", F.when(F.col("IBX_EDUCATION").isin('BLANK','3'), 'GROUP1').when(F.col("IBX_EDUCATION").isin('2','1','4'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Gender_binned", F.when(F.col("Gender").isin('BLANK','F','U'), 'GROUP1').when(F.col("Gender") == 'M', 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("home_market_value_binned", F.when(F.col("home_market_value").isin('P','Q','BLANK','N','I','O','J','H','A','K','M','F','G','S','L','R'), 'GROUP1').when(F.col("home_market_value").isin('E','D','B','C'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("MaritalStatus_binned", F.when(F.col("MaritalStatus").isin('A','BLANK','S'), 'GROUP1').when(F.col("MaritalStatus").isin('M','I','W','U','D'), 'GROUP2').when(F.col("MaritalStatus") == 'B', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("OriginCode_binned", F.when(F.col("OriginCode").isin('12','13','16','22','7','8','2','BLANK','3'), 'GROUP1').when(F.col("OriginCode") == '0', 'GROUP2').when(F.col("OriginCode").isin('1','5','20','9','6'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("PartyMix_binned", F.when(F.col("PartyMix").isin('BLANK','4','7','1'), 'GROUP1').when(F.col("PartyMix").isin('U','3','0','6','5'), 'GROUP2').when(F.col("PartyMix") == '2', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("State_binned", F.when(F.col("State").isin('NH','VA','WY','IA','CT','ME','BLANK','IN','VT','KS','OR','RI','MT','CO','NE','NY','AK','ID','MA','ND','NJ','IL','AR','MI','TX','UT','WV','KY','OK','SD'), 'GROUP1').when(F.col("State").isin('CA','WI','MS','HI','MN','NV','NM','TN','MD','GA','LA','NC','DE','FL','AL','DC','MO','AZ','SC','PA','WA','OH'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Inc_HH_Median_HH_Income_binned", F.when(F.col("CENS_INC_HH_MEDIAN_HOUSEHOLD_INC") <= 32799, '<=32799').when(F.col("CENS_INC_HH_MEDIAN_HOUSEHOLD_INC") <= 38999, '32799:38999').when(F.col("CENS_INC_HH_MEDIAN_HOUSEHOLD_INC") <= 43975.2, '38999:43975.2').when(F.col("CENS_INC_HH_MEDIAN_HOUSEHOLD_INC") <= 48823, '43975.2:48823').when(F.col("CENS_INC_HH_MEDIAN_HOUSEHOLD_INC") <= 54047, '48823:54047').when(F.col("CENS_INC_HH_MEDIAN_HOUSEHOLD_INC") <= 59611.2, '54047:59611.2').when(F.col("CENS_INC_HH_MEDIAN_HOUSEHOLD_INC") <= 66906.4, '59611.2:66906.4').when(F.col("CENS_INC_HH_MEDIAN_HOUSEHOLD_INC") <= 76470, '66906.4:76470').when(F.col("CENS_INC_HH_MEDIAN_HOUSEHOLD_INC") <= 91101, '76470:91101').otherwise('>91101'))
df_old_masters = df_old_masters.withColumn("Past12MoTouchCt_Priv_X_binned", F.when(F.col("Past12MoTouchCt_Priv_X") <= 0, '<=0').when(F.col("Past12MoTouchCt_Priv_X") <= 1, '0:1').when(F.col("Past12MoTouchCt_Priv_X") <= 2, '1:2').otherwise('>2'))
df_old_masters = df_old_masters.withColumn("pred11", 
    -3.6051394 +
    (F.when(F.col("Age_binned") == '57:60', 1).otherwise(0) * 0.7687623) + (F.when(F.col("Age_binned") == '60:63', 1).otherwise(0) * 1.2227045) + (F.when(F.col("Age_binned") == '63:65', 1).otherwise(0) * 1.4276816) + (F.when(F.col("Age_binned") == '65:68', 1).otherwise(0) * 1.4972794) + (F.when(F.col("Age_binned") == '68:71', 1).otherwise(0) * 1.3152388) + (F.when(F.col("Age_binned") == '71:74', 1).otherwise(0) * 1.1514749) + (F.when(F.col("Age_binned") == '74:78', 1).otherwise(0) * 0.978048) + (F.when(F.col("Age_binned") == '78:83', 1).otherwise(0) * 0.8611617) + (F.when(F.col("Age_binned") == '>83', 1).otherwise(0) * 0.7221614) +
    (F.when(F.col("State_binned") == 'GROUP2', 1).otherwise(0) * 0.1965567) + (F.when(F.col("Gender_binned") == 'GROUP2', 1).otherwise(0) * 0.279441) +
    (F.when(F.col("Inc_HH_Median_HH_Income_binned") == '32799:38999', 1).otherwise(0) * -0.0778173) + (F.when(F.col("Inc_HH_Median_HH_Income_binned") == '38999:43975.2', 1).otherwise(0) * -0.1600469) + (F.when(F.col("Inc_HH_Median_HH_Income_binned") == '43975.2:48823', 1).otherwise(0) * -0.2222569) + (F.when(F.col("Inc_HH_Median_HH_Income_binned") == '48823:54047', 1).otherwise(0) * -0.16565) + (F.when(F.col("Inc_HH_Median_HH_Income_binned") == '54047:59611.2', 1).otherwise(0) * -0.0797599) + (F.when(F.col("Inc_HH_Median_HH_Income_binned") == '59611.2:66906.4', 1).otherwise(0) * -0.2345995) + (F.when(F.col("Inc_HH_Median_HH_Income_binned") == '66906.4:76470', 1).otherwise(0) * -0.2059967) + (F.when(F.col("Inc_HH_Median_HH_Income_binned") == '76470:91101', 1).otherwise(0) * -0.1630863) + (F.when(F.col("Inc_HH_Median_HH_Income_binned") == '>91101', 1).otherwise(0) * -0.270349) +
    (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '0:1', 1).otherwise(0) * 0.1980137) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '1:2', 1).otherwise(0) * 0.0567297) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '2:3', 1).otherwise(0) * 0.1699938) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '>3', 1).otherwise(0) * 0.2574893) +
    (F.when(F.col("Pop_pct_Black_Only__binned") == '2:4', 1).otherwise(0) * 0.1542851) + (F.when(F.col("Pop_pct_Black_Only__binned") == '4:6', 1).otherwise(0) * 0.1195584) + (F.when(F.col("Pop_pct_Black_Only__binned") == '6:10', 1).otherwise(0) * 0.1167764) + (F.when(F.col("Pop_pct_Black_Only__binned") == '10:16', 1).otherwise(0) * 0.117352) + (F.when(F.col("Pop_pct_Black_Only__binned") == '16:27', 1).otherwise(0) * 0.1347264) + (F.when(F.col("Pop_pct_Black_Only__binned") == '27:46', 1).otherwise(0) * 0.0861789) + (F.when(F.col("Pop_pct_Black_Only__binned") == '46:91', 1).otherwise(0) * 0.1266595) + (F.when(F.col("Pop_pct_Black_Only__binned") == '91:239', 1).otherwise(0) * 0.1449179) + (F.when(F.col("Pop_pct_Black_Only__binned") == '>239', 1).otherwise(0) * 0.3870636) +
    (F.when(F.col("PartyMix_binned") == 'GROUP1', 1).otherwise(0) * -0.0727271) + (F.when(F.col("PartyMix_binned") == 'GROUP3', 1).otherwise(0) * 0.1017515) + (F.when(F.col("home_market_value_binned") == 'GROUP2', 1).otherwise(0) * 0.1085841) +
    (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '0:7.7333', 1).otherwise(0) * 0.1519256) + (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '7.7333:20.3467', 1).otherwise(0) * 0.0846952) + (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '>20.3467', 1).otherwise(0) * 0.1377697) +
    (F.when(F.col("NbrTimesSelEmailedInd_binned") == '0:1', 1).otherwise(0) * -0.0321959) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '1:10', 1).otherwise(0) * -0.0973367) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '10:40', 1).otherwise(0) * -0.0034926) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '40:139', 1).otherwise(0) * 0.1599528) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '>139', 1).otherwise(0) * 0.1574501) +
    (F.when(F.col("MaritalStatus_binned") == 'GROUP3', 1).otherwise(0) * 0.3256721) + (F.when(F.col("MaritalStatus_binned") == 'GROUP2', 1).otherwise(0) * 0.0718153) +
    (F.when(F.col("Past12MoTouchCt_Priv_X_binned") == '0:1', 1).otherwise(0) * 0.0493455) + (F.when(F.col("Past12MoTouchCt_Priv_X_binned") == '1:2', 1).otherwise(0) * 0.1271006) + (F.when(F.col("Past12MoTouchCt_Priv_X_binned") == '>2', 1).otherwise(0) * 0.2334178) +
    (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP1', 1).otherwise(0) * -0.0753094) +
    (F.when(F.col("OriginCode_binned") == 'GROUP3', 1).otherwise(0) * 0.2640813) + (F.when(F.col("OriginCode_binned") == 'GROUP1', 1).otherwise(0) * 0.050029) +
    (F.pow(F.col("No_of_Historic_Particpnts_UH_log"), 2) * -0.1487429))
df_old_masters = df_old_masters.withColumn("p_score11", F.exp(F.col("pred11")) / (1 + F.exp(F.col("pred11"))))

# MODEL 14
df_old_masters = df_old_masters.na.fill({
    "ACEV_Num_X": 0.19040788156007, "Advo_Last_Amt_X": 2.43321514602, "Advo_TTD_Amt_X": 7.0002165263756, "age_agg_ind": 69.171197303718,
    "Fndn_TTD_Amt_X": 5.5316098736027, "Home_purch_yr": 1996.3816060398, "CENS_HOMVAL_HOME_VALUE_CBSA_INDE": 109.93353188033,
    "infb_trend_telecom_cellular_user": 5.6865280642105, "MemXRenew": 4.8998428583244, "MonthsSinceLastAdvoContrib_X": 7.6036746339351,
    "MonthsSinceLastFndnContrib_X": 2.8837758031775, "NbrTimesSelEmailedInd": 29.982907396686, "Pop_pct_Asian_Only_Hisp": 0.50965209151212, "VoterCount": 8.4994624100571
})
df_old_masters = df_old_masters.withColumn("ACEV_Num_X_log", F.log(F.col("ACEV_Num_X") + 1))
df_old_masters = df_old_masters.withColumn("Advo_Last_Amt_X_log", F.log(F.col("Advo_Last_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("Advo_TTD_Amt_X_log", F.log(F.col("Advo_TTD_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("Fndn_TTD_Amt_X_log", F.log(F.col("Fndn_TTD_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("MonthsSinceLastFndnContrib_X_log", F.log(F.col("MonthsSinceLastFndnContrib_X") + 1.3667))
df_old_masters = df_old_masters.withColumn("Pop_pct_Asian_Only_Hisp_log", F.log(F.col("Pop_pct_Asian_Only_Hisp") + 1))
df_old_masters = df_old_masters.withColumn("Chase_Num_Active_Particpnts_binn", F.when(F.col("Chase_Num_Active_Particpnts").isin('1','BLANK'), 'GROUP1').when(F.col("Chase_Num_Active_Particpnts").isin('0','2'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("IBX_EDUCATION_binned", F.when(F.col("IBX_EDUCATION") == '1', 'GROUP1').when(F.col("IBX_EDUCATION").isin('4','2','BLANK'), 'GROUP2').when(F.col("IBX_EDUCATION") == '3', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Gender_binned", F.when(F.col("Gender").isin('F','U','BLANK'), 'GROUP1').when(F.col("Gender") == 'M', 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("home_market_value_binned", F.when(F.col("home_market_value").isin('B','C'), 'GROUP1').when(F.col("home_market_value").isin('D','A','E'), 'GROUP2').when(F.col("home_market_value").isin('F','G','H','BLANK','K','I'), 'GROUP3').when(F.col("home_market_value").isin('J','M'), 'GROUP4').when(F.col("home_market_value").isin('L','O','Q','P','N','S','R'), 'GROUP5').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("NYL_Num_Active_Particpnts_binned", F.when(F.col("NYL_Num_Active_Particpnts").isin('3','1','2'), 'GROUP1').when(F.col("NYL_Num_Active_Particpnts").isin('BLANK','0'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("OriginCode_binned", F.when(F.col("OriginCode").isin('16','22','8','3','1','20'), 'GROUP1').when(F.col("OriginCode").isin('BLANK','0','2','5','6','7','12','9','13'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("PartyMix_binned", F.when(F.col("PartyMix").isin('3','U'), 'GROUP1').when(F.col("PartyMix").isin('1','7','5','4','BLANK','0'), 'GROUP2').when(F.col("PartyMix") == '6', 'GROUP3').when(F.col("PartyMix") == '2', 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("VMIS_Num_Act_Flag_binned", F.when(F.col("VMIS_Num_Act_Flag") == '0', 'GROUP1').when(F.col("VMIS_Num_Act_Flag") == '1', 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("VoterStatus_binned", F.when(F.col("Voterstatus") == 'unregistered', 'GROUP1').when(F.col("Voterstatus").isin('unmatchedMember','BLANK','inactive'), 'GROUP2').when(F.col("Voterstatus").isin('dropped','active','multipleAppearances'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("HomVal_Home_Value_CBSA_Index_bin", F.when(F.col("CENS_HOMVAL_HOME_VALUE_CBSA_INDE") <= 60, '<=60').when(F.col("CENS_HOMVAL_HOME_VALUE_CBSA_INDE") <= 73, '60:73').when(F.col("CENS_HOMVAL_HOME_VALUE_CBSA_INDE") <= 83, '73:83').when(F.col("CENS_HOMVAL_HOME_VALUE_CBSA_INDE") <= 92, '83:92').when(F.col("CENS_HOMVAL_HOME_VALUE_CBSA_INDE") <= 101, '92:101').when(F.col("CENS_HOMVAL_HOME_VALUE_CBSA_INDE") <= 111, '101:111').when(F.col("CENS_HOMVAL_HOME_VALUE_CBSA_INDE") <= 122, '111:122').when(F.col("CENS_HOMVAL_HOME_VALUE_CBSA_INDE") <= 140, '122:140').when(F.col("CENS_HOMVAL_HOME_VALUE_CBSA_INDE") <= 169, '140:169').otherwise('>169'))
df_old_masters = df_old_masters.withColumn("MemXRenew_binned", F.when(F.col("MemXRenew") <= 1, '<=1').when(F.col("MemXRenew") <= 2, '1:2').when(F.col("MemXRenew") <= 3, '2:3').when(F.col("MemXRenew") <= 4, '3:4').when(F.col("MemXRenew") <= 4.8998428583244, '4:4.8998').when(F.col("MemXRenew") <= 6, '4.8998:6').when(F.col("MemXRenew") <= 7, '6:7').when(F.col("MemXRenew") <= 9, '7:9').otherwise('>9'))
df_old_masters = df_old_masters.withColumn("pred14", 
    -2.2068437 +
    (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP2', 1).otherwise(0) * 0.3596835) + (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP3', 1).otherwise(0) * 0.7243203) +
    (F.when(F.col("infb_trend_telecom_cellular__bin") == '2:3', 1).otherwise(0) * 0.0150347) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '3:4', 1).otherwise(0) * 0.0431557) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '4:5', 1).otherwise(0) * -0.0736764) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '5:6', 1).otherwise(0) * 0.0516264) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '6:7', 1).otherwise(0) * -0.0969091) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '7:8', 1).otherwise(0) * -0.1154547) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '8:9', 1).otherwise(0) * -0.2791474) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '9:10', 1).otherwise(0) * -0.4446962) +
    (F.when(F.col("PartyMix_binned") == 'GROUP4', 1).otherwise(0) * 0.4061314) + (F.when(F.col("PartyMix_binned") == 'GROUP1', 1).otherwise(0) * 0.0279047) + (F.when(F.col("PartyMix_binned") == 'GROUP3', 1).otherwise(0) * 0.0829152) +
    (F.when(F.col("VoterStatus_binned") == 'GROUP2', 1).otherwise(0) * -0.2301375) + (F.when(F.col("VoterStatus_binned") == 'GROUP1', 1).otherwise(0) * -0.6383422) +
    (F.when(F.col("Age_binned") == '57:60', 1).otherwise(0) * 0.0681849) + (F.when(F.col("Age_binned") == '60:63', 1).otherwise(0) * 0.1476286) + (F.when(F.col("Age_binned") == '63:65', 1).otherwise(0) * 0.1215362) + (F.when(F.col("Age_binned") == '65:68', 1).otherwise(0) * 0.0667009) + (F.when(F.col("Age_binned") == '68:71', 1).otherwise(0) * 0.0919781) + (F.when(F.col("Age_binned") == '71:74', 1).otherwise(0) * 0.0098294) + (F.when(F.col("Age_binned") == '74:78', 1).otherwise(0) * -0.1678305) + (F.when(F.col("Age_binned") == '78:83', 1).otherwise(0) * -0.3142386) + (F.when(F.col("Age_binned") == '>83', 1).otherwise(0) * -0.558163) +
    (F.when(F.col("VoterCount_binned") == '0:1', 1).otherwise(0) * -0.055422) + (F.when(F.col("VoterCount_binned") == '1:3', 1).otherwise(0) * 0.0252197) + (F.when(F.col("VoterCount_binned") == '3:5', 1).otherwise(0) * 0.115791) + (F.when(F.col("VoterCount_binned") == '5:8', 1).otherwise(0) * 0.1029083) + (F.when(F.col("VoterCount_binned") == '8:12', 1).otherwise(0) * 0.1919759) + (F.when(F.col("VoterCount_binned") == '12:16', 1).otherwise(0) * 0.3143218) + (F.when(F.col("VoterCount_binned") == '16:23', 1).otherwise(0) * 0.3540432) + (F.when(F.col("VoterCount_binned") == '>23', 1).otherwise(0) * 0.4929628) +
    (F.when(F.col("NbrTimesSelEmailedInd_binned") == '0:1', 1).otherwise(0) * -0.1379453) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '1:10', 1).otherwise(0) * 0.2038622) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '10:40', 1).otherwise(0) * 0.2212271) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '40:139', 1).otherwise(0) * 0.217544) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '>139', 1).otherwise(0) * 0.2591935) +
    (F.when(F.col("home_market_value_binned") == 'GROUP1', 1).otherwise(0) * -0.1161934) + (F.when(F.col("home_market_value_binned") == 'GROUP3', 1).otherwise(0) * 0.0606786) + (F.when(F.col("home_market_value_binned") == 'GROUP4', 1).otherwise(0) * 0.1100772) + (F.when(F.col("home_market_value_binned") == 'GROUP5', 1).otherwise(0) * 0.2629464) +
    (F.col("ACEV_Num_X_log") * 0.3050908) +
    (F.when(F.col("Home_purch_yr_binned") == '1986:1991', 1).otherwise(0) * 0.1867082) + (F.when(F.col("Home_purch_yr_binned") == '1991:1995', 1).otherwise(0) * 0.1804505) + (F.when(F.col("Home_purch_yr_binned") == '1995:1996.3816', 1).otherwise(0) * 0.1477159) + (F.when(F.col("Home_purch_yr_binned") == '1996.3816:1996.3816', 1).otherwise(0) * 0.3408989) + (F.when(F.col("Home_purch_yr_binned") == '1996.3816:2000', 1).otherwise(0) * 0.2098852) + (F.when(F.col("Home_purch_yr_binned") == '2000:2004', 1).otherwise(0) * 0.3495881) + (F.when(F.col("Home_purch_yr_binned") == '2004:2008', 1).otherwise(0) * 0.3833533) + (F.when(F.col("Home_purch_yr_binned") == '>2008', 1).otherwise(0) * 0.2413984) +
    (F.when(F.col("Gender_binned") == 'GROUP2', 1).otherwise(0) * 0.140086) + (F.when(F.col("VMIS_Num_Act_Flag_binned") == 'GROUP2', 1).otherwise(0) * 0.5828889) +
    (F.pow(F.col("Advo_TTD_Amt_X_log"), 3) * 0.0047138) + (F.when(F.col("NYL_Num_Active_Particpnts_binned") == 'GROUP1', 1).otherwise(0) * -0.2267069) +
    (F.pow(F.col("MonthsSinceLastFndnContrib_X_log"), 2) * 0.027148) + (F.col("Fndn_TTD_Amt_X_log") * -0.078558) +
    (F.when(F.col("MemXRenew_binned") == '1:2', 1).otherwise(0) * -0.1501309) + (F.when(F.col("MemXRenew_binned") == '2:3', 1).otherwise(0) * -0.0911604) + (F.when(F.col("MemXRenew_binned") == '3:4', 1).otherwise(0) * -0.1711437) + (F.when(F.col("MemXRenew_binned") == '4:4.8998', 1).otherwise(0) * -0.0741454) + (F.when(F.col("MemXRenew_binned") == '4.8998:6', 1).otherwise(0) * -0.1965737) + (F.when(F.col("MemXRenew_binned") == '6:7', 1).otherwise(0) * -0.1534749) + (F.when(F.col("MemXRenew_binned") == '7:9', 1).otherwise(0) * -0.3004379) +
    (F.when(F.col("OriginCode_binned") == 'GROUP1', 1).otherwise(0) * -0.2757299) +
    (F.when(F.col("HomVal_Home_Value_CBSA_Index_bin") == '60:73', 1).otherwise(0) * -0.0339548) + (F.when(F.col("HomVal_Home_Value_CBSA_Index_bin") == '73:83', 1).otherwise(0) * -0.0962302) + (F.when(F.col("HomVal_Home_Value_CBSA_Index_bin") == '83:92', 1).otherwise(0) * -0.0049633) + (F.when(F.col("HomVal_Home_Value_CBSA_Index_bin") == '92:101', 1).otherwise(0) * 0.0517151) + (F.when(F.col("HomVal_Home_Value_CBSA_Index_bin") == '101:111', 1).otherwise(0) * 0.0351836) + (F.when(F.col("HomVal_Home_Value_CBSA_Index_bin") == '111:122', 1).otherwise(0) * 0.0153807) + (F.when(F.col("HomVal_Home_Value_CBSA_Index_bin") == '122:140', 1).otherwise(0) * 0.0384144) + (F.when(F.col("HomVal_Home_Value_CBSA_Index_bin") == '140:169', 1).otherwise(0) * 0.0765664) + (F.when(F.col("HomVal_Home_Value_CBSA_Index_bin") == '>169', 1).otherwise(0) * 0.2401385) +
    (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '0:7.7333', 1).otherwise(0) * 0.1159973) + (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '7.7333:20.3467', 1).otherwise(0) * 0.1417381) + (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '>20.3467', 1).otherwise(0) * 0.2569216) +
    (F.when(F.col("Chase_Num_Active_Particpnts_binn") == 'GROUP1', 1).otherwise(0) * -0.211356) + (F.col("Advo_Last_Amt_X_log") * -0.0660669) + (F.pow(F.col("Pop_pct_Asian_Only_Hisp_log"), 2) * -0.0362005))
df_old_masters = df_old_masters.withColumn("p_score14", F.exp(F.col("pred14")) / (1 + F.exp(F.col("pred14"))))

# MODEL 16
df_old_masters = df_old_masters.na.fill({
    "ACEV_Num_X": 0.19040788156007, "Advo_Last_Amt_X": 2.43321514602, "age_agg_ind": 69.171197303718,
    "NbrTimesSelEmailedInd": 29.982907396686, "Past3MoTouchCt_Financial_X": 1.2420223563483, "Pop_pct_Asian_Only_Hisp": 0.50965209151212,
    "CENS_ETHNIC_POP_PERCENT_BLACK_ON": 86.233897387302, "vehicle_truck_motorcycle_rv": 29.974820739107
})
df_old_masters = df_old_masters.withColumn("ACEV_Num_X_log", F.log(F.col("ACEV_Num_X") + 1))
df_old_masters = df_old_masters.withColumn("Advo_Last_Amt_X_log", F.log(F.col("Advo_Last_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("Pop_pct_Asian_Only_Hisp_log", F.log(F.col("Pop_pct_Asian_Only_Hisp") + 1))
df_old_masters = df_old_masters.withColumn("Chase_Num_Active_Particpnts_binn", F.when(F.col("Chase_Num_Active_Particpnts") == '1', 'GROUP1').when(F.col("Chase_Num_Active_Particpnts").isin('0','BLANK','2'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("IBX_EDUCATION_binned", F.when(F.col("IBX_EDUCATION").isin('1','4'), 'GROUP1').when(F.col("IBX_EDUCATION").isin('2','BLANK'), 'GROUP2').when(F.col("IBX_EDUCATION") == '3', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Gender_binned", F.when(F.col("Gender") == 'M', 'GROUP1').when(F.col("Gender").isin('BLANK','F','U'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Hartford_Num_Active_Particpn_bin", F.when(F.col("Hartford_Num_Active_Particpnts").isin('4','3'), 'GROUP1').when(F.col("Hartford_Num_Active_Particpnts").isin('1','2'), 'GROUP2').when(F.col("Hartford_Num_Active_Particpnts").isin('0','BLANK'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("home_market_value_binned", F.when(F.col("home_market_value").isin('A','C','F','D','R','I'), 'GROUP1').when(F.col("home_market_value").isin('E','B','M','K','L','G','P','J','Q','H','S'), 'GROUP2').when(F.col("home_market_value").isin('BLANK','O','N'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("PartyMix_binned", F.when(F.col("PartyMix") == '1', 'GROUP1').when(F.col("PartyMix").isin('4','0','U','7','3'), 'GROUP2').when(F.col("PartyMix").isin('5','BLANK','6','2'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("VMIS_Flag_binned", F.when(F.col("VMIS_Flag") == '0', 'GROUP1').when(F.col("VMIS_Flag") == '1', 'GROUP2').when(F.col("VMIS_Flag") == 'BLANK', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("VoterStatus_binned", F.when(F.col("Voterstatus").isin('unmatchedMember','unregistered'), 'GROUP1').when(F.col("Voterstatus").isin('active','BLANK','dropped','multipleAppearances'), 'GROUP2').when(F.col("Voterstatus") == 'inactive', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("voter_party_input_binned", F.when(F.col("voter_party_input").isin('R','BLANK'), 'GROUP1').when(F.col("voter_party_input").isin('V','I'), 'GROUP2').when(F.col("voter_party_input") == 'D', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("pred16", 
    -1.5361318 +
    (F.when(F.col("Age_binned") == '57:60', 1).otherwise(0) * -0.1192138) + (F.when(F.col("Age_binned") == '60:63', 1).otherwise(0) * -0.1693542) + (F.when(F.col("Age_binned") == '63:65', 1).otherwise(0) * -0.2371361) + (F.when(F.col("Age_binned") == '65:68', 1).otherwise(0) * -0.3178282) + (F.when(F.col("Age_binned") == '68:71', 1).otherwise(0) * -0.4965464) + (F.when(F.col("Age_binned") == '71:74', 1).otherwise(0) * -0.6245308) + (F.when(F.col("Age_binned") == '74:78', 1).otherwise(0) * -0.8846212) + (F.when(F.col("Age_binned") == '78:83', 1).otherwise(0) * -0.9908897) + (F.when(F.col("Age_binned") == '>83', 1).otherwise(0) * -1.212554) +
    (F.when(F.col("Pop_pct_Black_Only__binned") == '2:4', 1).otherwise(0) * -0.0187374) + (F.when(F.col("Pop_pct_Black_Only__binned") == '4:6', 1).otherwise(0) * 0.1367959) + (F.when(F.col("Pop_pct_Black_Only__binned") == '6:10', 1).otherwise(0) * 0.0257249) + (F.when(F.col("Pop_pct_Black_Only__binned") == '10:16', 1).otherwise(0) * -0.0054738) + (F.when(F.col("Pop_pct_Black_Only__binned") == '16:27', 1).otherwise(0) * 0.0231065) + (F.when(F.col("Pop_pct_Black_Only__binned") == '27:46', 1).otherwise(0) * 0.062543) + (F.when(F.col("Pop_pct_Black_Only__binned") == '46:91', 1).otherwise(0) * 0.0705983) + (F.when(F.col("Pop_pct_Black_Only__binned") == '91:239', 1).otherwise(0) * 0.200191) + (F.when(F.col("Pop_pct_Black_Only__binned") == '>239', 1).otherwise(0) * 0.4746465) +
    (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP2', 1).otherwise(0) * 0.2052484) + (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP3', 1).otherwise(0) * 0.4146934) +
    (F.when(F.col("VMIS_Flag_binned") == 'GROUP2', 1).otherwise(0) * 1.2106889) + (F.when(F.col("VMIS_Flag_binned") == 'GROUP3', 1).otherwise(0) * -0.2734179) +
    (F.when(F.col("NbrTimesSelEmailedInd_binned") == '0:1', 1).otherwise(0) * -0.093266) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '1:10', 1).otherwise(0) * 0.1019261) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '10:40', 1).otherwise(0) * 0.2029731) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '40:139', 1).otherwise(0) * 0.2798553) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '>139', 1).otherwise(0) * 0.2835863) +
    (F.when(F.col("PartyMix_binned") == 'GROUP1', 1).otherwise(0) * -0.0219528) + (F.when(F.col("PartyMix_binned") == 'GROUP3', 1).otherwise(0) * 0.1791846) +
    (F.when(F.col("Gender_binned") == 'GROUP1', 1).otherwise(0) * -0.2009632) + (F.pow(F.col("ACEV_Num_X_log"), 2) * 0.3550835) +
    (F.when(F.col("vehicle_truck_motorcycle_rv_binn") == '0:29.9748', 1).otherwise(0) * 0.0185706) + (F.when(F.col("vehicle_truck_motorcycle_rv_binn") == '29.9748:100', 1).otherwise(0) * -0.1772017) + (F.when(F.col("vehicle_truck_motorcycle_rv_binn") == '100:101', 1).otherwise(0) * -0.1127234) + (F.when(F.col("vehicle_truck_motorcycle_rv_binn") == '>101', 1).otherwise(0) * -0.355115) +
    (F.pow(F.col("Advo_Last_Amt_X_log"), 2) * 0.0253405) + (F.when(F.col("VoterStatus_binned") == 'GROUP3', 1).otherwise(0) * 0.1557088) + (F.when(F.col("VoterStatus_binned") == 'GROUP1', 1).otherwise(0) * -0.2151291) +
    (F.when(F.col("home_market_value_binned") == 'GROUP2', 1).otherwise(0) * 0.0798524) + (F.when(F.col("home_market_value_binned") == 'GROUP3', 1).otherwise(0) * 0.2017161) +
    (F.when(F.col("voter_party_input_binned") == 'GROUP2', 1).otherwise(0) * -0.0825653) + (F.when(F.col("voter_party_input_binned") == 'GROUP1', 1).otherwise(0) * -0.1389019) +
    (F.when(F.col("Chase_Num_Active_Particpnts_binn") == 'GROUP1', 1).otherwise(0) * -0.2354936) + (F.when(F.col("Hartford_Num_Active_Particpn_bin") == 'GROUP2', 1).otherwise(0) * -0.1469911) + (F.when(F.col("Hartford_Num_Active_Particpn_bin") == 'GROUP1', 1).otherwise(0) * -1.6020304) +
    (F.col("Pop_pct_Asian_Only_Hisp_log") * 0.0703604) +
    (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '0:1', 1).otherwise(0) * 0.1956711) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '1:2', 1).otherwise(0) * 0.1096915) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '2:3', 1).otherwise(0) * 0.0603956) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '>3', 1).otherwise(0) * 0.1544926))
df_old_masters = df_old_masters.withColumn("p_score16", F.exp(F.col("pred16")) / (1 + F.exp(F.col("pred16"))))

# MODEL 20
df_old_masters = df_old_masters.na.fill({
    "ACEV_Num_X": 0.19040788156007, "Advo_Last_Amt_X": 2.43321514602, "age_agg_ind": 69.171197303718, "CurrentPartCt_Overall_X": 0.27918369556392,
    "Fndn_TTD_Amt_X": 5.5316098736027, "Home_purch_yr": 1996.3816060398, "infb_trend_telecom_cellular_user": 5.6865280642105,
    "NbrTimesSelEmailedInd": 29.982907396686, "Past12MoTouchCt_Health_X": 3.7184886458982, "Past3MoTouchCt_Health_X": 0.37962486805424,
    "CENS_ETHNIC_POP_PERCENT_BLACK_ON": 86.233897387302, "vehicle_truck_motorcycle_rv": 29.974820739107
})
df_old_masters = df_old_masters.withColumn("ACEV_Num_X_log", F.log(F.col("ACEV_Num_X") + 1))
df_old_masters = df_old_masters.withColumn("Advo_Last_Amt_X_log", F.log(F.col("Advo_Last_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("CurrentPartCt_Overall_X_normaliz", (F.col("CurrentPartCt_Overall_X") - 0.27542441828931) / 0.60379632322646)
df_old_masters = df_old_masters.withColumn("Fndn_TTD_Amt_X_log", F.log(F.col("Fndn_TTD_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("IBX_EDUCATION_binned", F.when(F.col("IBX_EDUCATION").isin('1','4'), 'GROUP1').when(F.col("IBX_EDUCATION") == 'BLANK', 'GROUP2').when(F.col("IBX_EDUCATION") == '2', 'GROUP3').when(F.col("IBX_EDUCATION") == '3', 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Gender_binned", F.when(F.col("Gender").isin('M','BLANK'), 'GROUP1').when(F.col("Gender").isin('F','U'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Hartford_Num_Active_Particpn_bin", F.when(F.col("Hartford_Num_Active_Particpnts").isin('4','1'), 'GROUP1').when(F.col("Hartford_Num_Active_Particpnts").isin('2','BLANK'), 'GROUP2').when(F.col("Hartford_Num_Active_Particpnts").isin('0','3'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("home_market_value_binned", F.when(F.col("home_market_value").isin('B','A','C','D'), 'GROUP1').when(F.col("home_market_value").isin('E','BLANK','F','L','R','H','M','K','G','O','I','N','Q','J','S','P'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("MaritalStatus_binned", F.when(F.col("MaritalStatus").isin('W','D','BLANK','S','I'), 'GROUP1').when(F.col("MaritalStatus") == 'M', 'GROUP2').when(F.col("MaritalStatus").isin('B','U','A'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("NYL_Num_Active_Particpnts_binned", F.when(F.col("NYL_Num_Active_Particpnts").isin('BLANK','1','0'), 'GROUP1').when(F.col("NYL_Num_Active_Particpnts").isin('2','3'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("PartyMix_binned", F.when(F.col("PartyMix").isin('1','BLANK'), 'GROUP1').when(F.col("PartyMix").isin('3','0','U','4'), 'GROUP2').when(F.col("PartyMix") == '2', 'GROUP3').when(F.col("PartyMix").isin('5','6'), 'GROUP4').when(F.col("PartyMix") == '7', 'GROUP5').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("VoterStatus_binned", F.when(F.col("Voterstatus").isin('multipleAppearances','unregistered'), 'GROUP1').when(F.col("Voterstatus").isin('BLANK','unmatchedMember','active','inactive'), 'GROUP2').when(F.col("Voterstatus") == 'dropped', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("voter_party_input_binned", F.when(F.col("voter_party_input").isin('BLANK','R'), 'GROUP1').when(F.col("voter_party_input").isin('V','I'), 'GROUP2').when(F.col("voter_party_input") == 'D', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("pred20", 
    -0.4361427 +
    (F.when(F.col("Age_binned") == '57:60', 1).otherwise(0) * -0.1144352) + (F.when(F.col("Age_binned") == '60:63', 1).otherwise(0) * -0.1727857) + (F.when(F.col("Age_binned") == '63:65', 1).otherwise(0) * -0.3620981) + (F.when(F.col("Age_binned") == '65:68', 1).otherwise(0) * -0.5070247) + (F.when(F.col("Age_binned") == '68:71', 1).otherwise(0) * -0.6299887) + (F.when(F.col("Age_binned") == '71:74', 1).otherwise(0) * -0.7617997) + (F.when(F.col("Age_binned") == '74:78', 1).otherwise(0) * -0.9348326) + (F.when(F.col("Age_binned") == '78:83', 1).otherwise(0) * -1.2295742) + (F.when(F.col("Age_binned") == '>83', 1).otherwise(0) * -1.7034101) +
    (F.when(F.col("Gender_binned") == 'GROUP1', 1).otherwise(0) * -0.3106724) +
    (F.when(F.col("infb_trend_telecom_cellular__bin") == '2:3', 1).otherwise(0) * 0.0081714) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '3:4', 1).otherwise(0) * -0.0659257) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '4:5', 1).otherwise(0) * -0.0995117) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '5:6', 1).otherwise(0) * -0.1204959) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '6:7', 1).otherwise(0) * -0.1552446) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '7:8', 1).otherwise(0) * -0.1983258) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '8:9', 1).otherwise(0) * -0.2421488) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '9:10', 1).otherwise(0) * -0.3877063) +
    (F.when(F.col("Pop_pct_Black_Only__binned") == '2:4', 1).otherwise(0) * 0.0230739) + (F.when(F.col("Pop_pct_Black_Only__binned") == '4:6', 1).otherwise(0) * 0.1139499) + (F.when(F.col("Pop_pct_Black_Only__binned") == '6:10', 1).otherwise(0) * 0.0661347) + (F.when(F.col("Pop_pct_Black_Only__binned") == '10:16', 1).otherwise(0) * 0.1315315) + (F.when(F.col("Pop_pct_Black_Only__binned") == '16:27', 1).otherwise(0) * 0.1298843) + (F.when(F.col("Pop_pct_Black_Only__binned") == '27:46', 1).otherwise(0) * 0.1101284) + (F.when(F.col("Pop_pct_Black_Only__binned") == '46:91', 1).otherwise(0) * 0.1471123) + (F.when(F.col("Pop_pct_Black_Only__binned") == '91:239', 1).otherwise(0) * 0.1873561) + (F.when(F.col("Pop_pct_Black_Only__binned") == '>239', 1).otherwise(0) * 0.4284041) +
    (F.col("ACEV_Num_X_log") * 0.2943368) +
    (F.when(F.col("PartyMix_binned") == 'GROUP1', 1).otherwise(0) * -0.0154011) + (F.when(F.col("PartyMix_binned") == 'GROUP3', 1).otherwise(0) * 0.1288615) + (F.when(F.col("PartyMix_binned") == 'GROUP4', 1).otherwise(0) * 0.1803456) + (F.when(F.col("PartyMix_binned") == 'GROUP5', 1).otherwise(0) * 0.3626423) +
    (F.pow(F.col("Advo_Last_Amt_X_log"), 2) * -0.0256688) +
    (F.when(F.col("NbrTimesSelEmailedInd_binned") == '0:1', 1).otherwise(0) * 0.0490229) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '1:10', 1).otherwise(0) * 0.1808505) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '10:40', 1).otherwise(0) * 0.1460531) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '40:139', 1).otherwise(0) * 0.1638045) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '>139', 1).otherwise(0) * 0.1505433) +
    (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP3', 1).otherwise(0) * 0.095096) + (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP4', 1).otherwise(0) * 0.1876277) + (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP2', 1).otherwise(0) * 0.0994876) +
    (F.when(F.col("VoterStatus_binned") == 'GROUP3', 1).otherwise(0) * 0.1426103) + (F.when(F.col("VoterStatus_binned") == 'GROUP1', 1).otherwise(0) * -0.2173797) +
    (F.when(F.col("MaritalStatus_binned") == 'GROUP1', 1).otherwise(0) * -0.2266738) + (F.when(F.col("MaritalStatus_binned") == 'GROUP2', 1).otherwise(0) * -0.1113372) +
    (F.when(F.col("home_market_value_binned") == 'GROUP2', 1).otherwise(0) * 0.1109455) + (F.col("CurrentPartCt_Overall_X_normaliz") * -0.045622) +
    (F.when(F.col("voter_party_input_binned") == 'GROUP2', 1).otherwise(0) * -0.0847559) + (F.when(F.col("voter_party_input_binned") == 'GROUP1', 1).otherwise(0) * -0.1139112) +
    (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '0:1', 1).otherwise(0) * 0.0964521) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '1:2', 1).otherwise(0) * 0.1462466) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '2:4', 1).otherwise(0) * 0.1333651) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '4:6', 1).otherwise(0) * 0.0485286) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '6:7', 1).otherwise(0) * 0.2251161) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '7:10', 1).otherwise(0) * 0.1003362) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '>10', 1).otherwise(0) * 0.1847061) +
    (F.when(F.col("Hartford_Num_Active_Particpn_bin") == 'GROUP1', 1).otherwise(0) * -0.1462307) + (F.when(F.col("Hartford_Num_Active_Particpn_bin") == 'GROUP2', 1).otherwise(0) * 0.1237945) +
    (F.col("Fndn_TTD_Amt_X_log") * -0.0315864) +
    (F.when(F.col("Past3MoTouchCt_Health_X_binned") == '0:1', 1).otherwise(0) * -0.0082667) + (F.when(F.col("Past3MoTouchCt_Health_X_binned") == '1:2', 1).otherwise(0) * -0.1317036) + (F.when(F.col("Past3MoTouchCt_Health_X_binned") == '>2', 1).otherwise(0) * -0.1110865) +
    (F.when(F.col("Home_purch_yr_binned") == '1986:1991', 1).otherwise(0) * 0.0119876) + (F.when(F.col("Home_purch_yr_binned") == '1991:1995', 1).otherwise(0) * 0.0566778) + (F.when(F.col("Home_purch_yr_binned") == '1995:1996.3816', 1).otherwise(0) * 0.0642816) + (F.when(F.col("Home_purch_yr_binned") == '1996.3816:1996.3816', 1).otherwise(0) * 0.1127984) + (F.when(F.col("Home_purch_yr_binned") == '1996.3816:2000', 1).otherwise(0) * 0.0364145) + (F.when(F.col("Home_purch_yr_binned") == '2000:2004', 1).otherwise(0) * 0.1560239) + (F.when(F.col("Home_purch_yr_binned") == '2004:2008', 1).otherwise(0) * 0.1736478) + (F.when(F.col("Home_purch_yr_binned") == '>2008', 1).otherwise(0) * 0.1398579) +
    (F.when(F.col("NYL_Num_Active_Particpnts_binned") == 'GROUP2', 1).otherwise(0) * 0.2381371) +
    (F.when(F.col("vehicle_truck_motorcycle_rv_binn") == '0:29.9748', 1).otherwise(0) * 0.0704692) + (F.when(F.col("vehicle_truck_motorcycle_rv_binn") == '29.9748:100', 1).otherwise(0) * -0.0734451) + (F.when(F.col("vehicle_truck_motorcycle_rv_binn") == '100:101', 1).otherwise(0) * 0.0612505) + (F.when(F.col("vehicle_truck_motorcycle_rv_binn") == '>101', 1).otherwise(0) * -0.0135431))
df_old_masters = df_old_masters.withColumn("p_score20", F.exp(F.col("pred20")) / (1 + F.exp(F.col("pred20"))))

# MODEL 21
df_old_masters = df_old_masters.na.fill({
    "ACEV_Num_X": 0.19040788156007, "Advo_Last_Amt_X": 2.43321514602, "age_agg_ind": 69.171197303718, "CENS_ETHNIC_POP_PERCENT_BLACK_ON": 86.233897387302
})
df_old_masters = df_old_masters.withColumn("ACEV_Num_X_log", F.log(F.col("ACEV_Num_X") + 1))
df_old_masters = df_old_masters.withColumn("Advo_Last_Amt_X_log", F.log(F.col("Advo_Last_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("IBX_EDUCATION_binned", F.when(F.col("IBX_EDUCATION").isin('1','4'), 'GROUP1').when(F.col("IBX_EDUCATION").isin('BLANK','2'), 'GROUP2').when(F.col("IBX_EDUCATION") == '3', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Gender_binned", F.when(F.col("Gender") == 'M', 'GROUP1').when(F.col("Gender").isin('BLANK','F'), 'GROUP2').when(F.col("Gender") == 'U', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("home_market_value_binned", F.when(F.col("home_market_value").isin('D','C','B','K','A','E','F','J'), 'GROUP1').when(F.col("home_market_value").isin('G','M','R','L','S','H','I','N','Q','O','P'), 'GROUP2').when(F.col("home_market_value") == 'BLANK', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("MaritalStatus_binned", F.when(F.col("MaritalStatus").isin('W','M','I'), 'GROUP1').when(F.col("MaritalStatus").isin('S','BLANK','B'), 'GROUP2').when(F.col("MaritalStatus").isin('U','D','A'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("VoterStatus_binned", F.when(F.col("Voterstatus").isin('unregistered','multipleAppearances'), 'GROUP1').when(F.col("Voterstatus").isin('active','inactive','BLANK','unmatchedMember','dropped'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("voter_party_input_binned", F.when(F.col("voter_party_input").isin('I','R','V','BLANK'), 'GROUP1').when(F.col("voter_party_input") == 'D', 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("pred21", 
    -0.8136165 +
    (F.when(F.col("Age_binned") == '57:60', 1).otherwise(0) * 0.0072684) + (F.when(F.col("Age_binned") == '60:63', 1).otherwise(0) * -0.0473638) + (F.when(F.col("Age_binned") == '63:65', 1).otherwise(0) * -0.1750886) + (F.when(F.col("Age_binned") == '65:68', 1).otherwise(0) * -0.2590866) + (F.when(F.col("Age_binned") == '68:71', 1).otherwise(0) * -0.4596243) + (F.when(F.col("Age_binned") == '71:74', 1).otherwise(0) * -0.5544154) + (F.when(F.col("Age_binned") == '74:78', 1).otherwise(0) * -0.8834354) + (F.when(F.col("Age_binned") == '78:83', 1).otherwise(0) * -1.0984761) + (F.when(F.col("Age_binned") == '>83', 1).otherwise(0) * -1.5844174) +
    (F.when(F.col("Gender_binned") == 'GROUP1', 1).otherwise(0) * -0.4836371) + (F.when(F.col("Gender_binned") == 'GROUP3', 1).otherwise(0) * 0.0083799) +
    (F.when(F.col("voter_party_input_binned") == 'GROUP1', 1).otherwise(0) * -0.1576113) +
    (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP2', 1).otherwise(0) * 0.1156466) + (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP3', 1).otherwise(0) * 0.2336442) +
    (F.when(F.col("home_market_value_binned") == 'GROUP2', 1).otherwise(0) * 0.1434208) + (F.when(F.col("home_market_value_binned") == 'GROUP3', 1).otherwise(0) * 0.0675841) +
    (F.pow(F.col("Advo_Last_Amt_X_log"), 2) * -0.0248988) + (F.when(F.col("VoterStatus_binned") == 'GROUP1', 1).otherwise(0) * -0.2351299) +
    (F.col("ACEV_Num_X_log") * 0.2030596) +
    (F.when(F.col("MaritalStatus_binned") == 'GROUP2', 1).otherwise(0) * -0.2701598) + (F.when(F.col("MaritalStatus_binned") == 'GROUP1', 1).otherwise(0) * -0.294439) +
    (F.when(F.col("Pop_pct_Black_Only__binned") == '2:4', 1).otherwise(0) * 0.0630515) + (F.when(F.col("Pop_pct_Black_Only__binned") == '4:6', 1).otherwise(0) * 0.1561224) + (F.when(F.col("Pop_pct_Black_Only__binned") == '6:10', 1).otherwise(0) * 0.1137237) + (F.when(F.col("Pop_pct_Black_Only__binned") == '10:16', 1).otherwise(0) * 0.1767356) + (F.when(F.col("Pop_pct_Black_Only__binned") == '16:27', 1).otherwise(0) * 0.1736645) + (F.when(F.col("Pop_pct_Black_Only__binned") == '27:46', 1).otherwise(0) * 0.1133898) + (F.when(F.col("Pop_pct_Black_Only__binned") == '46:91', 1).otherwise(0) * 0.1060281) + (F.when(F.col("Pop_pct_Black_Only__binned") == '91:239', 1).otherwise(0) * 0.2037517) + (F.when(F.col("Pop_pct_Black_Only__binned") == '>239', 1).otherwise(0) * 0.3412495))
df_old_masters = df_old_masters.withColumn("p_score21", F.exp(F.col("pred21")) / (1 + F.exp(F.col("pred21"))))

# MODEL 22
df_old_masters = df_old_masters.na.fill({
    "ACEV_Num_X": 0.19040788156007, "IBX_ADULTS_NUM_AGG_HHD": 2.5053778460679, "age_agg_ind": 69.171197303718, "CurrentPartCt_Overall_X": 0.27918369556392,
    "Fndn_Last_Amt_X": 1.6503369691721, "MonthsSinceLastFndnContrib_X": 2.8837758031775, "MonthsSinceLastOrder": 25.69275392992,
    "NbrTimesSelEmailedInd": 29.982907396686, "No_of_Historic_Particpnts_UHG": 0.11253549472059, "Past12MoTouchCt_Financial_X": 10.476493355347,
    "Past3MoTouchCt_AARP_X": 3.8344385200422, "Pop_pct_Asian_Only_Hisp": 0.50965209151212, "CENS_ETHNIC_POP_PERCENT_BLACK_ON": 86.233897387302, "VoterCount": 8.4994624100571
})
df_old_masters = df_old_masters.withColumn("ACEV_Num_X_log", F.log(F.col("ACEV_Num_X") + 1))
df_old_masters = df_old_masters.withColumn("CurrentPartCt_Overall_X_normaliz", (F.col("CurrentPartCt_Overall_X") - 0.27542441828931) / 0.60379632322646)
df_old_masters = df_old_masters.withColumn("Fndn_Last_Amt_X_log", F.log(F.col("Fndn_Last_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("MonthsSinceLastFndnContrib_X_log", F.log(F.col("MonthsSinceLastFndnContrib_X") + 1.3667))
df_old_masters = df_old_masters.withColumn("No_of_Historic_Particpnts_UH_log", F.log(F.col("No_of_Historic_Particpnts_UHG") + 1))
df_old_masters = df_old_masters.withColumn("Pop_pct_Asian_Only_Hisp_log", F.log(F.col("Pop_pct_Asian_Only_Hisp") + 1))
df_old_masters = df_old_masters.withColumn("IBX_EDUCATION_binned", F.when(F.col("IBX_EDUCATION") == '3', 'GROUP1').when(F.col("IBX_EDUCATION").isin('2','4'), 'GROUP2').when(F.col("IBX_EDUCATION") == '1', 'GROUP3').when(F.col("IBX_EDUCATION") == 'BLANK', 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("MaritalStatus_binned", F.when(F.col("MaritalStatus").isin('A','D','I','M','W','B'), 'GROUP1').when(F.col("MaritalStatus").isin('S','U'), 'GROUP2').when(F.col("MaritalStatus") == 'BLANK', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("NYL_Num_Active_Particpnts_binned", F.when(F.col("NYL_Num_Active_Particpnts") == '0', 'GROUP1').when(F.col("NYL_Num_Active_Particpnts").isin('2','BLANK','1','3'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("OriginCode_binned", F.when(F.col("OriginCode").isin('12','5','8','0'), 'GROUP1').when(F.col("OriginCode").isin('1','BLANK','16','6','7'), 'GROUP2').when(F.col("OriginCode").isin('3','2','22','20','9','13'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("PartyMix_binned", F.when(F.col("PartyMix").isin('3','1','5','0'), 'GROUP1').when(F.col("PartyMix").isin('U','2','4','6','7'), 'GROUP2').when(F.col("PartyMix") == 'BLANK', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("IBX_ADULTS_NUM_AGG_HHD_binne", F.when(F.col("IBX_ADULTS_NUM_AGG_HHD") <= 1, '<=1').when(F.col("IBX_ADULTS_NUM_AGG_HHD") <= 2, '1:2').when(F.col("IBX_ADULTS_NUM_AGG_HHD") <= 2.5053778460679, '2:2.5054').when(F.col("IBX_ADULTS_NUM_AGG_HHD") <= 3, '2.5054:3').when(F.col("IBX_ADULTS_NUM_AGG_HHD") <= 4, '3:4').otherwise('>4'))
df_old_masters = df_old_masters.withColumn("Past3MoTouchCt_AARP_X_binned", F.when(F.col("Past3MoTouchCt_AARP_X") <= 1, '<=1').when(F.col("Past3MoTouchCt_AARP_X") <= 2, '1:2').when(F.col("Past3MoTouchCt_AARP_X") <= 3, '2:3').when(F.col("Past3MoTouchCt_AARP_X") <= 4, '3:4').when(F.col("Past3MoTouchCt_AARP_X") <= 6, '4:6').when(F.col("Past3MoTouchCt_AARP_X") <= 8, '6:8').otherwise('>8'))
df_old_masters = df_old_masters.withColumn("pred22", 
    -1.2823107 +
    (F.when(F.col("Age_binned") == '57:60', 1).otherwise(0) * -0.1265558) + (F.when(F.col("Age_binned") == '60:63', 1).otherwise(0) * -0.198261) + (F.when(F.col("Age_binned") == '63:65', 1).otherwise(0) * -0.1564452) + (F.when(F.col("Age_binned") == '65:68', 1).otherwise(0) * -0.0822796) + (F.when(F.col("Age_binned") == '68:71', 1).otherwise(0) * -0.068817) + (F.when(F.col("Age_binned") == '71:74', 1).otherwise(0) * 0.0842868) + (F.when(F.col("Age_binned") == '74:78', 1).otherwise(0) * 0.168191) + (F.when(F.col("Age_binned") == '78:83', 1).otherwise(0) * 0.3827766) + (F.when(F.col("Age_binned") == '>83', 1).otherwise(0) * 0.5182036) +
    (F.when(F.col("Pop_pct_Black_Only__binned") == '2:4', 1).otherwise(0) * 0.012461) + (F.when(F.col("Pop_pct_Black_Only__binned") == '4:6', 1).otherwise(0) * 0.0180965) + (F.when(F.col("Pop_pct_Black_Only__binned") == '6:10', 1).otherwise(0) * 0.0061982) + (F.when(F.col("Pop_pct_Black_Only__binned") == '10:16', 1).otherwise(0) * 0.0473995) + (F.when(F.col("Pop_pct_Black_Only__binned") == '16:27', 1).otherwise(0) * 0.043209) + (F.when(F.col("Pop_pct_Black_Only__binned") == '27:46', 1).otherwise(0) * 0.0873504) + (F.when(F.col("Pop_pct_Black_Only__binned") == '46:91', 1).otherwise(0) * 0.1484804) + (F.when(F.col("Pop_pct_Black_Only__binned") == '91:239', 1).otherwise(0) * 0.2254174) + (F.when(F.col("Pop_pct_Black_Only__binned") == '>239', 1).otherwise(0) * 0.4967327) +
    (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP2', 1).otherwise(0) * -0.1125922) + (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP1', 1).otherwise(0) * -0.2860713) + (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP4', 1).otherwise(0) * 0.0425193) +
    (F.when(F.col("IBX_ADULTS_NUM_AGG_HHD_binne") == '1:2', 1).otherwise(0) * 0.1636726) + (F.when(F.col("IBX_ADULTS_NUM_AGG_HHD_binne") == '2:2.5054', 1).otherwise(0) * 0.0461416) + (F.when(F.col("IBX_ADULTS_NUM_AGG_HHD_binne") == '2.5054:3', 1).otherwise(0) * 0.3076141) + (F.when(F.col("IBX_ADULTS_NUM_AGG_HHD_binne") == '3:4', 1).otherwise(0) * 0.4354447) + (F.when(F.col("IBX_ADULTS_NUM_AGG_HHD_binne") == '>4', 1).otherwise(0) * 0.6463691) +
    (F.col("ACEV_Num_X_log") * -0.3641173) + (F.when(F.col("MaritalStatus_binned") == 'GROUP2', 1).otherwise(0) * 0.2068179) + (F.when(F.col("MaritalStatus_binned") == 'GROUP3', 1).otherwise(0) * 0.1065136) +
    (F.when(F.col("OriginCode_binned") == 'GROUP2', 1).otherwise(0) * 0.3040622) + (F.when(F.col("OriginCode_binned") == 'GROUP3', 1).otherwise(0) * 0.6808801) +
    (F.when(F.col("NYL_Num_Active_Particpnts_binned") == 'GROUP2', 1).otherwise(0) * 0.2350764) +
    (F.when(F.col("VoterCount_binned") == '0:1', 1).otherwise(0) * -0.029053) + (F.when(F.col("VoterCount_binned") == '1:3', 1).otherwise(0) * -0.1601901) + (F.when(F.col("VoterCount_binned") == '3:5', 1).otherwise(0) * -0.192751) + (F.when(F.col("VoterCount_binned") == '5:8', 1).otherwise(0) * -0.1194445) + (F.when(F.col("VoterCount_binned") == '8:12', 1).otherwise(0) * -0.1106818) + (F.when(F.col("VoterCount_binned") == '12:16', 1).otherwise(0) * -0.1275346) + (F.when(F.col("VoterCount_binned") == '16:23', 1).otherwise(0) * -0.1475857) + (F.when(F.col("VoterCount_binned") == '>23', 1).otherwise(0) * -0.2343963) +
    (F.pow(F.col("Pop_pct_Asian_Only_Hisp_log"), 2) * 0.0649058) +
    (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '0:2', 1).otherwise(0) * -0.1643413) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '2:5', 1).otherwise(0) * -0.1983474) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '5:8', 1).otherwise(0) * -0.1064698) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '8:10', 1).otherwise(0) * -0.119453) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '10:12', 1).otherwise(0) * -0.0305973) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '12:15', 1).otherwise(0) * -0.1320039) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '15:17', 1).otherwise(0) * -0.0895284) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '17:21', 1).otherwise(0) * 0.0240737) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '>21', 1).otherwise(0) * 0.0445998) +
    (F.when(F.col("NbrTimesSelEmailedInd_binned") == '0:1', 1).otherwise(0) * -0.0702961) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '1:10', 1).otherwise(0) * -0.1015844) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '10:40', 1).otherwise(0) * -0.0567118) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '40:139', 1).otherwise(0) * -0.0658035) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '>139', 1).otherwise(0) * -0.2065869) +
    (F.when(F.col("MonthsSinceLastOrder_binned") == '5.2:7.9667', 1).otherwise(0) * -0.0700811) + (F.when(F.col("MonthsSinceLastOrder_binned") == '7.9667:10.9667', 1).otherwise(0) * -0.0400085) + (F.when(F.col("MonthsSinceLastOrder_binned") == '10.9667:14.3333', 1).otherwise(0) * -0.0999291) + (F.when(F.col("MonthsSinceLastOrder_binned") == '14.3333:21.0667', 1).otherwise(0) * -0.0944925) + (F.when(F.col("MonthsSinceLastOrder_binned") == '21.0667:27.9', 1).otherwise(0) * -0.2116587) + (F.when(F.col("MonthsSinceLastOrder_binned") == '27.9:36.5', 1).otherwise(0) * -0.209684) + (F.when(F.col("MonthsSinceLastOrder_binned") == '36.5:44.5333', 1).otherwise(0) * -0.170824) + (F.when(F.col("MonthsSinceLastOrder_binned") == '44.5333:53.5', 1).otherwise(0) * -0.193178) + (F.when(F.col("MonthsSinceLastOrder_binned") == '>53.5', 1).otherwise(0) * -0.1854852) +
    (F.col("No_of_Historic_Particpnts_UH_log") * 0.6488357) + (F.pow(F.col("No_of_Historic_Particpnts_UH_log"), 2) * -0.5464993) +
    (F.when(F.col("Past3MoTouchCt_AARP_X_binned") == '1:2', 1).otherwise(0) * 0.0766768) + (F.when(F.col("Past3MoTouchCt_AARP_X_binned") == '2:3', 1).otherwise(0) * 0.0578884) + (F.when(F.col("Past3MoTouchCt_AARP_X_binned") == '3:4', 1).otherwise(0) * 0.0963723) + (F.when(F.col("Past3MoTouchCt_AARP_X_binned") == '4:6', 1).otherwise(0) * -0.0600293) + (F.when(F.col("Past3MoTouchCt_AARP_X_binned") == '6:8', 1).otherwise(0) * -0.0729467) + (F.when(F.col("Past3MoTouchCt_AARP_X_binned") == '>8', 1).otherwise(0) * -0.0867981) +
    (F.col("MonthsSinceLastFndnContrib_X_log") * 0.0120489) + (F.pow(F.col("Fndn_Last_Amt_X_log"), 3) * 0.0237691) + (F.col("Fndn_Last_Amt_X_log") * 0.4882909) +
    (F.col("CurrentPartCt_Overall_X_normaliz") * 0.0723931) + (F.pow(F.col("CurrentPartCt_Overall_X_normaliz"), 2) * -0.0156978) +
    (F.when(F.col("PartyMix_binned") == 'GROUP2', 1).otherwise(0) * 0.0550272) + (F.pow(F.col("Fndn_Last_Amt_X_log"), 2) * -0.236462))
df_old_masters = df_old_masters.withColumn("p_score22", F.exp(F.col("pred22")) / (1 + F.exp(F.col("pred22"))))

# MODEL 26
df_old_masters = df_old_masters.na.fill({
    "CurrentPartCt_Overall_X": 0.27918369556392, "Fndn_TTD_Amt_X": 7.0002165263756, "infb_trend_telecom_cellular_user": 5.6865280642105,
    "infb_trend_telecom_internet_user": 6.6462850454267, "NbrTimesSelEmailedInd": 29.982907396686, "No_of_Historic_Particpnts_UHG": 0.11253549472059,
    "number_of_lines_of_credit_X": 1.4485073212981, "Past12MoTouchCt_AARP_X": 13.66313909113, "CENS_ETHNIC_POP_PERCENT_BLACK_ON": 86.233897387302
})
df_old_masters = df_old_masters.withColumn("CurrentPartCt_Overall_X_normaliz", (F.col("CurrentPartCt_Overall_X") - 0.27542441828931) / 0.60379632322646)
df_old_masters = df_old_masters.withColumn("Fndn_TTD_Amt_X_log", F.log(F.col("Fndn_TTD_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("No_of_Historic_Particpnts_UH_log", F.log(F.col("No_of_Historic_Particpnts_UHG") + 1))
df_old_masters = df_old_masters.withColumn("IBX_EDUCATION_binned", F.when(F.col("IBX_EDUCATION") == '3', 'GROUP1').when(F.col("IBX_EDUCATION").isin('2','BLANK','1','4'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("home_market_value_binned", F.when(F.col("home_market_value").isin('S','K','Q','I','P'), 'GROUP1').when(F.col("home_market_value").isin('M','L','R','O','H','N','J','E','G','A','D','F'), 'GROUP2').when(F.col("home_market_value").isin('C','BLANK','B'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("OriginCode_binned", F.when(F.col("OriginCode").isin('12','13','16','22','6','7','8','9','0','2','3'), 'GROUP1').when(F.col("OriginCode").isin('BLANK','1
,'20','5'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("PartyMix_binned", F.when(F.col("PartyMix").isin('5','1','4','6','3'), 'GROUP1').when(F.col("PartyMix").isin('0','2','U','7','BLANK'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Past12MoTouchCt_AARP_X_binned", F.when(F.col("Past12MoTouchCt_AARP_X") <= 3, '<=3').when(F.col("Past12MoTouchCt_AARP_X") <= 5, '3:5').when(F.col("Past12MoTouchCt_AARP_X") <= 6, '5:6').when(F.col("Past12MoTouchCt_AARP_X") <= 8, '6:8').when(F.col("Past12MoTouchCt_AARP_X") <= 10, '8:10').when(F.col("Past12MoTouchCt_AARP_X") <= 12, '10:12').when(F.col("Past12MoTouchCt_AARP_X") <= 15, '12:15').when(F.col("Past12MoTouchCt_AARP_X") <= 20, '15:20').when(F.col("Past12MoTouchCt_AARP_X") <= 30, '20:30').otherwise('>30'))
df_old_masters = df_old_masters.withColumn("pred26", 
    -3.3958604 +
    (F.when(F.col("NbrTimesSelEmailedInd_binned") == '0:1', 1).otherwise(0) * -0.2765098) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '1:10', 1).otherwise(0) * -0.193079) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '10:40', 1).otherwise(0) * -0.4633486) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '40:139', 1).otherwise(0) * -0.4206569) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '>139', 1).otherwise(0) * -1.2466364) +
    (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP1', 1).otherwise(0) * -0.5998419) +
    (F.when(F.col("infb_trend_telecom_cellular__bin") == '2:3', 1).otherwise(0) * 0.1264218) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '3:4', 1).otherwise(0) * 0.4008832) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '4:5', 1).otherwise(0) * 0.238234) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '5:6', 1).otherwise(0) * 0.3260707) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '6:7', 1).otherwise(0) * 0.5064872) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '7:8', 1).otherwise(0) * 0.4280158) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '8:9', 1).otherwise(0) * 0.3361933) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '9:10', 1).otherwise(0) * 0.1060505) +
    (F.when(F.col("home_market_value_binned") == 'GROUP3', 1).otherwise(0) * 0.0959084) + (F.when(F.col("home_market_value_binned") == 'GROUP1', 1).otherwise(0) * -0.3928628) +
    (F.when(F.col("Pop_pct_Black_Only__binned") == '2:4', 1).otherwise(0) * -0.0327195) + (F.when(F.col("Pop_pct_Black_Only__binned") == '4:6', 1).otherwise(0) * 0.2089655) + (F.when(F.col("Pop_pct_Black_Only__binned") == '6:10', 1).otherwise(0) * -0.0924946) + (F.when(F.col("Pop_pct_Black_Only__binned") == '10:16', 1).otherwise(0) * -0.0920674) + (F.when(F.col("Pop_pct_Black_Only__binned") == '16:27', 1).otherwise(0) * -0.2480129) + (F.when(F.col("Pop_pct_Black_Only__binned") == '27:46', 1).otherwise(0) * -0.2035041) + (F.when(F.col("Pop_pct_Black_Only__binned") == '46:91', 1).otherwise(0) * 0.1105114) + (F.when(F.col("Pop_pct_Black_Only__binned") == '91:239', 1).otherwise(0) * 0.1375764) + (F.when(F.col("Pop_pct_Black_Only__binned") == '>239', 1).otherwise(0) * 0.3322337) +
    (F.col("Fndn_TTD_Amt_X_log") * 0.1087469) + (F.col("CurrentPartCt_Overall_X_normaliz") * 0.0980054) +
    (F.when(F.col("OriginCode_binned") == 'GROUP2', 1).otherwise(0) * 0.5059533) +
    (F.when(F.col("number_of_lines_of_credit_X_binn") == '0:1', 1).otherwise(0) * 0.01837) + (F.when(F.col("number_of_lines_of_credit_X_binn") == '1:2', 1).otherwise(0) * -0.2693133) + (F.when(F.col("number_of_lines_of_credit_X_binn") == '2:4', 1).otherwise(0) * -0.2841831) + (F.when(F.col("number_of_lines_of_credit_X_binn") == '>4', 1).otherwise(0) * -0.2789107) +
    (F.when(F.col("PartyMix_binned") == 'GROUP1', 1).otherwise(0) * -0.1903957) +
    (F.when(F.col("Past12MoTouchCt_AARP_X_binned") == '3:5', 1).otherwise(0) * -0.3217717) + (F.when(F.col("Past12MoTouchCt_AARP_X_binned") == '5:6', 1).otherwise(0) * 0.1095257) + (F.when(F.col("Past12MoTouchCt_AARP_X_binned") == '6:8', 1).otherwise(0) * -0.1096835) + (F.when(F.col("Past12MoTouchCt_AARP_X_binned") == '8:10', 1).otherwise(0) * -0.1579481) + (F.when(F.col("Past12MoTouchCt_AARP_X_binned") == '10:12', 1).otherwise(0) * -0.0916005) + (F.when(F.col("Past12MoTouchCt_AARP_X_binned") == '12:15', 1).otherwise(0) * -0.2592394) + (F.when(F.col("Past12MoTouchCt_AARP_X_binned") == '15:20', 1).otherwise(0) * 0.0471816) + (F.when(F.col("Past12MoTouchCt_AARP_X_binned") == '20:30', 1).otherwise(0) * 0.2234643) + (F.when(F.col("Past12MoTouchCt_AARP_X_binned") == '>30', 1).otherwise(0) * -0.0879394) +
    (F.col("No_of_Historic_Particpnts_UH_log") * -0.6389498) + (F.pow(F.col("No_of_Historic_Particpnts_UH_log"), 3) * 0.3665167) +
    (F.when(F.col("infb_trend_telecom_internet__bin") == '3:4', 1).otherwise(0) * -0.2436475) + (F.when(F.col("infb_trend_telecom_internet__bin") == '4:5', 1).otherwise(0) * -0.0197039) + (F.when(F.col("infb_trend_telecom_internet__bin") == '5:6', 1).otherwise(0) * 0.0975708) + (F.when(F.col("infb_trend_telecom_internet__bin") == '6:7', 1).otherwise(0) * -0.1128055) + (F.when(F.col("infb_trend_telecom_internet__bin") == '7:8', 1).otherwise(0) * 0.2818524) + (F.when(F.col("infb_trend_telecom_internet__bin") == '8:9', 1).otherwise(0) * 0.0504504) + (F.when(F.col("infb_trend_telecom_internet__bin") == '9:10', 1).otherwise(0) * 0.018486))
df_old_masters = df_old_masters.withColumn("p_score26", F.exp(F.col("pred26")) / (1 + F.exp(F.col("pred26"))))

# MODEL 29
df_old_masters = df_old_masters.na.fill({
    "ACEV_Num_X": 0.19040788156007, "age_agg_ind": 69.171197303718, "Fndn_Last_Amt_X": 1.6503369691721, "Home_purch_yr": 1996.3816060398,
    "infb_trend_telecom_cellular_user": 5.6865280642105, "MemXRenew": 4.8998428583244, "MonthsSinceLastAdvoContrib_X": 7.6036746339351,
    "MonthsSinceLastOrder": 25.69275392992, "No_of_Historic_Particpnts_UHG": 0.11253549472059, "NYL_Num_InActive_Particpnts": 0.040526011082623,
    "Past12MoTouchCt_Health_X": 3.7184886458982, "Past12MoTouchCt_Priv_X": 0.67039272471378, "Past3MoTouchCt_AARP_X": 3.8344385200422,
    "Past3MoTouchCt_Financial_X": 1.2420223563483, "Past3MoTouchCt_Health_X": 0.37962486805424
})
df_old_masters = df_old_masters.withColumn("ACEV_Num_X_log", F.log(F.col("ACEV_Num_X") + 1))
df_old_masters = df_old_masters.withColumn("Fndn_Last_Amt_X_log", F.log(F.col("Fndn_Last_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("No_of_Historic_Particpnts_UH_log", F.log(F.col("No_of_Historic_Particpnts_UHG") + 1))
df_old_masters = df_old_masters.withColumn("NYL_Num_InActive_Particpnts_log", F.log(F.col("NYL_Num_InActive_Particpnts") + 1))
df_old_masters = df_old_masters.withColumn("Chase_Num_InActive_Particpnt_bin", F.when(F.col("Chase_Num_InActive_Particpnts").isin('3','2'), 'GROUP1').when(F.col("Chase_Num_InActive_Particpnts") == '0', 'GROUP2').when(F.col("Chase_Num_InActive_Particpnts") == '1', 'GROUP3').when(F.col("Chase_Num_InActive_Particpnts") == 'BLANK', 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("MaritalStatus_binned", F.when(F.col("MaritalStatus") == 'A', 'GROUP1').when(F.col("MaritalStatus").isin('M','I'), 'GROUP2').when(F.col("MaritalStatus").isin('B','W','U','BLANK','D'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("NYL_Num_Active_Particpnts_binned", F.when(F.col("NYL_Num_Active_Particpnts").isin('3','2','0'), 'GROUP1').when(F.col("NYL_Num_Active_Particpnts") == '1', 'GROUP2').when(F.col("NYL_Num_Active_Particpnts") == 'BLANK', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("OriginCode_binned", F.when(F.col("OriginCode").isin('12','16','2','20','22','7','8','3','0'), 'GROUP1').when(F.col("OriginCode") == '1', 'GROUP2').when(F.col("OriginCode").isin('BLANK','5','6','9','13'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("PartyMix_binned", F.when(F.col("PartyMix").isin('7','4','6','5'), 'GROUP1').when(F.col("PartyMix").isin('0','3'), 'GROUP2').when(F.col("PartyMix").isin('1','2','U'), 'GROUP3').when(F.col("PartyMix") == 'BLANK', 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("pred29", 
    -3.1513065 +
    (F.when(F.col("infb_trend_telecom_cellular__bin") == '2:3', 1).otherwise(0) * 0.253981) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '3:4', 1).otherwise(0) * 0.2261467) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '4:5', 1).otherwise(0) * 0.365694) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '5:6', 1).otherwise(0) * 0.3726447) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '6:7', 1).otherwise(0) * 0.5605967) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '7:8', 1).otherwise(0) * 0.6032976) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '8:9', 1).otherwise(0) * 0.685644) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '9:10', 1).otherwise(0) * 0.7517005) +
    (F.when(F.col("IBX_ADULTS_NUM_AGG_HHD_binne") == '1:2', 1).otherwise(0) * 0.1429512) + (F.when(F.col("IBX_ADULTS_NUM_AGG_HHD_binne") == '2:2.5054', 1).otherwise(0) * 0.6213018) + (F.when(F.col("IBX_ADULTS_NUM_AGG_HHD_binne") == '2.5054:3', 1).otherwise(0) * 0.1472059) + (F.when(F.col("IBX_ADULTS_NUM_AGG_HHD_binne") == '3:4', 1).otherwise(0) * 0.0937865) + (F.when(F.col("IBX_ADULTS_NUM_AGG_HHD_binne") == '>4', 1).otherwise(0) * -0.1290917) +
    (F.when(F.col("Age_binned") == '57:60', 1).otherwise(0) * 0.0175093) + (F.when(F.col("Age_binned") == '60:63', 1).otherwise(0) * 0.0384181) + (F.when(F.col("Age_binned") == '63:65', 1).otherwise(0) * 0.0031547) + (F.when(F.col("Age_binned") == '65:68', 1).otherwise(0) * 0.1482679) + (F.when(F.col("Age_binned") == '68:71', 1).otherwise(0) * 0.2430981) + (F.when(F.col("Age_binned") == '71:74', 1).otherwise(0) * 0.1789144) + (F.when(F.col("Age_binned") == '74:78', 1).otherwise(0) * 0.3275681) + (F.when(F.col("Age_binned") == '78:83', 1).otherwise(0) * 0.4889489) + (F.when(F.col("Age_binned") == '>83', 1).otherwise(0) * 0.5593267) +
    (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '0:7.7333', 1).otherwise(0) * 0.1229795) + (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '7.7333:20.3467', 1).otherwise(0) * -0.2132111) + (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '>20.3467', 1).otherwise(0) * -0.2618091) +
    (F.col("NYL_Num_InActive_Particpnts_log") * 0.9505344) +
    (F.when(F.col("MemXRenew_binned") == '1:2', 1).otherwise(0) * -0.2185446) + (F.when(F.col("MemXRenew_binned") == '2:3', 1).otherwise(0) * -0.3236767) + (F.when(F.col("MemXRenew_binned") == '3:4', 1).otherwise(0) * -0.3965122) + (F.when(F.col("MemXRenew_binned") == '4:4.8998', 1).otherwise(0) * -1.3973068) + (F.when(F.col("MemXRenew_binned") == '4.8998:6', 1).otherwise(0) * -0.2947025) + (F.when(F.col("MemXRenew_binned") == '6:7', 1).otherwise(0) * -0.4042746) + (F.when(F.col("MemXRenew_binned") == '7:9', 1).otherwise(0) * -0.2260124) +
    (F.when(F.col("NYL_Num_Active_Particpnts_binned") == 'GROUP2', 1).otherwise(0) * 0.3088559) +
    (F.when(F.col("MaritalStatus_binned") == 'GROUP3', 1).otherwise(0) * 0.3382023) + (F.when(F.col("MaritalStatus_binned") == 'GROUP2', 1).otherwise(0) * 0.2471215) +
    (F.when(F.col("Home_purch_yr_binned") == '1986:1991', 1).otherwise(0) * -0.1669807) + (F.when(F.col("Home_purch_yr_binned") == '1991:1995', 1).otherwise(0) * -0.1978099) + (F.when(F.col("Home_purch_yr_binned") == '1995:1996.3816', 1).otherwise(0) * -0.1471891) + (F.when(F.col("Home_purch_yr_binned") == '1996.3816:1996.3816', 1).otherwise(0) * -0.0322919) + (F.when(F.col("Home_purch_yr_binned") == '1996.3816:2000', 1).otherwise(0) * -0.0800052) + (F.when(F.col("Home_purch_yr_binned") == '2000:2004', 1).otherwise(0) * -0.2055495) + (F.when(F.col("Home_purch_yr_binned") == '2004:2008', 1).otherwise(0) * -0.0967566) + (F.when(F.col("Home_purch_yr_binned") == '>2008', 1).otherwise(0) * 0.2201852) +
    (F.col("ACEV_Num_X_log") * -0.3148691) +
    (F.when(F.col("Chase_Num_InActive_Particpnt_bin") == 'GROUP3', 1).otherwise(0) * 0.3394521) + (F.when(F.col("Chase_Num_InActive_Particpnt_bin") == 'GROUP1', 1).otherwise(0) * -1.7859321) +
    (F.when(F.col("OriginCode_binned") == 'GROUP2', 1).otherwise(0) * 0.2991342) + (F.when(F.col("OriginCode_binned") == 'GROUP3', 1).otherwise(0) * 1.3864745) +
    (F.when(F.col("Past12MoTouchCt_Priv_X_binned") == '0:1', 1).otherwise(0) * 0.2147999) + (F.when(F.col("Past12MoTouchCt_Priv_X_binned") == '1:2', 1).otherwise(0) * 0.0994194) + (F.when(F.col("Past12MoTouchCt_Priv_X_binned") == '>2', 1).otherwise(0) * 0.2848696) +
    (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '0:1', 1).otherwise(0) * 0.0298351) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '1:2', 1).otherwise(0) * -0.1950725) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '2:3', 1).otherwise(0) * -0.0198663) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '>3', 1).otherwise(0) * -0.0386124) +
    (F.when(F.col("MonthsSinceLastOrder_binned") == '5.2:7.9667', 1).otherwise(0) * -0.10095) + (F.when(F.col("MonthsSinceLastOrder_binned") == '7.9667:10.9667', 1).otherwise(0) * -0.0753043) + (F.when(F.col("MonthsSinceLastOrder_binned") == '10.9667:14.3333', 1).otherwise(0) * -0.130325) + (F.when(F.col("MonthsSinceLastOrder_binned") == '14.3333:21.0667', 1).otherwise(0) * 0.0746262) + (F.when(F.col("MonthsSinceLastOrder_binned") == '21.0667:27.9', 1).otherwise(0) * -0.1947551) + (F.when(F.col("MonthsSinceLastOrder_binned") == '27.9:36.5', 1).otherwise(0) * -0.2826546) + (F.when(F.col("MonthsSinceLastOrder_binned") == '36.5:44.5333', 1).otherwise(0) * -0.1942911) + (F.when(F.col("MonthsSinceLastOrder_binned") == '44.5333:53.5', 1).otherwise(0) * -0.18116) + (F.when(F.col("MonthsSinceLastOrder_binned") == '>53.5', 1).otherwise(0) * -0.1213942) +
    (F.when(F.col("Past3MoTouchCt_AARP_X_binned") == '1:2', 1).otherwise(0) * 0.0436951) + (F.when(F.col("Past3MoTouchCt_AARP_X_binned") == '2:3', 1).otherwise(0) * 0.0452537) + (F.when(F.col("Past3MoTouchCt_AARP_X_binned") == '3:4', 1).otherwise(0) * 0.1212336) + (F.when(F.col("Past3MoTouchCt_AARP_X_binned") == '4:6', 1).otherwise(0) * 0.1294191) + (F.when(F.col("Past3MoTouchCt_AARP_X_binned") == '6:8', 1).otherwise(0) * -0.1637864) + (F.when(F.col("Past3MoTouchCt_AARP_X_binned") == '>8', 1).otherwise(0) * -0.136403) +
    (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '0:1', 1).otherwise(0) * 0.0275528) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '1:2', 1).otherwise(0) * -0.0323891) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '2:4', 1).otherwise(0) * -0.0496448) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '4:6', 1).otherwise(0) * -0.0637939) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '6:7', 1).otherwise(0) * 0.057181) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '7:10', 1).otherwise(0) * -0.2219234) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '>10', 1).otherwise(0) * -0.4401763) +
    (F.when(F.col("Past3MoTouchCt_Health_X_binned") == '0:1', 1).otherwise(0) * 0.2518803) + (F.when(F.col("Past3MoTouchCt_Health_X_binned") == '1:2', 1).otherwise(0) * 0.234961) + (F.when(F.col("Past3MoTouchCt_Health_X_binned") == '>2', 1).otherwise(0) * -0.0465139) +
    (F.when(F.col("PartyMix_binned") == 'GROUP3', 1).otherwise(0) * 0.1046597) + (F.when(F.col("PartyMix_binned") == 'GROUP1', 1).otherwise(0) * -0.0491383) +
    (F.pow(F.col("NYL_Num_InActive_Particpnts_log"), 3) * -0.3838727) + (F.col("Fndn_Last_Amt_X_log") * 0.0486969) + (F.col("No_of_Historic_Particpnts_UH_log") * -0.8350444) +
    (F.pow(F.col("No_of_Historic_Particpnts_UH_log"), 2) * 0.6968185))
df_old_masters = df_old_masters.withColumn("p_score29", F.exp(F.col("pred29")) / (1 + F.exp(F.col("pred29"))))

# MODEL 30
df_old_masters = df_old_masters.na.fill({
    "ACEV_Num_X": 0.19040788156007, "age_agg_ind": 69.171197303718, "CurrentPartCt_Overall_X": 0.27918369556392,
    "infb_trend_telecom_cellular_user": 5.6865280642105, "MonthsSinceLastAdvoContrib_X": 7.6036746339351, "NbrTimesSelEmailedInd": 29.982907396686,
    "Past12MoTouchCt_Priv_X": 0.67039272471378, "Past3MoTouchCt_Financial_X": 1.2420223563483, "Pop_pct_Black_Only_Hisp": 2.0458372817111
})
df_old_masters = df_old_masters.withColumn("ACEV_Num_X_log", F.log(F.col("ACEV_Num_X") + 1))
df_old_masters = df_old_masters.withColumn("CurrentPartCt_Overall_X_normaliz", (F.col("CurrentPartCt_Overall_X") - 0.27542441828931) / 0.60379632322646)
df_old_masters = df_old_masters.withColumn("Chase_Num_Active_Particpnts_binn", F.when(F.col("Chase_Num_Active_Particpnts").isin('BLANK','1'), 'GROUP1').when(F.col("Chase_Num_Active_Particpnts").isin('0','2'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("IBX_EDUCATION_binned", F.when(F.col("IBX_EDUCATION").isin('1','BLANK'), 'GROUP1').when(F.col("IBX_EDUCATION").isin('2','4'), 'GROUP2').when(F.col("IBX_EDUCATION") == '3', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Gender_binned", F.when(F.col("Gender") == 'BLANK', 'GROUP1').when(F.col("Gender") == 'M', 'GROUP2').when(F.col("Gender").isin('F','U'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("GE_Num_Active_Particpnts_binned", F.when(F.col("GE_Num_Active_Particpnts").isin('4','2','BLANK','1'), 'GROUP1').when(F.col("GE_Num_Active_Particpnts").isin('0','3'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Hartford_Num_Active_Particpn_bin", F.when(F.col("Hartford_Num_Active_Particpnts").isin('4','BLANK'), 'GROUP1').when(F.col("Hartford_Num_Active_Particpnts").isin('2','1','0'), 'GROUP2').when(F.col("Hartford_Num_Active_Particpnts") == '3', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("home_market_value_binned", F.when(F.col("home_market_value").isin('A','B'), 'GROUP1').when(F.col("home_market_value").isin('C','R','D','E','BLANK','J','F','L'), 'GROUP2').when(F.col("home_market_value").isin('M','G','Q','I','O','K','H','S','P','N'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("MaritalStatus_binned", F.when(F.col("MaritalStatus") == 'BLANK', 'GROUP1').when(F.col("MaritalStatus") == 'S', 'GROUP2').when(F.col("MaritalStatus").isin('M','I','B'), 'GROUP3').when(F.col("MaritalStatus").isin('U','W','D','A'), 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("NYL_Num_Active_Particpnts_binned", F.when(F.col("NYL_Num_Active_Particpnts").isin('3','BLANK','1'), 'GROUP1').when(F.col("NYL_Num_Active_Particpnts") == '0', 'GROUP2').when(F.col("NYL_Num_Active_Particpnts") == '2', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("PartyMix_binned", F.when(F.col("PartyMix").isin('1','BLANK'), 'GROUP1').when(F.col("PartyMix") == '0', 'GROUP2').when(F.col("PartyMix").isin('U','3','2'), 'GROUP3').when(F.col("PartyMix").isin('5','4','6','7'), 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("VMIS_Num_Act_Flag_binned", F.when(F.col("VMIS_Num_Act_Flag") == '0', 'GROUP1').when(F.col("VMIS_Num_Act_Flag") == '1', 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("voter_party_input_binned", F.when(F.col("voter_party_input") == 'BLANK', 'GROUP1').when(F.col("voter_party_input").isin('R','V'), 'GROUP2').when(F.col("voter_party_input").isin('D','I'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Pop_pct_Black_Only_Hisp_binned", F.when(F.col("Pop_pct_Black_Only_Hisp") <= 0, '<=0').when(F.col("Pop_pct_Black_Only_Hisp") <= 1, '0:1').when(F.col("Pop_pct_Black_Only_Hisp") <= 2, '1:2').when(F.col("Pop_pct_Black_Only_Hisp") <= 3, '2:3').when(F.col("Pop_pct_Black_Only_Hisp") <= 6, '3:6').otherwise('>6'))
df_old_masters = df_old_masters.withColumn("pred30", 
    -1.0069786 +
    (F.when(F.col("Age_binned") == '57:60', 1).otherwise(0) * -0.0511807) + (F.when(F.col("Age_binned") == '60:63', 1).otherwise(0) * -0.1511928) + (F.when(F.col("Age_binned") == '63:65', 1).otherwise(0) * -0.228185) + (F.when(F.col("Age_binned") == '65:68', 1).otherwise(0) * -0.3678476) + (F.when(F.col("Age_binned") == '68:71', 1).otherwise(0) * -0.558765) + (F.when(F.col("Age_binned") == '71:74', 1).otherwise(0) * -0.7809534) + (F.when(F.col("Age_binned") == '74:78', 1).otherwise(0) * -0.9545083) + (F.when(F.col("Age_binned") == '78:83', 1).otherwise(0) * -1.4180287) + (F.when(F.col("Age_binned") == '>83', 1).otherwise(0) * -1.6181764) +
    (F.when(F.col("NbrTimesSelEmailedInd_binned") == '0:1', 1).otherwise(0) * 0.1015664) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '1:10', 1).otherwise(0) * 0.6537366) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '10:40', 1).otherwise(0) * 0.5851188) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '40:139', 1).otherwise(0) * 0.7482841) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '>139', 1).otherwise(0) * 1.0329681) +
    (F.when(F.col("infb_trend_telecom_cellular__bin") == '2:3', 1).otherwise(0) * 0.0336173) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '3:4', 1).otherwise(0) * -0.03829) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '4:5', 1).otherwise(0) * -0.0747593) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '5:6', 1).otherwise(0) * -0.0388568) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '6:7', 1).otherwise(0) * -0.1266046) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '7:8', 1).otherwise(0) * -0.2255798) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '8:9', 1).otherwise(0) * -0.2489613) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '9:10', 1).otherwise(0) * -0.5085365) +
    (F.when(F.col("Gender_binned") == 'GROUP2', 1).otherwise(0) * -0.2085921) + (F.when(F.col("Gender_binned") == 'GROUP1', 1).otherwise(0) * 4.2359811) +
    (F.when(F.col("home_market_value_binned") == 'GROUP2', 1).otherwise(0) * 0.1562428) + (F.when(F.col("home_market_value_binned") == 'GROUP3', 1).otherwise(0) * 0.2457697) +
    (F.when(F.col("voter_party_input_binned") == 'GROUP2', 1).otherwise(0) * -0.1010683) + (F.when(F.col("voter_party_input_binned") == 'GROUP1', 1).otherwise(0) * -0.1724831) +
    (F.when(F.col("NYL_Num_Active_Particpnts_binned") == 'GROUP1', 1).otherwise(0) * -0.3271111) + (F.when(F.col("NYL_Num_Active_Particpnts_binned") == 'GROUP3', 1).otherwise(0) * -0.0320481) +
    (F.when(F.col("IBX_ADULTS_NUM_AGG_HHD_binne") == '1:2', 1).otherwise(0) * -0.0737544) + (F.when(F.col("IBX_ADULTS_NUM_AGG_HHD_binne") == '2:2.5054', 1).otherwise(0) * 0.0797839) + (F.when(F.col("IBX_ADULTS_NUM_AGG_HHD_binne") == '2.5054:3', 1).otherwise(0) * -0.1809401) + (F.when(F.col("IBX_ADULTS_NUM_AGG_HHD_binne") == '3:4', 1).otherwise(0) * -0.2181563) + (F.when(F.col("IBX_ADULTS_NUM_AGG_HHD_binne") == '>4', 1).otherwise(0) * -0.2279968) +
    (F.when(F.col("MaritalStatus_binned") == 'GROUP3', 1).otherwise(0) * -0.1509207) + (F.when(F.col("MaritalStatus_binned") == 'GROUP2', 1).otherwise(0) * -0.2676128) +
    (F.when(F.col("Pop_pct_Black_Only_Hisp_binned") == '0:1', 1).otherwise(0) * 0.0297458) + (F.when(F.col("Pop_pct_Black_Only_Hisp_binned") == '1:2', 1).otherwise(0) * 0.0747951) + (F.when(F.col("Pop_pct_Black_Only_Hisp_binned") == '2:3', 1).otherwise(0) * 0.1492178) + (F.when(F.col("Pop_pct_Black_Only_Hisp_binned") == '3:6', 1).otherwise(0) * 0.1148509) + (F.when(F.col("Pop_pct_Black_Only_Hisp_binned") == '>6', 1).otherwise(0) * 0.2177879) +
    (F.col("ACEV_Num_X_log") * 0.1506066) +
    (F.when(F.col("PartyMix_binned") == 'GROUP1', 1).otherwise(0) * -0.1002199) + (F.when(F.col("PartyMix_binned") == 'GROUP3', 1).otherwise(0) * 0.0381743) + (F.when(F.col("PartyMix_binned") == 'GROUP4', 1).otherwise(0) * 0.1151927) +
    (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '0:7.7333', 1).otherwise(0) * -0.1838013) + (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '7.7333:20.3467', 1).otherwise(0) * -0.0143219) + (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '>20.3467', 1).otherwise(0) * 0.0263405) +
    (F.when(F.col("VMIS_Num_Act_Flag_binned") == 'GROUP2', 1).otherwise(0) * 0.3485823) +
    (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP2', 1).otherwise(0) * 0.0629817) + (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP3', 1).otherwise(0) * 0.1083392) +
    (F.when(F.col("Hartford_Num_Active_Particpn_bin") == 'GROUP3', 1).otherwise(0) * 0.5089754) + (F.when(F.col("Hartford_Num_Active_Particpn_bin") == 'GROUP1', 1).otherwise(0) * -4.2718308) +
    (F.when(F.col("Past12MoTouchCt_Priv_X_binned") == '0:1', 1).otherwise(0) * -0.0802049) + (F.when(F.col("Past12MoTouchCt_Priv_X_binned") == '1:2', 1).otherwise(0) * -0.1227278) + (F.when(F.col("Past12MoTouchCt_Priv_X_binned") == '>2', 1).otherwise(0) * -0.3073325) +
    (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '0:1', 1).otherwise(0) * 0.1274594) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '1:2', 1).otherwise(0) * 0.0883015) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '2:3', 1).otherwise(0) * 0.119871) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '>3', 1).otherwise(0) * 0.1728288) +
    (F.when(F.col("GE_Num_Active_Particpnts_binned") == 'GROUP1', 1).otherwise(0) * -0.2160246) + (F.col("CurrentPartCt_Overall_X_normaliz") * 0.0562459) +
    (F.when(F.col("Chase_Num_Active_Particpnts_binn") == 'GROUP1', 1).otherwise(0) * -0.1300493))
df_old_masters = df_old_masters.withColumn("p_score30", F.exp(F.col("pred30")) / (1 + F.exp(F.col("pred30"))))

# MODEL 31
df_old_masters = df_old_masters.na.fill({
    "ACEV_Num_X": 0.19040788156007, "age_agg_ind": 69.171197303718, "Age_HH_pct_with_HHer_65_74": 138.22547718966,
    "Age_HH_pct_with_HHer_75_84": 87.192608636794, "Fndn_TTD_Amt_X": 5.5316098736027, "HistPartCt_Overall_X": 0.5212601835061,
    "CENS_HOMVAL_HOME_VALUE_CBSA_INDE": 109.93353188033, "CENS_INC_HH_MEDIAN_HOUSEHOLD_INC": 58864.123162312,
    "infb_trend_telecom_cellular_user": 5.6865280642105, "infb_trend_telecom_internet_user": 6.6462850454267, "MemXRenew": 4.8998428583244,
    "MonthsSinceLastAdvoContrib_X": 7.6036746339351, "MonthsSinceLastOrder": 25.69275392992, "NbrTimesSelEmailedInd": 29.982907396686,
    "No_of_Historic_Particpnts_UHG": 0.11253549472059, "CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL": 205919.20879924, "Past12MoTouchCt_Financial_X": 10.476493355347,
    "Past12MoTouchCt_Health_X": 3.7184886458982, "Past3MoTouchCt_Health_X": 0.37962486805424, "Pop_pct_Asian_Only_": 34.502044131583,
    "CENS_ETHNIC_POP_PERCENT_BLACK_ON": 86.233897387302, "Pop_pct_Black_Only_Hisp": 2.0458372817111, "VoterCount": 8.4994624100571
})
df_old_masters = df_old_masters.withColumn("ACEV_Num_X_log", F.log(F.col("ACEV_Num_X") + 1))
df_old_masters = df_old_masters.withColumn("Age_normalized", (F.col("age_agg_ind") - 69.154478135827) / 9.9271257966606)
df_old_masters = df_old_masters.withColumn("Age_HH_pct_with_HHer_65_74_norma", (F.col("Age_HH_pct_with_HHer_65_74") - 137.99082566921) / 52.326047579)
df_old_masters = df_old_masters.withColumn("Age_HH_pct_with_HHer_75_84_norma", (F.col("Age_HH_pct_with_HHer_75_84") - 86.906229628021) / 47.73607091011)
df_old_masters = df_old_masters.withColumn("Fndn_TTD_Amt_X_log", F.log(F.col("Fndn_TTD_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("HistPartCt_Overall_X_log", F.log(F.col("HistPartCt_Overall_X") + 1))
df_old_masters = df_old_masters.withColumn("HomVal_Home_Value_CBSA_Index_nor", (F.col("CENS_HOMVAL_HOME_VALUE_CBSA_INDE") - 109.57867880844) / 49.176504834964)
df_old_masters = df_old_masters.withColumn("Inc_HH_Median_HH_Income_normaliz", (F.col("CENS_INC_HH_MEDIAN_HOUSEHOLD_INC") - 58743.676831316) / 24457.665579011)
df_old_masters = df_old_masters.withColumn("infb_trend_telecom_cellular__nor", (F.col("infb_trend_telecom_cellular_user") - 5.6867752993541) / 2.8312492840162)
df_old_masters = df_old_masters.withColumn("infb_trend_telecom_internet__nor", (F.col("infb_trend_telecom_internet_user") - 6.6484323255784) / 2.5483400494176)
df_old_masters = df_old_masters.withColumn("MemXRenew_normalized", (F.col("MemXRenew") - 4.9005123959025) / 3.0470451785475)
df_old_masters = df_old_masters.withColumn("MonthsSinceLastAdvoContrib_X_log", F.log(F.col("MonthsSinceLastAdvoContrib_X") + 1.3667))
df_old_masters = df_old_masters.withColumn("MonthsSinceLastOrder_normalized", (F.col("MonthsSinceLastOrder") - 25.523097029038) / 18.52310125529)
df_old_masters = df_old_masters.withColumn("NbrTimesSelEmailedInd_normalized", (F.col("NbrTimesSelEmailedInd") - 29.809119693696) / 57.49682884759)
df_old_masters = df_old_masters.withColumn("No_of_Historic_Particpnts_UH_log", F.log(F.col("No_of_Historic_Particpnts_UHG") + 1))
df_old_masters = df_old_masters.withColumn("OOHU_Median_Home_Value_normalize", (F.col("CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL") - 204959.61692536) / 148180.86709533)
df_old_masters = df_old_masters.withColumn("Past12MoTouchCt_Financial_X_norm", (F.col("Past12MoTouchCt_Financial_X") - 10.453924174215) / 7.7967360321777)
df_old_masters = df_old_masters.withColumn("Past12MoTouchCt_Health_X_normali", (F.col("Past12MoTouchCt_Health_X") - 3.6997613494603) / 4.2478888779226)
df_old_masters = df_old_masters.withColumn("Past3MoTouchCt_Health_X_normaliz", (F.col("Past3MoTouchCt_Health_X") - 0.3760373162662) / 0.77850928781589)
df_old_masters = df_old_masters.withColumn("Pop_pct_Asian_Only__log", F.log(F.col("Pop_pct_Asian_Only_") + 1))
df_old_masters = df_old_masters.withColumn("Pop_pct_Black_Only__log", F.log(F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON") + 1))
df_old_masters = df_old_masters.withColumn("Pop_pct_Black_Only_Hisp_log", F.log(F.col("Pop_pct_Black_Only_Hisp") + 1))
df_old_masters = df_old_masters.withColumn("VoterCount_normalized", (F.col("VoterCount") - 8.4641383539724) / 9.37359914137)
df_old_masters = df_old_masters.withColumn("Chase_Num_Active_Particpnts_binn", F.when(F.col("Chase_Num_Active_Particpnts") == 'BLANK', 'GROUP1').when(F.col("Chase_Num_Active_Particpnts") == '0', 'GROUP2').when(F.col("Chase_Num_Active_Particpnts").isin('1','2'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Gender_binned", F.when(F.col("Gender") == 'BLANK', 'GROUP1').when(F.col("Gender") == 'U', 'GROUP2').when(F.col("Gender") == 'M', 'GROUP3').when(F.col("Gender") == 'F', 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Hartford_Num_Active_Particpn_bin", F.when(F.col("Hartford_Num_Active_Particpnts").isin('BLANK','4'), 'GROUP1').when(F.col("Hartford_Num_Active_Particpnts").isin('0','3'), 'GROUP2').when(F.col("Hartford_Num_Active_Particpnts") == '1', 'GROUP3').when(F.col("Hartford_Num_Active_Particpnts") == '2', 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("home_market_value_binned", F.when(F.col("home_market_value") == 'BLANK', 'GROUP1').when(F.col("home_market_value") == 'S', 'GROUP2').when(F.col("home_market_value").isin('P','M','O','Q','R','K','E','I','D','B','A','N','G','F','J','C','H'), 'GROUP3').when(F.col("home_market_value") == 'L', 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("MaritalStatus_binned", F.when(F.col("MaritalStatus").isin('A','BLANK'), 'GROUP1').when(F.col("MaritalStatus").isin('M','I'), 'GROUP2').when(F.col("MaritalStatus").isin('U','S','B','D','W'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("OriginCode_binned", F.when(F.col("OriginCode").isin('12','BLANK','22'), 'GROUP1').when(F.col("OriginCode").isin('0','7'), 'GROUP2').when(F.col("OriginCode").isin('1','3','2','5','6','9'), 'GROUP3').when(F.col("OriginCode").isin('20','16','13','8'), 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("PartyMix_binned", F.when(F.col("PartyMix") == 'BLANK', 'GROUP1').when(F.col("PartyMix").isin('4','7'), 'GROUP2').when(F.col("PartyMix").isin('U','6'), 'GROUP3').when(F.col("PartyMix").isin('0','5','3','1'), 'GROUP4').when(F.col("PartyMix") == '2', 'GROUP5').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("pred31", 
    -9.594466 +
    (F.col("ACEV_Num_X_log") * 1.6488933) +
    (F.when(F.col("Gender_binned") == 'GROUP3', 1).otherwise(0) * -0.236695) + (F.when(F.col("Gender_binned") == 'GROUP2', 1).otherwise(0) * -0.5072197) + (F.when(F.col("Gender_binned") == 'GROUP1', 1).otherwise(0) * 1.1023555) +
    (F.col("Age_normalized") * 0.2279055) + (F.col("HistPartCt_Overall_X_log") * 0.3065302) + (F.pow(F.col("Pop_pct_Black_Only__log"), 3) * -0.0004117) +
    (F.when(F.col("OriginCode_binned") == 'GROUP3', 1).otherwise(0) * 0.3963609) + (F.when(F.col("OriginCode_binned") == 'GROUP1', 1).otherwise(0) * -0.6075226) + (F.when(F.col("OriginCode_binned") == 'GROUP4', 1).otherwise(0) * 1.506375) +
    (F.col("infb_trend_telecom_cellular__nor") * 0.1254723) + (F.pow(F.col("Age_HH_pct_with_HHer_75_84_norma"), 2) * 0.0140679) + (F.pow(F.col("Age_normalized"), 3) * -0.0205794) +
    (F.when(F.col("Hartford_Num_Active_Particpn_bin") == 'GROUP3', 1).otherwise(0) * 0.1498721) + (F.when(F.col("Hartford_Num_Active_Particpn_bin") == 'GROUP4', 1).otherwise(0) * 0.4048231) + (F.when(F.col("Hartford_Num_Active_Particpn_bin") == 'GROUP1', 1).otherwise(0) * -0.6423962) +
    (F.col("No_of_Historic_Particpnts_UH_log") * -0.2583538) + (F.col("MonthsSinceLastAdvoContrib_X_log") * 0.134437) + (F.pow(F.col("infb_trend_telecom_cellular__nor"), 2) * -0.0437105) +
    (F.pow(F.col("Pop_pct_Asian_Only__log"), 2) * 0.003937) + (F.col("Age_HH_pct_with_HHer_65_74_norma") * 0.056824) +
    (F.when(F.col("home_market_value_binned") == 'GROUP4', 1).otherwise(0) * 0.1564295) + (F.when(F.col("home_market_value_binned") == 'GROUP2', 1).otherwise(0) * -0.381415) + (F.when(F.col("home_market_value_binned") == 'GROUP1', 1).otherwise(0) * 0.1229896) +
    (F.col("Fndn_TTD_Amt_X_log") * 0.0335674) + (F.col("NbrTimesSelEmailedInd_normalized") * 0.041277) + (F.col("VoterCount_normalized") * 0.0398945) +
    (F.when(F.col("MaritalStatus_binned") == 'GROUP3', 1).otherwise(0) * 8.3778504) + (F.when(F.col("MaritalStatus_binned") == 'GROUP2', 1).otherwise(0) * 8.2942746) +
    (F.col("HomVal_Home_Value_CBSA_Index_nor") * -0.0680645) + (F.col("Inc_HH_Median_HH_Income_normaliz") * 0.0387758) + (F.pow(F.col("ACEV_Num_X_log"), 2) * -1.6275227) +
    (F.pow(F.col("infb_trend_telecom_internet__nor"), 2) * 0.0392315) + (F.pow(F.col("MonthsSinceLastAdvoContrib_X_log"), 2) * -0.023851) +
    (F.col("Past12MoTouchCt_Financial_X_norm") * 0.0296052) + (F.pow(F.col("ACEV_Num_X_log"), 3) * 0.5238061) +
    (F.when(F.col("Chase_Num_Active_Particpnts_binn") == 'GROUP3', 1).otherwise(0) * -0.1279983) + (F.col("Pop_pct_Black_Only_Hisp_log") * 0.0371658) +
    (F.pow(F.col("Pop_pct_Black_Only__log"), 4) * 0.0003195) + (F.col("MonthsSinceLastOrder_normalized") * 0.0282256) +
    (F.when(F.col("PartyMix_binned") == 'GROUP5', 1).otherwise(0) * 0.0670129) + (F.when(F.col("PartyMix_binned") == 'GROUP2', 1).otherwise(0) * -0.1492488) + (F.when(F.col("PartyMix_binned") == 'GROUP3', 1).otherwise(0) * -0.008422) +
    (F.pow(F.col("Past3MoTouchCt_Health_X_normaliz"), 2) * 0.0079304) + (F.pow(F.col("Past12MoTouchCt_Health_X_normali"), 2) * -0.0295011) +
    (F.col("Past12MoTouchCt_Health_X_normali") * 0.055696) + (F.pow(F.col("OOHU_Median_Home_Value_normalize"), 2) * -0.0202778) +
    (F.col("OOHU_Median_Home_Value_normalize") * 0.08864) + (F.pow(F.col("MemXRenew_normalized"), 2) * 0.0290423) +
    (F.pow(F.col("infb_trend_telecom_cellular__nor"), 3) * -0.020831) + (F.col("infb_trend_telecom_internet__nor") * 0.1663573) +
    (F.pow(F.col("infb_trend_telecom_internet__nor"), 3) * -0.0909724) + (F.pow(F.col("infb_trend_telecom_internet__nor"), 4) * -0.0398221))
df_old_masters = df_old_masters.withColumn("p_score31", F.exp(F.col("pred31")) / (1 + F.exp(F.col("pred31"))))

# MODEL 33
df_old_masters = df_old_masters.na.fill({
    "Advo_TTD_Amt_X": 7.0002165263756, "age_agg_ind": 69.171197303718, "Fndn_Last_Amt_X": 1.6503369691721, "MemXRenew": 4.8998428583244,
    "MonthsSinceLastAdvoContrib_X": 7.6036746339351, "NbrTimesSelEmailedInd": 29.982907396686, "No_of_Historic_Particpnts_UHG": 0.11253549472059,
    "Past3MoTouchCt_Financial_X": 1.2420223563483, "Past3MoTouchCt_Health_X": 0.37962486805424, "Pop_pct_Asian_Only_Hisp": 0.50965209151212, "VoterCount": 8.4994624100571
})
df_old_masters = df_old_masters.withColumn("Advo_TTD_Amt_X_log", F.log(F.col("Advo_TTD_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("Fndn_Last_Amt_X_log", F.log(F.col("Fndn_Last_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("No_of_Historic_Particpnts_UH_log", F.log(F.col("No_of_Historic_Particpnts_UHG") + 1))
df_old_masters = df_old_masters.withColumn("Pop_pct_Asian_Only_Hisp_log", F.log(F.col("Pop_pct_Asian_Only_Hisp") + 1))
df_old_masters = df_old_masters.withColumn("IBX_EDUCATION_binned", F.when(F.col("IBX_EDUCATION") == '3', 'GROUP1').when(F.col("IBX_EDUCATION").isin('BLANK','2'), 'GROUP2').when(F.col("IBX_EDUCATION").isin('1','4'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Gender_binned", F.when(F.col("Gender").isin('M','BLANK'), 'GROUP1').when(F.col("Gender").isin('F','U'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("home_market_value_binned", F.when(F.col("home_market_value").isin('S','Q','R','O','P','M','K','L'), 'GROUP1').when(F.col("home_market_value").isin('A','I','N','J','H','F','D','G','BLANK'), 'GROUP2').when(F.col("home_market_value").isin('C','B','E'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("OriginCode_binned", F.when(F.col("OriginCode").isin('13','5','9','16','7','22','2','12','6','20'), 'GROUP1').when(F.col("OriginCode").isin('BLANK','1','0','3','8'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("PartyMix_binned", F.when(F.col("PartyMix").isin('4','7'), 'GROUP1').when(F.col("PartyMix").isin('1','5','BLANK'), 'GROUP2').when(F.col("PartyMix").isin('0','3','U','6'), 'GROUP3').when(F.col("PartyMix") == '2', 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("VMIS_Num_Act_Flag_binned", F.when(F.col("VMIS_Num_Act_Flag") == '0', 'GROUP1').when(F.col("VMIS_Num_Act_Flag") == '1', 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("pred33", 
    0.327334 +
    (F.when(F.col("Age_binned") == '57:60', 1).otherwise(0) * 0.0563966) + (F.when(F.col("Age_binned") == '60:63', 1).otherwise(0) * -0.0170126) + (F.when(F.col("Age_binned") == '63:65', 1).otherwise(0) * 0.0540492) + (F.when(F.col("Age_binned") == '65:68', 1).otherwise(0) * -0.0770418) + (F.when(F.col("Age_binned") == '68:71', 1).otherwise(0) * -0.1418374) + (F.when(F.col("Age_binned") == '71:74', 1).otherwise(0) * -0.17602) + (F.when(F.col("Age_binned") == '74:78', 1).otherwise(0) * -0.3487463) + (F.when(F.col("Age_binned") == '78:83', 1).otherwise(0) * -0.5218416) + (F.when(F.col("Age_binned") == '>83', 1).otherwise(0) * -0.7384903) +
    (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '0:7.7333', 1).otherwise(0) * 0.3365485) + (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '7.7333:20.3467', 1).otherwise(0) * 0.3633018) + (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '>20.3467', 1).otherwise(0) * 0.1889714) +
    (F.when(F.col("Gender_binned") == 'GROUP1', 1).otherwise(0) * -0.2472598) +
    (F.when(F.col("home_market_value_binned") == 'GROUP3', 1).otherwise(0) * 0.0723856) + (F.when(F.col("home_market_value_binned") == 'GROUP1', 1).otherwise(0) * -0.1255974) +
    (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP2', 1).otherwise(0) * -0.0536247) + (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP1', 1).otherwise(0) * -0.165668) +
    (F.when(F.col("PartyMix_binned") == 'GROUP2', 1).otherwise(0) * -0.0002462) + (F.when(F.col("PartyMix_binned") == 'GROUP4', 1).otherwise(0) * 0.1234709) + (F.when(F.col("PartyMix_binned") == 'GROUP1', 1).otherwise(0) * -0.1832421) +
    (F.pow(F.col("Fndn_Last_Amt_X_log"), 2) * -0.023428) + (F.col("No_of_Historic_Particpnts_UH_log") * 0.14806) +
    (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '0:1', 1).otherwise(0) * 0.0640434) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '1:2', 1).otherwise(0) * 0.1069673) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '2:3', 1).otherwise(0) * 0.2307814) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '>3', 1).otherwise(0) * 0.0805195) +
    (F.when(F.col("MemXRenew_binned") == '1:2', 1).otherwise(0) * -0.0124593) + (F.when(F.col("MemXRenew_binned") == '2:3', 1).otherwise(0) * -0.048471) + (F.when(F.col("MemXRenew_binned") == '3:4', 1).otherwise(0) * 0.0645789) + (F.when(F.col("MemXRenew_binned") == '4:4.8998', 1).otherwise(0) * 0.3257424) + (F.when(F.col("MemXRenew_binned") == '4.8998:6', 1).otherwise(0) * 0.1131086) + (F.when(F.col("MemXRenew_binned") == '6:7', 1).otherwise(0) * 0.0441552) + (F.when(F.col("MemXRenew_binned") == '7:9', 1).otherwise(0) * 0.1598944) +
    (F.when(F.col("Past3MoTouchCt_Health_X_binned") == '0:1', 1).otherwise(0) * 0.0493782) + (F.when(F.col("Past3MoTouchCt_Health_X_binned") == '1:2', 1).otherwise(0) * 0.1399812) + (F.when(F.col("Past3MoTouchCt_Health_X_binned") == '>2', 1).otherwise(0) * 0.2424806) +
    (F.pow(F.col("Advo_TTD_Amt_X_log"), 2) * 0.0094109) +
    (F.when(F.col("NbrTimesSelEmailedInd_binned") == '0:1', 1).otherwise(0) * -0.0563546) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '1:10', 1).otherwise(0) * 0.1494713) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '10:40', 1).otherwise(0) * 0.028353) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '40:139', 1).otherwise(0) * 0.1348043) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '>139', 1).otherwise(0) * 0.1139462) +
    (F.when(F.col("VoterCount_binned") == '0:1', 1).otherwise(0) * 0.0582764) + (F.when(F.col("VoterCount_binned") == '1:3', 1).otherwise(0) * -0.0103733) + (F.when(F.col("VoterCount_binned") == '3:5', 1).otherwise(0) * -0.0151767) + (F.when(F.col("VoterCount_binned") == '5:8', 1).otherwise(0) * 0.0020521) + (F.when(F.col("VoterCount_binned") == '8:12', 1).otherwise(0) * -0.0982954) + (F.when(F.col("VoterCount_binned") == '12:16', 1).otherwise(0) * -0.1336873) + (F.when(F.col("VoterCount_binned") == '16:23', 1).otherwise(0) * -0.1093828) + (F.when(F.col("VoterCount_binned") == '>23', 1).otherwise(0) * -0.1841244) +
    (F.when(F.col("OriginCode_binned") == 'GROUP1', 1).otherwise(0) * -0.4841592) + (F.when(F.col("VMIS_Num_Act_Flag_binned") == 'GROUP2', 1).otherwise(0) * -0.2696426) +
    (F.pow(F.col("Pop_pct_Asian_Only_Hisp_log"), 2) * -0.0275947))
df_old_masters = df_old_masters.withColumn("p_score33", F.exp(F.col("pred33")) / (1 + F.exp(F.col("pred33"))))

# MODEL 34
df_old_masters = df_old_masters.na.fill({
    "Advo_TTD_Amt_X": 7.0002165263756, "age_agg_ind": 69.171197303718, "CurrentPartCt_Overall_X": 0.27918369556392,
    "MonthsSinceLastFndnContrib_X": 2.8837758031775, "NbrTimesSelEmailedInd": 29.982907396686, "Pop_pct_Asian_Only_Hisp": 0.50965209151212,
    "CENS_ETHNIC_POP_PERCENT_BLACK_ON": 86.233897387302, "VoterCount": 8.4994624100571
})
df_old_masters = df_old_masters.withColumn("Advo_TTD_Amt_X_log", F.log(F.col("Advo_TTD_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("CurrentPartCt_Overall_X_normaliz", (F.col("CurrentPartCt_Overall_X") - 0.27542441828931) / 0.60379632322646)
df_old_masters = df_old_masters.withColumn("MonthsSinceLastFndnContrib_X_log", F.log(F.col("MonthsSinceLastFndnContrib_X") + 1.3667))
df_old_masters = df_old_masters.withColumn("Pop_pct_Asian_Only_Hisp_log", F.log(F.col("Pop_pct_Asian_Only_Hisp") + 1))
df_old_masters = df_old_masters.withColumn("IBX_EDUCATION_binned", F.when(F.col("IBX_EDUCATION") == '1', 'GROUP1').when(F.col("IBX_EDUCATION").isin('2','3','4'), 'GROUP2').when(F.col("IBX_EDUCATION") == 'BLANK', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("home_market_value_binned", F.when(F.col("home_market_value").isin('A','I','G','J','L','E','P','K','Q','C','F','O'), 'GROUP1').when(F.col("home_market_value").isin('D','M','B','H','S','R','N'), 'GROUP2').when(F.col("home_market_value") == 'BLANK', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("MaritalStatus_binned", F.when(F.col("MaritalStatus").isin('A','M','W'), 'GROUP1').when(F.col("MaritalStatus").isin('I','S','BLANK'), 'GROUP2').when(F.col("MaritalStatus").isin('B','D','U'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("NYL_Num_Active_Particpnts_binned", F.when(F.col("NYL_Num_Active_Particpnts") == '0', 'GROUP1').when(F.col("NYL_Num_Active_Particpnts") == 'BLANK', 'GROUP2').when(F.col("NYL_Num_Active_Particpnts").isin('1','2','3'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("OriginCode_binned", F.when(F.col("OriginCode").isin('12','13','16','7','8','5','0'), 'GROUP1').when(F.col("OriginCode").isin('BLANK','22','3','20'), 'GROUP2').when(F.col("OriginCode").isin('1','2','6','9'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("voter_party_input_binned", F.when(F.col("voter_party_input").isin('R','I'), 'GROUP1').when(F.col("voter_party_input") == 'V', 'GROUP2').when(F.col("voter_party_input") == 'BLANK', 'GROUP3').when(F.col("voter_party_input") == 'D', 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("pred34", 
    -0.5196935 +
    (F.when(F.col("Age_binned") == '57:60', 1).otherwise(0) * -0.3714036) + (F.when(F.col("Age_binned") == '60:63', 1).otherwise(0) * -0.8109004) + (F.when(F.col("Age_binned") == '63:65', 1).otherwise(0) * -1.1458062) + (F.when(F.col("Age_binned") == '65:68', 1).otherwise(0) * -1.4821366) + (F.when(F.col("Age_binned") == '68:71', 1).otherwise(0) * -1.7990256) + (F.when(F.col("Age_binned") == '71:74', 1).otherwise(0) * -2.1059967) + (F.when(F.col("Age_binned") == '74:78', 1).otherwise(0) * -2.5825154) + (F.when(F.col("Age_binned") == '78:83', 1).otherwise(0) * -3.0071829) + (F.when(F.col("Age_binned") == '>83', 1).otherwise(0) * -3.2888644) +
    (F.when(F.col("Pop_pct_Black_Only__binned") == '2:4', 1).otherwise(0) * -0.054825) + (F.when(F.col("Pop_pct_Black_Only__binned") == '4:6', 1).otherwise(0) * 0.0338065) + (F.when(F.col("Pop_pct_Black_Only__binned") == '6:10', 1).otherwise(0) * 0.0192314) + (F.when(F.col("Pop_pct_Black_Only__binned") == '10:16', 1).otherwise(0) * -0.0129855) + (F.when(F.col("Pop_pct_Black_Only__binned") == '16:27', 1).otherwise(0) * 0.070412) + (F.when(F.col("Pop_pct_Black_Only__binned") == '27:46', 1).otherwise(0) * 0.0512091) + (F.when(F.col("Pop_pct_Black_Only__binned") == '46:91', 1).otherwise(0) * 0.1392305) + (F.when(F.col("Pop_pct_Black_Only__binned") == '91:239', 1).otherwise(0) * 0.1975342) + (F.when(F.col("Pop_pct_Black_Only__binned") == '>239', 1).otherwise(0) * 0.5164908) +
    (F.when(F.col("MaritalStatus_binned") == 'GROUP3', 1).otherwise(0) * 0.2668731) + (F.when(F.col("MaritalStatus_binned") == 'GROUP2', 1).otherwise(0) * 0.1785944) +
    (F.when(F.col("OriginCode_binned") == 'GROUP3', 1).otherwise(0) * 0.4479168) + (F.when(F.col("OriginCode_binned") == 'GROUP2', 1).otherwise(0) * 0.1960752) +
    (F.when(F.col("NbrTimesSelEmailedInd_binned") == '0:1', 1).otherwise(0) * -0.0566808) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '1:10', 1).otherwise(0) * 0.2101086) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '10:40', 1).otherwise(0) * 0.1036747) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '40:139', 1).otherwise(0) * 0.211528) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '>139', 1).otherwise(0) * 0.3072708) +
    (F.when(F.col("VoterCount_binned") == '0:1', 1).otherwise(0) * -0.0708156) + (F.when(F.col("VoterCount_binned") == '1:3', 1).otherwise(0) * -0.0814804) + (F.when(F.col("VoterCount_binned") == '3:5', 1).otherwise(0) * -0.1593004) + (F.when(F.col("VoterCount_binned") == '5:8', 1).otherwise(0) * -0.1109026) + (F.when(F.col("VoterCount_binned") == '8:12', 1).otherwise(0) * -0.2490872) + (F.when(F.col("VoterCount_binned") == '12:16', 1).otherwise(0) * -0.3032703) + (F.when(F.col("VoterCount_binned") == '16:23', 1).otherwise(0) * -0.2183514) + (F.when(F.col("VoterCount_binned") == '>23', 1).otherwise(0) * -0.3768676) +
    (F.when(F.col("voter_party_input_binned") == 'GROUP1', 1).otherwise(0) * -0.2474163) + (F.when(F.col("voter_party_input_binned") == 'GROUP2', 1).otherwise(0) * -0.0864028) + (F.when(F.col("voter_party_input_binned") == 'GROUP3', 1).otherwise(0) * -0.0961684) +
    (F.when(F.col("NYL_Num_Active_Particpnts_binned") == 'GROUP3', 1).otherwise(0) * 0.308301) + (F.when(F.col("NYL_Num_Active_Particpnts_binned") == 'GROUP2', 1).otherwise(0) * 0.4361555) +
    (F.col("Pop_pct_Asian_Only_Hisp_log") * 0.1264006) +
    (F.when(F.col("home_market_value_binned") == 'GROUP2', 1).otherwise(0) * 0.1191344) + (F.when(F.col("home_market_value_binned") == 'GROUP3', 1).otherwise(0) * 0.0464486) +
    (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP2', 1).otherwise(0) * 0.0929939) + (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP3', 1).otherwise(0) * 0.1436663) +
    (F.pow(F.col("CurrentPartCt_Overall_X_normaliz"), 2) * -0.0141396) + (F.pow(F.col("MonthsSinceLastFndnContrib_X_log"), 2) * 0.0141433) +
    (F.pow(F.col("Advo_TTD_Amt_X_log"), 2) * -0.0095008))
df_old_masters = df_old_masters.withColumn("p_score34", F.exp(F.col("pred34")) / (1 + F.exp(F.col("pred34"))))

# MODEL 35
df_old_masters = df_old_masters.na.fill({
    "ACEV_Num_X": 0.19040788156007, "age_agg_ind": 69.171197303718, "MonthsSinceLastFndnContrib_X": 2.8837758031775, "MonthsSinceLastOrder": 25.69275392992,
    "NbrTimesSelEmailedInd": 29.982907396686, "OCCHU_Median_Length_of_Residence": 875.16989305537, "Past12MoTouchCt_AARP_X": 13.66313909113,
    "Past12MoTouchCt_Financial_X": 10.476493355347, "Past12MoTouchCt_Health_X": 3.7184886458982, "Past12MoTouchCt_Priv_X": 0.67039272471378,
    "Past3MoTouchCt_Financial_X": 1.2420223563483, "Pop_pct_Asian_Only_Hisp": 0.50965209151212, "CENS_ETHNIC_POP_PERCENT_BLACK_ON": 86.233897387302,
    "vehicle_truck_motorcycle_rv": 29.974820739107, "VoterCount": 8.4994624100571
})
df_old_masters = df_old_masters.withColumn("ACEV_Num_X_log", F.log(F.col("ACEV_Num_X") + 1))
df_old_masters = df_old_masters.withColumn("MonthsSinceLastFndnContrib_X_log", F.log(F.col("MonthsSinceLastFndnContrib_X") + 1.3667))
df_old_masters = df_old_masters.withColumn("Pop_pct_Asian_Only_Hisp_log", F.log(F.col("Pop_pct_Asian_Only_Hisp") + 1))
df_old_masters = df_old_masters.withColumn("IBX_EDUCATION_binned", F.when(F.col("IBX_EDUCATION") == '3', 'GROUP1').when(F.col("IBX_EDUCATION").isin('2','1'), 'GROUP2').when(F.col("IBX_EDUCATION").isin('BLANK','4'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("MaritalStatus_binned", F.when(F.col("MaritalStatus").isin('A','M','I','D'), 'GROUP1').when(F.col("MaritalStatus").isin('S','BLANK'), 'GROUP2').when(F.col("MaritalStatus").isin('U','B','W'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("NYL_Num_Active_Particpnts_binned", F.when(F.col("NYL_Num_Active_Particpnts").isin('3','0'), 'GROUP1').when(F.col("NYL_Num_Active_Particpnts").isin('BLANK','2'), 'GROUP2').when(F.col("NYL_Num_Active_Particpnts") == '1', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("OriginCode_binned", F.when(F.col("OriginCode").isin('16','7','6','22','0'), 'GROUP1').when(F.col("OriginCode").isin('BLANK','1'), 'GROUP2').when(F.col("OriginCode").isin('3','20','2','12','9','5','13','8'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("PartyMix_binned", F.when(F.col("PartyMix") == '4', 'GROUP1').when(F.col("PartyMix") == '1', 'GROUP2').when(F.col("PartyMix").isin('5','3','7'), 'GROUP3').when(F.col("PartyMix") == '0', 'GROUP4').when(F.col("PartyMix").isin('U','6'), 'GROUP5').when(F.col("PartyMix").isin('2','BLANK'), 'GROUP6').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("VMIS_Flag_binned", F.when(F.col("VMIS_Flag") == '0', 'GROUP1').when(F.col("VMIS_Flag") == '1', 'GROUP2').when(F.col("VMIS_Flag") == 'BLANK', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("VMIS_Num_Act_Flag_binned", F.when(F.col("VMIS_Num_Act_Flag") == '0', 'GROUP1').when(F.col("VMIS_Num_Act_Flag") == '1', 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("OCCHU_Median_Length_of_Resid_bin", F.when(F.col("OCCHU_Median_Length_of_Residence") <= 500, '<=500').when(F.col("OCCHU_Median_Length_of_Residence") <= 600, '500:600').when(F.col("OCCHU_Median_Length_of_Residence") <= 700, '600:700').when(F.col("OCCHU_Median_Length_of_Residence") <= 800, '700:800').when(F.col("OCCHU_Median_Length_of_Residence") <= 900, '800:900').when(F.col("OCCHU_Median_Length_of_Residence") <= 1000, '900:1000').when(F.col("OCCHU_Median_Length_of_Residence") <= 1100, '1000:1100').when(F.col("OCCHU_Median_Length_of_Residence") <= 1300, '1100:1300').otherwise('>1300'))
df_old_masters = df_old_masters.withColumn("pred35", 
    -1.2307139 +
    (F.when(F.col("Pop_pct_Black_Only__binned") == '2:4', 1).otherwise(0) * 0.0124546) + (F.when(F.col("Pop_pct_Black_Only__binned") == '4:6', 1).otherwise(0) * 0.1588294) + (F.when(F.col("Pop_pct_Black_Only__binned") == '6:10', 1).otherwise(0) * 0.1404007) + (F.when(F.col("Pop_pct_Black_Only__binned") == '10:16', 1).otherwise(0) * 0.1336469) + (F.when(F.col("Pop_pct_Black_Only__binned") == '16:27', 1).otherwise(0) * 0.0899027) + (F.when(F.col("Pop_pct_Black_Only__binned") == '27:46', 1).otherwise(0) * 0.1654607) + (F.when(F.col("Pop_pct_Black_Only__binned") == '46:91', 1).otherwise(0) * 0.2441952) + (F.when(F.col("Pop_pct_Black_Only__binned") == '91:239', 1).otherwise(0) * 0.3897195) + (F.when(F.col("Pop_pct_Black_Only__binned") == '>239', 1).otherwise(0) * 0.8287726) +
    (F.when(F.col("Age_binned") == '57:60', 1).otherwise(0) * 0.0241314) + (F.when(F.col("Age_binned") == '60:63', 1).otherwise(0) * -0.034723) + (F.when(F.col("Age_binned") == '63:65', 1).otherwise(0) * -0.0320818) + (F.when(F.col("Age_binned") == '65:68', 1).otherwise(0) * -0.1345668) + (F.when(F.col("Age_binned") == '68:71', 1).otherwise(0) * -0.2312021) + (F.when(F.col("Age_binned") == '71:74', 1).otherwise(0) * -0.2614061) + (F.when(F.col("Age_binned") == '74:78', 1).otherwise(0) * -0.4119476) + (F.when(F.col("Age_binned") == '78:83', 1).otherwise(0) * -0.539831) + (F.when(F.col("Age_binned") == '>83', 1).otherwise(0) * -0.4346094) +
    (F.when(F.col("MaritalStatus_binned") == 'GROUP3', 1).otherwise(0) * 0.3509137) + (F.when(F.col("MaritalStatus_binned") == 'GROUP2', 1).otherwise(0) * 0.1342523) +
    (F.when(F.col("PartyMix_binned") == 'GROUP2', 1).otherwise(0) * -0.0640216) + (F.when(F.col("PartyMix_binned") == 'GROUP6', 1).otherwise(0) * 0.1912625) + (F.when(F.col("PartyMix_binned") == 'GROUP3', 1).otherwise(0) * -0.0384375) + (F.when(F.col("PartyMix_binned") == 'GROUP1', 1).otherwise(0) * -0.3001079) + (F.when(F.col("PartyMix_binned") == 'GROUP5', 1).otherwise(0) * 0.0021422) +
    (F.when(F.col("VMIS_Num_Act_Flag_binned") == 'GROUP2', 1).otherwise(0) * 0.3188129) + (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP1', 1).otherwise(0) * -0.1895236) + (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP3', 1).otherwise(0) * -0.0370099) +
    (F.when(F.col("VoterCount_binned") == '0:1', 1).otherwise(0) * -0.0574212) + (F.when(F.col("VoterCount_binned") == '1:3', 1).otherwise(0) * -0.0538929) + (F.when(F.col("VoterCount_binned") == '3:5', 1).otherwise(0) * -0.1259202) + (F.when(F.col("VoterCount_binned") == '5:8', 1).otherwise(0) * -0.1264224) + (F.when(F.col("VoterCount_binned") == '8:12', 1).otherwise(0) * -0.1618897) + (F.when(F.col("VoterCount_binned") == '12:16', 1).otherwise(0) * -0.1767188) + (F.when(F.col("VoterCount_binned") == '16:23', 1).otherwise(0) * -0.2173947) + (F.when(F.col("VoterCount_binned") == '>23', 1).otherwise(0) * -0.2186165) +
    (F.when(F.col("OriginCode_binned") == 'GROUP2', 1).otherwise(0) * 0.3305797) + (F.when(F.col("OriginCode_binned") == 'GROUP3', 1).otherwise(0) * 0.5487042) +
    (F.when(F.col("Past12MoTouchCt_AARP_X_binned") == '3:5', 1).otherwise(0) * -0.0553833) + (F.when(F.col("Past12MoTouchCt_AARP_X_binned") == '5:6', 1).otherwise(0) * -0.1004396) + (F.when(F.col("Past12MoTouchCt_AARP_X_binned") == '6:8', 1).otherwise(0) * -0.0699824) + (F.when(F.col("Past12MoTouchCt_AARP_X_binned") == '8:10', 1).otherwise(0) * -0.0490722) + (F.when(F.col("Past12MoTouchCt_AARP_X_binned") == '10:12', 1).otherwise(0) * 0.019431) + (F.when(F.col("Past12MoTouchCt_AARP_X_binned") == '12:15', 1).otherwise(0) * 0.0017977) + (F.when(F.col("Past12MoTouchCt_AARP_X_binned") == '15:20', 1).otherwise(0) * 0.023071) + (F.when(F.col("Past12MoTouchCt_AARP_X_binned") == '20:30', 1).otherwise(0) * 0.1002983) + (F.when(F.col("Past12MoTouchCt_AARP_X_binned") == '>30', 1).otherwise(0) * 0.068252) +
    (F.when(F.col("vehicle_truck_motorcycle_rv_binn") == '0:29.9748', 1).otherwise(0) * 0.0949026) + (F.when(F.col("vehicle_truck_motorcycle_rv_binn") == '29.9748:100', 1).otherwise(0) * -0.1620255) + (F.when(F.col("vehicle_truck_motorcycle_rv_binn") == '100:101', 1).otherwise(0) * -0.0655042) + (F.when(F.col("vehicle_truck_motorcycle_rv_binn") == '>101', 1).otherwise(0) * -0.1911235) +
    (F.when(F.col("NYL_Num_Active_Particpnts_binned") == 'GROUP3', 1).otherwise(0) * 0.2602287) + (F.when(F.col("NYL_Num_Active_Particpnts_binned") == 'GROUP2', 1).otherwise(0) * 0.2289779) +
    (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '0:2', 1).otherwise(0) * -0.0531772) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '2:5', 1).otherwise(0) * -0.0174182) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '5:8', 1).otherwise(0) * -0.0764864) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '8:10', 1).otherwise(0) * -0.0094485) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '10:12', 1).otherwise(0) * 0.0102943) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '12:15', 1).otherwise(0) * -0.0240029) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '15:17', 1).otherwise(0) * 0.0472038) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '17:21', 1).otherwise(0) * 0.1232206) + (F.when(F.col("Past12MoTouchCt_Financial_X_binn") == '>21', 1).otherwise(0) * 0.0502613) +
    (F.when(F.col("OCCHU_Median_Length_of_Resid_bin") == '500:600', 1).otherwise(0) * -0.0166098) + (F.when(F.col("OCCHU_Median_Length_of_Resid_bin") == '600:700', 1).otherwise(0) * -0.0648994) + (F.when(F.col("OCCHU_Median_Length_of_Resid_bin") == '700:800', 1).otherwise(0) * -0.0960656) + (F.when(F.col("OCCHU_Median_Length_of_Resid_bin") == '800:900', 1).otherwise(0) * -0.0882048) + (F.when(F.col("OCCHU_Median_Length_of_Resid_bin") == '900:1000', 1).otherwise(0) * -0.0842236) + (F.when(F.col("OCCHU_Median_Length_of_Resid_bin") == '1000:1100', 1).otherwise(0) * -0.1207881) + (F.when(F.col("OCCHU_Median_Length_of_Resid_bin") == '1100:1300', 1).otherwise(0) * 0.015512) + (F.when(F.col("OCCHU_Median_Length_of_Resid_bin") == '>1300', 1).otherwise(0) * 0.1140438) +
    (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '0:1', 1).otherwise(0) * 0.0508834) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '1:2', 1).otherwise(0) * 0.0580981) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '2:4', 1).otherwise(0) * 0.0399479) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '4:6', 1).otherwise(0) * 0.0415633) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '6:7', 1).otherwise(0) * 0.2089038) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '7:10', 1).otherwise(0) * 0.1496551) + (F.when(F.col("Past12MoTouchCt_Health_X_binned") == '>10', 1).otherwise(0) * 0.0428235) +
    (F.when(F.col("MonthsSinceLastOrder_binned") == '5.2:7.9667', 1).otherwise(0) * -0.0297302) + (F.when(F.col("MonthsSinceLastOrder_binned") == '7.9667:10.9667', 1).otherwise(0) * 0.0178097) + (F.when(F.col("MonthsSinceLastOrder_binned") == '10.9667:14.3333', 1).otherwise(0) * -0.0368211) + (F.when(F.col("MonthsSinceLastOrder_binned") == '14.3333:21.0667', 1).otherwise(0) * 0.0144863) + (F.when(F.col("MonthsSinceLastOrder_binned") == '21.0667:27.9', 1).otherwise(0) * -0.0357949) + (F.when(F.col("MonthsSinceLastOrder_binned") == '27.9:36.5', 1).otherwise(0) * -0.1246394) + (F.when(F.col("MonthsSinceLastOrder_binned") == '36.5:44.5333', 1).otherwise(0) * -0.0829091) + (F.when(F.col("MonthsSinceLastOrder_binned") == '44.5333:53.5', 1).otherwise(0) * -0.1349263) + (F.when(F.col("MonthsSinceLastOrder_binned") == '>53.5', 1).otherwise(0) * -0.1869948) +
    (F.col("Pop_pct_Asian_Only_Hisp_log") * 0.0724914) +
    (F.when(F.col("NbrTimesSelEmailedInd_binned") == '0:1', 1).otherwise(0) * -0.1355681) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '1:10', 1).otherwise(0) * 0.0021139) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '10:40', 1).otherwise(0) * 0.0002693) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '40:139', 1).otherwise(0) * 0.1352407) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '>139', 1).otherwise(0) * 0.11123) +
    (F.col("MonthsSinceLastFndnContrib_X_log") * 0.0560583) +
    (F.when(F.col("Past12MoTouchCt_Priv_X_binned") == '0:1', 1).otherwise(0) * 0.0348784) + (F.when(F.col("Past12MoTouchCt_Priv_X_binned") == '1:2', 1).otherwise(0) * 0.0992397) + (F.when(F.col("Past12MoTouchCt_Priv_X_binned") == '>2', 1).otherwise(0) * 0.3386896) +
    (F.when(F.col("VMIS_Flag_binned") == 'GROUP2', 1).otherwise(0) * 0.5771831) + (F.when(F.col("VMIS_Flag_binned") == 'GROUP3', 1).otherwise(0) * -0.5020621) +
    (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '0:1', 1).otherwise(0) * 0.1525902) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '1:2', 1).otherwise(0) * 0.0396127) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '2:3', 1).otherwise(0) * 0.0411617) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '>3', 1).otherwise(0) * 0.008031) +
    (F.col("ACEV_Num_X_log") * -0.4803406) + (F.pow(F.col("ACEV_Num_X_log"), 2) * 0.4746391))
df_old_masters = df_old_masters.withColumn("p_score35", F.exp(F.col("pred35")) / (1 + F.exp(F.col("pred35"))))

# MODEL 38
df_old_masters = df_old_masters.na.fill({
    "ACEV_Num_X": 0.19040788156007, "Advo_TTD_Amt_X": 7.0002165263756, "age_agg_ind": 69.171197303718, "CurrentPartCt_Overall_X": 0.27918369556392,
    "infb_trend_telecom_cellular_user": 5.6865280642105, "NbrTimesSelEmailedInd": 29.982907396686, "No_of_Historic_Particpnts_UHG": 0.11253549472059,
    "CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL": 205919.20879924, "Pop_pct_Black_Only_Hisp": 2.0458372817111, "vehicle_truck_motorcycle_rv": 29.974820739107
})
df_old_masters = df_old_masters.withColumn("ACEV_Num_X_log", F.log(F.col("ACEV_Num_X") + 1))
df_old_masters = df_old_masters.withColumn("Advo_TTD_Amt_X_log", F.log(F.col("Advo_TTD_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("CurrentPartCt_Overall_X_normaliz", (F.col("CurrentPartCt_Overall_X") - 0.27542441828931) / 0.60379632322646)
df_old_masters = df_old_masters.withColumn("No_of_Historic_Particpnts_UH_log", F.log(F.col("No_of_Historic_Particpnts_UHG") + 1))
df_old_masters = df_old_masters.withColumn("Chase_Num_Active_Particpnts_binn", F.when(F.col("Chase_Num_Active_Particpnts").isin('1','BLANK'), 'GROUP1').when(F.col("Chase_Num_Active_Particpnts").isin('0','2'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("IBX_EDUCATION_binned", F.when(F.col("IBX_EDUCATION") == '1', 'GROUP1').when(F.col("IBX_EDUCATION") == 'BLANK', 'GROUP2').when(F.col("IBX_EDUCATION").isin('2','4'), 'GROUP3').when(F.col("IBX_EDUCATION") == '3', 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Hartford_Num_Active_Particpn_bin", F.when(F.col("Hartford_Num_Active_Particpnts").isin('4','3','2','BLANK','1'), 'GROUP1').when(F.col("Hartford_Num_Active_Particpnts") == '0', 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("MaritalStatus_binned", F.when(F.col("MaritalStatus").isin('D','BLANK','S'), 'GROUP1').when(F.col("MaritalStatus").isin('M','B','I','W'), 'GROUP2').when(F.col("MaritalStatus").isin('U','A'), 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("NYL_Num_Active_Particpnts_binned", F.when(F.col("NYL_Num_Active_Particpnts").isin('3','BLANK','1','0'), 'GROUP1').when(F.col("NYL_Num_Active_Particpnts") == '2', 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("PartyMix_binned", F.when(F.col("PartyMix").isin('1','BLANK'), 'GROUP1').when(F.col("PartyMix").isin('0','3'), 'GROUP2').when(F.col("PartyMix").isin('U','2','4'), 'GROUP3').when(F.col("PartyMix").isin('5','6','7'), 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("VoterStatus_binned", F.when(F.col("Voterstatus").isin('unregistered','BLANK','unmatchedMember','active','multipleAppearances','inactive'), 'GROUP1').when(F.col("Voterstatus") == 'dropped', 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("voter_party_input_binned", F.when(F.col("voter_party_input") == 'BLANK', 'GROUP1').when(F.col("voter_party_input").isin('R','V','I'), 'GROUP2').when(F.col("voter_party_input") == 'D', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("OOHU_Median_Home_Value_binned", F.when(F.col("CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL") <= 75555, '<=75555').when(F.col("CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL") <= 98555.6, '75555:98555.6').when(F.col("CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL") <= 121994.8, '98555.6:121994.8').when(F.col("CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL") <= 147063.6, '121994.8:147063.6').when(F.col("CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL") <= 167856, '147063.6:167856').when(F.col("CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL") <= 192831.2, '167856:192831.2').when(F.col("CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL") <= 226554.8, '192831.2:226554.8').when(F.col("CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL") <= 274601.8, '226554.8:274601.8').when(F.col("CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL") <= 374041, '274601.8:374041').otherwise('>374041'))
df_old_masters = df_old_masters.withColumn("pred38", 
    -1.7155132 +
    (F.when(F.col("Age_binned") == '57:60', 1).otherwise(0) * -0.1463811) + (F.when(F.col("Age_binned") == '60:63', 1).otherwise(0) * -0.1691207) + (F.when(F.col("Age_binned") == '63:65', 1).otherwise(0) * -0.3117623) + (F.when(F.col("Age_binned") == '65:68', 1).otherwise(0) * -0.4141047) + (F.when(F.col("Age_binned") == '68:71', 1).otherwise(0) * -0.6776778) + (F.when(F.col("Age_binned") == '71:74', 1).otherwise(0) * -0.8369032) + (F.when(F.col("Age_binned") == '74:78', 1).otherwise(0) * -1.2231101) + (F.when(F.col("Age_binned") == '78:83', 1).otherwise(0) * -1.6948679) + (F.when(F.col("Age_binned") == '>83', 1).otherwise(0) * -2.059338) +
    (F.when(F.col("NbrTimesSelEmailedInd_binned") == '0:1', 1).otherwise(0) * 0.1361111) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '1:10', 1).otherwise(0) * 0.5114908) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '10:40', 1).otherwise(0) * 0.5838634) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '40:139', 1).otherwise(0) * 0.7888237) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '>139', 1).otherwise(0) * 0.9496873) +
    (F.when(F.col("infb_trend_telecom_cellular__bin") == '2:3', 1).otherwise(0) * -0.0847757) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '3:4', 1).otherwise(0) * -0.1263354) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '4:5', 1).otherwise(0) * -0.1835441) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '5:6', 1).otherwise(0) * -0.0110825) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '6:7', 1).otherwise(0) * -0.307314) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '7:8', 1).otherwise(0) * -0.3208981) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '8:9', 1).otherwise(0) * -0.4108512) + (F.when(F.col("infb_trend_telecom_cellular__bin") == '9:10', 1).otherwise(0) * -0.8334686) +
    (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP3', 1).otherwise(0) * 0.243081) + (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP4', 1).otherwise(0) * 0.37523) + (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP2', 1).otherwise(0) * 0.1617584) +
    (F.when(F.col("voter_party_input_binned") == 'GROUP2', 1).otherwise(0) * -0.1615316) + (F.when(F.col("voter_party_input_binned") == 'GROUP1', 1).otherwise(0) * -0.2558745) +
    (F.when(F.col("vehicle_truck_motorcycle_rv_binn") == '0:29.9748', 1).otherwise(0) * -0.1922653) + (F.when(F.col("vehicle_truck_motorcycle_rv_binn") == '29.9748:100', 1).otherwise(0) * -0.1221579) + (F.when(F.col("vehicle_truck_motorcycle_rv_binn") == '100:101', 1).otherwise(0) * -0.0861769) + (F.when(F.col("vehicle_truck_motorcycle_rv_binn") == '>101', 1).otherwise(0) * -0.3184232) +
    (F.when(F.col("MaritalStatus_binned") == 'GROUP2', 1).otherwise(0) * -0.321592) + (F.when(F.col("MaritalStatus_binned") == 'GROUP1', 1).otherwise(0) * -0.4198165) +
    (F.when(F.col("Pop_pct_Black_Only_Hisp_binned") == '0:1', 1).otherwise(0) * -0.0790026) + (F.when(F.col("Pop_pct_Black_Only_Hisp_binned") == '1:2', 1).otherwise(0) * 0.1695048) + (F.when(F.col("Pop_pct_Black_Only_Hisp_binned") == '2:3', 1).otherwise(0) * 0.1266705) + (F.when(F.col("Pop_pct_Black_Only_Hisp_binned") == '3:6', 1).otherwise(0) * 0.1281003) + (F.when(F.col("Pop_pct_Black_Only_Hisp_binned") == '>6', 1).otherwise(0) * 0.208349) +
    (F.col("ACEV_Num_X_log") * 0.2814033) +
    (F.when(F.col("PartyMix_binned") == 'GROUP1', 1).otherwise(0) * -0.0647851) + (F.when(F.col("PartyMix_binned") == 'GROUP3', 1).otherwise(0) * 0.0821867) + (F.when(F.col("PartyMix_binned") == 'GROUP4', 1).otherwise(0) * 0.2391802) +
    (F.when(F.col("VoterStatus_binned") == 'GROUP2', 1).otherwise(0) * 0.3041411) + (F.col("Advo_TTD_Amt_X_log") * -0.047144) +
    (F.when(F.col("OOHU_Median_Home_Value_binned") == '75555:98555.6', 1).otherwise(0) * 0.132108) + (F.when(F.col("OOHU_Median_Home_Value_binned") == '98555.6:121994.8', 1).otherwise(0) * 0.1071965) + (F.when(F.col("OOHU_Median_Home_Value_binned") == '121994.8:147063.6', 1).otherwise(0) * 0.267661) + (F.when(F.col("OOHU_Median_Home_Value_binned") == '147063.6:167856', 1).otherwise(0) * 0.1280447) + (F.when(F.col("OOHU_Median_Home_Value_binned") == '167856:192831.2', 1).otherwise(0) * 0.0912144) + (F.when(F.col("OOHU_Median_Home_Value_binned") == '192831.2:226554.8', 1).otherwise(0) * 0.2741713) + (F.when(F.col("OOHU_Median_Home_Value_binned") == '226554.8:274601.8', 1).otherwise(0) * 0.0975653) + (F.when(F.col("OOHU_Median_Home_Value_binned") == '274601.8:374041', 1).otherwise(0) * 0.2842723) + (F.when(F.col("OOHU_Median_Home_Value_binned") == '>374041', 1).otherwise(0) * 0.2468442) +
    (F.when(F.col("Chase_Num_Active_Particpnts_binn") == 'GROUP1', 1).otherwise(0) * -0.1316102) + (F.when(F.col("NYL_Num_Active_Particpnts_binned") == 'GROUP2', 1).otherwise(0) * 0.5302668) +
    (F.col("CurrentPartCt_Overall_X_normaliz") * -0.061989) + (F.when(F.col("Hartford_Num_Active_Particpn_bin") == 'GROUP1', 1).otherwise(0) * 0.1570264) +
    (F.col("No_of_Historic_Particpnts_UH_log") * 0.1850864))
df_old_masters = df_old_masters.withColumn("p_score38", F.exp(F.col("pred38")) / (1 + F.exp(F.col("pred38"))))

# MODEL 39
df_old_masters = df_old_masters.na.fill({
    "ACEV_Num_X": 0.19040788156007, "Advo_Last_Amt_X": 2.43321514602, "Advo_TTD_Amt_X": 7.0002165263756, "age_agg_ind": 69.171197303718,
    "Fndn_Last_Amt_X": 1.6503369691721, "MemXRenew": 4.8998428583244, "MonthsSinceLastAdvoContrib_X": 7.6036746339351,
    "NbrTimesSelEmailedInd": 29.982907396686, "Overall_Historic_SP_Reltshps": 0.48890359220357, "Past3MoTouchCt_Financial_X": 1.2420223563483,
    "Past3MoTouchCt_Health_X": 0.37962486805424, "VoterCount": 8.4994624100571
})
df_old_masters = df_old_masters.withColumn("ACEV_Num_X_log", F.log(F.col("ACEV_Num_X") + 1))
df_old_masters = df_old_masters.withColumn("Advo_Last_Amt_X_log", F.log(F.col("Advo_Last_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("Advo_TTD_Amt_X_log", F.log(F.col("Advo_TTD_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("Fndn_Last_Amt_X_log", F.log(F.col("Fndn_Last_Amt_X") + 1))
df_old_masters = df_old_masters.withColumn("IBX_EDUCATION_binned", F.when(F.col("IBX_EDUCATION") == '3', 'GROUP1').when(F.col("IBX_EDUCATION").isin('2','BLANK'), 'GROUP2').when(F.col("IBX_EDUCATION") == '1', 'GROUP3').when(F.col("IBX_EDUCATION") == '4', 'GROUP4').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Gender_binned", F.when(F.col("Gender").isin('M','BLANK'), 'GROUP1').when(F.col("Gender").isin('F','U'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("home_market_value_binned", F.when(F.col("home_market_value") == 'S', 'GROUP1').when(F.col("home_market_value").isin('Q','R','P','O','M','L','K'), 'GROUP2').when(F.col("home_market_value").isin('J','I','N','H','F','A'), 'GROUP3').when(F.col("home_market_value").isin('G','D','BLANK','E','C'), 'GROUP4').when(F.col("home_market_value") == 'B', 'GROUP5').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("NYL_Num_Active_Particpnts_binned", F.when(F.col("NYL_Num_Active_Particpnts").isin('BLANK','0'), 'GROUP1').when(F.col("NYL_Num_Active_Particpnts").isin('1','2','3'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("PartyMix_binned", F.when(F.col("PartyMix").isin('7','4','5','1','BLANK'), 'GROUP1').when(F.col("PartyMix").isin('0','3','6','U'), 'GROUP2').when(F.col("PartyMix") == '2', 'GROUP3').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("Past3MoTouchCt_Priv_X_binned", F.when(F.col("Past12MoTouchCt_Priv_X") == '0', 'GROUP1').when(F.col("Past12MoTouchCt_Priv_X") == '1', 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("VMIS_Num_Act_Flag_binned", F.when(F.col("VMIS_Num_Act_Flag") == '0', 'GROUP1').when(F.col("VMIS_Num_Act_Flag") == '1', 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("voter_party_input_binned", F.when(F.col("voter_party_input").isin('R','V'), 'GROUP1').when(F.col("voter_party_input").isin('BLANK','D','I'), 'GROUP2').otherwise('GROUP1'))
df_old_masters = df_old_masters.withColumn("pred39", 
    0.8126984 +
    (F.when(F.col("Age_binned") == '57:60', 1).otherwise(0) * 0.0620558) + (F.when(F.col("Age_binned") == '60:63', 1).otherwise(0) * -0.0505119) + (F.when(F.col("Age_binned") == '63:65', 1).otherwise(0) * 0.0005899) + (F.when(F.col("Age_binned") == '65:68', 1).otherwise(0) * -0.1380772) + (F.when(F.col("Age_binned") == '68:71', 1).otherwise(0) * -0.1736228) + (F.when(F.col("Age_binned") == '71:74', 1).otherwise(0) * -0.1893533) + (F.when(F.col("Age_binned") == '74:78', 1).otherwise(0) * -0.3286345) + (F.when(F.col("Age_binned") == '78:83', 1).otherwise(0) * -0.4793529) + (F.when(F.col("Age_binned") == '>83', 1).otherwise(0) * -0.6706214) +
    (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '0:7.7333', 1).otherwise(0) * 0.3799817) + (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '7.7333:20.3467', 1).otherwise(0) * 0.3823722) + (F.when(F.col("MonthsSinceLastAdvoContrib_X_bin") == '>20.3467', 1).otherwise(0) * 0.1950539) +
    (F.when(F.col("Gender_binned") == 'GROUP1', 1).otherwise(0) * -0.2555545) +
    (F.when(F.col("home_market_value_binned") == 'GROUP5', 1).otherwise(0) * 0.1564684) + (F.when(F.col("home_market_value_binned") == 'GROUP4', 1).otherwise(0) * 0.0769697) + (F.when(F.col("home_market_value_binned") == 'GROUP2', 1).otherwise(0) * -0.1345604) + (F.when(F.col("home_market_value_binned") == 'GROUP1', 1).otherwise(0) * -0.3704212) +
    (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP2', 1).otherwise(0) * -0.100297) + (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP1', 1).otherwise(0) * -0.2184178) + (F.when(F.col("IBX_EDUCATION_binned") == 'GROUP4', 1).otherwise(0) * 0.3470653) +
    (F.when(F.col("voter_party_input_binned") == 'GROUP1', 1).otherwise(0) * -0.0956392) +
    (F.when(F.col("NbrTimesSelEmailedInd_binned") == '0:1', 1).otherwise(0) * -0.1102544) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '1:10', 1).otherwise(0) * 0.166881) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '10:40', 1).otherwise(0) * 0.0980952) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '40:139', 1).otherwise(0) * 0.1350733) + (F.when(F.col("NbrTimesSelEmailedInd_binned") == '>139', 1).otherwise(0) * 0.0787586) +
    (F.when(F.col("Past3MoTouchCt_Health_X_binned") == '0:1', 1).otherwise(0) * 0.0564706) + (F.when(F.col("Past3MoTouchCt_Health_X_binned") == '1:2', 1).otherwise(0) * 0.1504329) + (F.when(F.col("Past3MoTouchCt_Health_X_binned") == '>2', 1).otherwise(0) * 0.1936579) +
    (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '0:1', 1).otherwise(0) * 0.0866709) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '1:2', 1).otherwise(0) * 0.1211762) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '2:3', 1).otherwise(0) * 0.1943592) + (F.when(F.col("Past3MoTouchCt_Financial_X_binne") == '>3', 1).otherwise(0) * 0.180814) +
    (F.when(F.col("NYL_Num_Active_Particpnts_binned") == 'GROUP2', 1).otherwise(0) * 0.1536847) +
    (F.when(F.col("MemXRenew_binned") == '1:2', 1).otherwise(0) * -0.0253745) + (F.when(F.col("MemXRenew_binned") == '2:3', 1).otherwise(0) * -0.0446504) + (F.when(F.col("MemXRenew_binned") == '3:4', 1).otherwise(0) * 0.0405107) + (F.when(F.col("MemXRenew_binned") == '4:4.8998', 1).otherwise(0) * 0.1911479) + (F.when(F.col("MemXRenew_binned") == '4.8998:6', 1).otherwise(0) * 0.0932932) + (F.when(F.col("MemXRenew_binned") == '6:7', 1).otherwise(0) * 0.0462877) + (F.when(F.col("MemXRenew_binned") == '7:9', 1).otherwise(0) * 0.1655242) +
    (F.when(F.col("VoterCount_binned") == '0:1', 1).otherwise(0) * 0.0392553) + (F.when(F.col("VoterCount_binned") == '1:3', 1).otherwise(0) * -0.0475121) + (F.when(F.col("VoterCount_binned") == '3:5', 1).otherwise(0) * -0.0345169) + (F.when(F.col("VoterCount_binned") == '5:8', 1).otherwise(0) * 0.010233) + (F.when(F.col("VoterCount_binned") == '8:12', 1).otherwise(0) * -0.1121774) + (F.when(F.col("VoterCount_binned") == '12:16', 1).otherwise(0) * -0.147768) + (F.when(F.col("VoterCount_binned") == '16:23', 1).otherwise(0) * -0.1294077) + (F.when(F.col("VoterCount_binned") == '>23', 1).otherwise(0) * -0.2050784) +
    (F.when(F.col("VMIS_Num_Act_Flag_binned") == 'GROUP2', 1).otherwise(0) * -0.3395032) + (F.pow(F.col("Advo_TTD_Amt_X_log"), 2) * 0.0219248) +
    (F.pow(F.col("Fndn_Last_Amt_X_log"), 2) * -0.0189501) + (F.when(F.col("PartyMix_binned") == 'GROUP1', 1).otherwise(0) * -0.0015414) +
    (F.when(F.col("PartyMix_binned") == 'GROUP3', 1).otherwise(0) * 0.0972146) + (F.col("ACEV_Num_X_log") * -0.1302811) +
    (F.when(F.col("Overall_Historic_SP_Reltshps_bin") == '0:1', 1).otherwise(0) * 0.0566871) + (F.when(F.col("Overall_Historic_SP_Reltshps_bin") == '1:2', 1).otherwise(0) * 0.1371418) + (F.when(F.col("Overall_Historic_SP_Reltshps_bin") == '>2', 1).otherwise(0) * 0.1307582) +
    (F.when(F.col("Past3MoTouchCt_Priv_X_binned") == 'GROUP2', 1).otherwise(0) * 0.4850236) + (F.pow(F.col("Advo_Last_Amt_X_log"), 2) * -0.0215058))
df_old_masters = df_old_masters.withColumn("p_score39", F.exp(F.col("pred39")) / (1 + F.exp(F.col("pred39"))))

# Final scores rounding
score_cols = [f"p_score{i}" for i in [1, 2, 3, 6, 8, 9, 11, 14, 16, 20, 21, 22, 26, 29, 30, 31, 33, 34, 35, 38, 39]]
for col_name in score_cols:
    n_col_name = col_name.replace("p_score", "nscore")
    df_old_masters = df_old_masters.withColumn(n_col_name, F.round(F.col(col_name), 5))

# Vigintile calculations using joins
vigintile_map = {
    1: "new_nscore1_vigintile", 2: "new_nscore2_vigintile", 3: "new_nscore3_vigintile",
    6: "new_nscore6_vigintile", 8: "new_nscore8_vigintile", 9: "new_nscore9_vigintile",
    11: "new_nscore11_vigintil", 14: "new_nscore14_vigintil", 16: "new_nscore16_vigintil",
    20: "new_nscore20_vigintil", 21: "new_nscore21_vigintil", 22: "new_nscore22_vigintil",
    26: "new_nscore26_vigintil", 29: "new_nscore29_vigintil", 30: "new_nscore30_vigintil",
    31: "new_nscore31_vigintil", 33: "new_nscore33_vigintil", 34: "new_nscore34_vigintil",
    35: "new_nscore35_vigintil", 38: "new_nscore38_vigintil", 39: "new_nscore39_vigintil"
}

for i, fmt_name in vigintile_map.items():
    score_col = f"nscore{i}"
    vigintile_col = f"old_score{i}_vigintile"
    
    # Filter the format data for the current vigintile
    df_format_temp = df_new_masters2012_formats.filter(F.lower(F.col("FMTNAME")) == fmt_name.lower()).alias("fmt")
    
    # Join to map scores to vigintiles
    df_old_masters = df_old_masters.join(
        df_format_temp,
        on=df_old_masters[score_col].between(df_format_temp["START"], df_format_temp["END"]),
        how="left"
    ).withColumn(vigintile_col, F.col("fmt.LABEL").cast(IntegerType())).drop("fmt.FMTNAME", "fmt.START", "fmt.END", "fmt.LABEL")

# Keep final columns
final_cols = ["merkleid"] + [f"old_score{i}_vigintile" for i in vigintile_map.keys()]
df_old_masters = df_old_masters.select(final_cols)

# SQL Join
df_geo_appends_rpm_original = spark.table("intermed.geo_appends_rpm")
df_final_geo_appends_rpm = df_geo_appends_rpm_original.alias("a").join(
    df_old_masters.alias("b"),
    on=df_geo_appends_rpm_original.merkleid == df_old_masters.merkleid,
    how="left"
).select("a.*", *[f"b.{col}" for col in df_old_masters.columns if col != 'merkleid'])

# Save the final table
df_final_geo_appends_rpm.write.format("delta").mode("overwrite").saveAsTable("intermed.geo_appends_rpm")

# Frequencies for diagnostics
print(f"Frequencies - {MULDATE}")
for col_name in final_cols:
    if col_name.startswith("old_score"):
        print(f"Frequency for {col_name}:")
        df_final_geo_appends_rpm.groupBy(col_name).count().orderBy(col_name).show()
#End-DBShift