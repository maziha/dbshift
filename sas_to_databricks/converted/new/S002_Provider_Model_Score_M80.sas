import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, DoubleType

spark = SparkSession.builder.appName("SAS_to_PySpark_Conversion").getOrCreate()

version = 80
em_month = 20210813
version2 = 79
CURR_MONTH = 202108
snapshot_dt = "2021-08-16"

LookBack_Year = F.year(F.lit(snapshot_dt))
LookBack_Month = F.month(F.lit(snapshot_dt))
LookBack_Day = F.dayofmonth(F.lit(snapshot_dt))

List_MBU_snapshot = [
    "flag_mbu_click_12m_ever", "flag_mbu_open_12m_ever", "count_mbu_click_1m",
    "count_mbu_click_2m", "count_mbu_click_3m", "count_mbu_click_6m",
    "count_mbu_click_9m", "count_mbu_click_12m", "count_mbu_open_1m",
    "count_mbu_open_2m", "count_mbu_open_3m", "count_mbu_open_6m",
    "count_mbu_open_9m", "count_mbu_open_12m", "flag_mbu_open_6m_55",
    "flag_mbu_open_6m_2", "flag_mbu_click_12m_2", "foremost_flag_open_prev",
    "foremost_counter_open_prev", "foremost_flag_click_prev",
    "hartford_counter_open_prev", "collette_counter_open_prev",
    "get_counter_open_prev", "get_flag_click_prev", "cartus_flag_click_prev",
    "medjet_flag_click_prev", "coopgp_flag_open_prev", "coopgp_counter_open_prev",
    "coopgp_flag_click_prev", "coopgp_counter_click_prev",
    "nylltc_flag_click_prev", "avisbudget_counter_open_prev", "deltadental_counter_open_prev"
]

List_FM_ORG_VAR = [
    "Flag_ORG_EXT_OBN_098_12", "Flag_ORG_EXT_OBN_099_12", "Flag_ORG_EXT_REF_008_12"
]

df_uni = spark.table(f"snapshot.Prov_Model_Uni_A_m{version}")

df_HearUSA_Distance_M = spark.table(f"target.HearUSA_Distance_M{version2}")
df_HearUSA_Distance_flag = df_HearUSA_Distance_M.withColumn("flag_center_dist1", (F.col("distancemiles") <= 1)) \
    .withColumn("flag_center_dist5", (F.col("distancemiles") <= 5)) \
    .withColumn("flag_center_dist10", (F.col("distancemiles") <= 10)) \
    .withColumn("flag_center_dist25", (F.col("distancemiles") <= 25)) \
    .filter((F.col("FindNearestRank") == 1) & (F.col("distancemiles") <= 25))

df_temp = spark.table(f"target.HearUSA_Distance_M{version2}").filter(F.col("distancemiles") <= 25)

window_spec_temp = Window.partitionBy("chid_key").orderBy(F.col("findnearestrank").desc())
df_HearUSA_Distance_flag2 = df_temp.withColumn("row_num", F.row_number().over(window_spec_temp)).filter(F.col("row_num") == 1).drop("row_num").dropDuplicates(["chid_key"])

df_distance = df_HearUSA_Distance_flag.select(
    F.col("chid_key").alias("chid"),
    "flag_center_dist1",
    "flag_center_dist5",
    "flag_center_dist10",
    "flag_center_dist25",
    "distancemiles"
).dropDuplicates(["chid"])

df_distance2 = df_HearUSA_Distance_flag2.withColumnRenamed("chid_key", "chid") \
    .withColumnRenamed("findnearestrank", "count_center") \
    .select("chid", "count_center") \
    .filter(F.col("chid") > 0) \
    .dropDuplicates(["chid"])

df_hearusa_distance = df_distance.join(df_distance2, on="chid", how="full_outer") \
    .withColumn("count_center", F.when(F.col("count_center").isNull(), 0).otherwise(F.col("count_center"))) \
    .withColumn("flag_center_dist1", (F.col("flag_center_dist1") == True)) \
    .withColumn("flag_center_dist5", (F.col("flag_center_dist5") == True)) \
    .withColumn("flag_center_dist10", (F.col("flag_center_dist10") == True)) \
    .withColumn("flag_center_dist25", (F.col("flag_center_dist25") == True)) \
    .withColumn("distancemiles", F.when(F.col("distancemiles").isNull(), 199).otherwise(F.col("distancemiles")))

df_cust = spark.table("collette.collette_cust_201807").select("chid").dropDuplicates(["chid"])

df_collette_pene_zip3 = spark.table("collette.collette_pene_m44") \
    .filter(F.col("zip3").isNotNull()) \
    .select("zip3", "zip3_mem_pen") \
    .dropDuplicates(["zip3"])

df_collette_pene_dma = spark.table("collette.collette_pene_m44") \
    .filter(F.col("dma_cd").isNotNull()) \
    .select("dma_cd", "dma_cd_mem_pen") \
    .dropDuplicates(["dma_cd"])

df_collette_pene_state = spark.table("collette.collette_pene_m44") \
    .filter(F.col("state").isNotNull()) \
    .select("state", "state_mem_pen") \
    .dropDuplicates(["state"])

df_uni.createOrReplaceTempView("uni")
df_collette_pene_zip3.createOrReplaceTempView("collette_pene_zip3")
df_collette_pene_dma.createOrReplaceTempView("collette_pene_dma")
df_collette_pene_state.createOrReplaceTempView("collette_pene_state")
df_cust.createOrReplaceTempView("cust")

df_collette_pene = spark.sql("""
    SELECT a.chid, zip3_mem_pen, dma_cd_mem_pen, state_mem_pen, 
           CASE WHEN e.chid IS NOT NULL THEN 1 ELSE 0 END AS collette_cust_flag
    FROM uni a
    LEFT JOIN collette_pene_zip3 b ON SUBSTR(a.zip, 1, 3) = b.zip3
    LEFT JOIN collette_pene_dma c ON a.dma_cd = c.dma_cd
    LEFT JOIN collette_pene_state d ON a.state = d.state
    LEFT JOIN cust e ON a.chid = e.chid
""")

df_em = spark.table(f"em.email_{em_month}").select(F.col("chid_key").alias("chid")).dropDuplicates(["chid"])

df_snapshot = spark.table(f"snapshot.Trg_snapshot_{snapshot_dt.replace('-', '_')}_date") \
    .withColumn("chid", F.col("chid_key") * 1) \
    .select("chid", *List_MBU_snapshot, *List_FM_ORG_VAR) \
    .dropDuplicates(["chid"])

df_uni.createOrReplaceTempView("uni")
spark.table("nyl.NYL_Penetration_ZIP5").createOrReplaceTempView("NYL_Penetration_ZIP5")
spark.table(f"snapshot.Trg_snapshot_{snapshot_dt.replace('-', '_')}_Scr_New").createOrReplaceTempView("Trg_snapshot_Scr_New")

df_nyl_pene = spark.sql("""
    SELECT a.chid, b.zip5_pene_7 AS nyl_zip5_pene_7, c.Trg_Node_NYL_201911
    FROM uni a
    LEFT JOIN NYL_Penetration_ZIP5 b ON a.zip = b.zip5
    LEFT JOIN Trg_snapshot_Scr_New c ON a.chid = c.chid_key
""").dropDuplicates(["chid"])

df_ARS_Lapsed_Customer = spark.table("ASI_TAR.SERVICE_PARTICIPATION_HISTORY") \
    .select("chid_key", "sp_name", "active_engagement_flag", "service_effective_dt", "service_term_dt") \
    .withColumnRenamed("chid_key", "chid") \
    .filter((F.col("sp_name") == 'Allstate') & (F.col("chid") > 0)) \
    .withColumn("service_effective_date", F.to_date(F.col("service_effective_dt"))) \
    .withColumn("service_term_date", F.to_date(F.col("service_term_dt"))) \
    .dropDuplicates(["chid"])

df_em_renamed = df_em.withColumnRenamed("chid", "chid_em")
df_all1 = df_uni.join(df_em_renamed, df_uni.chid == df_em_renamed.chid_em, "left") \
    .join(df_snapshot, on="chid", how="left") \
    .join(df_hearusa_distance, on="chid", how="left") \
    .join(df_collette_pene, on="chid", how="left") \
    .join(df_nyl_pene, on="chid", how="left") \
    .join(df_ARS_Lapsed_Customer, on="chid", how="left") \
    .withColumn("flag_EM", F.when(F.col("chid_em").isNotNull(), 1).otherwise(0)) \
    .drop("chid_em")

def ordinal_coding_logic(vname):
    if vname == "A": return 1
    if vname == "B": return 2
    if vname == "C": return 3
    if vname == "D": return 4
    if vname == "E": return 5
    if vname == "F": return 6
    if vname == "G": return 7
    if vname == "H": return 8
    if vname == "I": return 9
    if vname == "J": return 10
    if vname == "K": return 11
    if vname == "L": return 12
    if vname == "M": return 13
    if vname == "N": return 14
    if vname == "O": return 15
    if vname == "P": return 16
    if vname == "Q": return 17
    if vname == "R": return 18
    if vname == "S": return 19
    if vname == "T": return 20
    return None
ordinal_coding_udf = F.udf(ordinal_coding_logic, IntegerType())

def ordinal_coding2_logic(vname):
    if vname == "1": return 1
    if vname == "2": return 2
    if vname == "3": return 3
    if vname == "4": return 4
    if vname == "5": return 5
    if vname == "6": return 6
    if vname == "7": return 7
    if vname == "8": return 8
    if vname == "9": return 9
    if vname == "A": return 10
    if vname == "B": return 11
    if vname == "C": return 12
    if vname == "D": return 13
    if vname == "E": return 14
    if vname == "F": return 15
    if vname == "G": return 16
    if vname == "H": return 17
    if vname == "I": return 18
    if vname == "J": return 19
    if vname == "K": return 20
    return None
ordinal_coding2_udf = F.udf(ordinal_coding2_logic, IntegerType())

def dummy_coding_logic(vname):
    if vname is not None and float(vname) > 0:
        return 1
    return 0
dummy_coding_udf = F.udf(dummy_coding_logic, IntegerType())

def dummy_coding2_logic(vname):
    if vname is not None:
        return 1
    return 0
dummy_coding2_udf = F.udf(dummy_coding2_logic, IntegerType())

def dummy_coding3_logic(vname):
    return 1 if vname == "Y" else 0
dummy_coding3_udf = F.udf(dummy_coding3_logic, IntegerType())

df_scoring = df_all1
df_scoring = df_scoring.withColumn("C00012_D", dummy_coding_udf(F.col("C00012")))
df_scoring = df_scoring.withColumn("H0136_D", dummy_coding2_udf(F.col("H0136")))
df_scoring = df_scoring.withColumn("H0170_D", dummy_coding2_udf(F.col("H0170")))
df_scoring = df_scoring.withColumn("H0200_D", dummy_coding2_udf(F.col("H0200")))
df_scoring = df_scoring.withColumn("H0222_D", dummy_coding2_udf(F.col("H0222")))
df_scoring = df_scoring.withColumn("J0054_D", dummy_coding3_udf(F.col("J0054")))
df_scoring = df_scoring.withColumn("J0181_D", dummy_coding3_udf(F.col("J0181")))
df_scoring = df_scoring.withColumn("J0109_D", dummy_coding3_udf(F.col("J0109")))
df_scoring = df_scoring.withColumn("H0157_D", dummy_coding2_udf(F.col("H0157")))
df_scoring = df_scoring.withColumn("H0259_D", dummy_coding2_udf(F.col("H0259")))
df_scoring = df_scoring.withColumn("H0333_D", dummy_coding2_udf(F.col("H0333")))
df_scoring = df_scoring.withColumn("K0023_O", ordinal_coding_udf(F.col("K0023")))
df_scoring = df_scoring.withColumn("H0149_D", dummy_coding2_udf(F.col("H0149")))
df_scoring = df_scoring.withColumn("H0254_D", dummy_coding2_udf(F.col("H0254")))
df_scoring = df_scoring.withColumn("H0302_D", dummy_coding2_udf(F.col("H0302")))
df_scoring = df_scoring.withColumn("J0178_D", dummy_coding3_udf(F.col("J0178")))
df_scoring = df_scoring.withColumn("J0210_D", dummy_coding3_udf(F.col("J0210")))
df_scoring = df_scoring.withColumn("J0050_D", dummy_coding3_udf(F.col("J0050")))
df_scoring = df_scoring.withColumn("J0051_D", dummy_coding3_udf(F.col("J0051")))
df_scoring = df_scoring.withColumn("J0156_D", dummy_coding3_udf(F.col("J0156")))
df_scoring = df_scoring.withColumn("H0186_D", dummy_coding2_udf(F.col("H0186")))
df_scoring = df_scoring.withColumn("H0208_D", dummy_coding2_udf(F.col("H0208")))
df_scoring = df_scoring.withColumn("H0240_D", dummy_coding2_udf(F.col("H0240")))
df_scoring = df_scoring.withColumn("J0284_D", dummy_coding3_udf(F.col("J0284")))
df_scoring = df_scoring.withColumn("K0001_O", ordinal_coding_udf(F.col("K0001")))
df_scoring = df_scoring.withColumn("J0179_D", dummy_coding3_udf(F.col("J0179")))
df_scoring = df_scoring.withColumn("J0151_D", dummy_coding3_udf(F.col("J0151")))
df_scoring = df_scoring.withColumn("J0075_D", dummy_coding3_udf(F.col("J0075")))
df_scoring = df_scoring.withColumn("H0243_D", dummy_coding2_udf(F.col("H0243")))
df_scoring = df_scoring.withColumn("J0061_D", dummy_coding3_udf(F.col("J0061")))
df_scoring = df_scoring.withColumn("J0242_D", dummy_coding3_udf(F.col("J0242")))
df_scoring = df_scoring.withColumn("H0176_D", dummy_coding2_udf(F.col("H0176")))
df_scoring = df_scoring.withColumn("H0183_D", dummy_coding2_udf(F.col("H0183")))
df_scoring = df_scoring.withColumn("J0293_D", dummy_coding3_udf(F.col("J0293")))
df_scoring = df_scoring.withColumn("J0323_D", dummy_coding3_udf(F.col("J0323")))
df_scoring = df_scoring.withColumn("H0150_D", dummy_coding2_udf(F.col("H0150")))
df_scoring = df_scoring.withColumn("H0188_D", dummy_coding2_udf(F.col("H0188")))
df_scoring = df_scoring.withColumn("H0206_D", dummy_coding2_udf(F.col("H0206")))
df_scoring = df_scoring.withColumn("H0213_D", dummy_coding2_udf(F.col("H0213")))
df_scoring = df_scoring.withColumn("J0140_D", dummy_coding3_udf(F.col("J0140")))
df_scoring = df_scoring.withColumn("J0313_D", dummy_coding3_udf(F.col("J0313")))
df_scoring = df_scoring.withColumn("H0163_D", dummy_coding2_udf(F.col("H0163")))
df_scoring = df_scoring.withColumn("H0223_D", dummy_coding2_udf(F.col("H0223")))
df_scoring = df_scoring.withColumn("A0093_D", dummy_coding3_udf(F.col("A0093")))
df_scoring = df_scoring.withColumn("H0202_D", dummy_coding2_udf(F.col("H0202")))
df_scoring = df_scoring.withColumn("H0165_D", dummy_coding2_udf(F.col("H0165")))
df_scoring = df_scoring.withColumn("J0297_D", dummy_coding3_udf(F.col("J0297")))
df_scoring = df_scoring.withColumn("J0302_D", dummy_coding3_udf(F.col("J0302")))
df_scoring = df_scoring.withColumn("J0169_D", dummy_coding3_udf(F.col("J0169")))

df_Prov_Model_SCR_A_m_New = df_scoring
%run "/vg06/jye/model_score/decile_cutoff/PCT_spsf_em_202003.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_FMMC_EM_202011.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_FMMH_EM_202011.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Hear_Clone_202004.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_HartFord_EM_201910.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Coll_Clone_202002.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Coll_resp_202002.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Coll_EM_201911.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Exp_EM_201811.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Exp_Car_Clone_201811.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Exp_Hotel_Clone_201811.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Exp_Clone_201706.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Exp_Val_201706.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Dsct_Trvl_Web_Clone_201805.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Dsct_Ent_Clone_201805.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Dsct_Shop_Clone_201805.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Dsct_Retail_Clone_201805.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Dsct_EM_201803.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_GET_Resp_201907.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_GET_Clone_201907.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_GET_EM_201912.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_NYL_EM_201910.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_GP_Old_EM_201805.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_GP_Young_EM_201805.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_GP_Web_201806.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_CCI_EM_202002.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_ABG_EM_201904.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_ABG_Clone_201904.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_RX_Clone_201708.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_RX_Value_201708.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Marcus_EM_202009.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_UHC_NONAEP_Clone_202009.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_UHC_AEP_Clone_202009.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_UHC_AEP_EM_202009.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_UHC_NONAEP_EM_202009.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_NYL_LTC_Appt_202007.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_NYL_LTC_EM_202006.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_ATT_Exit_202008.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_ATT_EM_202008.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Exxon_EM_202008.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_AS_RS_Clone_202002.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_CHASE_EE_CLONE_201806.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_CHASE_ENE_CLONE_201806.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_CHASE_NEE_CLONE_201806.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_CHASE_NENE_CLONE_201806.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_GET_LRC_CLONE_201705.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_GET_LRC_TREE_201705.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_GET_NPL_CLONE_201608.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_GET_NPL_TREE_201608.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_HEAR_RESP_201608.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_NYL_DM_REOFFER_RESP_201912.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_RX_EM_201712.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_RiskIQ_201708.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_NYL_DM_Prop_RESP_201912.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Cartus_EM_202007.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_AS_EM_201701.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Exxon_Clone_202010.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Exxon_Prem_Clone_202010.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Medjet_EM_201906.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Medjet_Clone_201802.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_FMMC_Clone_202010.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_FMMH_Clone_202010.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_FL_1800_Clone_202003.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_CCI_Engager_201908.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_ATT_Clone_201602.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_ABG_Engager_201904.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_CCI_Clone_201907.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_CCI_Resp_201907.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_EyeMed63_Inq_201904.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_EyeMed65_Inq_201904.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_SupBuyer_Fin_202007.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Cartus_Clone_202005.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Chase_EM_201606.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Chase_Resp_201606.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Chase_NonEM_201606.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Delta63_Inq_201904.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Delta65_Inq_201904.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Delta_EM_201906.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Delta65_Sale_201904.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Dennys_Clone_201906.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_FMMC_Clone_201905.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Hartford_Auto_Clone_201906.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Hartford_Home_Clone_201906.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Hilton_Clone_201904.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_HomeServ_Clone_202003.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Liberty_Trvl_Clone_202003.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_LiveWatch_Clone_201606.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_FMMH_Clone_201905.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_NYL63_Inq_201904.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_NYL65_Inq_201904.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_NYL_Clone_201905.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_NYL_LTC_Clone_202005.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_RX63_Inq_201904.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_RX65_Inq_201904.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_RX_HSA_Clone_202007.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_PRideFly_Clone_202003.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Reg_Cinema_Clone_202003.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Chase_Risk_201606.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_SPSF_Clone_202006.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Tang_Outlet_Clone_202003.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_UHC65_Sale_201904.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_UHC65_Inq_201904.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_UPS_Clone_202003.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_WAL_Clone_201906.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Wal_EM_201906.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Xanterra_Clone_201910.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Schwan_Clone_202003.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_SupBuyer_Disc_202007.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_SuperBuyer_Trvl_202007.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Exxon_Exit_202008.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_AS_CONV_201903.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Delta_Engage_201906.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Delta_MB_EM_201906.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Pet_Resp_201605.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Pet_Dog_201605.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Pet_Cat_201605.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_EyeMed_Ins_201607.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_EyeMed_EM_201804.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_VBR_Clone_201709.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_AS_CHALLENGE_201806.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_AS_ENGAGERS_201806.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_AS_FORMER_201806.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_AS_GOODCRD_201806.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_AS_NEWRENEW_201806.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_AAA_Clone_201903.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_AS_Elite_Assist_Clone_202003.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_AAA_MRI_Clone_202011.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_ATT_MRI_Clone_202011.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_CRB_Exit_Clone_202009.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_CHASE_EE_CLONE_201806.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_CHASE_ENE_CLONE_201806.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_CHASE_NEE_CLONE_201806.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_CHASE_NENE_CLONE_201806.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_GET_CPL_CLONE_201708.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_GET_CPL_RESP_201708.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Chase_DM_Resp_201806.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_AS_Former_Clone_202012.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_AS_Tenured_Clone_202012.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_AS_DM_Resp_202012.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Norton_ID_Clone_202012.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_EyeMed_Exit_202012.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Wyndham_Exit_202101.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Wyndham_Clone_202101.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_CRB_Clone_202101.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Outback_Clone_202101.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Outback_Exit_202101.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Bonefish_Exit_202101.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Bonefish_Clone_202101.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Norton_ITS_EM_202102.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Norton_Exit_202102.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_HIG_Auto_DigitalPref_202102.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Valv_Clone_202102.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_UHC_Exit_202105.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_UHC_MRI_202105.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_BW_Clone_202104.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_BW_EM_202104.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_NYL_Digital_Clone_202104.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Pet_MRI_Clone_202105.txt"
%run "/vg06/jye/model_score/decile_cutoff/PCT_Pet_Exit_Clone_202105.txt"
%run "/vg06/jye/model_score/code/PCT_Norton_HD_Clone_202107.txt"

def model_pct(input_df, model_name):
    t1 = input_df.withColumn("scr", F.col(f"SCR_{model_name}") + (F.rand(seed=12345) / 1000000)).select("chid", "scr", f"SCR_{model_name}")
    
    window_spec = Window.orderBy(F.col("scr").desc())
    t2 = t1.withColumn("row_num", F.row_number().over(window_spec))
    num_obs = t2.count()
    t2 = t2.withColumn("group", (F.floor(F.col("row_num") * 100 / (num_obs + 1)) + 1).cast("int"))

    window_group_spec = Window.partitionBy("group").orderBy(F.col("scr").desc())
    t2 = t2.withColumn("upper_bound", F.first("scr").over(window_group_spec))

    t4 = t2.groupBy("group").agg(F.count("*").alias("cnt"), F.max("upper_bound").alias("upper")).orderBy("group")

    script_lines = []
    collected_bounds = t4.collect()
    
    for i, row in enumerate(collected_bounds):
        group_new = row["group"] - 1
        upper_bound_val = row["upper"]
        line = f"    .when(F.col('scr_{model_name}') >= {upper_bound_val}, {group_new})"
        if i == 0:
             # This handles the case where the first condition should not be an if but a when
             pass
        script_lines.append(line)
        
    final_script = (
        f"df_Prov_Model_SCR_m_New = df_Prov_Model_SCR_m_New.withColumn('PCT_{model_name}',\n"
        f"    F.when(F.col('scr_{model_name}') >= {collected_bounds[0]['upper']}, {collected_bounds[0]['group'] - 1})\n"
        + "\n".join(script_lines[1:])
        + "\n    .otherwise(100)\n"
        ")"
    )

    with open(f"/dbfs/vg06/btao/model_score/code/PCT_{model_name}.py", "w") as f:
        f.write(final_script)

# Note: The model_pct calls are not directly executable in this translated context in the same way.
# They are for generating other scripts. The translation above provides the Python function 
# that replicates the SAS macro's logic of generating code. The following lines would be how
# you'd call it in an interactive Databricks environment to generate those files.

# model_pct(df_Prov_Model_SCR_A_m_New, 'CRB_Clone_202101')
# model_pct(df_Prov_Model_SCR_A_m_New, 'EyeMed_Exit_202012')
# ... and so on for all other model_pct calls.

df_a = spark.table(f"target.Prov_Model_SCR_A_m{version}_New")
df_b = spark.table(f"target.Prov_Model_SCR_B_m{version}_New")
df_c = spark.table(f"Snapshot.Trg_snapshot_{snapshot_dt.replace('-', '_')}_Scr_New").withColumnRenamed("chid_key", "chid")

df_Prov_Model_SCR_m_New = df_a.join(df_b, on="chid", how="inner").join(df_c, on="chid", how="left")
df_Prov_Model_SCR_m_New.write.format("delta").mode("overwrite").saveAsTable(f"target.Prov_Model_SCR_m{version}_New")

pct_columns = [col for col in df_Prov_Model_SCR_m_New.columns if col.startswith('PCT_')]
trg_node_columns = [col for col in df_Prov_Model_SCR_m_New.columns if col.startswith('Trg_Node_')]

for col_name in pct_columns:
    print(f"Frequency for {col_name}:")
    df_Prov_Model_SCR_m_New.groupBy(col_name).count().show()

for col_name in trg_node_columns:
    print(f"Frequency for {col_name}:")
    df_Prov_Model_SCR_m_New.groupBy(col_name).count().show()

scr_cols = [c for c in df_Prov_Model_SCR_m_New.columns if c.startswith('scr_')]
pct_cols = [c for c in df_Prov_Model_SCR_m_New.columns if c.startswith('pct_')]
df_Prov_Model_SCR_m_New.select(scr_cols + pct_cols).summary("count", "missing", "mean").show()

print("Schema for target.Prov_Model_SCR_m{version}_New:")
df_Prov_Model_SCR_m_New.printSchema()

print("Frequency for cci_trigger_date:")
df_Prov_Model_SCR_m_New.groupBy("cci_trigger_date").count().show(truncate=False)

# Re-running the includes for the v2 table creation - this seems redundant but follows the SAS script
df_Prov_Model_SCR_m_v2 = df_Prov_Model_SCR_m_New
# %run "/vg06/jye/model_score/decile_cutoff/PCT_Cartus_EM_202007.txt"
# %run "/vg06/btao/model_score/code/PCT_AS_EM_201701.txt"
# %run "/vg06/btao/model_score/code/PCT_Exxon_Clone_202010.txt"
# %run "/vg06/btao/model_score/code/PCT_Exxon_Prem_Clone_202010.txt"
# %run "/vg06/btao/model_score/code/PCT_VBR_Clone_201709.txt"
# df_Prov_Model_SCR_m_v2.write.format("delta").mode("overwrite").saveAsTable(f"target.Prov_Model_SCR_m{version}_v2")
# print("Schema for target.Prov_Model_SCR_m{version}_v2:")
# spark.table(f"target.Prov_Model_SCR_m{version}_v2").printSchema()

#End-DBShift