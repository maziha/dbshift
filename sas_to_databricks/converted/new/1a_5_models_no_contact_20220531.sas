import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import math

spark = SparkSession.builder.appName("models_no_contact").getOrCreate()

# Define macro variables from SAS environment
muldate = "20131202" # Based on SAS code comment *20131202;
runtype = "PROD" # Placeholder

# Reading the source table
df_geo_appends_rpm_initial = spark.table("intermed.geo_appends_rpm")

# data intermed.geo_appends_rpm;
# set intermed.geo_appends_rpm;
# if newsletter_opens_cnt_12mo>0 then newsletter_opens_cnt_12mo=0;
# run;
df_geo_appends_rpm_updated = df_geo_appends_rpm_initial.withColumn(
    "newsletter_opens_cnt_12mo",
    F.when(F.col("newsletter_opens_cnt_12mo") > 0, F.lit(0)).otherwise(F.col("newsletter_opens_cnt_12mo"))
)


# data alldata_scored(...); set intermed.geo_appends_rpm(keep=...);
columns_to_keep = [
    "mid_key", "memxrenew", "IBX_GRAND_CHILDREN_AGG_HHD", "mid_key", "sy_otsbn_polfund_2012b",
    "foundation_donors_12mo_i", "state", "advo_segment_cd", "old_score14_vigintile",
    "old_score20_vigintile", "old_score6_vigintile", "Advo_Last_Amt", "advo_hpc_amt",
    "IBX_INCOME_ESTIMATED_NARROW_RANG", "individual_engagers_12mo", "contact_leg_12mo_i",
    "state_activity_12mo_i", "aarporg_i", "suppression", "foundation_donors_checkb_12mo_i",
    "WORK_CLUSTERS", "IBX_COMMUNITY_INVOLVEMENT_ANIMAL", "IBX_COMMUNITY_INVOLVEMENT_CHILDR",
    "IBX_COMMUNITY_INVOLVEMENT_ENVIRO", "IBX_COMMUNITY_INVOLVEMENT_HEALTH",
    "IBX_COMMUNITY_INVOLVEMENT_RELIGI", "IBX_COMMUNITY_INVOLVEMENT_VETERA",
    "globally_opted_in", "ethniccode", "IBX_ADULT_AGE_75_P_AGG_HHD",
    "IBX_COMMUNITY_CHARITIES_AGG_HHD", "IBX_COMMUNITY_INVOLVEMENT_LIBERA",
    "IBX_DWELLING_TYPE_AGG_HHD", "IBX_NETWORTH_PREMIER_AGG_HHD", "IBX_OCCUPATION_INPUT_AGG_HHD",
    "IBX_PERSONIC_CLUSTER", "IBX_TELECOM_INTERNET_AGG_HHD", "Overall_Active_SP_Reltshps",
    "ch_acq", "old_score21_vigintile", "old_score26_vigintile", "old_score30_vigintile",
    "old_score35_vigintile", "old_score38_vigintile", "old_score9_vigintile",
    "diversity_flag_agg_ind", "drvs_flag", "memorigindate", "old_score1_vigintile",
    "old_score29_vigintile", "old_score3_vigintile", "teletown_12mo_i",
    "advocacy_petitions_12mo", "yeas_survey_12mo_i", "DENSITY_CLUSTERS",
    "IBX_PROPERTY_TYPE_AGG_HHD", "old_score2_vigintile", "IBX_COMMUNITY_INVOLVEMENT_CULTUR",
    "IBX_TELECOM_CELLULAR_AGG_HHD", "IBX_COMMUNITY_INVOLVEMENT_AID_AG",
    "IBX_ELDERLY_PARENT_AGG_HHD", "CENS_ETHNIC_POP_PERCENT_HISPANIC", "fndnothr_score",
    "category", "old_score11_vigintile", "age_agg_ind", "ibx_presence_of_senior_adult_agg",
    "maritalstatus", "IBX_HOME_MARKET_VALUE_DECILES_AG", "SMARTPHONE",
    "CENS_GRPQTRS_POP_PERCENT_COLLEGE", "donor", "old_score33_vigintile",
    "advocacy_donors_12mo_i", "FNDN_AARPPRO_DM", "newsletter_opens_cnt_12mo"
]
df_for_scoring = df_geo_appends_rpm_updated.select(*columns_to_keep)

# Start scoring model transformations
df_transformed = df_for_scoring.withColumn("community_animal_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_ANIMAL") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("community_children_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_CHILDR") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("community_enviro_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_ENVIRO") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("community_health_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_HEALTH") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("community_religion_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_RELIGI") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("community_veteran_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_VETERA") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("ethniccode_20_dum", F.when(F.col("ethniccode") == '20', 1).otherwise(0))
df_transformed = df_transformed.withColumn("goi_1_dum", F.when(F.col("globally_opted_in") == '1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("adult_75plus_dum", F.when(F.col("IBX_ADULT_AGE_75_P_AGG_HHD") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("community_charity_dum", F.when(F.col("IBX_COMMUNITY_CHARITIES_AGG_HHD") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("community_libera_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_LIBERA") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("dwelling_m_dum", F.when(F.col("IBX_DWELLING_TYPE_AGG_HHD") == 'M', 1).otherwise(0))
df_transformed = df_transformed.withColumn("networth_1_dum", F.when(F.col("IBX_NETWORTH_PREMIER_AGG_HHD") == '1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("job_y_dum", F.when(F.col("IBX_OCCUPATION_INPUT_AGG_HHD") == 'Y', 1).otherwise(0))
df_transformed = df_transformed.withColumn("personic_17_dum", F.when(F.col("IBX_PERSONIC_CLUSTER") == '17', 1).otherwise(0))
df_transformed = df_transformed.withColumn("internet_10_dum", F.when(F.col("IBX_TELECOM_INTERNET_AGG_HHD") == '10', 1).otherwise(0))
df_transformed = df_transformed.withColumn("active_sp_dum", F.when(F.col("Overall_Active_SP_Reltshps") >= '1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("chacq_u_dum", F.when(F.col("ch_acq") == 'U', 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters21_1to6_dum", F.when(F.col("old_score21_vigintile").isin('1','2','3','4','5','6'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters26_17to20_dum", F.when(F.col("old_score26_vigintile").isin('17','18','19','20'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters30_1to4_dum", F.when(F.col("old_score30_vigintile").isin('1','2','3','4'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters35_18to20_dum", F.when(F.col("old_score35_vigintile").isin('18','19','20'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters38_17to20_dum", F.when(F.col("old_score38_vigintile").isin('17','18','19','20'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters9_1to5_dum", F.when(F.col("old_score9_vigintile").isin('1','2','3','4','5'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters9_20_dum", F.when(F.col("old_score9_vigintile") == '20', 1).otherwise(0))
df_transformed = df_transformed.withColumn("income_7toc_dum", F.when(F.col("IBX_INCOME_ESTIMATED_NARROW_RANG").isin('7','8','9','A','B','C'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("property_a_dum", F.when(F.col("IBX_PROPERTY_TYPE_AGG_HHD") == 'A', 1).otherwise(0))
df_transformed = df_transformed.withColumn("density_4to7_dum", F.when(F.col("DENSITY_CLUSTERS").isin('4','5','6','7'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("workcluster_5_dum", F.when(F.col("WORK_CLUSTERS") == '5', 1).otherwise(0))
df_transformed = df_transformed.withColumn("workcluster_8_dum", F.when(F.col("WORK_CLUSTERS") == '8', 1).otherwise(0))
df_transformed = df_transformed.withColumn("workcluster_10_dum", F.when(F.col("WORK_CLUSTERS") == '10', 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters1_1to3_dum", F.when(F.col("old_score1_vigintile").isin('1','2','3'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters1_16to20_dum", F.when(F.col("old_score1_vigintile").isin('16','17','18','19','20'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters29_15to20_dum", F.when(F.col("old_score29_vigintile").isin('15','16','17','18','19','20'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters3_20_dum", F.when(F.col("old_score3_vigintile") == '20', 1).otherwise(0))
df_transformed = df_transformed.withColumn("age_65plus_dum", F.when(F.col("age_agg_ind") >= 65, 1).otherwise(0))
df_transformed = df_transformed.withColumn("teletown_12mo_i_c", F.coalesce(F.col("teletown_12mo_i"), F.lit(0)))
df_transformed = df_transformed.withColumn("state_activity_12mo_i_c", F.coalesce(F.col("state_activity_12mo_i"), F.lit(0)))
df_transformed = df_transformed.withColumn("advocacy_petitions_12mo", F.coalesce(F.col("advocacy_petitions_12mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("individual_engagers_12mo", F.coalesce(F.col("individual_engagers_12mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("yeas_survey_12mo_i_c", F.coalesce(F.col("yeas_survey_12mo_i"), F.lit(0)))
df_transformed = df_transformed.withColumn("DRVS_Flag", F.coalesce(F.col("drvs_flag"), F.lit(0)))
df_transformed = df_transformed.withColumn("sy_otsbn_polfund_2012b_c", F.coalesce(F.col("sy_otsbn_polfund_2012b"), F.lit(0)))
df_transformed = df_transformed.withColumn("DIVERSITY_FLag_AGG_IND", F.coalesce(F.col("diversity_flag_agg_ind"), F.lit(0)))
df_transformed = df_transformed.withColumn("diversity_hispanic", F.when(F.col("diversity_flag_agg_ind") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("diversity_aab", F.when(F.col("diversity_flag_agg_ind") == 2, 1).otherwise(0))
df_transformed = df_transformed.withColumn("E", F.to_date(F.lit(muldate), "yyyyMMdd"))
df_transformed = df_transformed.withColumn("F", F.to_date(F.col("memorigindate").cast("string"), "yyyyMMdd"))
df_transformed = df_transformed.withColumn("Membermonths", F.datediff(F.col("E"), F.col("F")) / 30.0)
df_transformed = df_transformed.withColumn("Membermonths", F.coalesce(F.col("Membermonths"), F.lit(0)))
df_transformed = df_transformed.withColumn("age_agg_ind_c", F.coalesce(F.col("age_agg_ind"), F.lit(67.16)))

df_transformed = df_transformed.withColumn("logit_aca_tth", 
    F.lit(-4.4068) +
    (F.col("diversity_hispanic") * -0.3655) +
    (F.col("diversity_aab") * 0.3243) +
    (F.col("Membermonths") * -0.00066) +
    (F.col("age_agg_ind_c") * 0.0143) +
    (F.col("community_religion_dum") * 0.0843) +
    (F.col("community_veteran_dum") * 0.0817) +
    (F.col("income_7toc_dum") * -0.1942) +
    (F.col("property_a_dum") * 0.0935) +
    (F.col("density_4to7_dum") * -0.3316) +
    (F.col("workcluster_5_dum") * 0.1908) +
    (F.col("workcluster_8_dum") * 0.4198) +
    (F.col("workcluster_10_dum") * 0.4752) +
    (F.col("masters1_1to3_dum") * 0.1602) +
    (F.col("masters26_17to20_dum") * -0.1354) +
    (F.col("masters29_15to20_dum") * -0.125) +
    (F.col("masters35_18to20_dum") * 0.0924) +
    (F.col("masters3_20_dum") * 0.1275) +
    (F.col("teletown_12mo_i_c") * 3.6558) +
    (F.col("state_activity_12mo_i_c") * 0.6955) +
    (F.col("advocacy_petitions_12mo") * 0.0483) +
    (F.col("individual_engagers_12mo") * 0.0607) +
    (F.col("yeas_survey_12mo_i_c") * 0.1866) +
    (F.col("DRVS_Flag") * 0.2131) +
    (F.col("sy_otsbn_polfund_2012b_c") * 0.00175)
)
df_transformed = df_transformed.withColumn("modelscore_aca_tth", F.exp(F.col("logit_aca_tth")) / (1 + F.exp(F.col("logit_aca_tth"))))
df_transformed = df_transformed.withColumn("community_aid_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_AID_AG") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("community_cultur_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_CULTUR") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("elderly_u_dum", F.when(F.col("IBX_ELDERLY_PARENT_AGG_HHD") == 'U', 1).otherwise(0))
df_transformed = df_transformed.withColumn("cell_1to2_dum", F.when(F.col("IBX_TELECOM_CELLULAR_AGG_HHD").isin('1','2'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters2_18to20_dum", F.when(F.col("old_score2_vigintile").isin('18','19','20'), 1).otherwise(0))

df_transformed = df_transformed.withColumn("community_total",
    F.col("community_charity_dum") +
    F.col("community_aid_dum") +
    F.col("community_animal_dum") +
    F.col("community_children_dum") +
    F.col("community_cultur_dum") +
    F.col("community_enviro_dum") +
    F.col("community_health_dum") +
    F.col("community_libera_dum") +
    F.col("community_religion_dum") +
    F.col("community_veteran_dum")
)
df_transformed = df_transformed.withColumn("community_total_atleast4", F.when(F.col("community_total") >= 4, 1).otherwise(0))
df_transformed = df_transformed.withColumn("yeas_survey_12mo_i_c", F.coalesce(F.col("yeas_survey_12mo_i"), F.lit(0.2017913)))
df_transformed = df_transformed.withColumn("sy_otsbn_polfund_2012b_c", F.coalesce(F.col("sy_otsbn_polfund_2012b"), F.lit(42.285666)))
df_transformed = df_transformed.withColumn("PERCENTHISPANIC_c", F.coalesce(F.col("CENS_ETHNIC_POP_PERCENT_HISPANIC"), F.lit(7.2795377)))
df_transformed = df_transformed.withColumn("logit_1", F.exp(
    F.lit(-1.3369) + 
    (F.col("ethniccode_20_dum") * -0.0571) + 
    (F.col("community_charity_dum") * 0.0329) + 
    (F.col("dwelling_m_dum") * -0.0662) + 
    (F.col("elderly_u_dum") * 0.0355) + 
    (F.col("networth_1_dum") * -0.0824) + 
    (F.col("personic_17_dum") * -0.0458) + 
    (F.col("cell_1to2_dum") * 0.0309) + 
    (F.col("masters21_1to6_dum") * -0.0253) + 
    (F.col("masters2_18to20_dum") * 0.1489) + 
    (F.col("masters38_17to20_dum") * 0.5175) + 
    (F.col("yeas_survey_12mo_i_c") * 0.0964) + 
    (F.col("sy_otsbn_polfund_2012b_c") * 0.0019) + 
    (F.col("PERCENTHISPANIC_c") * -0.00173) + 
    (F.col("community_total_atleast4") * 0.0529))
)
df_transformed = df_transformed.withColumn("WorkNSaveINT", F.col("logit_1") / (1 + F.col("logit_1")))
df_transformed = df_transformed.withColumn("age70_dum", F.when(F.col("age_agg_ind") >= 70, 1).otherwise(0))
df_transformed = df_transformed.withColumn("age60_dum", F.when((F.col("age_agg_ind") >= 60) & (F.col("age_agg_ind") < 70), 1).otherwise(0))
df_transformed = df_transformed.withColumn("fndnmodel_dum", F.when((F.col("fndnothr_score") >= 1) & (F.col("fndnothr_score") <= 16), 1).otherwise(0))
df_transformed = df_transformed.withColumn("score1_dum", F.when((F.col("old_score1_vigintile") >= 1) & (F.col("old_score1_vigintile") <= 7), 1).otherwise(0))
df_transformed = df_transformed.withColumn("senior_dum", F.when(F.col("ibx_presence_of_senior_adult_agg") == 'Y', 1).otherwise(0))
df_transformed = df_transformed.withColumn("married_dum", F.when(F.col("maritalstatus") == 'M', 1).otherwise(0))
df_transformed = df_transformed.withColumn("diversity_dum", F.when(F.col("diversity_flag_agg_ind").isin(1, 3), 1).otherwise(0))
df_transformed = df_transformed.withColumn("homevalue_10_dum", F.when(F.col("IBX_HOME_MARKET_VALUE_DECILES_AG") == '10', 1).otherwise(0))
df_transformed = df_transformed.withColumn("income_1to4_dum", F.when(F.col("IBX_INCOME_ESTIMATED_NARROW_RANG").isin('1','2','3','4'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("region_w_dum", F.when(F.upper(F.col("STATE")).isin('AK', 'CA', 'HI', 'NV', 'OR', 'UT', 'WA'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("cat_n_dum", F.when(F.col("category") == 'N', 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters11_19to20_dum", F.when(F.col("old_score11_vigintile").isin('19','20'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("teletown_12mo_i_c", F.coalesce(F.col("teletown_12mo_i"), F.lit(0.000030954)))
df_transformed = df_transformed.withColumn("Advo_Last_Amt_c", F.coalesce(F.col("Advo_Last_Amt"), F.lit(7.0253692)))
df_transformed = df_transformed.withColumn("MEDIA_SMARTPHONE_c", F.coalesce(F.col("SMARTPHONE"), F.lit(20.0828269)))

df_transformed = df_transformed.withColumn("logit_caregiving_score", 
    F.lit(-2.2352) + 
    (F.col("fndnmodel_dum") * 0.0703) + 
    (F.col("senior_dum") * 0.0658) + 
    (F.col("married_dum") * 0.1044) + 
    (F.col("diversity_dum") * -0.2129) + 
    (F.col("adult_75plus_dum") * 0.1014) + 
    (F.col("community_charity_dum") * 0.1519) + 
    (F.col("community_health_dum") * 0.1356) + 
    (F.col("community_religion_dum") * 0.1127) + 
    (F.col("dwelling_m_dum") * -0.1692) + 
    (F.col("homevalue_10_dum") * -0.1995) + 
    (F.col("income_1to4_dum") * 0.0829) + 
    (F.col("internet_10_dum") * 0.1427) + 
    (F.col("active_sp_dum") * -0.0857) + 
    (F.col("region_w_dum") * -0.1533) + 
    (F.col("cat_n_dum") * -0.2746) + 
    (F.col("chacq_u_dum") * -0.1086) + 
    (F.col("masters11_19to20_dum") * -0.1604) + 
    (F.col("masters9_1to5_dum") * -0.0962) + 
    (F.col("teletown_12mo_i_c") * 0.3977) + 
    (F.col("Advo_Last_Amt_c") * -0.00862) + 
    (F.col("yeas_survey_12mo_i_c") * 0.3334) + 
    (F.col("MEDIA_SMARTPHONE_c") * -0.0054)
)
df_transformed = df_transformed.withColumn("caregiving_score", F.exp(F.col("logit_caregiving_score")) / (1 + F.exp(F.col("logit_caregiving_score"))))
df_transformed = df_transformed.withColumn("donor", F.coalesce(F.col("donor"), F.lit(0)))
df_transformed = df_transformed.withColumn("grandchildren_dum", F.when(F.col("IBX_GRAND_CHILDREN_AGG_HHD") == 'Y', 1).otherwise(0))
df_transformed = df_transformed.withColumn("workcluster_1_dum", F.when(F.col("WORK_CLUSTERS") == '1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("advo_s1_dum", F.when(F.col("advo_segment_cd") == 'S1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters14_1to5_dum", F.when(F.col("old_score14_vigintile").isin('1','2','3','4','5'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters20_1to6_dum", F.when(F.col("old_score20_vigintile").isin('1','2','3','4','5','6'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters33_19to20_dum", F.when(F.col("old_score33_vigintile").isin('19','20'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters6_1to5_dum", F.when(F.col("old_score6_vigintile").isin('1','2','3','4','5'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("foundation_donors_12mo_i_c", F.coalesce(F.col("foundation_donors_12mo_i"), F.lit(0.000732289)))
df_transformed = df_transformed.withColumn("advocacy_donors_12mo_i_c", F.coalesce(F.col("advocacy_donors_12mo_i"), F.lit(0.0244875)))
df_transformed = df_transformed.withColumn("advo_hpc_amt_c", F.coalesce(F.col("advo_hpc_amt"), F.lit(7.4357913)))
df_transformed = df_transformed.withColumn("CENS_GRPQTRS_POP_PERCENT_COLL_c", F.coalesce(F.col("CENS_GRPQTRS_POP_PERCENT_COLLEGE"), F.lit(0.2840643)))
df_transformed = df_transformed.withColumn("contact_leg_12mo_i_c", F.coalesce(F.col("contact_leg_12mo_i"), F.lit(0.0131178)))
df_transformed = df_transformed.withColumn("FNDN_AARPPRO_DM_c", F.coalesce(F.col("FNDN_AARPPRO_DM"), F.lit(52.3362002)))
df_transformed = df_transformed.withColumn("state_activity_12mo_i_c", F.coalesce(F.col("state_activity_12mo_i"), F.lit(0.0014278)))
df_transformed = df_transformed.withColumn("aarporg_i_c", F.coalesce(F.col("aarporg_i"), F.lit(0.1009878)))
df_transformed = df_transformed.withColumn("newsletter_opens_cnt_12mo_c", F.coalesce(F.col("newsletter_opens_cnt_12mo"), F.lit(0.506085)))
df_transformed = df_transformed.withColumn("suppression_c", F.coalesce(F.col("suppression"), F.lit(0.000244617)))
df_transformed = df_transformed.withColumn("foundation_donors_box_12mo_i_c", F.coalesce(F.col("foundation_donors_checkb_12mo_i"), F.lit(0.000078934)))
df_transformed = df_transformed.withColumn("age_lt64_dum", F.when(F.col("age_agg_ind") <= 63, 1).otherwise(0))
df_transformed = df_transformed.withColumn("MemXRenew_c", F.coalesce(F.col("MemXRenew"), F.lit(0)))

df_transformed = df_transformed.withColumn("logit_advo_pro_em_score", 
    F.lit(-6.515) +
    (F.lit(1.0728) * F.col("donor")) + 
    (F.lit(0.1798) * F.col("goi_1_dum")) + 
    (F.lit(0.3103) * F.col("community_cultur_dum")) + 
    (F.lit(0.2162) * F.col("dwelling_m_dum")) + 
    (F.lit(-0.2402) * F.col("grandchildren_dum")) + 
    (F.lit(-0.1276) * F.col("income_1to4_dum")) + 
    (F.lit(0.2174) * F.col("workcluster_1_dum")) + 
    (F.lit(-0.4445) * F.col("advo_s1_dum")) + 
    (F.lit(0.1654) * F.col("masters14_1to5_dum")) + 
    (F.lit(-0.2344) * F.col("masters20_1to6_dum")) + 
    (F.lit(0.2119) * F.col("masters21_1to6_dum")) + 
    (F.lit(-0.1491) * F.col("masters26_17to20_dum")) + 
    (F.lit(-0.2395) * F.col("masters30_1to4_dum")) + 
    (F.lit(0.3425) * F.col("masters33_19to20_dum")) + 
    (F.lit(0.3219) * F.col("masters35_18to20_dum")) + 
    (F.lit(-0.3141) * F.col("masters6_1to5_dum")) + 
    (F.lit(0.6206) * F.col("masters9_20_dum")) + 
    (F.lit(0.5056) * F.col("foundation_donors_12mo_i_c")) + 
    (F.lit(0.6199) * F.col("advocacy_donors_12mo_i_c")) + 
    (F.lit(0.00415) * F.col("advo_hpc_amt_c")) + 
    (F.lit(0.3655) * F.col("yeas_survey_12mo_i_c")) + 
    (F.lit(0.0157) * F.col("CENS_GRPQTRS_POP_PERCENT_COLL_c")) + 
    (F.lit(1.195) * F.col("contact_leg_12mo_i_c")) + 
    (F.lit(-0.00885) * F.col("FNDN_AARPPRO_DM_c")) + 
    (F.lit(0.6566) * F.col("state_activity_12mo_i_c")) + 
    (F.lit(0.3024) * F.col("aarporg_i_c")) + 
    (F.lit(0.00258) * F.col("newsletter_opens_cnt_12mo_c")) + 
    (F.lit(0.2629) * F.col("suppression_c")) + 
    (F.lit(0.6065) * F.col("foundation_donors_box_12mo_i_c")) + 
    (F.lit(-0.7018) * F.col("age_lt64_dum")) + 
    (F.lit(0.0134) * F.col("MemXRenew_c"))
)
df_transformed = df_transformed.withColumn("advo_pro_em_score", F.exp(F.col("logit_advo_pro_em_score")) / (1 + F.exp(F.col("logit_advo_pro_em_score"))))

df_alldata_scored = df_transformed.select(
    "mid_key", "modelscore_aca_tth", "WorkNSaveINT", "caregiving_score", "advo_pro_em_score"
)

# proc sort data=alldata_scored noduprecs; by mid_key;
df_alldata_scored = df_alldata_scored.dropDuplicates(["mid_key"])

# proc rank data=alldata_scored out=centiles ties=low descending groups=99;
df_alldata_scored_ranked_prep = df_alldata_scored.withColumn("dummy_partition", F.lit(1))

window_aca_tth = Window.partitionBy("dummy_partition").orderBy(F.col("modelscore_aca_tth").desc())
window_worknsaveint = Window.partitionBy("dummy_partition").orderBy(F.col("WorkNSaveINT").desc())
window_caregiving = Window.partitionBy("dummy_partition").orderBy(F.col("caregiving_score").desc())
window_advo_pro_em = Window.partitionBy("dummy_partition").orderBy(F.col("advo_pro_em_score").desc())

# SAS groups=99 creates 100 buckets (0-99). ntile(100) creates 100 buckets (1-100).
# The subsequent SQL adds 1 to the SAS rank, making it 1-100.
# So ntile(100) is the correct translation of the combined logic.
df_centiles = df_alldata_scored_ranked_prep.select(
    "mid_key",
    F.ntile(100).over(window_aca_tth).alias("ACA_TTH"),
    F.ntile(100).over(window_worknsaveint).alias("WorkNSaveINT_PH"),
    F.ntile(100).over(window_caregiving).alias("CAREGIVER_PH"),
    F.ntile(100).over(window_advo_pro_em).alias("advo_pro_em")
)

# proc sql; create table intermed.geo_appends_rpm as select ...
df_geo_appends_rpm_base = spark.table("intermed.geo_appends_rpm")
df_geo_appends_rpm_base.createOrReplaceTempView("geo_appends_rpm")
df_centiles.createOrReplaceTempView("centiles")

base_cols = df_geo_appends_rpm_base.columns
rank_cols_to_replace = ["ACA_TTH", "WorkNSaveINT_PH", "CAREGIVER_PH", "advo_pro_em"]
rank_cols_to_replace_upper = [c.upper() for c in rank_cols_to_replace]

select_base_cols = [f"a.`{c}`" for c in base_cols if c.upper() not in rank_cols_to_replace_upper]

sql_query = f"""
    SELECT
        {', '.join(select_base_cols)},
        b.ACA_TTH,
        b.WorkNSaveINT_PH,
        b.CAREGIVER_PH,
        b.advo_pro_em
    FROM
        geo_appends_rpm a
    LEFT JOIN
        centiles b ON CAST(a.merkleid AS BIGINT) = b.mid_key
"""

df_geo_appends_rpm_new = spark.sql(sql_query)

df_geo_appends_rpm_new.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

# proc freq for diagnostics
# ODS PDF and Titles are for reporting and are translated as print statements.
print("AARP Advocacy DM Scoring Diagnostic Report")
print(f"Data as of {runtype} {muldate}")

df_final_for_freq = spark.table("intermed.geo_appends_rpm")

print(f"Frequency Distribution for Advocacy Model Score Segments: {runtype} {muldate} Data")
freq_columns = ["ACA_TTH", "WorkNSaveINT_PH", "CAREGIVER_PH", "advo_pro_em"]
for col_name in freq_columns:
    print(f"--- Frequency for {col_name} ---")
    df_final_for_freq.groupBy(col_name).count().orderBy(F.col(col_name).asc_nulls_first()).show(150, truncate=False)

#End-DBShift