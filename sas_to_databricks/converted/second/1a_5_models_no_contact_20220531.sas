import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType
from datetime import datetime

# Assume these variables are defined, e.g., through Databricks widgets
muldate = "20131202"
runtype = "PROD"

# Initialize Spark Session
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("models_no_contact").getOrCreate()

# SAS:
# data intermed.geo_appends_rpm;
# set intermed.geo_appends_rpm;
# if newsletter_opens_cnt_12mo>0 then newsletter_opens_cnt_12mo=0;
# run;
df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_geo_appends_rpm_updated = df_geo_appends_rpm.withColumn(
    "newsletter_opens_cnt_12mo",
    F.when(F.col("newsletter_opens_cnt_12mo") > 0, 0).otherwise(F.col("newsletter_opens_cnt_12mo"))
)

# SAS:
# data alldata_scored(keep=mid_key modelscore_aca_tth WorkNSaveINT caregiving_score advo_pro_em_score);
# set intermed.geo_appends_rpm(keep=...);
# ...
# run;

df_alldata_input = df_geo_appends_rpm_updated.select(
    "mid_key", "memxrenew", "ibx_grand_children_agg_hhd", 
    "sy_otsbn_polfund_2012b", "foundation_donors_12mo_i", 
    "state", "advo_segment_cd", "old_score14_vigintile", 
    "old_score20_vigintile", 
    "old_score6_vigintile", "advo_last_amt", "advo_hpc_amt", 
    "ibx_income_estimated_narrow_rang", 
    "individual_engagers_12mo", "contact_leg_12mo_i", "state_activity_12mo_i", "aarporg_i", "suppression", 
    "foundation_donors_checkb_12mo_i", "work_clusters",
    "ibx_community_involvement_animal",
    "ibx_community_involvement_childr",
    "ibx_community_involvement_enviro",
    "ibx_community_involvement_health",
    "ibx_community_involvement_religi",
    "ibx_community_involvement_vetera",
    "globally_opted_in",
    "ethniccode",
    "ibx_adult_age_75_p_agg_hhd",
    "ibx_community_charities_agg_hhd",
    "ibx_community_involvement_libera",
    "ibx_dwelling_type_agg_hhd",
    "ibx_networth_premier_agg_hhd",
    "ibx_occupation_input_agg_hhd",
    "ibx_personic_cluster",
    "ibx_telecom_internet_agg_hhd",
    "overall_active_sp_reltshps",
    "ch_acq",
    "old_score21_vigintile",
    "old_score26_vigintile",
    "old_score30_vigintile",
    "old_score35_vigintile",
    "old_score38_vigintile",
    "old_score9_vigintile",
    "diversity_flag_agg_ind", 
    "drvs_flag", 
    "memorigindate",
    "old_score1_vigintile", 
    "old_score29_vigintile", 
    "old_score3_vigintile",
    "teletown_12mo_i",
    "advocacy_petitions_12mo",
    "yeas_survey_12mo_i",
    "density_clusters", 
    "ibx_property_type_agg_hhd",
    "old_score2_vigintile",
    "ibx_community_involvement_cultur",
    "ibx_telecom_cellular_agg_hhd",
    "ibx_community_involvement_aid_ag",
    "ibx_elderly_parent_agg_hhd",
    "cens_ethnic_pop_percent_hispanic",
    "fndnothr_score", 
    "category", 
    "old_score11_vigintile",
    "age_agg_ind", 
    "ibx_presence_of_senior_adult_agg",
    "maritalstatus",
    "ibx_home_market_value_deciles_ag", 
    "smartphone",
    "cens_grpqtrs_pop_percent_college",
    "donor",
    "old_score33_vigintile",
    "advocacy_donors_12mo_i",
    "fndn_aarppro_dm", 
    "newsletter_opens_cnt_12mo"
)

df_transformed = df_alldata_input
df_transformed = df_transformed.withColumn("community_animal_dum", F.when(F.col("ibx_community_involvement_animal") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("community_children_dum", F.when(F.col("ibx_community_involvement_childr") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("community_enviro_dum", F.when(F.col("ibx_community_involvement_enviro") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("community_health_dum", F.when(F.col("ibx_community_involvement_health") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("community_religion_dum", F.when(F.col("ibx_community_involvement_religi") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("community_veteran_dum", F.when(F.col("ibx_community_involvement_vetera") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("ethniccode_20_dum", F.when(F.col("ethniccode") == '20', 1).otherwise(0))
df_transformed = df_transformed.withColumn("goi_1_dum", F.when(F.col("globally_opted_in") == '1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("adult_75plus_dum", F.when(F.col("ibx_adult_age_75_p_agg_hhd") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("community_charity_dum", F.when(F.col("ibx_community_charities_agg_hhd") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("community_libera_dum", F.when(F.col("ibx_community_involvement_libera") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("dwelling_m_dum", F.when(F.col("ibx_dwelling_type_agg_hhd") == 'M', 1).otherwise(0))
df_transformed = df_transformed.withColumn("networth_1_dum", F.when(F.col("ibx_networth_premier_agg_hhd") == '1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("job_y_dum", F.when(F.col("ibx_occupation_input_agg_hhd") == 'Y', 1).otherwise(0))
df_transformed = df_transformed.withColumn("personic_17_dum", F.when(F.col("ibx_personic_cluster") == '17', 1).otherwise(0))
df_transformed = df_transformed.withColumn("internet_10_dum", F.when(F.col("ibx_telecom_internet_agg_hhd") == '10', 1).otherwise(0))
df_transformed = df_transformed.withColumn("active_sp_dum", F.when(F.col("overall_active_sp_reltshps") >= '1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("chacq_u_dum", F.when(F.col("ch_acq") == 'U', 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters21_1to6_dum", F.when(F.col("old_score21_vigintile").isin('1', '2', '3', '4', '5', '6'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters26_17to20_dum", F.when(F.col("old_score26_vigintile").isin('17', '18', '19', '20'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters30_1to4_dum", F.when(F.col("old_score30_vigintile").isin('1', '2', '3', '4'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters35_18to20_dum", F.when(F.col("old_score35_vigintile").isin('18', '19', '20'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters38_17to20_dum", F.when(F.col("old_score38_vigintile").isin('17', '18', '19', '20'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters9_1to5_dum", F.when(F.col("old_score9_vigintile").isin('1', '2', '3', '4', '5'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters9_20_dum", F.when(F.col("old_score9_vigintile") == '20', 1).otherwise(0))
df_transformed = df_transformed.withColumn("income_7toc_dum", F.when(F.col("ibx_income_estimated_narrow_rang").isin('7', '8', '9', 'A', 'B', 'C'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("property_a_dum", F.when(F.col("ibx_property_type_agg_hhd") == 'A', 1).otherwise(0))
df_transformed = df_transformed.withColumn("density_4to7_dum", F.when(F.col("density_clusters").isin('4', '5', '6', '7'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("workcluster_5_dum", F.when(F.col("work_clusters") == '5', 1).otherwise(0))
df_transformed = df_transformed.withColumn("workcluster_8_dum", F.when(F.col("work_clusters") == '8', 1).otherwise(0))
df_transformed = df_transformed.withColumn("workcluster_10_dum", F.when(F.col("work_clusters") == '10', 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters1_1to3_dum", F.when(F.col("old_score1_vigintile").isin('1', '2', '3'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters1_16to20_dum", F.when(F.col("old_score1_vigintile").isin('16', '17', '18', '19', '20'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters29_15to20_dum", F.when(F.col("old_score29_vigintile").isin('15', '16', '17', '18', '19', '20'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters3_20_dum", F.when(F.col("old_score3_vigintile") == '20', 1).otherwise(0))
df_transformed = df_transformed.withColumn("age_65plus_dum", F.when(F.col("age_agg_ind") >= 65, 1).otherwise(0))
df_transformed = df_transformed.withColumn("teletown_12mo_i_c", F.coalesce(F.col("teletown_12mo_i"), F.lit(0)))
df_transformed = df_transformed.withColumn("state_activity_12mo_i_c", F.coalesce(F.col("state_activity_12mo_i"), F.lit(0)))
df_transformed = df_transformed.withColumn("advocacy_petitions_12mo", F.coalesce(F.col("advocacy_petitions_12mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("individual_engagers_12mo", F.coalesce(F.col("individual_engagers_12mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("yeas_survey_12mo_i_c", F.coalesce(F.col("yeas_survey_12mo_i"), F.lit(0)))
df_transformed = df_transformed.withColumn("drvs_flag", F.coalesce(F.col("drvs_flag"), F.lit(0)))
df_transformed = df_transformed.withColumn("sy_otsbn_polfund_2012b_c", F.coalesce(F.col("sy_otsbn_polfund_2012b"), F.lit(0)))
df_transformed = df_transformed.withColumn("diversity_flag_agg_ind", F.coalesce(F.col("diversity_flag_agg_ind"), F.lit(0)))
df_transformed = df_transformed.withColumn("diversity_hispanic", F.when(F.col("diversity_flag_agg_ind") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("diversity_aab", F.when(F.col("diversity_flag_agg_ind") == 2, 1).otherwise(0))
df_transformed = df_transformed.withColumn("Membermonths", (F.datediff(F.to_date(F.lit(muldate), "yyyyMMdd"), F.to_date(F.col("memorigindate").cast("string"), "yyyyMMdd")) / 30.0))
df_transformed = df_transformed.withColumn("Membermonths", F.coalesce(F.col("Membermonths"), F.lit(0)))
df_transformed = df_transformed.withColumn("age_agg_ind_c", F.coalesce(F.col("age_agg_ind"), F.lit(67.16)))
df_transformed = df_transformed.withColumn("logit_aca_tth", 
    -4.4068 +
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
    (F.col("drvs_flag") * 0.2131) +
    (F.col("sy_otsbn_polfund_2012b_c") * 0.00175)
)
df_transformed = df_transformed.withColumn("modelscore_aca_tth", F.exp(F.col("logit_aca_tth")) / (1 + F.exp(F.col("logit_aca_tth"))))
df_transformed = df_transformed.withColumn("community_aid_dum", F.when(F.col("ibx_community_involvement_aid_ag") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("community_cultur_dum", F.when(F.col("ibx_community_involvement_cultur") == 1, 1).otherwise(0))
df_transformed = df_transformed.withColumn("elderly_u_dum", F.when(F.col("ibx_elderly_parent_agg_hhd") == 'U', 1).otherwise(0))
df_transformed = df_transformed.withColumn("cell_1to2_dum", F.when(F.col("ibx_telecom_cellular_agg_hhd").isin('1', '2'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters2_18to20_dum", F.when(F.col("old_score2_vigintile").isin('18', '19', '20'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("community_total", F.col("community_charity_dum") + F.col("community_aid_dum") + F.col("community_animal_dum") + F.col("community_children_dum") + F.col("community_cultur_dum") + F.col("community_enviro_dum") + F.col("community_health_dum") + F.col("community_libera_dum") + F.col("community_religion_dum") + F.col("community_veteran_dum"))
df_transformed = df_transformed.withColumn("community_total_atleast4", F.when(F.col("community_total") >= 4, 1).otherwise(0))
df_transformed = df_transformed.withColumn("yeas_survey_12mo_i_c", F.coalesce(F.col("yeas_survey_12mo_i"), F.lit(0.2017913)))
df_transformed = df_transformed.withColumn("sy_otsbn_polfund_2012b_c", F.coalesce(F.col("sy_otsbn_polfund_2012b"), F.lit(42.285666)))
df_transformed = df_transformed.withColumn("percenthispanic_c", F.coalesce(F.col("cens_ethnic_pop_percent_hispanic"), F.lit(7.2795377)))
df_transformed = df_transformed.withColumn("logit_1", F.exp(
    -1.3369 +
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
    (F.col("percenthispanic_c") * -0.00173) +
    (F.col("community_total_atleast4") * 0.0529)
))
df_transformed = df_transformed.withColumn("WorkNSaveINT", F.col("logit_1") / (1 + F.col("logit_1")))
df_transformed = df_transformed.withColumn("age70_dum", F.when(F.col("age_agg_ind") >= 70, 1).otherwise(0))
df_transformed = df_transformed.withColumn("age60_dum", F.when((F.col("age_agg_ind") >= 60) & (F.col("age_agg_ind") < 70), 1).otherwise(0))
df_transformed = df_transformed.withColumn("fndnmodel_dum", F.when((F.col("fndnothr_score") >= 1) & (F.col("fndnothr_score") <= 16), 1).otherwise(0))
df_transformed = df_transformed.withColumn("score1_dum", F.when((F.col("old_score1_vigintile") >= 1) & (F.col("old_score1_vigintile") <= 7), 1).otherwise(0))
df_transformed = df_transformed.withColumn("senior_dum", F.when(F.col("ibx_presence_of_senior_adult_agg") == 'Y', 1).otherwise(0))
df_transformed = df_transformed.withColumn("married_dum", F.when(F.col("maritalstatus") == 'M', 1).otherwise(0))
df_transformed = df_transformed.withColumn("diversity_dum", F.when(F.col("diversity_flag_agg_ind").isin(1, 3), 1).otherwise(0))
df_transformed = df_transformed.withColumn("homevalue_10_dum", F.when(F.col("ibx_home_market_value_deciles_ag") == '10', 1).otherwise(0))
df_transformed = df_transformed.withColumn("income_1to4_dum", F.when(F.col("ibx_income_estimated_narrow_rang").isin('1', '2', '3', '4'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("region_w_dum", F.when(F.upper(F.col("state")).isin('AK', 'CA', 'HI', 'NV', 'OR', 'UT', 'WA'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("cat_n_dum", F.when(F.col("category") == 'N', 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters11_19to20_dum", F.when(F.col("old_score11_vigintile").isin('19', '20'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("teletown_12mo_i_c", F.coalesce(F.col("teletown_12mo_i"), F.lit(0.000030954)))
df_transformed = df_transformed.withColumn("advo_last_amt_c", F.coalesce(F.col("advo_last_amt"), F.lit(7.0253692)))
df_transformed = df_transformed.withColumn("media_smartphone_c", F.coalesce(F.col("smartphone"), F.lit(20.0828269)))
df_transformed = df_transformed.withColumn("logit_caregiving_score", 
    -2.2352 +
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
    (F.col("advo_last_amt_c") * -0.00862) +
    (F.col("yeas_survey_12mo_i_c") * 0.3334) +
    (F.col("media_smartphone_c") * -0.0054)
)
df_transformed = df_transformed.withColumn("caregiving_score", F.exp(F.col("logit_caregiving_score")) / (1 + F.exp(F.col("logit_caregiving_score"))))
df_transformed = df_transformed.withColumn("donor", F.coalesce(F.col("donor"), F.lit(0)))
df_transformed = df_transformed.withColumn("grandchildren_dum", F.when(F.col("ibx_grand_children_agg_hhd") == 'Y', 1).otherwise(0))
df_transformed = df_transformed.withColumn("workcluster_1_dum", F.when(F.col("work_clusters") == '1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("advo_s1_dum", F.when(F.col("advo_segment_cd") == 'S1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters14_1to5_dum", F.when(F.col("old_score14_vigintile").isin('1', '2', '3', '4', '5'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters20_1to6_dum", F.when(F.col("old_score20_vigintile").isin('1', '2', '3', '4', '5', '6'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters33_19to20_dum", F.when(F.col("old_score33_vigintile").isin('19', '20'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("masters6_1to5_dum", F.when(F.col("old_score6_vigintile").isin('1', '2', '3', '4', '5'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("foundation_donors_12mo_i_c", F.coalesce(F.col("foundation_donors_12mo_i"), F.lit(0.000732289)))
df_transformed = df_transformed.withColumn("advocacy_donors_12mo_i_c", F.coalesce(F.col("advocacy_donors_12mo_i"), F.lit(0.0244875)))
df_transformed = df_transformed.withColumn("advo_hpc_amt_c", F.coalesce(F.col("advo_hpc_amt"), F.lit(7.4357913)))
df_transformed = df_transformed.withColumn("cens_grpqtrs_pop_percent_coll_c", F.coalesce(F.col("cens_grpqtrs_pop_percent_college"), F.lit(0.2840643)))
df_transformed = df_transformed.withColumn("contact_leg_12mo_i_c", F.coalesce(F.col("contact_leg_12mo_i"), F.lit(0.0131178)))
df_transformed = df_transformed.withColumn("fndn_aarppro_dm_c", F.coalesce(F.col("fndn_aarppro_dm"), F.lit(52.3362002)))
df_transformed = df_transformed.withColumn("state_activity_12mo_i_c", F.coalesce(F.col("state_activity_12mo_i"), F.lit(0.0014278)))
df_transformed = df_transformed.withColumn("aarporg_i_c", F.coalesce(F.col("aarporg_i"), F.lit(0.1009878)))
df_transformed = df_transformed.withColumn("newsletter_opens_cnt_12mo_c", F.coalesce(F.col("newsletter_opens_cnt_12mo"), F.lit(0.506085)))
df_transformed = df_transformed.withColumn("suppression_c", F.coalesce(F.col("suppression"), F.lit(0.000244617)))
df_transformed = df_transformed.withColumn("foundation_donors_box_12mo_i_c", F.coalesce(F.col("foundation_donors_checkb_12mo_i"), F.lit(0.000078934)))
df_transformed = df_transformed.withColumn("age_lt64_dum", F.when(F.col("age_agg_ind") <= 63, 1).otherwise(0))
df_transformed = df_transformed.withColumn("memxrenew_c", F.coalesce(F.col("memxrenew"), F.lit(0)))
df_transformed = df_transformed.withColumn("logit_advo_pro_em_score", 
    -6.515 +
    (1.0728 * F.col("donor")) +
    (0.1798 * F.col("goi_1_dum")) +
    (0.3103 * F.col("community_cultur_dum")) +
    (0.2162 * F.col("dwelling_m_dum")) +
    (-0.2402 * F.col("grandchildren_dum")) +
    (-0.1276 * F.col("income_1to4_dum")) +
    (0.2174 * F.col("workcluster_1_dum")) +
    (-0.4445 * F.col("advo_s1_dum")) +
    (0.1654 * F.col("masters14_1to5_dum")) +
    (-0.2344 * F.col("masters20_1to6_dum")) +
    (0.2119 * F.col("masters21_1to6_dum")) +
    (-0.1491 * F.col("masters26_17to20_dum")) +
    (-0.2395 * F.col("masters30_1to4_dum")) +
    (0.3425 * F.col("masters33_19to20_dum")) +
    (0.3219 * F.col("masters35_18to20_dum")) +
    (-0.3141 * F.col("masters6_1to5_dum")) +
    (0.6206 * F.col("masters9_20_dum")) +
    (0.5056 * F.col("foundation_donors_12mo_i_c")) +
    (0.6199 * F.col("advocacy_donors_12mo_i_c")) +
    (0.00415 * F.col("advo_hpc_amt_c")) +
    (0.3655 * F.col("yeas_survey_12mo_i_c")) +
    (0.0157 * F.col("cens_grpqtrs_pop_percent_coll_c")) +
    (1.195 * F.col("contact_leg_12mo_i_c")) +
    (-0.00885 * F.col("fndn_aarppro_dm_c")) +
    (0.6566 * F.col("state_activity_12mo_i_c")) +
    (0.3024 * F.col("aarporg_i_c")) +
    (0.00258 * F.col("newsletter_opens_cnt_12mo_c")) +
    (0.2629 * F.col("suppression_c")) +
    (0.6065 * F.col("foundation_donors_box_12mo_i_c")) +
    (-0.7018 * F.col("age_lt64_dum")) +
    (0.0134 * F.col("memxrenew_c"))
)
df_transformed = df_transformed.withColumn("advo_pro_em_score", F.exp(F.col("logit_advo_pro_em_score")) / (1 + F.exp(F.col("logit_advo_pro_em_score"))))

df_alldata_scored = df_transformed.select("mid_key", "modelscore_aca_tth", "WorkNSaveINT", "caregiving_score", "advo_pro_em_score")

# SAS:
# proc sort data=alldata_scored noduprecs;
# 	by mid_key;
# run;
df_alldata_scored = df_alldata_scored.dropDuplicates(["mid_key"])

# SAS:
# proc rank data=alldata_scored out=centiles ties=low descending groups=99;
# var modelscore_aca_tth WorkNSaveINT caregiving_score advo_pro_em_score;
# ranks ACA_TTH WorkNSaveINT_PH CAREGIVER_PH advo_pro_em;
# run;
df_alldata_scored_dummy = df_alldata_scored.withColumn("dummy", F.lit(1))
window_aca = Window.partitionBy("dummy").orderBy(F.col("modelscore_aca_tth").desc())
window_work = Window.partitionBy("dummy").orderBy(F.col("WorkNSaveINT").desc())
window_care = Window.partitionBy("dummy").orderBy(F.col("caregiving_score").desc())
window_advo = Window.partitionBy("dummy").orderBy(F.col("advo_pro_em_score").desc())

df_centiles = df_alldata_scored_dummy.withColumn("ACA_TTH", F.ntile(99).over(window_aca)) \
    .withColumn("WorkNSaveINT_PH", F.ntile(99).over(window_work)) \
    .withColumn("CAREGIVER_PH", F.ntile(99).over(window_care)) \
    .withColumn("advo_pro_em", F.ntile(99).over(window_advo)) \
    .select("mid_key", "ACA_TTH", "WorkNSaveINT_PH", "CAREGIVER_PH", "advo_pro_em")

# SAS:
# proc sql;
# 	create table intermed.geo_appends_rpm
# 	as
# 	select a.*, 
# 			b.ACA_TTH+1 as ACA_TTH, b.WorkNSaveINT_PH+1 as WorkNSaveINT_PH, 
# 			b.CAREGIVER_PH+1 as CAREGIVER_PH, 
# 			b.advo_pro_em+1 as advo_pro_em
# 	from intermed.geo_appends_rpm a 
# 	left join centiles b
# 	on input(a.merkleid,10.)=b.mid_key;
# quit;
df_geo_appends_rpm_updated.createOrReplaceTempView("geo_appends_rpm")
df_centiles.createOrReplaceTempView("centiles")

sql_query = """
    SELECT
        a.*,
        b.ACA_TTH + 1 as new_ACA_TTH,
        b.WorkNSaveINT_PH + 1 as new_WorkNSaveINT_PH,
        b.CAREGIVER_PH + 1 as new_CAREGIVER_PH,
        b.advo_pro_em + 1 as new_advo_pro_em
    FROM
        geo_appends_rpm a
    LEFT JOIN
        centiles b ON CAST(a.merkleid AS BIGINT) = b.mid_key
"""
df_joined = spark.sql(sql_query)

# Drop old columns if they exist and rename new ones to match SAS behavior
cols_to_replace = ['ACA_TTH', 'WorkNSaveINT_PH', 'CAREGIVER_PH', 'advo_pro_em']
df_temp = df_joined
for col_name in cols_to_replace:
    if col_name in df_joined.columns:
        df_temp = df_temp.drop(col_name)
    df_temp = df_temp.withColumnRenamed(f"new_{col_name}", col_name)

df_final_geo_appends_rpm = df_temp
df_final_geo_appends_rpm.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")


# SAS:
# proc freq data = intermed.geo_appends_rpm;
# tables ACA_TTH WorkNSaveINT_PH CAREGIVER_PH advo_pro_em/ missing;
# TITLE "Frequency Distribution for Advocacy Model Score Segments: &runtype &MULDATE Data";
# run;

# Reload the final table to run freq counts on it
df_final_table_for_freq = spark.table("intermed.geo_appends_rpm")

print("ODS ESCAPECHAR='^';")
print("OPTIONS NODATE NUMBER CENTER;")
print(f"TITLE1 J=R '^S={{PREIMAGE=\"/vg02/aarp_sas/aarp_projects/prfl_rptg/AARPLOGO2.gif\"}}';")
print("TITLE2 J=L \"AARP Advocacy DM Scoring Diagnostic Report\";")
print(f"TITLE4 J=L \"Data as of {runtype} {muldate}\";")
print("FOOTNOTE \"MDSS - FOR INTERNAL USE ONLY\";")

print(f"\nFrequency Distribution for Advocacy Model Score Segments: {runtype} {muldate} Data\n")

freq_cols = ["ACA_TTH", "WorkNSaveINT_PH", "CAREGIVER_PH", "advo_pro_em"]
for col in freq_cols:
    print(f"--- Frequency for {col} ---")
    df_final_table_for_freq.groupBy(col).count().orderBy(col).show()
#End-DBShift