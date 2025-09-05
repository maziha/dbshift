import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import math

spark = SparkSession.builder.appName("models_st_20210412").getOrCreate()

# Reading source tables
df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_contact_history_sum = spark.table("intermed.contact_history_sum")
df_web_visits = spark.table("intermed.web_visits")

# Merging tables as per the DATA step MERGE statement
# This simulates a left-join based merge where only records from the first table are kept (if a;)
df_merged = df_geo_appends_rpm.join(
    df_contact_history_sum,
    on="mid_key",
    how="left"
).join(
    df_web_visits,
    on="mid_key",
    how="left"
)

# Applying all transformations from the DATA step
# The SAS DATA step implicitly calculates on a row-by-row basis.
# In PySpark, we create new columns based on transformations of existing ones.

df_transformed = df_merged.withColumn("num_clicks1mos_mfg", F.coalesce(F.col("num_clicks1mos_mfg"), F.lit(0)))
df_transformed = df_transformed.withColumn("cens_age_pop_percent_50_54", F.coalesce(F.col("cens_age_pop_percent_50_54"), F.lit(6.7888677)))
df_transformed = df_transformed.withColumn("cens_employ_pop18_plus_percent_z", F.coalesce(F.col("cens_employ_pop18_plus_percent_c"), F.lit(8.3904244)))
df_transformed = df_transformed.withColumn("cens_ethnic_pop_percent_white__c", F.coalesce(F.col("cens_ethnic_pop_percent_white_on"), F.lit(76.6362561)))
df_transformed = df_transformed.withColumn("cens_inc_family_inc_state_deci_c", F.coalesce(F.col("cens_inc_family_inc_state_decile"), F.lit(5.0304875)))
df_transformed = df_transformed.withColumn("cens_occup_empld_percent_persona", F.coalesce(F.col("cens_occup_empld_percent_persona"), F.lit(3.3249136)))
df_transformed = df_transformed.withColumn("cntct_lifstyle_12mo_agg_hhd", F.coalesce(F.col("cntct_lifstyle_12mo_agg_hhd"), F.lit(0)))
df_transformed = df_transformed.withColumn("individual_engagers_12mo", F.coalesce(F.col("individual_engagers_12mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("life_engage_svcprov_12mo", F.coalesce(F.col("life_engage_svcprov_12mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("mailercount_click_30days", F.coalesce(F.col("mailercount_click_30days"), F.lit(0)))
df_transformed = df_transformed.withColumn("state_activities_12mo", F.coalesce(F.col("state_activities_12mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("aa", F.when(F.col("diversity_flag_agg_ind") == 2, 1).otherwise(0))
df_transformed = df_transformed.withColumn("act_model", F.when(F.col("general_activist_model") >= 55, 1).otherwise(0))
df_transformed = df_transformed.withColumn("gun_model", F.when((F.col("gun_ownership_model") * 160) <= 50, 1).otherwise(0))
df_transformed = df_transformed.withColumn("ibx_community_conser1", F.when(F.col("ibx_community_involvement_conser") == '1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("ibx_home_equity_hhKLM", F.when(F.col("ibx_home_equity_available_agg_hh").isin('D', 'F'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("ibx_telecom_calling_services_456", F.when(F.col("ibx_telecom_calling_services_agg").isin('04', '05', '06'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("ibx_telecom_internet_agg_hhd10", F.when(F.col("ibx_telecom_internet_agg_hhd") == '10', 1).otherwise(0))
df_transformed = df_transformed.withColumn("advocacy_donations_12mo", F.coalesce(F.col("advocacy_donations_12mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("attended_aarp_event", F.coalesce(F.col("attended_aarp_event"), F.lit(0)))
df_transformed = df_transformed.withColumn("num_open_30days", F.coalesce(F.col("num_open_30days"), F.lit(0)))
df_transformed = df_transformed.withColumn("click_rate_1mos", F.when((F.col("num_open_30days").isNull()) | (F.col("num_open_30days") == 0), 0).otherwise(F.col("num_clicked_curmonth") / F.col("num_open_30days")))
df_transformed = df_transformed.withColumn("click_rate_1mos", F.coalesce(F.col("click_rate_1mos"), F.lit(0)))
df_transformed = df_transformed.withColumn("cntct_lifstyle_3mo_agg_hhd", F.coalesce(F.col("cntct_lifstyle_3mo_agg_hhd"), F.lit(0)))
df_transformed = df_transformed.withColumn("educ_model", F.when((F.col("educational_attainment_model") * 100).between(1, 20), 1).otherwise(0))
df_transformed = df_transformed.withColumn("fis_model", F.when(F.col("fiscal_policy_model").between(61, 87), 1).otherwise(0))
df_transformed = df_transformed.withColumn("mailercount_open_1_3mo", F.coalesce(F.col("mailercount_open_1_3mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("mailercount_open_30days", F.coalesce(F.col("mailercount_open_30days"), F.lit(0)))
df_transformed = df_transformed.withColumn("mailercount_open_3_6mo", F.coalesce(F.col("mailercount_open_3_6mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("num_health_visits_past_3months", F.coalesce(F.col("num_health_visits_past_3months"), F.lit(0)))
df_transformed = df_transformed.withColumn("orders_online", F.coalesce(F.col("orders_online"), F.lit(0)))
df_transformed = df_transformed.withColumn("cens_age_pop_percent_25_34", F.coalesce(F.col("cens_age_pop_percent_25_34"), F.lit(12.4839099)))
df_transformed = df_transformed.withColumn("cens_age_pop_percent_35_39", F.coalesce(F.col("cens_age_pop_percent_35_39"), F.lit(6.1643274)))
df_transformed = df_transformed.withColumn("cens_density_population_per_squa", F.coalesce(F.col("cens_density_population_per_squa"), F.lit(2901.98)))
df_transformed = df_transformed.withColumn("cens_earn_hh_percent_no_public_a", F.coalesce(F.col("cens_earn_hh_percent_no_public_a"), F.lit(97.8605537)))
df_transformed = df_transformed.withColumn("cens_indus_empld_percent_educati", F.coalesce(F.col("cens_indus_empld_percent_educati"), F.lit(9.6525155)))
df_transformed = df_transformed.withColumn("cens_lang_hh_percent_span_speak_", F.coalesce(F.col("cens_lang_hh_percent_span_speak_"), F.lit(0.7820375)))
df_transformed = df_transformed.withColumn("cens_tenancy_hu_percent_occupied", F.coalesce(F.col("cens_tenancy_hu_percent_occupied"), F.lit(89.636239)))
df_transformed = df_transformed.withColumn("live_answer_aft", F.coalesce(F.col("live_answer_aft"), F.lit(50)))
df_transformed = df_transformed.withColumn("Past3_AARP", F.when(F.col("Past3MoTouchCt_AARP").isin('5', '6', '7', '8', '9'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("ibx_community_aid_ag1", F.when(F.col("ibx_community_involvement_aid_ag") == '1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("ibx_community_vetera1", F.when(F.col("ibx_community_involvement_vetera") == '1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("ibx_health_diet1", F.when(F.col("ibx_health_diet") == '1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("nps_detractor", F.coalesce(F.col("nps_detractor"), F.lit(50)))
df_transformed = df_transformed.withColumn("num_clicked_curmonth", F.coalesce(F.col("num_clicked_curmonth"), F.lit(0)))

df_transformed = df_transformed.withColumn("fnf_dance_logit", 
    F.lit(-4.8925) +
    F.col("num_clicks1mos_mfg") * 0.0536 +
    F.col("cens_age_pop_percent_50_54") * -0.0491 +
    F.col("cens_employ_pop18_plus_percent_z") * 0.0187 +
    F.col("cens_ethnic_pop_percent_white__c") * -0.00579 +
    F.col("cens_inc_family_inc_state_deci_c") * 0.0591 +
    F.col("cens_occup_empld_percent_persona") * 0.0201 +
    F.col("cntct_lifstyle_12mo_agg_hhd") * 0.0219 +
    F.col("individual_engagers_12mo") * 0.0875 +
    F.col("life_engage_svcprov_12mo") * -0.3362 +
    F.col("mailercount_click_30days") * 0.5175 +
    F.col("nps_detractor") * 0.0117 +
    F.col("num_clicked_curmonth") * -0.0288 +
    F.col("state_activities_12mo") * 0.0378 +
    F.col("aa") * 0.2758 +
    F.col("act_model") * 0.405 +
    F.col("gun_model") * 0.5657 +
    F.col("ibx_community_conser1") * 0.429 +
    F.col("ibx_home_equity_hhKLM") * 0.2518 +
    F.col("ibx_telecom_calling_services_456") * 0.1665 +
    F.col("ibx_telecom_internet_agg_hhd10") * 0.1823 +
    F.col("advocacy_donations_12mo") * -0.477 +
    F.col("attended_aarp_event") * 1.5961 +
    F.col("click_rate_1mos") * 0.1391 +
    F.col("cntct_lifstyle_3mo_agg_hhd") * -0.0379 +
    F.col("educ_model") * -0.2349 +
    F.col("fis_model") * -0.1452 +
    F.col("mailercount_open_1_3mo") * 0.0283 +
    F.col("mailercount_open_30days") * 0.0755 +
    F.col("mailercount_open_3_6mo") * -0.0606 +
    F.col("num_health_visits_past_3months") * 0.0354 +
    F.col("orders_online") * -0.1204 +
    F.col("cens_age_pop_percent_25_34") * -0.0576 +
    F.col("cens_age_pop_percent_35_39") * 0.1098 +
    F.col("cens_density_population_per_squa") * 0.00000363 +
    F.col("cens_earn_hh_percent_no_public_a") * -0.0175 +
    F.col("cens_indus_empld_percent_educati") * 0.0153 +
    F.col("cens_lang_hh_percent_span_speak_") * 0.0292 +
    F.col("cens_tenancy_hu_percent_occupied") * -0.0105 +
    F.col("live_answer_aft") * -0.0025 +
    F.col("Past3_AARP") * 0.1805 +
    F.col("ibx_community_aid_ag1") * 0.3984 +
    F.col("ibx_community_vetera1") * -0.2246 +
    F.col("ibx_health_diet1") * -1.0299
)
df_transformed = df_transformed.withColumn("fnf_dance_score", F.exp(F.col("fnf_dance_logit")) / (F.lit(1) + F.exp(F.col("fnf_dance_logit"))))

# MFG_Virtual_Screenings model calculations
df_transformed = df_transformed.withColumn("EM_Clickrate", F.coalesce(F.col("EM_Clickrate"), F.lit(0)))
df_transformed = df_transformed.withColumn("cens_density_persons_per_hh_fo_c", F.coalesce(F.col("cens_density_persons_per_hh_for_"), F.lit(2.531332)))
df_transformed = df_transformed.withColumn("cens_earn_hh_percent_with_self_c", F.coalesce(F.col("cens_earn_hh_percent_with_self_e"), F.lit(11.1623184)))
df_transformed = df_transformed.withColumn("cens_ethnic_pop_percent_hi_nat_c", F.coalesce(F.col("cens_ethnic_pop_percent_hi_nat_o"), F.lit(0.1672356)))
df_transformed = df_transformed.withColumn("cens_heat_occhu_percent_oil_or_c", F.coalesce(F.col("cens_heat_occhu_percent_oil_or_k"), F.lit(8.8542167)))
df_transformed = df_transformed.withColumn("cens_hhsize_hh_percent_2_perso_c", F.coalesce(F.col("cens_hhsize_hh_percent_2_persons"), F.lit(35.2308658)))
df_transformed = df_transformed.withColumn("cens_occup_empld_percent_product", F.coalesce(F.col("cens_occup_empld_percent_product"), F.lit(6.192662)))
df_transformed = df_transformed.withColumn("live_answer_am", F.coalesce(F.col("live_answer_am"), F.lit(50)))
df_transformed = df_transformed.withColumn("live_answer_ct", F.coalesce(F.col("live_answer_ct"), F.lit(50)))
df_transformed = df_transformed.withColumn("live_answer_pm", F.coalesce(F.col("live_answer_pm"), F.lit(50)))
df_transformed = df_transformed.withColumn("num_clicked_3_6mo", F.coalesce(F.col("num_clicked_3_6mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("rpm2019_centile", F.coalesce(F.col("rpm2019_centile"), F.lit(50)))
df_transformed = df_transformed.withColumn("Chase_Num_Active", F.when(F.col("Chase_Num_Active_Particpnts") == '1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("gt74", F.when((F.col("age_agg_ind") > 70) & (F.col("age_agg_ind") <= 81), 1).otherwise(0))
df_transformed = df_transformed.withColumn("ibx_child_age_11_15_agg_hhd1", F.when(F.col("ibx_child_age_11_15_agg_hhd") == '1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("ibx_community_libera1", F.when(F.col("ibx_community_involvement_libera") == '1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("ibx_networth_12345", F.when(F.col("ibx_networth_premier_agg_hhd").isin('1', '2', '3', '4', '5'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("lifestage68", F.when(F.col("lifestage_segment").isin('6', '8'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("low_income", F.when(F.col("income_model").isin('$20,000 - $30,000', '$30,000 - $50,000', 'Less than $20,000'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("no_hunter", F.when(F.col("hunter_model_char") == 'Unlikely Hunter', 1).otherwise(0))
df_transformed = df_transformed.withColumn("Tele_Townhall_Engagers", F.coalesce(F.col("Tele_Townhall_Engagers"), F.lit(0)))
df_transformed = df_transformed.withColumn("advocacy_donor", F.coalesce(F.col("advocacy_donor"), F.lit(0)))
df_transformed = df_transformed.withColumn("advocacy_grassroots_engager", F.coalesce(F.col("advocacy_grassroots_engager"), F.lit(0)))
df_transformed = df_transformed.withColumn("advocacy_petitions_12mo", F.coalesce(F.col("advocacy_petitions_12mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("num_open_1_3mo", F.coalesce(F.col("num_open_1_3mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("click_rate_1_3mos", F.when((F.col("num_open_1_3mo").isNull()) | (F.col("num_open_1_3mo") == 0), 0).otherwise(F.col("num_clicked_1_3mo") / F.col("num_open_1_3mo")))
df_transformed = df_transformed.withColumn("click_rate_1_3mos", F.coalesce(F.col("click_rate_1_3mos"), F.lit(0)))
df_transformed = df_transformed.withColumn("driver_class_12mo", F.coalesce(F.col("driver_class_12mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("liveanswer_freq6_12", F.coalesce(F.col("liveanswer_freq6_12"), F.lit(0)))
df_transformed = df_transformed.withColumn("mailercount_click_1_3mo", F.coalesce(F.col("mailercount_click_1_3mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("mailercount_click_3_6mo", F.coalesce(F.col("mailercount_click_3_6mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("num_curr_participation_financial", F.coalesce(F.col("num_curr_participation_financial"), F.lit(0)))
df_transformed = df_transformed.withColumn("num_entertainment_visits_past_3m", F.coalesce(F.col("num_entertainment_visits_past_3m"), F.lit(0)))
df_transformed = df_transformed.withColumn("orders_all", F.coalesce(F.col("orders_all"), F.lit(0)))
df_transformed = df_transformed.withColumn("Fndn_TTD_Num", F.coalesce(F.col("Fndn_TTD_Num"), F.lit(0)))
df_transformed = df_transformed.withColumn("cens_age_pop_percent_60_64", F.coalesce(F.col("cens_age_pop_percent_60_64"), F.lit(7.0104361)))
df_transformed = df_transformed.withColumn("cens_educ_pop25_plus_median_ed_c", F.coalesce(F.col("cens_educ_pop25_plus_median_educ"), F.lit(12.5887527)))
df_transformed = df_transformed.withColumn("cens_indus_empld_percent_minin_c", F.coalesce(F.col("cens_indus_empld_percent_mining"), F.lit(1.4145076)))
df_transformed = df_transformed.withColumn("cens_indus_empld_percent_wholesa", F.coalesce(F.col("cens_indus_empld_percent_wholesa"), F.lit(2.6300329)))
df_transformed = df_transformed.withColumn("cens_lang_hh_percent_spanish_spe", F.coalesce(F.col("cens_lang_hh_percent_spanish_spe"), F.lit(4.2812996)))
df_transformed = df_transformed.withColumn("cens_occup_empld_percent_law_enf", F.coalesce(F.col("cens_occup_empld_percent_law_enf"), F.lit(1.0718944)))
df_transformed = df_transformed.withColumn("cens_occup_empld_percent_motor_v", F.coalesce(F.col("cens_occup_empld_percent_motor_v"), F.lit(3.159839)))
df_transformed = df_transformed.withColumn("cens_tenancy_occhu_percent_owner", F.coalesce(F.col("cens_tenancy_occhu_percent_owner"), F.lit(73.0532289)))
df_transformed = df_transformed.withColumn("cens_typ_pop_percent_grandchild_", F.coalesce(F.col("cens_typ_pop_percent_grandchild_"), F.lit(2.6667499)))
df_transformed = df_transformed.withColumn("cens_typ_pop_percent_stepchild_i", F.coalesce(F.col("cens_typ_pop_percent_stepchild_i"), F.lit(1.4904252)))
df_transformed = df_transformed.withColumn("health_medical_supplies_orth1", F.when(F.col("ibx_health_medical_supplies_orth") == '1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("health_y", F.when(F.col("health_ind") == 'Y', 1).otherwise(0))
df_transformed = df_transformed.withColumn("ibx_grand_children_agg_hhdy", F.when(F.col("ibx_grand_children_agg_hhd") == 'Y', 1).otherwise(0))
df_transformed = df_transformed.withColumn("ibx_health_vitamins_nutrition1", F.when(F.col("ibx_health_vitamins_nutrition") == '1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("ibx_occupation_input_agg_hhd789b", F.when(F.col("ibx_occupation_input_agg_hhd").isin('7', '8', '9'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("past3_Health", F.when(F.col("Past3MoTouchCt_Health").isin('6', '7', '8', '9'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("probdead", F.when(F.col("deadwood_model") == 'PROBDEAD', 1).otherwise(0))

df_transformed = df_transformed.withColumn("MFG_Virtual_Screenings_logit", 
    F.lit(-5.8751) +
    F.col("EM_Clickrate") * -0.0299 +
    F.col("num_clicks1mos_mfg") * 0.0813 +
    F.col("cens_density_persons_per_hh_fo_c") * -0.2055 +
    F.col("cens_earn_hh_percent_with_self_c") * 0.00636 +
    F.col("cens_ethnic_pop_percent_hi_nat_c") * 0.0293 +
    F.col("cens_heat_occhu_percent_oil_or_c") * -0.00361 +
    F.col("cens_hhsize_hh_percent_2_perso_c") * -0.00346 +
    F.col("cens_occup_empld_percent_product") * -0.00616 +
    F.col("cntct_lifstyle_12mo_agg_hhd") * 0.0139 +
    F.col("individual_engagers_12mo") * 0.4337 +
    F.col("live_answer_am") * 0.00146 +
    F.col("live_answer_ct") * 0.00174 +
    F.col("live_answer_pm") * -0.00243 +
    F.col("mailercount_click_30days") * 0.388 +
    F.col("nps_detractor") * 0.017 +
    F.col("num_clicked_curmonth") * -0.0222 +
    F.col("num_clicked_3_6mo") * -0.00897 +
    F.col("num_open_30days") * 0.00834 +
    F.col("rpm2019_centile") * 0.00111 +
    F.col("state_activities_12mo") * 0.1023 +
    F.col("Chase_Num_Active") * 0.1587 +
    F.col("aa") * -0.0677 +
    F.col("act_model") * 0.1876 +
    F.col("gt74") * -0.2399 +
    F.col("gun_model") * 0.527 +
    F.col("ibx_child_age_11_15_agg_hhd1") * 0.0843 +
    F.col("ibx_community_libera1") * 0.1379 +
    F.col("ibx_networth_12345") * -0.1321 +
    F.col("lifestage68") * -0.1366 +
    F.col("low_income") * -0.8203 +
    F.col("no_hunter") * 0.1293 +
    F.col("Tele_Townhall_Engagers") * -0.1486 +
    F.col("advocacy_donations_12mo") * -0.2919 +
    F.col("advocacy_donor") * -0.46 +
    F.col("advocacy_grassroots_engager") * -0.4999 +
    F.col("advocacy_petitions_12mo") * -0.18 +
    F.col("click_rate_1_3mos") * -0.0602 +
    F.col("driver_class_12mo") * -0.2759 +
    F.col("educ_model") * -0.0984 +
    F.col("fis_model") * -0.1226 +
    F.col("liveanswer_freq6_12") * -0.1287 +
    F.col("mailercount_click_1_3mo") * 0.0616 +
    F.col("mailercount_click_3_6mo") * -0.0793 +
    F.col("mailercount_open_30days") * 0.0223 +
    F.col("num_curr_participation_financial") * -0.1766 +
    F.col("num_entertainment_visits_past_3m") * 0.0402 +
    F.col("num_health_visits_past_3months") * 0.012 +
    F.col("num_open_1_3mo") * -0.011 +
    F.col("orders_all") * -0.1789 +
    F.col("Fndn_TTD_Num") * -0.05 +
    F.col("cens_age_pop_percent_60_64") * -0.0205 +
    F.col("cens_density_population_per_squa") * 0.000001656 +
    F.col("cens_educ_pop25_plus_median_ed_c") * 0.097 +
    F.col("cens_indus_empld_percent_minin_c") * -0.0158 +
    F.col("cens_indus_empld_percent_wholesa") * -0.00802 +
    F.col("cens_lang_hh_percent_spanish_spe") * 0.00389 +
    F.col("cens_occup_empld_percent_law_enf") * -0.0146 +
    F.col("cens_occup_empld_percent_motor_v") * -0.00733 +
    F.col("cens_tenancy_occhu_percent_owner") * 0.00131 +
    F.col("cens_typ_pop_percent_grandchild_") * 0.0272 +
    F.col("cens_typ_pop_percent_stepchild_i") * -0.0658 +
    F.col("live_answer_aft") * 0.00194 +
    F.col("Past3_AARP") * 0.1096 +
    F.col("health_medical_supplies_orth1") * -0.1886 +
    F.col("health_y") * -0.326 +
    F.col("ibx_community_aid_ag1") * 0.2502 +
    F.col("ibx_community_vetera1") * -0.1726 +
    F.col("ibx_grand_children_agg_hhdy") * -0.1193 +
    F.col("ibx_health_vitamins_nutrition1") * -0.3101 +
    F.col("ibx_occupation_input_agg_hhd789b") * -0.1031 +
    F.col("past3_Health") * 0.074 +
    F.col("probdead") * -0.3788
)
df_transformed = df_transformed.withColumn("MFG_Virtual_Screenings_score", F.exp(F.col("MFG_Virtual_Screenings_logit")) / (F.lit(1) + F.exp(F.col("MFG_Virtual_Screenings_logit"))))

# Veterans attendee model calculations
df_transformed = df_transformed.withColumn("ideology_c", F.coalesce(F.col("ideology"), F.lit(48.1292927)))
df_transformed = df_transformed.withColumn("SecAge_c", F.coalesce(F.col("SecAge"), F.lit(67.0979155)))
df_transformed = df_transformed.withColumn("sy_otsbn_polfund_2012b_c", F.coalesce(F.col("sy_otsbn_polfund_2012b"), F.lit(42.2381851)))
df_transformed = df_transformed.withColumn("DRVS_Flag_c", F.coalesce(F.col("DRVS_Flag"), F.lit(0)))
df_transformed = df_transformed.withColumn("Advo_Last_Amt_c", F.coalesce(F.col("Advo_Last_Amt"), F.lit(0)))
df_transformed = df_transformed.withColumn("partisanscore_c", F.coalesce(F.col("partisanscore"), F.lit(54.0456007)))
df_transformed = df_transformed.withColumn("cens_age_pop_median_age_of_fem_c", F.coalesce(F.col("cens_age_pop_median_age_of_femal"), F.lit(43.2814198)))
df_transformed = df_transformed.withColumn("cens_age_pop_percent_45_54_c", F.coalesce(F.col("cens_age_pop_percent_45_54"), F.lit(13.1837453)))
df_transformed = df_transformed.withColumn("cens_built_hu_percent_built_20_c", F.coalesce(F.col("cens_built_hu_percent_built_2000"), F.lit(15.3419027)))
df_transformed = df_transformed.withColumn("cens_commute_commuter_avg_trav_c", F.coalesce(F.col("cens_commute_commuter_avg_trav_t"), F.lit(27.7621239)))
df_transformed = df_transformed.withColumn("cens_commute_commuter_percent__c", F.coalesce(F.col("cens_commute_commuter_percent_tr"), F.lit(64.4022274)))
df_transformed = df_transformed.withColumn("cens_commute_wrkrs_percent_car_c", F.coalesce(F.col("cens_commute_wrkrs_percent_carpo"), F.lit(9.1667876)))
df_transformed = df_transformed.withColumn("cens_commute_wrkrs_percent_pub_c", F.coalesce(F.col("cens_commute_wrkrs_percent_publi"), F.lit(3.6961493)))
df_transformed = df_transformed.withColumn("cens_density_persons_per_hh_fo_c", F.coalesce(F.col("cens_density_persons_per_hh_for_"), F.lit(2.5199497)))
df_transformed = df_transformed.withColumn("cens_earn_hh_percent_with_publ_c", F.coalesce(F.col("cens_earn_hh_percent_with_public"), F.lit(2.1694009)))
df_transformed = df_transformed.withColumn("cens_educ_pop25_plus_median_ed_c", F.coalesce(F.col("cens_educ_pop25_plus_median_educ"), F.lit(12.6667126)))
df_transformed = df_transformed.withColumn("cens_educ_pop25_plus_percent_p_c", F.coalesce(F.col("cens_educ_pop25_plus_percent_pro"), F.lit(12.1956853)))
df_transformed = df_transformed.withColumn("cens_employ_labf_percent_emplo_c", F.coalesce(F.col("cens_employ_labf_percent_employe"), F.lit(95.6085827)))
df_transformed = df_transformed.withColumn("cens_employ_pop18_plus_percent_z", F.coalesce(F.col("cens_employ_pop18_plus_percent_c"), F.lit(8.3876098)))
df_transformed = df_transformed.withColumn("cens_ethnic_pop_percent_black__c", F.coalesce(F.col("cens_ethnic_pop_percent_black_on"), F.lit(11.0157326)))
df_transformed = df_transformed.withColumn("cens_ethnic_pop_percent_hi_nat_c", F.coalesce(F.col("cens_ethnic_pop_percent_hi_nat_o"), F.lit(0.1512596)))
df_transformed = df_transformed.withColumn("cens_ethnic_pop_percent_non_hi_c", F.coalesce(F.col("cens_ethnic_pop_percent_non_hisp"), F.lit(87.9354692)))
df_transformed = df_transformed.withColumn("cens_ethnic_pop_percent_some_o_c", F.coalesce(F.col("cens_ethnic_pop_percent_some_oth"), F.lit(4.2066218)))
df_transformed = df_transformed.withColumn("cens_ethnic_pop_percent_white__c", F.coalesce(F.col("cens_ethnic_pop_percent_white_on"), F.lit(76.3328691)))
df_transformed = df_transformed.withColumn("cens_gender_pop_percent_female_c", F.coalesce(F.col("cens_gender_pop_percent_female"), F.lit(51.2550157)))
df_transformed = df_transformed.withColumn("cens_heat_occhu_percent_oil_or_c", F.coalesce(F.col("cens_heat_occhu_percent_oil_or_k"), F.lit(6.0467233)))
df_transformed = df_transformed.withColumn("cens_heat_occhu_percent_utilit_c", F.coalesce(F.col("cens_heat_occhu_percent_utility_"), F.lit(46.9878519)))
df_transformed = df_transformed.withColumn("cens_hhsize_hh_percent_2_perso_c", F.coalesce(F.col("cens_hhsize_hh_percent_2_persons"), F.lit(35.0144644)))
df_transformed = df_transformed.withColumn("cens_homval_home_value_cbsa_in_c", F.coalesce(F.col("cens_homval_home_value_cbsa_inde"), F.lit(106.520917)))
df_transformed = df_transformed.withColumn("cens_homval_oohu_median_home_v_c", F.coalesce(F.col("cens_homval_oohu_median_home_val"), F.lit(262147.93)))
df_transformed = df_transformed.withColumn("cens_homval_oohu_percent_home__c", F.coalesce(F.col("cens_homval_oohu_percent_home_va"), F.lit(0.8938596)))
df_transformed = df_transformed.withColumn("cens_hustr_hu_percent_2_units_c", F.coalesce(F.col("cens_hustr_hu_percent_2_units"), F.lit(3.0382998)))
df_transformed = df_transformed.withColumn("cens_inc_family_inc_state_deci_c", F.coalesce(F.col("cens_inc_family_inc_state_decile"), F.lit(4.75708)))
df_transformed = df_transformed.withColumn("cens_inc_hh_median_family_hous_c", F.coalesce(F.col("cens_inc_hh_median_family_househ"), F.lit(84140)))
df_transformed = df_transformed.withColumn("cens_inc_hh_median_household_i_c", F.coalesce(F.col("cens_inc_hh_median_household_inc"), F.lit(70788.13)))
df_transformed = df_transformed.withColumn("cens_indus_empld_percent_finan_c", F.coalesce(F.col("cens_indus_empld_percent_finance"), F.lit(4.6524719)))
df_transformed = df_transformed.withColumn("cens_indus_empld_percent_manuf_c", F.coalesce(F.col("cens_indus_empld_percent_manufac"), F.lit(10.4096733)))
df_transformed = df_transformed.withColumn("cens_mortg_oohu_percent_no_mor_c", F.coalesce(F.col("cens_mortg_oohu_percent_no_mortg"), F.lit(36.8553594)))
df_transformed = df_transformed.withColumn("cens_occup_empld_percent_sales_c", F.coalesce(F.col("cens_occup_empld_percent_sales_a"), F.lit(10.949081)))
df_transformed = df_transformed.withColumn("cens_grpqtrs_pop_percent_colle_c", F.coalesce(F.col("cens_grpqtrs_pop_percent_college"), F.lit(0.2754903)))
df_transformed = df_transformed.withColumn("cens_indus_empld_percent_hlth__c", F.coalesce(F.col("cens_indus_empld_percent_hlth_ca"), F.lit(13.9804432)))
df_transformed = df_transformed.withColumn("cens_grpqtrs_pop_percent_nursi_c", F.coalesce(F.col("cens_grpqtrs_pop_percent_nursing"), F.lit(0.5382364)))
df_transformed = df_transformed.withColumn("cens_hustr_hu_percent_1_unit_d_c", F.coalesce(F.col("cens_hustr_hu_percent_1_unit_det"), F.lit(67.6395125)))
df_transformed = df_transformed.withColumn("cens_occup_empld_percent_farm__c", F.coalesce(F.col("cens_occup_empld_percent_farm_fi"), F.lit(0.7420326)))
df_transformed = df_transformed.withColumn("cens_occup_empld_percent_manag_c", F.coalesce(F.col("cens_occup_empld_percent_managem"), F.lit(10.8403697)))
df_transformed = df_transformed.withColumn("cens_occup_empld_percent_compu_c", F.coalesce(F.col("cens_occup_empld_percent_compute"), F.lit(2.5269442)))
df_transformed = df_transformed.withColumn("cens_occup_empld_percent_life__c", F.coalesce(F.col("cens_occup_empld_percent_life_ph"), F.lit(0.8832802)))
df_transformed = df_transformed.withColumn("cens_ethnic_pop_percent_asian__c", F.coalesce(F.col("cens_ethnic_pop_percent_asian_on"), F.lit(4.4497584)))
df_transformed = df_transformed.withColumn("cens_census_tract_c", F.coalesce(F.col("cens_census_tract"), F.lit(262755.72)))
df_transformed = df_transformed.withColumn("cens_educ_pop25_plus_percent_a_c", F.coalesce(F.col("cens_educ_pop25_plus_percent_ass"), F.lit(8.5041572)))
df_transformed = df_transformed.withColumn("cens_indus_empld_percent_minin_c", F.coalesce(F.col("cens_indus_empld_percent_mining"), F.lit(0.5585652)))
df_transformed = df_transformed.withColumn("cens_move_occhu_percent_turnov_c", F.coalesce(F.col("cens_move_occhu_percent_turnover"), F.lit(27.300389)))
df_transformed = df_transformed.withColumn("cens_earn_hh_percent_with_self_c", F.coalesce(F.col("cens_earn_hh_percent_with_self_e"), F.lit(11.2465211)))
df_transformed = df_transformed.withColumn("cens_age_pop_percent_30_34_c", F.coalesce(F.col("cens_age_pop_percent_30_34"), F.lit(5.9939435)))
df_transformed = df_transformed.withColumn("cens_heat_occhu_percent_other__c", F.coalesce(F.col("cens_heat_occhu_percent_other_he"), F.lit(0.5310901)))
df_transformed = df_transformed.withColumn("cens_indus_empld_percent_trans_c", F.coalesce(F.col("cens_indus_empld_percent_transpo"), F.lit(4.1645598)))
df_transformed = df_transformed.withColumn("cens_mortg_oohu_percent_two_mr_c", F.coalesce(F.col("cens_mortg_oohu_percent_two_mrtg"), F.lit(0.4497298)))
df_transformed = df_transformed.withColumn("cens_ethnic_hh_percent_hoh_his_c", F.coalesce(F.col("cens_ethnic_hh_percent_hoh_hispa"), F.lit(9.2315796)))
df_transformed = df_transformed.withColumn("num_curr_participation_discoun_c", F.coalesce(F.col("num_curr_participation_discounts"), F.lit(0)))
df_transformed = df_transformed.withColumn("november_general_election_day__c", F.coalesce(F.col("november_general_election_day_ag"), F.lit(71.4577884)))
df_transformed = df_transformed.withColumn("mail_readership_model_c", F.coalesce(F.col("mail_readership_model"), F.lit(67.0635022)))
df_transformed = df_transformed.withColumn("educational_attainment_model_c", F.coalesce(F.col("educational_attainment_model")*100, F.lit(31.6859209)))
df_transformed = df_transformed.withColumn("fiscal_policy_model_c", F.coalesce(F.col("fiscal_policy_model"), F.lit(48.8443754)))
df_transformed = df_transformed.withColumn("sy_otsbn_polfund_c", F.coalesce(F.col("sy_otsbn_polfund"), F.lit(11.0391933)))
df_transformed = df_transformed.withColumn("general_election_vote_propensi_c", F.coalesce(F.col("general_election_vote_propensity"), F.lit(73.1557013)))
df_transformed = df_transformed.withColumn("age_agg_ind_c", F.coalesce(F.col("age_agg_ind"), F.lit(71.436516)))
df_transformed = df_transformed.withColumn("likely_hisp_agg_c", F.coalesce(F.col("likely_hisp_agg"), F.lit(95.3701163)))
df_transformed = df_transformed.withColumn("num_giving_back_visits_past_3m_c", F.coalesce(F.col("num_giving_back_visits_past_3mon"), F.lit(0)))
df_transformed = df_transformed.withColumn("dm_c", F.coalesce(F.col("dm"), F.lit(0)))
df_transformed = df_transformed.withColumn("phn_c", F.coalesce(F.col("phn"), F.lit(0)))
df_transformed = df_transformed.withColumn("engaged_aarp_event_c", F.coalesce(F.col("engaged_aarp_event"), F.lit(0)))
df_transformed = df_transformed.withColumn("orders_36moterm_c", F.coalesce(F.col("orders_36moterm"), F.lit(0)))
df_transformed = df_transformed.withColumn("orders_60moterm_c", F.coalesce(F.col("orders_60moterm"), F.lit(0)))
df_transformed = df_transformed.withColumn("orders_altmedia_c", F.coalesce(F.col("orders_altmedia"), F.lit(0)))
df_transformed = df_transformed.withColumn("orders_online_c", F.coalesce(F.col("orders_online"), F.lit(0)))
df_transformed = df_transformed.withColumn("orders_serviceprovider_c", F.coalesce(F.col("orders_serviceprovider"), F.lit(0)))
df_transformed = df_transformed.withColumn("tax_aid_vol_12mo_c", F.coalesce(F.col("tax_aid_vol_12mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("VOTEPROP2016_c", F.coalesce(F.col("VOTEPROP2016"), F.lit(73.1581184)))
df_transformed = df_transformed.withColumn("teletown_12mo_c", F.coalesce(F.col("teletown_12mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("moviesfg_12mo_c", F.coalesce(F.col("moviesfg_12mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("blockparty_12mo_c", F.coalesce(F.col("blockparty_12mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("state_event_12mo_c", F.coalesce(F.col("state_event_12mo"), F.lit(0)))
df_transformed = df_transformed.withColumn("teletown_12mo_i_c", F.coalesce(F.col("teletown_12mo_i"), F.lit(0)))
df_transformed = df_transformed.withColumn("chapters_vol_12mo_i_c", F.coalesce(F.col("chapters_vol_12mo_i"), F.lit(0)))
df_transformed = df_transformed.withColumn("states_vol_12mo_i_c", F.coalesce(F.col("states_vol_12mo_i"), F.lit(0)))
df_transformed = df_transformed.withColumn("contact_leg_12mo_i_c", F.coalesce(F.col("contact_leg_12mo_i"), F.lit(0)))
df_transformed = df_transformed.withColumn("petition_sign_12mo_i_c", F.coalesce(F.col("petition_sign_12mo_i"), F.lit(0)))
df_transformed = df_transformed.withColumn("survey_resp_12mo_i_c", F.coalesce(F.col("survey_resp_12mo_i"), F.lit(0)))
df_transformed = df_transformed.withColumn("structured_12mo_i_c", F.coalesce(F.col("structured_12mo_i"), F.lit(0)))
df_transformed = df_transformed.withColumn("state_event_12mo_i_c", F.coalesce(F.col("state_event_12mo_i"), F.lit(0)))
df_transformed = df_transformed.withColumn("religiosity_model_c", F.coalesce(F.col("religiosity_model"), F.lit(53.9710106)))
df_transformed = df_transformed.withColumn("likely_cell_assignment_score_c", F.coalesce(F.col("likely_cell_assignment_score"), F.lit(48.3423543)))
df_transformed = df_transformed.withColumn("likely_landline_connectivity_s_c", F.coalesce(F.col("likely_landline_connectivity_sco"), F.lit(83.7025154)))
df_transformed = df_transformed.withColumn("likely_landline_assignment_sco_c", F.coalesce(F.col("likely_landline_assignment_score"), F.lit(8.9957142)))
df_transformed = df_transformed.withColumn("race_confidence_numeric_c", F.coalesce(F.col("race_confidence_numeric"), F.lit(91.5412516)))
df_transformed = df_transformed.withColumn("Motorcycling_c", F.coalesce(F.col("Motorcycling").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("auto_work_c", F.coalesce(F.col("auto_work").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("boating_sailing_c", F.coalesce(F.col("boating_sailing").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("broader_living_c", F.coalesce(F.col("broader_living").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("childrens_interests_c", F.coalesce(F.col("childrens_interests").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("collectibles_antiques_c", F.coalesce(F.col("collectibles_antiques").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("collectibles_coins_c", F.coalesce(F.col("collectibles_coins").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("diy_living_c", F.coalesce(F.col("diy_living").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("education_online_c", F.coalesce(F.col("education_online").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("home_furnishings_decorating_c", F.coalesce(F.col("home_furnishings_decorating").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("ibx_adult_age_55_64_agg_hhd_c", F.coalesce(F.col("ibx_adult_age_55_64_agg_hhd").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("ibx_adult_age_75_p_agg_hhd_c", F.coalesce(F.col("ibx_adult_age_75_p_agg_hhd").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("ibx_community_involvement_caus_c", F.coalesce(F.col("ibx_community_involvement_causes").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("ibx_grandchildren_premier_c", F.coalesce(F.col("ibx_grandchildren_premier").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("ibx_health_diabetic_c", F.coalesce(F.col("ibx_health_diabetic").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("music_collector_c", F.coalesce(F.col("music_collector").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("reading_best_sellers_c", F.coalesce(F.col("reading_best_sellers").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("reading_financial_newsletter_s_c", F.coalesce(F.col("reading_financial_newsletter_sub").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("spectator_sports_auto_motorcyc_c", F.coalesce(F.col("spect_sports_motorcycle_racing").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("spectator_sports_football_c", F.coalesce(F.col("spectator_sports_football").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("spectator_sports_hockey_c", F.coalesce(F.col("spectator_sports_hockey").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("sweepstakes_contests_c", F.coalesce(F.col("sweeps_contests").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("tv_guide_c", F.coalesce(F.col("tv_guide_network").cast("double"), F.lit(0)))
df_transformed = df_transformed.withColumn("auto_renew_flag_c", F.when(F.col("auto_renew_flag") == 'Y', 1).otherwise(0))
df_transformed = df_transformed.withColumn("ideology_20_dum", F.when(F.col("ideology_c") <= 20, 1).otherwise(0))
df_transformed = df_transformed.withColumn("ideology_80_dum", F.when(F.col("ideology_c") >= 80, 1).otherwise(0))
df_transformed = df_transformed.withColumn("partisanship_20_dum", F.when(F.col("partisanscore_c") <= 20, 1).otherwise(0))
df_transformed = df_transformed.withColumn("networth_1_dum", F.when(F.col("ibx_networth_premier_agg_hhd") == '1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("networth_b_dum", F.when(F.col("ibx_networth_premier_agg_hhd") == 'B', 1).otherwise(0))
df_transformed = df_transformed.withColumn("chacq_u_dum", F.when(F.col("ch_acq") == 'U', 1).otherwise(0))
df_transformed = df_transformed.withColumn("curterm_36_dum", F.when(F.col("cur_term") == '36', 1).otherwise(0))
df_transformed = df_transformed.withColumn("curterm_60_dum", F.when(F.col("cur_term") == '60', 1).otherwise(0))
df_transformed = df_transformed.withColumn("female_dum", F.when(F.col("gender_agg_ind") == 'F', 1).otherwise(0))
df_transformed = df_transformed.withColumn("adults1", F.when(F.col("IBX_ADULTS_NUMBER_OF_HOUSEHOLD_P") == '1', 1).otherwise(0))
df_transformed = df_transformed.withColumn("adults2", F.when(F.col("IBX_ADULTS_NUMBER_OF_HOUSEHOLD_P") == '2', 1).otherwise(0))
df_transformed = df_transformed.withColumn("business_u", F.when(F.col("IBX_BUSINESS_OWNER_AGG_HHD") == 'U', 1).otherwise(0))
df_transformed = df_transformed.withColumn("business_x", F.when(F.col("IBX_BUSINESS_OWNER_AGG_HHD") == 'X', 1).otherwise(0))
df_transformed = df_transformed.withColumn("dwelling_m", F.when(F.col("IBX_DWELLING_TYPE") == 'M', 1).otherwise(0))
df_transformed = df_transformed.withColumn("education_4", F.when(F.col("IBX_EDUCATION") == '4', 4).otherwise(0))
df_transformed = df_transformed.withColumn("homerange_a", F.when(F.col("IBX_HOME_ASSESSED_VALUE_RANGES") == 'A', 1).otherwise(0))
df_transformed = df_transformed.withColumn("homerange_b", F.when(F.col("IBX_HOME_ASSESSED_VALUE_RANGES") == 'B', 1).otherwise(0))
df_transformed = df_transformed.withColumn("homerange_c", F.when(F.col("IBX_HOME_ASSESSED_VALUE_RANGES") == 'C', 1).otherwise(0))
df_transformed = df_transformed.withColumn("homerange_d", F.when(F.col("IBX_HOME_ASSESSED_VALUE_RANGES") == 'D', 1).otherwise(0))
df_transformed = df_transformed.withColumn("homerange_eplus", F.when(~F.col("IBX_HOME_ASSESSED_VALUE_RANGES").isin('', ' ', 'A', 'B', 'C', 'D'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("homeloan_atog", F.when(F.col("IBX_HOME_LOAN_AMOUNT_1_RANGES").isin('A', 'B', 'C', 'D', 'E', 'F', 'G'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("income_D", F.when(F.col("IBX_HOUSEHOLD_INCOME") == 'D', 1).otherwise(0))
df_transformed = df_transformed.withColumn("income_E", F.when(F.col("IBX_HOUSEHOLD_INCOME") == 'E', 1).otherwise(0))
df_transformed = df_transformed.withColumn("income_F", F.when(F.col("IBX_HOUSEHOLD_INCOME") == 'F', 1).otherwise(0))
df_transformed = df_transformed.withColumn("income_GtoJ", F.when(F.col("IBX_HOUSEHOLD_INCOME").isin('G', 'H', 'I', 'J'), 1).otherwise(0))
df_transformed = df_transformed.withColumn("occupation_7", F.when(F.col("IBX_OCCUPATION_1ST_INDIVIDUAL_PR") == '7', 1).otherwise(0))
df_transformed = df_transformed.withColumn("occupation_8", F.when(F.col("IBX_OCCUPATION_1ST_INDIVIDUAL_PR") == '8', 1).otherwise(0))
df_transformed = df_transformed.withColumn("ibx_party_d", F.when(F.col("IBX_POLITICAL_PARTY_INPUT_INDIVI") == 'D', 1).otherwise(0))
df_transformed = df_transformed.withColumn("ibx_party_r", F.when(F.col("IBX_POLITICAL_PARTY_INPUT_INDIVI") == 'R', 1).otherwise(0))
df_transformed = df_transformed.withColumn("lifestage_5", F.when(F.col("LIFESTAGE_SEGMENT") == '5', 1).otherwise(0))
df_transformed = df_transformed.withColumn("lifestage_6", F.when(F.col("LIFESTAGE_SEGMENT") == '6', 1).otherwise(0))
df_transformed = df_transformed.withColumn("lifestage_7", F.when(F.col("LIFESTAGE_SEGMENT") == '7', 1).otherwise(0))
df_transformed = df_transformed.withColumn("lifestage_8", F.when(F.col("LIFESTAGE_SEGMENT") == '8', 1).otherwise(0))
df_transformed = df_transformed.withColumn("maritalstat_s", F.when(F.col("maritalstatus") == 'S', 1).otherwise(0))
df_transformed = df_transformed.withColumn("member_primary", F.when(F.col("MEMBER_FL_AGG_IND") == 'P', 1).otherwise(0))
df_transformed = df_transformed.withColumn("member_secondary", F.when(F.col("MEMBER_FL_AGG_IND") == 'S', 1).otherwise(0))
df_transformed = df_transformed.withColumn("IBX_HOME_PURCHASE_DT_PREMIER_num", F.when((F.col("IBX_HOME_PURCHASE_DT_PREMIER_AGG") == "") | F.col("IBX_HOME_PURCHASE_DT_PREMIER_AGG").isNull(), 0).otherwise(F.col("IBX_HOME_PURCHASE_DT_PREMIER_AGG").cast("double")))
df_transformed = df_transformed.withColumn("IBX_HOME_YEAR_BUILT_ACTUAL_num", F.when((F.col("IBX_HOME_YEAR_BUILT_ACTUAL") == "") | F.col("IBX_HOME_YEAR_BUILT_ACTUAL").isNull(), 0).otherwise(F.col("IBX_HOME_YEAR_BUILT_ACTUAL").cast("double")))
df_transformed = df_transformed.withColumn("IBX_MOVIE_MUSIC_GROUPING_num", F.when((F.col("IBX_MOVIE_MUSIC_GROUPING") == "") | F.col("IBX_MOVIE_MUSIC_GROUPING").isNull(), 0).otherwise(F.col("IBX_MOVIE_MUSIC_GROUPING").cast("double")))
df_transformed = df_transformed.withColumn("IBX_RECREATIONAL_VEHICLES_PR_num", F.when((F.col("IBX_RECREATIONAL_VEHICLES_PREMIE") == "") | F.col("IBX_RECREATIONAL_VEHICLES_PREMIE").isNull(), 0).otherwise(F.col("IBX_RECREATIONAL_VEHICLES_PREMIE").cast("double")))
df_transformed = df_transformed.withColumn("IBX_TRAVEL_CRUISE_AGG_HHD_num", F.when((F.col("IBX_TRAVEL_CRUISE_AGG_HHD") == "") | F.col("IBX_TRAVEL_CRUISE_AGG_HHD").isNull(), 0).otherwise(F.col("IBX_TRAVEL_CRUISE_AGG_HHD").cast("double")))

df_transformed = df_transformed.withColumn("veterans_attendee_score_logit",
    F.lit(-0.8671) + 
    F.lit(0.00416) * F.col("SecAge_c") +
    F.lit(-0.00471) * F.col("sy_otsbn_polfund_2012b_c") +
    F.lit(0.139) * F.col("DRVS_Flag_c") +
    F.lit(-0.00608) * F.col("Advo_Last_Amt_c") +
    F.lit(0.00409) * F.col("partisanscore_c") +
    F.lit(-0.0293) * F.col("cens_age_pop_median_age_of_fem_c") +
    F.lit(-0.0468) * F.col("cens_age_pop_percent_45_54_c") +
    F.lit(0.0045) * F.col("cens_built_hu_percent_built_20_c") +
    F.lit(-0.0439) * F.col("cens_commute_commuter_avg_trav_c") +
    F.lit(-0.00691) * F.col("cens_commute_commuter_percent__c") +
    F.lit(0.00809) * F.col("cens_commute_wrkrs_percent_car_c") +
    F.lit(0.0151) * F.col("cens_commute_wrkrs_percent_pub_c") +
    F.lit(-0.5804) * F.col("cens_density_persons_per_hh_fo_c") +
    F.lit(0.0119) * F.col("cens_earn_hh_percent_with_publ_c") +
    F.lit(0.246) * F.col("cens_educ_pop25_plus_median_ed_c") +
    F.lit(-0.0352) * F.col("cens_educ_pop25_plus_percent_p_c") +
    F.lit(0.0148) * F.col("cens_employ_labf_percent_emplo_c") +
    F.lit(0.0323) * F.col("cens_employ_pop18_plus_percent_z") +
    F.lit(-0.0272) * F.col("cens_ethnic_pop_percent_black__c") +
    F.lit(-0.0451) * F.col("cens_ethnic_pop_percent_hi_nat_c") +
    F.lit(0.0192) * F.col("cens_ethnic_pop_percent_non_hi_c") +
    F.lit(-0.0366) * F.col("cens_ethnic_pop_percent_some_o_c") +
    F.lit(-0.0346) * F.col("cens_ethnic_pop_percent_white__c") +
    F.lit(-0.026) * F.col("cens_gender_pop_percent_female_c") +
    F.lit(-0.0305) * F.col("cens_heat_occhu_percent_oil_or_c") +
    F.lit(-0.0116) * F.col("cens_heat_occhu_percent_utilit_c") +
    F.lit(-0.0163) * F.col("cens_hhsize_hh_percent_2_perso_c") +
    F.lit(-0.00261) * F.col("cens_homval_home_value_cbsa_in_c") +
    F.lit(-0.00000091) * F.col("cens_homval_oohu_median_home_v_c") +
    F.lit(-0.0275) * F.col("cens_homval_oohu_percent_home__c") +
    F.lit(-0.0124) * F.col("cens_hustr_hu_percent_2_units_c") +
    F.lit(0.192) * F.col("cens_inc_family_inc_state_deci_c") +
    F.lit(-0.00002) * F.col("cens_inc_hh_median_family_hous_c") +
    F.lit(0.000014) * F.col("cens_inc_hh_median_household_i_c") +
    F.lit(0.0338) * F.col("cens_indus_empld_percent_finan_c") +
    F.lit(-0.0175) * F.col("cens_indus_empld_percent_manuf_c") +
    F.lit(0.0136) * F.col("cens_mortg_oohu_percent_no_mor_c") +
    F.lit(0.00784) * F.col("cens_occup_empld_percent_sales_c") +
    F.lit(-0.0156) * F.col("cens_grpqtrs_pop_percent_colle_c") +
    F.lit(0.00807) * F.col("cens_indus_empld_percent_hlth__c") +
    F.lit(0.0226) * F.col("cens_grpqtrs_pop_percent_nursi_c") +
    F.lit(0.00434) * F.col("cens_hustr_hu_percent_1_unit_d_c") +
    F.lit(0.0292) * F.col("cens_occup_empld_percent_farm__c") +
    F.lit(0.0206) * F.col("cens_occup_empld_percent_manag_c") +
    F.lit(-0.0158) * F.col("cens_occup_empld_percent_compu_c") +
    F.lit(-0.0492) * F.col("cens_occup_empld_percent_life__c") +
    F.lit(-0.0316) * F.col("cens_ethnic_pop_percent_asian__c") +
    F.lit(0.0000002621) * F.col("cens_census_tract_c") +
    F.lit(-0.0108) * F.col("cens_educ_pop25_plus_percent_a_c") +
    F.lit(-0.0235) * F.col("cens_indus_empld_percent_minin_c") +
    F.lit(0.0175) * F.col("cens_move_occhu_percent_turnov_c") +
    F.lit(0.0504) * F.col("cens_earn_hh_percent_with_self_c") +
    F.lit(-0.0691) * F.col("cens_age_pop_percent_30_34_c") +
    F.lit(0.0299) * F.col("cens_heat_occhu_percent_other__c") +
    F.lit(0.0259) * F.col("cens_indus_empld_percent_trans_c") +
    F.lit(-0.04) * F.col("cens_mortg_oohu_percent_two_mr_c") +
    F.lit(0.0264) * F.col("cens_ethnic_hh_percent_hoh_his_c") +
    F.lit(0.2234) * F.col("num_curr_participation_discoun_c") +
    F.lit(0.0093) * F.col("november_general_election_day__c") +
    F.lit(0.0046) * F.col("mail_readership_model_c") +
    F.lit(-0.00711) * F.col("educational_attainment_model_c") +
    F.lit(0.00328) * F.col("fiscal_policy_model_c") +
    F.lit(-0.0296) * F.col("sy_otsbn_polfund_c") +
    F.lit(-0.0114) * F.col("general_election_vote_propensi_c") +
    F.lit(0.0216) * F.col("age_agg_ind_c") +
    F.lit(0.009) * F.col("likely_hisp_agg_c") +
    F.lit(0.6508) * F.col("num_giving_back_visits_past_3m_c") +
    F.lit(-0.5554) * F.col("dm_c") +
    F.lit(1.07) * F.col("phn_c") +
    F.lit(0.9567) * F.col("advocacy_grassroots_engager") +
    F.lit(2.5984) * F.col("attended_aarp_event") +
    F.lit(0.7347) * F.col("engaged_aarp_event_c") +
    F.lit(0.8515) * F.col("Tele_Townhall_Engagers") +
    F.lit(0.0221) * F.col("orders_36moterm_c") +
    F.lit(0.137) * F.col("orders_60moterm_c") +
    F.lit(0.0583) * F.col("orders_altmedia_c") +
    F.lit(0.0717) * F.col("orders_online") +
    F.lit(0.0695) * F.col("orders_serviceprovider_c") +
    F.lit(0.3268) * F.col("state_activities_12mo") +
    F.lit(0.9235) * F.col("tax_aid_vol_12mo_c") +
    F.lit(0.00974) * F.col("VOTEPROP2016_c") +
    F.lit(0.394) * F.col("teletown_12mo_c") +
    F.lit(-0.5918) * F.col("moviesfg_12mo_c") +
    F.lit(-2.9002) * F.col("blockparty_12mo_c") +
    F.lit(-0.4967) * F.col("state_event_12mo_c") +
    F.lit(4.2563) * F.col("teletown_12mo_i_c") +
    F.lit(3.0072) * F.col("chapters_vol_12mo_i_c") +
    F.lit(1.8601) * F.col("states_vol_12mo_i_c") +
    F.lit(-0.266) * F.col("contact_leg_12mo_i_c") +
    F.lit(-0.4661) * F.col("petition_sign_12mo_i_c") +
    F.lit(-0.5653) * F.col("survey_resp_12mo_i_c") +
    F.lit(-2.0149) * F.col("structured_12mo_i_c") +
    F.lit(1.3601) * F.col("state_event_12mo_i_c") +
    F.lit(0.00761) * F.col("religiosity_model_c") +
    F.lit(0.00741) * F.col("likely_cell_assignment_score_c") +
    F.lit(0.0287) * F.col("likely_landline_connectivity_s_c") +
    F.lit(-0.0319) * F.col("likely_landline_assignment_sco_c") +
    F.lit(-0.00358) * F.col("race_confidence_numeric_c") +
    F.lit(0.1257) * F.col("Motorcycling_c") +
    F.lit(0.0953) * F.col("auto_work_c") +
    F.lit(-0.1695) * F.col("boating_sailing_c") +
    F.lit(0.1829) * F.col("broader_living_c") +
    F.lit(-0.0824) * F.col("childrens_interests_c") +
    F.lit(-0.0834) * F.col("collectibles_antiques_c") +
    F.lit(0.1505) * F.col("collectibles_coins_c") +
    F.lit(0.1159) * F.col("diy_living_c") +
    F.lit(0.1756) * F.col("education_online_c") +
    F.lit(-0.2151) * F.col("home_furnishings_decorating_c") +
    F.lit(-0.129) * F.col("ibx_adult_age_55_64_agg_hhd_c") +
    F.lit(-0.1756) * F.col("ibx_adult_age_75_p_agg_hhd_c") +
    F.lit(-0.00000000000026) * F.col("ibx_community_involvement_caus_c") +
    F.lit(0.6305) * F.col("ibx_community_vetera1") +
    F.lit(0.1636) * F.col("ibx_grandchildren_premier_c") +
    F.lit(0.1242) * F.col("ibx_health_diabetic_c") +
    F.lit(-0.1177) * F.col("music_collector_c") +
    F.lit(-0.1551) * F.col("reading_best_sellers_c") +
    F.lit(0.1015) * F.col("reading_financial_newsletter_s_c") +
    F.lit(-0.1769) * F.col("spectator_sports_auto_motorcyc_c") +
    F.lit(0.1137) * F.col("spectator_sports_football_c") +
    F.lit(-0.169) * F.col("spectator_sports_hockey_c") +
    F.lit(0.1274) * F.col("sweepstakes_contests_c") +
    F.lit(0.1331) * F.col("tv_guide_c") +
    F.lit(-0.2211) * F.col("auto_renew_flag_c") +
    F.lit(0.3646) * F.col("ideology_20_dum") +
    F.lit(-0.1749) * F.col("ideology_80_dum") +
    F.lit(0.195) * F.col("partisanship_20_dum") +
    F.lit(-0.3096) * F.col("networth_1_dum") +
    F.lit(0.3135) * F.col("networth_b_dum") +
    F.lit(-0.1501) * F.col("chacq_u_dum") +
    F.lit(-0.1696) * F.col("curterm_36_dum") +
    F.lit(-0.2927) * F.col("curterm_60_dum") +
    F.lit(-0.1658) * F.col("female_dum") +
    F.lit(-0.2938) * F.col("adults1") +
    F.lit(-0.0907) * F.col("adults2") +
    F.lit(-0.3499) * F.col("business_u") +
    F.lit(-0.4557) * F.col("business_x") +
    F.lit(-0.2775) * F.col("dwelling_m") +
    F.lit(0.109) * F.col("education_4") +
    F.lit(-0.177) * F.col("homerange_a") +
    F.lit(-0.4181) * F.col("homerange_b") +
    F.lit(-0.5245) * F.col("homerange_c") +
    F.lit(-0.5654) * F.col("homerange_d") +
    F.lit(-0.6921) * F.col("homerange_eplus") +
    F.lit(0.1788) * F.col("homeloan_atog") +
    F.lit(-0.2069) * F.col("income_D") +
    F.lit(-0.1581) * F.col("income_E") +
    F.lit(-0.1696) * F.col("income_F") +
    F.lit(-0.3004) * F.col("income_GtoJ") +
    F.lit(0.2067) * F.col("occupation_7") +
    F.lit(0.1556) * F.col("occupation_8") +
    F.lit(-0.3031) * F.col("ibx_party_d") +
    F.lit(-0.495) * F.col("ibx_party_r") +
    F.lit(-0.2728) * F.col("lifestage_5") +
    F.lit(-0.4528) * F.col("lifestage_6") +
    F.lit(-0.4279) * F.col("lifestage_7") +
    F.lit(-0.4041) * F.col("lifestage_8") +
    F.lit(-0.1278) * F.col("maritalstat_s") +
    F.lit(-5.4214) * F.col("member_primary") +
    F.lit(1.1082) * F.col("member_secondary") +
    F.lit(0.000055) * F.col("IBX_HOME_PURCHASE_DT_PREMIER_num") +
    F.lit(0.000163) * F.col("IBX_HOME_YEAR_BUILT_ACTUAL_num") +
    F.lit(0.1529) * F.col("IBX_MOVIE_MUSIC_GROUPING_num") +
    F.lit(0.1922) * F.col("IBX_RECREATIONAL_VEHICLES_PR_num") +
    F.lit(0.0896) * F.col("IBX_TRAVEL_CRUISE_AGG_HHD_num")
)

df_transformed = df_transformed.withColumn("veterans_attendee_score", F.exp(F.col("veterans_attendee_score_logit")))

df_fnf = df_transformed.select("mid_key", "fnf_dance_score", "MFG_Virtual_Screenings_score", "veterans_attendee_score")

# PROC RANK translation
# Create a dummy column for global ranking as there is no BY statement
df_fnf_with_dummy = df_fnf.withColumn("dummy", F.lit(1))

# Define window specifications for each score
win_fnf_dance = Window.partitionBy("dummy").orderBy(F.col("fnf_dance_score").desc())
win_mfg_screenings = Window.partitionBy("dummy").orderBy(F.col("MFG_Virtual_Screenings_score").desc())
win_veterans = Window.partitionBy("dummy").orderBy(F.col("veterans_attendee_score").desc())

# Apply ntile function to create ranks
df_fnf_rank = df_fnf_with_dummy.withColumn("fnf_dance", F.ntile(99).over(win_fnf_dance)) \
                               .withColumn("MFG_Virtual_Screenings", F.ntile(99).over(win_mfg_screenings)) \
                               .withColumn("veterans_attendee", F.ntile(99).over(win_veterans)) \
                               .drop("dummy")

# PROC SQL to update geo_appends_rpm
df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_fnf_rank.createOrReplaceTempView("fnf_rank")

df_geo_appends_rpm_updated = spark.sql("""
    CREATE OR REPLACE TEMP VIEW geo_appends_rpm_view AS
    SELECT
        a.*,
        b.fnf_dance + 1 AS fnf_dance,
        b.MFG_Virtual_Screenings + 1 AS MFG_Virtual_Screenings,
        b.veterans_attendee + 1 AS veterans_attendee
    FROM
        geo_appends_rpm AS a
    LEFT JOIN
        fnf_rank AS b ON a.mid_key = b.mid_key
""")

# The spark.sql CREATE VIEW doesn't return a DataFrame, so we select from the view
df_geo_appends_rpm_final = spark.table("geo_appends_rpm_view")

# Writing the final updated table
df_geo_appends_rpm_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

# PROC FREQ translation
# Load the newly updated table for frequency counts
df_final_for_freq = spark.table("intermed.geo_appends_rpm")

print("Frequency Distribution for fnf_dance:")
df_final_for_freq.groupBy("fnf_dance").count().orderBy("fnf_dance").show()

print("Frequency Distribution for MFG_Virtual_Screenings:")
df_final_for_freq.groupBy("MFG_Virtual_Screenings").count().orderBy("MFG_Virtual_Screenings").show()

print("Frequency Distribution for veterans_attendee:")
df_final_for_freq.groupBy("veterans_attendee").count().orderBy("veterans_attendee").show()

#End-DBShift