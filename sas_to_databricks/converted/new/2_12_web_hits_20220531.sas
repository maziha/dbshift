import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType, IntegerType, DateType
from functools import reduce

spark = SparkSession.builder.appName("web_hits_conversion").getOrCreate()

df_web_visits_i = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/vg01/aarp_sas/ftp/incoming/web_visits_rpm.csv")

vars_schema = df_web_visits_i.dtypes
char_columns = [field for field, dtype in vars_schema if dtype == 'string']
new_names = [f"{col}_n" for col in char_columns]
rename_map = {f"{old}_n": old for old in char_columns}

df_test2 = df_web_visits_i
for old_col, new_col in zip(char_columns, new_names):
    df_test2 = df_test2.withColumn(new_col, F.col(old_col).cast(DoubleType()))

df_test2 = df_test2.drop(*char_columns)
for new_col, old_col in rename_map.items():
    df_test2 = df_test2.withColumnRenamed(new_col, old_col)

df_test2_filtered = df_test2.filter(F.col("mid_key").isNotNull())
numeric_cols = [c for c, dt in df_test2_filtered.dtypes if dt not in ('string', 'timestamp', 'date') and c != 'mid_key']
agg_exprs = [F.sum(c).alias(c) for c in numeric_cols]
df_web_visits = df_test2_filtered.groupBy("mid_key").agg(*agg_exprs)
df_web_visits.write.format("delta").mode("overwrite").saveAsTable("intermed.web_visits")

df_rr2 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/vg04/twalters/rr2.csv")

df_rest_rr = df_rr2.withColumnRenamed("rest_rr", "label").withColumnRenamed("new_week", "start")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_web_visits = spark.table("intermed.web_visits")
df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_web_visits.createOrReplaceTempView("web_visits")

df_combo = spark.sql("""
    SELECT
        a.mid_key,
        a.confidence,
        a.GENERAL_ELECTION_VOTE_PROPENSITY,
        a.memacctnum,
        a.NEW_SCORE35_CENTILE,
        a.NEW_SCORE37_CENTILE,
        a.NEW_SCORE44_CENTILE,
        a.EM_Clickrate,
        a.Overall_Active_SP_Reltshps,
        a.workstatus,
        a.ACEV_Flag,
        a.emu_indicator,
        a.CENS_CENSUS_TRACT,
        a.CENS_CENSUS_BLOCK_GROUP,
        a.CENS_COUNT_POPULATION,
        a.CENS_AGE_POP_PERCENT_50_54,
        a.CENS_EDUC_POP25_PLUS_PERCENT_ASS,
        a.CENS_INC_HH_MED_INC_HOUSEHOLDER_,
        a.CENS_INDUS_EMPLD_PERCENT_MINING,
        a.CENS_INDUS_EMPLD_PERCENT_INFORMA,
        a.CENS_MOVE_OCCHU_PERCENT_MOVED_IN,
        a.IBX_PC_OWNER_PREMIER,
        a.IBX_RETAIL_PURCHASES_MOST_FREQUE,
        a.IBX_ONLINE_AVERAGE_AMT_PER_ORDER,
        a.ibx_education_input_individual_p AS ibx_education,
        a.IBX_INCOME_ESTIMATED_NARROW_RANG,
        a.IBX_NETWORTH_PREMIER_AGG_HHD,
        a.IBX_BUSINESS_OWNER_AGG_HHD,
        a.IBX_ADULT_AGE_45_54_AGG_HHD,
        a.IBX_ADULTS_NUMBER_OF_HOUSEHOLD_P,
        a.orders_12moterm,
        a.cens_inc_hh_median_non_family_ho,
        a.fiscal_policy_model,
        a.religiosity_model,
        a.sy_otsbn_polfund_2012b,
        a.suppression,
        a.survey_resp_12mo,
        a.individual_engagers_12mo,
        a.NEW_SCORE19_CENTILE,
        a.NEW_SCORE30_CENTILE,
        a.NEW_SCORE50_CENTILE,
        a.RELATIONSHIP_SEG,
        a.EMAILABLE_AGG_IND,
        a.IBX_COMMUNITY_INVOLVEMENT_ENVIRO,
        a.IBX_MAIL_BUYER_CAT_HEALTH_AGG_HH,
        a.WORK_CLUSTERS,
        a.IBX_DONATION_CONTRIBUTION,
        a.IBX_HOME_OWNER,
        a.IBX_RECREATIONAL_VEHICLES_PREMIE,
        a.IBX_TOTAL_ONLINE_PURCHASES,
        a.Advo_Petition,
        a.Past3MoTouchCt_Overall,
        a.IBX_TRAVEL_CRUISE_AGG_HHD,
        b.driver_safety_hits
    FROM
        geo_appends_rpm a
    LEFT JOIN
        web_visits b ON a.mid_key = b.mid_key
""")

df_runner = df_combo
df_runner = df_runner.withColumn("CONFIDENCE_c", F.when(F.col("CONFIDENCE").isNull(), 0.9622891).otherwise(F.col("CONFIDENCE")))
df_runner = df_runner.withColumn("any_adobe_driver_hits", F.when(F.col("driver_safety_hits") > 0, 1).otherwise(0))
df_runner = df_runner.withColumn("GENERAL_ELECTION_VOTE_PROPENSI_c", F.when(F.col("GENERAL_ELECTION_VOTE_PROPENSITY").isNull(), 83.4860205).otherwise(F.col("GENERAL_ELECTION_VOTE_PROPENSITY")))
df_runner = df_runner.withColumn("NEW_SCORE35_CENTILE_c", F.when(F.col("NEW_SCORE35_CENTILE").isNull(), 46.5971046).otherwise(F.col("NEW_SCORE35_CENTILE")))
df_runner = df_runner.withColumn("NEW_SCORE37_CENTILE_c", F.when(F.col("NEW_SCORE37_CENTILE").isNull(), 41.0841141).otherwise(F.col("NEW_SCORE37_CENTILE")))
df_runner = df_runner.withColumn("NEW_SCORE44_CENTILE_c", F.when(F.col("NEW_SCORE44_CENTILE").isNull(), 35.9386308).otherwise(F.col("NEW_SCORE44_CENTILE")))
df_runner = df_runner.withColumn("EM_Clickrate_c", F.when(F.col("EM_Clickrate").isNull(), 50.1215209).otherwise(F.col("EM_Clickrate")))
df_runner = df_runner.withColumn("CENS_CENSUS_TRACT_c", F.when(F.col("CENS_CENSUS_TRACT").isNull(), 238992.57).otherwise(F.col("CENS_CENSUS_TRACT")))
df_runner = df_runner.withColumn("CENS_CENSUS_BLOCK_GROUP_c", F.when(F.col("CENS_CENSUS_BLOCK_GROUP").isNull(), 2.1202893).otherwise(F.col("CENS_CENSUS_BLOCK_GROUP")))
df_runner = df_runner.withColumn("CENS_COUNT_POPULATION_c", F.when(F.col("CENS_COUNT_POPULATION").isNull(), 2040.25).otherwise(F.col("CENS_COUNT_POPULATION")))
df_runner = df_runner.withColumn("CENS_AGE_POP_PERCENT_50_54_c", F.when(F.col("CENS_AGE_POP_PERCENT_50_54").isNull(), 7.3140779).otherwise(F.col("CENS_AGE_POP_PERCENT_50_54")))
df_runner = df_runner.withColumn("CENS_EDUC_POP25_PLUS_PERCENT_A_c", F.when(F.col("CENS_EDUC_POP25_PLUS_PERCENT_ASS").isNull(), 8.2612598).otherwise(F.col("CENS_EDUC_POP25_PLUS_PERCENT_ASS")))
df_runner = df_runner.withColumn("CENS_INC_HH_MED_INC_HOUSEHOLDE_c", F.when(F.col("CENS_INC_HH_MED_INC_HOUSEHOLDER_").isNull(), 46146.57).otherwise(F.col("CENS_INC_HH_MED_INC_HOUSEHOLDER_")))
df_runner = df_runner.withColumn("CENS_INDUS_EMPLD_PERCENT_MININ_c", F.when(F.col("CENS_INDUS_EMPLD_PERCENT_MINING").isNull(), 0.4503464).otherwise(F.col("CENS_INDUS_EMPLD_PERCENT_MINING")))
df_runner = df_runner.withColumn("CENS_INDUS_EMPLD_PERCENT_INFOR_c", F.when(F.col("CENS_INDUS_EMPLD_PERCENT_INFORMA").isNull(), 2.161574).otherwise(F.col("CENS_INDUS_EMPLD_PERCENT_INFORMA")))
df_runner = df_runner.withColumn("CENS_MOVE_OCCHU_PERCENT_MOVED__c", F.when(F.col("CENS_MOVE_OCCHU_PERCENT_MOVED_IN").isNull(), 2.7838014).otherwise(F.col("CENS_MOVE_OCCHU_PERCENT_MOVED_IN")))
df_runner = df_runner.withColumn("education_3_dum", F.when(F.col("ibx_education") == '3', 1).otherwise(0))
df_runner = df_runner.withColumn("income_1to2_dum", F.when(F.col("IBX_INCOME_ESTIMATED_NARROW_RANG").isin('1', '2'), 1).otherwise(0))
df_runner = df_runner.withColumn("networth_1_dum", F.when(F.col("IBX_NETWORTH_PREMIER_AGG_HHD") == '1', 1).otherwise(0))
df_runner = df_runner.withColumn("active_sp_dum", F.when(F.col("Overall_Active_SP_Reltshps") >= '1', 1).otherwise(0))
df_runner = df_runner.withColumn("workstatus_u_dum", F.when(F.col("workstatus") == 'U', 1).otherwise(0))
df_runner = df_runner.withColumn("acevdum", F.when(F.col("ACEV_Flag") == 'Y', 1).otherwise(0))
df_runner = df_runner.withColumn("business_u", F.when(F.col("IBX_BUSINESS_OWNER_AGG_HHD") == 'U', 1).otherwise(0))
df_runner = df_runner.withColumn("business_x", F.when(F.col("IBX_BUSINESS_OWNER_AGG_HHD") == 'X', 1).otherwise(0))
df_runner = df_runner.withColumn("pcowner", F.when(F.col("IBX_PC_OWNER_PREMIER") == 'Y', 1).otherwise(0))
df_runner = df_runner.withColumn("retail_b6", F.when(F.col("IBX_RETAIL_PURCHASES_MOST_FREQUE") == 'B6', 1).otherwise(0))
df_runner = df_runner.withColumn("IBX_ONLINE_AVERAGE_AMT_PER_O_num", F.when(F.col("IBX_ONLINE_AVERAGE_AMT_PER_ORDER") == "", 0).otherwise(F.col("IBX_ONLINE_AVERAGE_AMT_PER_ORDER")).cast(DoubleType()))
df_runner = df_runner.withColumn("IBX_ADULT_AGE_45_54_AGG_HHD_num", F.when(F.col("IBX_ADULT_AGE_45_54_AGG_HHD") == "", 0).otherwise(F.col("IBX_ADULT_AGE_45_54_AGG_HHD")).cast(DoubleType()))
df_runner = df_runner.withColumn("scorerun", F.exp(
    F.lit(-13.9794) + 
    F.lit(1.7146) * F.col("any_adobe_driver_hits") + 
    F.lit(4.2354) * F.col("CONFIDENCE_c") + 
    F.lit(0.0101) * F.col("GENERAL_ELECTION_VOTE_PROPENSI_c") + 
    F.lit(-0.00682) * F.col("NEW_SCORE35_CENTILE_c") + 
    F.lit(0.0105) * F.col("NEW_SCORE37_CENTILE_c") + 
    F.lit(0.007) * F.col("NEW_SCORE44_CENTILE_c") + 
    F.lit(-0.01) * F.col("EM_Clickrate_c") + 
    F.lit(-0.000000557) * F.col("CENS_CENSUS_TRACT_c") + 
    F.lit(0.0641) * F.col("CENS_CENSUS_BLOCK_GROUP_c") + 
    F.lit(0.000043) * F.col("CENS_COUNT_POPULATION_c") + 
    F.lit(0.0408) * F.col("CENS_AGE_POP_PERCENT_50_54_c") + 
    F.lit(0.0353) * F.col("CENS_EDUC_POP25_PLUS_PERCENT_A_c") + 
    F.lit(0.000001242) * F.col("CENS_INC_HH_MED_INC_HOUSEHOLDE_c") + 
    F.lit(0.0402) * F.col("CENS_INDUS_EMPLD_PERCENT_MININ_c") + 
    F.lit(0.0357) * F.col("CENS_INDUS_EMPLD_PERCENT_INFOR_c") + 
    F.lit(-0.0362) * F.col("CENS_MOVE_OCCHU_PERCENT_MOVED__c") + 
    F.lit(0.2947) * F.col("education_3_dum") + 
    F.lit(-0.4022) * F.col("income_1to2_dum") + 
    F.lit(-0.74) * F.col("networth_1_dum") + 
    F.lit(0.4705) * F.col("active_sp_dum") + 
    F.lit(0.3277) * F.col("workstatus_u_dum") + 
    F.lit(0.5053) * F.col("acevdum") + 
    F.lit(0.6527) * F.col("business_u") + 
    F.lit(0.8143) * F.col("business_x") + 
    F.lit(0.2763) * F.col("pcowner") + 
    F.lit(0.6386) * F.col("retail_b6") + 
    F.lit(0.00001) * F.col("IBX_ONLINE_AVERAGE_AMT_PER_O_num") + 
    F.lit(-0.4306) * F.col("IBX_ADULT_AGE_45_54_AGG_HHD_num")
))
df_runner = df_runner.withColumn("score_drvsafe_pro_em", F.col("scorerun") / (F.lit(1) + F.col("scorerun")))
df_runner = df_runner.withColumn("orders_12moterm_c", F.when(F.col("orders_12moterm").isNull(), 0).otherwise(F.col("orders_12moterm")))
df_runner = df_runner.withColumn("cens_inc_hh_median_non_family__c", F.when(F.col("cens_inc_hh_median_non_family_ho").isNull(), 46570.41).otherwise(F.col("cens_inc_hh_median_non_family_ho")))
df_runner = df_runner.withColumn("fiscal_policy_model_c", F.when(F.col("fiscal_policy_model").isNull(), 51.6897713).otherwise(F.col("fiscal_policy_model")))
df_runner = df_runner.withColumn("religious_c", F.when(F.col("religiosity_model").isNull(), 7.8066407).otherwise(F.col("religiosity_model")))
df_runner = df_runner.withColumn("local_news_c", F.lit(34.3197281))
df_runner = df_runner.withColumn("commuter_c", F.lit(44.8686797))
df_runner = df_runner.withColumn("sy_otsbn_polfund_2012b_c", F.when(F.col("sy_otsbn_polfund_2012b").isNull(), 32.8444735).otherwise(F.col("sy_otsbn_polfund_2012b")))
df_runner = df_runner.withColumn("suppression_c", F.when(F.col("suppression").isNull(), 0).otherwise(F.col("suppression")))
df_runner = df_runner.withColumn("survey_resp_12mo_c", F.when(F.col("survey_resp_12mo").isNull(), 0).otherwise(F.col("survey_resp_12mo")))
df_runner = df_runner.withColumn("individual_engagers_12mo_c", F.when(F.col("individual_engagers_12mo").isNull(), 0).otherwise(F.col("individual_engagers_12mo")))
df_runner = df_runner.withColumn("NEW_SCORE19_CENTILE_c", F.when(F.col("NEW_SCORE19_CENTILE").isNull(), 38.7091838).otherwise(F.col("NEW_SCORE19_CENTILE")))
df_runner = df_runner.withColumn("NEW_SCORE30_CENTILE_c", F.when(F.col("NEW_SCORE30_CENTILE").isNull(), 21.4840961).otherwise(F.col("NEW_SCORE30_CENTILE")))
df_runner = df_runner.withColumn("NEW_SCORE50_CENTILE_c", F.when(F.col("NEW_SCORE50_CENTILE").isNull(), 45.7060385).otherwise(F.col("NEW_SCORE50_CENTILE")))
df_runner = df_runner.withColumn("EM_Clickrate_c", F.when(F.col("EM_Clickrate").isNull(), 46.7864023).otherwise(F.col("EM_Clickrate")))
df_runner = df_runner.withColumn("RELATIONSHIP_SEG_c", F.when(F.col("RELATIONSHIP_SEG").isNull(), 66.1975029).otherwise(F.col("RELATIONSHIP_SEG")))
df_runner = df_runner.withColumn("Advo_Petition_c", F.when(F.col("Advo_Petition").isNull(), 0).otherwise(F.col("Advo_Petition")))
df_runner = df_runner.withColumn("emailable_dum", F.when(F.col("EMAILABLE_AGG_IND") == 'Y', 1).otherwise(0))
df_runner = df_runner.withColumn("community_enviro_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_ENVIRO") == 1, 1).otherwise(0))
df_runner = df_runner.withColumn("mail_health_dum", F.when(F.col("IBX_MAIL_BUYER_CAT_HEALTH_AGG_HH") == '1', 1).otherwise(0))
df_runner = df_runner.withColumn("past3touch_0_dum", F.when(F.col("Past3MoTouchCt_Overall") == "", 1).otherwise(0))
df_runner = df_runner.withColumn("workcluster_7_dum", F.when(F.col("WORK_CLUSTERS") == '7', 1).otherwise(0))
df_runner = df_runner.withColumn("adults4plus", F.when(F.col("IBX_ADULTS_NUMBER_OF_HOUSEHOLD_P").isin('4', '5', '6'), 1).otherwise(0))
df_runner = df_runner.withColumn("ibx_donation", F.when(F.col("IBX_DONATION_CONTRIBUTION").isNotNull(), 1).otherwise(0))
df_runner = df_runner.withColumn("owner", F.when(F.col("IBX_HOME_OWNER") == 'O', 1).otherwise(0))
df_runner = df_runner.withColumn("retail_b4", F.when(F.col("IBX_RETAIL_PURCHASES_MOST_FREQUE") == 'B4', 1).otherwise(0))
df_runner = df_runner.withColumn("IBX_RECREATIONAL_VEHICLES_PR_num", F.when(F.col("IBX_RECREATIONAL_VEHICLES_PREMIE") == "", 0).otherwise(F.col("IBX_RECREATIONAL_VEHICLES_PREMIE")).cast(DoubleType()))
df_runner = df_runner.withColumn("IBX_TOTAL_ONLINE_PURCHASES_num", F.when(F.col("IBX_TOTAL_ONLINE_PURCHASES") == "", 0).otherwise(F.col("IBX_TOTAL_ONLINE_PURCHASES")).cast(DoubleType()))
df_runner = df_runner.withColumn("IBX_TRAVEL_CRUISE_AGG_HHD_num", F.when(F.col("IBX_TRAVEL_CRUISE_AGG_HHD") == "", 0).otherwise(F.col("IBX_TRAVEL_CRUISE_AGG_HHD")).cast(DoubleType()))
df_runner = df_runner.withColumn("fndn_em_gift", F.exp(
    F.lit(-8.6687) + 
    F.lit(-0.0767) * F.col("orders_12moterm_c") +
    F.lit(0.000004505) * F.col("cens_inc_hh_median_non_family__c") +
    F.lit(-0.00428) * F.col("fiscal_policy_model_c") +
    F.lit(-0.0481) * F.col("religious_c") +
    F.lit(0.0214) * F.col("local_news_c") +
    F.lit(0.021) * F.col("commuter_c") +
    F.lit(0.0111) * F.col("sy_otsbn_polfund_2012b_c") +
    F.lit(1.1707) * F.col("suppression_c") +
    F.lit(0.3069) * F.col("survey_resp_12mo_c") +
    F.lit(0.2453) * F.col("individual_engagers_12mo_c") +
    F.lit(-0.0187) * F.col("NEW_SCORE19_CENTILE_c") +
    F.lit(0.0321) * F.col("NEW_SCORE30_CENTILE_c") +
    F.lit(-0.00847) * F.col("NEW_SCORE50_CENTILE_c") +
    F.lit(-0.0139) * F.col("EM_Clickrate_c") +
    F.lit(0.00207) * F.col("RELATIONSHIP_SEG_c") +
    F.lit(-0.2194) * F.col("Advo_Petition_c") +
    F.lit(-0.6268) * F.col("emailable_dum") +
    F.lit(0.4206) * F.col("community_enviro_dum") +
    F.lit(0.5028) * F.col("education_3_dum") +
    F.lit(0.3213) * F.col("mail_health_dum") +
    F.lit(0.2064) * F.col("past3touch_0_dum") +
    F.lit(1.0131) * F.col("workcluster_7_dum") +
    F.lit(-0.4213) * F.col("adults4plus") +
    F.lit(0.4522) * F.col("ibx_donation") +
    F.lit(0.3592) * F.col("owner") +
    F.lit(0.7437) * F.col("retail_b4") +
    F.lit(-0.3978) * F.col("IBX_RECREATIONAL_VEHICLES_PR_num") +
    F.lit(0.00639) * F.col("IBX_TOTAL_ONLINE_PURCHASES_num") +
    F.lit(-0.3927) * F.col("IBX_TRAVEL_CRUISE_AGG_HHD_num") 
))
df_runner = df_runner.select("score_drvsafe_pro_em", "mid_key", "fndn_em_gift")

df_runner = df_runner.dropDuplicates(["mid_key"])

df_runner_with_dummy = df_runner.withColumn("dummy_partition", F.lit(1))
window_drvsafe = Window.partitionBy("dummy_partition").orderBy(F.col("score_drvsafe_pro_em").desc())
window_fndn = Window.partitionBy("dummy_partition").orderBy(F.col("fndn_em_gift").desc())
df_bl_rank = df_runner_with_dummy.withColumn("drvsafe_pro_em", F.ntile(99).over(window_drvsafe))
df_bl_rank = df_bl_rank.withColumn("fndn_pro_em", F.ntile(99).over(window_fndn))
df_bl_rank = df_bl_rank.drop("dummy_partition")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_bl_rank.createOrReplaceTempView("bl_rank")
df_geo_appends_rpm_updated = spark.sql("""
    SELECT
        b.drvsafe_pro_em + 1 AS drvsafe_pro_em,
        b.fndn_pro_em + 1 AS fndn_pro_em,
        a.*
    FROM
        intermed.geo_appends_rpm AS a
    LEFT JOIN
        bl_rank AS b ON a.mid_key = b.mid_key
""")
df_geo_appends_rpm_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_contact_history_sum = spark.table("intermed.contact_history_sum")
df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_contact_history_sum.createOrReplaceTempView("contact_history_sum")

df_cpd_tek_attend_input = spark.sql("""
    SELECT
        a.*,
        c.liveanswer_freq_3,
        c.mailct,
        c.mailercount_open,
        c.mailercount_open_6mo,
        c.num_open
    FROM
        intermed.geo_appends_rpm(
            KEEP mid_key, fndn_pro_em, petadv12, CENS_GRPQTRS_POP_PERCENT_MILITAR,
                 CENS_AGE_POP_PERCENT_65_99_PLUS, driver_class_12mo, rpm_score,
                 cens_ethnic_pop_percent_hispanic, age_agg_ind, PartyAffiliation,
                 ReligionCode, state_activities_12mo, state_activity_12mo_i,
                 acev_flag, acev_num, vehicle_known_owned_number, deadwood_model,
                 DENSITY_CLUSTERS, IBX_TELECOM_CALLING_SERVICES_AGG, EMAILABLE_AGG_IND,
                 sy_otsbn_polfund_2012a, sy_otsbn_polfund_2012b, CENS_INC_HH_PERCENT_HOUSEHOLD_19,
                 cens_hustr_hu_percent_1_unit_det, CENS_EMPLOY_LABF_PERCENT_UNEMPLO,
                 cens_WHITECOLLAR, CENS_COMMUTE_COMMUTER_PERCENT_TR,
                 CENS_ETHNIC_POP_PERCENT_BLACK_ON, CENS_INC_HH_MEDIAN_HOUSEHOLD_INC,
                 CENS_RENT_RNTL_MEDIAN_RENT, GENERAL_ACTIVIST_MODEL, aarporg_i,
                 globally_opted_in, ch_acq, cur_term, gender_input, cens_BLUECOLLAR,
                 orders_60moterm, cntct_lifstyle_12mo_agg_hhd, memxrenew,
                 IBX_HOME_ASSESSED_VALUE_RANGES, orders_all, orders_36moterm,
                 orders_altmedia, orders_serviceprovider, cens_age_pop_percent_25_34,
                 cens_commute_wrkrs_percent_drove, cens_educ_pop25_plus_median_educ,
                 cens_employ_labf_percent_in_arme, cens_hustr_hu_percent_1_unit_att,
                 cens_inc_hh_med_inc_householder_, cens_indus_empld_percent_agric_f,
                 cens_indus_empld_percent_constru, cens_occup_empld_percent_legal,
                 cens_tenancy_hu_percent_occupied, cens_typ_pop_percent_female_hoh_,
                 cens_inc_family_inc_state_index, mail_readership_model, ideology,
                 mem_type_agg_act, tax_aid_vol_12mo, VOTEPROP2016,
                 petition_sign_12mo_i, auto_renew_start_dt, CENS_AGE_POP_PERCENT_60_64,
                 live_answer_am, live_answer_pm, IBX_HOME_MARKET_VALUE_DECILES_AG,
                 Overall_Active_SP_Reltshps, IBX_ADULTS_NUMBER_OF_HOUSEHOLD_P,
                 IBX_BUSINESS_OWNER_AGG_HHD, IBX_DONATION_CONTRIBUTION,
                 IBX_RETAIL_PURCHASES_MOST_FREQUE, MaritalStatus, fndn_aarppro_dm_score
        ) a
    LEFT JOIN
        intermed.contact_history_sum(
            WHERE mid_key IS NOT NULL
        ) c ON a.mid_key = c.mid_key
""")
df_cpd_tek_attend_input.write.format("delta").mode("overwrite").saveAsTable("intermed.cpd_tek_attend_input")

df_cpd_tek_attend_input = spark.table("intermed.cpd_tek_attend_input")
df_intermed_cpd_tek_attend = df_cpd_tek_attend_input
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("fndn_pro_em_c", F.when(F.col("fndn_pro_em").isNull(), 22.115).otherwise(F.col("fndn_pro_em")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("petadv12_c", F.when(F.col("petadv12").isNull(), 0).otherwise(F.col("petadv12")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("CENS_GRPQTRS_POP_PERCENT_MIL_c", F.when(F.col("CENS_GRPQTRS_POP_PERCENT_MILITAR").isNull(), 0.0136619).otherwise(F.col("CENS_GRPQTRS_POP_PERCENT_MILITAR")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("PERCENT65ANDOLDER_c", F.when(F.col("CENS_AGE_POP_PERCENT_65_99_PLUS").isNull(), 26.39).otherwise(F.col("CENS_AGE_POP_PERCENT_65_99_PLUS")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("driver_class_12mo", F.when(F.col("driver_class_12mo").isNull(), 0).otherwise(F.col("driver_class_12mo")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("rpm_score_c", F.when(F.col("rpm_score").isNull(), 9.2585).otherwise(F.col("rpm_score")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("fndn_aarppro_dm_score_c", F.when(F.col("fndn_aarppro_dm_score").isNull(), 0.0127582).otherwise(F.col("fndn_aarppro_dm_score")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("PERCENTHISPANIC_c", F.when(F.col("cens_ethnic_pop_percent_hispanic").isNull(), 5.92).otherwise(F.col("cens_ethnic_pop_percent_hispanic")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("age_50to62_dum", F.when((F.col("age_agg_ind") >= 50) & (F.col("age_agg_ind") <= 62), 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("age_70to78_dum", F.when((F.col("age_agg_ind") >= 70) & (F.col("age_agg_ind") <= 78), 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("Party_Aff", F.when(F.col("PartyAffiliation").isin('DEM', 'REP', 'NPA'), F.col("PartyAffiliation")).otherwise('OTH'))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("Party_OTH", F.when(F.col("Party_Aff") == 'OTH', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("Religion", F.when(F.col("ReligionCode").isin('C', 'X', 'P', 'J'), F.col("ReligionCode")).otherwise('Other'))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("E_Orthodox", F.when(F.col("Religion") == 'O', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("state_activities_12mo_n", F.when(F.col("state_activities_12mo").isNull(), 0).otherwise(F.col("state_activities_12mo")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("state_activity_12mo_i_c", F.when(F.col("state_activity_12mo_i").isNull(), 0).otherwise(F.col("state_activity_12mo_i")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("acevflag_c", F.when(F.col("acev_flag") == 'Y', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("acevnum_dum", F.when(F.col("acev_num").isin('', '0'), 0).otherwise(1))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("vehicle_3_dum", F.when(F.col("vehicle_known_owned_number") == '3', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("deadwood_dum", F.when(F.col("deadwood_model").isin('DEAD', 'PROBDEAD'), 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("sy_dense", F.when(F.col("DENSITY_CLUSTERS").isin('1', '2', '3', '4', '5', '6'), 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("TELECOM_CALLING_SERVICES_AGG123", F.when(F.col("IBX_TELECOM_CALLING_SERVICES_AGG").isin('01', '02', '03'), 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("emailable_dum", F.when(F.col("EMAILABLE_AGG_IND") == 'Y', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("sy_otsbn_polfund_2012a_c", F.when(F.col("sy_otsbn_polfund_2012a").isNull(), 73.858).otherwise(F.col("sy_otsbn_polfund_2012a")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("sy_otsbn_polfund_2012b_c", F.when(F.col("sy_otsbn_polfund_2012b").isNull(), 46.523).otherwise(F.col("sy_otsbn_polfund_2012b")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("PERCENTINPOVERTY_c", F.when(F.col("CENS_INC_HH_PERCENT_HOUSEHOLD_19").isNull(), 8.81).otherwise(F.col("CENS_INC_HH_PERCENT_HOUSEHOLD_19")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("PERCENTSINGLEUNITDWELLINGS_c", F.when(F.col("cens_hustr_hu_percent_1_unit_det").isNull(), 74.18).otherwise(F.col("cens_hustr_hu_percent_1_unit_det")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("PERCENTUNEMPLOYED_c", F.when(F.col("CENS_EMPLOY_LABF_PERCENT_UNEMPLO").isNull(), 3.66).otherwise(F.col("CENS_EMPLOY_LABF_PERCENT_UNEMPLO")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("PERCENTWHITECOLLAR_c", F.when(F.col("cens_WHITECOLLAR").isNull(), 41.38).otherwise(F.col("cens_WHITECOLLAR")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("commute_pct_ls30", F.when(F.col("CENS_COMMUTE_COMMUTER_PERCENT_TR").isNull(), 69.49).otherwise(F.col("CENS_COMMUTE_COMMUTER_PERCENT_TR")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("PERCENT_BLACK_c", F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON"))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("percent_black_change", F.when((F.col("percent_black_c").isNull()) & (F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON").isNotNull()), 1).otherwise(F.lit(None)))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("PERCENT_BLACK_c", F.when((F.col("percent_black_c").isNull()) & (F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON").isNotNull()), F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON") * 0.1).when(F.col("percent_black_c").isNull(), 3.7).otherwise(F.col("PERCENT_BLACK_c")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("MEDIAN_HH_inc", F.when(F.col("CENS_INC_HH_MEDIAN_HOUSEHOLD_INC").isNull(), 72748.94).otherwise(F.col("CENS_INC_HH_MEDIAN_HOUSEHOLD_INC")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("RENT_c", F.when(F.col("CENS_RENT_RNTL_MEDIAN_RENT").isNull(), 887.669).otherwise(F.col("CENS_RENT_RNTL_MEDIAN_RENT")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("SY_GENERALACTIVIST_c", F.when(F.col("GENERAL_ACTIVIST_MODEL").isNull(), 58.996).otherwise(F.col("GENERAL_ACTIVIST_MODEL")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("liveanswer_freq_3", F.when(F.col("liveanswer_freq_3").isNull(), 0).otherwise(F.col("liveanswer_freq_3")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("mailct", F.when(F.col("mailct").isNull(), 0).otherwise(F.col("mailct")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("mailercount_open", F.when(F.col("mailercount_open").isNull(), 0).otherwise(F.col("mailercount_open")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("aarporg_i", F.when(F.col("aarporg_i").isNull(), 0).otherwise(F.col("aarporg_i")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("goi_missing_dum", F.when(F.col("globally_opted_in") == "", 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("goi_1_dum", F.when(F.col("globally_opted_in") == '1', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("ch_acq_U", F.when(F.col("ch_acq") == 'U', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("curterm_36_dum", F.when(F.col("cur_term") == '36', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("gender_M", F.when(F.col("gender_input") == 'M', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("PERCENTbluecollar_c", F.when(F.col("cens_BLUECOLLAR").isNull(), 34).otherwise(F.col("cens_BLUECOLLAR")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("logit_CPD_TEK_ATTEND", 
    F.lit(-4.7261) +
    F.col("liveanswer_freq_3") * 0.1236 +
    F.col("mailct") * 0.1192 +
    F.col("mailercount_open") * -0.0354 +
    F.col("Party_OTH") * -0.1794 +
    F.col("E_Orthodox") * 0.351 +
    F.col("state_activities_12mo_n") * 0.117 +
    F.col("driver_class_12mo") * 0.6659 +
    F.col("state_activity_12mo_i_c") * 1.4285 +
    F.col("aarporg_i") * -0.186 +
    F.col("goi_missing_dum") * -0.3934 +
    F.col("goi_1_dum") * -0.6592 +
    F.col("acevflag_c") * 0.5936 +
    F.col("acevnum_dum") * 0.3501 +
    F.col("ch_acq_U") * -0.3147 +
    F.col("curterm_36_dum") * -0.1251 +
    F.col("gender_M") * -0.5229 +
    F.col("vehicle_3_dum") * -0.2976 +
    F.col("deadwood_dum") * -0.5778 +
    F.col("sy_dense") * -0.2208 +
    F.col("TELECOM_CALLING_SERVICES_AGG123") * 0.4647 +
    F.col("emailable_dum") * 0.8294 +
    F.col("fndn_pro_em_c") * -0.0184 +
    F.col("petadv12_c") * 0.3715 +
    F.col("rpm_score_c") * 0.0302 +
    F.col("fndn_aarppro_dm_score_c") * -4.851 +
    F.col("sy_otsbn_polfund_2012a_c") * 0.00346 +
    F.col("sy_otsbn_polfund_2012b_c") * 0.00436 +
    F.col("PERCENT65ANDOLDER_c") * -0.0054 +
    F.col("PERCENTBLUECOLLAR_c") * -0.0122 +
    F.col("PERCENTHISPANIC_c") * 0.00339 +
    F.col("PERCENTINPOVERTY_c") * 0.00658 +
    F.col("PERCENTSINGLEUNITDWELLINGS_c") * 0.00289 +
    F.col("PERCENTUNEMPLOYED_c") * -0.0286 +
    F.col("PERCENTWHITECOLLAR_c") * -0.013 +
    F.col("commute_pct_ls30") * 0.0107 +
    F.col("CENS_GRPQTRS_POP_PERCENT_MIL_c") * 0.0334 +
    F.col("PERCENT_BLACK_c") * 0.00708 +
    F.col("MEDIAN_HH_inc") * -0.00000304 +
    F.col("RENT_c") * -0.00023 +
    F.col("age_50to62_dum") * -0.4521 +
    F.col("age_70to78_dum") * 0.2535 +
    F.col("SY_GENERALACTIVIST_c") * 0.00232
)
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("CPD_TEK_ATTEND_score", F.exp(F.col("logit_CPD_TEK_ATTEND")) / (F.lit(1) + F.exp(F.col("logit_CPD_TEK_ATTEND"))))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("orders_all_c", F.when(F.col("orders_all").isNull(), 0).otherwise(F.col("orders_all")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("orders_36moterm_c", F.when(F.col("orders_36moterm").isNull(), 0).otherwise(F.col("orders_36moterm")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("orders_60moterm_c", F.when(F.col("orders_60moterm").isNull(), 0).otherwise(F.col("orders_60moterm")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("orders_altmedia_c", F.when(F.col("orders_altmedia").isNull(), 0).otherwise(F.col("orders_altmedia")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("orders_serviceprovider_c", F.when(F.col("orders_serviceprovider").isNull(), 0).otherwise(F.col("orders_serviceprovider")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("cens_age_pop_percent_25_34_c", F.when(F.col("cens_age_pop_percent_25_34").isNull(), 12.2927804).otherwise(F.col("cens_age_pop_percent_25_34")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("cens_commute_wrkrs_percent_dro_c", F.when(F.col("cens_commute_wrkrs_percent_drove").isNull(), 76.4881847).otherwise(F.col("cens_commute_wrkrs_percent_drove")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("cens_educ_pop25_plus_median_ed_c", F.when(F.col("cens_educ_pop25_plus_median_educ").isNull(), 12.8984596).otherwise(F.col("cens_educ_pop25_plus_median_educ")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("cens_employ_labf_percent_in_ar_c", F.when(F.col("cens_employ_labf_percent_in_arme").isNull(), 0.361856).otherwise(F.col("cens_employ_labf_percent_in_arme")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("cens_hustr_hu_percent_1_unit_a_c", F.when(F.col("cens_hustr_hu_percent_1_unit_att").isNull(), 7.8752926).otherwise(F.col("cens_hustr_hu_percent_1_unit_att")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("cens_inc_hh_median_household_i_c", F.when(F.col("cens_inc_hh_median_household_inc").isNull(), 79304.45).otherwise(F.col("cens_inc_hh_median_household_inc")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("cens_inc_hh_med_inc_householde_c", F.when(F.col("cens_inc_hh_med_inc_householder_").isNull(), 51859.43).otherwise(F.col("cens_inc_hh_med_inc_householder_")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("cens_indus_empld_percent_agric_c", F.when(F.col("cens_indus_empld_percent_agric_f").isNull(), 0.7304639).otherwise(F.col("cens_indus_empld_percent_agric_f")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("cens_indus_empld_percent_const_c", F.when(F.col("cens_indus_empld_percent_constru").isNull(), 6.2403522).otherwise(F.col("cens_indus_empld_percent_constru")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("cens_occup_empld_percent_legal_c", F.when(F.col("cens_occup_empld_percent_legal").isNull(), 1.4102832).otherwise(F.col("cens_occup_empld_percent_legal")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("cens_tenancy_hu_percent_occupi_c", F.when(F.col("cens_tenancy_hu_percent_occupied").isNull(), 93.085711).otherwise(F.col("cens_tenancy_hu_percent_occupied")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("cens_typ_pop_percent_female_ho_c", F.when(F.col("cens_typ_pop_percent_female_hoh_").isNull(), 9.2837903).otherwise(F.col("cens_typ_pop_percent_female_hoh_")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("cens_inc_family_inc_state_inde_c", F.when(F.col("cens_inc_family_inc_state_index").isNull(), 118.9860696).otherwise(F.col("cens_inc_family_inc_state_index")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("mail_readership_model_c", F.when(F.col("mail_readership_model").isNull(), 63.7139779).otherwise(F.col("mail_readership_model")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("general_activist_model_c", F.when(F.col("general_activist_model").isNull(), 36.1714992).otherwise(F.col("general_activist_model")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("ideology_model_c", F.when(F.col("ideology").isNull(), 51.5920033).otherwise(F.col("ideology")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("sy_otsbn_polfund_2012a_c", F.when(F.col("sy_otsbn_polfund_2012a").isNull(), 66.7345528).otherwise(F.col("sy_otsbn_polfund_2012a")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("mem_type_agg_act_c", F.when(F.col("mem_type_agg_act").isNull(), 0).otherwise(F.col("mem_type_agg_act")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("memxrenew_c", F.when(F.col("memxrenew").isNull(), 0).otherwise(F.col("memxrenew")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("tax_aid_vol_12mo_c", F.when(F.col("tax_aid_vol_12mo").isNull(), 0).otherwise(F.col("tax_aid_vol_12mo")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("VOTEPROP2016_c", F.when(F.col("VOTEPROP2016").isNull(), 70.8559185).otherwise(F.col("VOTEPROP2016")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("petition_sign_12mo_i_c", F.when(F.col("petition_sign_12mo_i").isNull(), 0).otherwise(F.col("petition_sign_12mo_i")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("auto_renew_start_dt_c", F.when(F.col("auto_renew_start_dt").isNull(), 0).otherwise(F.col("auto_renew_start_dt")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("cntct_lifstyle_12mo_agg_hhd_c", F.when(F.col("cntct_lifstyle_12mo_agg_hhd").isNull(), 0).otherwise(F.col("cntct_lifstyle_12mo_agg_hhd")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("Age_Pop_pct_60_64_c", F.when(F.col("CENS_AGE_POP_PERCENT_60_64").isNull(), 6.8916593).otherwise(F.col("CENS_AGE_POP_PERCENT_60_64")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("live_answer_am_c", F.when(F.col("live_answer_am").isNull(), 49.453786).otherwise(F.col("live_answer_am")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("live_answer_pm_c", F.when(F.col("live_answer_pm").isNull(), 50.4972235).otherwise(F.col("live_answer_pm")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("emailable_dum", F.when(F.col("EMAILABLE_AGG_IND") == 'Y', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("homevalue_10_dum", F.when(F.col("IBX_HOME_MARKET_VALUE_DECILES_AG") == '10', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("active_sp_dum", F.when(F.col("Overall_Active_SP_Reltshps") >= '1', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("chacq_f_dum", F.when(F.col("ch_acq") == 'F', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("curterm_60_dum", F.when(F.col("cur_term") == '60', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("adults2", F.when(F.col("IBX_ADULTS_NUMBER_OF_HOUSEHOLD_P") == '2', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("business_u", F.when(F.col("IBX_BUSINESS_OWNER_AGG_HHD") == 'U', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("ibx_donation", F.when(F.col("IBX_DONATION_CONTRIBUTION").isNotNull(), 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("homerange_a", F.when(F.col("IBX_HOME_ASSESSED_VALUE_RANGES") == 'A', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("homerange_b", F.when(F.col("IBX_HOME_ASSESSED_VALUE_RANGES") == 'B', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("homerange_c", F.when(F.col("IBX_HOME_ASSESSED_VALUE_RANGES") == 'C', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("retail_a4", F.when(F.col("IBX_RETAIL_PURCHASES_MOST_FREQUE") == 'A4', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("maritalstat_s", F.when(F.col("MaritalStatus") == 'S', 1).otherwise(0))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("drvs_score", F.exp(
    F.lit(-14.2921) + 
    F.lit(0.0716) * F.col("orders_all_c") +
    F.lit(0.0879) * F.col("orders_36moterm_c") +
    F.lit(0.2953) * F.col("orders_60moterm_c") +
    F.lit(-0.1418) * F.col("orders_altmedia_c") +
    F.lit(0.1589) * F.col("orders_serviceprovider_c") +
    F.lit(-0.0578) * F.col("cens_age_pop_percent_25_34_c") +
    F.lit(0.018) * F.col("cens_commute_wrkrs_percent_dro_c") +
    F.lit(0.2786) * F.col("cens_educ_pop25_plus_median_ed_c") +
    F.lit(0.0442) * F.col("cens_employ_labf_percent_in_ar_c") +
    F.lit(-0.0145) * F.col("cens_hustr_hu_percent_1_unit_a_c") +
    F.lit(-0.00001) * F.col("cens_inc_hh_median_household_i_c") +
    F.lit(0.000001159) * F.col("cens_inc_hh_med_inc_householde_c") +
    F.lit(0.0429) * F.col("cens_indus_empld_percent_agric_c") +
    F.lit(-0.0376) * F.col("cens_indus_empld_percent_const_c") +
    F.lit(-0.0653) * F.col("cens_occup_empld_percent_legal_c") +
    F.lit(0.0278) * F.col("cens_tenancy_hu_percent_occupi_c") +
    F.lit(0.0753) * F.col("cens_typ_pop_percent_female_ho_c") +
    F.lit(0.00643) * F.col("cens_inc_family_inc_state_inde_c") +
    F.lit(-0.00916) * F.col("mail_readership_model_c") +
    F.lit(0.0275) * F.col("general_activist_model_c") +
    F.lit(-0.018) * F.col("ideology_model_c") +
    F.lit(0.00645) * F.col("sy_otsbn_polfund_2012a_c") +
    F.lit(-1.0282) * F.col("mem_type_agg_act_c") +
    F.lit(-0.0752) * F.col("memxrenew_c") +
    F.lit(1.0992) * F.col("tax_aid_vol_12mo_c") +
    F.lit(0.00815) * F.col("VOTEPROP2016_c") +
    F.lit(1.5118) * F.col("petition_sign_12mo_i_c") +
    F.lit(-0.000000000473) * F.col("auto_renew_start_dt_c") +
    F.lit(0.0268) * F.col("cntct_lifstyle_12mo_agg_hhd_c") +
    F.lit(-0.0962) * F.col("Age_Pop_pct_60_64_c") +
    F.lit(-0.0128) * F.col("live_answer_am_c") +
    F.lit(0.0154) * F.col("live_answer_pm_c") +
    F.lit(0.926) * F.col("emailable_dum") +
    F.lit(-0.2681) * F.col("goi_missing_dum") +
    F.lit(-0.7811) * F.col("homevalue_10_dum") +
    F.lit(0.3432) * F.col("active_sp_dum") +
    F.lit(-0.8417) * F.col("chacq_f_dum") +
    F.lit(-0.3192) * F.col("curterm_36_dum") +
    F.lit(-0.6561) * F.col("curterm_60_dum") +
    F.lit(0.3485) * F.col("acevflag_c") +
    F.lit(0.234) * F.col("adults2") +
    F.lit(-0.3797) * F.col("business_u") +
    F.lit(0.2674) * F.col("ibx_donation") +
    F.lit(0.5231) * F.col("homerange_a") +
    F.lit(0.4524) * F.col("homerange_b") +
    F.lit(0.3897) * F.col("homerange_c") +
    F.lit(-0.4766) * F.col("retail_a4") +
    F.lit(0.3012) * F.col("maritalstat_s")
))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.withColumn("drvsafe_pro_dm_score", F.col("drvs_score") / (F.lit(1) + F.col("drvs_score")))
df_intermed_cpd_tek_attend = df_intermed_cpd_tek_attend.select("mid_key", "CPD_TEK_ATTEND_score", "drvsafe_pro_dm_score", "drvs_score")
df_intermed_cpd_tek_attend.write.format("delta").mode("overwrite").saveAsTable("intermed.cpd_tek_attend")

df_cpd_tek_attend = spark.table("intermed.cpd_tek_attend")
df_cpd_tek_attend_with_dummy = df_cpd_tek_attend.withColumn("dummy_partition", F.lit(1))
window_cpd = Window.partitionBy("dummy_partition").orderBy(F.col("CPD_TEK_ATTEND_score").desc())
window_drvsafe = Window.partitionBy("dummy_partition").orderBy(F.col("drvsafe_pro_dm_score").desc())
df_cpd_tek_attend_rank = df_cpd_tek_attend_with_dummy.withColumn("CPD_TEK_ATTEND", F.ntile(99).over(window_cpd))
df_cpd_tek_attend_rank = df_cpd_tek_attend_rank.withColumn("drvsafe_pro_dm", F.ntile(99).over(window_drvsafe))
df_cpd_tek_attend_rank = df_cpd_tek_attend_rank.drop("dummy_partition")
df_cpd_tek_attend_rank.write.format("delta").mode("overwrite").saveAsTable("intermed.cpd_tek_attend_rank")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_cpd_tek_attend_rank = spark.table("intermed.cpd_tek_attend_rank")
df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_cpd_tek_attend_rank.createOrReplaceTempView("cpd_tek_attend_rank")

df_geo_appends_rpm_updated = spark.sql("""
    SELECT
        b.CPD_TEK_ATTEND + 1 AS CPD_TEK_ATTEND,
        b.drvsafe_pro_dm + 1 AS drvsafe_pro_dm,
        a.*
    FROM
        intermed.geo_appends_rpm AS a
    LEFT JOIN
        intermed.cpd_tek_attend_rank AS b ON a.mid_key = b.mid_key
""")
df_geo_appends_rpm_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_web_visits = spark.table("intermed.web_visits")
df_contact_history_sum = spark.table("intermed.contact_history_sum")
df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_web_visits.createOrReplaceTempView("web_visits")
df_contact_history_sum.createOrReplaceTempView("contact_history_sum")

df_analysis = spark.sql("""
    SELECT
        a.*,
        b.work_job_hits,
        b.all_hits_30,
        c.call_freq,
        c.mailct_all,
        c.num_click,
        c.num_open_6mo,
        c.click_rate_6mo,
        c.mailercount_click_6mo,
        c.mailercount_open_6mo,
        c.mailercount_sent_180,
        c.num_open_30days,
        c.mailercount_click_30days,
        c.num_sent_curmonth,
        c.mailercount_sent_30days,
        c.liveanswer_freq_3,
        c.num_clicked_1_3mo,
        c.num_open_1_3mo,
        c.liveanswer_freq3_6,
        c.mailercount_click_1_3mo,
        c.liveanswer_freq6_12,
        c.mailercount_open_1_3mo
    FROM
        geo_appends_rpm a
    LEFT JOIN
        web_visits b ON a.mid_key = b.mid_key
    LEFT JOIN
        contact_history_sum c ON a.mid_key = c.mid_key
""")
df_analysis.write.format("delta").mode("overwrite").saveAsTable("intermed.analysis")

df_analysis = spark.table("intermed.analysis")
df_multi_scores = df_analysis
df_multi_scores = df_multi_scores.withColumn("MemOriginDate_char", F.col("MemOriginDate").cast(StringType()))
df_multi_scores = df_multi_scores.withColumn("Year1", F.substring(F.col("MemOriginDate_char"), 1, 4))
df_multi_scores = df_multi_scores.withColumn("month1", F.substring(F.col("MemOriginDate_char"), 5, 2))
df_multi_scores = df_multi_scores.withColumn("day1", F.substring(F.col("MemOriginDate_char"), 7, 2))
df_multi_scores = df_multi_scores.withColumn("mem_origin_date", F.to_date(F.concat_ws("-", F.col("Year1"), F.col("month1"), F.col("day1"))))
df_multi_scores = df_multi_scores.withColumn("MemOrigin_today_yr", (F.datediff(F.current_date(), F.col("mem_origin_date")) / 365.25).cast(IntegerType()))
df_multi_scores = df_multi_scores.withColumn("tenure01", F.when(F.col("MemOrigin_today_yr") <= 1, 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("tenure25", F.when((F.col("MemOrigin_today_yr") > 1) & (F.col("MemOrigin_today_yr") <= 5), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("tenure610", F.when((F.col("MemOrigin_today_yr") > 5) & (F.col("MemOrigin_today_yr") <= 10), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("age_63to70_dum", F.when((F.col("age_agg_ind") >= 63) & (F.col("age_agg_ind") <= 70), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("Past12MoTouchCt_Financial2", F.when(F.col("Past12MoTouchCt_Financial").isin('1', '2', '3'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("ftc_dnc_append_fly", F.when(F.col("ftc_dnc_append_fl") == 'Y', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("exercise_health1", F.when(F.col("exercise_health_group") == '1', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("R1_IBX_HOUSEHOLD_INCOME129", F.when(F.col("IBX_HOUSEHOLD_INCOME").isin('3', '4', '5', '6', '9', 'A', 'B'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("Overall_Historic_SP_123", F.when(F.col("Overall_Historic_SP_Reltshps").isin('1', '2', '0'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("NEW_SCORE48_Y", F.when(F.col("NEW_SCORE48_CENTILE").isin('44', '68', '79'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("es_region", F.when(F.col("region").isin('Central Region', 'West Region'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("east_region", F.when(F.col("region").isin('East Coast Region'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("aca_tth_y", F.when(F.col("aca_tth").isin('5', '71', '89', '97'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("HEALTH_SECURITY", F.when(F.col("NEW_SCORE2_CENTILE").isin('81', '85', '93', '2'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("AARP_HEALTH", F.when(F.col("NEW_SCORE3_CENTILE").isin('42', '63', '73', '96', '98'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("sp_rel_c", F.when(F.col("Overall_Active_SP_Reltshps").isin(1, 2, 3, 4), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("VOTEPROP2016_c", F.when(F.col("general_election_vote_propensity").isNull(), 83.8246349).otherwise(F.col("general_election_vote_propensity")))
df_multi_scores = df_multi_scores.withColumn("lifestage_123", F.when(F.col("Life_stage").isin('1', '2', '3'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("lifestage_678", F.when(F.col("Life_stage").isin('6', '7', '8'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("CENS_ETHNIC_POP_PERCENT_HI_c", F.when(F.col("CENS_ETHNIC_POP_PERCENT_HI_NAT_O").isNull(), 0.1173).otherwise(F.col("CENS_ETHNIC_POP_PERCENT_HI_NAT_O")))
df_multi_scores = df_multi_scores.withColumn("OOHU_MEDIAN_HOME_VAL_c", F.when(F.col("CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL").isNull(), 240435.8).otherwise(F.col("CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL")))
df_multi_scores = df_multi_scores.withColumn("RENT_c", F.when(F.col("CENS_RENT_RNTL_MEDIAN_RENT").isNull(), 836).otherwise(F.col("CENS_RENT_RNTL_MEDIAN_RENT")))
df_multi_scores = df_multi_scores.withColumn("PERCENT_HISPANIC", F.when(F.col("CENS_ETHNIC_POP_PERCENT_HISPANIC").isNull(), 10.8).otherwise(F.col("CENS_ETHNIC_POP_PERCENT_HISPANIC")))
df_multi_scores = df_multi_scores.withColumn("IBX_HEALTH_DIABETIC1", F.when(F.col("IBX_HEALTH_DIABETIC") == '1', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("PERCENT_BLACK_c", F.when(F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON").isNull(), 10.5).otherwise(F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON")))
df_multi_scores = df_multi_scores.withColumn("HH_pct_Spanish_Speaking_c", F.when(F.col("CENS_LANG_HH_PERCENT_SPANISH_SPE").isNull(), 78).otherwise(F.col("CENS_LANG_HH_PERCENT_SPANISH_SPE")))
df_multi_scores = df_multi_scores.withColumn("HomVal_Home_Value_CBSA_Index_c", F.when(F.col("CENS_HOMVAL_HOME_VALUE_CBSA_INDE").isNull(), 111).otherwise(F.col("CENS_HOMVAL_HOME_VALUE_CBSA_INDE")))
df_multi_scores = df_multi_scores.withColumn("Inc_HH_Median_HH_Income_c", F.when(F.col("CENS_HOMVAL_HOME_VALUE_CBSA_INDE").isNull(), 65711.44).otherwise(F.col("CENS_HOMVAL_HOME_VALUE_CBSA_INDE")))
df_multi_scores = df_multi_scores.withColumn("commute_pct_public", F.when(F.col("CENS_COMMUTE_WRKRS_PERCENT_PUBLI").isNull(), 3.7).otherwise(F.col("CENS_COMMUTE_WRKRS_PERCENT_PUBLI")))
df_multi_scores = df_multi_scores.withColumn("Marital_SM", F.when(F.col("MaritalStatus").isin('M', 'S'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("SP_SP", F.when(F.col("CENS_LANG_HH_PERCENT_SPANISH_SPE").isNull(), 7.75).otherwise(F.col("CENS_LANG_HH_PERCENT_SPANISH_SPE")))
df_multi_scores = df_multi_scores.withColumn("NO_MORTG", F.when(F.col("CENS_MORTG_OOHU_PERCENT_NO_MORTG").isNull(), 34.1).otherwise(F.col("CENS_MORTG_OOHU_PERCENT_NO_MORTG")))
df_multi_scores = df_multi_scores.withColumn("Pop_pct_Asian_Only_Hisp_c", F.when(F.col("Pop_pct_Asian_Only_Hisp").isNull(), 0.47).otherwise(F.col("Pop_pct_Asian_Only_Hisp")))
df_multi_scores = df_multi_scores.withColumn("SY_EDUCSCORE_c", F.when(F.col("EDUCATIONAL_ATTAINMENT_MODEL").isNull(), 0.36).otherwise(F.col("EDUCATIONAL_ATTAINMENT_MODEL")))
df_multi_scores = df_multi_scores.withColumn("ENG_SP", F.when(F.col("CENS_LANG_HH_PERCENT_ENGLISH_SPE").isNull(), 85).otherwise(F.col("CENS_LANG_HH_PERCENT_ENGLISH_SPE")))
df_multi_scores = df_multi_scores.withColumn("CENS_EARN_HH_PERCENT_NO_EARNIN_c", F.when(F.col("CENS_AGE_POP_PERCENT_60_64").isNull(), 6.73).otherwise(F.col("CENS_AGE_POP_PERCENT_60_64")))
df_multi_scores = df_multi_scores.withColumn("PERCENTCAUCASIANANDOTHER_c", F.when(F.col("CENS_ETHNIC_POP_PERCENT_WHITE_ON").isNull(), 82.4).otherwise(F.col("CENS_ETHNIC_POP_PERCENT_WHITE_ON")))
df_multi_scores = df_multi_scores.withColumn("PERCENTMANAGEMENTPROFESSIONALS_c", F.when(F.col("cens_MANAGEMENTPROFESSIONALS").isNull(), 14.4).otherwise(F.col("cens_MANAGEMENTPROFESSIONALS")))
df_multi_scores = df_multi_scores.withColumn("PERCENTUNEMPLOYED_c", F.when(F.col("CENS_EMPLOY_LABF_PERCENT_UNEMPLO").isNull(), 4.2).otherwise(F.col("CENS_EMPLOY_LABF_PERCENT_UNEMPLO")))
df_multi_scores = df_multi_scores.withColumn("CurrentPartCt_Overall12", F.when(F.col("CurrentPartCt_Overall").isin('2'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("aff_utility", F.when(F.col("NEW_SCORE36_CENTILE").isin('15', '21', '29', '39', '52', '53', '67', '76', '78', '80'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("aff_utility_", F.when(F.col("NEW_SCORE36_CENTILE").isin('27', '21', '72', '94'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("IBX_CREDIT_CARD_FREQ_24", F.when(F.col("IBX_CREDIT_CARD_FREQ_24_P_AGG_HH").isin('1', '3', '6'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("Past12MoTouchCt_Health_123", F.when(F.col("Past12MoTouchCt_Health").isin('1', '2', '3'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("NEW_SCORE48", F.when(F.col("NEW_SCORE48_CENTILE").isin('42', '82', '85', '88', '92'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("NEW_SCORE21", F.when(F.col("NEW_SCORE21_CENTILE").isin('37', '52', '65', '74', '76', '85'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("HEALTH_DECLINE", F.when(F.col("NEW_SCORE1_CENTILE").isin('13', '16', '25', '28'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("MEDICARE", F.when(F.col("NEW_SCORE33_CENTILE").isin('5', '7', '12', '24', '38', '40'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("Fraudwatch_loR", F.when(F.col("Fraudwatch_lo").isin('4', '6', '8', '11', '14', '23', '25', '27', '30', '32', '33', '34', '38'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("internet_1", F.when(F.col("IBX_TELECOM_INTERNET_AGG_HHD").isin('01', '02', '03'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("INTERNET_9_10", F.when(F.col("IBX_TELECOM_INTERNET_AGG_HHD").isin('09', '10'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("drvsafe_pro_em_c", F.when(F.col("drvsafe_pro_em").isin('1', '2', '3', '25', '31', '65'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("fndnothr_score_c", F.when(F.col("fndnothr_score").isin('1', '2', '3', '25', '26', '28', '38'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("health_touch123", F.when(F.col("Past3MoTouchCt_Health").isin('1', '2', '3'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("job_1245_dum", F.when(F.col("IBX_OCCUPATION_INPUT_AGG_HHD").isin('2', '4', '3'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("IBX_HEALTHY_BEHAVIOUR_HHD_1dum", F.when(F.col("IBX_HEALTHY_BEHAVIOUR_AGG_HHD") == '1', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("ch_acq_U", F.when(F.col("ch_acq") == 'U', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("ch_acq_I", F.when(F.col("ch_acq") == 'I', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("sy_dense", F.when(F.col("DENSITY_CLUSTERS").isin('9', '10', '11'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("CENS_EARN_HH_PERCENT_WITH_EARN_c", F.when(F.col("CENS_AGE_POP_PERCENT_55_64").isNull(), 14.23).otherwise(F.col("CENS_AGE_POP_PERCENT_55_64")))
df_multi_scores = df_multi_scores.withColumn("PERCENTCOLLEGEGRADS_c", F.when(F.col("cens_educ_pop25_plus_percent_col").isNull(), 29.7).otherwise(F.col("cens_educ_pop25_plus_percent_col")))
df_multi_scores = df_multi_scores.withColumn("PERCENTWHITECOLLAR_c", F.when(F.col("cens_WHITECOLLAR").isNull(), 40.19).otherwise(F.col("cens_WHITECOLLAR")))
df_multi_scores = df_multi_scores.withColumn("teletown_12mo_ic", F.when(F.col("teletown_12mo_i") == 1, 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("homevalue_9to10_dum", F.when(F.col("IBX_HOME_MARKET_VALUE_DECILES_AG").isin('09', '10'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("Party_Aff", F.when(F.col("PartyAffiliation").isin('DEM', 'REP', 'NPA'), F.col("PartyAffiliation")).otherwise('OTH'))
df_multi_scores = df_multi_scores.withColumn("Party_NPA", F.when(F.col("Party_Aff") == 'NPA', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("Party_REP", F.when(F.col("Party_Aff") == 'REP', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("goi_1_dum", F.when(F.col("globally_opted_in") == '1', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("GeneralElectn2012_AM", F.when(F.col("GeneralElectn2012").isin('A', 'M'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("Phoenix", F.when(F.col("community").isin('Phoenix', 'Phoenix 2016'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("rpm_score_c", F.when(F.col("rpm_score").isNull(), 9).otherwise(F.col("rpm_score")))
df_multi_scores = df_multi_scores.withColumn("MemXRenew_c", F.when(F.col("MemXRenew").isNull(), 5.26).otherwise(F.col("MemXRenew")))
df_multi_scores = df_multi_scores.withColumn("pct_live", F.when(F.col("pct_live").isNull(), 0).otherwise(F.col("pct_live")))
df_multi_scores = df_multi_scores.withColumn("logit_vali_lv", 
    F.lit(1.1675) +
    F.col("tenure01") * -0.5668 +
    F.col("tenure25") * -0.5336 +
    F.col("tenure610") * -0.3896 +
    F.col("pct_live") * 0.1568 +
    F.col("age_63to70_dum") * 0.0862 +
    F.col("Past12MoTouchCt_Financial2") * -0.0466 +
    F.col("ftc_dnc_append_fly") * 0.0851 +
    F.col("exercise_health1") * 0.064 +
    F.col("goi_1_dum") * -0.0481 +
    F.col("GeneralElectn2012_AM") * -0.1536 +
    F.col("R1_IBX_HOUSEHOLD_INCOME129") * 0.0361 +
    F.col("Overall_Historic_SP_123") * -0.1105 +
    F.col("NEW_SCORE48_Y") * -0.0826 +
    F.col("es_region") * -0.153 +
    F.col("east_region") * 0.0773 +
    F.col("Phoenix") * -0.2182 +
    F.col("aca_tth_y") * 0.0708 +
    F.col("HEALTH_SECURITY") * 0.081 +
    F.col("AARP_HEALTH") * 0.0583 +
    F.col("rpm_score_c") * 0.017 +
    F.col("MemXRenew_c") * -0.0107 +
    F.col("sp_rel_c") * -0.0429 +
    F.col("VOTEPROP2016_c") * 0.000853 +
    F.col("lifestage_678") * -0.0858 +
    F.col("lifestage_123") * -0.0495 +
    F.col("CENS_ETHNIC_POP_PERCENT_HI_c") * -0.0712 +
    F.col("OOHU_MEDIAN_HOME_VAL_c") * -0.000000548 +
    F.col("RENT_c") * -0.00006 +
    F.col("PERCENT_HISPANIC") * -0.00271 +
    F.col("IBX_HEALTH_DIABETIC1") * 0.0473 +
    F.col("PERCENT_BLACK_c") * 0.00478 +
    F.col("HH_pct_Spanish_Speaking_c") * -0.00199 +
    F.col("HomVal_Home_Value_CBSA_Index_c") * 0.000728 +
    F.col("Inc_HH_Median_HH_Income_c") * 0.000003227 +
    F.col("commute_pct_public") * 0.00421 +
    F.col("Marital_SM") * 0.0649 +
    F.col("SP_SP") * 0.0166 +
    F.col("NO_MORTG") * 0.00207 +
    F.col("Pop_pct_Asian_Only_Hisp_c") * -0.0158 +
    F.col("SY_EDUCSCORE_c") * -0.221 +
    F.col("ENG_SP") * -0.00487 +
    F.col("CENS_EARN_HH_PERCENT_NO_EARNIN_c") * -0.0106 +
    F.col("PERCENTCAUCASIANANDOTHER_c") * 0.00386 +
    F.col("PERCENTMANAGEMENTPROFESSIONALS_c") * -0.00427 +
    F.col("PERCENTUNEMPLOYED_c") * -0.00873
)
df_multi_scores = df_multi_scores.withColumn("score_vali_lv", F.exp(F.col("logit_vali_lv")) / (F.lit(1) + F.exp(F.col("logit_vali_lv"))))
df_multi_scores = df_multi_scores.withColumn("logit_vali_q3", 
    F.lit(-1.0493) +
    F.col("pct_live") * -0.1092 +
    F.col("CurrentPartCt_Overall12") * -0.0871 +
    F.col("aff_utility") * 0.051 +
    F.col("aff_utility_") * -0.0805 +
    F.col("IBX_CREDIT_CARD_FREQ_24") * 0.0485 +
    F.col("Past12MoTouchCt_Health_123") * 0.0596 +
    F.col("NEW_SCORE21") * 0.0737 +
    F.col("NEW_SCORE48") * -0.1302 +
    F.col("NEW_SCORE48_Y") * 0.0849 +
    F.col("aca_tth_y") * 0.1562 +
    F.col("HEALTH_DECLINE") * 0.1074 +
    F.col("HEALTH_SECURITY") * 0.145 +
    F.col("AARP_HEALTH") * 0.0882 +
    F.col("MEDICARE") * 0.0767 +
    F.col("Fraudwatch_loR") * 0.0891 +
    F.col("INTERNET_9_10") * -0.053 +
    F.col("internet_1") * -0.0781 +
    F.col("drvsafe_pro_em_c") * 0.1065 +
    F.col("fndnothr_score_c") * 0.1038 +
    F.col("health_touch123") * -0.0475 +
    F.col("job_1245_dum") * 0.0669 +
    F.col("IBX_HEALTHY_BEHAVIOUR_HHD_1dum") * -0.0633 +
    F.col("HH_pct_Spanish_Speaking_c") * -0.00016 +
    F.col("ch_acq_U") * 0.1131 +
    F.col("ch_acq_I") * 0.1438 +
    F.col("homevalue_9to10_dum") * 0.0718 +
    F.col("Party_NPA") * -0.1202 +
    F.col("Party_REP") * -0.0671 +
    F.col("sy_dense") * 0.0504 +
    F.col("SY_EDUCSCORE_c") * 0.205 +
    F.col("CENS_EARN_HH_PERCENT_WITH_EARN_c") * -0.00785 +
    F.col("PERCENTCOLLEGEGRADS_c") * -0.00306 +
    F.col("PERCENTWHITECOLLAR_c") * 0.00294 +
    F.col("teletown_12mo_ic") * 0.242
)
df_multi_scores = df_multi_scores.withColumn("score_vali_q3", F.exp(F.col("logit_vali_q3")) / (F.lit(1) + F.exp(F.col("logit_vali_q3"))))
df_multi_scores = df_multi_scores.withColumn("score_medicaid", F.col("score_vali_lv") * F.col("score_vali_q3"))
df_multi_scores = df_multi_scores.withColumn("networth_A9_dum", F.when(F.col("IBX_NETWORTH_PREMIER_AGG_HHD").isin('A', 'B', '8', '9'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("youngadult_dum", F.when(F.col("IBX_PRESENCE_OF_YOUNG_ADULT_AGG_") == 'Y', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("internet_1", F.when(F.col("IBX_TELECOM_INTERNET_AGG_HHD").isin('01', '02', '03', '04'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("work_job_hits", F.when(F.col("work_job_hits").isNull(), 0).otherwise(F.col("work_job_hits")))
df_multi_scores = df_multi_scores.withColumn("call_freq", F.when(F.col("call_freq").isNull(), 0).otherwise(F.col("call_freq")))
df_multi_scores = df_multi_scores.withColumn("mailct_all", F.when(F.col("mailct_all").isNull(), 0).otherwise(F.col("mailct_all")))
df_multi_scores = df_multi_scores.withColumn("num_click", F.when(F.col("num_click").isNull(), 0).otherwise(F.col("num_click")))
df_multi_scores = df_multi_scores.withColumn("num_open_6mo", F.when(F.col("num_open_6mo").isNull(), 0).otherwise(F.col("num_open_6mo")))
df_multi_scores = df_multi_scores.withColumn("click_rate_6mo", F.when(F.col("click_rate_6mo").isNull(), 0).otherwise(F.col("click_rate_6mo")))
df_multi_scores = df_multi_scores.withColumn("mailercount_click_6mo", F.when(F.col("mailercount_click_6mo").isNull(), 0).otherwise(F.col("mailercount_click_6mo")))
df_multi_scores = df_multi_scores.withColumn("mailercount_open_6mo", F.when(F.col("mailercount_open_6mo").isNull(), 0).otherwise(F.col("mailercount_open_6mo")))
df_multi_scores = df_multi_scores.withColumn("live_answer_am", F.when(F.col("live_answer_am").isNull(), 99).otherwise(F.col("live_answer_am")))
df_multi_scores = df_multi_scores.withColumn("live_answer_pm", F.when(F.col("live_answer_pm").isNull(), 99).otherwise(F.col("live_answer_pm")))
df_multi_scores = df_multi_scores.withColumn("live_answer_aft", F.when(F.col("live_answer_aft").isNull(), 99).otherwise(F.col("live_answer_aft")))
df_multi_scores = df_multi_scores.withColumn("emailable_dum", F.when(F.col("EMAILABLE_AGG_IND") == 'N', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("order_term_3yr", F.when(F.col("na2") == '36', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("order_term_5yr", F.when(F.col("na2") == '60', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("Past12MoTouchCt_AARP_c", F.when(F.col("Past12MoTouchCt_AARP") == "", 0).otherwise(1))
df_multi_scores = df_multi_scores.withColumn("HomVal_Home_Value_CBSA_Index_c", F.when(F.col("CENS_HOMVAL_HOME_VALUE_CBSA_INDE").isNull(), 109).otherwise(F.col("CENS_HOMVAL_HOME_VALUE_CBSA_INDE")))
df_multi_scores = df_multi_scores.withColumn("diversity_flag_1", F.when(F.col("diversity_flag_agg_ind") == 1, 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("diversity_flag_2", F.when(F.col("diversity_flag_agg_ind") == 2, 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("drvsafe_pro_em", F.when(F.col("drvsafe_pro_em").isNull(), 50).otherwise(F.col("drvsafe_pro_em")))
df_multi_scores = df_multi_scores.withColumn("fndn_pro_em_c", F.when(F.col("fndn_pro_em").isNull(), 50).otherwise(F.col("fndn_pro_em")))
df_multi_scores = df_multi_scores.withColumn("CENS_COMMUTE_COMMUTER_PERCENT_c", F.when(F.col("CENS_COMMUTE_COMMUTER_PERCENT_TR").isNull(), 65).otherwise(F.col("CENS_COMMUTE_COMMUTER_PERCENT_TR")))
df_multi_scores = df_multi_scores.withColumn("TURNOVER_5YS", F.when(F.col("CENS_MOVE_OCCHU_PERCENT_TURNOVER").isNull(), 29.31).otherwise(F.col("CENS_MOVE_OCCHU_PERCENT_TURNOVER")))
df_multi_scores = df_multi_scores.withColumn("RENT", F.when(F.col("CENS_RENT_RNTL_MEDIAN_RENT").isNull(), 893).otherwise(F.col("CENS_RENT_RNTL_MEDIAN_RENT")))
df_multi_scores = df_multi_scores.withColumn("newsletter_opens_cnt_12mo_c", F.when(F.col("newsletter_opens_cnt_12mo").isNull(), 0).otherwise(F.col("newsletter_opens_cnt_12mo")))
df_multi_scores = df_multi_scores.withColumn("suppression_c", F.when(F.col("suppression") == 0, 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("curterm_36_dum", F.when(F.col("cur_term") == '36', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("curterm_60_dum", F.when(F.col("cur_term") == '60', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("age_50to59_dum", F.when((F.col("age_agg_ind") >= 50) & (F.col("age_agg_ind") <= 56), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("age_60to69_dum", F.when((F.col("age_agg_ind") >= 57) & (F.col("age_agg_ind") <= 67), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("age_gt67_dum", F.when(F.col("age_agg_ind") >= 68, 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("NEW_SCORE6_CENTILE", F.when(F.col("NEW_SCORE6_CENTILE").isNull(), 99).otherwise(F.col("NEW_SCORE6_CENTILE")))
df_multi_scores = df_multi_scores.withColumn("NEW_SCORE8_CENTILE", F.when(F.col("NEW_SCORE8_CENTILE").isNull(), 99).otherwise(F.col("NEW_SCORE8_CENTILE")))
df_multi_scores = df_multi_scores.withColumn("adults_number_1", F.when(F.col("IBX_ADULTS_NUM_AGG_HHD").isin('1', '2'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("advo_dm_65", F.when(F.col("advo_dm_65plus").isin('1', '2', '3', '4', '5', '6', '7', '8', '9', '10'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("workcluster_wealthy", F.when(F.col("WORK_CLUSTERS").isin('1', '2'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("IBX_ADULTS_NUM_AGG_HHDls3", F.when(F.col("IBX_ADULTS_NUM_AGG_HHD").isin('4', '5', '6'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("grandchildren_dum", F.when(F.col("IBX_GRAND_CHILDREN_AGG_HHD") == 'Y', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("cruise_y", F.when(F.col("IBX_TRAVEL_CRUISE_AGG_HHD") == '1', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("PERCENT_CIVILIAN_VET", F.when(F.col("CENS_EMPLOY_POP18_PLUS_PERCENT_C").isNull(), 9).otherwise(F.col("CENS_EMPLOY_POP18_PLUS_PERCENT_C")))
df_multi_scores = df_multi_scores.withColumn("UTILITY_gas", F.when(F.col("CENS_HEAT_OCCHU_PERCENT_UTILITY_").isNull(), 49).otherwise(F.col("CENS_HEAT_OCCHU_PERCENT_UTILITY_")))
df_multi_scores = df_multi_scores.withColumn("children_presence_of_household_n", F.when(F.col("ibx_children_presence_of_househo") == 'N', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("state_activities_12moc", F.when(F.col("state_activities_12mo") == 0, 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("PERCENTbluecollar_c", F.when(F.col("cens_BLUECOLLAR").isNull(), 34).otherwise(F.col("cens_BLUECOLLAR")))
df_multi_scores = df_multi_scores.withColumn("HOME_LOAN_EF", F.when(F.col("IBX_HOME_LOAN_AMOUNT_1_RANGES").isin('E', 'K', 'L', 'G'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("EM_Clickrate_n", F.when(F.col("EM_Clickrate").isNull(), 50).otherwise(F.col("EM_Clickrate")))
df_multi_scores = df_multi_scores.withColumn("logit_work_jobs", 
    F.lit(-3.9655) +
    F.col("networth_A9_dum") * -0.0804 +
    F.col("youngadult_dum") * 0.1644 +
    F.col("job_1245_dum") * 0.0672 +
    F.col("internet_1") * 0.2351 +
    F.col("work_job_hits") * 0.00089 +
    F.col("call_freq") * -0.039 +
    F.col("mailct_all") * -0.00306 +
    F.col("num_click") * 0.00411 +
    F.col("num_open_6mo") * -0.00244 +
    F.col("click_rate_6mo") * 0.0119 +
    F.col("mailercount_click_6mo") * 0.2936 +
    F.col("mailercount_open_6mo") * 0.0174 +
    F.col("live_answer_am") * -0.00161 +
    F.col("live_answer_pm") * 0.00274 +
    F.col("live_answer_aft") * 0.00238 +
    F.col("emailable_dum") * 0.9039 +
    F.col("order_term_5yr") * 0.1575 +
    F.col("order_term_3yr") * 0.2296 +
    F.col("Past12MoTouchCt_AARP_c") * -0.3047 +
    F.col("Past12MoTouchCt_Financial2") * 0.1889 +
    F.col("HomVal_Home_Value_CBSA_Index_c") * -0.00066 +
    F.col("diversity_flag_1") * 0.4323 +
    F.col("diversity_flag_2") * 0.5956 +
    F.col("drvsafe_pro_em") * -0.00373 +
    F.col("fndn_pro_em_c") * -0.00346 +
    F.col("CENS_COMMUTE_COMMUTER_PERCENT_c") * -0.00149 +
    F.col("TURNOVER_5YS") * 0.00607 +
    F.col("RENT") * 0.000088 +
    F.col("newsletter_opens_cnt_12mo_c") * 0.00274 +
    F.col("suppression_c") * 0.132 +
    F.col("curterm_36_dum") * -0.2499 +
    F.col("curterm_60_dum") * -0.2953 +
    F.col("age_60to69_dum") * 0.4753 +
    F.col("age_50to59_dum") * 0.4091 +
    F.col("age_gt67_dum") * 0.2665 +
    F.col("NEW_SCORE6_CENTILE") * 0.00264 +
    F.col("NEW_SCORE8_CENTILE") * -0.00559 +
    F.col("adults_number_1") * -0.135 +
    F.col("goi_1_dum") * 0.13 +
    F.col("advo_dm_65") * -0.1198 +
    F.col("sy_dense") * -0.1047 +
    F.col("workcluster_wealthy") * 0.0591 +
    F.col("IBX_ADULTS_NUM_AGG_HHDls3") * -0.1263 +
    F.col("grandchildren_dum") * -0.0976 +
    F.col("cruise_y") * -0.0535 +
    F.col("PERCENT_CIVILIAN_VET") * -0.00778 +
    F.col("PERCENT_BLACK_c") * 0.00197 +
    F.col("UTILITY_gas") * -0.0011 +
    F.col("children_presence_of_household_n") * -0.0999 +
    F.col("state_activities_12moc") * -0.2168 +
    F.col("PERCENTBLUECOLLAR_c") * -0.00519 +
    F.col("HOME_LOAN_EF") * 0.1165 +
    F.col("GeneralElectn2012_AM") * -0.0898 +
    F.col("EM_Clickrate_n") * -0.0206
)
df_multi_scores = df_multi_scores.withColumn("score_work_jobs", F.exp(F.col("logit_work_jobs")) / (F.lit(1) + F.exp(F.col("logit_work_jobs"))))
df_multi_scores = df_multi_scores.withColumn("job_1245_dum", F.when(F.col("IBX_OCCUPATION_INPUT_AGG_HHD").isin('2', '3'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("age_51to56_dum", F.when((F.col("age_agg_ind") >= 51) & (F.col("age_agg_ind") <= 56), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("liveanswer_freq_3", F.when(F.col("liveanswer_freq_3").isNull(), 0).otherwise(F.col("liveanswer_freq_3")))
df_multi_scores = df_multi_scores.withColumn("survey_resp_12mo", F.when(F.col("survey_resp_12mo").isNull(), 0.5505885).otherwise(F.col("survey_resp_12mo")))
df_multi_scores = df_multi_scores.withColumn("individual_engagers_12mo", F.when(F.col("individual_engagers_12mo").isNull(), 0.7031802).otherwise(F.col("individual_engagers_12mo")))
df_multi_scores = df_multi_scores.withColumn("mailercount_open_1_3mo", F.when(F.col("mailercount_open_1_3mo").isNull(), 0).otherwise(F.col("mailercount_open_1_3mo")))
df_multi_scores = df_multi_scores.withColumn("foundation_donations_12mo", F.when(F.col("foundation_donations_12mo").isNull(), 0.1156007).otherwise(F.col("foundation_donations_12mo")))
df_multi_scores = df_multi_scores.withColumn("general_activist_model_c", F.coalesce(F.col("general_activist_model"), F.lit(61.3698537)))
df_multi_scores = df_multi_scores.withColumn("cens_ethnic_pop_percent_black_c", F.coalesce(F.col("cens_ethnic_pop_percent_black_on"), F.lit(7.7961404)))
df_multi_scores = df_multi_scores.withColumn("cens_earn_hh_percent_with_self_e", F.when(F.col("cens_earn_hh_percent_with_self_e").isNull(), 11.2440831).otherwise(F.col("cens_earn_hh_percent_with_self_e")))
df_multi_scores = df_multi_scores.withColumn("liveanswer_freq6_12", F.when(F.col("liveanswer_freq6_12").isNull(), 0).otherwise(F.col("liveanswer_freq6_12")))
df_multi_scores = df_multi_scores.withColumn("aarporg_i", F.when(F.col("aarporg_i").isNull(), 0).otherwise(F.col("aarporg_i")))
df_multi_scores = df_multi_scores.withColumn("mailercount_click_1mos", F.when(F.col("mailercount_click_30days").isNull(), 0).otherwise(F.col("mailercount_click_30days")))
df_multi_scores = df_multi_scores.withColumn("cens_marr_pop15_plus_percent_spo", F.when(F.col("cens_marr_pop15_plus_percent_spo").isNull(), 48.8945914).otherwise(F.col("cens_marr_pop15_plus_percent_spo")))
df_multi_scores = df_multi_scores.withColumn("single", F.when(F.col("MaritalStatus") == 'S', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("general_activist_model_c", F.coalesce(F.col("general_activist_model"), F.lit(61.3698537)))
df_multi_scores = df_multi_scores.withColumn("cens_educ_pop25_plus_median_educ", F.when(F.col("cens_educ_pop25_plus_median_educ").isNull(), 12.5875249).otherwise(F.col("cens_educ_pop25_plus_median_educ")))
df_multi_scores = df_multi_scores.withColumn("community_charity_dum", F.when(F.col("IBX_COMMUNITY_CHARITIES_AGG_HHD") == 1, 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("MemXRenew_c", F.when(F.col("MemXRenew").isNull(), 0).otherwise(F.col("MemXRenew")))
df_multi_scores = df_multi_scores.withColumn("advo_dm_50_64_logit", 
    F.lit(-4.2445) +
    F.col("age_51to56_dum") * -0.3883 +
    F.col("liveanswer_freq6_12") * -0.1209 +
    F.col("individual_engagers_12mo") * 0.5061 +
    F.col("aarporg_i") * -1.1342 +
    F.col("mailercount_click_1mos") * 0.2341 +
    F.col("cens_marr_pop15_plus_percent_spo") * -0.0103 +
    F.col("foundation_donations_12mo") * 0.141 +
    F.col("single") * 0.2125 +
    F.col("general_activist_model_c") * 0.0059 +
    F.col("ch_acq_U") * -0.5651 +
    F.col("cens_educ_pop25_plus_median_educ") * -0.1199 +
    F.col("community_charity_dum") * 0.3609 +
    F.col("MemXRenew_c") * 0.0928
)
df_multi_scores = df_multi_scores.withColumn("advo_5064_logit", F.exp(F.col("advo_dm_50_64_logit")) / (F.lit(1) + F.exp(F.col("advo_dm_50_64_logit"))))
df_multi_scores = df_multi_scores.withColumn("orders_online_c", F.when(F.col("orders_online").isNull(), 0).otherwise(F.col("orders_online")))
df_multi_scores = df_multi_scores.withColumn("likely_hisp_agg", F.when(F.col("likely_hisp_agg").isNull(), 99).otherwise(F.col("likely_hisp_agg")))
df_multi_scores = df_multi_scores.withColumn("likely_black_agg", F.when(F.col("likely_black_agg").isNull(), 99).otherwise(F.col("likely_black_agg")))
df_multi_scores = df_multi_scores.withColumn("state_activity_12mo_i_c", F.when(F.col("state_activity_12mo_i").isNull(), 0).otherwise(F.col("state_activity_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("national_activity_12mo_i_c", F.when(F.col("national_activity_12mo_i").isNull(), 0).otherwise(F.col("national_activity_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("driver_class_12mo_i_c", F.when(F.col("driver_class_12mo_i").isNull(), 0).otherwise(F.col("driver_class_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("driver_online_12mo_i_c", F.when(F.col("driver_online_12mo_i").isNull(), 0).otherwise(F.col("driver_online_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("yeas_survey_12mo_i_c", F.when(F.col("yeas_survey_12mo_i").isNull(), 0).otherwise(F.col("yeas_survey_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("teletown_12mo_i_c", F.when(F.col("teletown_12mo_i").isNull(), 0).otherwise(F.col("teletown_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("chapters_vol_12mo_i_c", F.when(F.col("chapters_vol_12mo_i").isNull(), 0).otherwise(F.col("chapters_vol_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("driver_safety_vol_12mo_i_c", F.when(F.col("driver_safety_vol_12mo_i").isNull(), 0).otherwise(F.col("driver_safety_vol_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("exp_corp_vol_12mo_i_c", F.when(F.col("exp_corp_vol_12mo_i").isNull(), 0).otherwise(F.col("exp_corp_vol_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("tax_aid_vol_12mo_i_c", F.when(F.col("tax_aid_vol_12mo_i").isNull(), 0).otherwise(F.col("tax_aid_vol_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("states_vol_12mo_i_c", F.when(F.col("states_vol_12mo_i").isNull(), 0).otherwise(F.col("states_vol_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("other_vol_12mo_i_c", F.when(F.col("other_vol_12mo_i").isNull(), 0).otherwise(F.col("other_vol_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("advocacy_donors_12mo_i_c", F.when(F.col("advocacy_donors_12mo_i").isNull(), 0).otherwise(F.col("advocacy_donors_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("advocacy_signers_12mo_i_c", F.when(F.col("advocacy_signers_12mo_i").isNull(), 0).otherwise(F.col("advocacy_signers_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("foundation_donors_12mo_i_c", F.when(F.col("foundation_donors_12mo_i").isNull(), 0).otherwise(F.col("foundation_donors_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("foundation_donations_checkb_12_c", F.when(F.col("foundation_donations_checkb_12mo").isNull(), 0).otherwise(F.col("foundation_donations_checkb_12mo")))
df_multi_scores = df_multi_scores.withColumn("contact_leg_12mo_i_c", F.when(F.col("contact_leg_12mo_i").isNull(), 0).otherwise(F.col("contact_leg_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("petition_sign_12mo_i_c", F.when(F.col("petition_sign_12mo_i").isNull(), 0).otherwise(F.col("petition_sign_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("petition_col_12mo_i_c", F.when(F.col("petition_col_12mo_i").isNull(), 0).otherwise(F.col("petition_col_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("leg_off_vis_12mo_i_c", F.when(F.col("leg_off_vis_12mo_i").isNull(), 0).otherwise(F.col("leg_off_vis_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("event_host_12mo_i_c", F.when(F.col("event_host_12mo_i").isNull(), 0).otherwise(F.col("event_host_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("outbound_call_12mo_i_c", F.when(F.col("outbound_call_12mo_i").isNull(), 0).otherwise(F.col("outbound_call_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("survey_resp_12mo_i_c", F.when(F.col("survey_resp_12mo_i").isNull(), 0).otherwise(F.col("survey_resp_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("story_sub_12mo_i_c", F.when(F.col("story_sub_12mo_i").isNull(), 0).otherwise(F.col("story_sub_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("moviesfg_12mo_i_c", F.when(F.col("moviesfg_12mo_i").isNull(), 0).otherwise(F.col("moviesfg_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("structured_12mo_i_c", F.when(F.col("structured_12mo_i").isNull(), 0).otherwise(F.col("structured_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("popups_12mo_i_c", F.when(F.col("popups_12mo_i").isNull(), 0).otherwise(F.col("popups_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("blockparty_12mo_i_c", F.when(F.col("blockparty_12mo_i").isNull(), 0).otherwise(F.col("blockparty_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("state_event_12mo_i_c", F.when(F.col("state_event_12mo_i").isNull(), 0).otherwise(F.col("state_event_12mo_i")))
df_multi_scores = df_multi_scores.withColumn("aarporg_i_c", F.when(F.col("aarporg_i").isNull(), 0).otherwise(F.col("aarporg_i")))
df_multi_scores = df_multi_scores.withColumn("life_engage_svcprov_12mo_c", F.when(F.col("life_engage_svcprov_12mo_nps").isNull(), 0).otherwise(F.col("life_engage_svcprov_12mo_nps")))
df_multi_scores = df_multi_scores.withColumn("activist_i_c", F.when(F.col("activist_i").isNull(), 0).otherwise(F.col("activist_i")))
df_multi_scores = df_multi_scores.withColumn("diff_engage", 
    F.col("state_activity_12mo_i_c") +
    F.col("national_activity_12mo_i_c") +
    F.col("driver_class_12mo_i_c") +
    F.col("driver_online_12mo_i_c") +
    F.col("yeas_survey_12mo_i_c") +
    F.col("teletown_12mo_i_c") +
    F.col("chapters_vol_12mo_i_c") +
    F.col("driver_safety_vol_12mo_i_c") +
    F.col("exp_corp_vol_12mo_i_c") +
    F.col("tax_aid_vol_12mo_i_c") +
    F.col("states_vol_12mo_i_c") +
    F.col("other_vol_12mo_i_c") +
    F.col("advocacy_donors_12mo_i_c") +
    F.col("advocacy_signers_12mo_i_c") +
    F.col("foundation_donors_12mo_i_c") +
    F.col("foundation_donations_checkb_12_c") +
    F.col("contact_leg_12mo_i_c") +
    F.col("petition_sign_12mo_i_c") +
    F.col("petition_col_12mo_i_c") +
    F.col("leg_off_vis_12mo_i_c") +
    F.col("event_host_12mo_i_c") +
    F.col("outbound_call_12mo_i_c") +
    F.col("survey_resp_12mo_i_c") +
    F.col("story_sub_12mo_i_c") +
    F.col("moviesfg_12mo_i_c") +
    F.col("structured_12mo_i_c") +
    F.col("popups_12mo_i_c") +
    F.col("blockparty_12mo_i_c") +
    F.col("state_event_12mo_i_c") +
    F.col("aarporg_i_c") +
    F.col("activist_i_c")
)
df_multi_scores = df_multi_scores.withColumn("age_miss", F.when(F.col("age_agg_ind").isNull(), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("age_50to59", F.when((F.col("age_agg_ind") >= 50) & (F.col("age_agg_ind") <= 59), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("MemXRenew_c", F.when(F.col("MemXRenew").isNull(), 0).otherwise(F.col("MemXRenew")))
df_multi_scores = df_multi_scores.withColumn("times_renewed_0_dum", F.when(F.col("memxrenew_c") == 0, 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("times_renewed_1_dum", F.when(F.col("memxrenew_c") == 1, 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("catalist_educ_low", F.when((F.col("educational_attainment_model") >= 0) & (F.col("educational_attainment_model") <= 0.2), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("catalist_educ_high", F.when(F.col("educational_attainment_model") >= 0.55, 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("orders_all_c", F.when(F.col("orders_all").isNull(), 0).otherwise(F.col("orders_all")))
df_multi_scores = df_multi_scores.withColumn("orders_12moterm_c", F.when(F.col("orders_12moterm").isNull(), 0).otherwise(F.col("orders_12moterm")))
df_multi_scores = df_multi_scores.withColumn("orders_60moterm_c", F.when(F.col("orders_60moterm").isNull(), 0).otherwise(F.col("orders_60moterm")))
df_multi_scores = df_multi_scores.withColumn("orders_acqmail_c", F.when(F.col("orders_acqmail").isNull(), 0).otherwise(F.col("orders_acqmail")))
df_multi_scores = df_multi_scores.withColumn("orders_altmedia_c", F.when(F.col("orders_altmedia").isNull(), 0).otherwise(F.col("orders_altmedia")))
df_multi_scores = df_multi_scores.withColumn("orders_serviceprovider_c", F.when(F.col("orders_serviceprovider").isNull(), 0).otherwise(F.col("orders_serviceprovider")))
df_multi_scores = df_multi_scores.withColumn("MAIL_READERSHIP_MODEL_c", F.when(F.col("MAIL_READERSHIP_MODEL").isNull(), 70.042774).otherwise(F.col("MAIL_READERSHIP_MODEL")))
df_multi_scores = df_multi_scores.withColumn("UNINSURED_MODEL_c", F.when(F.col("UNINSURED_MODEL").isNull(), 8.3829072).otherwise(F.col("UNINSURED_MODEL")))
df_multi_scores = df_multi_scores.withColumn("open_count_30_c", F.when(F.col("num_open_30days").isNull(), 0).otherwise(F.col("num_open_30days")))
df_multi_scores = df_multi_scores.withColumn("mailercount_click_30_c", F.when(F.col("mailercount_click_30days").isNull(), 0).otherwise(F.col("mailercount_click_30days")))
df_multi_scores = df_multi_scores.withColumn("sent_count_30_c", F.when(F.col("num_sent_curmonth").isNull(), 0).otherwise(F.col("num_sent_curmonth")))
df_multi_scores = df_multi_scores.withColumn("mailercount_sent_30_c", F.when(F.col("mailercount_sent_30days").isNull(), 0).otherwise(F.col("mailercount_sent_30days")))
df_multi_scores = df_multi_scores.withColumn("activities_3mo_c", F.when(F.col("activities_3mo").isNull(), 0).otherwise(F.col("activities_3mo")))
df_multi_scores = df_multi_scores.withColumn("num_hits_last30_c", F.when(F.col("all_hits_30").isNull(), 0).otherwise(F.col("all_hits_30")))
df_multi_scores = df_multi_scores.withColumn("MemOriginDate_c", F.when(F.col("MemOriginDate").isNull(), 20045011.58).otherwise(F.col("MemOriginDate")))
df_multi_scores = df_multi_scores.withColumn("MemPaidDate_c", F.when(F.col("MemPaidDate").isNull(), 201976.02).otherwise(F.col("MemPaidDate")))
df_multi_scores = df_multi_scores.withColumn("advo_hpc_amt_c", F.when(F.col("advo_hpc_amt").isNull(), 0).otherwise(F.col("advo_hpc_amt")))
df_multi_scores = df_multi_scores.withColumn("fndn_mrhpc_dt_c", F.when(F.col("fndn_mrhpc_dt").isNull(), 0).otherwise(F.col("fndn_mrhpc_dt")))
df_multi_scores = df_multi_scores.withColumn("partisanscore_c", F.when(F.col("partisanscore").isNull(), 57.3320274).otherwise(F.col("partisanscore")))
df_multi_scores = df_multi_scores.withColumn("ideology_c", F.when(F.col("ideology").isNull(), 45.2221506).otherwise(F.col("ideology")))
df_multi_scores = df_multi_scores.withColumn("suppression_c", F.when(F.col("suppression").isNull(), 0).otherwise(F.col("suppression")))
df_multi_scores = df_multi_scores.withColumn("contact_leg_12mo_c", F.when(F.col("contact_leg_12mo").isNull(), 0).otherwise(F.col("contact_leg_12mo")))
df_multi_scores = df_multi_scores.withColumn("num_months_c", F.when(F.col("num_months").isNull(), 148.5149909).otherwise(F.col("num_months")))
df_multi_scores = df_multi_scores.withColumn("goi_missing_dum", F.when(F.col("globally_opted_in") == "", 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("child_6to10_dum", F.when(F.col("IBX_CHILD_AGE_06_10_AGG_HHD") == 1, 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("community_childr_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_CHILDR") == 1, 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("community_health_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_HEALTH") == 1, 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("community_libera_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_LIBERA") == 1, 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("community_religi_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_RELIGI") == 1, 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("community_vetera_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_VETERA") == 1, 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("homebiz_dum", F.when(F.col("IBX_HOME_BUSINESS_AGG_HHD") == 'Y', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("mail_health_dum", F.when(F.col("IBX_MAIL_BUYER_CAT_HEALTH_AGG_HH") == '1', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("networth_1_dum", F.when(F.col("IBX_NETWORTH_PREMIER_AGG_HHD") == '1', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("networth_b_dum", F.when(F.col("IBX_NETWORTH_PREMIER_AGG_HHD") == 'B', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("cell_10_dum", F.when(F.col("IBX_TELECOM_CELLULAR_AGG_HHD") == '10', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("active_sp_dum", F.when(F.col("Overall_Active_SP_Reltshps") >= '1', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("region_w_dum", F.when(F.col("region") == 'West Region', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("advo_s1_dum", F.when(F.col("advo_segment_cd") == 'S1', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("cat_n_dum", F.when(F.col("category") == 'N', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("acevdum", F.when(F.col("ACEV_Flag") == 'Y', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("ethnicity_we", F.when(F.col("ETHNICITY") == "Western European", 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("female_dum", F.when(F.col("gender_agg_ind") == 'F', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("adults1", F.when(F.col("IBX_ADULTS_NUMBER_OF_HOUSEHOLD_P") == '1', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("owner", F.when(F.col("IBX_HOME_OWNER") == 'O', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("income_AtoC", F.when(F.col("IBX_HOUSEHOLD_INCOME").isin('A', 'B', 'C'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("pcowner", F.when(F.col("IBX_PC_OWNER_PREMIER") == 'Y', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("ibx_party_d", F.when(F.col("IBX_POLITICAL_PARTY_INPUT_INDIVI") == 'D', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("workingwoman", F.when(F.col("IBX_WORKING_WOMAN_AGG_HHD") == 'Y', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("lifestage_7", F.when(F.col("life_stage") == '7', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("member_secondary", F.when(F.col("MEMBER_FL_AGG_IND") == 'S', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("cntct_lifstyle_3mo_agg_hhd_c", F.when(F.col("cntct_lifstyle_3mo_agg_hhd").isNull(), 0).otherwise(F.col("cntct_lifstyle_3mo_agg_hhd")))
df_multi_scores = df_multi_scores.withColumn("AGE_AGG_IND_c", F.when(F.col("AGE_AGG_IND").isNull(), 67.0991973).otherwise(F.col("AGE_AGG_IND")))
df_multi_scores = df_multi_scores.withColumn("nps_detractor_score", F.exp(
    F.lit(56.095) + 
    F.col("LIKELY_HISP_AGG") * 0.00305 + 
    F.col("LIKELY_BLACK_AGG") * 0.00846 + 
    F.col("diff_engage") * -0.0507 + 
    F.col("age_miss") * 0.1121 + 
    F.col("times_renewed_0_dum") * 0.511 + 
    F.col("times_renewed_1_dum") * 0.142 + 
    F.col("catalist_educ_low") * -0.0412 + 
    F.col("catalist_educ_high") * 0.0662 + 
    F.col("age_50to59") * -0.00565 + 
    F.col("orders_all_c") * -0.0775 + 
    F.col("orders_12moterm_c") * 0.0323 + 
    F.col("orders_60moterm_c") * -0.1305 + 
    F.col("orders_acqmail_c") * 0.0788 + 
    F.col("orders_altmedia_c") * -0.0331 + 
    F.col("orders_online_c") * 0.0246 + 
    F.col("orders_serviceprovider_c") * 0.0586 + 
    F.col("MAIL_READERSHIP_MODEL_c") * 0.00258 + 
    F.col("UNINSURED_MODEL_c") * -0.00472 + 
    F.col("open_count_30_c") * -0.00674 + 
    F.col("mailercount_click_30_c") * -0.0608 + 
    F.col("sent_count_30_c") * -0.012 + 
    F.col("mailercount_sent_30_c") * 0.0378 + 
    F.col("life_engage_svcprov_12mo_c") * -0.2191 + 
    F.col("activities_3mo_c") * -0.1958 + 
    F.col("num_hits_last30_c") * -0.00032 + 
    F.col("cntct_lifstyle_3mo_agg_hhd_c") * -0.0108 + 
    F.col("MemOriginDate_c") * -0.00000276 + 
    F.col("MemXRenew_c") * -0.0319 + 
    F.col("MemPaidDate_c") * -0.00002 + 
    F.col("advo_hpc_amt_c") * -0.00649 + 
    F.col("fndn_mrhpc_dt_c") * -0.0000000101 + 
    F.col("partisanscore_c") * -0.00292 + 
    F.col("ideology_c") * -0.0051 + 
    F.col("AGE_AGG_IND_c") * 0.00791 + 
    F.col("foundation_donations_checkb_12_c") * 0.1064 + 
    F.col("suppression_c") * 0.4147 + 
    F.col("contact_leg_12mo_c") * -0.0239 + 
    F.col("advocacy_donors_12mo_i_c") * -0.2638 + 
    F.col("moviesfg_12mo_i_c") * -0.3227 + 
    F.col("state_event_12mo_i_c") * -0.2772 + 
    F.col("aarporg_i_c") * 0.1716 + 
    F.col("num_months_c") * -0.00083 + 
    F.col("goi_missing_dum") * 0.2479 + 
    F.col("goi_1_dum") * 0.1812 + 
    F.col("child_6to10_dum") * -0.1009 + 
    F.col("community_childr_dum") * -0.1496 + 
    F.col("community_health_dum") * -0.0793 + 
    F.col("community_libera_dum") * -0.1593 + 
    F.col("community_religi_dum") * 0.1039 + 
    F.col("community_vetera_dum") * -0.0743 + 
    F.col("grandchildren_dum") * -0.06 + 
    F.col("homebiz_dum") * 0.082 + 
    F.col("mail_health_dum") * -0.0695 + 
    F.col("networth_1_dum") * -0.1479 + 
    F.col("networth_b_dum") * 0.103 + 
    F.col("cell_10_dum") * 0.1042 + 
    F.col("active_sp_dum") * -0.1322 + 
    F.col("region_w_dum") * 0.1678 + 
    F.col("advo_s1_dum") * 0.0736 + 
    F.col("cat_n_dum") * -0.1347 + 
    F.col("ch_acq_U") * 0.0957 + 
    F.col("curterm_36_dum") * -0.2317 + 
    F.col("curterm_60_dum") * -0.1878 + 
    F.col("acevdum") * 0.0535 + 
    F.col("ethnicity_we") * 0.0871 + 
    F.col("female_dum") * -0.1942 + 
    F.col("adults1") * 0.1043 + 
    F.col("owner") * 0.1107 + 
    F.col("income_AtoC") * -0.068 + 
    F.col("pcowner") * 0.1027 + 
    F.col("ibx_party_d") * -0.0423 + 
    F.col("workingwoman") * -0.0672 + 
    F.col("lifestage_7") * 0.0581 + 
    F.col("member_secondary") * -0.9928
))
df_multi_scores = df_multi_scores.withColumn("driver_class_12mo", F.when(F.col("driver_class_12mo").isNull(), 0).otherwise(F.col("driver_class_12mo")))
df_multi_scores = df_multi_scores.withColumn("CELL9_10", F.when(F.col("IBX_TELECOM_CELLULAR_AGG_HHD").isin('09', '10'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("Drvsafe_pro_dm", F.when(F.col("Drvsafe_pro_dm").isNull(), 50).otherwise(F.col("Drvsafe_pro_dm")))
df_multi_scores = df_multi_scores.withColumn("sy_otsbn_polfund_2012b_c", F.when(F.col("sy_otsbn_polfund_2012b").isNull(), 32).otherwise(F.col("sy_otsbn_polfund_2012b")))
df_multi_scores = df_multi_scores.withColumn("NEW_SCORE46_CENTILE", F.when(F.col("NEW_SCORE46_CENTILE").isNull(), 31).otherwise(F.col("NEW_SCORE46_CENTILE")))
df_multi_scores = df_multi_scores.withColumn("NEW_SCORE32_CENTILE", F.when(F.col("NEW_SCORE32_CENTILE").isNull(), 99).otherwise(F.col("NEW_SCORE32_CENTILE")))
df_multi_scores = df_multi_scores.withColumn("advo_s23_dum", F.when(F.col("advo_segment_cd").isin('S4', 'S3'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("ACEV_Num12", F.when(F.col("ACEV_Num").isin('1', '2'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("dwelling_m_dum", F.when(F.col("IBX_DWELLING_TYPE_AGG_HHD") == 'S', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("driver_safety_tek_dm_logit", 
    F.lit(-7.0009) +
    F.col("driver_class_12mo") * 0.8603 +
    F.col("CELL9_10") * 0.4943 +
    F.col("mailercount_click_6mo") * 0.3837 +
    F.col("Drvsafe_pro_dm") * -0.0189 +
    F.col("sy_otsbn_polfund_2012b_c") * 0.00968 +
    F.col("NO_MORTG") * -0.0146 +
    F.col("state_activity_12mo_i_c") * 2.5031 +
    F.col("NEW_SCORE46_CENTILE") * -0.0123 +
    F.col("NEW_SCORE32_CENTILE") * 0.0114 +
    F.col("advo_s23_dum") * 0.8073 +
    F.col("ACEV_Num12") * 1.2333 +
    F.col("dwelling_m_dum") * 0.6117
)
df_multi_scores = df_multi_scores.withColumn("driver_safety_tek_dm_score", F.exp(F.col("driver_safety_tek_dm_logit")) / (F.lit(1) + F.exp(F.col("driver_safety_tek_dm_logit"))))
df_multi_scores = df_multi_scores.withColumn("NEW_SCORE2_CENTILE", F.when(F.col("NEW_SCORE2_CENTILE").isNull(), 99).otherwise(F.col("NEW_SCORE2_CENTILE")))
df_multi_scores = df_multi_scores.withColumn("Work_Status", F.when(F.col("WorkStatus").isin('F', 'P', 'R'), F.col("WorkStatus")).otherwise('U'))
df_multi_scores = df_multi_scores.withColumn("part_time", F.when(F.col("Work_Status") == 'P', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("TRAVEL_TYPE_d", F.when(F.col("IBX_TRAVEL_TYPE_AGG_HHD") == 'D', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("Pop_pct_Asian_Only__c", F.when(F.col("Pop_pct_Asian_Only_").isNull(), 42.7925167).otherwise(F.col("Pop_pct_Asian_Only_")))
df_multi_scores = df_multi_scores.withColumn("Pop_pct_Black_Only__c", F.when(F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON").isNull(), 101.483149).otherwise(F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON")))
df_multi_scores = df_multi_scores.withColumn("fndn_pro_em_c", F.when(F.col("fndn_pro_em").isNull(), 48.8373496).otherwise(F.col("fndn_pro_em")))
df_multi_scores = df_multi_scores.withColumn("fndnothr_score_c", F.when(F.col("fndnothr_score").isNull(), 47.2572735).otherwise(F.col("fndnothr_score")))
df_multi_scores = df_multi_scores.withColumn("ideology_c", F.when(F.col("ideology").isNull(), 42.037414).otherwise(F.col("ideology")))
df_multi_scores = df_multi_scores.withColumn("renewals_c", F.when(F.col("memxrenew").isNull(), 0).otherwise(F.col("memxrenew")))
df_multi_scores = df_multi_scores.withColumn("sp_rel_c", F.when(F.col("Overall_Active_SP_Reltshps").isNull(), 0.2299647).otherwise(F.col("Overall_Active_SP_Reltshps")))
df_multi_scores = df_multi_scores.withColumn("sy_otsbn_polfund_2012a_c", F.when(F.col("sy_otsbn_polfund_2012a").isNull(), 70.6188244).otherwise(F.col("sy_otsbn_polfund_2012a")))
df_multi_scores = df_multi_scores.withColumn("commute_pct_ls30", F.when(F.col("CENS_COMMUTE_COMMUTER_PERCENT_TR") <= 30, 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("FAMILY_HOUSEHOLDS__c", F.when(F.col("CENS_COUNT_FAMILY_HOUSEHOLDS").isNull(), 495.5623261).otherwise(F.col("CENS_COUNT_FAMILY_HOUSEHOLDS")))
df_multi_scores = df_multi_scores.withColumn("POP25_PLUS_PERCENT_BAC", F.when(F.col("CENS_EDUC_POP25_PLUS_PERCENT_BAC").isNull(), 19.5274609).otherwise(F.col("CENS_EDUC_POP25_PLUS_PERCENT_BAC")))
df_multi_scores = df_multi_scores.withColumn("POP_PERCENT_NON_HISP", F.when(F.col("CENS_ETHNIC_POP_PERCENT_NON_HISP").isNull(), 89.1885857).otherwise(F.col("CENS_ETHNIC_POP_PERCENT_NON_HISP")))
df_multi_scores = df_multi_scores.withColumn("PERCENT_WHITE", F.when(F.col("CENS_ETHNIC_POP_PERCENT_WHITE_ON").isNull(), 78.3627683).otherwise(F.col("CENS_ETHNIC_POP_PERCENT_WHITE_ON")))
df_multi_scores = df_multi_scores.withColumn("CENS_ETHNIC_POP_PERCENT_SOME_c", F.when(F.col("CENS_ETHNIC_POP_PERCENT_SOME_OTH").isNull(), 3.735558).otherwise(F.col("CENS_ETHNIC_POP_PERCENT_SOME_OTH")))
df_multi_scores = df_multi_scores.withColumn("EMP_FINANCE", F.when(F.col("CENS_INDUS_EMPLD_PERCENT_FINANCE").isNull(), 4.9457672).otherwise(F.col("CENS_INDUS_EMPLD_PERCENT_FINANCE")))
df_multi_scores = df_multi_scores.withColumn("age_50to56_dum", F.when((F.col("age_agg_ind") >= 50) & (F.col("age_agg_ind") <= 58), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("age_gt81_dum", F.when(F.col("age_agg_ind") >= 81, 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("age_57to64_dum", F.when((F.col("age_agg_ind") >= 59) & (F.col("age_agg_ind") <= 66), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("SY_GENERALACTIVIST_c", F.when(F.col("GENERAL_ACTIVIST_MODEL").isNull(), 60.3503459).otherwise(F.col("GENERAL_ACTIVIST_MODEL")))
df_multi_scores = df_multi_scores.withColumn("Party_DEM", F.when(F.col("Party_Aff") == 'DEM', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("full_time", F.when(F.col("Work_Status") == 'F', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("retire", F.when(F.col("Work_Status") == 'R', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("ethniccode_20_dum", F.when(F.col("EthnicCode") == '20', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("VoterStatus_active", F.when(F.col("VoterStatus") == 'active', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("ch_acq_F", F.when(F.col("ch_acq") == 'F', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("hitech_dum", F.when(F.col("hitech_merch") == '1', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("otherdonors_dum", F.when(F.col("other_donors") == '1', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("IBX_ADULTS_NUM_AGG_HHDls3", F.when(F.col("IBX_ADULTS_NUM_AGG_HHD").isin('1', '2'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("CHILD_PRESENCE_dum", F.when(F.col("IBX_CHILD_PRESENCE_AGG_HHD") == 'Y', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("job_78_dum", F.when(F.col("IBX_OCCUPATION_INPUT_AGG_HHD").isin('7', '8'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("internet_2to4", F.when(F.col("IBX_TELECOM_INTERNET_AGG_HHD").isin('02', '03', '04'), 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("PERCENT_CIVILIAN_VET", F.when(F.col("CENS_EMPLOY_POP18_PLUS_PERCENT_C").isNull(), 8.9).otherwise(F.col("CENS_EMPLOY_POP18_PLUS_PERCENT_C")))
df_multi_scores = df_multi_scores.withColumn("PERCENT_HISPANIC", F.when(F.col("CENS_ETHNIC_POP_PERCENT_HISPANIC").isNull(), 0.4).otherwise(F.col("CENS_ETHNIC_POP_PERCENT_HISPANIC")))
df_multi_scores = df_multi_scores.withColumn("CENS_ETHNIC_POP_PERCENT_HI_c", F.when(F.col("CENS_ETHNIC_POP_PERCENT_HI_NAT_O").isNull(), 0.1598293).otherwise(F.col("CENS_ETHNIC_POP_PERCENT_HI_NAT_O")))
df_multi_scores = df_multi_scores.withColumn("personic_2_dum", F.when(F.col("IBX_PERSONIC_CLUSTER") == '02', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("education_3_dum", F.when(F.col("IBX_EDUCATION") == '3', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("VOTEPROP2016_c", F.when(F.col("general_election_vote_propensity").isNull(), 85.126).otherwise(F.col("general_election_vote_propensity")))
df_multi_scores = df_multi_scores.withColumn("liveanswer_freq3_6", F.when(F.col("liveanswer_freq3_6").isNull(), 0).otherwise(F.col("liveanswer_freq3_6")))
df_multi_scores = df_multi_scores.withColumn("liveanswer_freq6_12", F.when(F.col("liveanswer_freq6_12").isNull(), 0).otherwise(F.col("liveanswer_freq6_12")))
df_multi_scores = df_multi_scores.withColumn("rpm_score_c", F.when(F.col("rpm_score").isNull(), 9.2585).otherwise(F.col("rpm_score")))
df_multi_scores = df_multi_scores.withColumn("acevnum_dum", F.when(F.col("acev_num").isin('', '0'), 0).otherwise(1))
df_multi_scores = df_multi_scores.withColumn("cat_r_dum", F.when(F.col("category") == 'R', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("ch_acq_D", F.when(F.col("ch_acq") == 'D', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("SY_GUNSCORE_c", F.when(F.col("GUN_OWNERSHIP_MODEL").isNull(), 0.373).otherwise(F.col("GUN_OWNERSHIP_MODEL")))
df_multi_scores = df_multi_scores.withColumn("aarporg_i", F.when(F.col("aarporg_i").isNull(), 0).otherwise(F.col("aarporg_i")))
df_multi_scores = df_multi_scores.withColumn("curterm_12_dum", F.when(F.col("cur_term") == '12', 1).otherwise(0))
df_multi_scores = df_multi_scores.withColumn("logit_medicare", 
    F.lit(-0.4325) +
    F.col("NEW_SCORE2_CENTILE") * -0.00095 +
    F.col("call_freq") * 0.00802 +
    F.col("pct_live") * 0.1436 +
    F.col("liveanswer_freq_3") * 0.0412 +
    F.col("liveanswer_freq3_6") * 0.0746 +
    F.col("liveanswer_freq6_12") * 0.0427 +
    F.col("Party_DEM") * 0.0414 +
    F.col("Party_REP") * 0.0254 +
    F.col("full_time") * -0.1122 +
    F.col("part_time") * -0.0783 +
    F.col("retire") * -0.069 +
    F.col("driver_class_12mo_i_c") * 0.1292 +
    F.col("yeas_survey_12mo_i_c") * 0.1466 +
    F.col("teletown_12mo_ic") * 0.1438 +
    F.col("advocacy_signers_12mo_i_c") * 0.1195 +
    F.col("aarporg_i") * -0.0354 +
    F.col("ethniccode_20_dum") * -0.0665 +
    F.col("goi_missing_dum") * 0.0195 +
    F.col("VoterStatus_active") * 0.0725 +
    F.col("acevnum_dum") * -0.0341 +
    F.col("cat_r_dum") * -0.0987 +
    F.col("ch_acq_D") * -0.0245 +
    F.col("ch_acq_F") * 0.059 +
    F.col("curterm_60_dum") * -0.0541 +
    F.col("curterm_12_dum") * 0.0777 +
    F.col("hitech_dum") * 0.0253 +
    F.col("otherdonors_dum") * 0.0281 +
    F.col("sy_dense") * -0.0357 +
    F.col("IBX_ADULTS_NUM_AGG_HHDls3") * -0.0191 +
    F.col("CHILD_PRESENCE_dum") * -0.0282 +
    F.col("grandchildren_dum") * 0.045 +
    F.col("networth_b_dum") * 0.081 +
    F.col("job_78_dum") * 0.0223 +
    F.col("INTERNET_9_10") * 0.0361 +
    F.col("internet_2to4") * -0.0278 +
    F.col("cruise_y") * 0.0209 +
    F.col("TRAVEL_TYPE_d") * -0.0296 +
    F.col("Pop_pct_Asian_Only__c") * 0.000462 +
    F.col("Pop_pct_Black_Only__c") * 0.000527 +
    F.col("fndn_pro_em_c") * -0.00094 +
    F.col("fndnothr_score_c") * -0.00055 +
    F.col("ideology_c") * 0.00199 +
    F.col("renewals_c") * -0.0434 +
    F.col("rpm_score_c") * -0.0144 +
    F.col("sp_rel_c") * 0.0285 +
    F.col("sy_otsbn_polfund_2012a_c") * 0.000761 +
    F.col("SY_GUNSCORE_c") * 0.1919 +
    F.col("commute_pct_ls30") * -0.00095 +
    F.col("FAMILY_HOUSEHOLDS__c") * -0.00002 +
    F.col("POP25_PLUS_PERCENT_BAC") * -0.00122 +
    F.col("PERCENT_CIVILIAN_VET") * -0.00218 +
    F.col("POP_PERCENT_NON_HISP") * -0.00961 +
    F.col("PERCENT_HISPANIC") * -0.0111 +
    F.col("PERCENT_WHITE") * 0.00583 +
    F.col("CENS_ETHNIC_POP_PERCENT_SOME_c") * 0.00951 +
    F.col("CENS_ETHNIC_POP_PERCENT_HI_c") * -0.0454 +
    F.col("EMP_FINANCE") * -0.00306 +
    F.col("personic_2_dum") * -0.075 +
    F.col("education_3_dum") * 0.0203 +
    F.col("age_50to56_dum") * 0.3007 +
    F.col("age_gt81_dum") * -0.0363 +
    F.col("age_57to64_dum") * -0.0539 +
    F.col("SY_GENERALACTIVIST_c") * -0.00041 +
    F.col("VOTEPROP2016_c") * 0.00128
)
df_multi_scores = df_multi_scores.withColumn("score_medicare", F.exp(F.col("logit_medicare")) / (F.lit(1) + F.exp(F.col("logit_medicare"))))
df_multi_scores = df_multi_scores.select(
    "mid_key", "score_medicaid", "score_work_jobs", "advo_5064_logit", "nps_detractor_score",
    "driver_safety_tek_dm_score", "score_medicare", "age_51to56_dum", "liveanswer_freq6_12",
    "individual_engagers_12mo", "aarporg_i", "mailercount_click_1mos", "cens_marr_pop15_plus_percent_spo",
    "foundation_donations_12mo", "single", "general_activist_model_c", "ch_acq_U",
    "cens_educ_pop25_plus_median_educ", "community_charity_dum", "MemXRenew_c"
)
df_multi_scores.write.format("delta").mode("overwrite").saveAsTable("intermed.multi_scores")

df_multi_scores = spark.table("intermed.multi_scores")
df_multi_scores_with_dummy = df_multi_scores.withColumn("dummy_partition", F.lit(1))
window_spec = Window.partitionBy("dummy_partition")
df_score_ranks = df_multi_scores_with_dummy.withColumn("LO_MEDICAID", F.ntile(99).over(window_spec.orderBy(F.col("score_medicaid").desc())))
df_score_ranks = df_score_ranks.withColumn("work_jobs_em", F.ntile(99).over(window_spec.orderBy(F.col("score_work_jobs").desc())))
df_score_ranks = df_score_ranks.withColumn("advo_dm_50_64", F.ntile(99).over(window_spec.orderBy(F.col("advo_5064_logit").desc())))
df_score_ranks = df_score_ranks.withColumn("nps_detractor", F.ntile(99).over(window_spec.orderBy(F.col("nps_detractor_score").desc())))
df_score_ranks = df_score_ranks.withColumn("driver_safety_tek_dm", F.ntile(99).over(window_spec.orderBy(F.col("driver_safety_tek_dm_score").desc())))
df_score_ranks = df_score_ranks.withColumn("LO_MEDICARE", F.ntile(99).over(window_spec.orderBy(F.col("score_medicare").desc())))
df_score_ranks = df_score_ranks.drop("dummy_partition")
df_score_ranks.write.format("delta").mode("overwrite").saveAsTable("intermed.score_ranks")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_geo_appends_rpm_dedup = df_geo_appends_rpm.dropDuplicates(["merkleid"])
df_geo_appends_rpm_dedup.write.format("delta").mode("overwrite").saveAsTable("intermed.geo_appends_rpm_dedup")

df_score_ranks = spark.table("intermed.score_ranks").filter(F.col("mid_key").isNotNull() & ~F.col("mid_key").isin(1, 0))
df_score_ranks = df_score_ranks.dropDuplicates(["mid_key"])

df_geo_appends_rpm_dedup = spark.table("intermed.geo_appends_rpm_dedup")
df_score_ranks_str_key = df_score_ranks.withColumn("merkleid", F.col("mid_key").cast(StringType()))

df_geo_appends_rpm2 = df_geo_appends_rpm_dedup.alias("a").join(
    df_score_ranks_str_key.alias("b"),
    on="merkleid",
    how="left"
).select(
    (F.col("b.lo_medicaid") + 1).alias("lo_medicaid"),
    (F.col("b.work_jobs_em") + 1).alias("work_jobs_em"),
    (F.col("b.advo_dm_50_64") + 1).alias("advo_dm_50_64"),
    (F.col("b.nps_detractor") + 1).alias("nps_detractor"),
    (F.col("b.driver_safety_tek_dm") + 1).alias("driver_safety_tek_dm"),
    (F.col("b.lo_medicare") + 1).alias("lo_medicare"),
    "a.*"
)
df_geo_appends_rpm2.write.option("compression", "snappy").format("delta").mode("overwrite").saveAsTable("intermed.geo_appends_rpm2")

df_geo_appends_rpm2 = spark.table("intermed.geo_appends_rpm2")
df_web_visits = spark.table("intermed.web_visits")
df_contact_history_sum = spark.table("intermed.contact_history_sum")
df_geo_appends_rpm2.createOrReplaceTempView("geo_appends_rpm2")
df_web_visits.createOrReplaceTempView("web_visits")
df_contact_history_sum.createOrReplaceTempView("contact_history_sum")

df_analysis2 = spark.sql("""
    SELECT
        a.*,
        b.saving_planning_hits,
        c.call_freq,
        c.liveanswer_freq_3,
        c.liveanswer_freq3_6,
        c.liveanswer_freq6_12,
        c.num_click,
        c.num_open_6mo,
        c.click_rate_6mo,
        c.mailercount_click_6mo,
        c.mailercount_click_1_3mo,
        c.num_open_30days,
        c.mailercount_click_30days,
        c.mailercount_click_3_6mo,
        c.mailercount_open_30days,
        c.mailercount_open_3_6mo,
        c.mailercount_open_1_3mo,
        c.num_clicked_curmonth,
        c.num_ib_1_3mo,
        c.num_ib_30days,
        c.num_ib_3_6mo,
        c.num_open_3_6mo,
        c.num_clicked_1_3mo,
        c.num_inb_30days,
        c.num_clicked_past12,
        c.num_ct,
        c.num_sent_past12
    FROM
        geo_appends_rpm2 a
    LEFT JOIN
        web_visits b ON a.mid_key = b.mid_key
    LEFT JOIN
        contact_history_sum c ON a.mid_key = c.mid_key
""")
df_analysis2.write.option("compression", "snappy").format("delta").mode("overwrite").saveAsTable("intermed.analysis2")

df_analysis2 = spark.table("intermed.analysis2")
df_save_plan = df_analysis2
df_save_plan = df_save_plan.withColumn("cat_r_dum", F.when(F.col("category") == 'R', 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("individual_engagers_12mo23", F.when(F.col("individual_engagers_12mo").isin(2, 3, 4), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("children_presence_of_household_n", F.when(F.col("ibx_children_presence_of_househo") == 'N', 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("Env_Humant_Educ_c", F.when(F.col("Env_Humant_Educ") == '1', 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("state_activities_12moc", F.when(F.col("state_activities_12mo") == 0, 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("yeas_survey_12mo_i", F.when(F.col("yeas_survey_12mo_i").isNull(), 0).otherwise(F.col("yeas_survey_12mo_i")))
df_save_plan = df_save_plan.withColumn("CENS_EARN_HH_PERCENT_WITH_SELF_E", F.when(F.col("CENS_EARN_HH_PERCENT_WITH_SELF_E").isNull(), 10.5).otherwise(F.col("CENS_EARN_HH_PERCENT_WITH_SELF_E")))
df_save_plan = df_save_plan.withColumn("Work_Jobs_EM_c", F.when(F.col("Work_Jobs_EM").isNull(), 99).otherwise(F.col("Work_Jobs_EM")))
df_save_plan = df_save_plan.withColumn("EM_clickrate_c", F.when(F.col("EM_clickrate").isNull(), 0).otherwise(F.col("EM_clickrate")))
df_save_plan = df_save_plan.withColumn("ch_acq_D", F.when(F.col("ch_acq") == 'D', 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("ch_acq_U", F.when(F.col("ch_acq") == 'U', 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("ch_acq_I", F.when(F.col("ch_acq") == 'I', 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("commute_pct_public", F.when(F.col("CENS_COMMUTE_WRKRS_PERCENT_PUBLI").isNull(), 5.18).otherwise(F.col("CENS_COMMUTE_WRKRS_PERCENT_PUBLI")))
df_save_plan = df_save_plan.withColumn("UTILITY_gas", F.when(F.col("CENS_HEAT_OCCHU_PERCENT_UTILITY_").isNull(), 49).otherwise(F.col("CENS_HEAT_OCCHU_PERCENT_UTILITY_")))
df_save_plan = df_save_plan.withColumn("PERCENT_FEMALE", F.when(F.col("CENS_GENDER_POP_PERCENT_FEMALE").isNull(), 51.4).otherwise(F.col("CENS_GENDER_POP_PERCENT_FEMALE")))
df_save_plan = df_save_plan.withColumn("POP_PERCENT_NON_HISP", F.when(F.col("CENS_ETHNIC_POP_PERCENT_NON_HISP").isNull(), 87).otherwise(F.col("CENS_ETHNIC_POP_PERCENT_NON_HISP")))
df_save_plan = df_save_plan.withColumn("IBX_HOME_VALUE_RANGES_BCDEF", F.when(F.col("IBX_HOME_VALUE_RANGES_AGG_HHD").isin('K', 'L', 'H', 'I', 'J'), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("cruise_y", F.when(F.col("IBX_TRAVEL_CRUISE_AGG_HHD") == '1', 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("elderly_u_dum", F.when(F.col("IBX_ELDERLY_PARENT_AGG_HHD") == 'U', 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("IBX_ADULTS_NUM_AGG_HHDls3", F.when(F.col("IBX_ADULTS_NUM_AGG_HHD").isin('4', '5', '6'), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("SY_OTSBN_POLFUND_c", F.when(F.col("SY_OTSBN_POLFUND").isNull(), 13).otherwise(F.col("SY_OTSBN_POLFUND")))
df_save_plan = df_save_plan.withColumn("cens_WHITECOLLAR", F.when(F.col("cens_WHITECOLLAR").isNull(), 38).otherwise(F.col("cens_WHITECOLLAR")))
df_save_plan = df_save_plan.withColumn("cens_age_pop_percent_65_99_plus", F.when(F.col("cens_age_pop_percent_65_99_plus").isNull(), 18.8).otherwise(F.col("cens_age_pop_percent_65_99_plus")))
df_save_plan = df_save_plan.withColumn("Overall_Historic_SP_123", F.when(F.col("Overall_Historic_SP_Reltshps").isin('0'), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("ACEV_Num12", F.when(F.col("ACEV_Num").isin('1', '2'), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("R1_ACEV_Flag", F.when(F.col("ACEV_Flag") == 'Y', 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("advo_s23_dum", F.when(F.col("advo_segment_cd").isin('S4', 'S3'), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("Globally_Opted_In_c", F.when(F.col("Globally_Opted_In") == '1', 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("NEW_SCORE50_CENTILE", F.when(F.col("NEW_SCORE50_CENTILE").isNull(), 99).otherwise(F.col("NEW_SCORE50_CENTILE")))
df_save_plan = df_save_plan.withColumn("NEW_SCORE6_CENTILE", F.when(F.col("NEW_SCORE6_CENTILE").isNull(), 99).otherwise(F.col("NEW_SCORE6_CENTILE")))
df_save_plan = df_save_plan.withColumn("age_60to69_dum", F.when((F.col("age_agg_ind") >= 60) & (F.col("age_agg_ind") <= 66), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("age_gt67_dum", F.when(F.col("age_agg_ind") >= 67, 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("ideology_c", F.when(F.col("ideology").isNull(), 42.1).otherwise(F.col("ideology")))
df_save_plan = df_save_plan.withColumn("curterm_60_dum", F.when(F.col("cur_term") == '60', 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("curterm_12_dum", F.when(F.col("cur_term") == '12', 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("driver_class_12mo", F.when(F.col("driver_class_12mo").isNull(), 0).otherwise(F.col("driver_class_12mo")))
df_save_plan = df_save_plan.withColumn("newsletter_opens_cnt_12mo_c", F.when(F.col("newsletter_opens_cnt_12mo").isNull(), 0).otherwise(F.col("newsletter_opens_cnt_12mo")))
df_save_plan = df_save_plan.withColumn("NO_MORTG", F.when(F.col("CENS_MORTG_OOHU_PERCENT_NO_MORTG").isNull(), 34.1).otherwise(F.col("CENS_MORTG_OOHU_PERCENT_NO_MORTG")))
df_save_plan = df_save_plan.withColumn("EMP_ACCOR_FOOD", F.when(F.col("CENS_INDUS_EMPLD_PERCENT_ACCOMOD").isNull(), 6.4).otherwise(F.col("CENS_INDUS_EMPLD_PERCENT_ACCOMOD")))
df_save_plan = df_save_plan.withColumn("CENS_COMMUTE_COMMUTER_PERCENT_TR", F.when(F.col("CENS_COMMUTE_COMMUTER_PERCENT_TR").isNull(), 65).otherwise(F.col("CENS_COMMUTE_COMMUTER_PERCENT_TR")))
df_save_plan = df_save_plan.withColumn("drvsafe_pro_em_c", F.when(F.col("drvsafe_pro_em").isNull(), 50).otherwise(F.col("drvsafe_pro_em")))
df_save_plan = df_save_plan.withColumn("MemXRenew_c", F.when(F.col("MemXRenew").isNull(), 3.03).otherwise(F.col("MemXRenew")))
df_save_plan = df_save_plan.withColumn("Party_Aff", F.when(F.col("PartyAffiliation").isin('DEM', 'REP', 'NPA'), F.col("PartyAffiliation")).otherwise('OTH'))
df_save_plan = df_save_plan.withColumn("Party_DEM", F.when(F.col("Party_Aff") == 'DEM', 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("Past12MoTouchCt_Travel_c", F.when(F.col("Past12MoTouchCt_Travel").isin('1', '2', '3'), 0).otherwise(1))
df_save_plan = df_save_plan.withColumn("Past12MoTouchCt_Overall_c", F.when(F.col("Past12MoTouchCt_Overall").isin('1', '2', '3'), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("IBX_PC_USER_FL_AGG_HHD_dum1", F.when(F.col("IBX_PC_USER_FL_AGG_HHD") == '1', 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("WORKING_WOMAN_y", F.when(F.col("IBX_WORKING_WOMAN_AGG_HHD") == 'Y', 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("live_answer_pm_c", F.when(F.col("live_answer_pm").isNull(), 99).otherwise(F.col("live_answer_pm")))
df_save_plan = df_save_plan.withColumn("saving_planning_hits", F.when(F.col("saving_planning_hits").isNull(), 0).otherwise(F.col("saving_planning_hits")))
df_save_plan = df_save_plan.withColumn("job_78_dum", F.when(F.col("IBX_OCCUPATION_INPUT_AGG_HHD").isin('7', '8'), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("mail_health_dum", F.when(F.col("IBX_MAIL_BUYER_CAT_HEALTH_AGG_HH") == '1', 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("networth_A9_dum", F.when(F.col("IBX_NETWORTH_PREMIER_AGG_HHD").isin('A', 'B', '8', '9'), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("networth_13_dum", F.when(F.col("IBX_NETWORTH_PREMIER_AGG_HHD").isin('1', '2', '3'), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("income_ab_dum", F.when(F.col("IBX_INCOME_ESTIMATED_NARROW_RANG").isin('A', 'B', 'C', 'D'), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("gender_M", F.when(F.col("gender_input") == 'M', 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("call_freq", F.when(F.col("call_freq").isNull(), 0).otherwise(F.col("call_freq")))
df_save_plan = df_save_plan.withColumn("liveanswer_freq_3", F.when(F.col("liveanswer_freq_3").isNull(), 0).otherwise(F.col("liveanswer_freq_3")))
df_save_plan = df_save_plan.withColumn("num_click", F.when(F.col("num_click").isNull(), 0).otherwise(F.col("num_click")))
df_save_plan = df_save_plan.withColumn("num_open_6mo", F.when(F.col("num_open_6mo").isNull(), 0).otherwise(F.col("num_open_6mo")))
df_save_plan = df_save_plan.withColumn("click_rate_6mo", F.when(F.col("click_rate_6mo").isNull(), 0).otherwise(F.col("click_rate_6mo")))
df_save_plan = df_save_plan.withColumn("mailercount_click_6mo", F.when(F.col("mailercount_click_6mo").isNull(), 0).otherwise(F.col("mailercount_click_6mo")))
df_save_plan = df_save_plan.withColumn("save_plan_logit", 
    F.lit(-2.8813) +
    F.col("income_ab_dum") * 0.0639 +
    F.col("networth_13_dum") * -0.2824 +
    F.col("networth_A9_dum") * 0.1531 +
    F.col("mail_health_dum") * -0.0827 +
    F.col("job_78_dum") * -0.1059 +
    F.col("saving_planning_hits") * 0.00589 +
    F.col("call_freq") * -0.0266 +
    F.col("liveanswer_freq_3") * 0.2597 +
    F.col("num_click") * 0.00203 +
    F.col("num_open_6mo") * -0.00219 +
    F.col("click_rate_6mo") * 0.0129 +
    F.col("mailercount_click_6mo") * 0.4445 +
    F.col("live_answer_pm_c") * 0.00269 +
    F.col("WORKING_WOMAN_y") * 0.0996 +
    F.col("IBX_PC_USER_FL_AGG_HHD_dum1") * -0.1201 +
    F.col("Past12MoTouchCt_Overall_c") * 0.2834 +
    F.col("Past12MoTouchCt_Travel_c") * 0.1122 +
    F.col("Party_DEM") * -0.0886 +
    F.col("MemXRenew_c") * -0.0274 +
    F.col("drvsafe_pro_em_c") * -0.00972 +
    F.col("CENS_COMMUTE_COMMUTER_PERCENT_TR") * 0.00344 +
    F.col("EMP_ACCOR_FOOD") * -0.0055 +
    F.col("NO_MORTG") * 0.00429 +
    F.col("newsletter_opens_cnt_12mo_c") * 0.00416 +
    F.col("driver_class_12mo") * -0.3269 +
    F.col("curterm_60_dum") * 0.0894 +
    F.col("curterm_12_dum") * -0.1668 +
    F.col("ideology_c") * -0.00305 +
    F.col("age_60to69_dum") * -0.1582 +
    F.col("age_gt67_dum") * -0.6945 +
    F.col("NEW_SCORE50_CENTILE") * 0.00268 +
    F.col("NEW_SCORE6_CENTILE") * -0.00591 +
    F.col("Globally_Opted_In_c") * -0.8948 +
    F.col("advo_s23_dum") * -0.3742 +
    F.col("R1_ACEV_Flag") * -0.2914 +
    F.col("ACEV_Num12") * 0.1468 +
    F.col("Overall_Historic_SP_123") * 0.0995 +
    F.col("CENS_AGE_POP_PERCENT_65_99_PLUS") * -0.0068 +
    F.col("cens_WHITECOLLAR") * -0.004 +
    F.col("SY_OTSBN_POLFUND_c") * 0.0127 +
    F.col("IBX_ADULTS_NUM_AGG_HHDls3") * 0.0825 +
    F.col("elderly_u_dum") * 0.2551 +
    F.col("cruise_y") * -0.2008 +
    F.col("IBX_HOME_VALUE_RANGES_BCDEF") * 0.2135 +
    F.col("POP_PERCENT_NON_HISP") * 0.00337 +
    F.col("PERCENT_FEMALE") * -0.0107 +
    F.col("UTILITY_gas") * 0.00116 +
    F.col("commute_pct_public") * 0.00332 +
    F.col("ch_acq_D") * 0.1413 +
    F.col("ch_acq_U") * -0.119 +
    F.col("ch_acq_I") * 0.2669 +
    F.col("gender_M") * 0.3559 +
    F.col("cat_r_dum") * -0.0849 +
    F.col("individual_engagers_12mo23") * -0.099 +
    F.col("children_presence_of_household_n") * 0.0849 +
    F.col("Env_Humant_Educ_c") * -0.0818 +
    F.col("state_activities_12moc") * 0.2048 +
    F.col("yeas_survey_12mo_i") * -0.2291 +
    F.col("CENS_EARN_HH_PERCENT_WITH_SELF_E") * 0.00666 +
    F.col("Work_Jobs_EM_c") * -0.0173 +
    F.col("EM_Clickrate_c") * -0.0147
)
df_save_plan = df_save_plan.withColumn("save_plan_score", F.exp(F.col("save_plan_logit")) / (F.lit(1) + F.exp(F.col("save_plan_logit"))))
df_save_plan = df_save_plan.withColumn("age_62to71_dum", F.when((F.col("age_agg_ind") >= 62) & (F.col("age_agg_ind") <= 71), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("state_activity_12mo_i", F.when(F.col("state_activity_12mo_i").isNull(), 0).otherwise(F.col("state_activity_12mo_i")))
df_save_plan = df_save_plan.withColumn("num_open_30days", F.when(F.col("num_open_30days").isNull(), 0).otherwise(F.col("num_open_30days")))
df_save_plan = df_save_plan.withColumn("mailercount_click_1_3mo", F.when(F.col("mailercount_click_1_3mo").isNull(), 0).otherwise(F.col("mailercount_click_1_3mo")))
df_save_plan = df_save_plan.withColumn("medicare_em_score", 
    F.lit(-5.7391) +
    F.col("mailercount_click_1_3mo") * 0.2641 +
    F.col("num_open_30days") * 0.0241 +
    F.col("state_activity_12mo_i") * 0.8319 +
    F.col("age_62to71_dum") * 1.0706 +
    F.col("Work_Jobs_EM_c") * -0.0206 +
    F.col("EM_Clickrate_c") * -0.0212
)
df_save_plan = df_save_plan.withColumn("Medicare_em_attend_score", F.exp(F.col("medicare_em_score")) / (F.lit(1) + F.exp(F.col("medicare_em_score"))))
df_save_plan = df_save_plan.withColumn("num_inb_30days", F.when(F.col("num_inb_30days").isNull(), 0).otherwise(F.col("num_inb_30days")))
df_save_plan = df_save_plan.withColumn("job_1245_dum", F.when(F.col("IBX_OCCUPATION_INPUT_AGG_HHD").isin('1', '2', '4', '3'), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("VEHICLE_DOMINANT_C", F.when(F.col("IBX_VEHICLE_DOMINANT_AGG_HHD").isin('G', 'B'), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("live_answer_pm", F.when(F.col("live_answer_pm").isNull(), 99).otherwise(F.col("live_answer_pm")))
df_save_plan = df_save_plan.withColumn("IBX_FINANCIAL_INVESTOR_HHD_1dum", F.when(F.col("IBX_FINANCIAL_INVESTOR_AGG_HHD") == '1', 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("ibx_pc_user_fl_agg_hhdy", F.when(F.col("ibx_pc_user_fl_agg_hhd") == 'Y', 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("diversity_flag_0", F.when(F.col("diversity_flag_agg_ind").isNull() | (F.col("diversity_flag_agg_ind") == 0), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("CENS_COMMUTE_WRKRS_PERCENT_WOR_c", F.when(F.col("CENS_AGE_POP_PERCENT_55_59").isNull(), 7.46).otherwise(F.col("CENS_AGE_POP_PERCENT_55_59")))
df_save_plan = df_save_plan.withColumn("avg_commte", F.when(F.col("CENS_COMMUTE_COMMUTER_AVG_TRAV_T").isNull(), 27.3).otherwise(F.col("CENS_COMMUTE_COMMUTER_AVG_TRAV_T")))
df_save_plan = df_save_plan.withColumn("sy_otsbn_polfund_2012a_c", F.when(F.col("sy_otsbn_polfund_2012a").isNull(), 51).otherwise(F.col("sy_otsbn_polfund_2012a")))
df_save_plan = df_save_plan.withColumn("sy_otsbn_polfund_2012b_c", F.coalesce(F.col("sy_otsbn_polfund_2012b"), F.lit(32)))
df_save_plan = df_save_plan.withColumn("advocacy_signers_12mo_i1", F.coalesce(F.col("advocacy_signers_12mo_i"), F.lit(0)))
df_save_plan = df_save_plan.withColumn("advocacy_donations_ytd", F.when(F.col("advocacy_donations_ytd").isNull(), 0).otherwise(F.col("advocacy_donations_ytd")))
df_save_plan = df_save_plan.withColumn("lifestage_678", F.when(F.col("Lifestage_Segment").isin('1', '3', '8'), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("curterm_36_dum", F.when(F.col("cur_term") == '36', 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("NEW_SCORE2_CENTILE", F.when(F.col("NEW_SCORE2_CENTILE").isNull(), 50).otherwise(F.col("NEW_SCORE2_CENTILE")))
df_save_plan = df_save_plan.withColumn("NEW_SCORE1_CENTILE_c", F.coalesce(F.col("NEW_SCORE1_CENTILE"), F.lit(55)))
df_save_plan = df_save_plan.withColumn("NEW_SCORE3_CENTILE", F.when(F.col("NEW_SCORE3_CENTILE").isNull(), 56).otherwise(F.col("NEW_SCORE3_CENTILE")))
df_save_plan = df_save_plan.withColumn("age_72to79_dum", F.when((F.col("age_agg_ind") >= 72) & (F.col("age_agg_ind") <= 79), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("age_gt80", F.when(F.col("age_agg_ind") >= 80, 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("adults_number_1", F.when(F.col("IBX_ADULTS_NUM_AGG_HHD").isin('1', '2'), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("R1_IBX_HOUSEHOLD_INCOME129", F.when(F.col("IBX_HOUSEHOLD_INCOME").isin('E', 'F', 'G', 'H', 'I', 'J'), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("PERCENT_HOH_HISPA", F.when(F.col("CENS_ETHNIC_HH_PERCENT_HOH_HISPA").isNull(), 8.13).otherwise(F.col("CENS_ETHNIC_HH_PERCENT_HOH_HISPA")))
df_save_plan = df_save_plan.withColumn("PERCENT_WHITE", F.when(F.col("CENS_ETHNIC_POP_PERCENT_WHITE_ON").isNull(), 74.9).otherwise(F.col("CENS_ETHNIC_POP_PERCENT_WHITE_ON")))
df_save_plan = df_save_plan.withColumn("aarporg_i", F.when(F.col("aarporg_i").isNull(), 0).otherwise(F.col("aarporg_i")))
df_save_plan = df_save_plan.withColumn("EMP_FINANCE", F.when(F.col("CENS_INDUS_EMPLD_PERCENT_FINANCE").isNull(), 4.9).otherwise(F.col("CENS_INDUS_EMPLD_PERCENT_FINANCE")))
df_save_plan = df_save_plan.withColumn("Work_Jobs_EM", F.when(F.col("Work_Jobs_EM").isNull(), 99).otherwise(F.col("Work_Jobs_EM")))
df_save_plan = df_save_plan.withColumn("EM_clickrate_c", F.coalesce(F.col("EM_clickrate"), F.lit(0)))
df_save_plan = df_save_plan.withColumn("mailercount_open_30days", F.when(F.col("mailercount_open_30days").isNull(), 0).otherwise(F.col("mailercount_open_30days")))
df_save_plan = df_save_plan.withColumn("mailercount_click_30days", F.when(F.col("mailercount_click_30days").isNull(), 0).otherwise(F.col("mailercount_click_30days")))
df_save_plan = df_save_plan.withColumn("mailercount_click_3_6mo", F.when(F.col("mailercount_click_3_6mo").isNull(), 0).otherwise(F.col("mailercount_click_3_6mo")))
df_save_plan = df_save_plan.withColumn("fndn_pro_em", F.when(F.col("fndn_pro_em").isNull(), 20).otherwise(F.col("fndn_pro_em")))
df_save_plan = df_save_plan.withColumn("ACEV_Num12", F.when(F.col("ACEV_Num").isin('1', '2'), 1).otherwise(0))
df_save_plan = df_save_plan.withColumn("caregiving_em_attend_logit", 
    F.lit(-2.4534) +
    F.col("mailercount_click_30days") * 0.0697 +
    F.col("mailercount_click_3_6mo") * 0.0681 +
    F.col("mailercount_open_30days") * -0.0379 +
    F.col("num_inb_30days") * 0.0191 +
    F.col("job_1245_dum") * -0.1214 +
    F.col("VEHICLE_DOMINANT_C") * -0.3501 +
    F.col("live_answer_pm") * -0.00373 +
    F.col("IBX_FINANCIAL_INVESTOR_HHD_1dum") * 0.1346 +
    F.col("ibx_pc_user_fl_agg_hhdy") * 0.1768 +
    F.col("MemXRenew_c") * -0.0239 +
    F.col("diversity_flag_0") * -0.4963 +
    F.col("fndn_pro_em") * -0.00817 +
    F.col("CENS_COMMUTE_WRKRS_PERCENT_WOR_c") * 0.0345 +
    F.col("avg_commte") * -0.01 +
    F.col("sy_otsbn_polfund_2012a_c") * -0.00689 +
    F.col("sy_otsbn_polfund_2012b_c") * 0.00488 +
    F.col("state_activity_12mo_i") * 0.8799 +
    F.col("advocacy_signers_12mo_i1") * 0.2799 +
    F.col("advocacy_donations_ytd") * -0.2613 +
    F.col("lifestage_678") * 0.1416 +
    F.col("curterm_36_dum") * -0.131 +
    F.col("curterm_12_dum") * -0.3037 +
    F.col("NEW_SCORE1_CENTILE_c") * -0.00558 +
    F.col("NEW_SCORE2_CENTILE") * -0.00476 +
    F.col("NEW_SCORE3_CENTILE") * 0.00314 +
    F.col("age_72to79_dum") * -0.3356 +
    F.col("age_gt80") * -0.3531 +
    F.col("adults_number_1") * -0.1187 +
    F.col("advo_s23_dum") * 0.3207 +
    F.col("R1_IBX_HOUSEHOLD_INCOME129") * -0.162 +
    F.col("ACEV_Num12") * 1.0157 +
    F.col("SY_OTSBN_POLFUND_c") * -0.02 +
    F.col("PERCENT_HOH_HISPA") * 0.00512 +
    F.col("PERCENT_WHITE") * -0.0072 +
    F.col("UTILITY_gas") * -0.00199 +
    F.col("gender_M") * -0.3575 +
    F.col("cat_r_dum") * 0.2062 +
    F.col("aarporg_i") * 0.5068 +
    F.col("EMP_FINANCE") * -0.0255 +
    F.col("Work_Jobs_EM") * -0.0305 +
    F.col("EM_Clickrate_c") * -0.00782
)
df_save_plan = df_save_plan.withColumn("caregiving_em_attend_score", F.exp(F.col("caregiving_em_attend_logit")) / (F.lit(1) + F.exp(F.col("caregiving_em_attend_logit"))))
df_save_plan = df_save_plan.select("mid_key", "save_plan_score", "Medicare_em_attend_score", "caregiving_em_attend_score", "memstatus")
df_save_plan.write.format("delta").mode("overwrite").saveAsTable("intermed.save_plan")

df_save_plan = spark.table("intermed.save_plan")
df_save_plan_with_dummy = df_save_plan.withColumn("dummy_partition", F.lit(1))
window_spec = Window.partitionBy("dummy_partition")
df_score_ranks = df_save_plan_with_dummy.withColumn("save_plan", F.ntile(99).over(window_spec.orderBy(F.col("save_plan_score").desc())))
df_score_ranks = df_score_ranks.withColumn("Medicare_em_attend", F.ntile(99).over(window_spec.orderBy(F.col("Medicare_em_attend_score").desc())))
df_score_ranks = df_score_ranks.withColumn("caregiving_em_attend", F.ntile(99).over(window_spec.orderBy(F.col("caregiving_em_attend_score").desc())))
df_score_ranks = df_score_ranks.drop("dummy_partition")
df_score_ranks.write.format("delta").mode("overwrite").saveAsTable("intermed.score_ranks")

df_geo_appends_rpm2 = spark.table("intermed.geo_appends_rpm2")
df_score_ranks = spark.table("intermed.score_ranks")
df_geo_appends_rpm2.createOrReplaceTempView("geo_appends_rpm2")
df_score_ranks.createOrReplaceTempView("score_ranks")

df_geo_appends_rpm_updated = spark.sql("""
    SELECT
        b.save_plan + 1 AS save_plan,
        b.Medicare_em_attend + 1 AS Medicare_em_attend,
        b.caregiving_em_attend + 1 AS caregiving_em_attend,
        a.*
    FROM
        intermed.geo_appends_rpm2 AS a
    LEFT JOIN
        intermed.score_ranks AS b ON a.mid_key = b.mid_key
""")
df_geo_appends_rpm_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_contact_history_sum = spark.table("intermed.contact_history_sum")
df_geo_appends_rpm = df_geo_appends_rpm.withColumn("merkleid_str", F.col("merkleid").cast(StringType()))
df_contact_history_sum = df_contact_history_sum.withColumn("mid_key_str", F.col("mid_key").cast(StringType()))

df_ipl_input = df_geo_appends_rpm.alias("a").join(
    df_contact_history_sum.alias("b"),
    F.col("a.merkleid_str") == F.col("b.mid_key_str"),
    "left"
).select(
    "a.*",
    "b.call_freq", "b.liveanswer_freq_3", "b.liveanswer_freq3_6", "b.mailercount_click_30days",
    "b.mailercount_open_30days", "b.mailercount_open_1_3mo", "b.mailercount_open_3_6mo",
    "b.num_ib_1_3mo", "b.num_ib_3_6mo", "b.num_clicked_curmonth", "b.num_clicked_1_3mo", "b.num_open_30days"
).drop("merkleid_str", "mid_key_str")
df_ipl_input.write.option("compression", "snappy").format("delta").mode("overwrite").saveAsTable("intermed.ipl_input")

df_ipl_input = spark.table("intermed.ipl_input")
df_ipl_models = df_ipl_input
df_ipl_models = df_ipl_models.withColumn("click_rate_1mos", F.col("num_clicked_curmonth") / F.col("num_open_30days"))
df_ipl_models = df_ipl_models.withColumn("click_rate_1mos", F.when(F.col("click_rate_1mos").isNull(), 0).otherwise(F.col("click_rate_1mos")))
df_ipl_models = df_ipl_models.withColumn("liveanswer_freq_3", F.when(F.col("liveanswer_freq_3").isNull(), 0).otherwise(F.col("liveanswer_freq_3")))
df_ipl_models = df_ipl_models.withColumn("advo_s34_dum", F.when(F.col("advo_segment_cd").isin('S4', 'S3'), 1).otherwise(0))
df_ipl_models = df_ipl_models.withColumn("individual_engagers_12mo", F.when(F.col("individual_engagers_12mo").isNull(), 0.7031802).otherwise(F.col("individual_engagers_12mo")))
df_ipl_models = df_ipl_models.withColumn("aarporg_i", F.when(F.col("aarporg_i").isNull(), 0).otherwise(F.col("aarporg_i")))
df_ipl_models = df_ipl_models.withColumn("age_71to81_dum", F.when((F.col("age_agg_ind") >= 71) & (F.col("age_agg_ind") <= 81), 1).otherwise(0))
df_ipl_models = df_ipl_models.withColumn("age_gt82", F.when(F.col("age_agg_ind") >= 82, 1).otherwise(0))
df_ipl_models = df_ipl_models.withColumn("mailercount_open_3_6mo", F.when(F.col("mailercount_open_3_6mo").isNull(), 0).otherwise(F.col("mailercount_open_3_6mo")))
df_ipl_models = df_ipl_models.withColumn("cens_density_persons_per_hh_for_", F.when(F.col("cens_density_persons_per_hh_for_").isNull(), 2.5294776).otherwise(F.col("cens_density_persons_per_hh_for_")))
df_ipl_models = df_ipl_models.withColumn("call_freq", F.when(F.col("call_freq").isNull(), 0).otherwise(F.col("call_freq")))
df_ipl_models = df_ipl_models.withColumn("goi_dum", F.when(F.col("EMAILABLE_AGG_IND") == 'Y', 1).otherwise(0))
df_ipl_models = df_ipl_models.withColumn("state_activities_12mo", F.when(F.col("state_activities_12mo").isNull(), 0.0255043).otherwise(F.col("state_activities_12mo")))
df_ipl_models = df_ipl_models.withColumn("cens_occup_empld_percent_bus_and", F.when(F.col("cens_occup_empld_percent_bus_and").isNull(), 4.4665885).otherwise(F.col("cens_occup_empld_percent_bus_and")))
df_ipl_models = df_ipl_models.withColumn("state_event_12mo", F.when(F.col("state_event_12mo").isNull(), 1.1858166).otherwise(F.col("state_event_12mo")))
df_ipl_models = df_ipl_models.withColumn("moviesfg_12mo", F.when(F.col("moviesfg_12mo").isNull(), 0.3852711).otherwise(F.col("moviesfg_12mo")))
df_ipl_models = df_ipl_models.withColumn("cens_occup_empld_percent_healthc", F.when(F.col("cens_occup_empld_percent_healthc").isNull(), 1.9956405).otherwise(F.col("cens_occup_empld_percent_healthc")))
df_ipl_models = df_ipl_models.withColumn("general_activist_model", F.when(F.col("general_activist_model").isNull(), 61.3698537).otherwise(F.col("general_activist_model")))
df_ipl_models = df_ipl_models.withColumn("cens_urban_pop_percent_urban_in_", F.when(F.col("cens_urban_pop_percent_urban_in_").isNull(), 66.2287488).otherwise(F.col("cens_urban_pop_percent_urban_in_")))
df_ipl_models = df_ipl_models.withColumn("cens_hustr_hu_percent_1_unit_det", F.when(F.col("cens_hustr_hu_percent_1_unit_det").isNull(), 73.2272901).otherwise(F.col("cens_hustr_hu_percent_1_unit_det")))
df_ipl_models = df_ipl_models.withColumn("cens_age_pop_percent_45_54", F.when(F.col("cens_age_pop_percent_45_54").isNull(), 13.5680336).otherwise(F.col("cens_age_pop_percent_45_54")))
df_ipl_models = df_ipl_models.withColumn("gun_ownership_model_i", F.when(F.col("gun_ownership_model_i").isNull(), 60).otherwise(F.col("gun_ownership_model_i")))
df_ipl_models = df_ipl_models.withColumn("cens_educ_pop25_plus_median_educ", F.when(F.col("cens_educ_pop25_plus_median_educ").isNull(), 12.5875249).otherwise(F.col("cens_educ_pop25_plus_median_educ")))
df_ipl_models = df_ipl_models.withColumn("cens_occup_empld_percent_law_enf", F.when(F.col("cens_occup_empld_percent_law_enf").isNull(), 1.210143).otherwise(F.col("cens_occup_empld_percent_law_enf")))
df_ipl_models = df_ipl_models.withColumn("cens_rent_rntl_median_rent", F.when(F.col("cens_rent_rntl_median_rent").isNull(), 856.7530365).otherwise(F.col("cens_rent_rntl_median_rent")))
df_ipl_models = df_ipl_models.withColumn("cens_lang_hh_percent_spanish_spe", F.when(F.col("cens_lang_hh_percent_spanish_spe").isNull(), 8.3127669).otherwise(F.col("cens_lang_hh_percent_spanish_spe")))
df_ipl_models = df_ipl_models.withColumn("driver_safety_tek_dm", F.when(F.col("driver_safety_tek_dm").isNull(), 37.7293443).otherwise(F.col("driver_safety_tek_dm")))
df_ipl_models = df_ipl_models.withColumn("caregiving_em_attend", F.when(F.col("caregiving_em_attend").isNull(), 58.456352).otherwise(F.col("caregiving_em_attend")))
df_ipl_models = df_ipl_models.withColumn("IPL_CARE_DM_REG_logit", 
    F.lit(-1.9034) +
    F.col("liveanswer_freq_3") * 0.3568 +
    F.col("advo_s34_dum") * 0.5077 +
    F.col("individual_engagers_12mo") * 0.1551 +
    F.col("aarporg_i") * -0.4299 +
    F.col("age_71to81_dum") * -0.3577 +
    F.col("age_gt82") * 2.6146 +
    F.col("mailercount_open_3_6mo") * -0.45 +
    F.col("cens_density_persons_per_hh_for_") * -0.5544 +
    F.col("call_freq") * -0.1542 +
    F.col("goi_dum") * -0.3151 +
    F.col("state_activities_12mo") * 0.2849 +
    F.col("cens_occup_empld_percent_bus_and") * 0.0257 +
    F.col("state_event_12mo") * -0.5647 +
    F.col("moviesfg_12mo") * -0.5535 +
    F.col("cens_occup_empld_percent_healthc") * 0.0284 +
    F.col("general_activist_model") * 0.00606 +
    F.col("cens_urban_pop_percent_urban_in_") * 0.011 +
    F.col("cens_hustr_hu_percent_1_unit_det") * 0.0104 +
    F.col("cens_age_pop_percent_45_54") * 0.0509 +
    F.col("gun_ownership_model_i") * -0.0114 +
    F.col("cens_educ_pop25_plus_median_educ") * -0.2188 +
    F.col("cens_occup_empld_percent_law_enf") * 0.0535 +
    F.col("cens_rent_rntl_median_rent") * -0.00051 +
    F.col("cens_lang_hh_percent_spanish_spe") * 0.0103 +
    F.col("driver_safety_tek_dm") * -0.0249 +
    F.col("caregiving_em_attend") * -0.0143
)
df_ipl_models = df_ipl_models.withColumn("IPL_CARE_DM_REG_score", F.exp(F.col("IPL_CARE_DM_REG_logit")) / (F.lit(1) + F.exp(F.col("IPL_CARE_DM_REG_logit"))))
df_ipl_models = df_ipl_models.withColumn("liveanswer_freq3_6", F.when(F.col("liveanswer_freq3_6").isNull(), 0).otherwise(F.col("liveanswer_freq3_6")))
df_ipl_models = df_ipl_models.withColumn("mailercount_open_30days", F.when(F.col("mailercount_open_30days").isNull(), 0).otherwise(F.col("mailercount_open_30days")))
df_ipl_models = df_ipl_models.withColumn("num_ib_3_6mo", F.when(F.col("num_ib_3_6mo").isNull(), 0).otherwise(F.col("num_ib_3_6mo")))
df_ipl_models = df_ipl_models.withColumn("activist_dum", F.when(F.col("activist").isin('S3', 'S5'), 1).otherwise(0))
df_ipl_models = df_ipl_models.withColumn("cens_move_occhu_percent_new_list", F.when(F.col("cens_move_occhu_percent_new_list").isNull(), 27.9431599).otherwise(F.col("cens_move_occhu_percent_new_list")))
df_ipl_models = df_ipl_models.withColumn("cens_inc_hh_median_household_inc", F.when(F.col("cens_inc_hh_median_household_inc").isNull(), 68436.42).otherwise(F.col("cens_inc_hh_median_household_inc")))
df_ipl_models = df_ipl_models.withColumn("cens_typ_pop_percent_grandchild_", F.when(F.col("cens_typ_pop_percent_grandchild_").isNull(), 2.1370728).otherwise(F.col("cens_typ_pop_percent_grandchild_")))
df_ipl_models = df_ipl_models.withColumn("ch_acq_U", F.when(F.col("ch_acq") == 'U', 1).otherwise(0))
df_ipl_models = df_ipl_models.withColumn("CELL9_10", F.when(F.col("IBX_TELECOM_CELLULAR_AGG_HHD").isin('09', '10'), 1).otherwise(0))
df_ipl_models = df_ipl_models.withColumn("driver_class_12mo", F.when(F.col("driver_class_12mo").isNull(), 0.0087473).otherwise(F.col("driver_class_12mo")))
df_ipl_models = df_ipl_models.withColumn("driver_online_12mo", F.when(F.col("driver_online_12mo").isNull(), 0.0039667).otherwise(F.col("driver_online_12mo")))
df_ipl_models = df_ipl_models.withColumn("IPL_JOB_DM_REG_logit", 
    F.lit(-3.4162) +
    F.col("liveanswer_freq3_6") * 0.3409 +
    F.col("advo_s34_dum") * 0.548 +
    F.col("aarporg_i") * -0.2545 +
    F.col("age_71to81_dum") * -2.2724 +
    F.col("mailercount_open_30days") * -0.372 +
    F.col("mailercount_open_3_6mo") * -0.6503 +
    F.col("num_ib_3_6mo") * 0.0284 +
    F.col("activist_dum") * 0.5457 +
    F.col("state_activities_12mo") * 0.2242 +
    F.col("cens_move_occhu_percent_new_list") * 0.0112 +
    F.col("state_event_12mo") * -0.4277 +
    F.col("moviesfg_12mo") * -0.5726 +
    F.col("cens_inc_hh_median_household_inc") * -0.00000617 +
    F.col("general_activist_model") * 0.00565 +
    F.col("gun_ownership_model_i") * -0.00882 +
    F.col("cens_typ_pop_percent_grandchild_") * -0.0458 +
    F.col("ch_acq_U") * -0.1923 +
    F.col("CELL9_10") * -0.2256 +
    F.col("driver_class_12mo") * 0.964 +
    F.col("driver_online_12mo") * 0.7315 +
    F.col("driver_safety_tek_dm") * -0.018 +
    F.col("caregiving_em_attend") * -0.026
)
df_ipl_models = df_ipl_models.withColumn("IPL_JOB_DM_REG_score", F.exp(F.col("IPL_JOB_DM_REG_logit")) / (F.lit(1) + F.exp(F.col("IPL_JOB_DM_REG_logit"))))
df_ipl_models = df_ipl_models.withColumn("age_51to60_dum", F.when((F.col("age_agg_ind") >= 51) & (F.col("age_agg_ind") <= 60), 1).otherwise(0))
df_ipl_models = df_ipl_models.withColumn("click_rate_1mos", F.when(F.col("click_rate_1mos").isNull(), 0).otherwise(F.col("click_rate_1mos")))
df_ipl_models = df_ipl_models.withColumn("mailercount_open_1_3mo", F.when(F.col("mailercount_open_1_3mo").isNull(), 0).otherwise(F.col("mailercount_open_1_3mo")))
df_ipl_models = df_ipl_models.withColumn("cens_commute_commuter_percent_tr", F.when(F.col("cens_commute_commuter_percent_tr").isNull(), 69.7398104).otherwise(F.col("cens_commute_commuter_percent_tr")))
df_ipl_models = df_ipl_models.withColumn("cens_homval_oohu_median_home_val", F.when(F.col("cens_homval_oohu_median_home_val").isNull(), 232924.81).otherwise(F.col("cens_homval_oohu_median_home_val")))
df_ipl_models = df_ipl_models.withColumn("CHILD_PRESENCE_dum", F.when(F.col("IBX_CHILD_PRESENCE_AGG_HHD") == 'Y', 1).otherwise(0))
df_ipl_models = df_ipl_models.withColumn("newsletter_opens_cnt_12mo", F.coalesce(F.col("newsletter_opens_cnt_12mo"), F.lit(3.264157)))
df_ipl_models = df_ipl_models.withColumn("cens_indus_empld_percent_educati", F.when(F.col("cens_indus_empld_percent_educati").isNull(), 9.1990768).otherwise(F.col("cens_indus_empld_percent_educati")))
df_ipl_models = df_ipl_models.withColumn("cens_mortg_oohu_percent_no_mortg", F.when(F.col("cens_mortg_oohu_percent_no_mortg").isNull(), 34.03
454).otherwise(F.col("cens_mortg_oohu_percent_no_mortg")))
df_ipl_models = df_ipl_models.withColumn("cens_commute_commuter_avg_trav_t", F.when(F.col("cens_commute_commuter_avg_trav_t").isNull(), 25.3309236).otherwise(F.col("cens_commute_commuter_avg_trav_t")))
df_ipl_models = df_ipl_models.withColumn("cens_census_tract", F.when(F.col("cens_census_tract").isNull(), 206411.28).otherwise(F.col("cens_census_tract")))
df_ipl_models = df_ipl_models.withColumn("cens_ethnic_pop_percent_white_on", F.when(F.col("cens_ethnic_pop_percent_white_on").isNull(), 76.9225635).otherwise(F.col("cens_ethnic_pop_percent_white_on")))
df_ipl_models = df_ipl_models.withColumn("cens_inc_hh_percent_household_in", F.when(F.col("cens_inc_hh_percent_household_in").isNull(), 4.986367).otherwise(F.col("cens_inc_hh_percent_household_in")))
df_ipl_models = df_ipl_models.withColumn("no_hunter", F.when(F.col("HUNTER_MODEL_char") == 'Unlikely Hunter', 1).otherwise(0))
df_ipl_models = df_ipl_models.withColumn("fiscal_policy_model", F.when(F.col("fiscal_policy_model").isNull(), 46.9632164).otherwise(F.col("fiscal_policy_model")))
df_ipl_models = df_ipl_models.withColumn("job_78_dum", F.when(F.col("IBX_OCCUPATION_INPUT_AGG_HHD").isin('7', '8', '9', 'Y', 'Z'), 1).otherwise(0))
df_ipl_models = df_ipl_models.withColumn("CELL1_4", F.when(F.col("IBX_TELECOM_CELLULAR_AGG_HHD").isin('01', '02', '03', '04'), 1).otherwise(0))
df_ipl_models = df_ipl_models.withColumn("cens_inc_family_inc_state_decile", F.when(F.col("cens_inc_family_inc_state_decile").isNull(), 5.0566134).otherwise(F.col("cens_inc_family_inc_state_decile")))
df_ipl_models = df_ipl_models.withColumn("EM_Clickrate", F.when(F.col("EM_Clickrate").isNull(), 50).otherwise(F.col("EM_Clickrate")))
df_ipl_models = df_ipl_models.withColumn("IPL_TECH_DM_REG_logit", 
    F.lit(-4.3438) +
    F.col("advo_s34_dum") * 0.3242 +
    F.col("individual_engagers_12mo") * 0.1907 +
    F.col("aarporg_i") * -0.4427 +
    F.col("age_51to60_dum") * -1.2937 +
    F.col("age_71to81_dum") * 0.339 +
    F.col("click_rate_1mos") * -1.0764 +
    F.col("mailercount_open_1_3mo") * -0.2185 +
    F.col("goi_dum") * -0.3388 +
    F.col("cens_commute_commuter_percent_tr") * 0.0167 +
    F.col("cens_homval_oohu_median_home_val") * -0.000000569 +
    F.col("state_activities_12mo") * 0.2502 +
    F.col("CHILD_PRESENCE_dum") * -0.1741 +
    F.col("newsletter_opens_cnt_12mo") * 0.00164 +
    F.col("state_event_12mo") * -0.2804 +
    F.col("moviesfg_12mo") * -0.6457 +
    F.col("cens_indus_empld_percent_educati") * 0.0103 +
    F.col("general_activist_model") * 0.00623 +
    F.col("cens_mortg_oohu_percent_no_mortg") * 0.00615 +
    F.col("cens_commute_commuter_avg_trav_t") * 0.0227 +
    F.col("cens_census_tract") * -0.00000044 +
    F.col("cens_urban_pop_percent_urban_in_") * -0.00377 +
    F.col("gun_ownership_model_i") * -0.0078 +
    F.col("cens_ethnic_pop_percent_white_on") * -0.00619 +
    F.col("cens_inc_hh_percent_household_in") * 0.0145 +
    F.col("ch_acq_U") * -0.3716 +
    F.col("no_hunter") * 0.2411 +
    F.col("fiscal_policy_model") * -0.00307 +
    F.col("cens_lang_hh_percent_spanish_spe") * 0.00634 +
    F.col("job_78_dum") * 0.2365 +
    F.col("CELL1_4") * -0.3168 +
    F.col("CELL9_10") * -0.1363 +
    F.col("cens_inc_family_inc_state_decile") * 0.041 +
    F.col("driver_class_12mo") * 0.7301 +
    F.col("driver_safety_tek_dm") * -0.0263 +
    F.col("caregiving_em_attend") * -0.00378 +
    F.col("EM_Clickrate") * -0.0065
)
df_ipl_models = df_ipl_models.withColumn("IPL_TECH_DM_REG_score", F.exp(F.col("IPL_TECH_DM_REG_logit")) / (F.lit(1) + F.exp(F.col("IPL_TECH_DM_REG_logit"))))
df_ipl_models = df_ipl_models.withColumn("num_clicked_1_3mo", F.when(F.col("num_clicked_1_3mo").isNull(), 0).otherwise(F.col("num_clicked_1_3mo")))
df_ipl_models = df_ipl_models.withColumn("grandchildren_dum", F.when(F.col("IBX_GRAND_CHILDREN_AGG_HHD") == 'Y', 1).otherwise(0))
df_ipl_models = df_ipl_models.withColumn("ibx_adult_age_75", F.when(F.col("ibx_adult_age_75_p_agg_hhd") == '1', 1).otherwise(0))
df_ipl_models = df_ipl_models.withColumn("cens_ethnic_pop_percent_hispanic", F.when(F.col("cens_ethnic_pop_percent_hispanic").isNull(), 12.4651286).otherwise(F.col("cens_ethnic_pop_percent_hispanic")))
df_ipl_models = df_ipl_models.withColumn("cens_heat_occhu_percent_other_he", F.when(F.col("cens_heat_occhu_percent_other_he").isNull(), 0.5653551).otherwise(F.col("cens_heat_occhu_percent_other_he")))
df_ipl_models = df_ipl_models.withColumn("ipl_care_em_logit", 
    F.lit(-1.5324) +
    F.col("age_71to81_dum") * -0.7208 +
    F.col("age_gt82") * 2.6436 +
    F.col("mailercount_open_1_3mo") * -0.2649 +
    F.col("num_clicked_1_3mo") * 0.1215 +
    F.col("cens_density_persons_per_hh_for_") * -0.4909 +
    F.col("goi_dum") * -1.9469 +
    F.col("state_activities_12mo") * 0.3684 +
    F.col("grandchildren_dum") * 0.3722 +
    F.col("newsletter_opens_cnt_12mo") * 0.00267 +
    F.col("state_event_12mo") * -0.8018 +
    F.col("moviesfg_12mo") * -0.9754 +
    F.col("ibx_adult_age_75") * 0.3942 +
    F.col("general_activist_model") * 0.0104 +
    F.col("gun_ownership_model_i") * -0.0091 +
    F.col("cens_typ_pop_percent_grandchild_") * 0.108 +
    F.col("cens_ethnic_pop_percent_hispanic") * 0.013 +
    F.col("cens_occup_empld_percent_law_enf") * 0.0496 +
    F.col("cens_heat_occhu_percent_other_he") * -0.2032 +
    F.col("CELL9_10") * -0.4534 +
    F.col("driver_safety_tek_dm") * -0.0506 +
    F.col("caregiving_em_attend") * -0.0163
)
df_ipl_models = df_ipl_models.withColumn("ipl_care_em_score", F.exp(F.col("ipl_care_em_logit")) / (F.lit(1) + F.exp(F.col("ipl_care_em_logit"))))
df_ipl_models = df_ipl_models.withColumn("mailercount_click_30days", F.when(F.col("mailercount_click_30days").isNull(), 0).otherwise(F.col("mailercount_click_30days")))
df_ipl_models = df_ipl_models.withColumn("num_clicked_curmonth", F.when(F.col("num_clicked_curmonth").isNull(), 0).otherwise(F.col("num_clicked_curmonth")))
df_ipl_models = df_ipl_models.withColumn("single", F.when(F.col("MaritalStatus") == 'S', 1).otherwise(0))
df_ipl_models = df_ipl_models.withColumn("cens_ethnic_pop_percent_some_oth", F.when(F.col("cens_ethnic_pop_percent_some_oth").isNull(), 4.9474971).otherwise(F.col("cens_ethnic_pop_percent_some_oth")))
df_ipl_models = df_ipl_models.withColumn("petition_col_12mo", F.when(F.col("petition_col_12mo").isNull(), 0.1998559).otherwise(F.col("petition_col_12mo")))
df_ipl_models = df_ipl_models.withColumn("cens_educ_pop25_plus_percent_bac", F.when(F.col("cens_educ_pop25_plus_percent_bac").isNull(), 18.8933979).otherwise(F.col("cens_educ_pop25_plus_percent_bac")))
df_ipl_models = df_ipl_models.withColumn("cens_indus_empld_percent_manufac", F.when(F.col("cens_indus_empld_percent_manufac").isNull(), 7.6021087).otherwise(F.col("cens_indus_empld_percent_manufac")))
df_ipl_models = df_ipl_models.withColumn("educational_attainment_model", F.col("educational_attainment_model") * 100)
df_ipl_models = df_ipl_models.withColumn("educational_attainment_model", F.when(F.col("educational_attainment_model").isNull(), 32.2845081).otherwise(F.col("educational_attainment_model")))
df_ipl_models = df_ipl_models.withColumn("work_jobs_em", F.when(F.col("work_jobs_em").isNull(), 57.9448769).otherwise(F.col("work_jobs_em")))
df_ipl_models = df_ipl_models.withColumn("ipl_job_em_logit", 
    F.lit(-2.0578) +
    F.col("advo_s34_dum") * 0.4741 +
    F.col("individual_engagers_12mo") * 0.1132 +
    F.col("age_71to81_dum") * -1.0554 +
    F.col("mailercount_click_30days") * -0.7643 +
    F.col("mailercount_open_1_3mo") * -0.2579 +
    F.col("num_clicked_curmonth") * 0.1671 +
    F.col("cens_density_persons_per_hh_for_") * -0.5485 +
    F.col("goi_dum") * -0.8204 +
    F.col("state_activities_12mo") * 0.2951 +
    F.col("single") * 0.2177 +
    F.col("state_event_12mo") * -0.5024 +
    F.col("moviesfg_12mo") * -1.3822 +
    F.col("cens_age_pop_percent_45_54") * 0.0347 +
    F.col("cens_ethnic_pop_percent_some_oth") * 0.0196 +
    F.col("cens_ethnic_pop_percent_white_on") * -0.00539 +
    F.col("petition_col_12mo") * 0.8738 +
    F.col("cens_educ_pop25_plus_percent_bac") * 0.0172 +
    F.col("cens_indus_empld_percent_manufac") * -0.02 +
    F.col("fiscal_policy_model") * 0.00527 +
    F.col("educational_attainment_model") * -0.0112 +
    F.col("CELL9_10") * -0.2895 +
    F.col("driver_class_12mo") * 0.7354 +
    F.col("driver_safety_tek_dm") * -0.0238 +
    F.col("caregiving_em_attend") * -0.0329 +
    F.col("work_jobs_em") * -0.0319
)
df_ipl_models = df_ipl_models.withColumn("ipl_job_em_score", F.exp(F.col("ipl_job_em_logit")) / (F.lit(1) + F.exp(F.col("ipl_job_em_logit"))))
df_ipl_models = df_ipl_models.withColumn("MemXrenew", F.when(F.col("MemXrenew").isNull(), 0).otherwise(F.col("MemXrenew")))
df_ipl_models = df_ipl_models.withColumn("age_61to70_dum", F.when((F.col("age_agg_ind") >= 61) & (F.col("age_agg_ind") <= 70), 1).otherwise(0))
df_ipl_models = df_ipl_models.withColumn("num_ib_1_3mo", F.when(F.col("num_ib_1_3mo").isNull(), 0).otherwise(F.col("num_ib_1_3mo")))
df_ipl_models = df_ipl_models.withColumn("num_open_30days", F.when(F.col("num_open_30days").isNull(), 0).otherwise(F.col("num_open_30days")))
df_ipl_models = df_ipl_models.withColumn("cens_move_occhu_percent_turnover", F.when(F.col("cens_move_occhu_percent_turnover").isNull(), 31.3522392).otherwise(F.col("cens_move_occhu_percent_turnover")))
df_ipl_models = df_ipl_models.withColumn("advocacy_donations_12mo", F.when(F.col("advocacy_donations_12mo").isNull(), 0.0756742).otherwise(F.col("advocacy_donations_12mo")))
df_ipl_models = df_ipl_models.withColumn("cens_marr_pop15_plus_percent_spo", F.when(F.col("cens_marr_pop15_plus_percent_spo").isNull(), 48.8945914).otherwise(F.col("cens_marr_pop15_plus_percent_spo")))
df_ipl_models = df_ipl_models.withColumn("cens_heat_occhu_percent_oil_or_k", F.when(F.col("cens_heat_occhu_percent_oil_or_k").isNull(), 9.7654324).otherwise(F.col("cens_heat_occhu_percent_oil_or_k")))
df_ipl_models = df_ipl_models.withColumn("ibx_adult_25_34", F.when(F.col("ibx_adult_age_25_34_agg_hhd") == '1', 1).otherwise(0))
df_ipl_models = df_ipl_models.withColumn("petition_sign_12mo", F.when(F.col("petition_sign_12mo").isNull(), 0.0064377).otherwise(F.col("petition_sign_12mo")))
df_ipl_models = df_ipl_models.withColumn("ipl_tek_em_logit", 
    F.lit(-2.0433) +
    F.col("EM_Clickrate") * -0.00449 +
    F.col("MemXRenew") * 0.0345 +
    F.col("individual_engagers_12mo") * 0.2449 +
    F.col("aarporg_i") * -0.7462 +
    F.col("age_61to70_dum") * 1.2135 +
    F.col("age_71to81_dum") * 1.6491 +
    F.col("age_gt82") * 3.8526 +
    F.col("mailercount_open_30days") * -0.4774 +
    F.col("num_ib_1_3mo") * -0.0391 +
    F.col("num_open_30days") * 0.0598 +
    F.col("goi_dum") * -1.4407 +
    F.col("cens_move_occhu_percent_turnover") * -0.0177 +
    F.col("advocacy_donations_12mo") * -0.4039 +
    F.col("state_activities_12mo") * 0.3272 +
    F.col("cens_marr_pop15_plus_percent_spo") * -0.015 +
    F.col("state_event_12mo") * -0.337 +
    F.col("moviesfg_12mo") * -0.6903 +
    F.col("general_activist_model") * 0.00925 +
    F.col("cens_heat_occhu_percent_oil_or_k") * -0.014 +
    F.col("cens_census_tract") * -0.000000633 +
    F.col("gun_ownership_model_i") * -0.00656 +
    F.col("ibx_adult_25_34") * -0.3987 +
    F.col("ch_acq_U") * -0.4493 +
    F.col("petition_sign_12mo") * -1.0781 +
    F.col("cens_indus_empld_percent_manufac") * -0.0241 +
    F.col("cens_occup_empld_percent_law_enf") * 0.0517 +
    F.col("driver_safety_tek_dm") * -0.0452 +
    F.col("caregiving_em_attend") * -0.012
)
df_ipl_models = df_ipl_models.withColumn("ipl_tek_em_score", F.exp(F.col("ipl_tek_em_logit")) / (F.lit(1) + F.exp(F.col("ipl_tek_em_logit"))))
df_ipl_models = df_ipl_models.select("mid_key", "IPL_CARE_DM_REG_score", "IPL_JOB_DM_REG_score", "IPL_TECH_DM_REG_score", "ipl_care_em_score", "ipl_job_em_score", "ipl_tek_em_score")
df_ipl_models.write.format("delta").mode("overwrite").saveAsTable("intermed.ipl_models")

df_ipl_models = spark.table("intermed.ipl_models")
df_ipl_models_with_dummy = df_ipl_models.withColumn("dummy_partition", F.lit(1))
window_spec = Window.partitionBy("dummy_partition")
df_score_ranks = df_ipl_models_with_dummy.withColumn("IPL_CARE_DM_REG", F.ntile(99).over(window_spec.orderBy(F.col("IPL_CARE_DM_REG_score").desc())))
df_score_ranks = df_score_ranks.withColumn("IPL_JOB_DM_REG", F.ntile(99).over(window_spec.orderBy(F.col("IPL_JOB_DM_REG_score").desc())))
df_score_ranks = df_score_ranks.withColumn("IPL_TECH_DM_REG", F.ntile(99).over(window_spec.orderBy(F.col("IPL_TECH_DM_REG_score").desc())))
df_score_ranks = df_score_ranks.withColumn("ipl_care_em", F.ntile(99).over(window_spec.orderBy(F.col("ipl_care_em_score").desc())))
df_score_ranks = df_score_ranks.withColumn("ipl_job_em", F.ntile(99).over(window_spec.orderBy(F.col("ipl_job_em_score").desc())))
df_score_ranks = df_score_ranks.withColumn("ipl_tek_em", F.ntile(99).over(window_spec.orderBy(F.col("ipl_tek_em_score").desc())))
df_score_ranks = df_score_ranks.drop("dummy_partition")
df_score_ranks.write.format("delta").mode("overwrite").saveAsTable("intermed.score_ranks")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_score_ranks = spark.table("intermed.score_ranks")
df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_score_ranks.createOrReplaceTempView("score_ranks")

df_geo_appends_rpm_updated = spark.sql("""
    SELECT
        b.IPL_CARE_DM_REG + 1 AS IPL_CARE_DM_REG,
        b.IPL_JOB_DM_REG + 1 AS IPL_JOB_DM_REG,
        b.IPL_TECH_DM_REG + 1 AS IPL_TECH_DM_REG,
        b.ipl_care_em + 1 AS ipl_care_em,
        b.ipl_job_em + 1 AS ipl_job_em,
        b.ipl_tek_em + 1 AS ipl_tech_em,
        a.*
    FROM
        intermed.geo_appends_rpm AS a
    LEFT JOIN
        intermed.score_ranks AS b ON a.mid_key = b.mid_key
""")
df_geo_appends_rpm_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_web_visits = spark.table("intermed.web_visits")
df_contact_history_sum = spark.table("intermed.contact_history_sum")
df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_web_visits.createOrReplaceTempView("web_visits")
df_contact_history_sum.createOrReplaceTempView("contact_history_sum")

df_analysis3 = spark.sql("""
    SELECT
        a.*,
        b.num_clicks_renew_1mos,
        b.num_clicks_renew_1to3mos,
        b.num_clicks_renew_1wk,
        b.num_clicks_renew_3to6mos,
        c.call_freq,
        c.pct_live2,
        c.liveanswer_freq_3,
        c.liveanswer_freq3_6,
        c.num_click,
        c.num_open_6mo,
        c.click_rate_6mo,
        c.mailercount_click_6mo,
        c.mailercount_click_1_3mo,
        c.mailercount_click_30days,
        c.mailercount_click_3_6mo,
        c.mailercount_open_30days,
        c.mailercount_open_3_6mo,
        c.mailercount_open_1_3mo,
        c.num_clicked_curmonth,
        c.num_clicked_3_6mo,
        c.num_ib_1_3mo,
        c.num_ib_30days,
        c.num_ib_3_6mo,
        c.num_open_1_3mo,
        c.num_open_3_6mo,
        c.num_clicked_1_3mo,
        c.num_inb_30days,
        c.liveanswer_comp_freq_3,
        c.liveanswer_comp_freq3_6,
        c.liveanswer_comp_freq6_12,
        c.num_open_30days,
        c.mailercount_open_6mo,
        c.num_sent_curmonth,
        c.mailercount_sent_30days,
        c.mailercount_sent_180,
        c.num_clicked_past12,
        c.num_ct,
        c.num_sent_past12,
        c.mailct_past12,
        c.mailct_all,
        c.call_freq_12mo_both
    FROM
        geo_appends_rpm a
    LEFT JOIN
        web_visits b ON a.mid_key = b.mid_key
    LEFT JOIN
        contact_history_sum c ON a.mid_key = c.mid_key
""")
df_analysis3.write.option("compression", "snappy").format("delta").mode("overwrite").saveAsTable("intermed.analysis3")

df_analysis3 = spark.table("intermed.analysis3")
df_cpd_replace_post = df_analysis3
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE4_CENTILE", F.when(F.col("NEW_SCORE4_CENTILE").isNull(), 99).otherwise(F.col("NEW_SCORE4_CENTILE")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE40_CENTILE", F.when(F.col("NEW_SCORE40_CENTILE").isNull(), 99).otherwise(F.col("NEW_SCORE40_CENTILE")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("em_clickrate_c", F.when(F.col("em_clickrate").isNull(), 99).otherwise(F.col("em_clickrate")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IPL_CARE_DM_REG_c", F.coalesce(F.col("IPL_CARE_DM_REG"), F.lit(99)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("age_60to66_dum", F.when((F.col("age_agg_ind") >= 60) & (F.col("age_agg_ind") <= 66), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("age_gt75_dum", F.when(F.col("age_agg_ind") >= 75, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE46_CENTILE_c", F.coalesce(F.col("NEW_SCORE46_CENTILE"), F.lit(99)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("R_number_of_lines_of_credit", F.when(F.col("IBX_NUM_OF_LINES_OF_CREDIT").isin('1', '2', '3', '4', '7'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("VOTEPROP2016_c", F.when(F.col("general_election_vote_propensity").isNull(), 83.8246349).otherwise(F.col("general_election_vote_propensity")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ideology_c", F.when(F.col("ideology").isNull(), 44).otherwise(F.col("ideology")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("petadv12_c", F.when(F.col("petadv12") == 1, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IBX_ADULTS_NUM_AGG_HHDls3", F.when(F.col("IBX_ADULTS_NUM_AGG_HHD").isin('1', '6'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("elderly_u_dum", F.when(F.col("IBX_ELDERLY_PARENT_AGG_HHD") == 'Y', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("CENS_GRPQTRS_POP_PERCENT_MIL_c", F.when(F.col("CENS_GRPQTRS_POP_PERCENT_MILITAR").isNull(), 0.01).otherwise(F.col("CENS_GRPQTRS_POP_PERCENT_MILITAR")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("networth_1_dum", F.when(F.col("IBX_NETWORTH_PREMIER_AGG_HHD").isin('6', '7', '8', '9'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("HH_pct_Spanish_Speaking_c", F.when(F.col("CENS_LANG_HH_PERCENT_SPANISH_SPE").isNull(), 91).otherwise(F.col("CENS_LANG_HH_PERCENT_SPANISH_SPE") * 10))
df_cpd_replace_post = df_cpd_replace_post.withColumn("gender_M", F.when(F.col("gender_input") == 'M', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("cat_r_dum", F.when(F.col("category") == 'R', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("aarporg_i", F.when(F.col("aarporg_i").isNull(), 0).otherwise(F.col("aarporg_i")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("deadwood_dum", F.when(F.col("DEADWOOD_MODEL").isin('NOTDEAD'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("sy_otsbn_polfund_2012b_c", F.when(F.col("sy_otsbn_polfund_2012b").isNull(), 33).otherwise(F.col("sy_otsbn_polfund_2012b")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("SY_GUNSCORE_c", F.when(F.col("GUN_OWNERSHIP_MODEL").isNull(), 0.373).otherwise(F.col("GUN_OWNERSHIP_MODEL")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("CENS_COMMUTE_WRKRS_PERCENT_DRO_c", F.when(F.col("CENS_AGE_POP_PERCENT_45_54").isNull(), 14.1).otherwise(F.col("CENS_AGE_POP_PERCENT_45_54")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("PERCENTCAUCASIANANDOTHER_c", F.when(F.col("CENS_ETHNIC_POP_PERCENT_WHITE_ON").isNull(), 80).otherwise(F.col("CENS_ETHNIC_POP_PERCENT_WHITE_ON")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("PERCENTUNEMPLOYED_c", F.when(F.col("CENS_EMPLOY_LABF_PERCENT_UNEMPLO").isNull(), 4.2).otherwise(F.col("CENS_EMPLOY_LABF_PERCENT_UNEMPLO")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("lifestage_678", F.when(F.col("Lifestage_Segment").isin('6', '7', '8'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("lifestage_123", F.when(F.col("Lifestage_Segment").isin('1', '2', '3'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("Past12MoTouchCt_Financial2", F.when(F.col("Past12MoTouchCt_Financial").isin('1', '2', '3'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("GeneralElectn2012_AM", F.when(F.col("GeneralElectn2012").isin('A', 'M'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_click", F.when(F.col("num_click").isNull(), 0).otherwise(F.col("num_click")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("mailct_all", F.when(F.col("mailct_all").isNull(), 0).otherwise(F.col("mailct_all")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_open_6mo", F.when(F.col("num_open_6mo").isNull(), 0).otherwise(F.col("num_open_6mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("click_rate_6mo", F.when(F.col("click_rate_6mo").isNull(), 0).otherwise(F.col("click_rate_6mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("mailercount_click_6mo", F.when(F.col("mailercount_click_6mo").isNull(), 0).otherwise(F.col("mailercount_click_6mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("mailercount_open_6mo", F.when(F.col("mailercount_open_6mo").isNull(), 0).otherwise(F.col("mailercount_open_6mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("cruise_y", F.when(F.col("IBX_TRAVEL_CRUISE_AGG_HHD") == '1', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IBX_HEALTHY_BEHAVIOUR_HHD_1dum", F.when(F.col("IBX_HEALTHY_BEHAVIOUR_AGG_HHD") == '1', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("PERCENT_CIVILIAN_VET", F.when(F.col("CENS_EMPLOY_POP18_PLUS_PERCENT_C").isNull(), 8.9).otherwise(F.col("CENS_EMPLOY_POP18_PLUS_PERCENT_C")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("homevalue_9to10_dum", F.when(F.col("IBX_HOME_MARKET_VALUE_DECILES_AG").isin('09', '10'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("community_religi_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_RELIGI") == 1, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("caregiving_em_logit", 
    F.lit(-2.7296) +
    F.col("NEW_SCORE4_CENTILE") * -0.00272 +
    F.col("NEW_SCORE40_CENTILE") * -0.00169 +
    F.col("em_clickrate_c") * -0.033 +
    F.col("IPL_CARE_DM_REG_c") * -0.00317 +
    F.col("age_60to66_dum") * -0.083 +
    F.col("age_gt75_dum") * 0.3388 +
    F.col("mailct_all") * -0.00611 +
    F.col("num_click") * 0.00783 +
    F.col("num_open_6mo") * -0.00255 +
    F.col("click_rate_6mo") * 0.0103 +
    F.col("mailercount_click_6mo") * 0.1333 +
    F.col("mailercount_open_6mo") * 0.0207 +
    F.col("Past12MoTouchCt_Financial2") * 0.1177 +
    F.col("GeneralElectn2012_AM") * 0.1186 +
    F.col("NEW_SCORE46_CENTILE_c") * 0.00198 +
    F.col("R_number_of_lines_of_credit") * 0.083 +
    F.col("VOTEPROP2016_c") * -0.00244 +
    F.col("lifestage_678") * -0.2638 +
    F.col("lifestage_123") * -0.1508 +
    F.col("ideology_c") * -0.00473 +
    F.col("petadv12_c") * -0.1376 +
    F.col("IBX_ADULTS_NUM_AGG_HHDls3") * -0.1137 +
    F.col("elderly_u_dum") * 0.0968 +
    F.col("cruise_y") * -0.0727 +
    F.col("CENS_GRPQTRS_POP_PERCENT_MIL_c") * 0.0248 +
    F.col("IBX_HEALTHY_BEHAVIOUR_HHD_1dum") * 0.0921 +
    F.col("PERCENT_CIVILIAN_VET") * 0.00992 +
    F.col("networth_1_dum") * 0.0649 +
    F.col("HH_pct_Spanish_Speaking_c") * 0.000335 +
    F.col("gender_M") * -0.1611 +
    F.col("cat_r_dum") * 0.121 +
    F.col("homevalue_9to10_dum") * -0.1049 +
    F.col("aarporg_i") * -0.0776 +
    F.col("community_religi_dum") * 0.0944 +
    F.col("deadwood_dum") * -0.1236 +
    F.col("sy_otsbn_polfund_2012b_c") * 0.00204 +
    F.col("SY_GUNSCORE_c") * -0.2832 +
    F.col("CENS_COMMUTE_WRKRS_PERCENT_DRO_c") * 0.00994 +
    F.col("PERCENTCAUCASIANANDOTHER_c") * -0.00255 +
    F.col("PERCENTUNEMPLOYED_c") * -0.0142
)
df_cpd_replace_post = df_cpd_replace_post.withColumn("caregiving_em_score", F.exp(F.col("caregiving_em_logit")) / (F.lit(1) + F.exp(F.col("caregiving_em_logit"))))
df_cpd_replace_post = df_cpd_replace_post.withColumn("click_rate_3_6mos", F.when(F.col("click_rate_3_6mos").isNull(), 0).otherwise(F.col("click_rate_3_6mos")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("mailercount_click_30days", F.when(F.col("mailercount_click_30days").isNull(), 0).otherwise(F.col("mailercount_click_30days")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("mailercount_click_3_6mo", F.when(F.col("mailercount_click_3_6mo").isNull(), 0).otherwise(F.col("mailercount_click_3_6mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("mailercount_open_30days", F.when(F.col("mailercount_open_30days").isNull(), 0).otherwise(F.col("mailercount_open_30days")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("mailercount_open_3_6mo", F.when(F.col("mailercount_open_3_6mo").isNull(), 0).otherwise(F.col("mailercount_open_3_6mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("mailercount_open_1_3mo", F.when(F.col("mailercount_open_1_3mo").isNull(), 0).otherwise(F.col("mailercount_open_1_3mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_clicked_1_3mo", F.when(F.col("num_clicked_1_3mo").isNull(), 0).otherwise(F.col("num_clicked_1_3mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_clicked_curmonth", F.when(F.col("num_clicked_curmonth").isNull(), 0).otherwise(F.col("num_clicked_curmonth")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_clicked_3_6mo", F.when(F.col("num_clicked_3_6mo").isNull(), 0).otherwise(F.col("num_clicked_3_6mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_ib_1_3mo", F.when(F.col("num_ib_1_3mo").isNull(), 0).otherwise(F.col("num_ib_1_3mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_ib_30days", F.when(F.col("num_ib_30days").isNull(), 0).otherwise(F.col("num_ib_30days")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_ib_3_6mo", F.when(F.col("num_ib_3_6mo").isNull(), 0).otherwise(F.col("num_ib_3_6mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_open_1_3mo", F.when(F.col("num_open_1_3mo").isNull(), 0).otherwise(F.col("num_open_1_3mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_open_3_6mo", F.when(F.col("num_open_3_6mo").isNull(), 0).otherwise(F.col("num_open_3_6mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_open_30days", F.when(F.col("num_open_30days").isNull(), 0).otherwise(F.col("num_open_30days")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("mailercount_click_1_3mo", F.when(F.col("mailercount_click_1_3mo").isNull(), 0).otherwise(F.col("mailercount_click_1_3mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("state_event_12mo", F.when(F.col("state_event_12mo").isNull(), 0).otherwise(F.col("state_event_12mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("individual_engagers_12mo", F.when(F.col("individual_engagers_12mo").isNull(), 0).otherwise(F.col("individual_engagers_12mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("structured_12mo", F.when(F.col("structured_12mo").isNull(), 0).otherwise(F.col("structured_12mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("moviesfg_12mo", F.when(F.col("moviesfg_12mo").isNull(), 0).otherwise(F.col("moviesfg_12mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("state_event_12mo_i", F.when(F.col("state_event_12mo_i").isNull(), 0).otherwise(F.col("state_event_12mo_i")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("structured_12mo_i", F.when(F.col("structured_12mo_i").isNull(), 0).otherwise(F.col("structured_12mo_i")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("survey_resp_12mo", F.when(F.col("survey_resp_12mo").isNull(), 0).otherwise(F.col("survey_resp_12mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("contact_leg_12mo", F.when(F.col("contact_leg_12mo").isNull(), 0).otherwise(F.col("contact_leg_12mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("moviesfg_12mo_i", F.when(F.col("moviesfg_12mo_i").isNull(), 0).otherwise(F.col("moviesfg_12mo_i")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("states_vol_12mo", F.when(F.col("states_vol_12mo").isNull(), 0).otherwise(F.col("states_vol_12mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("states_vol_12mo_i", F.when(F.col("states_vol_12mo_i").isNull(), 0).otherwise(F.col("states_vol_12mo_i")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("state_activities_12mo", F.when(F.col("state_activities_12mo").isNull(), 0).otherwise(F.col("state_activities_12mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("newsletter_opens_cnt_12mo", F.when(F.col("newsletter_opens_cnt_12mo").isNull(), 0).otherwise(F.col("newsletter_opens_cnt_12mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("DENSITY_CLUSTERS_1_2_3", F.when(F.col("DENSITY_CLUSTERS").isin('1', '2', '3'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ADVO_SEGMENT_CD_S3_S4_S5", F.when(F.col("ADVO_SEGMENT_CD").isin('S3', 'S4', 'S5'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("PC_Owner_Y", F.when(F.col("ibx_pc_owner_premier") == 'Y', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("INTERNET__8_10", F.when(F.col("IBX_TRENDS_FOR_TELECOM_INTERNET_").isin('08', '10'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("INTERNET__1", F.when(F.col("IBX_TRENDS_FOR_TELECOM_INTERNET_").isin('01'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("INTERNET__2_3_4", F.when(F.col("IBX_TRENDS_FOR_TELECOM_INTERNET_").isin('02', '03', '04'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE16_CENTILE_c", F.coalesce(F.col("NEW_SCORE16_CENTILE"), F.lit(27)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE24_CENTILE", F.when(F.col("NEW_SCORE24_CENTILE").isNull(), 24).otherwise(F.col("NEW_SCORE24_CENTILE")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE25_CENTILE", F.when(F.col("NEW_SCORE25_CENTILE").isNull(), 47).otherwise(F.col("NEW_SCORE25_CENTILE")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("live_answer_aft", F.when(F.col("live_answer_aft").isNull(), 62).otherwise(F.col("live_answer_aft")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("live_answer_pm_c", F.when(F.col("live_answer_pm").isNull(), 64).otherwise(F.col("live_answer_pm")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("FNDN_Housing_c", F.coalesce(F.col("FNDN_Housing"), F.lit(42)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IPL_CARE_DM_REG_c", F.coalesce(F.col("IPL_CARE_DM_REG"), F.lit(50)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IPL_job_DM_REG_c", F.coalesce(F.col("IPL_job_DM_REG"), F.lit(41)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("em_clickrate_c", F.when(F.col("EM_Clickrate").isNull(), 50).otherwise(F.col("EM_Clickrate")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("drvsafe_pro_em_c", F.when(F.col("drvsafe_pro_em").isNull(), 36).otherwise(F.col("drvsafe_pro_em")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("fndn_pro_em_c", F.coalesce(F.col("fndn_pro_em"), F.lit(20)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("work_jobs_em_c", F.coalesce(F.col("work_jobs_em"), F.lit(17)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("GUN_OWNERSHIP_MODEL_c", F.coalesce(F.col("GUN_OWNERSHIP_MODEL"), F.lit(0.31)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("CENS_COMMUTE_WRKRS_PERCENT_P_c", F.coalesce(F.col("CENS_COMMUTE_WRKRS_PERCENT_PUBLI"), F.lit(0.6)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("CENS_MARR_POP15_PLUS_PERCENT_NEV", F.when(F.col("CENS_MARR_POP15_PLUS_PERCENT_NEV").isNull(), 28.4).otherwise(F.col("CENS_MARR_POP15_PLUS_PERCENT_NEV")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("click_rate_1_3mos", F.when((F.col("num_open_1_3mo") == 0) | F.col("num_open_1_3mo").isNull(), 0).otherwise(F.col("num_clicked_1_3mo") / F.col("num_open_1_3mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("click_rate_1_3mos", F.when(F.col("click_rate_1_3mos").isNull(), 0).otherwise(F.col("click_rate_1_3mos")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("click_rate_1mos", F.when((F.col("num_open_30days") == 0) | F.col("num_open_30days").isNull(), 0).otherwise(F.col("num_clicked_curmonth") / F.col("num_open_30days")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("click_rate_1mos", F.when(F.col("click_rate_1mos").isNull(), 0).otherwise(F.col("click_rate_1mos")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("click_rate_3_6mos", F.when((F.col("num_open_3_6mo") == 0) | F.col("num_open_3_6mo").isNull(), 0).otherwise(F.col("num_clicked_3_6mo") / F.col("num_open_3_6mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("click_rate_3_6mos", F.when(F.col("click_rate_3_6mos").isNull(), 0).otherwise(F.col("click_rate_3_6mos")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("state_activity_12mo_i", F.when(F.col("state_activity_12mo_i").isNull(), 0).otherwise(F.col("state_activity_12mo_i")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("driver_online_12mo", F.when(F.col("driver_online_12mo").isNull(), 0).otherwise(F.col("driver_online_12mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("VEHICLE_DOMINANT_C", F.when(F.col("IBX_VEHICLE_DOMINANT_AGG_HHD").isin('C', 'B'), 1).otherwise(0)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("fndnothr_score_c", F.when(F.col("fndnothr_score").isNull(), 50).otherwise(F.col("fndnothr_score")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("HOUSEHOLDS__c", F.when(F.col("CENS_COUNT_HOUSEHOLDS").isNull(), 780).otherwise(F.col("CENS_COUNT_HOUSEHOLDS")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("MEDIAN_HH_inc", F.when(F.col("CENS_INC_HH_MEDIAN_HOUSEHOLD_INC").isNull(), 68838).otherwise(F.col("CENS_INC_HH_MEDIAN_HOUSEHOLD_INC")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("EMP_EDUCATION", F.when(F.col("CENS_INDUS_EMPLD_PERCENT_EDUCATI").isNull(), 9.83).otherwise(F.col("CENS_INDUS_EMPLD_PERCENT_EDUCATI")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE16_CENTILE_c", F.coalesce(F.col("NEW_SCORE16_CENTILE"), F.lit(54)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("rpm_score_c", F.when(F.col("old_rpm_score").isNull(), 9).otherwise(F.col("old_rpm_score")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("PERCENTCAUCASIANANDOTHER_c", F.when(F.col("CENS_ETHNIC_POP_PERCENT_WHITE_ON").isNull(), 74).otherwise(F.col("CENS_ETHNIC_POP_PERCENT_WHITE_ON")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("SY_OTSBN_POLFUND_c", F.when(F.col("SY_OTSBN_POLFUND").isNull(), 13).otherwise(F.col("SY_OTSBN_POLFUND")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("commute_pct_public", F.when(F.col("CENS_COMMUTE_WRKRS_PERCENT_PUBLI").isNull(), 5.18).otherwise(F.col("CENS_COMMUTE_WRKRS_PERCENT_PUBLI")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("POPULATION_PER_SQUA", F.when(F.col("CENS_DENSITY_POPULATION_PER_SQUA").isNull(), 5352).otherwise(F.col("CENS_DENSITY_POPULATION_PER_SQUA")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ch_acq_D", F.when(F.col("ch_acq") == 'D', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("individual_engagers_12mo23", F.when(F.col("individual_engagers_12mo").isin(2, 3, 4), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("homerange_a_dum", F.when(F.col("IBX_HOME_PURCHASED_AMT_RANGES_AG") == 'A', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IPL_job_DM_REG_c", F.coalesce(F.col("IPL_job_DM_REG"), F.lit(99)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("age_50to66_em", F.when(F.col("age_agg_ind") <= 66, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("EM_Clickrate_c", F.when(F.col("EM_Clickrate").isNull(), 0).otherwise(F.col("EM_Clickrate")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ipl_TEch_dm_reg_c", F.coalesce(F.col("ipl_TEch_dm_reg"), F.lit(49)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("curterm_12_dum", F.when(F.col("cur_term") == '12', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("driver_class_12mo", F.when(F.col("driver_class_12mo").isNull(), 0).otherwise(F.col("driver_class_12mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("drvsafe_pro_em_c", F.coalesce(F.col("drvsafe_pro_em"), F.lit(50)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("Drvsafe_pro_dm", F.when(F.col("Drvsafe_pro_dm").isNull(), 50).otherwise(F.col("Drvsafe_pro_dm")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("curterm_36_dum", F.when(F.col("cur_term") == '36', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ACEV_Num12", F.when(F.col("ACEV_Num").isin('1', '2'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("UTILITY_gas", F.when(F.col("CENS_HEAT_OCCHU_PERCENT_UTILITY_").isNull(), 49).otherwise(F.col("CENS_HEAT_OCCHU_PERCENT_UTILITY_")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ch_acq_U", F.when(F.col("ch_acq") == 'U', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("driver_safety_tek_em_logit", 
    F.lit(-5.1128) +
    F.col("driver_online_12mo") * 0.4263 +
    F.col("driver_class_12mo") * 0.7436 +
    F.col("mailercount_click_6mo") * 0.1835 +
    F.col("VEHICLE_DOMINANT_C") * -0.3144 +
    F.col("IBX_HEALTHY_BEHAVIOUR_HHD_1dum") * -0.2862 +
    F.col("drvsafe_pro_em_c") * -0.00976 +
    F.col("Drvsafe_pro_dm") * -0.0174 +
    F.col("fndnothr_score_c") * 0.00412 +
    F.col("HOUSEHOLDS__c") * 0.000245 +
    F.col("MEDIAN_HH_inc") * -0.00000798 +
    F.col("EMP_EDUCATION") * -0.0224 +
    F.col("state_activity_12mo_i") * 3.1812 +
    F.col("curterm_36_dum") * -0.2954 +
    F.col("curterm_12_dum") * -0.5167 +
    F.col("NEW_SCORE16_CENTILE_c") * 0.00602 +
    F.col("ACEV_Num12") * 0.7814 +
    F.col("rpm_score_c") * 0.0649 +
    F.col("PERCENTCAUCASIANANDOTHER_c") * -0.00847 +
    F.col("SY_OTSBN_POLFUND_c") * -0.0215 +
    F.col("UTILITY_gas") * 0.00875 +
    F.col("commute_pct_public") * -0.0284 +
    F.col("POPULATION_PER_SQUA") * -0.00002 +
    F.col("ch_acq_D") * -0.2146 +
    F.col("ch_acq_U") * -0.5928 +
    F.col("individual_engagers_12mo23") * 0.2022 +
    F.col("homerange_a_dum") * 0.5078 +
    F.col("SY_GUNSCORE_c") * -0.853 +
    F.col("IPL_JOB_DM_REG") * 0.00606 +
    F.col("age_50to66_em") * -0.6945 +
    F.col("EM_Clickrate_c") * -0.0108 +
    F.col("ipl_TEch_dm_reg_c") * 0.00677
)
df_cpd_replace_post = df_cpd_replace_post.withColumn("driver_safety_tek_em_score", F.exp(F.col("driver_safety_tek_em_logit")) / (F.lit(1) + F.exp(F.col("driver_safety_tek_em_logit"))))
df_cpd_replace_post = df_cpd_replace_post.withColumn("es_region", F.when(F.col("region").isin('Central Region', 'West Region', 'South Region'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("Mega_region", F.when(F.col("region") == 'Mega Region', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("life_engage_1mo", F.when(F.col("life_engage_1mo").isNull(), 0).otherwise(F.col("life_engage_1mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("internet_1", F.when(F.col("IBX_TELECOM_INTERNET_AGG_HHD").isin('08', '09', '10'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("MemXRenew_c", F.when(F.col("MemXRenew").isNull(), 3.03).otherwise(F.col("MemXRenew")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("diversity_flag_1", F.when(F.col("diversity_flag_agg_ind") == 1, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("fndn_pro_em_c", F.coalesce(F.col("fndn_pro_em"), F.lit(50)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("CENS_COMMUTE_WRKRS_PERCENT_WOR_c", F.when(F.col("CENS_AGE_POP_PERCENT_55_59").isNull(), 7.46).otherwise(F.col("CENS_AGE_POP_PERCENT_55_59")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("household_nols18_c", F.when(F.col("CENS_CHILD_HH_PERCENT_WITHOUT_PE").isNull(), 68.6).otherwise(F.col("CENS_CHILD_HH_PERCENT_WITHOUT_PE")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("CENS_COMMUTE_COMMUTER_PERCENT_TR", F.when(F.col("CENS_COMMUTE_COMMUTER_PERCENT_TR").isNull(), 65).otherwise(F.col("CENS_COMMUTE_COMMUTER_PERCENT_TR")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("sy_otsbn_polfund_2012b_c", F.when(F.col("sy_otsbn_polfund_2012b").isNull(), 32).otherwise(F.col("sy_otsbn_polfund_2012b")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("PCT_HOME_VAL_LS10K", F.when(F.col("CENS_HOMVAL_OOHU_PERCENT_HOME_VA").isNull(), 1.08).otherwise(F.col("CENS_HOMVAL_OOHU_PERCENT_HOME_VA")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("RENT", F.when(F.col("CENS_RENT_RNTL_MEDIAN_RENT").isNull(), 893).otherwise(F.col("CENS_RENT_RNTL_MEDIAN_RENT")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("age_gt80_dum", F.when(F.col("age_agg_ind") >= 80, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE33_CENTILE_c", F.coalesce(F.col("NEW_SCORE33_CENTILE"), F.lit(56)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("advo_s23_dum", F.when(F.col("advo_segment_cd").isin('S4', 'S3'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("PERCENTSINGLEUNITDWELLINGS", F.when(F.col("cens_hustr_hu_percent_1_unit_det").isNull(), 65.2).otherwise(F.col("cens_hustr_hu_percent_1_unit_det")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("CENS_ETHNIC_POP_PERCENT_HI_c", F.when(F.col("CENS_ETHNIC_POP_PERCENT_HI_NAT_O").isNull(), 0.1173).otherwise(F.col("CENS_ETHNIC_POP_PERCENT_HI_NAT_O")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("OOHU_MEDIAN_HOME_VAL", F.when(F.col("CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL").isNull(), 261096).otherwise(F.col("CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IBX_HOME_VALUE_RANGES_BCDEF", F.when(F.col("IBX_HOME_VALUE_RANGES_AGG_HHD").isin('K', 'L', 'H', 'I', 'J'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("PERCENT_EMPLOYED", F.when(F.col("CENS_EMPLOY_LABF_PERCENT_EMPLOYE").isNull(), 94.67).otherwise(F.col("CENS_EMPLOY_LABF_PERCENT_EMPLOYE")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("PERCENT_HOH_HISPA", F.when(F.col("CENS_ETHNIC_HH_PERCENT_HOH_HISPA").isNull(), 8.13).otherwise(F.col("CENS_ETHNIC_HH_PERCENT_HOH_HISPA")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("PERSONS_PER_HH", F.when(F.col("CENS_DENSITY_PERSONS_PER_HH_FOR_").isNull(), 2.55).otherwise(F.col("CENS_DENSITY_PERSONS_PER_HH_FOR_")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("hitech_dum", F.when(F.col("hitech_merch") == '1', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("children_presence_of_household_n", F.when(F.col("ibx_children_presence_of_househo") == 'N', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("SP_SP", F.when(F.col("CENS_LANG_HH_PERCENT_SPANISH_SPE").isNull(), 7.75).otherwise(F.col("CENS_LANG_HH_PERCENT_SPANISH_SPE")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("SY_EDUCSCORE_c", F.when(F.col("EDUCATIONAL_ATTAINMENT_MODEL").isNull(), 0.36).otherwise(F.col("EDUCATIONAL_ATTAINMENT_MODEL")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("SY_HUNTERMODEL_c", F.when(F.col("HUNTER_MODEL").isNull(), 0.33).otherwise(F.col("HUNTER_MODEL")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("CENS_EARN_HH_PERCENT_WITH_SELF_E", F.when(F.col("CENS_EARN_HH_PERCENT_WITH_SELF_E").isNull(), 10.5).otherwise(F.col("CENS_EARN_HH_PERCENT_WITH_SELF_E")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("liveanswer_freq_3", F.when(F.col("liveanswer_freq_3").isNull(), 0).otherwise(F.col("liveanswer_freq_3")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("liveanswer_freq3_6", F.when(F.col("liveanswer_freq3_6").isNull(), 0).otherwise(F.col("liveanswer_freq3_6")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("mailct_past12", F.when(F.col("mailct_past12").isNull(), 0).otherwise(F.col("mailct_past12")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("call_freq_12mo_both", F.when(F.col("call_freq_12mo_both").isNull(), 0).otherwise(F.col("call_freq_12mo_both")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("logit_medicare_tth", 
    F.lit(-3.6864) +
    F.col("es_region") * 0.5753 +
    F.col("Mega_region") * -0.4269 +
    F.col("click_rate_1_3mos") * -0.3635 +
    F.col("mailercount_click_1_3mo") * 0.2334 +
    F.col("life_engage_1mo") * 0.0629 +
    F.col("internet_1") * 0.1157 +
    F.col("call_freq_12mo_both") * 0.1739 +
    F.col("liveanswer_freq_3") * 0.3441 +
    F.col("liveanswer_freq3_6") * 0.1556 +
    F.col("mailct_past12") * 0.00492 +
    F.col("IBX_HEALTHY_BEHAVIOUR_HHD_1dum") * 0.2523 +
    F.col("MemXRenew_c") * 0.0351 +
    F.col("diversity_flag_1") * 0.385 +
    F.col("fndn_pro_em_c") * 0.0042 +
    F.col("fndnothr_score_c") * -0.00445 +
    F.col("CENS_COMMUTE_WRKRS_PERCENT_DRO_c") * -0.0684 +
    F.col("CENS_COMMUTE_WRKRS_PERCENT_WOR_c") * 0.1103 +
    F.col("household_nols18_c") * -0.0179 +
    F.col("CENS_COMMUTE_COMMUTER_PERCENT_TR") * -0.00532 +
    F.col("sy_otsbn_polfund_2012b_c") * 0.00533 +
    F.col("PCT_HOME_VAL_LS10K") * 0.0246 +
    F.col("RENT") * -0.00015 +
    F.col("age_gt80_dum") * -0.2461 +
    F.col("NEW_SCORE33_CENTILE_c") * 0.00303 +
    F.col("advo_s23_dum") * 0.3744 +
    F.col("PERCENTSINGLEUNITDWELLINGS") * 0.00314 +
    F.col("CENS_ETHNIC_POP_PERCENT_HI_c") * -0.4179 +
    F.col("OOHU_MEDIAN_HOME_VAL") * -0.00000051 +
    F.col("IBX_HOME_VALUE_RANGES_BCDEF") * -0.2283 +
    F.col("PERCENT_EMPLOYED") * 0.0359 +
    F.col("PERCENT_HOH_HISPA") * 0.034 +
    F.col("UTILITY_gas") * 0.00661 +
    F.col("PERSONS_PER_HH") * -0.8512 +
    F.col("hitech_dum") * 0.1017 +
    F.col("children_presence_of_household_n") * 0.2119 +
    F.col("SP_SP") * -0.018 +
    F.col("SY_EDUCSCORE_c") * 0.8212 +
    F.col("SY_GUNSCORE_c") * 0.5562 +
    F.col("SY_HUNTERMODEL_c") * -0.2156 +
    F.col("IPL_JOB_DM_REG_c") * -0.00351 +
    F.col("CENS_EARN_HH_PERCENT_WITH_SELF_E") * 0.0116
)
df_cpd_replace_post = df_cpd_replace_post.withColumn("score_medicare_tth", F.exp(F.col("logit_medicare_tth")) / (F.lit(1) + F.exp(F.col("logit_medicare_tth"))))
df_cpd_replace_post = df_cpd_replace_post.withColumn("homevalue_1to2_dum", F.when(F.col("IBX_HOME_MARKET_VALUE_DECILES_AG").isin('01', '02', '03', '04'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("VEHICLE_DOMINANT_C", F.when(F.col("IBX_VEHICLE_DOMINANT_AGG_HHD").isin('D', 'B'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("emailable_dum", F.when(F.col("EMAILABLE_AGG_IND") == 'N', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("age_56to59_dum", F.when((F.col("age_agg_ind") >= 56) & (F.col("age_agg_ind") <= 59), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("age_60to65_dum", F.when((F.col("age_agg_ind") >= 60) & (F.col("age_agg_ind") <= 65), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE8_CENTILE", F.when(F.col("NEW_SCORE8_CENTILE").isNull(), 99).otherwise(F.col("NEW_SCORE8_CENTILE")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE39_CENTILE", F.when(F.col("NEW_SCORE39_CENTILE").isNull(), 99).otherwise(F.col("NEW_SCORE39_CENTILE")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IBX_CREDIT_CARD_FREQ_24", F.when(F.col("IBX_CREDIT_CARD_FREQ_24_P_AGG_HH").isin('1', '3', '2'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("workcluster_wealthy", F.when(F.col("WORK_CLUSTERS").isin('1', '2', '3', '11', '12', 'z'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("activist_ic", F.when(F.col("activist_i") == 1, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("live_answer_pm", F.when(F.col("live_answer_pm").isNull(), 99).otherwise(F.col("live_answer_pm")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("state_activities_12moc", F.when(F.col("state_activities_12mo") == 0, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("work_jobs_em_c", F.coalesce(F.col("work_jobs_em"), F.lit(99)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("sy_otsbn_polfund_2012a_c", F.when(F.col("sy_otsbn_polfund_2012a").isNull(), 51).otherwise(F.col("sy_otsbn_polfund_2012a")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("soc_sec_em_att_logit", 
    F.lit(-5.6728) +
    F.col("mailercount_click_1_3mo") * 0.5035 +
    F.col("mailercount_open_1_3mo") * 0.0513 +
    F.col("homevalue_1to2_dum") * -0.1659 +
    F.col("VEHICLE_DOMINANT_C") * -0.2912 +
    F.col("live_answer_pm") * 0.00569 +
    F.col("emailable_dum") * 1.2366 +
    F.col("MemXRenew_c") * -0.068 +
    F.col("drvsafe_pro_em_c") * -0.00805 +
    F.col("sy_otsbn_polfund_2012a_c") * -0.00966 +
    F.col("age_56to59_dum") * 0.4422 +
    F.col("age_60to65_dum") * 1.2564 +
    F.col("NEW_SCORE8_CENTILE") * -0.00639 +
    F.col("NEW_SCORE39_CENTILE") * 0.00638 +
    F.col("advo_s23_dum") * 0.266 +
    F.col("ACEV_Num12") * 0.2792 +
    F.col("IBX_CREDIT_CARD_FREQ_24") * 0.1498 +
    F.col("workcluster_wealthy") * -0.2765 +
    F.col("ch_acq_D") * -0.2065 +
    F.col("ch_acq_U") * -0.3043 +
    F.col("aarporg_i") * 0.5036 +
    F.col("state_activities_12moc") * -0.5479 +
    F.col("activist_ic") * 0.3196 +
    F.col("work_jobs_em_c") * -0.0332 +
    F.col("EM_Clickrate_c") * -0.0115
)
df_cpd_replace_post = df_cpd_replace_post.withColumn("soc_sec_em_att_score", F.exp(F.col("soc_sec_em_att_logit")) / (F.lit(1) + F.exp(F.col("soc_sec_em_att_logit"))))
df_cpd_replace_post = df_cpd_replace_post.withColumn("TELECOM_CALLING_SERVICES_AGG2345", F.when(F.col("IBX_TELECOM_CALLING_SERVICES_AGG").isin('04', '02', '03', '05'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("live_answer_am", F.when(F.col("live_answer_am").isNull(), 99).otherwise(F.col("live_answer_am")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("partisanscore_c", F.when(F.col("partisanscore").isNull(), 54).otherwise(F.col("partisanscore")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("newsletter_opens_cnt_12mo_c", F.when(F.col("newsletter_opens_cnt_12mo").isNull(), 0).otherwise(F.col("newsletter_opens_cnt_12mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("age_gt65", F.when(F.col("age_agg_ind") > 65, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("advo_dm_65", F.when(F.col("advo_dm_65plus").isin('1', '2', '3', '4', '5', '6', '7', '8', '9', '10'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("PERCENT_BLACK", F.when(F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON").isNull(), 10.5).otherwise(F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("suppression_c", F.when(F.col("suppression") == 0, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("Globally_Opted_In_c", F.when(F.col("Globally_Opted_In") == '1', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("networth_13_dum", F.when(F.col("IBX_NETWORTH_PREMIER_AGG_HHD").isin('1', '2', '3'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("mail_health_dum", F.when(F.col("IBX_MAIL_BUYER_CAT_HEALTH_AGG_HH") == '1', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("soc_sec_em_logit", 
    F.lit(-3.75) +
    F.col("mailercount_click_1_3mo") * 0.2679 +
    F.col("mailercount_click_30days") * 0.2186 +
    F.col("mailercount_click_3_6mo") * 0.0633 +
    F.col("mailercount_open_1_3mo") * 0.0264 +
    F.col("mailercount_open_30days") * 0.0668 +
    F.col("mailercount_open_3_6mo") * -0.0144 +
    F.col("num_clicked_1_3mo") * 0.0168 +
    F.col("num_clicked_curmonth") * 0.00986 +
    F.col("networth_13_dum") * 0.1085 +
    F.col("mail_health_dum") * -0.1068 +
    F.col("TELECOM_CALLING_SERVICES_AGG2345") * 0.0547 +
    F.col("live_answer_am") * -0.00194 +
    F.col("emailable_dum") * 0.5611 +
    F.col("MemXRenew_c") * -0.0263 +
    F.col("partisanscore_c") * -0.0009 +
    F.col("newsletter_opens_cnt_12mo_c") * -0.00454 +
    F.col("suppression_c") * -0.3012 +
    F.col("age_56to59_dum") * 0.5306 +
    F.col("age_60to65_dum") * 0.8663 +
    F.col("age_gt65") * 0.5479 +
    F.col("NEW_SCORE8_CENTILE") * -0.00233 +
    F.col("NEW_SCORE39_CENTILE") * 0.00354 +
    F.col("IPL_JOB_DM_REG_c") * -0.00123 +
    F.col("Globally_Opted_In_c") * -0.2171 +
    F.col("advo_dm_65") * -0.2095 +
    F.col("advo_s23_dum") * 0.1029 +
    F.col("PERCENT_BLACK") * -0.00217 +
    F.col("PERSONS_PER_HH") * 0.1031 +
    F.col("ch_acq_D") * -0.0701 +
    F.col("ch_acq_U") * -0.2354 +
    F.col("activist_ic") * 0.1422 +
    F.col("work_jobs_em_c") * -0.0127 +
    F.col("EM_Clickrate_c") * -0.0153
)
df_cpd_replace_post = df_cpd_replace_post.withColumn("soc_sec_em_score", F.exp(F.col("soc_sec_em_logit")) / (F.lit(1) + F.exp(F.col("soc_sec_em_logit"))))
df_cpd_replace_post = df_cpd_replace_post.withColumn("FNDN_AARPPRO_DM_score_c", F.coalesce(F.col("FNDN_AARPPRO_DM_score"), F.lit(0.0104543)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("donadv12", F.when(F.col("donadv12").isNull(), 0).otherwise(F.col("donadv12")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("foundation_donors_12mo_i", F.when(F.col("foundation_donors_12mo_i").isNull(), 0).otherwise(F.col("foundation_donors_12mo_i")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("donfnd12", F.when(F.col("donfnd12").isNull(), 0).otherwise(F.col("donfnd12")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("foundation_donations_12mo", F.when(F.col("foundation_donations_12mo").isNull(), 0).otherwise(F.col("foundation_donations_12mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("FNDN_AARPPRO_DM", F.when(F.col("FNDN_AARPPRO_DM").isNull(), 42.0453284).otherwise(F.col("FNDN_AARPPRO_DM")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("advo_dm_50_64_c", F.coalesce(F.col("advo_dm_50_64"), F.lit(46.8673492)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("petadv12", F.when(F.col("petadv12").isNull(), 0).otherwise(F.col("petadv12")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("advo_pro_em_c", F.coalesce(F.col("advo_pro_em"), F.lit(42.0201349)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("advocacy_signers_12mo_i", F.when(F.col("advocacy_signers_12mo_i").isNull(), 0).otherwise(F.col("advocacy_signers_12mo_i")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IPL_CARE_DM_REG_c", F.coalesce(F.col("IPL_CARE_DM_REG"), F.lit(49.8608415)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("nps_detractor_c", F.coalesce(F.col("nps_detractor"), F.lit(51.2784518)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("advocacy_petitions_12mo", F.coalesce(F.col("advocacy_petitions_12mo"), F.lit(0)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("advo_dm_65plus_30_86", F.when(F.col("advo_dm_65plus").isin(30, 35, 39, 44, 45, 49, 50, 54, 55, 58, 72, 74, 86), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("advo_dm_65plus_13_96", F.when(F.col("advo_dm_65plus").isin(13, 19, 28, 29, 34, 95, 96), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("advo_dm_65plus_9_88", F.when(F.col("advo_dm_65plus").isin(9, 17, 20, 22, 31, 47, 51, 52, 61, 62, 63, 73, 76, 78, 79, 80, 81, 85, 88), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("sy_otsbn_polfund_2012b_c", F.coalesce(F.col("sy_otsbn_polfund_2012b"), F.lit(41.9595068)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE43_CENTILE", F.when(F.col("NEW_SCORE43_CENTILE").isNull(), 50.9320392).otherwise(F.col("NEW_SCORE43_CENTILE")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ibx_adult_age_65_74_agg_hhd1", F.when(F.col("ibx_adult_age_65_74_agg_hhd").isNull() | (F.col("ibx_adult_age_65_74_agg_hhd") == '0'), 0).otherwise(1))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE26_CENTILE_c", F.coalesce(F.col("NEW_SCORE26_CENTILE"), F.lit(48.5192286)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("WorkNSaveINT_PH", F.when(F.col("WorkNSaveINT_PH").isNull(), 46.3975126).otherwise(F.col("WorkNSaveINT_PH")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ibx_age_24i_86f", F.when(F.col("ibx_age_input_individual_default").isin('24I', '28I', '30F', '32I', '48F', '50F', '52F', '54F', '58I', '66F', '70F', '80F', '82F', '82I', '86F'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE38_CENTILE", F.when(F.col("NEW_SCORE38_CENTILE").isNull(), 60.4508748).otherwise(F.col("NEW_SCORE38_CENTILE")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE42_CENTILE_c", F.coalesce(F.col("NEW_SCORE42_CENTILE"), F.lit(58.7826992)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("AGE_AGG_IND_77_87", F.when((F.col("age_agg_ind") >= 77) & (F.col("age_agg_ind") <= 87), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("AGE_AGG_IND_64_76", F.when((F.col("age_agg_ind") >= 64) & (F.col("age_agg_ind") <= 76), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE11_CENTILE", F.when(F.col("NEW_SCORE11_CENTILE").isNull(), 44.8839675).otherwise(F.col("NEW_SCORE11_CENTILE")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE1_CENTILE_c", F.coalesce(F.col("NEW_SCORE1_CENTILE"), F.lit(47.4579505)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("SY_OTSBN_POLFUND", F.when(F.col("SY_OTSBN_POLFUND").isNull(), 10.328614).otherwise(F.col("SY_OTSBN_POLFUND")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ipl_JOB_dm_reg_c", F.coalesce(F.col("ipl_JOB_dm_reg"), F.lit(55.2279503)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("kids_shows_c", F.coalesce(F.col("kids_shows"), F.lit(34.9413625)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ACA_TTH", F.when(F.col("ACA_TTH").isNull(), 45.2265512).otherwise(F.col("ACA_TTH")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("Fraudwatch_lo", F.when(F.col("Fraudwatch_lo").isNull(), 37.1784098).otherwise(F.col("Fraudwatch_lo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("gun_ownership_model", F.when(F.col("gun_ownership_model").isNull(), 0.345742).otherwise(F.col("gun_ownership_model")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("rx_65_plus_logit", 
    F.col("FNDN_AARPPRO_DM_score_c") * 8.60643042786294 +
    F.col("donadv12") * 1.09855654759559 +
    F.col("foundation_donors_12mo_i") * 0.823115861378214 +
    F.col("donfnd12") * -0.382168461925332 +
    F.col("foundation_donations_12mo") * 0.131200029987364 +
    F.col("FNDN_AARPPRO_DM") * -0.0069342546062161 +
    F.col("advo_dm_50_64_c") * -0.00466104201264523 +
    F.col("petadv12") * 0.310065912872416 +
    F.col("advo_pro_em_c") * -0.00243171745801739 +
    F.col("individual_engagers_12mo") * 0.0579981741099003 +
    F.col("advocacy_signers_12mo_i") * -0.0809102242933807 +
    F.col("IPL_CARE_DM_REG_c") * -0.00563494944931193 +
    F.col("nps_detractor_c") * -0.00388164050181566 +
    F.col("advocacy_petitions_12mo") * -0.0407948215744115 +
    F.col("advo_dm_65plus_30_86") * -1.23816708959498 +
    F.col("advo_dm_65plus_13_96") * 0.110258751108585 +
    F.col("sy_otsbn_polfund_2012b_c") * -0.00417355253414476 +
    F.col("NEW_SCORE43_CENTILE") * -0.00300127324548748 +
    F.col("ibx_adult_age_65_74_agg_hhd1") * -0.0013921301236746 +
    F.col("NEW_SCORE26_CENTILE_c") * -0.00723338315159471 +
    F.col("WorkNSaveINT_PH") * 0.00014747606005109 +
    F.col("ibx_age_24i_86f") * 0.415535778258912 +
    F.col("NEW_SCORE38_CENTILE") * -0.00436063902530226 +
    F.col("NEW_SCORE42_CENTILE_c") * -0.00525948210781949 +
    F.col("AGE_AGG_IND_77_87") * 0.231976603700301 +
    F.col("advo_dm_65plus_9_88") * -0.530777485692503 +
    F.col("NEW_SCORE11_CENTILE") * -0.008601871555848 +
    F.col("NEW_SCORE1_CENTILE_c") * -0.00119581347169316 +
    F.col("sy_otsbn_polfund") * 0.00239784029610294 +
    F.col("AGE_AGG_IND_64_76") * 0.337770186308619 +
    F.col("ipl_JOB_dm_reg_c") * -0.000890118070254565 +
    F.col("kids_shows_c") * -0.0301649272827363 +
    F.col("ACA_TTH") * 0.00044071919631025 +
    F.col("Fraudwatch_lo") * -0.00265875189270062 +
    F.col("gun_ownership_model") * -0.900914856873976
)
df_cpd_replace_post = df_cpd_replace_post.withColumn("rx_65_plus_score", F.exp(F.col("rx_65_plus_logit")) / (F.lit(1) + F.exp(F.col("rx_65_plus_logit"))))
df_cpd_replace_post = df_cpd_replace_post.withColumn("advo_dm_65plus_2_95", F.when(F.col("advo_dm_65plus").isin(2, 3, 4, 5, 6, 7, 8, 11, 12, 13, 14, 18, 19, 22, 27, 32, 34, 35, 42, 49, 50, 95), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("advo_dm_65plus_1_96", F.when(F.col("advo_dm_65plus").isin(1, 37, 39, 40, 46, 47, 52, 54, 58, 60, 64, 65, 67, 68, 69, 70, 74, 75, 77, 80, 81, 82, 85, 91, 94, 96), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("FNDN_AARPPRO_DM_score_c", F.coalesce(F.col("FNDN_AARPPRO_DM_score"), F.lit(0.0077011)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("advo_dm_50_64_c", F.coalesce(F.col("advo_dm_50_64"), F.lit(71.0355303)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("advo_pro_em_c", F.coalesce(F.col("advo_pro_em"), F.lit(76.1808953)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("sy_otsbn_polfund_2012a_c", F.coalesce(F.col("sy_otsbn_polfund_2012a"), F.lit(30.4523383)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE46_CENTILE", F.when(F.col("NEW_SCORE46_CENTILE").isNull(), 31.434098).otherwise(F.col("NEW_SCORE46_CENTILE")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("MemXRenew_c", F.coalesce(F.col("MemXRenew"), F.lit(2.1139477)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE42_CENTILE_c", F.coalesce(F.col("NEW_SCORE42_CENTILE"), F.lit(35.025787)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("kids_shows_c", F.coalesce(F.col("kids_shows"), F.lit(49.1274631)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE34_CENTILE", F.when(F.col("NEW_SCORE34_CENTILE").isNull(), 24.2888764).otherwise(F.col("NEW_SCORE34_CENTILE")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ibx_age_in_two_year_1899", F.when(F.col("ibx_age_in_two_year_increments_1").isin(18, 20, 22, 32, 36, 44, 46, 48, 50, 82, 86, 88, 90, 92, 94, 96, 98, 99), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ibx_age_in_two_year_2484", F.when(F.col("ibx_age_in_two_year_increments_1").isin(24, 30, 40, 64, 66, 72, 74, 76, 78, 80, 84), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_months_c", F.coalesce(F.col("num_months"), F.lit(69.7592793)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("contact_leg_12mo_i", F.when(F.col("contact_leg_12mo_i").isNull(), 0).otherwise(F.col("contact_leg_12mo_i")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE1_CENTILE_c", F.coalesce(F.col("NEW_SCORE1_CENTILE"), F.lit(64.8280755)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE22_CENTILE_c", F.coalesce(F.col("NEW_SCORE22_CENTILE"), F.lit(32.7084533)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ibx_personic_cluster_2_64", F.when(F.col("ibx_personic_cluster").isin(2, 3, 12, 18, 29, 32, 45, 46, 50, 51, 53, 54, 57, 61, 64), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("rx_5064_logit", 
    F.col("advo_dm_65plus_2_95") * 0.720209998586087 +
    F.col("FNDN_AARPPRO_DM_score_c") * 39.7572783554454 +
    F.col("donadv12") * 1.19445885852835 +
    F.col("advo_dm_65plus_1_96") * -1.48969918180243 +
    F.col("advo_dm_50_64_c") * 0.00178094792256697 +
    F.col("advo_pro_em_c") * 0.00293029602852905 +
    F.col("sy_otsbn_polfund_2012a_c") * 0.00129398869762144 +
    F.col("contact_leg_12mo") * 0.321144986201138 +
    F.col("NEW_SCORE46_CENTILE") * -0.00239348251266538 +
    F.col("memxrenew_c") * 0.0664283039042919 +
    F.col("ch_acq_U") * -0.0894332858113645 +
    F.col("NEW_SCORE42_CENTILE_c") * -0.00233555321370643 +
    F.col("kids_shows_c") * -0.0854633177400143 +
    F.col("NEW_SCORE34_CENTILE") * -0.00443244592222334 +
    F.col("ibx_age_in_two_year_1899") * -1.6986322153099 +
    F.col("num_months_c") * 0.000892457794721769 +
    F.col("donfnd12") * 4.88907050032271 +
    F.col("contact_leg_12mo_i") * 0.823703947110163 +
    F.col("NEW_SCORE1_CENTILE_c") * -0.0134339380757344 +
    F.col("NEW_SCORE22_CENTILE_c") * -0.0020768951527242 +
    F.col("ibx_personic_cluster_2_64") * 0.791635254588673 +
    F.col("foundation_donors_12mo_i") * -4.33254981756511 +
    F.col("ibx_age_in_two_year_2484") * 0.349537009311488
)
df_cpd_replace_post = df_cpd_replace_post.withColumn("rx_5064_score", F.exp(F.col("rx_5064_logit")) / (F.lit(1) + F.exp(F.col("rx_5064_logit"))))
df_cpd_replace_post = df_cpd_replace_post.withColumn("mempaiddate2", F.last_day(F.to_date(F.col("mempaiddate").cast(StringType()), "yyyyMMdd")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("weeks_active", F.datediff(F.col("mempaiddate2"), F.current_date()) / 7)
df_cpd_replace_post = df_cpd_replace_post.withColumn("weeks_active", F.when(F.col("weeks_active") > 1000, 1001).otherwise(F.col("weeks_active")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("rest_rr", 
    F.when(F.col("weeks_active") >= -41, F.col("weeks_active")) 
    .otherwise(0) 
)
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_clicks_renew_1mos", F.when(F.col("num_clicks_renew_1mos").isNull(), 0).otherwise(F.col("num_clicks_renew_1mos")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_clicks_renew_1to3mos", F.when(F.col("num_clicks_renew_1to3mos").isNull(), 0).otherwise(F.col("num_clicks_renew_1to3mos")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_clicks_renew_1wk", F.when(F.col("num_clicks_renew_1wk").isNull(), 0).otherwise(F.col("num_clicks_renew_1wk")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_clicks_renew_3to6mos", F.when(F.col("num_clicks_renew_3to6mos").isNull(), 0).otherwise(F.col("num_clicks_renew_3to6mos")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("AGE_50_60", F.when((F.col("age_agg_ind") >= 50) & (F.col("age_agg_ind") <= 63), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("AGE_60_80", F.when((F.col("age_agg_ind") >= 64) & (F.col("age_agg_ind") <= 69), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("AGE_80_90", F.when((F.col("age_agg_ind") > 70) & (F.col("age_agg_ind") <= 85), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ibx_age_in_two_year_7084", F.when((F.col("ibx_age_in_two_year_increments_1") >= 70) & (F.col("ibx_age_in_two_year_increments_1") <= 84), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("lifestage_789", F.when(F.col("Lifestage_Segment").isin('7', '8'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE17_CENTILE", F.when(F.col("NEW_SCORE17_CENTILE").isNull(), 50).otherwise(F.col("NEW_SCORE17_CENTILE")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE35_CENTILE", F.when(F.col("NEW_SCORE35_CENTILE").isNull(), 50).otherwise(F.col("NEW_SCORE35_CENTILE")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE37_CENTILE", F.when(F.col("NEW_SCORE37_CENTILE").isNull(), 50).otherwise(F.col("NEW_SCORE37_CENTILE")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NEW_SCORE33_CENTILE", F.when(F.col("NEW_SCORE33_CENTILE").isNull(), 50).otherwise(F.col("NEW_SCORE33_CENTILE")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IPL_CARE_DM_REG", F.when(F.col("IPL_CARE_DM_REG").isNull(), 50).otherwise(F.col("IPL_CARE_DM_REG")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ipl_TEch_dm_reg", F.when(F.col("ipl_TEch_dm_reg").isNull(), 50).otherwise(F.col("ipl_TEch_dm_reg")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("NPS_DETRACTOR_c", F.coalesce(F.col("NPS_DETRACTOR"), F.lit(50)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("advo_dm_50_64_c", F.coalesce(F.col("advo_dm_50_64"), F.lit(50)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("order_num", F.when(F.col("order_num").isNull(), 0).otherwise(F.col("order_num")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("advo_s34_dum", F.when(F.col("advo_segment_cd").isin('S2', 'S3', 'S4', 'S5'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("advo_s0_dum", F.when(F.col("advo_segment_cd") == 'S0', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("order_term_12", F.when(F.col("order_term") == 12, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("order_term_36", F.when(F.col("order_term") == 36, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("order_term_60", F.when(F.col("order_term") == 60, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("sy_dense", F.when(F.col("DENSITY_CLUSTERS").isin('1', '2', '3'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("sy_dense789", F.when(F.col("DENSITY_CLUSTERS").isin('7', '8', '9'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("CHILD_PRESENCE_dum", F.when(F.col("IBX_CHILD_PRESENCE_AGG_HHD") == 'N', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("dwelling_m_dum", F.when(F.col("IBX_DWELLING_TYPE_AGG_HHD") == 'M', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("grandchildren_dum", F.when(F.col("IBX_GRAND_CHILDREN_AGG_HHD") == 'Y', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ibx_home_equity_HN", F.when(F.col("ibx_home_equity_available_agg_hh").isin('H', 'I', 'J', 'K', 'L', 'M', 'N'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ibx_home_lender_B", F.when(F.col("ibx_home_lender_type_1") == 'P', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("property_type_b", F.when(F.col("ibx_home_property_type_details") == 'B', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("R1_IBX_HOUSEHOLD_INCOME1", F.when(F.col("IBX_HOUSEHOLD_INCOME").isin('1', '2'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("invest_x", F.when(F.col("ibx_investment_agg_hhd") == 'X', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("networth_A9_dum", F.when(F.col("IBX_NETWORTH_PREMIER_AGG_HHD").isin('A', 'B', '9'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("job_12", F.when(F.col("ibx_occupation_1st_individual_pr").isin('2', '1'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("race_abi", F.when(F.col("ibx_race_cd_input_individual_pre").isin('A', 'B', 'I'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("vehicle_cf", F.when(F.col("ibx_vehicle_dominant_lifestyle_p").isin('C', 'F'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("voter_status_dum", F.when(F.col("voterstatus").isin('dropped', 'inactive', 'multipleAppearances'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("order_type_B", F.when(F.col("order_type") == 'B', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("workcluster_wealthy", F.when(F.col("WORK_CLUSTERS").isin('1', '2', '3'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("vetera", F.when(F.col("ibx_community_involvement_vetera") == 1, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("libera", F.when(F.col("ibx_community_involvement_libera") == 1, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("education_3", F.when(F.col("ibx_education_1st_individual") == 3, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("CELL9_10", F.when(F.col("IBX_TELECOM_CELLULAR_AGG_HHD").isin(1, 2), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("internet_1", F.when(F.col("IBX_TELECOM_INTERNET_AGG_HHD").isin(1, 2, 3), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ibx_trends_for_telecom_8910", F.when(F.col("ibx_trends_for_telecom_optional_").isin('08', '09', '10'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IBX_ADULTS_NUM_AGG_HHDls3", F.when(F.col("IBX_ADULTS_NUM_AGG_HHD").isin('1', '2', '3'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("internet_c", F.coalesce(F.col("internet"), F.lit(62.8374723)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("radio_c", F.coalesce(F.col("radio"), F.lit(38.6685073)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("smartphone", F.when(F.col("smartphone").isNull(), 18.4067391).otherwise(F.col("smartphone")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("sy_otsbn_polfund_2012a_c", F.coalesce(F.col("sy_otsbn_polfund_2012a"), F.lit(69.0285316)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("sy_otsbn_polfund_2012b_c", F.coalesce(F.col("sy_otsbn_polfund_2012b"), F.lit(43.163444)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("life_engage_sumamt_12mo", F.when(F.col("life_engage_sumamt_12mo").isNull(), 0).otherwise(F.col("life_engage_sumamt_12mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("life_engage_svcprov_12mo", F.when(F.col("life_engage_svcprov_12mo").isNull(), 0).otherwise(F.col("life_engage_svcprov_12mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("life_engage_svcprov_3mo", F.when(F.col("life_engage_svcprov_3mo").isNull(), 0).otherwise(F.col("life_engage_svcprov_3mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("life_engage_6mo", F.when(F.col("life_engage_6mo").isNull(), 0).otherwise(F.col("life_engage_6mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("pct_live2", F.when(F.col("pct_live2").isNull(), 0).otherwise(F.col("pct_live2")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("liveanswer_comp_freq_3", F.when(F.col("liveanswer_comp_freq_3").isNull(), 0).otherwise(F.col("liveanswer_comp_freq_3")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("liveanswer_comp_freq3_6", F.when(F.col("liveanswer_comp_freq3_6").isNull(), 0).otherwise(F.col("liveanswer_comp_freq3_6")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("liveanswer_comp_freq6_12", F.when(F.col("liveanswer_comp_freq6_12").isNull(), 0).otherwise(F.col("liveanswer_comp_freq6_12")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("call_freq", F.when(F.col("call_freq").isNull(), 0).otherwise(F.col("call_freq")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("rpm_logit", 
    F.lit(-4.0719) +
    F.col("rest_rr") * 4.8776 +
    F.col("num_clicks_renew_1mos") * 0.4416 +
    F.col("num_clicks_renew_1to3mos") * -0.0608 +
    F.col("num_clicks_renew_1wk") * 0.3939 +
    F.col("num_clicks_renew_3to6mos") * -0.0549 +
    F.col("AGE_50_60") * 0.2165 +
    F.col("lifestage_789") * 0.0971 +
    F.col("NEW_SCORE37_CENTILE") * 0.0154 +
    F.col("NEW_SCORE33_CENTILE") * -0.0123 +
    F.col("NEW_SCORE17_CENTILE") * -0.00507 +
    F.col("NEW_SCORE35_CENTILE") * 0.00751 +
    F.col("IPL_CARE_DM_REG") * -0.0167 +
    F.col("ipl_TEch_dm_reg") * 0.0109 +
    F.col("nps_detractor_c") * 0.00786 +
    F.col("advo_dm_50_64_c") * -0.00801 +
    F.col("order_num") * 0.0701 +
    F.col("advo_s34_dum") * 0.4371 +
    F.col("advo_s0_dum") * -0.2009 +
    F.col("order_term_12") * -0.4439 +
    F.col("order_term_36") * -0.635 +
    F.col("order_term_60") * -1.2559 +
    F.col("sy_dense") * -0.1202 +
    F.col("sy_dense789") * 0.1295 +
    F.col("CHILD_PRESENCE_dum") * 0.0655 +
    F.col("dwelling_m_dum") * -0.0974 +
    F.col("elderly_u_dum") * 0.3175 +
    F.col("grandchildren_dum") * -0.0572 +
    F.col("ibx_home_equity_HN") * 0.1542 +
    F.col("ibx_home_lender_B") * 0.1259 +
    F.col("property_type_b") * 0.077 +
    F.col("R1_IBX_HOUSEHOLD_INCOME1") * -0.0918 +
    F.col("invest_x") * -0.0869 +
    F.col("networth_A9_dum") * 0.1806 +
    F.col("networth_13_dum") * -0.2172 +
    F.col("job_12") * 0.1387 +
    F.col("race_abi") * -0.4124 +
    F.col("vehicle_cf") * 0.1037 +
    F.col("voter_status_dum") * -0.9454 +
    F.col("order_type_B") * -0.4423 +
    F.col("workcluster_wealthy") * 0.1321 +
    F.col("AGE_60_80") * 0.5036 +
    F.col("AGE_80_90") * 0.2749 +
    F.col("ibx_age_in_two_year_7084") * 0.099 +
    F.col("vetera") * -0.1309 +
    F.col("libera") * -0.2427 +
    F.col("education_3") * 0.0585 +
    F.col("CELL9_10") * -0.1066 +
    F.col("internet_1") * 0.059 +
    F.col("ibx_trends_for_telecom_8910") * -0.1315 +
    F.col("IBX_ADULTS_NUM_AGG_HHDls3") * 0.071 +
    F.col("internet_c") * 0.00507 +
    F.col("radio_c") * 0.0088 +
    F.col("smartphone") * -0.00799 +
    F.col("sy_otsbn_polfund_2012a_c") * 0.00233 +
    F.col("sy_otsbn_polfund_2012b_c") * -0.00476 +
    F.col("foundation_donations_12mo") * 0.101 +
    F.col("individual_engagers_12mo") * 0.2358 +
    F.col("life_engage_sumamt_12mo") * 0.000035 +
    F.col("life_engage_svcprov_12mo") * 0.5056 +
    F.col("life_engage_svcprov_3mo") * 0.189 +
    F.col("life_engage_6mo") * -0.004 +
    F.col("state_activities_12mo") * -0.0722 +
    F.col("click_rate_1_3mos") * 0.0742 +
    F.col("click_rate_1mos") * 0.0736 +
    F.col("click_rate_3_6mos") * 0.0292 +
    F.col("num_clicked_curmonth") * 0.0316 +
    F.col("num_inb_30days") * 0.0234 +
    F.col("num_ib_3_6mo") * 0.00489 +
    F.col("num_open_1_3mo") * -0.0107 +
    F.col("call_freq") * 0.1589 +
    F.col("pct_live2") * 0.2843 +
    F.col("liveanswer_comp_freq_3") * -0.1455 +
    F.col("liveanswer_comp_freq3_6") * -0.1422 +
    F.col("liveanswer_comp_freq6_12") * -0.1493
)
df_cpd_replace_post = df_cpd_replace_post.withColumn("rpm_score", F.exp(F.col("rpm_logit")) / (F.lit(1) + F.exp(F.col("rpm_logit"))))
df_cpd_replace_post = df_cpd_replace_post.withColumn("rpm_score", F.when(F.col("rpm_score").isNull(), 0.999999).otherwise(F.col("rpm_score")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("new_kx_create_dt_date", F.to_date(F.col("new_kx_create_dt")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("curr_order_create_dt_date", F.to_date(F.col("curr_order_create_dt"), "yyyyMMdd"))
df_cpd_replace_post = df_cpd_replace_post.withColumn("rpm_score", F.when((F.col("new_kx_create_dt").isNotNull()) & (F.col("new_kx_create_dt_date") >= F.col("curr_order_create_dt_date")), 1).otherwise(F.col("rpm_score")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("orders_all_c", F.when(F.col("orders_all").isNull(), 0).otherwise(F.col("orders_all")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("orders_12moterm_c", F.when(F.col("orders_12moterm").isNull(), 0).otherwise(F.col("orders_12moterm")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("orders_60moterm_c", F.when(F.col("orders_60moterm").isNull(), 0).otherwise(F.col("orders_60moterm")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("orders_renewals_c", F.when(F.col("orders_renewals").isNull(), 0).otherwise(F.col("orders_renewals")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("orders_online_c", F.when(F.col("orders_online").isNull(), 0).otherwise(F.col("orders_online")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("dvr_c", F.when(F.col("dvr").isNull(), 23.1282804).otherwise(F.col("dvr")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("radio_c", F.when(F.col("radio").isNull(), 51.1969468).otherwise(F.col("radio")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("internet_c", F.when(F.col("internet").isNull(), 81.8233696).otherwise(F.col("internet")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("open_count_30_c", F.when(F.col("num_open_30days").isNull(), 0).otherwise(F.col("num_open_30days")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("mailercount_open_30_c", F.when(F.col("mailercount_open_30days").isNull(), 0).otherwise(F.col("mailercount_open_30days")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("open_count_180_c", F.when(F.col("num_open_6mo").isNull(), 0).otherwise(F.col("num_open_6mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("mailercount_open_180_c", F.when(F.col("mailercount_open_6mo").isNull(), 0).otherwise(F.col("mailercount_open_6mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("mailercount_click_180_c", F.when(F.col("mailercount_click_6mo").isNull(), 0).otherwise(F.col("mailercount_click_6mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("sent_count_30_c", F.when(F.col("num_sent_curmonth").isNull(), 0).otherwise(F.col("num_sent_curmonth")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("mailercount_sent_30_c", F.when(F.col("mailercount_sent_30days").isNull(), 0).otherwise(F.col("mailercount_sent_30days")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("mailercount_sent_180_c", F.when(F.col("mailercount_sent_180").isNull(), 0).otherwise(F.col("mailercount_sent_180")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("life_engage_12mo_c", F.when(F.col("life_engage_12mo").isNull(), 0).otherwise(F.col("life_engage_12mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("life_engage_3mo_c", F.when(F.col("life_engage_3mo").isNull(), 0).otherwise(F.col("life_engage_3mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("activities_3mo_c", F.when(F.col("activities_3mo").isNull(), 0).otherwise(F.col("activities_3mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("AGE_AGG_IND_c", F.when(F.col("AGE_AGG_IND").isNull(), 61.252184).otherwise(F.col("AGE_AGG_IND")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("memxrenew_c", F.when(F.col("memxrenew").isNull(), 0).otherwise(F.col("memxrenew")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("contact_leg_12mo_c", F.when(F.col("contact_leg_12mo").isNull(), 0).otherwise(F.col("contact_leg_12mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("teletown_12mo_i_c", F.when(F.col("teletown_12mo_i").isNull(), 0).otherwise(F.col("teletown_12mo_i")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("advocacy_donors_12mo_i_c", F.when(F.col("advocacy_donors_12mo_i").isNull(), 0).otherwise(F.col("advocacy_donors_12mo_i")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("foundation_donors_12mo_i_c", F.when(F.col("foundation_donors_12mo_i").isNull(), 0).otherwise(F.col("foundation_donors_12mo_i")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("contact_leg_12mo_i_c", F.when(F.col("contact_leg_12mo_i").isNull(), 0).otherwise(F.col("contact_leg_12mo_i")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("survey_resp_12mo_i_c", F.when(F.col("survey_resp_12mo_i").isNull(), 0).otherwise(F.col("survey_resp_12mo_i")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_months_c", F.when(F.col("num_months").isNull(), 89.1028014).otherwise(F.col("num_months")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("cntct_lifstyle_12mo_agg_hhd_c", F.when(F.col("cntct_lifstyle_12mo_agg_hhd").isNull(), 0).otherwise(F.col("cntct_lifstyle_12mo_agg_hhd")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("cntct_lifstyle_3mo_agg_hhd_c", F.when(F.col("cntct_lifstyle_3mo_agg_hhd").isNull(), 0).otherwise(F.col("cntct_lifstyle_3mo_agg_hhd")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("MemOriginDate_c", F.when(F.col("MemOriginDate").isNull(), 20105913.99).otherwise(F.col("MemOriginDate")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("Fndn_TTD_Num_c", F.when(F.col("Fndn_TTD_Num").isNull(), 0).otherwise(F.col("Fndn_TTD_Num")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("Fndn_Last_Dt_c", F.when(F.col("Fndn_Last_Dt").isNull(), 20130019.61).otherwise(F.col("Fndn_Last_Dt")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("Advo_Last_Amt_c", F.when(F.col("Advo_Last_Amt").isNull(), 0).otherwise(F.col("Advo_Last_Amt")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("fndn_hpc_amt_c", F.when(F.col("fndn_hpc_amt").isNull(), 0).otherwise(F.col("fndn_hpc_amt")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("fndn_mrhpc_dt_c", F.when(F.col("fndn_mrhpc_dt").isNull(), 20129538.67).otherwise(F.col("fndn_mrhpc_dt")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("EM_Clickrate_c", F.coalesce(F.col("em_clickrate"), F.lit(32.6040789)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("RELATIONSHIP_SEG_c", F.when(F.col("RELATIONSHIP_SEG").isNull(), 66.2788865).otherwise(F.col("RELATIONSHIP_SEG")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("totalmailings_c", F.when(F.col("totalmailings").isNull(), 0).otherwise(F.col("totalmailings")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("goi_1_dum", F.when(F.col("globally_opted_in") == '1', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("community_charity_dum", F.when(F.col("IBX_COMMUNITY_CHARITIES_AGG_HHD") == 1, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("community_health_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_HEALTH") == 1, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("deadwood_dum", F.when(F.col("DEADWOOD_MODEL").isin('DEAD', 'PROBDEAD'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("advo_s1_dum", F.when(F.col("advo_segment_cd") == 'S1', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ibx_donation", F.when(F.col("IBX_DONATION_CONTRIBUTION").isNotNull(), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("retail_a3", F.when(F.col("IBX_RETAIL_PURCHASES_MOST_FREQUE") == 'A3', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("retail_c1", F.when(F.col("IBX_RETAIL_PURCHASES_MOST_FREQUE") == 'C1', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("lifestage_4", F.when(F.col("LIFESTAGE_SEGMENT") == '4', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("member_secondary", F.when(F.col("MEMBER_FL_AGG_IND") == 'S', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IBX_INVESTING_FINANCE_GROUPI_num", F.when(F.col("IBX_INVESTING_FINANCE_GROUPING_P") == "", 0).otherwise(F.col("IBX_INVESTING_FINANCE_GROUPING_P")).cast(DoubleType()))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IBX_TRAVEL_FOREIGN_PREMIER_num", F.when(F.col("IBX_TRAVEL_FOREIGN_PREMIER") == "", 0).otherwise(F.col("IBX_TRAVEL_FOREIGN_PREMIER")).cast(DoubleType()))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IBX_VEHICLE_KNOWN_OWNED_NUMB_num", F.when(F.col("IBX_VEHICLE_KNOWN_OWNED_NUMBER_P") == "", 0).otherwise(F.col("IBX_VEHICLE_KNOWN_OWNED_NUMBER_P")).cast(DoubleType()))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IBX_ADULT_AGE_55_64_AGG_HHD_num", F.when(F.col("IBX_ADULT_AGE_55_64_AGG_HHD") == "", 0).otherwise(F.col("IBX_ADULT_AGE_55_64_AGG_HHD")).cast(DoubleType()))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IBX_HEALTH_MEDICAL_SUPPLIES_num", F.when(F.col("IBX_HEALTH_MEDICAL_SUPPLIES") == "", 0).otherwise(F.col("IBX_HEALTH_MEDICAL_SUPPLIES")).cast(DoubleType()))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IBX_HEALTH_NUTRACEUTICALS_VI_num", F.when(F.col("IBX_HEALTH_NUTRACEUTICALS_VITAMI") == "", 0).otherwise(F.col("IBX_HEALTH_NUTRACEUTICALS_VITAMI")).cast(DoubleType()))
df_cpd_replace_post = df_cpd_replace_post.withColumn("job_78_dum", F.when(F.col("IBX_OCCUPATION_INPUT_AGG_HHD").isin('7', '8'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("fndn_em_proseng_logit", F.exp(
    F.lit(-62.3055) + 
    F.col("orders_all_c") * 0.1033 +
    F.col("orders_12moterm_c") * -0.0586 +
    F.col("orders_60moterm_c") * 0.1181 +
    F.col("orders_renewals_c") * 0.0842 +
    F.col("orders_online_c") * 0.0784 +
    F.col("dvr_c") * -0.0378 +
    F.col("radio_c") * -0.00945 +
    F.col("internet_c") * 0.0108 +
    F.col("open_count_30_c") * 0.0214 +
    F.col("mailercount_open_30_c") * -0.0671 +
    F.col("open_count_180_c") * -0.00389 +
    F.col("mailercount_open_180_c") * 0.0988 +
    F.col("mailercount_click_180_c") * 0.175 +
    F.col("sent_count_30_c") * 0.0216 +
    F.col("mailercount_sent_30_c") * -0.2095 +
    F.col("mailercount_sent_180_c") * -0.0813 +
    F.col("life_engage_12mo_c") * -0.00826 +
    F.col("life_engage_3mo_c") * 0.0415 +
    F.col("activities_3mo_c") * 0.0954 +
    F.col("AGE_AGG_IND_c") * 0.0101 +
    F.col("memxrenew_c") * -0.1174 +
    F.col("newsletter_opens_cnt_12mo_c") * -0.0177 +
    F.col("contact_leg_12mo_c") * 0.0371 +
    F.col("teletown_12mo_i_c") * -2.4377 +
    F.col("advocacy_donors_12mo_i_c") * 0.5942 +
    F.col("foundation_donors_12mo_i_c") * 0.4749 +
    F.col("contact_leg_12mo_i_c") * 0.3319 +
    F.col("survey_resp_12mo_i_c") * 0.337 +
    F.col("individual_engagers_12mo") * 0.0722 +
    F.col("num_months_c") * 0.00465 +
    F.col("cntct_lifstyle_12mo_agg_hhd_c") * -0.0419 +
    F.col("cntct_lifstyle_3mo_agg_hhd_c") * 0.202 +
    F.col("MemOriginDate_c") * 0.000007177 +
    F.col("Fndn_TTD_Num_c") * 0.0596 +
    F.col("Fndn_Last_Dt_c") * 0.000018 +
    F.col("Advo_Last_Amt_c") * 0.00482 +
    F.col("fndn_hpc_amt_c") * 0.00259 +
    F.col("fndn_mrhpc_dt_c") * -0.00002 +
    F.col("EM_Clickrate_c") * -0.0175 +
    F.col("RELATIONSHIP_SEG_c") * 0.00475 +
    F.col("totalmailings_c") * 0.0276 +
    F.col("goi_1_dum") * -0.2376 +
    F.col("community_charity_dum") * 0.2197 +
    F.col("community_health_dum") * 0.1316 +
    F.col("vetera") * 0.1576 +
    F.col("job_78_dum") * -0.2946 +
    F.col("deadwood_dum") * -0.528 +
    F.col("advo_s1_dum") * -0.3156 +
    F.col("curterm_36_dum") * 0.2462 +
    F.col("ibx_donation") * 0.3745 +
    F.col("retail_a3") * 0.3064 +
    F.col("retail_c1") * 0.1939 +
    F.col("lifestage_4") * -0.1324 +
    F.col("member_secondary") * 0.4729 +
    F.col("IBX_INVESTING_FINANCE_GROUPI_num") * -0.1967 +
    F.col("IBX_TRAVEL_FOREIGN_PREMIER_num") * -0.1861 +
    F.col("IBX_VEHICLE_KNOWN_OWNED_NUMB_num") * -0.074 +
    F.col("IBX_ADULT_AGE_55_64_AGG_HHD_num") * 0.1248 +
    F.col("IBX_HEALTH_MEDICAL_SUPPLIES_num") * 0.2367 +
    F.col("IBX_HEALTH_NUTRACEUTICALS_VI_num") * 0.1859
))
df_cpd_replace_post = df_cpd_replace_post.withColumn("FNDN_AARPPRO_DM_score_b4_190819", F.col("FNDN_AARPPRO_DM_score"))
df_cpd_replace_post = df_cpd_replace_post.withColumn("FNDN_AARPPRO_DM_b4_190819", F.col("FNDN_AARPPRO_DM")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ProspMail_c", F.when(F.col("ProspMail").isNull(), 0).otherwise(F.col("ProspMail")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ProspMail_1to2_dum", F.when(F.col("ProspMail_c").isin(1, 2), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("SecAge_c", F.when(F.col("SecAge").isNull(), 67.3254117).otherwise(F.col("SecAge")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("cens_employ_pop18_plus_percent_c", F.when(F.col("cens_employ_pop18_plus_percent_c").isNull(), 8.2776992).otherwise(F.col("cens_employ_pop18_plus_percent_c")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("cens_heat_occhu_percent_oil_or_c", F.when(F.col("cens_heat_occhu_percent_oil_or_k").isNull(), 6.1768487).otherwise(F.col("cens_heat_occhu_percent_oil_or_k")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("cens_indus_empld_percent_manuf_c", F.when(F.col("cens_indus_empld_percent_manufac").isNull(), 10.4746439).otherwise(F.col("cens_indus_empld_percent_manufac")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("cens_occup_empld_percent_law_e_c", F.when(F.col("cens_occup_empld_percent_law_enf").isNull(), 1.0734889).otherwise(F.col("cens_occup_empld_percent_law_enf")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("fiscal_policy_model_c", F.when(F.col("fiscal_policy_model").isNull(), 49.5168002).otherwise(F.col("fiscal_policy_model")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("dm_c", F.when(F.col("dm").isNull(), 0).otherwise(F.col("dm")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("advocacy_donor_c", F.when(F.col("advocacy_donor").isNull(), 0).otherwise(F.col("advocacy_donor")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("Advo_Petition_c", F.when(F.col("Advo_Petition").isNull(), 0).otherwise(F.col("Advo_Petition")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("orders_altmedia_c", F.when(F.col("orders_altmedia").isNull(), 0).otherwise(F.col("orders_altmedia")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("SY_GENERALACTIVIST_c", F.when(F.col("SY_GENERALACTIVIST").isNull(), 47.9031359).otherwise(F.col("SY_GENERALACTIVIST")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_months_c", F.when(F.col("num_months").isNull(), 0).otherwise(F.col("num_months")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("general_activism_model_c", F.when(F.col("general_activist_model").isNull(), 34.9943091).otherwise(F.col("general_activist_model")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("likely_landline_connectivity_s_c", F.when(F.col("likely_landline_connectivity_sco").isNull(), 83.0054993).otherwise(F.col("likely_landline_connectivity_sco")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("spanish_speaker_model_c", F.when(F.col("spanish_speaker_model").isNull(), 32.6543272).otherwise(F.col("spanish_speaker_model")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ideology_model_c", F.when(F.col("ideology").isNull(), 49.1559663).otherwise(F.col("ideology")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("Career_c", F.when(F.col("Career").isNull(), 0).otherwise(F.col("Career").cast("double")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("Equestrian_c", F.when(F.col("Equestrian").isNull(), 0).otherwise(F.col("Equestrian").cast("double")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("Tennis_c", F.when(F.col("Tennis").isNull(), 0).otherwise(F.col("Tennis").cast("double")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("collectibles_antiques_c", F.when(F.col("collectibles_antiques").isNull(), 0).otherwise(F.col("collectibles_antiques").cast("double")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("collectibles_arts_c", F.when(F.col("collectibles_arts").isNull(), 0).otherwise(F.col("collectibles_arts").cast("double")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("consumer_electronics_c", F.when(F.col("consumer_electronics").isNull(), 0).otherwise(F.col("consumer_electronics").cast("double")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ibx_community_charities_premie_c", F.when(F.col("ibx_community_charities_premier").isNull(), 0).otherwise(F.col("ibx_community_charities_premier").cast("double")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ibx_current_affairs_politics_p_c", F.when(F.col("ibx_current_affairs_politics_pre").isNull(), 0).otherwise(F.col("ibx_current_affairs_politics_pre").cast("double")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("music_home_stereo_c", F.when(F.col("music_home_stereo").isNull(), 0).otherwise(F.col("music_home_stereo").cast("double")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("reading_grouping_c", F.when(F.col("reading_grouping").isNull(), 0).otherwise(F.col("reading_grouping").cast("double")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ideology_c", F.when(F.col("ideology").isNull(), 48.7105462).otherwise(F.col("ideology")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("ideology_20_dum", F.when(F.col("ideology_c") <= 20, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("chacq_u_dum", F.when(F.col("ch_acq") == 'U', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("business_u", F.when(F.col("IBX_BUSINESS_OWNER_AGG_HHD") == 'U', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("homeloan_htok", F.when(F.col("IBX_HOME_LOAN_AMOUNT_1_RANGES").isin('H', 'I', 'J', 'K'), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("retail_d1", F.when(F.col("IBX_RETAIL_PURCHASES_MOST_FREQUE") == 'D1', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IBX_MOVIE_MUSIC_GROUPING_num", F.when(F.col("IBX_MOVIE_MUSIC_GROUPING") == "", 0).otherwise(F.col("IBX_MOVIE_MUSIC_GROUPING")).cast(DoubleType()))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IBX_NUM_LINES_OF_CREDIT_AGG__num", F.when(F.col("IBX_NUM_LINES_OF_CREDIT_AGG_HHD") == "", 0).otherwise(F.col("IBX_NUM_LINES_OF_CREDIT_AGG_HHD")).cast(DoubleType()))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IBX_TOTAL_ONLINE_PURCHASES_num", F.when(F.col("IBX_TOTAL_ONLINE_PURCHASES") == "", 0).otherwise(F.col("IBX_TOTAL_ONLINE_PURCHASES")).cast(DoubleType()))
df_cpd_replace_post = df_cpd_replace_post.withColumn("likely_black_agg_c", F.when(F.col("likely_black_agg").isNull(), 92.2485785).otherwise(F.col("likely_black_agg")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("fndn_aarppro_dm_logit", F.exp(
    F.lit(-4.77) + 
    F.col("ProspMail_1to2_dum") * 0.197 +
    F.col("cntct_lifstyle_12mo_agg_hhd_c") * -0.0235 +
    F.col("SecAge_c") * 0.0103 +
    F.col("cens_employ_pop18_plus_percent_c") * -0.0201 +
    F.col("cens_heat_occhu_percent_oil_or_c") * 0.00564 +
    F.col("cens_indus_empld_percent_manuf_c") * 0.0109 +
    F.col("cens_occup_empld_percent_law_e_c") * 0.0347 +
    F.col("fiscal_policy_model_c") * -0.00596 +
    F.col("likely_black_agg_c") * -0.00516 +
    F.col("dm_c") * -1.229 +
    F.col("advocacy_donor_c") * 0.5379 +
    F.col("Advo_Petition_c") * -0.0636 +
    F.col("totalmailings_c") * 0.0428 +
    F.col("orders_altmedia_c") * 0.1061 +
    F.col("advocacy_petitions_12mo") * 0.208 +
    F.col("newsletter_opens_cnt_12mo_c") * -0.1695 +
    F.col("SY_GENERALACTIVIST_c") * 0.0209 +
    F.col("foundation_donors_12mo_i_c") * 1.3923 +
    F.col("structured_12mo_i") * 1.651 +
    F.col("num_months_c") * -0.0008 +
    F.col("general_activism_model_c") * -0.016 +
    F.col("likely_landline_connectivity_s_c") * 0.00929 +
    F.col("spanish_speaker_model_c") * -0.0195 +
    F.col("ideology_model_c") * 0.0079 +
    F.col("Career_c") * 0.2231 +
    F.col("Equestrian_c") * -0.4922 +
    F.col("Tennis_c") * 0.3658 +
    F.col("collectibles_antiques_c") * -0.2267 +
    F.col("collectibles_arts_c") * 0.327 +
    F.col("consumer_electronics_c") * -0.2413 +
    F.col("ibx_adult_age_55_64_agg_hhd_num") * 0.2108 +
    F.col("community_charity_dum") * 1.1949 +
    F.col("ibx_community_charities_premie_c") * -0.9233 +
    F.col("ibx_current_affairs_politics_p_c") * -0.2646 +
    F.col("ibx_health_medical_supplies_num") * 0.3053 +
    F.col("music_home_stereo_c") * -0.2073 +
    F.col("reading_grouping_c") * -0.2899 +
    F.col("ideology_20_dum") * 0.2624 +
    F.col("chacq_u_dum") * -0.4655 +
    F.col("business_u") * -0.2039 +
    F.col("homeloan_htok") * -0.1914 +
    F.col("retail_d1") * -0.5512 +
    F.col("IBX_MOVIE_MUSIC_GROUPING_num") * 0.3066 +
    F.col("IBX_NUM_LINES_OF_CREDIT_AGG__num") * -0.0341 +
    F.col("IBX_TOTAL_ONLINE_PURCHASES_num") * -0.0218
))
df_cpd_replace_post = df_cpd_replace_post.withColumn("orders_36moterm_c", F.when(F.col("orders_36moterm").isNull(), 0).otherwise(F.col("orders_36moterm")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("cens_built_hu_percent_built_lt_c", F.when(F.col("cens_built_hu_percent_built_lt19").isNull(), 10.2462499).otherwise(F.col("cens_built_hu_percent_built_lt19")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("cens_commute_commuter_avg_trav_c", F.when(F.col("cens_commute_commuter_avg_trav_t").isNull(), 28.3195732).otherwise(F.col("cens_commute_commuter_avg_trav_t")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("cens_commute_wrkrs_percent_pub_c", F.when(F.col("cens_commute_wrkrs_percent_publi").isNull(), 4.2186635).otherwise(F.col("cens_commute_wrkrs_percent_publi")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("cens_commute_wrkrs_percent_wor_c", F.when(F.col("cens_commute_wrkrs_percent_work_").isNull(), 4.7296193).otherwise(F.col("cens_commute_wrkrs_percent_work_")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("cens_ethnic_pop_percent_am_ind_c", F.when(F.col("cens_ethnic_pop_percent_am_ind_a").isNull(), 0.7379849).otherwise(F.col("cens_ethnic_pop_percent_am_ind_a")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("cens_rent_rntl_aggregate_contr_c", F.when(F.col("cens_rent_rntl_aggregate_contrac").isNull(), 213802.59).otherwise(F.col("cens_rent_rntl_aggregate_contrac")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("religious_c", F.when(F.col("religious").isNull(), 8.5853273).otherwise(F.col("religious")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("mailercount_click_30_c", F.when(F.col("mailercount_click_30days").isNull(), 0).otherwise(F.col("mailercount_click_30days")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_clicked_past12", F.coalesce(F.col("num_clicked_past12"), F.lit(0)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_ct", F.coalesce(F.col("num_ct"), F.lit(0)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_sent_past12", F.coalesce(F.col("num_sent_past12"), F.lit(0)))
df_cpd_replace_post = df_cpd_replace_post.withColumn("age_agg_ind_c", F.when(F.col("age_agg_ind").isNull(), 63.3605498).otherwise(F.col("age_agg_ind")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("advocacy_donations_12mo_c", F.when(F.col("advocacy_donations_12mo").isNull(), 0).otherwise(F.col("advocacy_donations_12mo")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("suppression_c", F.when(F.col("suppression").isNull(), 0.0011949).otherwise(F.col("suppression")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("driver_online_12mo_i_c", F.when(F.col("driver_online_12mo_i").isNull(), 0).otherwise(F.col("driver_online_12mo_i")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("activist_i_c", F.when(F.col("activist_i").isNull(), 0).otherwise(F.col("activist_i")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("memorigin_c", F.when(F.col("memorigin").isNull(), 18076.29).otherwise(F.col("memorigin")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("num_months_c", F.when(F.col("num_months").isNull(), 0).otherwise(F.col("num_months")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("MemOriginDate_c", F.when(F.col("MemOriginDate").isNull(), 20096043.1).otherwise(F.col("MemOriginDate")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("DRVS_Flag_c", F.when(F.col("DRVS_Flag").isNull(), 0).otherwise(F.col("DRVS_Flag")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("live_answer_pm_c", F.when(F.col("live_answer_pm").isNull(), 68.7991).otherwise(F.col("live_answer_pm")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("EM_Clickrate_c", F.when(F.col("EM_Clickrate").isNull(), 43.8026291).otherwise(F.col("EM_Clickrate")))
df_cpd_replace_post = df_cpd_replace_post.withColumn("emailable_dum", F.when(F.col("EMAILABLE_AGG_IND") == 'Y', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("non_active_dum", F.when(F.col("memstatus") != '0', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("child_6to10_dum", F.when(F.col("IBX_CHILD_AGE_06_10_AGG_HHD") == 1, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("community_animal_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_ANIMAL") == 1, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("community_childr_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_CHILDR") == 1, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("active_sp_dum", F.when(F.col("Overall_Active_SP_Reltshps") >= '1', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("past3touch_0_dum", F.when(F.col("Past3MoTouchCt_Overall") == "", 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("chacq_f_dum", F.when(F.col("ch_acq") == 'F', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("chacq_u_dum", F.when(F.col("ch_acq") == 'U', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("currentaffairs", F.when(F.col("IBX_CURRENT_AFFAIRS_AGG_HHD") == 1, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("greenliving", F.when(F.col("IBX_GREEN_LIVING") == 1, 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("homeloan_rate_f", F.when(F.col("IBX_HOME_LOAN_INTEREST_RT_AGG_HH") == 'F', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("retail_b6", F.when(F.col("IBX_RETAIL_PURCHASES_MOST_FREQUE") == 'B6', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("lifestage_7", F.when(F.col("LIFESTAGE_SEGMENT") == '7', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IBX_HOME_GARDEN_AGG_HHD_num", F.when(F.col("IBX_HOME_GARDEN_AGG_HHD") == "", 0).otherwise(F.col("IBX_HOME_GARDEN_AGG_HHD")).cast(DoubleType()))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IBX_HEALTH_HOMEOPATHIC_num", F.when(F.col("IBX_HEALTH_HOMEOPATHIC") == "", 0).otherwise(F.col("IBX_HEALTH_HOMEOPATHIC")).cast(DoubleType()))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IBX_HEALTH_ORTHOPEDIC_num", F.when(F.col("IBX_HEALTH_ORTHOPEDIC") == "", 0).otherwise(F.col("IBX_HEALTH_ORTHOPEDIC")).cast(DoubleType()))
df_cpd_replace_post = df_cpd_replace_post.withColumn("IBX_INVESTORS_HIGHLY_LIKELY_num", F.when(F.col("IBX_INVESTORS_HIGHLY_LIKELY") == 'Y', 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("auto_renew_dum", F.when(F.col("auto_renew_start_dt").isNotNull(), 1).otherwise(0))
df_cpd_replace_post = df_cpd_replace_post.withColumn("fraud_click_em_logit", F.exp(
    F.lit(-79.2575) + 
    F.col("orders_36moterm_c") * 0.0933 +
    F.col("cens_built_hu_percent_built_lt_c") * -0.00296 +
    F.col("cens_commute_commuter_avg_trav_c") * -0.00407 +
    F.col("cens_commute_wrkrs_percent_pub_c") * 0.00414 +
    F.col("cens_commute_wrkrs_percent_wor_c") * 0.00794 +
    F.col("cens_ethnic_pop_percent_am_ind_c") * 0.00759 +
    F.col("cens_rent_rntl_aggregate_contr_c") * -0.0000000945 +
    F.col("religious_c") * -0.0145 +
    F.col("open_count_30_c") * 0.00594 +
    F.col("mailercount_open_30_c") * 0.0736 +
    F.col("mailercount_click_30_c") * 0.1139 +
    F.col("mailercount_open_180_c") * 0.0118 +
    F.col("mailercount_click_180_c") * 0.1544 +
    F.col("num_clicked_past12") * 0.00867 +
    F.col("sent_count_30_c") * 0.0115 +
    F.col("mailercount_sent_30_c") * 0.025 +
    F.col("num_ct") * -0.0055 +
    F.col("num_sent_past12") * -0.0045 +
    F.col("age_agg_ind_c") * 0.03 +
    F.col("memxrenew_c") * -0.0438 +
    F.col("advocacy_donations_12mo_c") * 0.0585 +
    F.col("suppression_c") * 0.3671 +
    F.col("driver_online_12mo_i_c") * 0.1909 +
    F.col("activist_i_c") * 0.139 +
    F.col("memorigin_c") * 0.000222 +
    F.col("num_months_c") * 0.00893 +
    F.col("cntct_lifstyle_12mo_agg_hhd_c") * -0.0142 +
    F.col("MemOriginDate_c") * 0.000003474 +
    F.col("DRVS_Flag_c") * 0.1597 +
    F.col("fndn_hpc_amt_c") * 0.000734 +
    F.col("live_answer_pm_c") * -0.00129 +
    F.col("EM_Clickrate_c") * -0.0447 +
    F.col("emailable_dum") * -0.4863 +
    F.col("non_active_dum") * 0.4242 +
    F.col("child_6to10_dum") * -0.1249 +
    F.col("community_animal_dum") * -0.1165 +
    F.col("community_childr_dum") * -0.1148 +
    F.col("grandchildren_dum") * -0.0972 +
    F.col("active_sp_dum") * 0.1239 +
    F.col("past3touch_0_dum") * 0.2282 +
    F.col("chacq_f_dum") * 0.1092 +
    F.col("chacq_u_dum") * -0.2626 +
    F.col("currentaffairs") * -0.0831 +
    F.col("greenliving") * 0.0965 +
    F.col("homeloan_rate_f") * 0.0818 +
    F.col("retail_b6") * -0.2423 +
    F.col("lifestage_7") * 0.1529 +
    F.col("IBX_HOME_GARDEN_AGG_HHD_num") * 0.0523 +
    F.col("IBX_HEALTH_HOMEOPATHIC_num") * -0.0403 +
    F.col("IBX_HEALTH_ORTHOPEDIC_num") * 0.1074 +
    F.col("IBX_INVESTORS_HIGHLY_LIKELY_num") * 0.1044 +
    F.col("auto_renew_dum") * -0.0565
))
df_cpd_replace_post.write.format("delta").mode("overwrite").saveAsTable("intermed.cpd_replace_post")

df_cpd_replace_post = spark.table("intermed.cpd_replace_post")
df_cpd_replace_post_with_dummy = df_cpd_replace_post.withColumn("dummy_partition", F.lit(1))
window_spec = Window.partitionBy("dummy_partition")
df_score_ranks = df_cpd_replace_post_with_dummy.withColumn("soc_sec_em", F.ntile(99).over(window_spec.orderBy(F.col("soc_sec_em_score").desc())))
df_score_ranks = df_score_ranks.withColumn("soc_sec_em_att", F.ntile(99).over(window_spec.orderBy(F.col("soc_sec_em_att_score").desc())))
df_score_ranks = df_score_ranks.withColumn("rx_advo_65plus", F.ntile(99).over(window_spec.orderBy(F.col("rx_65_plus_score").desc())))
df_score_ranks = df_score_ranks.withColumn("rx_advo_50_64", F.ntile(99).over(window_spec.orderBy(F.col("rx_5064_score").desc())))
df_score_ranks = df_score_ranks.withColumn("fndn_em_proseng", F.ntile(99).over(window_spec.orderBy(F.col("fndn_em_proseng_logit").desc())))
df_score_ranks = df_score_ranks.withColumn("fndn_aarppro_dm", F.ntile(99).over(window_spec.orderBy(F.col("fndn_aarppro_dm_logit").desc())))
df_score_ranks = df_score_ranks.withColumn("fraud_click_em", F.ntile(99).over(window_spec.orderBy(F.col("fraud_click_em_logit").desc())))
df_score_ranks = df_score_ranks.withColumn("caregiving_em", F.ntile(99).over(window_spec.orderBy(F.col("caregiving_em_score").desc())))
df_score_ranks = df_score_ranks.withColumn("driver_safety_tek_em", F.ntile(99).over(window_spec.orderBy(F.col("driver_safety_tek_em_score").desc())))
df_score_ranks = df_score_ranks.withColumn("medicare_tth", F.ntile(99).over(window_spec.orderBy(F.col("score_medicare_tth").desc())))
df_score_ranks = df_score_ranks.drop("dummy_partition")
df_score_ranks.write.format("delta").mode("overwrite").saveAsTable("intermed.score_ranks")

df_cpd_replace_post = spark.table("intermed.cpd_replace_post")
df_actives = df_cpd_replace_post.filter((F.col("rpm_score") != 1) & (F.col("memstatus") == '0'))
df_actives_with_dummy = df_actives.withColumn("dummy_partition", F.lit(1))
window_rpm = Window.partitionBy("dummy_partition").orderBy(F.col("rpm_score").desc())
df_score_ranks_rpm_actives = df_actives_with_dummy.withColumn("rpm2019_centile", F.ntile(100).over(window_rpm)).drop("dummy_partition")
df_score_ranks_rpm_actives.write.format("delta").mode("overwrite").saveAsTable("intermed.score_ranks_rpm_actives")

df_expires = df_cpd_replace_post.filter((F.col("rpm_score") != 1) & (F.col("memstatus") == '5'))
df_expires_with_dummy = df_expires.withColumn("dummy_partition", F.lit(1))
df_score_ranks_rpm_expires = df_expires_with_dummy.withColumn("rpm2019_centile", F.ntile(100).over(window_rpm)).drop("dummy_partition")
df_score_ranks_rpm_expires.write.format("delta").mode("overwrite").saveAsTable("intermed.score_ranks_rpm_expires")

df_score_ranks_rpm_actives = spark.table("intermed.score_ranks_rpm_actives").withColumn("source", F.lit("actives"))
df_cpd_replace_post_emu = spark.table("intermed.cpd_replace_post").filter((F.col("rpm_score") == 1) | (F.col("memstatus") == 'E')).withColumn("source", F.lit("emu"))
df_score_ranks_rpm_expires = spark.table("intermed.score_ranks_rpm_expires").withColumn("source", F.lit("expires"))

df_score_ranks_rpm = df_score_ranks_rpm_actives.unionByName(df_cpd_replace_post_emu, allowMissingColumns=True)
df_score_ranks_rpm = df_score_ranks_rpm.unionByName(df_score_ranks_rpm_expires, allowMissingColumns=True)

df_score_ranks_rpm = df_score_ranks_rpm.withColumn("rpm2019_centile", F.when(F.col("source").isin("actives", "expires"), F.col("rpm2019_centile") + 1).otherwise(F.col("rpm2019_centile")))
df_score_ranks_rpm = df_score_ranks_rpm.withColumn("rpm2019_centile", F.when(F.col("rpm_score") == 1, 0).otherwise(F.col("rpm2019_centile")))
df_score_ranks_rpm = df_score_ranks_rpm.withColumn("rpm2019_centile", F.when(F.col("memstatus") == 'E', 99).otherwise(F.col("rpm2019_centile")))
df_score_ranks_rpm = df_score_ranks_rpm.withColumn("rpm2019_centile", F.when(F.col("rpm2019_centile") == 100, 99).otherwise(F.col("rpm2019_centile")))
df_score_ranks_rpm.write.format("delta").mode("overwrite").saveAsTable("intermed.score_ranks_rpm")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_geo_appends_rpm = df_geo_appends_rpm.withColumn("FNDN_AARPPRO_DM_score_BKP", F.col("FNDN_AARPPRO_DM_score"))
df_geo_appends_rpm.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_score_ranks = spark.table("intermed.score_ranks")
df_score_ranks_rpm = spark.table("intermed.score_ranks_rpm")
df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_score_ranks.createOrReplaceTempView("score_ranks")
df_score_ranks_rpm.createOrReplaceTempView("score_ranks_rpm")

df_geo_appends_rpm_updated = spark.sql("""
    SELECT
        b.soc_sec_em + 1 AS soc_sec_em,
        b.soc_sec_em_att + 1 AS soc_sec_em_att,
        b.rx_advo_65plus + 1 AS rx_advo_65plus,
        b.rx_advo_50_64 + 1 AS rx_advo_50_64,
        c.rpm2019_centile,
        LEAST(ROUND(c.rpm_score * 100, 1), 99) AS rpm2019_prob,
        b.fndn_em_proseng + 1 AS fndn_em_proseng,
        b.fndn_aarppro_dm + 1 AS fndn_aarppro_dm,
        b.FNDN_AARPPRO_DM_score_b4_190819,
        b.FNDN_AARPPRO_DM_b4_190819,
        b.fraud_click_em + 1 AS fraud_click_em,
        b.caregiving_em + 1 AS caregiving_em,
        b.driver_safety_tek_em + 1 AS driver_safety_tek_em,
        b.medicare_tth + 1 AS medicare_tth,
        a.*
    FROM
        intermed.geo_appends_rpm AS a
    LEFT JOIN
        intermed.score_ranks AS b ON a.mid_key = b.mid_key
    LEFT JOIN
        intermed.score_ranks_rpm AS c ON a.mid_key = c.mid_key
""")
df_geo_appends_rpm_updated = df_geo_appends_rpm_updated.drop("FNDN_AARPPRO_DM_score", "FNDN_AARPPRO_DM")
df_geo_appends_rpm_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_geo_cd_2010_table = df_geo_appends_rpm.groupBy("geo_cd_2010").agg(F.mean("ideology").alias("geo_cd_2010_mean"))
df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_contact_history_sum = spark.table("intermed.contact_history_sum")
df_contact_history_sum.createOrReplaceTempView("contact_history_sum")

df_analysis3 = spark.sql("""
    SELECT
        a.*,
        c.mailercount_click_30days,
        c.num_clicked_curmonth,
        c.num_inb_30days,
        c.num_open_30days,
        c.mailercount_click_1_3mo,
        c.mailercount_click_3_6mo,
        c.mailercount_open_30days,
        c.num_inb_1_3mo
    FROM
        geo_appends_rpm a
    LEFT JOIN
        contact_history_sum c ON a.mid_key = c.mid_key
""")
df_analysis3.write.option("compression", "snappy").format("delta").mode("overwrite").saveAsTable("intermed.analysis3")

df_analysis3 = spark.table("intermed.analysis3")
df_score_autobuy = df_analysis3
df_score_autobuy = df_score_autobuy.withColumn("emailable_dum", F.when(F.col("EMAILABLE_AGG_IND") == 'Y', 1).otherwise(0))
df_score_autobuy = df_score_autobuy.withColumn("EM_Clickrate", F.when(F.col("EM_Clickrate").isNull(), 0).otherwise(F.col("EM_Clickrate")))
df_score_autobuy = df_score_autobuy.withColumn("gender_M", F.when(F.col("Gender_agg_ind") == 'M', 1).otherwise(0))
df_score_autobuy = df_score_autobuy.withColumn("cens_earn_hh_percent_with_public", F.when(F.col("cens_earn_hh_percent_with_public").isNull(), 2.1385642).otherwise(F.col("cens_earn_hh_percent_with_public")))
df_score_autobuy = df_score_autobuy.withColumn("cens_ethnic_pop_percent_white_on", F.when(F.col("cens_ethnic_pop_percent_white_on").isNull(), 76.6362561).otherwise(F.col("cens_ethnic_pop_percent_white_on")))
df_score_autobuy = df_score_autobuy.withColumn("live_answer_pm", F.when(F.col("live_answer_pm").isNull(), 50).otherwise(F.col("live_answer_pm")))
df_score_autobuy = df_score_autobuy.withColumn("mailercount_click_1mos", F.when(F.col("mailercount_click_30days").isNull(), 0).otherwise(F.col("mailercount_click_30days")))
df_score_autobuy = df_score_autobuy.withColumn("num_click_1mos", F.when(F.col("num_clicked_curmonth").isNull(), 0).otherwise(F.col("num_clicked_curmonth")))
df_score_autobuy = df_score_autobuy.withColumn("num_inb_1mos", F.when(F.col("num_inb_30days").isNull(), 0).otherwise(F.col("num_inb_30days")))
df_score_autobuy = df_score_autobuy.withColumn("num_open_1mos", F.when(F.col("num_open_30days").isNull(), 0).otherwise(F.col("num_open_30days")))
df_score_autobuy = df_score_autobuy.withColumn("num_inb_1_3mo", F.when(F.col("num_inb_1_3mo").isNull(), 0).otherwise(F.col("num_inb_1_3mo")))
df_score_autobuy = df_score_autobuy.withColumn("rpm2019_centile", F.when(F.col("rpm2019_centile").isNull(), 50).otherwise(F.col("rpm2019_centile")))
df_score_autobuy = df_score_autobuy.withColumn("advocacy_grassroots_engager", F.when(F.col("advocacy_grassroots_engager").isNull(), 0).otherwise(F.col("advocacy_grassroots_engager")))
df_score_autobuy = df_score_autobuy.withColumn("attended_aarp_event", F.when(F.col("attended_aarp_event").isNull(), 0).otherwise(F.col("attended_aarp_event")))
df_score_autobuy = df_score_autobuy.withColumn("mailercount_click_1_3mo", F.when(F.col("mailercount_click_1_3mo").isNull(), 0).otherwise(F.col("mailercount_click_1_3mo")))
df_score_autobuy = df_score_autobuy.withColumn("mailercount_click_3_6mo", F.when(F.col("mailercount_click_3_6mo").isNull(), 0).otherwise(F.col("mailercount_click_3_6mo")))
df_score_autobuy = df_score_autobuy.withColumn("mailercount_open_30days", F.when(F.col("mailercount_open_30days").isNull(), 0).otherwise(F.col("mailercount_open_30days")))
df_score_autobuy = df_score_autobuy.withColumn("orders_12moterm", F.when(F.col("orders_12moterm").isNull(), 0).otherwise(F.col("orders_12moterm")))
df_score_autobuy = df_score_autobuy.withColumn("Fndn_TTD_Num", F.when(F.col("Fndn_TTD_Num").isNull(), 0).otherwise(F.col("Fndn_TTD_Num")))
df_score_autobuy = df_score_autobuy.withColumn("cens_indus_empld_percent_informa", F.when(F.col("cens_indus_empld_percent_informa").isNull(), 1.8703271).otherwise(F.col("cens_indus_empld_percent_informa")))
df_score_autobuy = df_score_autobuy.withColumn("cens_mortg_oohu_percent_no_mortg", F.when(F.col("cens_mortg_oohu_percent_no_mortg").isNull(), 39.017767).otherwise(F.col("cens_mortg_oohu_percent_no_mortg")))
df_score_autobuy = df_score_autobuy.withColumn("cens_occup_empld_percent_motor_v", F.when(F.col("cens_occup_empld_percent_motor_v").isNull(), 3.159839).otherwise(F.col("cens_occup_empld_percent_motor_v")))
df_score_autobuy = df_score_autobuy.withColumn("cens_occup_empld_percent_sales_a", F.when(F.col("cens_occup_empld_percent_sales_a").isNull(), 10.5967723).otherwise(F.col("cens_occup_empld_percent_sales_a")))
df_score_autobuy = df_score_autobuy.withColumn("ADVO35", F.when(F.col("advo_segment_cd").isin('S3', 'S5'), 1).otherwise(0))
df_score_autobuy = df_score_autobuy.withColumn("ibx_grand_children_agg_hhdy", F.when(F.col("ibx_grand_children_agg_hhd") == 'Y', 1).otherwise(0))
df_score_autobuy = df_score_autobuy.withColumn("logit_autobuy", 
    F.lit(-4.6243) +
    F.col("emailable_dum") * -0.8407 +
    F.col("EM_Clickrate") * -0.00581 +
    F.col("gender_M") * 0.217 +
    F.col("cens_earn_hh_percent_with_public") * -0.0424 +
    F.col("cens_ethnic_pop_percent_white_on") * -0.00756 +
    F.col("live_answer_pm") * 0.00489 +
    F.col("mailercount_click_1mos") * 0.418 +
    F.col("num_click_1mos") * -1.0116 +
    F.col("num_inb_1mos") * 1.0997 +
    F.col("num_open_1mos") * -1.0778 +
    F.col("rpm2019_centile") * -0.00586 +
    F.col("advocacy_grassroots_engager") * -0.5752 +
    F.col("attended_aarp_event") * -0.8893 +
    F.col("mailercount_click_1_3mo") * 0.2226 +
    F.col("mailercount_click_3_6mo") * 0.1506 +
    F.col("mailercount_open_30days") * 0.1057 +
    F.col("num_inb_1_3mo") * -0.0234 +
    F.col("orders_12moterm") * -0.0389 +
    F.col("Fndn_TTD_Num") * -0.1128 +
    F.col("cens_indus_empld_percent_informa") * 0.0403 +
    F.col("cens_mortg_oohu_percent_no_mortg") * 0.0118 +
    F.col("cens_occup_empld_percent_motor_v") * 0.0416 +
    F.col("cens_occup_empld_percent_sales_a") * 0.0301 +
    F.col("ADVO35") * 0.3687 +
    F.col("ibx_grand_children_agg_hhdy") * -0.3141
)
df_score_autobuy = df_score_autobuy.withColumn("score_autobuy", F.exp(F.col("logit_autobuy")) / (F.lit(1) + F.exp(F.col("logit_autobuy"))))
df_score_autobuy = df_score_autobuy.select(
    "mid_key", "score_autobuy", "emailable_dum", "EM_Clickrate", "gender_M", "cens_earn_hh_percent_with_public",
    "cens_ethnic_pop_percent_white_on", "live_answer_pm", "mailercount_click_1mos", "num_click_1mos", "num_inb_1mos",
    "num_open_1mos", "rpm2019_centile", "advocacy_grassroots_engager", "attended_aarp_event", "mailercount_click_1_3mo",
    "mailercount_click_3_6mo", "mailercount_open_30days", "num_inb_1_3mo", "orders_12moterm", "Fndn_TTD_Num",
    "cens_indus_empld_percent_informa", "cens_mortg_oohu_percent_no_mortg", "cens_occup_empld_percent_motor_v",
    "cens_occup_empld_percent_sales_a", "ADVO35", "ibx_grand_children_agg_hhdy"
)
df_score_autobuy.write.format("delta").mode("overwrite").saveAsTable("intermed.score_autobuy")

df_score_autobuy = spark.table("intermed.score_autobuy")
columns_to_check = [
    "emailable_dum", "EM_Clickrate", "gender_M", "cens_earn_hh_percent_with_public", "cens_ethnic_pop_percent_white_on",
    "live_answer_pm", "mailercount_click_1mos", "num_click_1mos", "num_inb_1mos", "num_open_1mos", "rpm2019_centile",
    "advocacy_grassroots_engager", "attended_aarp_event", "mailercount_click_1_3mo", "mailercount_click_3_6mo",
    "mailercount_open_30days", "num_inb_1_3mo", "orders_12moterm", "Fndn_TTD_Num", "cens_indus_empld_percent_informa",
    "cens_mortg_oohu_percent_no_mortg", "cens_occup_empld_percent_motor_v", "cens_occup_empld_percent_sales_a",
    "ADVO35", "ibx_grand_children_agg_hhdy"
]
for col_name in columns_to_check:
    print(f"Frequency for {col_name}:")
    df_score_autobuy.groupBy(col_name).count().orderBy(col_name).show(truncate=False)

df_score_autobuy = spark.table("intermed.score_autobuy")
df_score_autobuy_with_dummy = df_score_autobuy.withColumn("dummy_partition", F.lit(1))
window_auto_buy = Window.partitionBy("dummy_partition").orderBy(F.col("score_autobuy").desc())
df_score_ranks = df_score_autobuy_with_dummy.withColumn("Auto_Buying_EM", F.ntile(99).over(window_auto_buy)).drop("dummy_partition")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_geo_appends_rpm = df_geo_appends_rpm.withColumn("merkleid_int", F.col("merkleid").cast(IntegerType()))
df_score_ranks = df_score_ranks.withColumnRenamed("mid_key", "mid_key_b")
df_geo_appends_rpm_updated = df_geo_appends_rpm.alias("a").join(
    df_score_ranks.alias("b"),
    F.col("a.merkleid_int") == F.col("b.mid_key_b"),
    "left"
).select(
    (F.col("b.Auto_Buying_EM") + 1).alias("Auto_Buying_EM"),
    "a.*"
).drop("merkleid_int", "mid_key_b")
df_geo_appends_rpm_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
columns_to_check_final = [
    "live_answer_am", "live_answer_aft", "live_answer_pm", "TAS_Volunteer", "Soc_Sec_LO", "FNDN_Housing",
    "Fraudwatch_lo", "Auto_Buying_EM", "CPD_CARE_ATTEND", "CPD_JOBS_ATTEND", "lo_aca", "WorkNSaveACT_PH",
    "advo_dm_65plus", "Rx_lo", "NEW_SCORE11_CENTILE"
]
for col_name in columns_to_check_final:
    print(f"Frequency for {col_name}:")
    df_geo_appends_rpm.groupBy(col_name).count().orderBy(col_name).show(truncate=False)

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_web_visits = spark.table("intermed.web_visits")
df_contact_history_sum = spark.table("intermed.contact_history_sum")
df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_web_visits.createOrReplaceTempView("web_visits")
df_contact_history_sum.createOrReplaceTempView("contact_history_sum")

df_analysis3 = spark.sql("""
    SELECT
        a.*,
        b.num_clicks_renew_1mos,
        b.num_clicks_renew_1to3mos,
        b.num_clicks_renew_1wk,
        b.num_clicks_renew_3to6mos,
        c.call_freq,
        c.pct_live2,
        c.liveanswer_freq_3,
        c.liveanswer_freq3_6,
        c.num_click,
        c.num_open_6mo,
        c.click_rate_6mo,
        c.mailercount_click_6mo,
        c.mailercount_click_1_3mo,
        c.mailercount_click_30days,
        c.mailercount_click_3_6mo,
        c.mailercount_open_30days,
        c.mailercount_open_3_6mo,
        c.mailercount_open_1_3mo,
        c.num_clicked_curmonth,
        c.num_clicked_3_6mo,
        c.num_ib_1_3mo,
        c.num_ib_30days,
        c.num_ib_3_6mo,
        c.num_open_1_3mo,
        c.num_open_3_6mo,
        c.num_clicked_1_3mo,
        c.num_inb_30days,
        c.liveanswer_comp_freq_3,
        c.liveanswer_comp_freq3_6,
        c.liveanswer_comp_freq6_12,
        c.num_open_30days,
        c.mailercount_open_6mo,
        c.num_sent_curmonth,
        c.mailercount_sent_30days,
        c.mailercount_sent_180,
        c.num_clicked_past12,
        c.num_ct,
        c.num_sent_past12,
        c.mailct_past12,
        c.mailct_all,
        c.call_freq_12mo_both
    FROM
        geo_appends_rpm a
    LEFT JOIN
        web_visits b ON a.mid_key = b.mid_key
    LEFT JOIN
        contact_history_sum c ON a.mid_key = c.mid_key
""")
df_analysis3.write.option("compression", "snappy").format("delta").mode("overwrite").saveAsTable("intermed.analysis3")

df_analysis3 = spark.table("intermed.analysis3")
df_work_jobs_score = df_analysis3
df_work_jobs_score = df_work_jobs_score.withColumn("NEW_SCORE4_CENTILE", F.when(F.col("NEW_SCORE4_CENTILE").isNull(), 99).otherwise(F.col("NEW_SCORE4_CENTILE")))
df_work_jobs_score = df_work_jobs_score.withColumn("NEW_SCORE40_CENTILE", F.when(F.col("NEW_SCORE40_CENTILE").isNull(), 99).otherwise(F.col("NEW_SCORE40_CENTILE")))
df_work_jobs_score = df_work_jobs_score.withColumn("em_clickrate_c", F.when(F.col("em_clickrate").isNull(), 99).otherwise(F.col("em_clickrate")))
df_work_jobs_score = df_work_jobs_score.withColumn("IPL_CARE_DM_REG_c", F.coalesce(F.col("IPL_CARE_DM_REG"), F.lit(99)))
df_work_jobs_score = df_work_jobs_score.withColumn("age_60to66_dum", F.when((F.col("age_agg_ind") >= 60) & (F.col("age_agg_ind") <= 66), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("age_gt75_dum", F.when(F.col("age_agg_ind") >= 75, 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("NEW_SCORE46_CENTILE_c", F.coalesce(F.col("NEW_SCORE46_CENTILE"), F.lit(99)))
df_work_jobs_score = df_work_jobs_score.withColumn("R_number_of_lines_of_credit", F.when(F.col("IBX_NUM_OF_LINES_OF_CREDIT").isin('1', '2', '3', '4', '7'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("VOTEPROP2016_c", F.when(F.col("general_election_vote_propensity").isNull(), 83.8246349).otherwise(F.col("general_election_vote_propensity")))
df_work_jobs_score = df_work_jobs_score.withColumn("ideology_c", F.when(F.col("ideology").isNull(), 44).otherwise(F.col("ideology")))
df_work_jobs_score = df_work_jobs_score.withColumn("petadv12_c", F.when(F.col("petadv12") == 1, 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("IBX_ADULTS_NUM_AGG_HHDls3", F.when(F.col("IBX_ADULTS_NUM_AGG_HHD").isin('1', '6'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("elderly_u_dum", F.when(F.col("IBX_ELDERLY_PARENT_AGG_HHD") == 'Y', 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("CENS_GRPQTRS_POP_PERCENT_MIL_c", F.when(F.col("CENS_GRPQTRS_POP_PERCENT_MILITAR").isNull(), 0.01).otherwise(F.col("CENS_GRPQTRS_POP_PERCENT_MILITAR")))
df_work_jobs_score = df_work_jobs_score.withColumn("networth_1_dum", F.when(F.col("IBX_NETWORTH_PREMIER_AGG_HHD").isin('6', '7', '8', '9'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("HH_pct_Spanish_Speaking_c", F.when(F.col("CENS_LANG_HH_PERCENT_SPANISH_SPE").isNull(), 91).otherwise(F.col("CENS_LANG_HH_PERCENT_SPANISH_SPE") * 10))
df_work_jobs_score = df_work_jobs_score.withColumn("gender_M", F.when(F.col("gender_input") == 'M', 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("cat_r_dum", F.when(F.col("category") == 'R', 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("aarporg_i", F.when(F.col("aarporg_i").isNull(), 0).otherwise(F.col("aarporg_i")))
df_work_jobs_score = df_work_jobs_score.withColumn("deadwood_dum", F.when(F.col("DEADWOOD_MODEL").isin('NOTDEAD'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("sy_otsbn_polfund_2012b_c", F.when(F.col("sy_otsbn_polfund_2012b").isNull(), 33).otherwise(F.col("sy_otsbn_polfund_2012b")))
df_work_jobs_score = df_work_jobs_score.withColumn("SY_GUNSCORE_c", F.when(F.col("GUN_OWNERSHIP_MODEL").isNull(), 0.373).otherwise(F.col("GUN_OWNERSHIP_MODEL")))
df_work_jobs_score = df_work_jobs_score.withColumn("CENS_COMMUTE_WRKRS_PERCENT_DRO_c", F.when(F.col("CENS_AGE_POP_PERCENT_45_54").isNull(), 14.1).otherwise(F.col("CENS_AGE_POP_PERCENT_45_54")))
df_work_jobs_score = df_work_jobs_score.withColumn("PERCENTCAUCASIANANDOTHER_c", F.when(F.col("CENS_ETHNIC_POP_PERCENT_WHITE_ON").isNull(), 80).otherwise(F.col("CENS_ETHNIC_POP_PERCENT_WHITE_ON")))
df_work_jobs_score = df_work_jobs_score.withColumn("PERCENTUNEMPLOYED_c", F.when(F.col("CENS_EMPLOY_LABF_PERCENT_UNEMPLO").isNull(), 4.2).otherwise(F.col("CENS_EMPLOY_LABF_PERCENT_UNEMPLO")))
df_work_jobs_score = df_work_jobs_score.withColumn("lifestage_678", F.when(F.col("Lifestage_Segment").isin('6', '7', '8'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("lifestage_123", F.when(F.col("Lifestage_Segment").isin('1', '2', '3'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("Past12MoTouchCt_Financial2", F.when(F.col("Past12MoTouchCt_Financial").isin('1', '2', '3'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("GeneralElectn2012_AM", F.when(F.col("GeneralElectn2012").isin('A', 'M'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("num_click", F.when(F.col("num_click").isNull(), 0).otherwise(F.col("num_click")))
df_work_jobs_score = df_work_jobs_score.withColumn("mailct_all", F.when(F.col("mailct_all").isNull(), 0).otherwise(F.col("mailct_all")))
df_work_jobs_score = df_work_jobs_score.withColumn("num_open_6mo", F.when(F.col("num_open_6mo").isNull(), 0).otherwise(F.col("num_open_6mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("click_rate_6mo", F.when(F.col("click_rate_6mo").isNull(), 0).otherwise(F.col("click_rate_6mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("mailercount_click_6mo", F.when(F.col("mailercount_click_6mo").isNull(), 0).otherwise(F.col("mailercount_click_6mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("mailercount_open_6mo", F.when(F.col("mailercount_open_6mo").isNull(), 0).otherwise(F.col("mailercount_open_6mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("cruise_y", F.when(F.col("IBX_TRAVEL_CRUISE_AGG_HHD") == '1', 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("IBX_HEALTHY_BEHAVIOUR_HHD_1dum", F.when(F.col("IBX_HEALTHY_BEHAVIOUR_AGG_HHD") == '1', 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("PERCENT_CIVILIAN_VET", F.when(F.col("CENS_EMPLOY_POP18_PLUS_PERCENT_C").isNull(), 8.9).otherwise(F.col("CENS_EMPLOY_POP18_PLUS_PERCENT_C")))
df_work_jobs_score = df_work_jobs_score.withColumn("homevalue_9to10_dum", F.when(F.col("IBX_HOME_MARKET_VALUE_DECILES_AG").isin('09', '10'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("community_religi_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_RELIGI") == 1, 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("click_rate_3_6mos", F.when(F.col("click_rate_3_6mos").isNull(), 0).otherwise(F.col("click_rate_3_6mos")))
df_work_jobs_score = df_work_jobs_score.withColumn("mailercount_click_30days", F.when(F.col("mailercount_click_30days").isNull(), 0).otherwise(F.col("mailercount_click_30days")))
df_work_jobs_score = df_work_jobs_score.withColumn("mailercount_click_3_6mo", F.when(F.col("mailercount_click_3_6mo").isNull(), 0).otherwise(F.col("mailercount_click_3_6mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("mailercount_open_30days", F.when(F.col("mailercount_open_30days").isNull(), 0).otherwise(F.col("mailercount_open_30days")))
df_work_jobs_score = df_work_jobs_score.withColumn("mailercount_open_3_6mo", F.when(F.col("mailercount_open_3_6mo").isNull(), 0).otherwise(F.col("mailercount_open_3_6mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("mailercount_open_1_3mo", F.when(F.col("mailercount_open_1_3mo").isNull(), 0).otherwise(F.col("mailercount_open_1_3mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("num_clicked_1_3mo", F.when(F.col("num_clicked_1_3mo").isNull(), 0).otherwise(F.col("num_clicked_1_3mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("num_clicked_curmonth", F.when(F.col("num_clicked_curmonth").isNull(), 0).otherwise(F.col("num_clicked_curmonth")))
df_work_jobs_score = df_work_jobs_score.withColumn("num_clicked_3_6mo", F.when(F.col("num_clicked_3_6mo").isNull(), 0).otherwise(F.col("num_clicked_3_6mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("num_ib_1_3mo", F.when(F.col("num_ib_1_3mo").isNull(), 0).otherwise(F.col("num_ib_1_3mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("num_ib_30days", F.when(F.col("num_ib_30days").isNull(), 0).otherwise(F.col("num_ib_30days")))
df_work_jobs_score = df_work_jobs_score.withColumn("num_ib_3_6mo", F.when(F.col("num_ib_3_6mo").isNull(), 0).otherwise(F.col("num_ib_3_6mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("num_open_1_3mo", F.when(F.col("num_open_1_3mo").isNull(), 0).otherwise(F.col("num_open_1_3mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("num_open_3_6mo", F.when(F.col("num_open_3_6mo").isNull(), 0).otherwise(F.col("num_open_3_6mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("num_open_30days", F.when(F.col("num_open_30days").isNull(), 0).otherwise(F.col("num_open_30days")))
df_work_jobs_score = df_work_jobs_score.withColumn("mailercount_click_1_3mo", F.when(F.col("mailercount_click_1_3mo").isNull(), 0).otherwise(F.col("mailercount_click_1_3mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("state_event_12mo", F.when(F.col("state_event_12mo").isNull(), 0).otherwise(F.col("state_event_12mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("individual_engagers_12mo", F.when(F.col("individual_engagers_12mo").isNull(), 0).otherwise(F.col("individual_engagers_12mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("structured_12mo", F.when(F.col("structured_12mo").isNull(), 0).otherwise(F.col("structured_12mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("moviesfg_12mo", F.when(F.col("moviesfg_12mo").isNull(), 0).otherwise(F.col("moviesfg_12mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("state_event_12mo_i", F.when(F.col("state_event_12mo_i").isNull(), 0).otherwise(F.col("state_event_12mo_i")))
df_work_jobs_score = df_work_jobs_score.withColumn("structured_12mo_i", F.when(F.col("structured_12mo_i").isNull(), 0).otherwise(F.col("structured_12mo_i")))
df_work_jobs_score = df_work_jobs_score.withColumn("survey_resp_12mo", F.when(F.col("survey_resp_12mo").isNull(), 0).otherwise(F.col("survey_resp_12mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("contact_leg_12mo", F.when(F.col("contact_leg_12mo").isNull(), 0).otherwise(F.col("contact_leg_12mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("moviesfg_12mo_i", F.when(F.col("moviesfg_12mo_i").isNull(), 0).otherwise(F.col("moviesfg_12mo_i")))
df_work_jobs_score = df_work_jobs_score.withColumn("states_vol_12mo", F.when(F.col("states_vol_12mo").isNull(), 0).otherwise(F.col("states_vol_12mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("states_vol_12mo_i", F.when(F.col("states_vol_12mo_i").isNull(), 0).otherwise(F.col("states_vol_12mo_i")))
df_work_jobs_score = df_work_jobs_score.withColumn("state_activities_12mo", F.when(F.col("state_activities_12mo").isNull(), 0).otherwise(F.col("state_activities_12mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("newsletter_opens_cnt_12mo", F.when(F.col("newsletter_opens_cnt_12mo").isNull(), 0).otherwise(F.col("newsletter_opens_cnt_12mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("DENSITY_CLUSTERS_1_2_3", F.when(F.col("DENSITY_CLUSTERS").isin('1', '2', '3'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("ADVO_SEGMENT_CD_S3_S4_S5", F.when(F.col("ADVO_SEGMENT_CD").isin('S3', 'S4', 'S5'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("PC_Owner_Y", F.when(F.col("ibx_pc_owner_premier") == 'Y', 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("INTERNET__8_10", F.when(F.col("IBX_TRENDS_FOR_TELECOM_INTERNET_").isin('08', '10'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("INTERNET__1", F.when(F.col("IBX_TRENDS_FOR_TELECOM_INTERNET_").isin('01'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("INTERNET__2_3_4", F.when(F.col("IBX_TRENDS_FOR_TELECOM_INTERNET_").isin('02', '03', '04'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("NEW_SCORE16_CENTILE_c", F.coalesce(F.col("NEW_SCORE16_CENTILE"), F.lit(27)))
df_work_jobs_score = df_work_jobs_score.withColumn("NEW_SCORE24_CENTILE", F.when(F.col("NEW_SCORE24_CENTILE").isNull(), 24).otherwise(F.col("NEW_SCORE24_CENTILE")))
df_work_jobs_score = df_work_jobs_score.withColumn("NEW_SCORE25_CENTILE", F.when(F.col("NEW_SCORE25_CENTILE").isNull(), 47).otherwise(F.col("NEW_SCORE25_CENTILE")))
df_work_jobs_score = df_work_jobs_score.withColumn("live_answer_aft", F.when(F.col("live_answer_aft").isNull(), 62).otherwise(F.col("live_answer_aft")))
df_work_jobs_score = df_work_jobs_score.withColumn("live_answer_pm_c", F.when(F.col("live_answer_pm").isNull(), 64).otherwise(F.col("live_answer_pm")))
df_work_jobs_score = df_work_jobs_score.withColumn("FNDN_Housing_c", F.coalesce(F.col("FNDN_Housing"), F.lit(42)))
df_work_jobs_score = df_work_jobs_score.withColumn("Auto_Buying_EM", F.when(F.col("Auto_Buying_EM").isNull(), 75).otherwise(F.col("Auto_Buying_EM")))
df_work_jobs_score = df_work_jobs_score.withColumn("IPL_CARE_DM_REG_c", F.coalesce(F.col("IPL_CARE_DM_REG"), F.lit(50)))
df_work_jobs_score = df_work_jobs_score.withColumn("IPL_job_DM_REG_c", F.coalesce(F.col("IPL_job_DM_REG"), F.lit(41)))
df_work_jobs_score = df_work_jobs_score.withColumn("em_clickrate_c", F.when(F.col("EM_Clickrate").isNull(), 50).otherwise(F.col("EM_Clickrate")))
df_work_jobs_score = df_work_jobs_score.withColumn("drvsafe_pro_em_c", F.when(F.col("drvsafe_pro_em").isNull(), 36).otherwise(F.col("drvsafe_pro_em")))
df_work_jobs_score = df_work_jobs_score.withColumn("fndn_pro_em_c", F.coalesce(F.col("fndn_pro_em"), F.lit(20)))
df_work_jobs_score = df_work_jobs_score.withColumn("work_jobs_em_c", F.coalesce(F.col("work_jobs_em"), F.lit(17)))
df_work_jobs_score = df_work_jobs_score.withColumn("GUN_OWNERSHIP_MODEL_c", F.coalesce(F.col("GUN_OWNERSHIP_MODEL"), F.lit(0.31)))
df_work_jobs_score = df_work_jobs_score.withColumn("CENS_COMMUTE_WRKRS_PERCENT_P_c", F.coalesce(F.col("CENS_COMMUTE_WRKRS_PERCENT_PUBLI"), F.lit(0.6)))
df_work_jobs_score = df_work_jobs_score.withColumn("CENS_MARR_POP15_PLUS_PERCENT_NEV", F.when(F.col("CENS_MARR_POP15_PLUS_PERCENT_NEV").isNull(), 28.4).otherwise(F.col("CENS_MARR_POP15_PLUS_PERCENT_NEV")))
df_work_jobs_score = df_work_jobs_score.withColumn("click_rate_1_3mos", F.when((F.col("num_open_1_3mo") == 0) | F.col("num_open_1_3mo").isNull(), 0).otherwise(F.col("num_clicked_1_3mo") / F.col("num_open_1_3mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("click_rate_1_3mos", F.when(F.col("click_rate_1_3mos").isNull(), 0).otherwise(F.col("click_rate_1_3mos")))
df_work_jobs_score = df_work_jobs_score.withColumn("click_rate_1mos", F.when((F.col("num_open_30days") == 0) | F.col("num_open_30days").isNull(), 0).otherwise(F.col("num_clicked_curmonth") / F.col("num_open_30days")))
df_work_jobs_score = df_work_jobs_score.withColumn("click_rate_1mos", F.when(F.col("click_rate_1mos").isNull(), 0).otherwise(F.col("click_rate_1mos")))
df_work_jobs_score = df_work_jobs_score.withColumn("click_rate_3_6mos", F.when((F.col("num_open_3_6mo") == 0) | F.col("num_open_3_6mo").isNull(), 0).otherwise(F.col("num_clicked_3_6mo") / F.col("num_open_3_6mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("click_rate_3_6mos", F.when(F.col("click_rate_3_6mos").isNull(), 0).otherwise(F.col("click_rate_3_6mos")))
df_work_jobs_score = df_work_jobs_score.withColumn("state_activity_12mo_i", F.when(F.col("state_activity_12mo_i").isNull(), 0).otherwise(F.col("state_activity_12mo_i")))
df_work_jobs_score = df_work_jobs_score.withColumn("work_jobs_attendee_em_logit", 
    F.col("NEW_SCORE16_CENTILE_c") * -0.00384710301423336 +
    F.col("NEW_SCORE24_CENTILE") * -0.0246779396694745 +
    F.col("NEW_SCORE25_CENTILE") * -0.00565000503777539 +
    F.col("live_answer_aft") * -0.00311957819045753 +
    F.col("live_answer_pm") * -0.0201105417805634 +
    F.col("FNDN_Housing_c") * -0.0143514106438761 +
    F.col("Auto_Buying_EM") * -0.00938036651265641 +
    F.col("IPL_CARE_DM_REG_c") * 0.00672197001729598 +
    F.col("IPL_job_DM_REG_c") * -0.00881575682938203 +
    F.col("em_clickrate_c") * -0.017904288525954 +
    F.col("drvsafe_pro_em_c") * -0.000572316294374849 +
    F.col("fndn_pro_em_c") * -0.00588719785522563 +
    F.col("work_jobs_em_c") * -0.0156443832924109 +
    F.col("GUN_OWNERSHIP_MODEL_c") * -0.829377917908508 +
    F.col("CENS_COMMUTE_WRKRS_PERCENT_P_c") * 0.00198431776963286 +
    F.col("CENS_MARR_POP15_PLUS_PERCENT_NEV") * -0.00604936296734162 +
    F.col("num_ib_30days") * -0.000757405589866533 +
    F.col("num_clicked_curmonth") * -0.0382843336334235 +
    F.col("num_open_30days") * 0.00175680757952443 +
    F.col("click_rate_1mos") * 0.0621029064685251 +
    F.col("mailercount_click_30days") * 0.149622792965448 +
    F.col("mailercount_open_30days") * 0.0567319556075903 +
    F.col("num_ib_1_3mo") * 0.0118408371893592 +
    F.col("num_clicked_1_3mo") * 0.00475307120020135 +
    F.col("mailercount_click_1_3mo") * 0.0310434908063198 +
    F.col("num_open_1_3mo") * 0.0147475464025559 +
    F.col("mailercount_open_1_3mo") * -0.072981964435864 +
    F.col("click_rate_1_3mos") * -0.0325088638035665 +
    F.col("num_ib_3_6mo") * -0.0036410097453186 +
    F.col("num_clicked_3_6mo") * -0.00215687330557026 +
    F.col("mailercount_click_3_6mo") * -0.0765720648843344 +
    F.col("num_open_3_6mo") * -0.00445814926120152 +
    F.col("mailercount_open_3_6mo") * -0.0492238738722516 +
    F.col("click_rate_3_6mos") * -0.01
0134077677395812 +
    F.col("state_activities_12mo") * -0.0633283230959882 +
    F.col("state_activity_12mo_i") * 0.993306847608114 +
    F.col("state_event_12mo") * 0.414223452823489 +
    F.col("individual_engagers_12mo") * -0.158287052945086 +
    F.col("structured_12mo") * -4.08215929957436 +
    F.col("moviesfg_12mo") * 0.528472833978728 +
    F.col("state_event_12mo_i") * -0.100561229051113 +
    F.col("structured_12mo_i") * 5.41456331400332 +
    F.col("survey_resp_12mo") * 0.0987218377861994 +
    F.col("newsletter_opens_cnt_12mo") * 0.00437521603361079 +
    F.col("contact_leg_12mo") * 0.0144515347778664 +
    F.col("moviesfg_12mo_i") * -0.861881715434179 +
    F.col("states_vol_12mo") * -3.39227034242535 +
    F.col("states_vol_12mo_i") * 3.88897609235363 +
    F.col("DENSITY_CLUSTERS_1_2_3") * 0.146661685168876 +
    F.col("ADVO_SEGMENT_CD_S3_S4_S5") * 0.457938414150794 +
    F.col("PC_Owner_Y") * 0.637492169169348 +
    F.col("INTERNET__8_10") * -0.566086845655218 +
    F.col("INTERNET__1") * 0.0448979074658065 +
    F.col("INTERNET__2_3_4") * 0.0414439838257837
)
df_work_jobs_score = df_work_jobs_score.withColumn("work_jobs_attendee_em_score", F.exp(F.col("work_jobs_attendee_em_logit")) / (F.lit(1) + F.exp(F.col("work_jobs_attendee_em_logit"))))
df_work_jobs_score = df_work_jobs_score.select("mid_key", "work_jobs_attendee_em_score")
df_work_jobs_score.write.format("delta").mode("overwrite").saveAsTable("intermed.work_jobs_score")

df_work_jobs_score = spark.table("intermed.work_jobs_score")
df_work_jobs_score_with_dummy = df_work_jobs_score.withColumn("dummy_partition", F.lit(1))
window_work_jobs = Window.partitionBy("dummy_partition").orderBy(F.col("work_jobs_attendee_em_score").desc())
df_score_ranks = df_work_jobs_score_with_dummy.withColumn("work_jobs_attendee_em", F.ntile(99).over(window_work_jobs)).drop("dummy_partition")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_geo_appends_rpm = df_geo_appends_rpm.withColumn("mid_key", F.col("mid_key").cast(IntegerType()))
df_score_ranks = df_score_ranks.withColumnRenamed("mid_key", "mid_key_b")
df_geo_appends_rpm_updated = df_geo_appends_rpm.alias("a").join(
    df_score_ranks.alias("b"),
    F.col("a.mid_key") == F.col("b.mid_key_b"),
    "left"
).select(
    (F.col("b.work_jobs_attendee_em") + 1).alias("work_jobs_attendee_em"),
    "a.*"
).drop("mid_key_b")
df_geo_appends_rpm_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_contact_history_sum = spark.table("intermed.contact_history_sum")
df_geo_cd_2010_table = spark.table("geo_cd_2010_table")
df_majorgifts_fortim = spark.table("weiss.majorgifts_fortim")
df_democurr = spark.table(f"aarpdata.{democurr}")

df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_contact_history_sum.createOrReplaceTempView("contact_history_sum")
df_geo_cd_2010_table.createOrReplaceTempView("geo_cd_2010_table")
df_majorgifts_fortim.createOrReplaceTempView("majorgifts_fortim")
df_democurr.createOrReplaceTempView("democurr")

df_analysis3 = spark.sql("""
    SELECT
        a.*,
        b.lgbtq_dotorg_visitor,
        c.live_answer_ct,
        c.liveanswer_comp_freq3_6,
        c.liveanswer_comp_freq6_12,
        c.liveanswer_comp_freq_3,
        c.mailercount_click_1_3mo,
        c.pct_live,
        c.liveanswer_freq3_6,
        c.mailercount_click_30days,
        c.num_clicked_curmonth,
        c.num_clicked_3_6mo,
        c.num_inb_30days,
        c.num_ib_3_6mo,
        c.num_open_30days,
        c.num_open_3_6mo,
        c.liveanswer_freq_3,
        c.mailercount_open_1_3mo,
        c.liveanswer_freq6_12,
        c.pct_live2,
        d.geo_cd_2010_mean,
        e.majorgiftmidkey,
        demo.cooking_gourmet,
        demo.sewing_knitting_needlework
    FROM
        intermed.geo_appends_rpm a
    LEFT JOIN
        intermed.web_visits b ON a.mid_key = b.mid_key
    LEFT JOIN
        intermed.contact_history_sum c ON a.mid_key = c.mid_key
    LEFT JOIN
        geo_cd_2010_table d ON a.geo_cd_2010 = d.geo_cd_2010
    LEFT JOIN
        majorgifts_fortim e ON a.mid_key = e.mid_key
    LEFT JOIN
        democurr demo ON a.mid_key = demo.mid_key
""")
df_analysis3.write.option("compression", "snappy").format("delta").mode("overwrite").saveAsTable("intermed.analysis3")

df_analysis3 = spark.table("intermed.analysis3")
df_fraud_tth = df_analysis3
df_fraud_tth.write.format("delta").mode("overwrite").saveAsTable("intermed.fraud_tth")

df_fraud_tth = spark.table("intermed.fraud_tth")
df_fraud_tth_with_dummy = df_fraud_tth.withColumn("dummy_partition", F.lit(1))
window_spec = Window.partitionBy("dummy_partition")
df_score_ranks = df_fraud_tth_with_dummy.withColumn("fraud_tth", F.ntile(99).over(window_spec.orderBy(F.col("fraud_tth_score").desc())))
df_score_ranks = df_score_ranks.withColumn("covid19_tth", F.ntile(99).over(window_spec.orderBy(F.col("covid19_tth_score").desc())))
df_score_ranks = df_score_ranks.withColumn("LGBTQ_ally", F.ntile(99).over(window_spec.orderBy(F.col("lgbtq_ally_score").desc())))
df_score_ranks = df_score_ranks.withColumn("speakers_bureau_vol", F.ntile(99).over(window_spec.orderBy(F.col("speakers_bureau_vol_score").desc())))
df_score_ranks = df_score_ranks.withColumn("volunteer_leaders", F.ntile(99).over(window_spec.orderBy(F.col("volunteer_leaders_score").desc())))
df_score_ranks = df_score_ranks.withColumn("Major_Gifts", F.ntile(99).over(window_spec.orderBy(F.col("Major_Gifts_score").desc())))
df_score_ranks = df_score_ranks.withColumn("finra_pckt", F.ntile(99).over(window_spec.orderBy(F.col("finra_pckt_score").desc())))
df_score_ranks = df_score_ranks.withColumn("advo_dm_65plus", F.ntile(99).over(window_spec.orderBy(F.col("advo_dm_65plus_score").desc())))
df_score_ranks = df_score_ranks.withColumn("ttar_em", F.ntile(99).over(window_spec.orderBy(F.col("ttar_em_score").desc())))
df_score_ranks = df_score_ranks.withColumn("ttar_dm", F.ntile(99).over(window_spec.orderBy(F.col("ttar_dm_score").desc())))
df_score_ranks = df_score_ranks.withColumn("covid_vacc_tth", F.ntile(99).over(window_spec.orderBy(F.col("covid_vacc_tth_score").desc())))
df_score_ranks = df_score_ranks.drop("dummy_partition")
df_score_ranks.write.format("delta").mode("overwrite").saveAsTable("intermed.score_ranks")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_score_ranks = spark.table("intermed.score_ranks")
df_fraud_tth = spark.table("intermed.fraud_tth")
df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_score_ranks.createOrReplaceTempView("score_ranks")
df_fraud_tth.createOrReplaceTempView("fraud_tth")

df_geo_appends_rpm_updated = spark.sql("""
    SELECT
        b.fraud_tth + 1 AS fraud_tth,
        b.covid19_tth + 1 AS covid19_tth,
        b.LGBTQ_ally + 1 AS LGBTQ_ally,
        b.speakers_bureau_vol + 1 AS speakers_bureau_vol,
        b.volunteer_leaders + 1 AS volunteer_leaders,
        c.DAPM_Catalist_Ideology,
        b.Major_Gifts + 1 AS Major_Gifts,
        b.finra_pckt + 1 AS Finra_Pckt,
        b.advo_dm_65plus + 1 AS advo_dm_65plus,
        b.ttar_em + 1 AS ttar_em,
        b.ttar_dm + 1 AS ttar_dm,
        b.covid_vacc_tth + 1 AS covid_vacc_tth,
        a.*
    FROM
        intermed.geo_appends_rpm AS a
    LEFT JOIN
        intermed.score_ranks AS b ON a.mid_key = b.mid_key
    LEFT JOIN
        intermed.fraud_tth c ON a.mid_key = c.mid_key
""")
df_geo_appends_rpm_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
columns_to_check_final_report = [
    "drvsafe_pro_em", "lo_medicaid", "work_jobs_em", "nps_detractor", "caregiving_em", "save_plan",
    "driver_safety_tek_dm", "driver_safety_tek_em", "cpd_tek_attend", "fndn_pro_em", "lo_medicare",
    "cpd_tek_attend", "drvsafe_pro_dm", "Medicare_em_attend", "work_jobs_attendee_em", "caregiving_em_attend",
    "medicare_tth", "soc_sec_em", "soc_sec_em_att", "rx_advo_65plus", "rx_advo_50_64", "rpm2019_centile",
    "rpm2019_prob", "fndn_em_proseng", "fndn_aarppro_dm", "fraud_click_em", "ipl_care_dm_reg", "ipl_job_dm_reg",
    "ipl_TEch_dm_reg", "ipl_care_em", "ipl_job_em", "ipl_TEch_em", "fraud_tth", "advo_dm_50_64",
    "covid19_tth", "LGBTQ_ally", "speakers_bureau_vol", "volunteer_leaders", "DAPM_Catalist_Ideology",
    "Major_Gifts", "Finra_Pckt", "advo_dm_65plus", "ttar_em", "ttar_dm", "covid_vacc_tth"
]
for col_name in columns_to_check_final_report:
    print(f"Frequency for {col_name}:")
    df_geo_appends_rpm.groupBy(col_name).count().orderBy(col_name).show(truncate=False)

df_analysis3 = spark.table("intermed.analysis3")
df_work_jobs_score = df_analysis3
df_work_jobs_score = df_work_jobs_score.withColumn("NEW_SCORE4_CENTILE", F.when(F.col("NEW_SCORE4_CENTILE").isNull(), 99).otherwise(F.col("NEW_SCORE4_CENTILE")))
df_work_jobs_score = df_work_jobs_score.withColumn("NEW_SCORE40_CENTILE", F.when(F.col("NEW_SCORE40_CENTILE").isNull(), 99).otherwise(F.col("NEW_SCORE40_CENTILE")))
df_work_jobs_score = df_work_jobs_score.withColumn("em_clickrate_c", F.when(F.col("em_clickrate").isNull(), 99).otherwise(F.col("em_clickrate")))
df_work_jobs_score = df_work_jobs_score.withColumn("IPL_CARE_DM_REG_c", F.coalesce(F.col("IPL_CARE_DM_REG"), F.lit(99)))
df_work_jobs_score = df_work_jobs_score.withColumn("age_60to66_dum", F.when((F.col("age_agg_ind") >= 60) & (F.col("age_agg_ind") <= 66), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("age_gt75_dum", F.when(F.col("age_agg_ind") >= 75, 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("NEW_SCORE46_CENTILE_c", F.coalesce(F.col("NEW_SCORE46_CENTILE"), F.lit(99)))
df_work_jobs_score = df_work_jobs_score.withColumn("R_number_of_lines_of_credit", F.when(F.col("IBX_NUM_OF_LINES_OF_CREDIT").isin('1', '2', '3', '4', '7'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("VOTEPROP2016_c", F.when(F.col("general_election_vote_propensity").isNull(), 83.8246349).otherwise(F.col("general_election_vote_propensity")))
df_work_jobs_score = df_work_jobs_score.withColumn("ideology_c", F.when(F.col("ideology").isNull(), 44).otherwise(F.col("ideology")))
df_work_jobs_score = df_work_jobs_score.withColumn("petadv12_c", F.when(F.col("petadv12") == 1, 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("IBX_ADULTS_NUM_AGG_HHDls3", F.when(F.col("IBX_ADULTS_NUM_AGG_HHD").isin('1', '6'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("elderly_u_dum", F.when(F.col("IBX_ELDERLY_PARENT_AGG_HHD") == 'Y', 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("CENS_GRPQTRS_POP_PERCENT_MIL_c", F.when(F.col("CENS_GRPQTRS_POP_PERCENT_MILITAR").isNull(), 0.01).otherwise(F.col("CENS_GRPQTRS_POP_PERCENT_MILITAR")))
df_work_jobs_score = df_work_jobs_score.withColumn("networth_1_dum", F.when(F.col("IBX_NETWORTH_PREMIER_AGG_HHD").isin('6', '7', '8', '9'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("HH_pct_Spanish_Speaking_c", F.when(F.col("CENS_LANG_HH_PERCENT_SPANISH_SPE").isNull(), 91).otherwise(F.col("CENS_LANG_HH_PERCENT_SPANISH_SPE") * 10))
df_work_jobs_score = df_work_jobs_score.withColumn("gender_M", F.when(F.col("gender_input") == 'M', 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("cat_r_dum", F.when(F.col("category") == 'R', 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("aarporg_i", F.when(F.col("aarporg_i").isNull(), 0).otherwise(F.col("aarporg_i")))
df_work_jobs_score = df_work_jobs_score.withColumn("deadwood_dum", F.when(F.col("DEADWOOD_MODEL").isin('NOTDEAD'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("sy_otsbn_polfund_2012b_c", F.when(F.col("sy_otsbn_polfund_2012b").isNull(), 33).otherwise(F.col("sy_otsbn_polfund_2012b")))
df_work_jobs_score = df_work_jobs_score.withColumn("SY_GUNSCORE_c", F.when(F.col("GUN_OWNERSHIP_MODEL").isNull(), 0.373).otherwise(F.col("GUN_OWNERSHIP_MODEL")))
df_work_jobs_score = df_work_jobs_score.withColumn("CENS_COMMUTE_WRKRS_PERCENT_DRO_c", F.when(F.col("CENS_AGE_POP_PERCENT_45_54").isNull(), 14.1).otherwise(F.col("CENS_AGE_POP_PERCENT_45_54")))
df_work_jobs_score = df_work_jobs_score.withColumn("PERCENTCAUCASIANANDOTHER_c", F.when(F.col("CENS_ETHNIC_POP_PERCENT_WHITE_ON").isNull(), 80).otherwise(F.col("CENS_ETHNIC_POP_PERCENT_WHITE_ON")))
df_work_jobs_score = df_work_jobs_score.withColumn("PERCENTUNEMPLOYED_c", F.when(F.col("CENS_EMPLOY_LABF_PERCENT_UNEMPLO").isNull(), 4.2).otherwise(F.col("CENS_EMPLOY_LABF_PERCENT_UNEMPLO")))
df_work_jobs_score = df_work_jobs_score.withColumn("lifestage_678", F.when(F.col("Lifestage_Segment").isin('6', '7', '8'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("lifestage_123", F.when(F.col("Lifestage_Segment").isin('1', '2', '3'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("Past12MoTouchCt_Financial2", F.when(F.col("Past12MoTouchCt_Financial").isin('1', '2', '3'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("GeneralElectn2012_AM", F.when(F.col("GeneralElectn2012").isin('A', 'M'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("num_click", F.when(F.col("num_click").isNull(), 0).otherwise(F.col("num_click")))
df_work_jobs_score = df_work_jobs_score.withColumn("mailct_all", F.when(F.col("mailct_all").isNull(), 0).otherwise(F.col("mailct_all")))
df_work_jobs_score = df_work_jobs_score.withColumn("num_open_6mo", F.when(F.col("num_open_6mo").isNull(), 0).otherwise(F.col("num_open_6mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("click_rate_6mo", F.when(F.col("click_rate_6mo").isNull(), 0).otherwise(F.col("click_rate_6mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("mailercount_click_6mo", F.when(F.col("mailercount_click_6mo").isNull(), 0).otherwise(F.col("mailercount_click_6mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("mailercount_open_6mo", F.when(F.col("mailercount_open_6mo").isNull(), 0).otherwise(F.col("mailercount_open_6mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("cruise_y", F.when(F.col("IBX_TRAVEL_CRUISE_AGG_HHD") == '1', 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("IBX_HEALTHY_BEHAVIOUR_HHD_1dum", F.when(F.col("IBX_HEALTHY_BEHAVIOUR_AGG_HHD") == '1', 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("PERCENT_CIVILIAN_VET", F.when(F.col("CENS_EMPLOY_POP18_PLUS_PERCENT_C").isNull(), 8.9).otherwise(F.col("CENS_EMPLOY_POP18_PLUS_PERCENT_C")))
df_work_jobs_score = df_work_jobs_score.withColumn("homevalue_9to10_dum", F.when(F.col("IBX_HOME_MARKET_VALUE_DECILES_AG").isin('09', '10'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("community_religi_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_RELIGI") == 1, 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("click_rate_3_6mos", F.when(F.col("click_rate_3_6mos").isNull(), 0).otherwise(F.col("click_rate_3_6mos")))
df_work_jobs_score = df_work_jobs_score.withColumn("mailercount_click_30days", F.when(F.col("mailercount_click_30days").isNull(), 0).otherwise(F.col("mailercount_click_30days")))
df_work_jobs_score = df_work_jobs_score.withColumn("mailercount_click_3_6mo", F.when(F.col("mailercount_click_3_6mo").isNull(), 0).otherwise(F.col("mailercount_click_3_6mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("mailercount_open_30days", F.when(F.col("mailercount_open_30days").isNull(), 0).otherwise(F.col("mailercount_open_30days")))
df_work_jobs_score = df_work_jobs_score.withColumn("mailercount_open_3_6mo", F.when(F.col("mailercount_open_3_6mo").isNull(), 0).otherwise(F.col("mailercount_open_3_6mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("mailercount_open_1_3mo", F.when(F.col("mailercount_open_1_3mo").isNull(), 0).otherwise(F.col("mailercount_open_1_3mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("num_clicked_1_3mo", F.when(F.col("num_clicked_1_3mo").isNull(), 0).otherwise(F.col("num_clicked_1_3mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("num_clicked_curmonth", F.when(F.col("num_clicked_curmonth").isNull(), 0).otherwise(F.col("num_clicked_curmonth")))
df_work_jobs_score = df_work_jobs_score.withColumn("num_clicked_3_6mo", F.when(F.col("num_clicked_3_6mo").isNull(), 0).otherwise(F.col("num_clicked_3_6mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("num_ib_1_3mo", F.when(F.col("num_ib_1_3mo").isNull(), 0).otherwise(F.col("num_ib_1_3mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("num_ib_30days", F.when(F.col("num_ib_30days").isNull(), 0).otherwise(F.col("num_ib_30days")))
df_work_jobs_score = df_work_jobs_score.withColumn("num_ib_3_6mo", F.when(F.col("num_ib_3_6mo").isNull(), 0).otherwise(F.col("num_ib_3_6mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("num_open_1_3mo", F.when(F.col("num_open_1_3mo").isNull(), 0).otherwise(F.col("num_open_1_3mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("num_open_3_6mo", F.when(F.col("num_open_3_6mo").isNull(), 0).otherwise(F.col("num_open_3_6mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("num_open_30days", F.when(F.col("num_open_30days").isNull(), 0).otherwise(F.col("num_open_30days")))
df_work_jobs_score = df_work_jobs_score.withColumn("mailercount_click_1_3mo", F.when(F.col("mailercount_click_1_3mo").isNull(), 0).otherwise(F.col("mailercount_click_1_3mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("state_event_12mo", F.when(F.col("state_event_12mo").isNull(), 0).otherwise(F.col("state_event_12mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("individual_engagers_12mo", F.when(F.col("individual_engagers_12mo").isNull(), 0).otherwise(F.col("individual_engagers_12mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("structured_12mo", F.when(F.col("structured_12mo").isNull(), 0).otherwise(F.col("structured_12mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("moviesfg_12mo", F.when(F.col("moviesfg_12mo").isNull(), 0).otherwise(F.col("moviesfg_12mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("state_event_12mo_i", F.when(F.col("state_event_12mo_i").isNull(), 0).otherwise(F.col("state_event_12mo_i")))
df_work_jobs_score = df_work_jobs_score.withColumn("structured_12mo_i", F.when(F.col("structured_12mo_i").isNull(), 0).otherwise(F.col("structured_12mo_i")))
df_work_jobs_score = df_work_jobs_score.withColumn("survey_resp_12mo", F.when(F.col("survey_resp_12mo").isNull(), 0).otherwise(F.col("survey_resp_12mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("contact_leg_12mo", F.when(F.col("contact_leg_12mo").isNull(), 0).otherwise(F.col("contact_leg_12mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("moviesfg_12mo_i", F.when(F.col("moviesfg_12mo_i").isNull(), 0).otherwise(F.col("moviesfg_12mo_i")))
df_work_jobs_score = df_work_jobs_score.withColumn("states_vol_12mo", F.when(F.col("states_vol_12mo").isNull(), 0).otherwise(F.col("states_vol_12mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("states_vol_12mo_i", F.when(F.col("states_vol_12mo_i").isNull(), 0).otherwise(F.col("states_vol_12mo_i")))
df_work_jobs_score = df_work_jobs_score.withColumn("state_activities_12mo", F.when(F.col("state_activities_12mo").isNull(), 0).otherwise(F.col("state_activities_12mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("newsletter_opens_cnt_12mo", F.when(F.col("newsletter_opens_cnt_12mo").isNull(), 0).otherwise(F.col("newsletter_opens_cnt_12mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("DENSITY_CLUSTERS_1_2_3", F.when(F.col("DENSITY_CLUSTERS").isin('1', '2', '3'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("ADVO_SEGMENT_CD_S3_S4_S5", F.when(F.col("ADVO_SEGMENT_CD").isin('S3', 'S4', 'S5'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("PC_Owner_Y", F.when(F.col("ibx_pc_owner_premier") == 'Y', 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("INTERNET__8_10", F.when(F.col("IBX_TRENDS_FOR_TELECOM_INTERNET_").isin('08', '10'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("INTERNET__1", F.when(F.col("IBX_TRENDS_FOR_TELECOM_INTERNET_").isin('01'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("INTERNET__2_3_4", F.when(F.col("IBX_TRENDS_FOR_TELECOM_INTERNET_").isin('02', '03', '04'), 1).otherwise(0))
df_work_jobs_score = df_work_jobs_score.withColumn("NEW_SCORE16_CENTILE_c", F.coalesce(F.col("NEW_SCORE16_CENTILE"), F.lit(27)))
df_work_jobs_score = df_work_jobs_score.withColumn("NEW_SCORE24_CENTILE", F.when(F.col("NEW_SCORE24_CENTILE").isNull(), 24).otherwise(F.col("NEW_SCORE24_CENTILE")))
df_work_jobs_score = df_work_jobs_score.withColumn("NEW_SCORE25_CENTILE", F.when(F.col("NEW_SCORE25_CENTILE").isNull(), 47).otherwise(F.col("NEW_SCORE25_CENTILE")))
df_work_jobs_score = df_work_jobs_score.withColumn("live_answer_aft", F.when(F.col("live_answer_aft").isNull(), 62).otherwise(F.col("live_answer_aft")))
df_work_jobs_score = df_work_jobs_score.withColumn("live_answer_pm_c", F.when(F.col("live_answer_pm").isNull(), 64).otherwise(F.col("live_answer_pm")))
df_work_jobs_score = df_work_jobs_score.withColumn("FNDN_Housing_c", F.coalesce(F.col("FNDN_Housing"), F.lit(42)))
df_work_jobs_score = df_work_jobs_score.withColumn("Auto_Buying_EM", F.when(F.col("Auto_Buying_EM").isNull(), 75).otherwise(F.col("Auto_Buying_EM")))
df_work_jobs_score = df_work_jobs_score.withColumn("IPL_CARE_DM_REG_c", F.coalesce(F.col("IPL_CARE_DM_REG"), F.lit(50)))
df_work_jobs_score = df_work_jobs_score.withColumn("IPL_job_DM_REG_c", F.coalesce(F.col("IPL_job_DM_REG"), F.lit(41)))
df_work_jobs_score = df_work_jobs_score.withColumn("em_clickrate_c", F.when(F.col("EM_Clickrate").isNull(), 50).otherwise(F.col("EM_Clickrate")))
df_work_jobs_score = df_work_jobs_score.withColumn("drvsafe_pro_em_c", F.when(F.col("drvsafe_pro_em").isNull(), 36).otherwise(F.col("drvsafe_pro_em")))
df_work_jobs_score = df_work_jobs_score.withColumn("fndn_pro_em_c", F.coalesce(F.col("fndn_pro_em"), F.lit(20)))
df_work_jobs_score = df_work_jobs_score.withColumn("work_jobs_em_c", F.coalesce(F.col("work_jobs_em"), F.lit(17)))
df_work_jobs_score = df_work_jobs_score.withColumn("GUN_OWNERSHIP_MODEL_c", F.coalesce(F.col("GUN_OWNERSHIP_MODEL"), F.lit(0.31)))
df_work_jobs_score = df_work_jobs_score.withColumn("CENS_COMMUTE_WRKRS_PERCENT_P_c", F.coalesce(F.col("CENS_COMMUTE_WRKRS_PERCENT_PUBLI"), F.lit(0.6)))
df_work_jobs_score = df_work_jobs_score.withColumn("CENS_MARR_POP15_PLUS_PERCENT_NEV", F.when(F.col("CENS_MARR_POP15_PLUS_PERCENT_NEV").isNull(), 28.4).otherwise(F.col("CENS_MARR_POP15_PLUS_PERCENT_NEV")))
df_work_jobs_score = df_work_jobs_score.withColumn("click_rate_1_3mos", F.when((F.col("num_open_1_3mo") == 0) | F.col("num_open_1_3mo").isNull(), 0).otherwise(F.col("num_clicked_1_3mo") / F.col("num_open_1_3mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("click_rate_1_3mos", F.when(F.col("click_rate_1_3mos").isNull(), 0).otherwise(F.col("click_rate_1_3mos")))
df_work_jobs_score = df_work_jobs_score.withColumn("click_rate_1mos", F.when((F.col("num_open_30days") == 0) | F.col("num_open_30days").isNull(), 0).otherwise(F.col("num_clicked_curmonth") / F.col("num_open_30days")))
df_work_jobs_score = df_work_jobs_score.withColumn("click_rate_1mos", F.when(F.col("click_rate_1mos").isNull(), 0).otherwise(F.col("click_rate_1mos")))
df_work_jobs_score = df_work_jobs_score.withColumn("click_rate_3_6mos", F.when((F.col("num_open_3_6mo") == 0) | F.col("num_open_3_6mo").isNull(), 0).otherwise(F.col("num_clicked_3_6mo") / F.col("num_open_3_6mo")))
df_work_jobs_score = df_work_jobs_score.withColumn("click_rate_3_6mos", F.when(F.col("click_rate_3_6mos").isNull(), 0).otherwise(F.col("click_rate_3_6mos")))
df_work_jobs_score = df_work_jobs_score.withColumn("state_activity_12mo_i", F.when(F.col("state_activity_12mo_i").isNull(), 0).otherwise(F.col("state_activity_12mo_i")))
df_work_jobs_score = df_work_jobs_score.withColumn("work_jobs_attendee_em_logit", 
    F.col("NEW_SCORE16_CENTILE_c") * -0.00384710301423336 +
    F.col("NEW_SCORE24_CENTILE") * -0.0246779396694745 +
    F.col("NEW_SCORE25_CENTILE") * -0.00565000503777539 +
    F.col("live_answer_aft") * -0.00311957819045753 +
    F.col("live_answer_pm") * -0.0201105417805634 +
    F.col("FNDN_Housing_c") * -0.0143514106438761 +
    F.col("Auto_Buying_EM") * -0.00938036651265641 +
    F.col("IPL_CARE_DM_REG_c") * 0.00672197001729598 +
    F.col("IPL_job_DM_REG_c") * -0.00881575682938203 +
    F.col("em_clickrate_c") * -0.017904288525954 +
    F.col("drvsafe_pro_em_c") * -0.000572316294374849 +
    F.col("fndn_pro_em_c") * -0.00588719785522563 +
    F.col("work_jobs_em_c") * -0.0156443832924109 +
    F.col("GUN_OWNERSHIP_MODEL_c") * -0.829377917908508 +
    F.col("CENS_COMMUTE_WRKRS_PERCENT_P_c") * 0.00198431776963286 +
    F.col("CENS_MARR_POP15_PLUS_PERCENT_NEV") * -0.00604936296734162 +
    F.col("num_ib_30days") * -0.000757405589866533 +
    F.col("num_clicked_curmonth") * -0.0382843336334235 +
    F.col("num_open_30days") * 0.00175680757952443 +
    F.col("click_rate_1mos") * 0.0621029064685251 +
    F.col("mailercount_click_30days") * 0.149622792965448 +
    F.col("mailercount_open_30days") * 0.0567319556075903 +
    F.col("num_ib_1_3mo") * 0.0118408371893592 +
    F.col("num_clicked_1_3mo") * 0.00475307120020135 +
    F.col("mailercount_click_1_3mo") * 0.0310434908063198 +
    F.col("num_open_1_3mo") * 0.0147475464025559 +
    F.col("mailercount_open_1_3mo") * -0.072981964435864 +
    F.col("click_rate_1_3mos") * -0.0325088638035665 +
    F.col("num_ib_3_6mo") * -0.0036410097453186 +
    F.col("num_clicked_3_6mo") * -0.00215687330557026 +
    F.col("mailercount_click_3_6mo") * -0.0765720648843344 +
    F.col("num_open_3_6mo") * -0.00445814926120152 +
    F.col("mailercount_open_3_6mo") * -0.0492238738722516 +
    F.col("click_rate_3_6mos") * -0.0134077677395812 +
    F.col("state_activities_12mo") * -0.0633283230959882 +
    F.col("state_activity_12mo_i") * 0.993306847608114 +
    F.col("state_event_12mo") * 0.414223452823489 +
    F.col("individual_engagers_12mo") * -0.158287052945086 +
    F.col("structured_12mo") * -4.08215929957436 +
    F.col("moviesfg_12mo") * 0.528472833978728 +
    F.col("state_event_12mo_i") * -0.100561229051113 +
    F.col("structured_12mo_i") * 5.41456331400332 +
    F.col("survey_resp_12mo") * 0.0987218377861994 +
    F.col("newsletter_opens_cnt_12mo") * 0.00437521603361079 +
    F.col("contact_leg_12mo") * 0.0144515347778664 +
    F.col("moviesfg_12mo_i") * -0.861881715434179 +
    F.col("states_vol_12mo") * -3.39227034242535 +
    F.col("states_vol_12mo_i") * 3.88897609235363 +
    F.col("DENSITY_CLUSTERS_1_2_3") * 0.146661685168876 +
    F.col("ADVO_SEGMENT_CD_S3_S4_S5") * 0.457938414150794 +
    F.col("PC_Owner_Y") * 0.637492169169348 +
    F.col("INTERNET__8_10") * -0.566086845655218 +
    F.col("INTERNET__1") * 0.0448979074658065 +
    F.col("INTERNET__2_3_4") * 0.0414439838257837
)
df_work_jobs_score = df_work_jobs_score.withColumn("work_jobs_attendee_em_score", F.exp(F.col("work_jobs_attendee_em_logit")) / (F.lit(1) + F.exp(F.col("work_jobs_attendee_em_logit"))))
df_work_jobs_score = df_work_jobs_score.select("mid_key", "work_jobs_attendee_em_score")
df_work_jobs_score.write.format("delta").mode("overwrite").saveAsTable("intermed.work_jobs_score")

df_work_jobs_score = spark.table("intermed.work_jobs_score")
df_work_jobs_score_with_dummy = df_work_jobs_score.withColumn("dummy_partition", F.lit(1))
window_work_jobs = Window.partitionBy("dummy_partition").orderBy(F.col("work_jobs_attendee_em_score").desc())
df_score_ranks = df_work_jobs_score_with_dummy.withColumn("work_jobs_attendee_em", F.ntile(99).over(window_work_jobs)).drop("dummy_partition")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_geo_appends_rpm = df_geo_appends_rpm.withColumn("mid_key", F.col("mid_key").cast(IntegerType()))
df_score_ranks = df_score_ranks.withColumnRenamed("mid_key", "mid_key_b")
df_geo_appends_rpm_updated = df_geo_appends_rpm.alias("a").join(
    df_score_ranks.alias("b"),
    F.col("a.mid_key") == F.col("b.mid_key_b"),
    "left"
).select(
    (F.col("b.work_jobs_attendee_em") + 1).alias("work_jobs_attendee_em"),
    "a.*"
).drop("mid_key_b")
df_geo_appends_rpm_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

#End-DBShift