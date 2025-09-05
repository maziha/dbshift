import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, DoubleType
from functools import reduce

# Set up environment variables from the SAS script
weekday = 1

# The following SAS %include directives are assumed to be handled by the Databricks environment setup.
# Credentials should be managed via Databricks secrets and scopes.
# LIBNAME assignments are translated to spark.table() calls with appropriate database names.
# %include '/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/Creds.sas' /source2;
# %include '/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/ReadLibnames.sas' /source2;

# Reference to the database specified by the &ref macro variable in SAS
ref_db = "ref" # Assuming 'ref' is the database name for the library

# Load base tables into DataFrames
df_geo_appends_rpm_intermed = spark.table("intermed.geo_appends_rpm")
df_f_vmis = spark.table(f"{ref_db}.f_vmis")
df_d_vtm_individual = spark.table(f"{ref_db}.d_vtm_individual")

# Register temp views for spark.sql
df_geo_appends_rpm_intermed.createOrReplaceTempView("geo_appends_rpm")
df_f_vmis.createOrReplaceTempView("f_vmis")
df_d_vtm_individual.createOrReplaceTempView("d_vtm_individual")

# PROC SQL: Create vt1
df_vt1 = spark.sql("""
    SELECT
        a.mid_key
    FROM intermed.geo_appends_rpm AS a
    LEFT JOIN f_vmis AS b ON a.mid_key = b.mid_key
    WHERE date(b.serv_end_dt) BETWEEN date_sub(current_date(), 730) AND current_date()
""")

# PROC SQL: Create vt2
df_vt2 = spark.sql("""
    SELECT
        a.mid_key
    FROM intermed.geo_appends_rpm AS a
    LEFT JOIN d_vtm_individual AS b ON a.mid_key = b.mid_key
    WHERE b.VTM_ASSIGNMENT_LAST_END_DT BETWEEN date_sub(current_date(), 730) AND current_date()
""")

# DATA STEP: vt_helper
df_vt_helper = df_vt1.unionByName(df_vt2)

# PROC SQL: Create vt
df_vt_helper.createOrReplaceTempView("vt_helper")
df_vt = spark.sql("""
    SELECT
        mid_key,
        COUNT(mid_key) AS vol_ct_24mos
    FROM vt_helper
    GROUP BY mid_key
""")

# Load other necessary tables
df_contact_history_sum = spark.table("intermed.contact_history_sum")

# Register temp views for the main join
df_vt.createOrReplaceTempView("vt")
df_contact_history_sum.createOrReplaceTempView("contact_history_sum")

# PROC SQL: Create tim.all_engage
df_all_engage = spark.sql("""
    SELECT
        a.*,
        {weekday} AS weekday,
        live_answer_ct,
        coalesce(call_freq, 0) AS call_freq,
        coalesce(pct_live, 0) AS pct_live,
        COALESCE(d.liveanswer_freq_3, 0) AS liveanswer_freq_3,
        COALESCE(d.liveanswer_freq3_6, 0) AS liveanswer_freq3_6,
        COALESCE(d.liveanswer_freq6_12, 0) AS liveanswer_freq6_12,
        COALESCE(d.liveanswer_comp_freq_3, 0) AS liveanswer_comp_freq_3,
        COALESCE(d.liveanswer_comp_freq3_6, 0) AS liveanswer_comp_freq3_6,
        COALESCE(d.liveanswer_comp_freq6_12, 0) AS liveanswer_comp_freq6_12,
        coalesce(c.vol_ct_24mos, 0) AS vol_ct_24mos,
        COALESCE(d.poll_ct, 0) AS poll_ct,
        COALESCE(d.poll_noaskct, 0) AS poll_noaskct,
        COALESCE(d.poll_anact, 0) AS poll_anact,
        COALESCE(d.num_inb / num_ct, 0) AS inbound_rate,
        COALESCE(d.num_click, 0) AS num_click,
        coalesce(d.num_open, 0) AS num_open,
        coalesce(d.click_rate, 0) AS click_rate,
        coalesce(d.mailercount_click, 0) AS mailercount_click,
        coalesce(d.mailercount_open, 0) AS mailercount_open,
        coalesce(d.care_ct, 0) AS care_ct,
        coalesce(d.mailct, 0) AS mailct,
        num_clicked_curmonth / num_sent_curmonth AS click_pct_curmonth,
        num_clicked_past12 / num_sent_past12 AS clickrate_past12,
        coalesce(pct_live2, 0) AS pct_live2,
        coalesce(d.mailercount_click_30days, 0) AS mailercount_click_30days,
        coalesce(d.num_inb_3_6mo, 0) AS num_inb_3_6mo
    FROM geo_appends_rpm AS a
    LEFT JOIN vt AS c ON a.mid_key = c.mid_key
    LEFT JOIN contact_history_sum AS d ON a.mid_key = d.mid_key
""")

# PROC SORT NODUPKEY
df_all_engage = df_all_engage.dropDuplicates(["mid_key"])

# DATA STEP: Update tim.all_engage
df_all_engage = df_all_engage.withColumn(
    "newsletter_opens_cnt_12mo",
    F.when(F.col("newsletter_opens_cnt_12mo") > 0, 0).otherwise(F.col("newsletter_opens_cnt_12mo"))
)
df_all_engage.write.format("delta").mode("overwrite").saveAsTable("tim.all_engage")


# DATA STEP: Create email_contact_model_curmonth
df_email_contact_model_curmonth = spark.table("tim.all_engage").filter(F.col("emu_indicator") == 'Y')

df_email_contact_model_curmonth = df_email_contact_model_curmonth.withColumn(
    "click_pct_curmonth", F.coalesce(F.col("click_pct_curmonth"), F.lit(0))
)
df_email_contact_model_curmonth = df_email_contact_model_curmonth.withColumn("clickrate_pastmonth", F.col("click_pct_curmonth"))
df_email_contact_model_curmonth = df_email_contact_model_curmonth.withColumn("Age", F.col("age_agg_ind"))
df_email_contact_model_curmonth = df_email_contact_model_curmonth.withColumn(
    "MemXRenew", F.when(F.col("MemXRenew") > 9, 9).otherwise(F.col("MemXRenew"))
)
df_email_contact_model_curmonth = df_email_contact_model_curmonth.withColumn("AAES_sent", F.coalesce(F.col("AAES_sent"), F.lit(0)))
df_email_contact_model_curmonth = df_email_contact_model_curmonth.withColumn("AAMD_sent", F.coalesce(F.col("AAMD_sent"), F.lit(0)))
df_email_contact_model_curmonth = df_email_contact_model_curmonth.withColumn("AAOS_sent", F.coalesce(F.col("AAOS_sent"), F.lit(0)))
df_email_contact_model_curmonth = df_email_contact_model_curmonth.withColumn("AWSO_sent", F.coalesce(F.col("AWSO_sent"), F.lit(0)))
df_email_contact_model_curmonth = df_email_contact_model_curmonth.withColumn("AAVT_sent", F.coalesce(F.col("AAVT_sent"), F.lit(0)))
df_email_contact_model_curmonth = df_email_contact_model_curmonth.withColumn("AAES_click", F.coalesce(F.col("AAES_click"), F.lit(0)))
df_email_contact_model_curmonth = df_email_contact_model_curmonth.withColumn("AWSO_click", F.coalesce(F.col("AWSO_click"), F.lit(0)))
df_email_contact_model_curmonth = df_email_contact_model_curmonth.withColumn("AAVT_click", F.coalesce(F.col("AAVT_click"), F.lit(0)))
df_email_contact_model_curmonth = df_email_contact_model_curmonth.withColumn("AAMD_click", F.coalesce(F.col("AAMD_click"), F.lit(0)))

df_email_contact_model_curmonth = df_email_contact_model_curmonth.select(
    "mid_key", "ch_acq", "memxrenew", "clickrate_past12",
    "AAES_sent", "AAMD_sent", "AAOS_sent", "AWSO_sent", "AAVT_sent",
    "AAES_click", "AWSO_click", "AAVT_click", "AAMD_click", "age", "clickrate_pastmonth"
)

df_email_contact_model_curmonth.write.format("delta").mode("overwrite").saveAsTable("intermed.email_contact_model_curmonth")

# PROC SQL: sent_sum_group
df_contact_obct_mailer = spark.table("intermed.contact_obct_mailer")
df_contact_obct_mailer.createOrReplaceTempView("contact_obct_mailer")
df_sent_sum_group = spark.sql("""
    SELECT mailer_id, SUM(num_sent_past12) as num_sent
    FROM contact_obct_mailer
    GROUP BY mailer_id
""")

# PROC SQL: click_sum_group
df_contact_ibmailer = spark.table("intermed.contact_ibmailer")
df_contact_ibmailer.createOrReplaceTempView("contact_ibmailer")
df_click_sum_group = spark.sql("""
    SELECT mailer_id, SUM(num_clicked_past12_mailer) as num_clicked
    FROM contact_ibmailer
    GROUP BY mailer_id
""")

# PROC SQL: bymailerid
df_sent_sum_group.createOrReplaceTempView("sent_sum_group")
df_click_sum_group.createOrReplaceTempView("click_sum_group")
df_bymailerid = spark.sql("""
    SELECT *, num_clicked / num_sent as click_rate
    FROM sent_sum_group a
    LEFT JOIN click_sum_group b
    ON a.mailer_id = b.mailer_id
""")

# PROC TRANSPOSE
df_bymailerid_pivot = df_bymailerid.groupBy().pivot("mailer_id").agg(F.first("click_rate"))
renamed_cols = [F.col(c).alias(f"{c}_clickrate") for c in df_bymailerid_pivot.columns]
df_bymailerid_transpose = df_bymailerid_pivot.select(renamed_cols)
df_bymailerid_transpose.write.format("delta").mode("overwrite").saveAsTable("intermed.bymailerid_transpose")

# DATA STEP: all_score_temp (feature engineering)
df_all_score_temp = spark.table("tim.all_engage")

# A very large number of transformations. This corresponds to the large DATA STEP in SAS.
df_all_score_temp = df_all_score_temp.withColumn("grandchildren_dum", F.when(F.col("IBX_GRAND_CHILDREN_AGG_HHD") == 'Y', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("CELL9_10", F.when(F.col("IBX_TELECOM_CELLULAR_AGG_HHD").isin('09', '10'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("advo_s34_dum", F.when(F.col("advo_segment_cd").isin('S4', 'S3'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("survey_resp_12mo", F.coalesce(F.col("survey_resp_12mo"), F.lit(0.5505885)))
df_all_score_temp = df_all_score_temp.withColumn("individual_engagers_12mo_c", F.coalesce(F.col("individual_engagers_12mo"), F.lit(0.7031802)))
df_all_score_temp = df_all_score_temp.withColumn("cens_heat_occhu_percent_utility_", F.coalesce(F.col("cens_heat_occhu_percent_utility_"), F.lit(49.6349962)))
df_all_score_temp = df_all_score_temp.withColumn("age_gt77", F.when((F.col("age_agg_ind") >= 78) & (F.col("age_agg_ind") <= 100), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("cens_homval_home_value_cbsa_n", F.coalesce(F.col("cens_homval_home_value_cbsa_inde"), F.lit(110.6964045)))
df_all_score_temp = df_all_score_temp.withColumn("cens_density_persons_per_hh_n", F.coalesce(F.col("cens_density_persons_per_hh_for_"), F.lit(2.5294776)))
df_all_score_temp = df_all_score_temp.withColumn("age_51to70_dum", F.when((F.col("age_agg_ind") >= 51) & (F.col("age_agg_ind") <= 70), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("teletown_12mo", F.coalesce(F.col("teletown_12mo"), F.lit(1.3949284)))
df_all_score_temp = df_all_score_temp.withColumn("cens_commute_commuter_percent_tr", F.coalesce(F.col("cens_commute_commuter_percent_tr"), F.lit(69.7398104)))
df_all_score_temp = df_all_score_temp.withColumn("cens_move_occhu_percent_moved_in", F.coalesce(F.col("cens_move_occhu_percent_moved_in"), F.lit(2.1748308)))
df_all_score_temp = df_all_score_temp.withColumn("cens_homval_oohu_median_home_v_n", F.coalesce(F.col("cens_homval_oohu_median_home_val"), F.lit(232924.81)))
df_all_score_temp = df_all_score_temp.withColumn("cens_built_hu_percent_built_lt19", F.coalesce(F.col("cens_built_hu_percent_built_lt19"), F.lit(8.1350758)))
df_all_score_temp = df_all_score_temp.withColumn("cens_move_occhu_percent_turnover", F.coalesce(F.col("cens_move_occhu_percent_turnover"), F.lit(31.3522392)))
df_all_score_temp = df_all_score_temp.withColumn("activist_dum", F.when(F.col("activist").isin('S3', 'S5'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("cens_earn_hh_percent_with_earnin", F.coalesce(F.col("cens_earn_hh_percent_with_earnin"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("income1234", F.when(F.col("IBX_INCOME_ESTIMATED_NARROW_RANG").isin('1', '2', '3', '4'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("cens_earn_hh_percent_no_earn_n", F.coalesce(F.col("cens_earn_hh_percent_no_earnings"), F.lit(23.2570141)))
df_all_score_temp = df_all_score_temp.withColumn("advocacy_donations_12mo_n", F.coalesce(F.col("advocacy_donations_12mo"), F.lit(0.0756742)))
df_all_score_temp = df_all_score_temp.withColumn("state_activities_12mo_n", F.coalesce(F.col("state_activities_12mo"), F.lit(0.0255043)))
df_all_score_temp = df_all_score_temp.withColumn("cens_marr_pop15_plus_percent_spo", F.coalesce(F.col("cens_marr_pop15_plus_percent_spo"), F.lit(48.8945914)))
df_all_score_temp = df_all_score_temp.withColumn("cens_move_occhu_percent_new_li_n", F.coalesce(F.col("cens_move_occhu_percent_new_list"), F.lit(27.9431599)))
df_all_score_temp = df_all_score_temp.withColumn("advocacy_petitions_12mo_n", F.coalesce(F.col("advocacy_petitions_12mo"), F.lit(0.1880439)))
df_all_score_temp = df_all_score_temp.withColumn("cens_occup_empld_percent_bus_and", F.coalesce(F.col("cens_occup_empld_percent_bus_and"), F.lit(4.4665885)))
df_all_score_temp = df_all_score_temp.withColumn("foundation_donations_12mo", F.coalesce(F.col("foundation_donations_12mo"), F.lit(0.1156007)))
df_all_score_temp = df_all_score_temp.withColumn("single", F.when(F.col("MARITALSTATus") == 'S', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("orders_digital", F.coalesce(F.col("orders_online"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("cens_earn_hh_percent_with_public", F.coalesce(F.col("cens_earn_hh_percent_with_public"), F.lit(2.2179306)))
df_all_score_temp = df_all_score_temp.withColumn("medical_supplies", F.when(F.col("ibx_health_medical_supplies") == '1', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("newsletter_opens_cnt_12mo_c", F.coalesce(F.col("newsletter_opens_cnt_12mo"), F.lit(3.264157)))
df_all_score_temp = df_all_score_temp.withColumn("state_event_12mo", F.coalesce(F.col("state_event_12mo"), F.lit(1.1858166)))
df_all_score_temp = df_all_score_temp.withColumn("moviesfg_12mo", F.coalesce(F.col("moviesfg_12mo"), F.lit(0.3852711)))
df_all_score_temp = df_all_score_temp.withColumn("ibx_adult_age_75", F.when(F.col("ibx_adult_age_75_p_agg_hhd") == '1', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("cens_occup_empld_percent_farm_fi", F.coalesce(F.col("cens_occup_empld_percent_farm_fi"), F.lit(0.5659673)))
df_all_score_temp = df_all_score_temp.withColumn("cens_earn_hh_percent_no_wage_sal", F.coalesce(F.col("cens_earn_hh_percent_no_wage_sal"), F.lit(26.3927564)))
df_all_score_temp = df_all_score_temp.withColumn("cens_inc_hh_median_household_i_n", F.coalesce(F.col("cens_inc_hh_median_household_inc"), F.lit(68436.42)))
df_all_score_temp = df_all_score_temp.withColumn("cens_homval_oohu_percent_home_n", F.coalesce(F.col("cens_homval_oohu_percent_home_va"), F.lit(0.9186118)))
df_all_score_temp = df_all_score_temp.withColumn("cens_indus_empld_percent_informa", F.coalesce(F.col("cens_indus_empld_percent_informa"), F.lit(1.7703097)))
df_all_score_temp = df_all_score_temp.withColumn("cens_indus_empld_percent_transpo", F.coalesce(F.col("cens_indus_empld_percent_transpo"), F.lit(3.7275195)))
df_all_score_temp = df_all_score_temp.withColumn("cens_occup_empld_percent_healt_c", F.coalesce(F.col("cens_occup_empld_percent_healthc"), F.lit(1.9956405)))
df_all_score_temp = df_all_score_temp.withColumn("cens_indus_empld_percent_mining", F.coalesce(F.col("cens_indus_empld_percent_mining"), F.lit(1.0356997)))
df_all_score_temp = df_all_score_temp.withColumn("cens_indus_empld_percent_educa_n", F.coalesce(F.col("cens_indus_empld_percent_educati"), F.lit(9.1990768)))
df_all_score_temp = df_all_score_temp.withColumn("general_activist_model_n", F.coalesce(F.col("general_activist_model"), F.lit(61.3698537)))
df_all_score_temp = df_all_score_temp.withColumn("cens_mortg_oohu_percent_no_mor_n", F.coalesce(F.col("cens_mortg_oohu_percent_no_mortg"), F.lit(34.0301454)))
df_all_score_temp = df_all_score_temp.withColumn("aarporg_i", F.coalesce(F.col("aarporg_i"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("emailable_dum", F.when(F.col("EMAILABLE_AGG_IND") == 'Y', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("CHILD_PRESENCE_dum", F.when(F.col("IBX_CHILD_PRESENCE_AGG_HHD") == 'Y', 1).otherwise(0))

df_all_score_temp = df_all_score_temp.withColumn("logit_morning", 
    -2.2409 +
    (F.col("liveanswer_freq_3") * 1.0194) +
    (F.col("liveanswer_freq3_6") * 0.1953) +
    (F.col("liveanswer_freq6_12") * 0.2914) +
    (F.col("pct_live") * 1.3478) +
    (F.col("advo_s34_dum") * 0.7324) +
    (F.col("survey_resp_12mo") * -0.5) +
    (F.col("individual_engagers_12mo_c") * 0.1306) +
    (F.col("cens_heat_occhu_percent_utility_") * -0.00243) +
    (F.col("aarporg_i") * -0.1744) +
    (F.col("age_gt77") * 0.2001) +
    (F.col("cens_homval_home_value_cbsa_n") * 0.00191) +
    (F.col("cens_density_persons_per_hh_n") * 0.0754) +
    (F.col("call_freq") * -0.1283) +
    (F.col("emailable_dum") * -0.1571) +
    (F.col("age_51to70_dum") * -0.2015) +
    (F.col("teletown_12mo") * 0.2412) +
    (F.col("cens_commute_commuter_percent_tr") * -0.00256) +
    (F.col("cens_move_occhu_percent_moved_in") * 0.0133) +
    (F.col("cens_homval_oohu_median_home_v_n") * -0.000000825) +
    (F.col("cens_built_hu_percent_built_lt19") * 0.00198) +
    (F.col("cens_move_occhu_percent_turnover") * 0.00517) +
    (F.col("activist_dum") * -0.172) +
    (F.col("cens_earn_hh_percent_with_earnin") * -0.00222) +
    (F.col("income1234") * 0.0661) +
    (F.col("cens_earn_hh_percent_no_earn_n") * -0.0137) +
    (F.col("advocacy_donations_12mo_n") * -0.0697) +
    (F.col("state_activities_12mo_n") * -0.1315) +
    (F.col("grandchildren_dum") * 0.051) +
    (F.col("cens_marr_pop15_plus_percent_spo") * 0.00367) +
    (F.col("cens_move_occhu_percent_new_li_n") * -0.00206) +
    (F.col("CELL9_10") * 0.076) +
    (F.col("CHILD_PRESENCE_dum") * -0.0592) +
    (F.col("advocacy_petitions_12mo_n") * -0.056) +
    (F.col("cens_occup_empld_percent_bus_and") * -0.0051) +
    (F.col("foundation_donations_12mo") * -0.0468) +
    (F.col("single") * -0.0486) +
    (F.col("orders_digital") * -0.0166) +
    (F.col("cens_earn_hh_percent_with_public") * -0.00492) +
    (F.col("medical_supplies") * 0.079) +
    (F.col("newsletter_opens_cnt_12mo_c") * -0.00145) +
    (F.col("state_event_12mo") * 0.2581) +
    (F.col("moviesfg_12mo") * 0.4457) +
    (F.col("ibx_adult_age_75") * 0.0662) +
    (F.col("cens_occup_empld_percent_farm_fi") * 0.00689) +
    (F.col("cens_earn_hh_percent_no_wage_sal") * 0.0121) +
    (F.col("cens_inc_hh_median_household_i_n") * -0.00000164) +
    (F.col("cens_homval_oohu_percent_home_n") * 0.00601) +
    (F.col("cens_indus_empld_percent_informa") * 0.00839) +
    (F.col("cens_indus_empld_percent_transpo") * -0.00548) +
    (F.col("cens_occup_empld_percent_healt_c") * 0.00674) +
    (F.col("cens_indus_empld_percent_mining") * 0.00606) +
    (F.col("cens_indus_empld_percent_educa_n") * -0.00226) +
    (F.col("general_activist_model_n") * -0.00071) +
    (F.col("cens_mortg_oohu_percent_no_mor_n") * 0.00487)
)
df_all_score_temp = df_all_score_temp.withColumn("score_morning", F.exp(F.col("logit_morning")) / (1 + F.exp(F.col("logit_morning"))))

df_all_score_temp = df_all_score_temp.withColumn("educational_attainment_model", F.coalesce(F.col("educational_attainment_model"), F.lit(32.2845081)))
df_all_score_temp = df_all_score_temp.withColumn("cens_age_pop_median_age_of_fem_n", F.coalesce(F.col("cens_age_pop_median_age_of_femal"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("cens_census_tract", F.coalesce(F.col("cens_census_tract"), F.lit(206411.28)))
df_all_score_temp = df_all_score_temp.withColumn("cens_age_pop_percent_60_64_n", F.coalesce(F.col("cens_age_pop_percent_60_64"), F.lit(7.227523)))
df_all_score_temp = df_all_score_temp.withColumn("cens_ethnic_pop_percent_hisp_n", F.coalesce(F.col("cens_ethnic_pop_percent_hispanic"), F.lit(12.4651286)))
df_all_score_temp = df_all_score_temp.withColumn("cens_typ_pop_percent_stepchild_i", F.coalesce(F.col("cens_typ_pop_percent_stepchild_i"), F.lit(1.4256173)))
df_all_score_temp = df_all_score_temp.withColumn("cens_ethnic_pop_percent_black_n", F.coalesce(F.col("cens_ethnic_pop_percent_black_on"), F.lit(7.7961404)))
df_all_score_temp = df_all_score_temp.withColumn("personic_2_dum", F.when(F.col("IBX_PERSONIC_CLUSTER").isin('09', '15', '28', '49', '50', '51'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("cens_gender_pop_percent_female", F.coalesce(F.col("cens_gender_pop_percent_female"), F.lit(50.9805777)))
df_all_score_temp = df_all_score_temp.withColumn("cens_urban_pop_percent_urban_c", F.coalesce(F.col("cens_urban_pop_percent_urban_in_"), F.lit(66.2287488)))
df_all_score_temp = df_all_score_temp.withColumn("home_market_valueABCD", F.when(F.col("ibx_home_market_value_premier").isin('A', 'B', 'C', 'D'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("cens_earn_hh_percent_with_self_e", F.coalesce(F.col("cens_earn_hh_percent_with_self_e"), F.lit(11.2440831)))
df_all_score_temp = df_all_score_temp.withColumn("cens_occup_empld_percent_law_enf", F.coalesce(F.col("cens_occup_empld_percent_law_enf"), F.lit(1.210143)))
df_all_score_temp = df_all_score_temp.withColumn("cens_commute_commuter_avg_trav_c", F.coalesce(F.col("cens_commute_commuter_avg_trav_t"), F.lit(25.3309236)))
df_all_score_temp = df_all_score_temp.withColumn("petition_col_12mo", F.coalesce(F.col("petition_col_12mo"), F.lit(0.1998559)))
df_all_score_temp = df_all_score_temp.withColumn("gun_ownership_model", F.coalesce(F.col("gun_ownership_model"), F.lit(60)))
df_all_score_temp = df_all_score_temp.withColumn("cable_c", F.coalesce(F.col("cable"), F.lit(64.2686859)))
df_all_score_temp = df_all_score_temp.withColumn("cens_occup_empld_percent_health_", F.coalesce(F.col("cens_occup_empld_percent_health_"), F.lit(3.8729753)))
df_all_score_temp = df_all_score_temp.withColumn("cens_rent_rntl_median_rent", F.coalesce(F.col("cens_rent_rntl_median_rent"), F.lit(856.7530365)))
df_all_score_temp = df_all_score_temp.withColumn("R1_IBX_HOUSEHOLD_INCOME129", F.when(F.col("IBX_HOUSEHOLD_INCOME").isin('E', 'F', 'G', 'H', 'I', 'J'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("CENS_ETHNIC_POP_PERCENT_SOME_c", F.coalesce(F.col("cens_ethnic_pop_percent_some_oth"), F.lit(4.9474971)))
df_all_score_temp = df_all_score_temp.withColumn("cens_occup_empld_percent_constr_", F.coalesce(F.col("cens_occup_empld_percent_constr_"), F.lit(5.7595537)))
df_all_score_temp = df_all_score_temp.withColumn("cens_indus_empld_percent_manufac", F.coalesce(F.col("cens_indus_empld_percent_manufac"), F.lit(7.6021087)))
df_all_score_temp = df_all_score_temp.withColumn("cens_lang_hh_percent_spanish_spe", F.coalesce(F.col("cens_lang_hh_percent_spanish_spe"), F.lit(8.3127669)))
df_all_score_temp = df_all_score_temp.withColumn("job_78_dum", F.when(F.col("IBX_OCCUPATION_INPUT_AGG_HHD").isin('7', '8', '9', 'Y', 'Z'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("cens_heat_occhu_percent_other_c", F.coalesce(F.col("cens_heat_occhu_percent_other_he"), F.lit(0.5653551)))

df_all_score_temp = df_all_score_temp.withColumn("logit_night", 
    -0.2104 +
    (F.col("liveanswer_freq_3") * 0.3195) +
    (F.col("liveanswer_freq3_6") * 0.0311) +
    (F.col("liveanswer_freq6_12") * -0.1267) +
    (F.col("pct_live") * 2.1201) +
    (F.col("advo_s34_dum") * 0.4461) +
    (F.col("survey_resp_12mo") * -0.4055) +
    (F.col("cens_mortg_oohu_percent_no_mor_n") * 0.00836) +
    (F.col("cens_homval_oohu_median_home_v_n") * -0.00000163) +
    (F.col("age_gt77") * 0.1656) +
    (F.col("educational_attainment_model") * -0.00415) +
    (F.col("cens_age_pop_median_age_of_fem_n") * -0.00736) +
    (F.col("cens_census_tract") * 0.00000009064) +
    (F.col("cens_homval_home_value_cbsa_n") * 0.00235) +
    (F.col("cens_age_pop_percent_60_64_n") * -0.0332) +
    (F.col("cens_ethnic_pop_percent_hisp_n") * -0.0059) +
    (F.col("aarporg_i") * -0.0915) +
    (F.col("cens_typ_pop_percent_stepchild_i") * -0.0659) +
    (F.col("cens_density_persons_per_hh_n") * -0.1894) +
    (F.col("cens_ethnic_pop_percent_black_n") * 0.00359) +
    (F.col("emailable_dum") * -0.1153) +
    (F.col("cens_marr_pop15_plus_percent_spo") * 0.00845) +
    (F.col("age_51to70_dum") * -0.1201) +
    (F.col("teletown_12mo") * -0.2236) +
    (F.col("personic_2_dum") * 0.0704) +
    (F.col("cens_gender_pop_percent_female") * -0.0132) +
    (F.col("cens_inc_hh_median_household_i_n") * 0.000003764) +
    (F.col("cens_urban_pop_percent_urban_c") * -0.00144) +
    (F.col("home_market_valueABCD") * 0.0919) +
    (F.col("cens_earn_hh_percent_with_self_e") * 0.00506) +
    (F.col("cens_move_occhu_percent_turnover") * -0.00452) +
    (F.col("cens_occup_empld_percent_law_enf") * -0.0128) +
    (F.col("single") * -0.0395) +
    (F.col("cens_commute_commuter_avg_trav_c") * 0.00482) +
    (F.col("cens_indus_empld_percent_mining") * 0.00773) +
    (F.col("income1234") * 0.0612) +
    (F.col("petition_col_12mo") * -0.2929) +
    (F.col("grandchildren_dum") * 0.0594) +
    (F.col("gun_ownership_model") * 0.00124) +
    (F.col("cens_built_hu_percent_built_lt19") * 0.000984) +
    (F.col("cable_c") * 0.00342) +
    (F.col("cens_occup_empld_percent_health_") * -0.0058) +
    (F.col("newsletter_opens_cnt_12mo_c") * -0.00109) +
    (F.col("cens_rent_rntl_median_rent") * -0.00007) +
    (F.col("R1_IBX_HOUSEHOLD_INCOME129") * -0.0407) +
    (F.col("medical_supplies") * 0.0518) +
    (F.col("cens_indus_empld_percent_informa") * -0.00518) +
    (F.col("CENS_ETHNIC_POP_PERCENT_SOME_c") * 0.00441) +
    (F.col("cens_occup_empld_percent_constr_") * 0.00572) +
    (F.col("cens_indus_empld_percent_manufac") * 0.00297) +
    (F.col("cens_lang_hh_percent_spanish_spe") * 0.00265) +
    (F.col("job_78_dum") * 0.05) +
    (F.col("ibx_adult_age_75") * 0.0631) +
    (F.col("cens_heat_occhu_percent_other_c") * 0.012)
)
df_all_score_temp = df_all_score_temp.withColumn("score_night", F.exp(F.col("logit_night")) / (1 + F.exp(F.col("logit_night"))))

df_all_score_temp = df_all_score_temp.withColumn("cens_heat_occhu_percent_oil_or_k", F.coalesce(F.col("cens_heat_occhu_percent_oil_or_k"), F.lit(9.7654324)))
df_all_score_temp = df_all_score_temp.withColumn("cens_hustr_hu_percent_1_unit_det", F.coalesce(F.col("cens_hustr_hu_percent_1_unit_det"), F.lit(73.2272901)))
df_all_score_temp = df_all_score_temp.withColumn("cens_age_pop_percent_45_54_n", F.coalesce(F.col("cens_age_pop_percent_45_54"), F.lit(13.5680336)))
df_all_score_temp = df_all_score_temp.withColumn("cens_age_pop_percent_55_64", F.coalesce(F.col("cens_age_pop_percent_55_64"), F.lit(14.7231188)))
df_all_score_temp = df_all_score_temp.withColumn("cens_ethnic_pop_percent_white_c", F.coalesce(F.col("cens_ethnic_pop_percent_white_on"), F.lit(76.9225635)))
df_all_score_temp = df_all_score_temp.withColumn("orders_36moterm", F.coalesce(F.col("orders_36moterm"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("CELL1_4", F.when(F.col("IBX_TELECOM_CELLULAR_AGG_HHD").isin('01', '02', '03', '04'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("eng_sp", F.coalesce(F.col("cens_lang_hh_percent_english_spe"), F.lit(84.978586)))
df_all_score_temp = df_all_score_temp.withColumn("cens_heat_occhu_percent_bottle_c", F.coalesce(F.col("cens_heat_occhu_percent_bottle_o"), F.lit(5.3217587)))
df_all_score_temp = df_all_score_temp.withColumn("cens_educ_pop25_plus_percent_b_c", F.coalesce(F.col("cens_educ_pop25_plus_percent_bac"), F.lit(18.8933979)))
df_all_score_temp = df_all_score_temp.withColumn("cens_inc_hh_percent_household_in", F.coalesce(F.col("cens_inc_hh_percent_household_in"), F.lit(4.986367)))
df_all_score_temp = df_all_score_temp.withColumn("cens_typ_pop_percent_grandchild_", F.coalesce(F.col("cens_typ_pop_percent_grandchild_"), F.lit(2.1370728)))
df_all_score_temp = df_all_score_temp.withColumn("ibx_adult_25_34", F.when(F.col("ibx_adult_age_25_34_agg_hhd") == '1', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("petition_sign_12mo", F.coalesce(F.col("petition_sign_12mo"), F.lit(0.0064377)))
df_all_score_temp = df_all_score_temp.withColumn("cens_educ_pop25_plus_median_educ", F.coalesce(F.col("cens_educ_pop25_plus_median_educ"), F.lit(12.5875249)))
df_all_score_temp = df_all_score_temp.withColumn("orders_dm", F.coalesce(F.coalesce(F.col("orders_acqmail"), F.lit(0)) + F.coalesce(F.col("orders_winback"), F.lit(0)), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("fiscal_policy_model_c", F.coalesce(F.col("fiscal_policy_model"), F.lit(46.9632164)))
df_all_score_temp = df_all_score_temp.withColumn("community_childr_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_CHILDR") == 1, 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("cens_inc_family_inc_state_decile", F.coalesce(F.col("cens_inc_family_inc_state_decile"), F.lit(5.0566134)))
df_all_score_temp = df_all_score_temp.withColumn("ch_acq_U", F.when(F.col("ch_acq") == 'U', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("community_charity_dum", F.when(F.col("IBX_COMMUNITY_CHARITIES_AGG_HHD") == 1, 1).otherwise(0))

df_all_score_temp = df_all_score_temp.withColumn("logit_afternoon", 
    -3.2021 +
    (F.col("liveanswer_freq_3") * 0.9296) +
    (F.col("liveanswer_freq3_6") * -0.0822) +
    (F.col("liveanswer_freq6_12") * 0.0895) +
    (F.col("pct_live") * 2.2052) +
    (F.col("advo_s34_dum") * 0.4056) +
    (F.col("cens_heat_occhu_percent_oil_or_k") * -0.0116) +
    (F.col("cens_commute_commuter_avg_trav_c") * 0.0153) +
    (F.col("age_51to70_dum") * -0.2361) +
    (F.col("cens_heat_occhu_percent_utility_") * -0.00271) +
    (F.col("general_activist_model_n") * -0.00196) +
    (F.col("emailable_dum") * -0.1232) +
    (F.col("cens_earn_hh_percent_with_earnin") * -0.00349) +
    (F.col("cens_census_tract") * 0.0000001581) +
    (F.col("cens_urban_pop_percent_urban_c") * 0.00139) +
    (F.col("cens_mortg_oohu_percent_no_mor_n") * 0.00555) +
    (F.col("cens_hustr_hu_percent_1_unit_det") * 0.00187) +
    (F.col("cens_homval_home_value_cbsa_n") * 0.00313) +
    (F.col("cens_age_pop_percent_45_54_n") * 0.014) +
    (F.col("CENS_ETHNIC_POP_PERCENT_SOME_c") * 0.0143) +
    (F.col("cens_age_pop_percent_55_64") * -0.00945) +
    (F.col("gun_ownership_model") * 0.000778) +
    (F.col("aarporg_i") * -0.0773) +
    (F.col("cens_typ_pop_percent_stepchild_i") * 0.0454) +
    (F.col("cens_ethnic_pop_percent_white_c") * 0.00281) +
    (F.col("orders_36moterm") * -0.0171) +
    (F.col("CELL1_4") * -0.1277) +
    (F.col("petition_col_12mo") * 0.4622) +
    (F.col("ibx_adult_age_75") * 0.1067) +
    (F.col("eng_sp") * 0.00598) +
    (F.col("cens_heat_occhu_percent_bottle_c") * -0.00538) +
    (F.col("cens_educ_pop25_plus_percent_b_c") * -0.00756) +
    (F.col("cens_homval_oohu_median_home_v_n") * -0.00000155) +
    (F.col("cens_inc_hh_percent_household_in") * 0.00731) +
    (F.col("cens_typ_pop_percent_grandchild_") * 0.0302) +
    (F.col("ibx_adult_25_34") * -0.0893) +
    (F.col("R1_IBX_HOUSEHOLD_INCOME129") * -0.0369) +
    (F.col("ch_acq_U") * -0.1216) +
    (F.col("grandchildren_dum") * 0.056) +
    (F.col("advocacy_donations_12mo_n") * -0.0295) +
    (F.col("teletown_12mo") * -0.3058) +
    (F.col("income1234") * 0.0514) +
    (F.col("petition_sign_12mo") * -0.4738) +
    (F.col("cens_indus_empld_percent_manufac") * 0.00524) +
    (F.col("cens_occup_empld_percent_health_") * 0.00719) +
    (F.col("cens_educ_pop25_plus_median_educ") * 0.0402) +
    (F.col("orders_dm") * -0.0151) +
    (F.col("single") * -0.0577) +
    (F.col("call_freq") * -0.076) +
    (F.col("CELL9_10") * 0.0448) +
    (F.col("fiscal_policy_model_c") * 0.00131) +
    (F.col("community_childr_dum") * -0.0441) +
    (F.col("community_charity_dum") * 0.027) +
    (F.col("cens_inc_family_inc_state_decile") * 0.00919) +
    (F.col("cens_indus_empld_percent_informa") * 0.00659)
)
df_all_score_temp = df_all_score_temp.withColumn("score_afternoon", F.exp(F.col("logit_afternoon")) / (1 + F.exp(F.col("logit_afternoon"))))

# Previous live answer statements
df_all_score_temp = df_all_score_temp.withColumn("Party_Aff", F.when(F.col("PartyAffiliation").isin('DEM', 'REP', 'NPA'), F.col("PartyAffiliation")).otherwise('OTH'))
df_all_score_temp = df_all_score_temp.withColumn("Religion", F.when(F.col("ReligionCode").isin('C', 'X', 'P', 'J'), F.col("ReligionCode")).otherwise('Other'))
df_all_score_temp = df_all_score_temp.withColumn("Work_Status", F.when(F.col("WorkStatus").isin('F', 'P', 'R'), F.col("WorkStatus")).otherwise('U'))
df_all_score_temp = df_all_score_temp.withColumn("acevflag_c", F.when(F.col("acev_flag") == 'Y', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("advo_s1_dum", F.when(F.col("advo_segment_cd") == 'S1', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("ch_acq_D", F.when(F.col("ch_acq") == 'D', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("Marital_S", F.when(F.col("MaritalStatus") == 'S', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("curterm_36_dum", F.when(F.col("cur_term") == '36', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("audio_visual_composite_c", F.when(F.col("audio_visual_composite") == '1', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("sy_dense", F.when(F.col("DENSITY_CLUSTERS").isin('1', '2', '3', '4', '5', '6'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("workcluster_wealthy", F.when(F.col("WORK_CLUSTERS").isin('1', '2'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("workcluster_6_dum", F.when(F.col("WORK_CLUSTERS") == '6', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("goi_missing_dum", F.when(F.col("globally_opted_in") == '', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("community_health_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_HEALTH") == 1, 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("income_1to4_dum", F.when(F.col("IBX_INCOME_ESTIMATED_NARROW_RANG").isin('1', '2', '3', '4'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("income_du_dum", F.when(F.col("IBX_INCOME_ESTIMATED_NARROW_RANG").isin('D', 'U'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("job_78_dum", F.when(F.col("IBX_OCCUPATION_INPUT_AGG_HHD").isin('7', '8'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("internet_1", F.when(F.col("IBX_TELECOM_INTERNET_AGG_HHD") == '01', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("TRAVEL_TYPE_b", F.when(F.col("IBX_TRAVEL_TYPE_AGG_HHD") == 'B', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("VEHICLE_DOMINANT_C", F.when(F.col("ibx_vehicle_dominant_lifestyle_p") == 'C', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("VEHICLE_OWNED_y", F.when(F.col("IBX_VEHICLE_OWNED_AGG_HHD") == 'Y', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("Party_DEM", F.when(F.col("Party_Aff") == 'DEM', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("Party_NPA", F.when(F.col("Party_Aff") == 'NPA', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("retire", F.when(F.col("Work_Status") == 'R', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("Catholic", F.when(F.col("Religion") == 'C', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("home_owner", F.when(F.col("ibx_home_owner_renter_premier") == 'O', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("commute_pct_ls30", F.coalesce(F.col("CENS_COMMUTE_COMMUTER_PERCENT_TR"), F.lit(69.49)))
df_all_score_temp = df_all_score_temp.withColumn("PERCENT_FEMALE", F.coalesce(F.col("CENS_GENDER_POP_PERCENT_FEMALE"), F.lit(50.8)))
df_all_score_temp = df_all_score_temp.withColumn("POP25_PLUS_MEDIAN_EDUC", F.coalesce(F.col("CENS_EDUC_POP25_PLUS_MEDIAN_EDUC"), F.lit(12.93)))
df_all_score_temp = df_all_score_temp.withColumn("POP25_PLUS_PERCENT_BAC", F.coalesce(F.col("CENS_EDUC_POP25_PLUS_PERCENT_BAC"), F.lit(22.53)))
df_all_score_temp = df_all_score_temp.withColumn("HH_PERCENT_2_PERSONS", F.coalesce(F.col("CENS_HHSIZE_HH_PERCENT_2_PERSONS"), F.lit(35.15)))
df_all_score_temp = df_all_score_temp.withColumn("commute_pct_public", F.coalesce(F.col("CENS_COMMUTE_WRKRS_PERCENT_PUBLI"), F.lit(4.81)))
df_all_score_temp = df_all_score_temp.withColumn("OOHU_Median_Home_Value_c", F.coalesce(F.col("CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL"), F.lit(281590.69)))
df_all_score_temp = df_all_score_temp.withColumn("HomVal_Home_Value_CBSA_Index_c", F.coalesce(F.col("CENS_HOMVAL_HOME_VALUE_CBSA_INDE"), F.lit(109.54)))
df_all_score_temp = df_all_score_temp.withColumn("diversity_flag_c", F.when((F.col("diversity_flag_agg_ind") == 0) | (F.col("diversity_flag_agg_ind").isNull()), 0).otherwise(1))
df_all_score_temp = df_all_score_temp.withColumn("drvsafe_pro_em_c", F.coalesce(F.col("drvsafe_pro_em_old"), F.lit(5.6)))
df_all_score_temp = df_all_score_temp.withColumn("sy_otsbn_polfund_2012a_c", F.coalesce(F.col("sy_otsbn_polfund_2012a"), F.lit(73.858)))
df_all_score_temp = df_all_score_temp.withColumn("PERCENTBLUECOLLAR_c", F.coalesce(F.col("cens_BLUECOLLAR"), F.lit(33.88)))
df_all_score_temp = df_all_score_temp.withColumn("PERCENTCOLLEGEGRADS_c", F.coalesce(F.col("cens_educ_pop25_plus_percent_col"), F.lit(34.26)))
df_all_score_temp = df_all_score_temp.withColumn("PERCENTMANAGEMENTPROFESSIONALS_c", F.coalesce(F.col("cens_MANAGEMENTPROFESSIONALS"), F.lit(16.02)))
df_all_score_temp = df_all_score_temp.withColumn("PERCENTSINGLEUNITDWELLINGS_c", F.coalesce(F.col("cens_hustr_hu_percent_1_unit_det"), F.lit(74.18)))
df_all_score_temp = df_all_score_temp.withColumn("SY_EDUCSCORE_c", F.coalesce(F.col("EDUCATIONAL_ATTAINMENT_MODEL"), F.lit(0.426)))
df_all_score_temp = df_all_score_temp.withColumn("SY_GUNSCORE_c", F.coalesce(F.col("GUN_OWNERSHIP_MODEL"), F.lit(0.373)))
df_all_score_temp = df_all_score_temp.withColumn("CENS_EARN_HH_PERCENT_NO_EARNIN_c", F.coalesce(F.col("CENS_AGE_POP_PERCENT_60_64"), F.lit(6.63)))
df_all_score_temp = df_all_score_temp.withColumn("sp_rel_c", F.coalesce(F.col("Overall_Active_SP_Reltshps"), F.lit(0.199)))
df_all_score_temp = df_all_score_temp.withColumn("WORKERS_c", F.coalesce(F.col("CENS_COUNT_WORKERS"), F.lit(857)))
df_all_score_temp = df_all_score_temp.withColumn("PERCENT_CIVILIAN_VET", F.coalesce(F.col("CENS_EMPLOY_POP18_PLUS_PERCENT_C"), F.lit(8.9)))
df_all_score_temp = df_all_score_temp.withColumn("PERCENT_EMPLOYED", F.coalesce(F.col("CENS_EMPLOY_LABF_PERCENT_EMPLOYE"), F.lit(94.67)))
df_all_score_temp = df_all_score_temp.withColumn("SY_GENERALACTIVIST_c", F.coalesce(F.col("GENERAL_ACTIVIST_MODEL"), F.lit(58.996)))
df_all_score_temp = df_all_score_temp.withColumn("UTILITY_gas", F.coalesce(F.col("CENS_HEAT_OCCHU_PERCENT_UTILITY_"), F.lit(62.07)))
df_all_score_temp = df_all_score_temp.withColumn("PCT_HOME_VAL_LS10K", F.coalesce(F.col("CENS_HOMVAL_OOHU_PERCENT_HOME_VA"), F.lit(1.17)))
df_all_score_temp = df_all_score_temp.withColumn("SPOUSE_PRS", F.coalesce(F.col("CENS_MARR_POP15_PLUS_PERCENT_SPO"), F.lit(52.06)))
df_all_score_temp = df_all_score_temp.withColumn("WIDOWED", F.coalesce(F.col("CENS_MARR_POP15_PLUS_PERCENT_WID"), F.lit(6.16)))
df_all_score_temp = df_all_score_temp.withColumn("RENT_c", F.coalesce(F.col("CENS_RENT_RNTL_MEDIAN_RENT"), F.lit(887.669)))
df_all_score_temp = df_all_score_temp.withColumn("age_60to80_dum", F.when((F.col("age_agg_ind") > 60) & (F.col("age_agg_ind") < 80), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("HH_pct_Spanish_Speaking_c", F.coalesce(F.col("CENS_LANG_HH_PERCENT_SPANISH_SPE"), F.lit(68.685)))
df_all_score_temp = df_all_score_temp.withColumn("TELECOM_CALLING_SERVICES_AGG123", F.when(F.col("IBX_TELECOM_CALLING_SERVICES_AGG").isin('01', '02', '03'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("renewals_c", F.coalesce(F.col("MemXRenew"), F.lit(5)))
df_all_score_temp = df_all_score_temp.withColumn("POPULATION_c", F.coalesce(F.col("CENS_COUNT_POPULATION"), F.lit(1755.76)))
df_all_score_temp = df_all_score_temp.withColumn("EMP_ACCOR_FOOD", F.coalesce(F.col("CENS_INDUS_EMPLD_PERCENT_ACCOMOD"), F.lit(5.96)))
df_all_score_temp = df_all_score_temp.withColumn("newsletter_opens_cnt_12mo_c", F.coalesce(F.col("newsletter_opens_cnt_12mo"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("teletown_12mo_ic", F.coalesce(F.col("teletown_12mo_i"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("WORKING_WOMAN_y", F.when(F.col("IBX_WORKING_WOMAN_AGG_HHD") == 'Y', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("dwelling_m_dum", F.when(F.col("IBX_DWELLING_TYPE_AGG_HHD") == 'M', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("activist_i", F.coalesce(F.col("activist_i"), F.lit(0)))

df_all_score_temp = df_all_score_temp.withColumn("goi_1_dum", F.when(F.col("globally_opted_in") == '1', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("ch_acq_F", F.when(F.col("ch_acq") == 'F', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("gender_M", F.when(F.col("Gender_agg_ind") == 'M', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("Party_REP", F.when(F.col("Party_Aff") == 'REP', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("PERCENTASIAN_c", F.coalesce(F.col("cens_ethnic_pop_percent_asian_on"), F.lit(3.53)))
df_all_score_temp = df_all_score_temp.withColumn("household_nols18_c", F.coalesce(F.col("CENS_CHILD_HH_PERCENT_WITHOUT_PE"), F.lit(67.72)))
df_all_score_temp = df_all_score_temp.withColumn("PERSONS_PER_HH", F.coalesce(F.col("CENS_DENSITY_PERSONS_PER_HH_FOR_"), F.lit(2.55)))
df_all_score_temp = df_all_score_temp.withColumn("OOHU_MEDIAN_HOME_VAL_c", F.coalesce(F.col("CENS_HOMVAL_OOHU_MEDIAN_HOME_VAL"), F.lit(282862.24)))
df_all_score_temp = df_all_score_temp.withColumn("age_57to63_dum", F.when((F.col("age_agg_ind") >= 57) & (F.col("age_agg_ind") <= 63), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("SY_OTSBN_POLFUND_c", F.coalesce(F.col("SY_OTSBN_POLFUND"), F.lit(9.89)))
df_all_score_temp = df_all_score_temp.withColumn("state_activities_12mo_n", F.coalesce(F.col("state_activities_12mo"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("yeas_survey_12mo_i", F.coalesce(F.col("yeas_survey_12mo_i"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("other_vol_12mo_i", F.coalesce(F.col("other_vol_12mo_i"), F.lit(0)))

df_all_score_temp = df_all_score_temp.withColumn("logit_tas_volunteer",
    -14.6085 +
    (F.col("Party_DEM") * 0.5458) +
    (F.col("Party_REP") * 0.9269) +
    (F.col("vol_ct_24mos") * 1.7237) +
    (F.col("retire") * -0.7563) +
    (F.col("liveanswer_freq_3") * 0.3286) +
    (F.col("liveanswer_freq3_6") * 0.9735) +
    (F.col("state_activities_12mo_n") * 0.2056) +
    (F.col("yeas_survey_12mo_i") * 0.8191) +
    (F.col("other_vol_12mo_i") * 2.5871) +
    (F.col("goi_1_dum") * 0.8069) +
    (F.col("acevflag_c") * 0.6042) +
    (F.col("ch_acq_F") * -1.0503) +
    (F.col("gender_M") * -0.3816) +
    (F.col("HomVal_Home_Value_CBSA_Index_c") * 0.00539) +
    (F.col("PERCENTASIAN_c") * 0.0735) +
    (F.col("household_nols18_c") * 0.0685) +
    (F.col("PERSONS_PER_HH") * 1.2317) +
    (F.col("PERCENT_CIVILIAN_VET") * 0.0531) +
    (F.col("OOHU_MEDIAN_HOME_VAL_c") * -0.00000523) +
    (F.col("PCT_HOME_VAL_LS10K") * 0.038) +
    (F.col("age_57to63_dum") * -0.6562) +
    (F.col("SY_GENERALACTIVIST_c") * 0.00887) +
    (F.col("SY_OTSBN_POLFUND_c") * -0.0347)
)
df_all_score_temp = df_all_score_temp.withColumn("tas_score", F.exp(F.col("logit_tas_volunteer")) / (1 + F.exp(F.col("logit_tas_volunteer"))))

df_all_score_temp = df_all_score_temp.withColumn("ethniccode_20_dum", F.when(F.col("EthnicCode") == '20', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("VoterStatus_active", F.when(F.col("VoterStatus") == 'active', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("hitech_dum", F.when(F.col("hitech_merch") == '1', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("otherdonors_dum", F.when(F.col("other_donors") == '1', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("vehicle_3_dum", F.when(F.col("vehicle_known_owned_number") == '3', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("deadwood_dum", F.when(F.col("deadwood_model").isin('DEAD', 'PROBDEAD'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("child_0to5_dum", F.when(F.col("IBX_CHILD_AGE_00_05_AGG_HHD") == 1, 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("child_6to10_dum", F.when(F.col("IBX_CHILD_AGE_06_10_AGG_HHD") == 1, 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("community_vetera_dum", F.when(F.col("IBX_COMMUNITY_INVOLVEMENT_VETERA") == 1, 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("homevalue_9to10_dum", F.when(F.col("IBX_HOME_MARKET_VALUE_DECILES_AG").isin('09', '10'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("homerange_a_dum", F.when(F.col("IBX_HOME_PURCHASED_AMT_RANGES_AG") == 'A', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("networth_b_dum", F.when(F.col("IBX_NETWORTH_PREMIER_AGG_HHD") == 'B', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("TELECOM_20PCT_LONG_DISTANCE_123", F.when(F.col("IBX_TELECOM_20PCT_LONG_DISTANCE_").isin('01', '02', '03'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("cell_1to2", F.when(F.col("IBX_TELECOM_CELLULAR_AGG_HHD").isin('01', '02'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("Party_OTH", F.when(F.col("Party_Aff") == 'OTH', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("E_Orthodox", F.when(F.col("Religion") == 'O', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("Inc_HH_Median_HH_Income_c", F.coalesce(F.col("CENS_HOMVAL_HOME_VALUE_CBSA_INDE"), F.lit(72018.51)))
df_all_score_temp = df_all_score_temp.withColumn("MemXRenew_c", F.coalesce(F.col("MemXRenew"), F.lit(5.26)))
df_all_score_temp = df_all_score_temp.withColumn("OCCHU_Median_Length_of_Resi_c", F.coalesce(F.col("OCCHU_Median_Length_of_Residence"), F.lit(973.55)))
df_all_score_temp = df_all_score_temp.withColumn("Pop_pct_Asian_Only_Hisp_c", F.coalesce(F.col("Pop_pct_Asian_Only_Hisp"), F.lit(0.5076)))
df_all_score_temp = df_all_score_temp.withColumn("Pop_pct_Asian_Only__c", F.coalesce(F.col("Pop_pct_Asian_Only_"), F.lit(50.5046)))
df_all_score_temp = df_all_score_temp.withColumn("Pop_pct_Black_Only_Hisp_c", F.coalesce(F.col("Pop_pct_Black_Only_Hisp"), F.lit(2.1350)))
df_all_score_temp = df_all_score_temp.withColumn("donfnd12_c", F.coalesce(F.col("donfnd12"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("ideology_c", F.coalesce(F.col("ideology"), F.lit(42.72)))
df_all_score_temp = df_all_score_temp.withColumn("partisanscore_c", F.coalesce(F.col("partisanscore"), F.lit(49.49)))
df_all_score_temp = df_all_score_temp.withColumn("rpm_score_c", F.coalesce(F.col("rpm_score"), F.lit(9.2585)))
df_all_score_temp = df_all_score_temp.withColumn("PERCENTCAUCASIANANDOTHER_c", F.coalesce(F.col("CENS_ETHNIC_POP_PERCENT_WHITE_ON"), F.lit(88.37)))
df_all_score_temp = df_all_score_temp.withColumn("PERCENTHOMEOWNERS_c", F.coalesce(F.col("cens_tenancy_occhu_percent_owner"), F.lit(74.03)))
df_all_score_temp = df_all_score_temp.withColumn("PERCENTUNEMPLOYED_c", F.coalesce(F.col("CENS_EMPLOY_LABF_PERCENT_UNEMPLO"), F.lit(3.66)))
df_all_score_temp = df_all_score_temp.withColumn("PERCENTWHITECOLLAR_c", F.coalesce(F.col("cens_WHITECOLLAR"), F.lit(41.38)))
df_all_score_temp = df_all_score_temp.withColumn("SY_FISCALPOLICYMODEL_c", F.coalesce(F.col("FISCAL_POLICY_MODEL"), F.lit(44.98)))
df_all_score_temp = df_all_score_temp.withColumn("SY_HUNTERMODEL_c", F.coalesce(F.col("HUNTER_MODEL"), F.lit(0.358)))
df_all_score_temp = df_all_score_temp.withColumn("CENS_EARN_HH_PERCENT_WITH_SUPP_c", F.coalesce(F.col("CENS_AGE_POP_MEDIAN_AGE_OF_MALES"), F.lit(40)))
df_all_score_temp = df_all_score_temp.withColumn("CENS_EARN_HH_PERCENT_NO_SUPPLE_c", F.coalesce(F.col("CENS_AGE_POP_MEDIAN_AGE_OF_FEMAL"), F.lit(42)))
df_all_score_temp = df_all_score_temp.withColumn("CENS_COMMUTE_WRKRS_PERCENT_DRO_c", F.coalesce(F.col("CENS_AGE_POP_PERCENT_45_54"), F.lit(14.68)))
df_all_score_temp = df_all_score_temp.withColumn("CENS_COMMUTE_WRKRS_PERCENT_WOR_c", F.coalesce(F.col("CENS_AGE_POP_PERCENT_55_59"), F.lit(7.59)))
df_all_score_temp = df_all_score_temp.withColumn("CENS_EARN_HH_PERCENT_NO_WAGE_c_c", F.coalesce(F.col("CENS_AGE_POP_PERCENT_65_99_PLUS"), F.lit(16.82)))
df_all_score_temp = df_all_score_temp.withColumn("avg_commte", F.coalesce(F.col("CENS_COMMUTE_COMMUTER_AVG_TRAV_T"), F.lit(26)))
df_all_score_temp = df_all_score_temp.withColumn("commute_pct_cp", F.coalesce(F.col("CENS_COMMUTE_WRKRS_PERCENT_CARPO"), F.lit(9.05)))
df_all_score_temp = df_all_score_temp.withColumn("RENTAL_UNITS_c", F.coalesce(F.col("CENS_COUNT_RENTAL_UNITS"), F.lit(179.95)))
df_all_score_temp = df_all_score_temp.withColumn("PERCENT_NO_EARNINGS", F.coalesce(F.col("CENS_EARN_HH_PERCENT_NO_EARNINGS"), F.lit(20.69)))
df_all_score_temp = df_all_score_temp.withColumn("PERCENT_BLACK_c", F.coalesce(F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON"), F.lit(3.7)))
df_all_score_temp = df_all_score_temp.withColumn("PERCENT_WHITE", F.coalesce(F.col("CENS_ETHNIC_POP_PERCENT_WHITE_ON"), F.lit(83.91)))
df_all_score_temp = df_all_score_temp.withColumn("MEDIAN_HH_inc", F.coalesce(F.col("CENS_INC_HH_MEDIAN_HOUSEHOLD_INC"), F.lit(72748.94)))
df_all_score_temp = df_all_score_temp.withColumn("EMP_EDUCATION", F.coalesce(F.col("CENS_INDUS_EMPLD_PERCENT_EDUCATI"), F.lit(10.24)))
df_all_score_temp = df_all_score_temp.withColumn("EMP_FINANCE", F.coalesce(F.col("CENS_INDUS_EMPLD_PERCENT_FINANCE"), F.lit(5.98)))
df_all_score_temp = df_all_score_temp.withColumn("ENG_SP", F.coalesce(F.col("CENS_LANG_HH_PERCENT_ENGLISH_SPE"), F.lit(84.96)))
df_all_score_temp = df_all_score_temp.withColumn("NO_MORTG", F.coalesce(F.col("CENS_MORTG_OOHU_PERCENT_NO_MORTG"), F.lit(31.97)))
df_all_score_temp = df_all_score_temp.withColumn("VOTEPROP2016_c", F.coalesce(F.col("general_election_vote_propensity"), F.lit(85.126)))
df_all_score_temp = df_all_score_temp.withColumn("national_activities_12mo", F.coalesce(F.col("national_activities_12mo"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("driver_online_12mo", F.coalesce(F.col("driver_online_12mo"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("advocacy_petitions_12mo_n", F.coalesce(F.col("advocacy_petitions_12mo"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("driver_class_12mo_i_n", F.coalesce(F.col("driver_class_12mo_i"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("advocacy_donations_12mo_n", F.coalesce(F.col("advocacy_donations_12mo"), F.lit(0)))

df_all_score_temp = df_all_score_temp.withColumn("logit_ss", 
    1.0767 +
    (F.col("Party_DEM") * -0.2954) +
    (F.col("Party_OTH") * -0.3819) +
    (F.col("Party_REP") * -0.1034) +
    (F.col("E_Orthodox") * -0.1914) +
    (F.col("retire") * 0.0881) +
    (F.col("liveanswer_freq_3") * 0.3109) +
    (F.col("liveanswer_freq3_6") * 0.3716) +
    (F.col("liveanswer_freq6_12") * 0.2133) +
    (F.col("national_activities_12mo") * 1.6896) +
    (F.col("driver_online_12mo") * -0.5416) +
    (F.col("advocacy_donations_12mo_n") * -0.071) +
    (F.col("newsletter_opens_cnt_12mo_c") * 0.00627) +
    (F.col("teletown_12mo_ic") * -0.3011) +
    (F.col("ethniccode_20_dum") * -0.1863) +
    (F.col("VoterStatus_active") * -0.4103) +
    (F.col("ch_acq_U") * -0.0694) +
    (F.col("Marital_S") * 0.054) +
    (F.col("audio_visual_composite_c") * -0.0413) +
    (F.col("gender_M") * -0.1015) +
    (F.col("hitech_dum") * -0.068) +
    (F.col("home_owner") * -0.0635) +
    (F.col("otherdonors_dum") * 0.0744) +
    (F.col("vehicle_3_dum") * 0.1622) +
    (F.col("deadwood_dum") * -0.2757) +
    (F.col("sy_dense") * 0.0461) +
    (F.col("workcluster_wealthy") * 0.085) +
    (F.col("workcluster_6_dum") * 0.1314) +
    (F.col("child_0to5_dum") * -0.1612) +
    (F.col("child_6to10_dum") * -0.0898) +
    (F.col("community_health_dum") * -0.052) +
    (F.col("homevalue_9to10_dum") * 0.0878) +
    (F.col("homerange_a_dum") * 0.1078) +
    (F.col("income_1to4_dum") * 0.0587) +
    (F.col("income_du_dum") * -0.0655) +
    (F.col("networth_b_dum") * 0.1489) +
    (F.col("TELECOM_20PCT_LONG_DISTANCE_123") * 0.1135) +
    (F.col("TELECOM_CALLING_SERVICES_AGG123") * -0.0573) +
    (F.col("cell_1to2") * -0.1146) +
    (F.col("internet_1") * -0.1404) +
    (F.col("VEHICLE_DOMINANT_C") * -0.0533) +
    (F.col("VEHICLE_OWNED_y") * 0.211) +
    (F.col("HH_pct_Spanish_Speaking_c") * 0.00203) +
    (F.col("HomVal_Home_Value_CBSA_Index_c") * 0.000885) +
    (F.col("Inc_HH_Median_HH_Income_c") * -0.00079) +
    (F.col("MemXRenew_c") * 0.0293) +
    (F.col("OCCHU_Median_Length_of_Resi_c") * -0.00051) +
    (F.col("OOHU_Median_Home_Value_c") * -0.00000099) +
    (F.col("Pop_pct_Asian_Only_Hisp_c") * -0.0236) +
    (F.col("Pop_pct_Asian_Only__c") * 0.00263) +
    (F.col("Pop_pct_Black_Only_Hisp_c") * 0.0192) +
    (F.col("diversity_flag_c") * 0.1322) +
    (F.col("drvsafe_pro_em_c") * -0.0087) +
    (F.col("ideology_c") * -0.0147) +
    (F.col("partisanscore_c") * 0.00617) +
    (F.col("rpm_score_c") * -0.0175) +
    (F.col("PERCENTASIAN_c") * -0.0284) +
    (F.col("PERCENTCAUCASIANANDOTHER_c") * -0.00432) +
    (F.col("PERCENT_WHITE") * 0.0117) +
    (F.col("PERCENTCOLLEGEGRADS_c") * 0.00928) +
    (F.col("PERCENTHOMEOWNERS_c") * 0.00218) +
    (F.col("PERCENTMANAGEMENTPROFESSIONALS_c") * -0.00867) +
    (F.col("PERCENTSINGLEUNITDWELLINGS_c") * -0.00157) +
    (F.col("PERCENTUNEMPLOYED_c") * -0.0193) +
    (F.col("PERCENTWHITECOLLAR_c") * 0.00449) +
    (F.col("SY_FISCALPOLICYMODEL_c") * -0.00299) +
    (F.col("SY_GUNSCORE_c") * 0.3242) +
    (F.col("SY_HUNTERMODEL_c") * 0.1687) +
    (F.col("CENS_EARN_HH_PERCENT_WITH_SUPP_c") * -0.0156) +
    (F.col("CENS_EARN_HH_PERCENT_NO_SUPPLE_c") * -0.011) +
    (F.col("CENS_COMMUTE_WRKRS_PERCENT_DRO_c") * 0.1017) +
    (F.col("CENS_COMMUTE_WRKRS_PERCENT_WOR_c") * -0.0775) +
    (F.col("CENS_EARN_HH_PERCENT_NO_EARNIN_c") * -0.0232) +
    (F.col("CENS_EARN_HH_PERCENT_NO_WAGE_c_c") * 0.0258) +
    (F.col("household_nols18_c") * -0.018) +
    (F.col("commute_pct_ls30") * -0.00727) +
    (F.col("avg_commte") * -0.0436) +
    (F.col("commute_pct_cp") * 0.0105) +
    (F.col("commute_pct_public") * -0.0514) +
    (F.col("RENTAL_UNITS_c") * -0.00037) +
    (F.col("WORKERS_c") * 0.000129) +
    (F.col("PERSONS_PER_HH") * -0.4556) +
    (F.col("PERCENT_NO_EARNINGS") * -0.00788) +
    (F.col("POP25_PLUS_MEDIAN_EDUC") * 0.1165) +
    (F.col("POP25_PLUS_PERCENT_BAC") * 0.016) +
    (F.col("PERCENT_EMPLOYED") * 0.00536) +
    (F.col("PERCENT_BLACK_c") * 0.0208) +
    (F.col("PERCENT_FEMALE") * -0.00757) +
    (F.col("UTILITY_gas") * -0.0185) +
    (F.col("HH_PERCENT_2_PERSONS") * 0.029) +
    (F.col("PCT_HOME_VAL_LS10K") * -0.0135) +
    (F.col("MEDIAN_HH_inc") * 0.000784) +
    (F.col("EMP_ACCOR_FOOD") * 0.00798) +
    (F.col("EMP_EDUCATION") * -0.00801) +
    (F.col("EMP_FINANCE") * 0.00701) +
    (F.col("ENG_SP") * 0.00843) +
    (F.col("SPOUSE_PRS") * 0.00673) +
    (F.col("WIDOWED") * 0.0227) +
    (F.col("NO_MORTG") * 0.0055) +
    (F.col("SY_GENERALACTIVIST_c") * 0.00903) +
    (F.col("SY_OTSBN_POLFUND_c") * -0.0295) +
    (F.col("VOTEPROP2016_c") * -0.00312)
)
df_all_score_temp = df_all_score_temp.withColumn("score_ss", F.exp(F.col("logit_ss")) / (1 + F.exp(F.col("logit_ss"))))

df_all_score_temp = df_all_score_temp.withColumn("masters39_1_dum", F.when(F.col("old_score39_vigintile").isin('12', '11', '8', '4'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("masters39_19to20_dum", F.when(F.col("old_score39_vigintile").isin('1', '2', '20'), 1).otherwise(0))

df_all_score_temp = df_all_score_temp.withColumn("logit_ss2", 
    -0.64 +
    (F.col("Party_OTH") * -0.1467) +
    (F.col("Catholic") * 0.0628) +
    (F.col("liveanswer_freq_3") * -0.2011) +
    (F.col("poll_noaskct") * -0.0769) +
    (F.col("advocacy_petitions_12mo_n") * 0.0472) +
    (F.col("driver_class_12mo_i_n") * 0.2922) +
    (F.col("activist_i") * 0.1274) +
    (F.col("community_vetera_dum") * 0.0658) +
    (F.col("TRAVEL_TYPE_b") * -0.0601) +
    (F.col("VEHICLE_OWNED_y") * 0.089) +
    (F.col("masters39_1_dum") * 0.0572) +
    (F.col("masters39_19to20_dum") * -0.1381) +
    (F.col("HomVal_Home_Value_CBSA_Index_c") * 0.00265) +
    (F.col("OOHU_Median_Home_Value_c") * -0.00000205) +
    (F.col("donfnd12_c") * -0.0854) +
    (F.col("weekday") * -0.0784) +
    (F.col("SY_EDUCSCORE_c") * 0.2603) +
    (F.col("SY_HUNTERMODEL_c") * -0.1032) +
    (F.col("CENS_COMMUTE_WRKRS_PERCENT_WOR_c") * -0.0151) +
    (F.col("POPULATION_c") * 0.000016) +
    (F.col("PERCENT_BLACK_c") * 0.0128) +
    (F.col("PERCENT_WHITE") * 0.0102) +
    (F.col("UTILITY_gas") * -0.00129) +
    (F.col("HH_PERCENT_2_PERSONS") * -0.00401) +
    (F.col("MEDIAN_HH_inc") * 0.000002484) +
    (F.col("EMP_FINANCE") * 0.0108) +
    (F.col("ENG_SP") * -0.00301) +
    (F.col("NO_MORTG") * 0.0033)
)
df_all_score_temp = df_all_score_temp.withColumn("score_ss2", F.exp(F.col("logit_ss2")) / (1 + F.exp(F.col("logit_ss2"))))
df_all_score_temp = df_all_score_temp.withColumn("comb_score_ss", F.col("score_ss") * F.col("score_ss2"))

df_all_score_temp = df_all_score_temp.withColumn("curterm_60_dum", F.when(F.col("cur_term") == '60', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("IBX_ADULTS_NUM_AGG_HHDls3", F.when(F.col("IBX_ADULTS_NUM_AGG_HHD").isin('1', '2'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("homebiz_dum", F.when(F.col("IBX_HOME_BUSINESS_AGG_HHD") == 'Y', 1).otherwise(0))

df_all_score_temp = df_all_score_temp.withColumn("logit_fndn_housing", 
    -7.5884 +
    (F.col("advocacy_petitions_12mo_n") * 0.1036) +
    (F.col("teletown_12mo_ic") * 0.7792) +
    (F.col("goi_missing_dum") * -0.2741) +
    (F.col("curterm_36_dum") * -0.2867) +
    (F.col("curterm_60_dum") * -0.5409) +
    (F.col("Marital_S") * 0.4661) +
    (F.col("otherdonors_dum") * 0.2639) +
    (F.col("IBX_ADULTS_NUM_AGG_HHDls3") * 0.2954) +
    (F.col("child_6to10_dum") * -0.4403) +
    (F.col("community_charity_dum") * 0.3107) +
    (F.col("community_health_dum") * -0.244) +
    (F.col("dwelling_m_dum") * -0.8278) +
    (F.col("homebiz_dum") * 0.2121) +
    (F.col("internet_1") * 0.5194) +
    (F.col("WORKING_WOMAN_y") * 0.2379) +
    (F.col("HomVal_Home_Value_CBSA_Index_c") * -0.00425) +
    (F.col("Pop_pct_Black_Only_Hisp_c") * 0.0138) +
    (F.col("diversity_flag_c") * 0.4498) +
    (F.col("drvsafe_pro_em_c") * -0.0628) +
    (F.col("partisanscore_c") * 0.00623) +
    (F.col("renewals_c") * -0.0422) +
    (F.col("sp_rel_c") * 0.2507) +
    (F.col("PERCENTUNEMPLOYED_c") * 0.0365) +
    (F.col("SY_GUNSCORE_c") * -1.2358) +
    (F.col("UTILITY_gas") * -0.00289) +
    (F.col("age_60to80_dum") * 0.8411)
)
df_all_score_temp = df_all_score_temp.withColumn("fndn_housing_score", F.exp(F.col("logit_fndn_housing")) / (1 + F.exp(F.col("logit_fndn_housing"))))

df_all_score_temp = df_all_score_temp.withColumn("masters9_20_dum", F.when(F.col("old_score9_vigintile") == '20', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("masters31_num", F.coalesce(F.col("old_score31_vigintile").cast("double"), F.lit(9.71617)))

df_all_score_temp = df_all_score_temp.withColumn("advocacy_donors_12mo_i_c", F.coalesce(F.col("advocacy_donors_12mo_i"), F.lit(0.0244875)))
df_all_score_temp = df_all_score_temp.withColumn("advocacy_signers_12mo_i_c", F.coalesce(F.col("advocacy_signers_12mo_i"), F.lit(0.0556328)))
df_all_score_temp = df_all_score_temp.withColumn("cat_n_dum", F.when(F.col("category") == 'N', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("CENS_ETHNIC_POP_PERCENT_HI_c", F.coalesce(F.col("CENS_ETHNIC_POP_PERCENT_HI_NAT_O"), F.lit(0.1316017)))
df_all_score_temp = df_all_score_temp.withColumn("fndnmodel_dum", F.when((F.col("fndnothr_score") >= 1) & (F.col("fndnothr_score") <= 16), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("foundation_donors_12mo_i_c", F.coalesce(F.col("foundation_donors_12mo_i"), F.lit(0.000732289)))
df_all_score_temp = df_all_score_temp.withColumn("individual_engagers_12mo_c", F.coalesce(F.col("individual_engagers_12mo"), F.lit(0.3749404)))
df_all_score_temp = df_all_score_temp.withColumn("MemXRenew_c", F.coalesce(F.col("MemXRenew"), F.lit(4.1587933)))
df_all_score_temp = df_all_score_temp.withColumn("teletown_12mo_i_c", F.coalesce(F.col("teletown_12mo_i"), F.lit(0.000030954)))
df_all_score_temp = df_all_score_temp.withColumn("VoterStatus_dropped", F.when(F.col("VoterStatus") == 'dropped', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("foundation_donors_12mo_i", F.coalesce(F.col("foundation_donors_12mo_i"), F.lit(0.0632636)))
df_all_score_temp = df_all_score_temp.withColumn("driver_online_12mo_i_c", F.coalesce(F.col("driver_online_12mo_i"), F.lit(0.0016107)))
df_all_score_temp = df_all_score_temp.withColumn("state_activity_12mo_i_c", F.coalesce(F.col("state_activity_12mo_i"), F.lit(0.0022009)))
df_all_score_temp = df_all_score_temp.withColumn("contact_leg_12mo_i_c", F.coalesce(F.col("contact_leg_12mo_i"), F.lit(0.0036116)))
df_all_score_temp = df_all_score_temp.withColumn("driver_class_12mo_i_c", F.coalesce(F.col("driver_class_12mo_i"), F.lit(0.0077633)))
df_all_score_temp = df_all_score_temp.withColumn("foundation_donors_box_12mo_i_c", F.coalesce(F.col("foundation_donors_checkb_12mo_i"), F.lit(0.0091639)))
df_all_score_temp = df_all_score_temp.withColumn("teletown_12mo_i_c", F.coalesce(F.col("teletown_12mo_i"), F.lit(0.012876)))
df_all_score_temp = df_all_score_temp.withColumn("FNDN_AARPPRO_DM_score_c", F.coalesce(F.col("FNDN_AARPPRO_DM_score"), F.lit(0.0138623)))
df_all_score_temp = df_all_score_temp.withColumn("diversity_dum", F.when(F.col("diversity_flag_agg_ind").isin(1, 3), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("advocacy_donors_12mo_i_c", F.coalesce(F.col("advocacy_donors_12mo_i"), F.lit(0.0424382)))
df_all_score_temp = df_all_score_temp.withColumn("masters26_17to20_dum", F.when(F.col("old_score26_vigintile").isin('17', '18', '19', '20'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("foundation_donors_12mo_i_c", F.coalesce(F.col("foundation_donors_12mo_i"), F.lit(0.0638174)))
df_all_score_temp = df_all_score_temp.withColumn("advocacy_donations_12mo_c", F.coalesce(F.col("advocacy_donations_12mo"), F.lit(0.065228)))
df_all_score_temp = df_all_score_temp.withColumn("advocacy_signers_12mo_i_c", F.coalesce(F.col("advocacy_signers_12mo_i"), F.lit(0.0719009)))
df_all_score_temp = df_all_score_temp.withColumn("masters6_1to5_dum", F.when(F.col("old_score6_vigintile").isin('1', '2', '3', '4', '5'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("CENS_ETHNIC_POP_PERCENT_HI_c", F.coalesce(F.col("CENS_ETHNIC_POP_PERCENT_HI_NAT_O"), F.lit(0.1222503)))
df_all_score_temp = df_all_score_temp.withColumn("advocacy_petitions_12mo_c", F.coalesce(F.col("advocacy_petitions_12mo"), F.lit(0.1426613)))
df_all_score_temp = df_all_score_temp.withColumn("internet_10_dum", F.when(F.col("IBX_TELECOM_INTERNET_AGG_HHD") == '10', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("masters38_17to20_dum", F.when(F.col("old_score38_vigintile").isin('17', '18', '19', '20'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("senior_dum", F.when(F.col("ibx_presence_of_senior_adult_agg") == 'Y', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("individual_engagers_12mo_c", F.coalesce(F.col("individual_engagers_12mo"), F.lit(0.3676381)))
df_all_score_temp = df_all_score_temp.withColumn("Female", F.when(F.col("Gender_agg_ind") == 'F', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("masters22_16to20_dum", F.when(F.col("old_score22_vigintile").isin('16', '17', '18', '19', '20'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("age70_dum", F.when(F.col("age_agg_ind") >= 70, 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("married_dum", F.when(F.col("maritalStatus") == 'M', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("Fndn_TTD_Num_c", F.coalesce(F.col("fndn_ttd_num"), F.lit(3.2920469)))
df_all_score_temp = df_all_score_temp.withColumn("PERCENTHISPANIC_c", F.coalesce(F.col("cens_ethnic_pop_percent_hispanic"), F.lit(6.5840589)))
df_all_score_temp = df_all_score_temp.withColumn("sy_otsbn_polfund_2012b_c", F.coalesce(F.col("sy_otsbn_polfund_2012b"), F.lit(45.5317558)))

df_all_score_temp = df_all_score_temp.withColumn("logit_fraudwatch",
    -2.6138 +
    (F.col("driver_online_12mo_i_c") * -0.5431) +
    (F.col("state_activity_12mo_i_c") * 0.3145) +
    (F.col("contact_leg_12mo_i_c") * -0.0977) +
    (F.col("driver_class_12mo_i_c") * 0.2249) +
    (F.col("foundation_donors_box_12mo_i_c") * -0.0785) +
    (F.col("teletown_12mo_i_c") * 0.0556) +
    (F.col("fndn_aarppro_dm_score_c") * 2.6253) +
    (F.col("ethniccode_20_dum") * 0.5453) +
    (F.col("diversity_dum") * -0.2963) +
    (F.col("masters9_20_dum") * 0.1395) +
    (F.col("advocacy_donors_12mo_i_c") * -0.3395) +
    (F.col("masters26_17to20_dum") * -0.062) +
    (F.col("foundation_donors_12mo_i_c") * -0.0366) +
    (F.col("advocacy_donations_12mo_c") * 0.1086) +
    (F.col("advocacy_signers_12mo_i_c") * 0.2303) +
    (F.col("masters6_1to5_dum") * -0.184) +
    (F.col("CENS_ETHNIC_POP_PERCENT_HI_c") * -0.1468) +
    (F.col("advocacy_petitions_12mo_c") * -0.0488) +
    (F.col("vehicle_3_dum") * -0.0642) +
    (F.col("job_78_dum") * 0.0604) +
    (F.col("curterm_36_dum") * 0.0477) +
    (F.col("internet_10_dum") * 0.1058) +
    (F.col("curterm_60_dum") * 0.1598) +
    (F.col("masters38_17to20_dum") * -0.0353) +
    (F.col("senior_dum") * 0.076) +
    (F.col("individual_engagers_12mo_c") * 0.00912) +
    (F.col("Female") * -0.1038) +
    (F.col("community_charity_dum") * 0.1074) +
    (F.col("masters22_16to20_dum") * -0.0722) +
    (F.col("age70_dum") * 0.1049) +
    (F.col("married_dum") * 0.0943) +
    (F.col("Fndn_TTD_Num_c") * -0.0101) +
    (F.col("CENS_ETHNIC_POP_PERCENT_SOME_c") * 0.00589) +
    (F.col("MemXRenew_c") * 0.0372) +
    (F.col("PERCENTHISPANIC_c") * -0.00296) +
    (F.col("sy_otsbn_polfund_2012b_c") * 0.00186)
)
df_all_score_temp = df_all_score_temp.withColumn("score_fraudwatch", F.exp(F.col("logit_fraudwatch")) / (1 + F.exp(F.col("logit_fraudwatch"))))

df_all_score_temp = df_all_score_temp.withColumn("mail_health_dum", F.when(F.col("IBX_MAIL_BUYER_CAT_HEALTH_AGG_HH") == '1', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("percent_black_change", F.when((F.col("PERCENT_BLACK_c").isNull()) & (F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON").isNotNull()), 1))
df_all_score_temp = df_all_score_temp.withColumn("PERCENT_BLACK_c", 
    F.when(F.col("percent_black_change") == 1, F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON") * 0.1)
    .otherwise(F.coalesce(F.col("CENS_ETHNIC_POP_PERCENT_BLACK_ON"), F.lit(3.7)))
)

df_all_score_temp = df_all_score_temp.withColumn("AAB_dum", F.when(F.col("diversity_flag_agg_ind") == 2, 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("acevnum_dum", F.when(F.col("acev_num").isin('', '0'), 0).otherwise(1))
df_all_score_temp = df_all_score_temp.withColumn("full_time", F.when(F.col("Work_Status") == 'F', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("POPULATION_PER_SQUA", F.coalesce(F.col("CENS_DENSITY_POPULATION_PER_SQUA"), F.lit(4165)))
df_all_score_temp = df_all_score_temp.withColumn("PERCENT_HISPANIC", F.coalesce(F.col("CENS_ETHNIC_POP_PERCENT_HISPANIC"), F.lit(0.4)))
df_all_score_temp = df_all_score_temp.withColumn("education_3_dum", F.when(F.col("IBX_EDUCATION") == '3', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("contact_leg_12mo_i", F.coalesce(F.col("contact_leg_12mo_i"), F.lit(0)))

df_all_score_temp = df_all_score_temp.withColumn("political_c", F.when(F.col("political") == '1', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("diversity_flag_2", F.when(F.col("diversity_flag_agg_ind") == 2, 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("diversity_flag_3", F.when(F.col("diversity_flag_agg_ind") == 3, 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("POP_PERCENT_NON_HISP", F.coalesce(F.col("CENS_ETHNIC_POP_PERCENT_NON_HISP"), F.lit(90.59)))
df_all_score_temp = df_all_score_temp.withColumn("EMP_HLTH_CARE", F.coalesce(F.col("CENS_INDUS_EMPLD_PERCENT_HLTH_CA"), F.lit(13.94)))
df_all_score_temp = df_all_score_temp.withColumn("state_activity_12mo_i", F.coalesce(F.col("state_activity_12mo_i"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("advocacy_signers_12mo_i", F.coalesce(F.col("advocacy_signers_12mo_i"), F.lit(0)))

df_all_score_temp = df_all_score_temp.withColumn("logit_CPD_CARE_ATTEND", 
    -2.1823 +
    (F.col("ch_acq_U") * -0.2999) +
    (F.col("diversity_flag_2") * 0.4312) +
    (F.col("diversity_flag_3") * 0.5254) +
    (F.col("political_c") * 0.1948) +
    (F.col("advo_s1_dum") * -0.1279) +
    (F.col("curterm_60_dum") * -0.232) +
    (F.col("curterm_36_dum") * -0.3839) +
    (F.col("goi_missing_dum") * -0.4718) +
    (F.col("gender_M") * -0.5638) +
    (F.col("CENS_ETHNIC_POP_PERCENT_HI_c") * 0.1442) +
    (F.col("state_activities_12mo_n") * 0.0868) +
    (F.col("vehicle_3_dum") * -0.1855) +
    (F.col("POP_PERCENT_NON_HISP") * -0.0207) +
    (F.col("mailercount_click") * 0.1251) +
    (F.col("mailct") * 0.0647) +
    (F.col("call_freq") * 0.0404) +
    (F.col("CENS_ETHNIC_POP_PERCENT_SOME_c") * -0.0208) +
    (F.col("mailercount_open") * -0.0455) +
    (F.col("PERCENTCAUCASIANANDOTHER_c") * -0.00981) +
    (F.col("PCT_HOME_VAL_LS10K") * 0.0187) +
    (F.col("SPOUSE_PRS") * -0.00715) +
    (F.col("EMP_FINANCE") * -0.0189) +
    (F.col("PERCENTWHITECOLLAR_c") * -0.0118) +
    (F.col("PERCENTBLUECOLLAR_c") * -0.0165) +
    (F.col("num_click") * -0.0162) +
    (F.col("EMP_EDUCATION") * -0.00909) +
    (F.col("Pop_pct_Black_Only_Hisp_c") * -0.0083) +
    (F.col("ENG_SP") * 0.0104) +
    (F.col("commute_pct_cp") * -0.01) +
    (F.col("EMP_HLTH_CARE") * -0.00972) +
    (F.col("sy_otsbn_polfund_2012b_c") * 0.00413) +
    (F.col("SY_GENERALACTIVIST_c") * 0.00374) +
    (F.col("num_open") * 0.00215) +
    (F.col("drvsafe_pro_em_c") * -0.0176) +
    (F.col("RENT_c") * -0.00045) +
    (F.col("state_activity_12mo_i") * 2.0339) +
    (F.col("driver_class_12mo_i_n") * 0.6536) +
    (F.col("teletown_12mo_ic") * 0.8515) +
    (F.col("advocacy_signers_12mo_i") * 0.3107) +
    (F.col("aarporg_i") * -0.15) +
    (F.col("ethniccode_20_dum") * 0.3628) +
    (F.col("goi_1_dum") * -0.8097) +
    (F.col("acevflag_c") * 0.8597) +
    (F.col("emailable_dum") * 0.6841)
)
df_all_score_temp = df_all_score_temp.withColumn("CPD_CARE_ATTEND_score", F.exp(F.col("logit_CPD_CARE_ATTEND")) / (1 + F.exp(F.col("logit_CPD_CARE_ATTEND"))))

df_all_score_temp = df_all_score_temp.withColumn("FAMILY_HOUSEHOLDS__c", F.coalesce(F.col("CENS_COUNT_FAMILY_HOUSEHOLDS"), F.lit(463.7)))
df_all_score_temp = df_all_score_temp.withColumn("personic_2_dum", F.when(F.col("IBX_PERSONIC_CLUSTER") == '02', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("age_57to64_dum", F.when((F.col("age_agg_ind") > 57) & (F.col("age_agg_ind") <= 64), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("driver_online_12mo_i", F.coalesce(F.col("driver_online_12mo_i"), F.lit(0)))

df_all_score_temp = df_all_score_temp.withColumn("logit_CPD_JOBS_ATTEND",
    -7.8549 +
    (F.col("liveanswer_freq6_12") * 0.2544) +
    (F.col("mailct") * 0.2592) +
    (F.col("sy_otsbn_polfund_2012a_c") * -0.0119) +
    (F.col("VOTEPROP2016_c") * 0.00577) +
    (F.col("driver_online_12mo_i") * 1.4755) +
    (F.col("SY_GUNSCORE_c") * -1.3724) +
    (F.col("personic_2_dum") * -1.0532) +
    (F.col("sy_otsbn_polfund_2012b_c") * 0.0104) +
    (F.col("POP25_PLUS_MEDIAN_EDUC") * 0.1736) +
    (F.col("FAMILY_HOUSEHOLDS__c") * 0.000228) +
    (F.col("Party_NPA") * -0.5263) +
    (F.col("CENS_EARN_HH_PERCENT_NO_WAGE_c_c") * -0.0173) +
    (F.col("SPOUSE_PRS") * -0.0186) +
    (F.col("ENG_SP") * 0.0107) +
    (F.col("drvsafe_pro_em_c") * -0.0144) +
    (F.col("age_57to64_dum") * 0.2407) +
    (F.col("state_activity_12mo_i") * 1.6343) +
    (F.col("driver_class_12mo_i_n") * 1.2326)
)
df_all_score_temp = df_all_score_temp.withColumn("CPD_JOBS_ATTEND_score", F.exp(F.col("logit_CPD_JOBS_ATTEND")) / (1 + F.exp(F.col("logit_CPD_JOBS_ATTEND"))))

df_all_score_temp = df_all_score_temp.withColumn("Past12MoTouchCt_Financial2", F.when(F.col("Past12MoTouchCt_Financial") == '2', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("adults_number_23", F.when(F.col("IBX_ADULTS_NUM_AGG_HHD").isin('2', '3'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("current_politicsy", F.when(F.col("ibx_current_affairs_politics_pre") == '1', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("GeneralElectn2012_AM", F.when(F.col("GeneralElectn2012").isin('A', 'M'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("R1_number_of_lines_of_credit", 
    F.when(F.col("IBX_NUM_OF_LINES_OF_CREDIT").isin(' ', '3', '4', '9'), 97.309716204)
    .when(F.col("IBX_NUM_OF_LINES_OF_CREDIT").isin('1', '2', '8'), 101.27671586)
    .otherwise(105.00579691)
)
df_all_score_temp = df_all_score_temp.withColumn("R1_IBX_HOUSEHOLD_INCOME1", F.when(F.col("IBX_HOUSEHOLD_INCOME") == '1', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("NEW_SCORE23_CENTILE", F.coalesce(F.col("NEW_SCORE23_CENTILE"), F.lit(57)))
df_all_score_temp = df_all_score_temp.withColumn("Phoenix", F.when(F.col("community").isin('Phoenix', 'Phoenix 2016'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("rpm_score_c", F.coalesce(F.col("rpm_score"), F.lit(9)))
df_all_score_temp = df_all_score_temp.withColumn("sp_rel_c", F.coalesce(F.col("Overall_Active_SP_Reltshps"), F.lit(0.2299647)))
df_all_score_temp = df_all_score_temp.withColumn("lifestage_678", F.when(F.col("Life_stage").isin('6', '7', '8'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("order_term_1yr", F.when(F.col("NA2") == '12', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("cruise_y", F.when(F.col("IBX_TRAVEL_CRUISE_AGG_HHD") == '1', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("age_gt81_dum", F.when(F.col("age_agg_ind") >= 81, 1).otherwise(0))

df_all_score_temp = df_all_score_temp.withColumn("logit_aca", 
    -2.3871 +
    (F.col("liveanswer_freq3_6") * 0.0487) +
    (F.col("Past12MoTouchCt_Financial2") * -0.0879) +
    (F.col("adults_number_23") * 0.0439) +
    (F.col("current_politicsy") * 0.0433) +
    (F.col("GeneralElectn2012_AM") * 0.0514) +
    (F.col("R1_number_of_lines_of_credit") * 0.00644) +
    (F.col("R1_IBX_HOUSEHOLD_INCOME1") * -0.174) +
    (F.col("NEW_SCORE23_CENTILE") * 0.000805) +
    (F.col("Phoenix") * 0.1322) +
    (F.col("care_ct") * -0.0041) +
    (F.col("audio_visual_composite_c") * 0.0408) +
    (F.col("rpm_score_c") * 0.0239) +
    (F.col("MemXRenew_c") * 0.0203) +
    (F.col("sp_rel_c") * -0.0347) +
    (F.col("PERCENTHOMEOWNERS_c") * -0.00093) +
    (F.col("lifestage_678") * 0.1317) +
    (F.col("order_term_1yr") * -0.0791) +
    (F.col("job_78_dum") * 0.039) +
    (F.col("cruise_y") * -0.0501)
)
df_all_score_temp = df_all_score_temp.withColumn("score_aca", F.exp(F.col("logit_aca")) / (1 + F.exp(F.col("logit_aca"))))

df_all_score_temp = df_all_score_temp.withColumn("LIKELY_HISP_AGG_c", F.coalesce(F.col("LIKELY_HISP_AGG"), F.lit(95.9544026)))
df_all_score_temp = df_all_score_temp.withColumn("orders_all_c", F.coalesce(F.col("orders_all"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("orders_12moterm_c", F.coalesce(F.col("orders_12moterm"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("orders_acqmail_c", F.coalesce(F.col("orders_acqmail"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("orders_altmedia_c", F.coalesce(F.col("orders_altmedia"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("orders_online_c", F.coalesce(F.col("orders_online"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("CENS_AGE_POP_PERCENT_30_34_c", F.coalesce(F.col("CENS_AGE_POP_PERCENT_30_34"), F.lit(5.889819)))
df_all_score_temp = df_all_score_temp.withColumn("CENS_EARN_HH_PERCENT_NO_PUBLIC_c", F.coalesce(F.col("CENS_EARN_HH_PERCENT_NO_PUBLIC_A"), F.lit(97.7235297)))
df_all_score_temp = df_all_score_temp.withColumn("CENS_ETHNIC_POP_PERCENT_HI_NAT_c", F.coalesce(F.col("CENS_ETHNIC_POP_PERCENT_HI_NAT_O"), F.lit(0.1455971)))
df_all_score_temp = df_all_score_temp.withColumn("CENS_GRPQTRS_POP_PERCENT_NURSI_c", F.coalesce(F.col("CENS_GRPQTRS_POP_PERCENT_NURSING"), F.lit(0.5538566)))
df_all_score_temp = df_all_score_temp.withColumn("CENS_HEAT_OCCHU_PERCENT_BOTTLE_c", F.coalesce(F.col("CENS_HEAT_OCCHU_PERCENT_BOTTLE_O"), F.lit(5.5785299)))
df_all_score_temp = df_all_score_temp.withColumn("CENS_HEAT_OCCHU_PERCENT_OTHER__c", F.coalesce(F.col("CENS_HEAT_OCCHU_PERCENT_OTHER_HE"), F.lit(0.5102518)))
df_all_score_temp = df_all_score_temp.withColumn("CENS_INDUS_EMPLD_PERCENT_WHOLE_c", F.coalesce(F.col("CENS_INDUS_EMPLD_PERCENT_WHOLESA"), F.lit(2.7510418)))
df_all_score_temp = df_all_score_temp.withColumn("CENS_INDUS_EMPLD_PERCENT_TRANS_c", F.coalesce(F.col("CENS_INDUS_EMPLD_PERCENT_TRANSPO"), F.lit(3.9116536)))
df_all_score_temp = df_all_score_temp.withColumn("CENS_MORTG_OOHU_PERCENT_TWO_MR_c", F.coalesce(F.col("CENS_MORTG_OOHU_PERCENT_TWO_MRTG"), F.lit(0.5288521)))
df_all_score_temp = df_all_score_temp.withColumn("CENS_OCCUP_EMPLD_PERCENT_HEALT_c", F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_HEALTHC"), F.lit(2.2849048)))
df_all_score_temp = df_all_score_temp.withColumn("CENS_URBAN_POP_PERCENT_URBAN_I_c", F.coalesce(F.col("CENS_URBAN_POP_PERCENT_URBAN_IN_"), F.lit(67.5604012)))
df_all_score_temp = df_all_score_temp.withColumn("CENS_MOVE_OCCHU_PERCENT_NEW_LI_c", F.coalesce(F.col("CENS_MOVE_OCCHU_PERCENT_NEW_LIST"), F.lit(24.388775)))
df_all_score_temp = df_all_score_temp.withColumn("GENERAL_ACTIVIST_MODEL_c", F.coalesce(F.col("GENERAL_ACTIVIST_MODEL"), F.lit(58.7223737)))
df_all_score_temp = df_all_score_temp.withColumn("PARTISANscore_c", F.coalesce(F.col("PARTISANscore"), F.lit(53.1617964)))
df_all_score_temp = df_all_score_temp.withColumn("RADIO_c", F.coalesce(F.col("RADIO"), F.lit(34.0590774)))
df_all_score_temp = df_all_score_temp.withColumn("CABLE_c", F.coalesce(F.col("CABLE"), F.lit(65.9248976)))
df_all_score_temp = df_all_score_temp.withColumn("GAME_SHOWS_c", F.coalesce(F.col("GAME_SHOWS"), F.lit(43.290936)))
df_all_score_temp = df_all_score_temp.withColumn("mailercount_click_30_c", F.coalesce(F.col("mailercount_click_30days"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("mailercount_open_365_c", F.coalesce(F.col("mailercount_open"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("driver_safety_vol_12mo_c", F.coalesce(F.col("driver_safety_vol_12mo"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("foundation_donations_checkb_12_c", F.coalesce(F.col("foundation_donations_checkb_12mo"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("SY_GENERALACTIVIST_c", F.coalesce(F.col("GENERAL_ACTIVIST_MODEL"), F.lit(58.7249824)))
df_all_score_temp = df_all_score_temp.withColumn("VOTEPROP2016_c", F.coalesce(F.col("general_election_vote_propensity"), F.lit(83.7027055)))
df_all_score_temp = df_all_score_temp.withColumn("driver_class_12mo_i_c", F.coalesce(F.col("driver_class_12mo_i"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("contact_leg_12mo_i_c", F.coalesce(F.col("contact_leg_12mo_i"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("petition_sign_12mo_i_c", F.coalesce(F.col("petition_sign_12mo_i"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("num_months_c", F.coalesce(F.col("num_months"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("cntct_lifstyle_12mo_agg_hhd_c", F.coalesce(F.col("cntct_lifstyle_12mo_agg_hhd"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("advo_hpc_dt_c", F.coalesce(F.col("advo_hpc_dt"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("renewals_c", F.coalesce(F.col("memxrenew"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("petadv12_c", F.coalesce(F.col("petadv12"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("NEW_SCORE10_CENTILE_c", F.coalesce(F.col("NEW_SCORE10_CENTILE"), F.lit(59.4387314)))
df_all_score_temp = df_all_score_temp.withColumn("NEW_SCORE14_CENTILE_c", F.coalesce(F.col("NEW_SCORE14_CENTILE"), F.lit(46.1087557)))
df_all_score_temp = df_all_score_temp.withColumn("NEW_SCORE17_CENTILE_c", F.coalesce(F.col("NEW_SCORE17_CENTILE"), F.lit(53.923653)))
df_all_score_temp = df_all_score_temp.withColumn("NEW_SCORE18_CENTILE_c", F.coalesce(F.col("NEW_SCORE18_CENTILE"), F.lit(45.25923)))
df_all_score_temp = df_all_score_temp.withColumn("NEW_SCORE29_CENTILE_c", F.coalesce(F.col("NEW_SCORE29_CENTILE"), F.lit(46.8114424)))
df_all_score_temp = df_all_score_temp.withColumn("NEW_SCORE33_CENTILE_c", F.coalesce(F.col("NEW_SCORE33_CENTILE"), F.lit(52.2868898)))
df_all_score_temp = df_all_score_temp.withColumn("NEW_SCORE40_CENTILE_c", F.coalesce(F.col("NEW_SCORE40_CENTILE"), F.lit(56.0724358)))
df_all_score_temp = df_all_score_temp.withColumn("NEW_SCORE41_CENTILE_c", F.coalesce(F.col("NEW_SCORE41_CENTILE"), F.lit(56.9366247)))
df_all_score_temp = df_all_score_temp.withColumn("NEW_SCORE43_CENTILE_c", F.coalesce(F.col("NEW_SCORE43_CENTILE"), F.lit(49.8555553)))
df_all_score_temp = df_all_score_temp.withColumn("ProspMail_c", F.coalesce(F.col("ProspMail"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("Acknow_c", F.coalesce(F.col("Acknow"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("Advo_Petition_c", F.coalesce(F.col("advo_petition"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("totalmailings_c", F.coalesce(F.col("totalmailings"), F.lit(0)))
df_all_score_temp = df_all_score_temp.withColumn("job_1_dum", F.when(F.col("IBX_OCCUPATION_INPUT_AGG_HHD") == '1', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("job_y_dum", F.when(F.col("IBX_OCCUPATION_INPUT_AGG_HHD") == 'Y', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("active_sp_dum", F.when(F.col("Overall_Active_SP_Reltshps") >= '1', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("ibx_donation", F.when(F.col("IBX_DONATION_CONTRIBUTION"), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("greenliving", F.when(F.col("IBX_GREEN_LIVING") == 1, 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("income_AtoC", F.when(F.col("IBX_HOUSEHOLD_INCOME").isin('A', 'B', 'C'), 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("ibx_party_d", F.when(F.col("IBX_POLITICAL_PARTY_INPUT_INDIVI") == 'D', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("retail_a1", F.when(F.col("IBX_RETAIL_PURCHASES_MOST_FREQUE") == 'A1', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("retail_a3", F.when(F.col("IBX_RETAIL_PURCHASES_MOST_FREQUE") == 'A3', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("lifestage_3", F.when(F.col("LIFE_stage") == '3', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("member_secondary", F.when(F.col("MEMBER_FL_AGG_IND") == 'S', 1).otherwise(0))
df_all_score_temp = df_all_score_temp.withColumn("IBX_HOME_YEAR_BUILT_ACTUAL_num", F.when(F.col("IBX_HOME_YEAR_BUILT_ACTUAL") == '', 0).otherwise(F.col("IBX_HOME_YEAR_BUILT_ACTUAL").cast("double")))
df_all_score_temp = df_all_score_temp.withColumn("IBX_VEHICLE_KNOWN_OWNED_NUMB_num", F.when(F.col("IBX_VEHICLE_KNOWN_OWNED_NUMBER_P") == '', 0).otherwise(F.col("IBX_VEHICLE_KNOWN_OWNED_NUMBER_P").cast("double")))
df_all_score_temp = df_all_score_temp.withColumn("IBX_ADULT_AGE_45_54_AGG_HHD_num", F.when(F.col("IBX_ADULT_AGE_45_54_AGG_HHD") == '', 0).otherwise(F.col("IBX_ADULT_AGE_45_54_AGG_HHD").cast("double")))
df_all_score_temp = df_all_score_temp.withColumn("IBX_HEALTH_BEAUTY_num", F.when(F.col("ibx_health_beauty") == '', 0).otherwise(F.col("ibx_health_beauty").cast("double")))

df_all_score_temp = df_all_score_temp.withColumn("logit_advo_dm_65plus", F.exp(
    -6.1105 + 
    F.col("LIKELY_HISP_AGG_c") * -0.00285 + 
    F.col("orders_all_c") * 0.0378 + 
    F.col("orders_12moterm_c") * -0.0262 + 
    F.col("orders_acqmail_c") * 0.0924 + 
    F.col("orders_altmedia_c") * 0.0875 + 
    F.col("orders_online_c") * -0.1242 + 
    F.col("CENS_AGE_POP_PERCENT_30_34_c") * -0.0126 + 
    F.col("CENS_EARN_HH_PERCENT_NO_PUBLIC_c") * 0.0156 + 
    F.col("CENS_ETHNIC_POP_PERCENT_HI_NAT_c") * 0.0349 + 
    F.col("CENS_GRPQTRS_POP_PERCENT_NURSI_c") * -0.0154 + 
    F.col("CENS_HEAT_OCCHU_PERCENT_BOTTLE_c") * 0.00416 + 
    F.col("CENS_HEAT_OCCHU_PERCENT_OTHER__c") * -0.0348 + 
    F.col("CENS_INDUS_EMPLD_PERCENT_WHOLE_c") * 0.0146 + 
    F.col("CENS_INDUS_EMPLD_PERCENT_TRANS_c") * -0.0123 + 
    F.col("CENS_MORTG_OOHU_PERCENT_TWO_MR_c") * -0.0234 + 
    F.col("CENS_OCCUP_EMPLD_PERCENT_HEALT_c") * 0.0117 + 
    F.col("CENS_URBAN_POP_PERCENT_URBAN_I_c") * 0.00183 + 
    F.col("CENS_MOVE_OCCHU_PERCENT_NEW_LI_c") * 0.00508 + 
    F.col("GENERAL_ACTIVIST_MODEL_c") * 0.00531 + 
    F.col("PARTISANscore_c") * 0.0039 + 
    F.col("RADIO_c") * -0.00661 + 
    F.col("CABLE_c") * -0.00502 + 
    F.col("GAME_SHOWS_c") * 0.00588 + 
    F.col("mailercount_click_30_c") * 0.1891 + 
    F.col("mailercount_open_365_c") * -0.0219 + 
    F.col("driver_safety_vol_12mo_c") * 2.7501 + 
    F.col("foundation_donations_checkb_12_c") * 0.3488 + 
    F.col("SY_GENERALACTIVIST_c") * -0.00413 + 
    F.col("VOTEPROP2016_c") * 0.00223 + 
    F.col("driver_class_12mo_i_c") * 0.3169 + 
    F.col("contact_leg_12mo_i_c") * 0.4581 + 
    F.col("petition_sign_12mo_i_c") * 0.6763 + 
    F.col("num_months_c") * -0.00118 + 
    F.col("cntct_lifstyle_12mo_agg_hhd_c") * -0.00904 + 
    F.col("advo_hpc_dt_c") * 0.00000001277 + 
    F.col("renewals_c") * -0.0143 + 
    F.col("donfnd12_c") * 0.8465 + 
    F.col("petadv12_c") * 0.3988 + 
    F.col("NEW_SCORE10_CENTILE_c") * -0.0029 + 
    F.col("NEW_SCORE14_CENTILE_c") * -0.00204 + 
    F.col("NEW_SCORE17_CENTILE_c") * -0.00319 + 
    F.col("NEW_SCORE18_CENTILE_c") * 0.00325 + 
    F.col("NEW_SCORE29_CENTILE_c") * -0.00153 + 
    F.col("NEW_SCORE33_CENTILE_c") * -0.00311 + 
    F.col("NEW_SCORE40_CENTILE_c") * 0.00396 + 
    F.col("NEW_SCORE41_CENTILE_c") * -0.00301 + 
    F.col("NEW_SCORE43_CENTILE_c") * 0.00347 + 
    F.col("ProspMail_c") * 0.2061 + 
    F.col("Acknow_c") * 3.0336 + 
    F.col("Advo_Petition_c") * -0.032 + 
    F.col("totalmailings_c") * -0.3165 + 
    F.col("community_vetera_dum") * -0.1014 + 
    F.col("job_1_dum") * 0.0922 + 
    F.col("job_y_dum") * 0.1511 + 
    F.col("active_sp_dum") * 0.1162 + 
    F.col("deadwood_dum") * -0.4591 + 
    F.col("ibx_donation") * 0.408 + 
    F.col("greenliving") * 0.1357 + 
    F.col("income_AtoC") * 0.0928 + 
    F.col("ibx_party_d") * 0.0787 + 
    F.col("retail_a1") * 0.346 + 
    F.col("retail_a3") * -0.2684 + 
    F.col("lifestage_3") * 0.2086 + 
    F.col("member_secondary") * 1.6861 + 
    F.col("IBX_HOME_YEAR_BUILT_ACTUAL_num") * 0.000055 + 
    F.col("IBX_VEHICLE_KNOWN_OWNED_NUMB_num") * -0.0316 + 
    F.col("IBX_ADULT_AGE_45_54_AGG_HHD_num") * -0.1443 + 
    F.col("IBX_HEALTH_BEAUTY_num") * 0.1657
))
df_all_score_temp = df_all_score_temp.withColumn("advo_dm_65plus_score", F.col("logit_advo_dm_65plus") / (1 + F.col("logit_advo_dm_65plus")))

df_all_score_temp.write.format("delta").mode("overwrite").saveAsTable("tim.all_score_temp")

# PROC RANK
df_all_score_temp = spark.table("tim.all_score_temp")
df_with_dummy = df_all_score_temp.withColumn("dummy", F.lit(1))
window_spec_morning = Window.partitionBy("dummy").orderBy(F.col("score_morning").desc())
window_spec_night = Window.partitionBy("dummy").orderBy(F.col("score_night").desc())
window_spec_afternoon = Window.partitionBy("dummy").orderBy(F.col("score_afternoon").desc())
window_spec_tas = Window.partitionBy("dummy").orderBy(F.col("tas_score").desc())
window_spec_ss = Window.partitionBy("dummy").orderBy(F.col("comb_score_ss").desc())
window_spec_housing = Window.partitionBy("dummy").orderBy(F.col("fndn_housing_score").desc())
window_spec_fraudwatch = Window.partitionBy("dummy").orderBy(F.col("score_fraudwatch").desc())
window_spec_care_attend = Window.partitionBy("dummy").orderBy(F.col("CPD_CARE_ATTEND_score").desc())
window_spec_jobs_attend = Window.partitionBy("dummy").orderBy(F.col("CPD_JOBS_ATTEND_score").desc())
window_spec_aca = Window.partitionBy("dummy").orderBy(F.col("score_aca").desc())
window_spec_advo_dm_65plus = Window.partitionBy("dummy").orderBy(F.col("advo_dm_65plus_score").desc())

df_score_ranks = df_with_dummy.withColumn("live_answer_am", F.ntile(99).over(window_spec_morning)) \
    .withColumn("live_answer_pm", F.ntile(99).over(window_spec_night)) \
    .withColumn("live_answer_aft", F.ntile(99).over(window_spec_afternoon)) \
    .withColumn("TAS_Volunteer", F.ntile(99).over(window_spec_tas)) \
    .withColumn("Soc_Sec_LO", F.ntile(99).over(window_spec_ss)) \
    .withColumn("FNDN_Housing", F.ntile(99).over(window_spec_housing)) \
    .withColumn("Fraudwatch_lo", F.ntile(99).over(window_spec_fraudwatch)) \
    .withColumn("CPD_CARE_ATTEND", F.ntile(99).over(window_spec_care_attend)) \
    .withColumn("CPD_JOBS_ATTEND", F.ntile(99).over(window_spec_jobs_attend)) \
    .withColumn("lo_aca", F.ntile(99).over(window_spec_aca)) \
    .withColumn("advo_dm_65plus", F.ntile(99).over(window_spec_advo_dm_65plus)) \
    .drop("dummy")

# PROC SQL to update geo_appends_rpm
df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_score_ranks.createOrReplaceTempView("score_ranks")
df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")

df_geo_appends_rpm_updated = spark.sql("""
    SELECT
        a.*,
        b.live_answer_am + 1 AS live_answer_am,
        b.live_answer_aft + 1 AS live_answer_aft,
        b.live_answer_pm + 1 AS live_answer_pm,
        b.TAS_Volunteer + 1 AS TAS_Volunteer,
        b.Soc_Sec_LO + 1 AS Soc_Sec_LO,
        b.FNDN_Housing + 1 AS FNDN_Housing,
        b.Fraudwatch_lo + 1 AS Fraudwatch_lo,
        b.CPD_CARE_ATTEND + 1 AS CPD_CARE_ATTEND,
        b.CPD_JOBS_ATTEND + 1 AS CPD_JOBS_ATTEND,
        b.lo_aca + 1 AS lo_aca,
        b.advo_dm_65plus + 1 AS advo_dm_65plus,
        b.pct_live
    FROM geo_appends_rpm a
    LEFT JOIN score_ranks b
    ON CAST(a.merkleid AS BIGINT) = b.mid_key
""")
df_geo_appends_rpm_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

# DATA STEP for all_again
df_all_score_temp_form_db = spark.table("tim.all_score_temp")
df_geo_appends_rpm_form_db = spark.table("intermed.geo_appends_rpm")

df_all_again = df_all_score_temp_form_db.join(df_geo_appends_rpm_form_db, "mid_key", "inner")

# Transformations for all_again
df_all_again = df_all_again.withColumn("MemOriginDate_char", F.col("MemOriginDate").cast("string"))
df_all_again = df_all_again.withColumn("Year1", F.substring(F.col("MemOriginDate_char"), 1, 4))
df_all_again = df_all_again.withColumn("month1", F.substring(F.col("MemOriginDate_char"), 5, 2))
df_all_again = df_all_again.withColumn("day1", F.substring(F.col("MemOriginDate_char"), 7, 2))
df_all_again = df_all_again.withColumn("mem_origin_date", F.to_date(F.concat_ws('-', F.col('Year1'), F.col('month1'), F.col('day1'))))
df_all_again = df_all_again.withColumn("MemOrigin_today_yr", (F.months_between(F.current_date(), F.col("mem_origin_date")) / 12).cast("int"))
df_all_again = df_all_again.withColumn("tenure815", F.when((F.col("MemOrigin_today_yr") >= 8) & (F.col("MemOrigin_today_yr") <= 15), 1).otherwise(0))
df_all_again = df_all_again.withColumn("age_50to59_dum", F.when((F.col("age_agg_ind") >= 50) & (F.col("age_agg_ind") <= 59), 1).otherwise(0))
df_all_again = df_all_again.withColumn("age_70to81_dum", F.when((F.col("age_agg_ind") >= 70) & (F.col("age_agg_ind") <= 81), 1).otherwise(0))
df_all_again = df_all_again.withColumn("NEW_SCORE38_CENTILE", F.coalesce(F.col("NEW_SCORE38_CENTILE"), F.lit(99)))
df_all_again = df_all_again.withColumn("live_answer_am", F.coalesce(F.col("live_answer_am"), F.lit(99)))
df_all_again = df_all_again.withColumn("advo_s23_dum", F.when(F.col("advo_segment_cd").isin('S2', 'S3'), 1).otherwise(0))
df_all_again = df_all_again.withColumn("Overall_Historic_SP_123", F.when(F.col("Overall_Historic_SP_Reltshps") == '0', 1).otherwise(0))
df_all_again = df_all_again.withColumn("NEW_SCORE3_CENTILE", F.coalesce(F.col("NEW_SCORE3_CENTILE"), F.lit(99)))
df_all_again = df_all_again.withColumn("rpm_score_c", F.coalesce(F.col("rpm_score"), F.lit(9)))
df_all_again = df_all_again.withColumn("VOTEPROP2016_c", F.coalesce(F.col("general_election_vote_propensity"), F.lit(83.8246349)))
df_all_again = df_all_again.withColumn("advocacy_donations_12moNO", F.when(F.col("advocacy_donations_12mo").isin('2', '1'), 1).otherwise(0))
df_all_again = df_all_again.withColumn("ideology_c", F.coalesce(F.col("ideology"), F.lit(42.1)))
df_all_again = df_all_again.withColumn("partisanscore_c", F.coalesce(F.col("partisanscore"), F.lit(54)))
df_all_again = df_all_again.withColumn("vehicle_2_dum", F.when(F.col("vehicle_known_owned_number") == '2', 1).otherwise(0))
df_all_again = df_all_again.withColumn("SY_OTSBN_POLFUND_c", F.coalesce(F.col("SY_OTSBN_POLFUND"), F.lit(10.9)))
df_all_again = df_all_again.withColumn("elderly_u_dum", F.when(F.col("IBX_ELDERLY_PARENT_AGG_HHD") == 'Y', 1).otherwise(0))
df_all_again = df_all_again.withColumn("IBX_HEALTHY_BEHAVIOUR_HHD_1dum", F.when(F.col("IBX_HEALTHY_BEHAVIOUR_AGG_HHD") == '1', 1).otherwise(0))
df_all_again = df_all_again.withColumn("IBX_HOME_VALUE_RANGES_BCDEF", F.when(F.col("IBX_HOME_VALUE_RANGES_AGG_HHD").isin('K', 'L', 'M', 'N'), 1).otherwise(0))
df_all_again = df_all_again.withColumn("ch_acq_I", F.when(F.col("ch_acq") == 'I', 1).otherwise(0))
df_all_again = df_all_again.withColumn("advocacy_signers_12mo_i1", F.when(F.col("advocacy_signers_12mo_i") == 1, 1).otherwise(0))
df_all_again = df_all_again.withColumn("deadwood_dum", F.when(F.col("DEADWOOD_MODEL") == 'NOTDEAD', 1).otherwise(0))
df_all_again = df_all_again.withColumn("HistPartCt_Overall12", F.when(F.col("HistPartCt_Overall").isin('1', '2'), 1).otherwise(0))

df_all_again = df_all_again.withColumn("logit_worknsave", 
    -2.3081 +
    (F.col("tenure815") * 0.0853) +
    (F.col("call_freq") * -0.0357) +
    (F.col("pct_live2") * 0.2706) +
    (F.col("liveanswer_freq_3") * 0.0226) +
    (F.col("liveanswer_freq3_6") * 0.0401) +
    (F.col("liveanswer_freq6_12") * 0.0258) +
    (F.col("age_gt81_dum") * -0.0724) +
    (F.col("age_50to59_dum") * -0.0927) +
    (F.col("age_70to81_dum") * -0.041) +
    (F.col("NEW_SCORE38_CENTILE") * -0.00087) +
    (F.col("live_answer_am") * -0.00082) +
    (F.col("goi_1_dum") * -0.0291) +
    (F.col("advo_s23_dum") * 0.0536) +
    (F.col("acevflag_c") * -0.0268) +
    (F.col("Overall_Historic_SP_123") * 0.0319) +
    (F.col("NEW_SCORE3_CENTILE") * 0.000414) +
    (F.col("rpm_score_c") * 0.00998) +
    (F.col("MemXRenew_c") * -0.0034) +
    (F.col("VOTEPROP2016_c") * 0.000596) +
    (F.col("advocacy_donations_12moNO") * -0.0391) +
    (F.col("ideology_c") * 0.000818) +
    (F.col("partisanscore_c") * 0.000459) +
    (F.col("vehicle_2_dum") * 0.0262) +
    (F.col("SY_OTSBN_POLFUND_c") * 0.00194) +
    (F.col("elderly_u_dum") * 0.0161) +
    (F.col("grandchildren_dum") * 0.0224) +
    (F.col("CENS_ETHNIC_POP_PERCENT_SOME_c") * 0.00429) +
    (F.col("IBX_HEALTHY_BEHAVIOUR_HHD_1dum") * 0.0494) +
    (F.col("IBX_HOME_VALUE_RANGES_BCDEF") * 0.0341) +
    (F.col("POP_PERCENT_NON_HISP") * 0.00249) +
    (F.col("UTILITY_gas") * -0.00044) +
    (F.col("HomVal_Home_Value_CBSA_Index_c") * -0.00021) +
    (F.col("ch_acq_I") * -0.0434) +
    (F.col("state_activity_12mo_i") * 0.1005) +
    (F.col("advocacy_signers_12mo_i1") * 0.067) +
    (F.col("gender_M") * -0.0479) +
    (F.col("otherdonors_dum") * 0.0269) +
    (F.col("OCCHU_Median_Length_of_Resi_c") * 0.000023) +
    (F.col("homerange_a_dum") * -0.0294) +
    (F.col("Party_REP") * 0.0694) +
    (F.col("deadwood_dum") * 0.0215) +
    (F.col("TELECOM_CALLING_SERVICES_AGG123") * 0.0174) +
    (F.col("NO_MORTG") * -0.00092) +
    (F.col("Pop_pct_Black_Only_Hisp_c") * 0.00169) +
    (F.col("PERCENTBLUECOLLAR_c") * -0.00101) +
    (F.col("CENS_COMMUTE_WRKRS_PERCENT_DRO_c") * 0.00415) +
    (F.col("teletown_12mo_ic") * 0.1235) +
    (F.col("HistPartCt_Overall12") * 0.0224)
)
df_all_again = df_all_again.withColumn("score_worknsave", F.exp(F.col("logit_worknsave")) / (1 + F.exp(F.col("logit_worknsave"))))

df_all_again = df_all_again.withColumn("age_gt71", F.when(F.col("age_agg_ind") >= 71, 1).otherwise(0))
df_all_again = df_all_again.withColumn("international", F.when(F.col("ibx_vacation_travel_internationa").isin('01', '10'), 1).otherwise(0))
df_all_again = df_all_again.withColumn("income56aci", F.when(F.col("ibx_household_income").isin('5', '6', 'A', 'C', 'I'), 1).otherwise(0))
df_all_again = df_all_again.withColumn("base_rc_dt", F.when(F.col("ibx_base_record_verification_dt") == '20191', 1).otherwise(0))
df_all_again = df_all_again.withColumn("driver_class_12mo", F.coalesce(F.col("driver_class_12mo"), F.lit(0.0062241)))
df_all_again = df_all_again.withColumn("individual_engagers_12mo", F.coalesce(F.col("individual_engagers_12mo"), F.lit(0.6427774)))
df_all_again = df_all_again.withColumn("likely_black_agg", F.coalesce(F.col("likely_black_agg"), F.lit(91.2634362)))
df_all_again = df_all_again.withColumn("live_answer_pm", F.coalesce(F.col("live_answer_pm"), F.lit(54.3805041)))
df_all_again = df_all_again.withColumn("advocacy_donations_12mo", F.coalesce(F.col("advocacy_donations_12mo"), F.lit(0.0459594)))
df_all_again = df_all_again.withColumn("cens_census_block_group", F.coalesce(F.col("cens_census_block_group"), F.lit(2.1519445)))
df_all_again = df_all_again.withColumn("cens_earn_hh_percent_no_other_ty", F.coalesce(F.col("cens_earn_hh_percent_no_other_ty"), F.lit(88.2650923)))
df_all_again = df_all_again.withColumn("cens_ethnic_pop_percent_asian_on", F.coalesce(F.col("cens_ethnic_pop_percent_asian_on"), F.lit(6.0686706)))
df_all_again = df_all_again.withColumn("cens_heat_occhu_percent_oil_or_k", F.coalesce(F.col("cens_heat_occhu_percent_oil_or_k"), F.lit(9.6580005)))
df_all_again = df_all_again.withColumn("cens_homval_oohu_percent_home_va", F.coalesce(F.col("cens_homval_oohu_percent_home_va"), F.lit(0.6179638)))
df_all_again = df_all_again.withColumn("cens_inc_hh_median_household_inc", F.coalesce(F.col("cens_inc_hh_median_household_inc"), F.lit(80479.36)))
df_all_again = df_all_again.withColumn("cens_indus_empld_percent_hlth_ca", F.coalesce(F.col("cens_indus_empld_percent_hlth_ca"), F.lit(14.2002689)))
df_all_again = df_all_again.withColumn("cens_occup_empld_percent_compute", F.coalesce(F.col("cens_occup_empld_percent_compute"), F.lit(3.1122913)))

df_all_again = df_all_again.withColumn("logit_rx_lo", 
    -1.6484 +
    (F.col("num_inb_3_6mo") * -0.00953) +
    (F.col("age_gt71") * 0.0978) +
    (F.col("international") * 0.1012) +
    (F.col("income56aci") * 0.0944) +
    (F.col("grandchildren_dum") * 0.0825) +
    (F.col("base_rc_dt") * 0.27) +
    (F.col("driver_class_12mo") * -0.5109) +
    (F.col("individual_engagers_12mo") * 0.0514) +
    (F.col("likely_black_agg") * -0.00239) +
    (F.col("live_answer_pm") * 0.00225) +
    (F.col("liveanswer_freq_3") * 0.1524) +
    (F.col("liveanswer_freq3_6") * 0.2022) +
    (F.col("advocacy_donations_12mo") * 0.0923) +
    (F.col("cens_census_block_group") * 0.0431) +
    (F.col("cens_earn_hh_percent_no_other_ty") * -0.00867) +
    (F.col("cens_ethnic_pop_percent_asian_on") * 0.00427) +
    (F.col("cens_heat_occhu_percent_oil_or_k") * -0.00273) +
    (F.col("cens_heat_occhu_percent_utility_") * -0.00184) +
    (F.col("cens_homval_oohu_percent_home_va") * 0.0243) +
    (F.col("cens_inc_hh_median_household_inc") * 0.000001857) +
    (F.col("cens_indus_empld_percent_hlth_ca") * -0.00903) +
    (F.col("cens_occup_empld_percent_compute") * -0.0152)
)
df_all_again = df_all_again.withColumn("rx_lo_score", F.exp(F.col("logit_rx_lo")) / (1 + F.exp(F.col("logit_rx_lo"))))

# PROC RANK
df_with_dummy2 = df_all_again.withColumn("dummy", F.lit(1))
window_spec_worknsave = Window.partitionBy("dummy").orderBy(F.col("score_worknsave").desc())
window_spec_rx_lo = Window.partitionBy("dummy").orderBy(F.col("rx_lo_score").desc())

df_score_ranks2 = df_with_dummy2.withColumn("WorkNSaveACT_PH", F.ntile(99).over(window_spec_worknsave)) \
    .withColumn("Rx_lo", F.ntile(99).over(window_spec_rx_lo)) \
    .drop("dummy")

# PROC SQL to update geo_appends_rpm again
df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_score_ranks2.createOrReplaceTempView("score_ranks2")
df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")

df_geo_appends_rpm_updated2 = spark.sql("""
    SELECT
        a.*,
        b.WorkNSaveACT_PH + 1 AS WorkNSaveACT_PH,
        b.Rx_lo + 1 AS Rx_lo
    FROM geo_appends_rpm a
    LEFT JOIN score_ranks2 b ON a.mid_key = b.mid_key
""")
df_geo_appends_rpm_updated2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

# DATA STEP for masters model
df_masters = spark.table("intermed.geo_appends_rpm")

df_masters = df_masters.withColumn("GroupEthnicCode", F.coalesce(F.col("GroupEthnicCode"), F.lit('*')))
df_masters = df_masters.withColumn("IBX_OCCUPATION_1ST_INDIVIDUAL_PR", F.coalesce(F.col("IBX_OCCUPATION_1ST_INDIVIDUAL_PR"), F.lit('*')))
df_masters = df_masters.withColumn("IBX_PERSONIC_CLUSTER", F.coalesce(F.col("IBX_PERSONIC_CLUSTER"), F.lit('*')))
df_masters = df_masters.withColumn("ibx_vehicle_dominant_lifestyle_p", F.coalesce(F.col("ibx_vehicle_dominant_lifestyle_p"), F.lit('*')))

df_masters = df_masters.withColumn("GroupEthnicCode_binned", 
    F.when(F.col("GroupEthnicCode").isin('D', 'I', 'F', '*'), 'GROUP1')
    .when(F.col("GroupEthnicCode").isin('A', 'Z', 'G'), 'GROUP2')
    .when(F.col("GroupEthnicCode").isin('K', 'H', 'J', 'B', 'L', 'E', 'N', 'C'), 'GROUP3')
    .when(F.col("GroupEthnicCode") == 'O', 'GROUP4')
    .otherwise('GROUP1')
)
df_masters = df_masters.withColumn("IBX_OCCUPATION_1ST_INDIVIDUA_bin", 
    F.when(F.col("IBX_OCCUPATION_1ST_INDIVIDUAL_PR").isin('G', 'I', 'K', 'A', '8', 'D', 'C'), 'GROUP1')
    .when(F.col("IBX_OCCUPATION_1ST_INDIVIDUAL_PR").isin('7', 'J', 'W', 'X', '9', 'V'), 'GROUP2')
    .when(F.col("IBX_OCCUPATION_1ST_INDIVIDUAL_PR").isin('*', 'B', 'Y', '6', '3'), 'GROUP3')
    .when(F.col("IBX_OCCUPATION_1ST_INDIVIDUAL_PR").isin('2', '5', '4', '1', 'Z', 'F', 'E', 'H'), 'GROUP4')
    .otherwise('GROUP1')
)
df_masters = df_masters.withColumn("IBX_PERSONIC_CLUSTER_binned", 
    F.when(F.col("IBX_PERSONIC_CLUSTER").isin('9', '39', '49', '43', '12'), 'GROUP1')
    .when(F.col("IBX_PERSONIC_CLUSTER").isin('66', '46', '11', '28', '52', '58', '22', '64', '45', '53', '48', '61', '68', '38', '67', '63'), 'GROUP2')
    .when(F.col("IBX_PERSONIC_CLUSTER").isin('09', '62', '33', '29', '65', '57', '54', '13', '59', '50', '01', '04', '60', '36', '41', '07', '21', '69', '17', '31', '47', '14', '51', '19', '70', '18'), 'GROUP3')
    .when(F.col("IBX_PERSONIC_CLUSTER").isin('23', '40', '02', '55', '03', '05', '*', '32', '08', '16', '26', '42', '44', '35'), 'GROUP4')
    .when(F.col("IBX_PERSONIC_CLUSTER").isin('15', '25', '34', '10', '37', '06', '24', '20', '30', '27', '56'), 'GROUP5')
    .otherwise('GROUP1')
)
df_masters = df_masters.withColumn("ibx_vehicle_dominant_lifesty_bin", 
    F.when(F.col("ibx_vehicle_dominant_lifestyle_p").isin('*', 'F', 'A', 'D'), 'GROUP1')
    .when(F.col("ibx_vehicle_dominant_lifestyle_p").isin('E', 'C'), 'GROUP2')
    .when(F.col("ibx_vehicle_dominant_lifestyle_p").isin('B', 'G'), 'GROUP3')
    .otherwise('GROUP1')
)
df_masters = df_masters.withColumn("advo_dm_65plus_binned", 
    F.when(F.col("advo_dm_65plus").isNull(), '.MISS')
    .when(F.col("advo_dm_65plus") <= 6, '<=6')
    .when(F.col("advo_dm_65plus") <= 56, '6:56')
    .when(F.col("advo_dm_65plus") <= 65, '56:65')
    .when(F.col("advo_dm_65plus") <= 76, '65:76')
    .otherwise('>76')
)
df_masters = df_masters.withColumn("AGE_AGG_IND_binned", 
    F.when(F.col("AGE_AGG_IND").isNull(), '.MISS')
    .when(F.col("AGE_AGG_IND") <= 56, '<=56')
    .when(F.col("AGE_AGG_IND") <= 59, '56:59')
    .when(F.col("AGE_AGG_IND") <= 63, '59:63')
    .when(F.col("AGE_AGG_IND") <= 68, '63:68')
    .when(F.col("AGE_AGG_IND") <= 70, '68:70')
    .when(F.col("AGE_AGG_IND") <= 74, '70:74')
    .when(F.col("AGE_AGG_IND") <= 78, '74:78')
    .otherwise('>78')
)
df_masters = df_masters.withColumn("IBX_LENGTH_OF_RESIDENCE_AGG__bin", 
    F.when(F.col("IBX_LENGTH_OF_RESIDENCE_AGG_HHD").isNull(), '.MISS')
    .when(F.col("IBX_LENGTH_OF_RESIDENCE_AGG_HHD") <= 1, '<=1')
    .when(F.col("IBX_LENGTH_OF_RESIDENCE_AGG_HHD") <= 12, '1:12')
    .otherwise('>12')
)
df_masters = df_masters.withColumn("Past12MoTouchCt_Health_binned", 
    F.when(F.col("Past12MoTouchCt_Health").isNull(), '.MISS')
    .when(F.col("Past12MoTouchCt_Health") <= 2, '<=2')
    .when(F.col("Past12MoTouchCt_Health") <= 5, '2:5')
    .when(F.col("Past12MoTouchCt_Health") <= 9, '5:9')
    .otherwise('>9')
)
df_masters = df_masters.withColumn("renewals_binned", 
    F.when(F.col("memxrenew").isNull(), '.MISS')
    .when(F.col("memxrenew") <= 1, '<=1')
    .when(F.col("memxrenew") <= 2, '1:2')
    .when(F.col("memxrenew") <= 7, '2:7')
    .otherwise('>7')
)
df_masters = df_masters.withColumn("SecAge_binned", 
    F.when(F.col("SecAge").isNull(), '.MISS')
    .when(F.col("SecAge") <= 52, '<=52')
    .when(F.col("SecAge") <= 56, '52:56')
    .when(F.col("SecAge") <= 60, '56:60')
    .when(F.col("SecAge") <= 63, '60:63')
    .when(F.col("SecAge") <= 65, '63:65')
    .when(F.col("SecAge") <= 68, '65:68')
    .when(F.col("SecAge") <= 70, '68:70')
    .when(F.col("SecAge") <= 74, '70:74')
    .otherwise('>74')
)

df_masters = df_masters.withColumn("pred11", 
    -2.8871346 +
    F.when(F.col("AGE_AGG_IND_binned") == '56:59', 0.7213119).otherwise(0) +
    F.when(F.col("AGE_AGG_IND_binned") == '59:63', 1.4272725).otherwise(0) +
    F.when(F.col("AGE_AGG_IND_binned") == '63:68', 1.6361648).otherwise(0) +
    F.when(F.col("AGE_AGG_IND_binned") == '68:70', 1.2161192).otherwise(0) +
    F.when(F.col("AGE_AGG_IND_binned") == '70:74', 0.9700294).otherwise(0) +
    F.when(F.col("AGE_AGG_IND_binned") == '74:78', 0.6040413).otherwise(0) +
    F.when(F.col("AGE_AGG_IND_binned") == '>78', 0.2032639).otherwise(0) +
    F.when(F.col("AGE_AGG_IND_binned") == '.MISS', 0.9491802).otherwise(0) +
    F.when(F.col("SecAge_binned") == '52:56', 0.1597576).otherwise(0) +
    F.when(F.col("SecAge_binned") == '56:60', 0.0800794).otherwise(0) +
    F.when(F.col("SecAge_binned") == '60:63', 0.4829075).otherwise(0) +
    F.when(F.col("SecAge_binned") == '63:65', 0.707094).otherwise(0) +
    F.when(F.col("SecAge_binned") == '65:68', 0.5249849).otherwise(0) +
    F.when(F.col("SecAge_binned") == '68:70', 0.4087669).otherwise(0) +
    F.when(F.col("SecAge_binned") == '70:74', 0.1439517).otherwise(0) +
    F.when(F.col("SecAge_binned") == '>74', 0.245812).otherwise(0) +
    F.when(F.col("SecAge_binned") == '.MISS', 0.1584376).otherwise(0) +
    F.when(F.col("IBX_PERSONIC_CLUSTER_binned") == 'GROUP4', 0.1672897).otherwise(0) +
    F.when(F.col("IBX_PERSONIC_CLUSTER_binned") == 'GROUP5', 0.2756467).otherwise(0) +
    F.when(F.col("IBX_PERSONIC_CLUSTER_binned") == 'GROUP2', -0.1766039).otherwise(0) +
    F.when(F.col("IBX_PERSONIC_CLUSTER_binned") == 'GROUP1', -0.6051935).otherwise(0) +
    F.when(F.col("IBX_OCCUPATION_1ST_INDIVIDUA_bin") == 'GROUP3', -0.0207762).otherwise(0) +
    F.when(F.col("IBX_OCCUPATION_1ST_INDIVIDUA_bin") == 'GROUP2', -0.2425464).otherwise(0) +
    F.when(F.col("IBX_OCCUPATION_1ST_INDIVIDUA_bin") == 'GROUP1', -0.5298581).otherwise(0) +
    F.when(F.col("IBX_LENGTH_OF_RESIDENCE_AGG__bin") == '1:12', -0.4178569).otherwise(0) +
    F.when(F.col("IBX_LENGTH_OF_RESIDENCE_AGG__bin") == '>12', -0.2797477).otherwise(0) +
    F.when(F.col("IBX_LENGTH_OF_RESIDENCE_AGG__bin") == '.MISS', -0.26539).otherwise(0) +
    F.when(F.col("ibx_vehicle_dominant_lifesty_bin") == 'GROUP3', 0.3320858).otherwise(0) +
    F.when(F.col("ibx_vehicle_dominant_lifesty_bin") == 'GROUP2', 0.1397524).otherwise(0) +
    F.when(F.col("advo_dm_65plus_binned") == '6:56', -0.3376137).otherwise(0) +
    F.when(F.col("advo_dm_65plus_binned") == '56:65', -0.0939498).otherwise(0) +
    F.when(F.col("advo_dm_65plus_binned") == '65:76', -0.3806411).otherwise(0) +
    F.when(F.col("advo_dm_65plus_binned") == '>76', -0.323822).otherwise(0) +
    F.when(F.col("advo_dm_65plus_binned") == '.MISS', 0.5868213).otherwise(0) +
    F.when(F.col("GroupEthnicCode_binned") == 'GROUP3', 0.0688789).otherwise(0) +
    F.when(F.col("GroupEthnicCode_binned") == 'GROUP1', -0.9812486).otherwise(0) +
    F.when(F.col("GroupEthnicCode_binned") == 'GROUP4', 0.3527713).otherwise(0) +
    F.when(F.col("renewals_binned") == '1:2', -0.0427741).otherwise(0) +
    F.when(F.col("renewals_binned") == '2:7', 0.0139229).otherwise(0) +
    F.when(F.col("renewals_binned") == '>7', -0.219005).otherwise(0) +
    F.when(F.col("Past12MoTouchCt_Health_binned") == '2:5', 0.1971018).otherwise(0) +
    F.when(F.col("Past12MoTouchCt_Health_binned") == '5:9', 0.1170709).otherwise(0) +
    F.when(F.col("Past12MoTouchCt_Health_binned") == '>9', 0.3022678).otherwise(0) +
    F.when(F.col("Past12MoTouchCt_Health_binned") == '.MISS', -0.0133842).otherwise(0)
)
df_masters = df_masters.withColumn("p_score11", F.exp(F.col("pred11")) / (1 + F.exp(F.col("pred11"))))

# PROC RANK
df_masters_with_dummy = df_masters.withColumn("dummy", F.lit(1))
window_spec_p11 = Window.partitionBy("dummy").orderBy(F.col("p_score11").desc())
df_master_centiles = df_masters_with_dummy.withColumn("centile11", F.ntile(99).over(window_spec_p11)).drop("dummy")

# PROC SQL for final update
df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_master_centiles.createOrReplaceTempView("master_centiles")
df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_geo_appends_rpm_final = spark.sql("""
    SELECT
        a.*,
        b.centile11 + 1 AS NEW_SCORE11_CENTILE
    FROM geo_appends_rpm a
    LEFT JOIN master_centiles b ON a.mid_key = b.mid_key
""")
df_geo_appends_rpm_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

# PROC FREQ for diagnostics
df_diagnostics = spark.table("intermed.geo_appends_rpm")
print("Frequency Distribution for live_answer_am")
df_diagnostics.groupBy("live_answer_am").count().orderBy("live_answer_am").show(100)
print("Frequency Distribution for live_answer_aft")
df_diagnostics.groupBy("live_answer_aft").count().orderBy("live_answer_aft").show(100)
print("Frequency Distribution for live_answer_pm")
df_diagnostics.groupBy("live_answer_pm").count().orderBy("live_answer_pm").show(100)
print("Frequency Distribution for TAS_Volunteer")
df_diagnostics.groupBy("TAS_Volunteer").count().orderBy("TAS_Volunteer").show(100)
print("Frequency Distribution for Soc_Sec_LO")
df_diagnostics.groupBy("Soc_Sec_LO").count().orderBy("Soc_Sec_LO").show(100)
print("Frequency Distribution for FNDN_Housing")
df_diagnostics.groupBy("FNDN_Housing").count().orderBy("FNDN_Housing").show(100)
print("Frequency Distribution for Fraudwatch_lo")
df_diagnostics.groupBy("Fraudwatch_lo").count().orderBy("Fraudwatch_lo").show(100)
print("Frequency Distribution for CPD_CARE_ATTEND")
df_diagnostics.groupBy("CPD_CARE_ATTEND").count().orderBy("CPD_CARE_ATTEND").show(100)
print("Frequency Distribution for CPD_JOBS_ATTEND")
df_diagnostics.groupBy("CPD_JOBS_ATTEND").count().orderBy("CPD_JOBS_ATTEND").show(100)
print("Frequency Distribution for lo_aca")
df_diagnostics.groupBy("lo_aca").count().orderBy("lo_aca").show(100)
print("Frequency Distribution for WorkNSaveACT_PH")
df_diagnostics.groupBy("WorkNSaveACT_PH").count().orderBy("WorkNSaveACT_PH").show(100)
print("Frequency Distribution for advo_dm_65plus")
df_diagnostics.groupBy("advo_dm_65plus").count().orderBy("advo_dm_65plus").show(100)
print("Frequency Distribution for Rx_lo")
df_diagnostics.groupBy("Rx_lo").count().orderBy("Rx_lo").show(100)
print("Frequency Distribution for NEW_SCORE11_CENTILE")
df_diagnostics.groupBy("NEW_SCORE11_CENTILE").count().orderBy("NEW_SCORE11_CENTILE").show(100)

# Input variable tracking section
df_contact_history_sum = spark.table("intermed.contact_History_sum")

# Define columns and their labels
var_list_with_labels = [
    ("num_inb", "Number Emails Clicked or Opened Past 6 Mos"),
    ("num_inb_30days", "Number Emails Clicked or Opened Past 30 Days"),
    ("num_inb_3_6mo", "Number Emails Clicked or Opened Betw. Past 3 and 6 Mos"),
    ("num_inb_1_3mo", "Number Emails Clicked or Opened Betw. Past 1 and 3 Mos"),
    ("num_click", "Number Emails Clicked Past 6 Mos"),
    ("num_clicked_curmonth", "Number Emails Clicked Past 30 Days"),
    ("num_clicked_past12", "Number Emails Clicked Past 12 Mos"),
    ("num_clicked_1_3mo", "Number Emails Clicked Betw. Past 1 and 3 Mos"),
    ("num_clicked_3_6mo", "Number Emails Clicked Betw. Past 3 and 6 Mos"),
    ("mailercount_click", "Number Mailer IDs Clicked Past 12 Mos"),
    ("mailercount_click_6mo", "Number Email Mailer IDs Clicked Past 6 Mos"),
    ("mailercount_click_30days", "Number Email Mailer IDs Clicked Past 30 Days"),
    ("mailercount_click_1_3mo", "Number Email Mailer IDs Clicked Betw. Past 1 and 3 Mos"),
    ("mailercount_click_3_6mo", "Number Email Mailer IDs Clicked Betw. Past 3 and 6 Mos"),
    ("num_open", "Number Emails Opened Past 12 Mos"),
    ("num_open_6mo", "Number Emails Opened Past 6 Mos"),
    ("num_open_30days", "Number Emails Opened Past 30 Days"),
    ("num_ib_3mo", "Number Emails Clicked or Opened Past 3 Mos"),
    ("num_open_1_3mo", "Number Emails Opened Betw. Past 1 and 3 Mos"),
    ("num_open_3_6mo", "Number Emails Opened Betw. Past 3 and 6 Mos"),
    ("num_ib_30days", "Number Emails Clicked or Opened Summed Past 30 Days"),
    ("num_ib_1_3mo", "Number Emails Clicked or Opened Summed Betw. Past 1 and 3 Mos"),
    ("num_ib_3_6mo", "Number Emails Clicked or Opened Summed Betw. Past 3 and 6 Mos"),
    ("mailercount_open", "Number Email Mailer IDs Opened Past 12 Mos"),
    ("mailercount_open_6mo", "Number Email Mailer IDs Opened Past 6 Mos"),
    ("mailercount_open_30days", "Number Email Mailer IDs Opened Past 30 Days"),
    ("mailercount_open_3_6mo", "Number Email Mailer IDs Opened Betw. Past 3 and 6 Mos"),
    ("mailercount_open_1_3mo", "Number Email Mailer IDs Opened Betw. Past 1 and 3 Mos"),
    ("num_sent_curmonth", "Number Emails Received Past 30 Days"),
    ("num_sent_past12", "Number Emails Received Past 12 Mos"),
    ("num_ct", "Number Emails Received Past 6 Mos"),
    ("care_ct", "Number Emails Received Past 12 Mos from Specified Mailer IDs"),
    ("call_freq", "Number Phone Calls Received Past 12 Mos"),
    ("live_answer_ct", "Number Phone Calls Live Answered Past 12 Mos"),
    ("poll_ct", "Number Phone Calls Poll Asked and Answered Past 12 Mos"),
    ("poll_noaskct", "Number Phone Calls Poll Asked Past 12 Mos"),
    ("poll_anact", "Number Phone Calls Poll Asked and Not Answered Past 12 Mos"),
    ("liveanswer_freq_3", "Number Phone Calls Live Answered Past 3 Mos"),
    ("liveanswer_freq3_6", "Number Phone Calls Live Answered Betw. Past 3 and 6 Mos"),
    ("liveanswer_freq6_12", "Number Phone Calls Live Answered Betw. Past 6 and 12 Mos"),
    ("liveanswer_comp_freq_3", "Number Phone Calls Live Answered or Completed Past 3 Mos"),
    ("liveanswer_comp_freq3_6", "Number Phone Calls Live Answered or Completed Betw. Past 3 and 6 Mos"),
    ("liveanswer_comp_freq6_12", "Number Phone Calls Live Answered or Completed Betw. Past 6 and 12 Mos"),
    ("mailct", "Number Mail Pieces Responded To Past 12 Mos"),
    ("mailercount_sent_180", "Number Email Mailer IDs Received Past 180 Days"),
    ("mailercount_sent_30days", "Number Email Mailer IDs Received Past 30 Days"),
    ("mailct_all", "Number Mail Pieces Received Past 24 Mos"),
    ("mailct_past12", "Number Mail Pieces Received Past 12 Mos"),
    ("call_freq_12mo_both", "Number Phone Call Records Past 12 Mos")
]
var_list = [v[0] for v in var_list_with_labels]

# PROC MEANS
agg_exprs = [
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"{c}_NMiss") for c in var_list
] + [
    F.mean(c).alias(f"{c}_Mean") for c in var_list
] + [
    F.expr(f'percentile_approx({c}, 0.5)').alias(f"{c}_Median") for c in var_list
] + [
    F.stddev(c).alias(f"{c}_Std") for c in var_list
] + [
    F.min(c).alias(f"{c}_Min") for c in var_list
] + [
    F.expr(f'percentile_approx({c}, 0.25)').alias(f"{c}_P25") for c in var_list
] + [
    F.expr(f'percentile_approx({c}, 0.75)').alias(f"{c}_P75") for c in var_list
] + [
    F.max(c).alias(f"{c}_Max") for c in var_list
]
df_contact_means = df_contact_history_sum.agg(*agg_exprs)

# PROC TRANSPOSE
num_stats = len(df_contact_means.columns)
unpivot_expr = f"stack({num_stats}, {', '.join([f'cast(\"{c}\" as string), cast(`{c}` as string)' for c in df_contact_means.columns])}) as (_NAME_, COL1)"
df_contact_means_trans = df_contact_means.selectExpr(unpivot_expr)

# Add _LABEL_ column
labels_map = {v[0]: v[1] for v in var_list_with_labels}
def get_label(name_stat):
    parts = name_stat.rsplit('_', 1)
    var_name = parts[0]
    return labels_map.get(var_name, None)

get_label_udf = F.udf(get_label, StringType())
df_contact_means_trans = df_contact_means_trans.withColumn("_LABEL_", get_label_udf(F.col("_NAME_")))

# Update logic
input_table_name = "scoring.Input_Num_Var_&runtype._&muldate" # This should be parameterized
try:
    df_target = spark.table(input_table_name)
    df_new_names = df_contact_means_trans.select("_NAME_").distinct()
    
    # Remove old rows that will be replaced
    df_target_filtered = df_target.join(df_new_names, "_NAME_", "left_anti")
    
    # Union the remaining old data with the new data
    df_final_input_vars = df_target_filtered.unionByName(df_contact_means_trans)
except Exception:
    # If the target table does not exist, the new data becomes the table
    df_final_input_vars = df_contact_means_trans

# Save the updated table
df_final_input_vars.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(input_table_name)

#End-DBShift