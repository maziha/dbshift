import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import datetime
from dateutil.relativedelta import relativedelta

# This script assumes the following Python variables have been defined in the Databricks environment:
# spark: The SparkSession object.
# conn: The JDBC connection string component for the database type (e.g., 'sqlserver', 'oracle').
# dsn: The Data Source Name or server/host details for the JDBC connection.
# usern: The username for the database connection.
# passw: The password for the database connection.
# ref2: The database schema name for tables like f_contact_history_analytic.
# email_feed_end_dt: A string representing the end date for the email feed (e.g., '2023-12-31').
# runtype: A string indicating the run type (e.g., 'PROD').
# MULDATE: A string for the reporting date (e.g., '20231231').

enddt = datetime.date.today().strftime('%Y-%m-%d')
enddt_date = datetime.datetime.strptime(enddt, '%Y-%m-%d').date()
startdt_3mo_date = enddt_date - relativedelta(months=3)
startdt_3mo = startdt_3mo_date.strftime('%Y-%m-%d')

print(f"{enddt} {startdt_3mo}")

sql_query = f"""
(select mid_key, mailer_id,
	Sum(case when contact_disposition='EC' and cast(contact_dt as date) between '{startdt_3mo}' and '{enddt}' then 1 Else 0 End) as num_click_mailer_3mo
from {ref2}.f_contact_history_analytic a
left join {ref2}.d_contact_history_ib_analytic b
on a.d_contact_history_ib_key=b.d_contact_history_ib_key
left join {ref2}.d_campaign_analytic f
		on a.D_CAMPAIGN_KEY=f.D_CAMPAIGN_KEY
where b.comm_channel = 'E' and contact_direction='I' and cast(contact_dt as date) <= '{email_feed_end_dt}'
group by mid_key, mailer_id
order by mid_key, mailer_id
) as subq
"""

df_from_db = spark.read \
    .format("jdbc") \
    .option("url", dsn) \
    .option("dbtable", sql_query) \
    .option("user", usern) \
    .option("password", passw) \
    .load()

df_contact_ibmailer = df_from_db.filter(F.col("num_click_mailer_3mo") > 0)

df_contact_ibmailer.createOrReplaceTempView("contact_ibmailer")

df_totalclick = spark.sql("""
    SELECT
        mid_key,
        SUM(num_click_mailer_3mo) AS click_count
    FROM
        contact_ibmailer
    GROUP BY
        mid_key
""")

df_diffclick = spark.sql("""
    SELECT
        mid_key,
        COUNT(mailer_id) AS mailercount_click
    FROM
        contact_ibmailer
    GROUP BY
        mid_key
""")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")

df_geo_appends_rpm_modified = df_geo_appends_rpm.withColumn(
    "newsletter_opens_cnt_ytd",
    F.when(F.col("newsletter_opens_cnt_ytd") > 0, 0).otherwise(F.col("newsletter_opens_cnt_ytd"))
)

df_geo_appends_rpm_modified.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

df_geo_appends_rpm_reloaded = spark.table("intermed.geo_appends_rpm")
df_geo_appends_rpm_reloaded.createOrReplaceTempView("geo_appends_rpm")
df_totalclick.createOrReplaceTempView("totalclick")
df_diffclick.createOrReplaceTempView("diffclick")

df_emu_email_data = spark.sql("""
    SELECT
        a.cur_term,
        a.rpm_score,
        a.national_activities_12mo,
        a.mid_key,
        a.MAX_INDV_INSIGHT_UPDATE_DT,
        a.CENS_EDUC_POP25_PLUS_PERCENT_BAC,
        a.DRVS_Flag,
        a.globally_opted_in,
        a.advo_segment_cd,
        a.old_score22_vigintile,
        a.old_score35_vigintile,
        a.age_agg_ind,
        a.SecAge,
        a.cntct_lifstyle_12mo_agg_hhd,
        a.cntct_lifstyle_3mo_agg_hhd,
        a.EMAILABLE_AGG_IND,
        a.newsletter_opens_cnt_ytd,
        a.aarporg_i,
        a.IBX_CHILD_AGE_06_10_AGG_HHD,
        a.IBX_HOME_BUSINESS_AGG_HHD,
        a.CENS_STATE_CODE,
        a.CENS_AGE_POP_MEDIAN_AGE_OF_FEMAL,
        a.CENS_CHILD_HH_PERCENT_FAM_WITH_P,
        a.CENS_CHILD_HH_PERCENT_FEMALE_HOH,
        a.CENS_COMMUTE_WRKRS_PERCENT_PUBLI,
        a.CENS_EARN_HH_PERCENT_WITH_PUBLIC,
        a.CENS_OCCUP_EMPLD_PERCENT_SALES_A,
        a.CENS_OCCUP_EMPLD_PERCENT_TRANS_A,
        a.CENS_RENT_RNTL_MEDIAN_RENT,
        a.CENS_TYP_POP_PERCENT_STEPCHILD_I,
        a.NOVEMBER_GENERAL_ELECTION_DAY_AG,
        a.RADIO,
        a.SMARTPHONE,
        a.GUN_OWNERSHIP_MODEL,
        c.click_count,
        e.mailercount_click
    FROM
        geo_appends_rpm AS a
    LEFT JOIN
        totalclick AS c ON a.mid_key = c.mid_key
    LEFT JOIN
        diffclick AS e ON a.mid_key = e.mid_key
""")

df_scoring_allmodel = df_emu_email_data

df_scoring_allmodel = df_scoring_allmodel.withColumn("emailable_dum", F.when(F.col("EMAILABLE_AGG_IND") == 'Y', 1).otherwise(0))
df_scoring_allmodel = df_scoring_allmodel.withColumn("goi_missing_dum", F.when((F.col("globally_opted_in") == '') | (F.col("globally_opted_in").isNull()), 1).otherwise(0))
df_scoring_allmodel = df_scoring_allmodel.withColumn("child_6to10_dum", F.when(F.col("IBX_CHILD_AGE_06_10_AGG_HHD") == 1, 1).otherwise(0))
df_scoring_allmodel = df_scoring_allmodel.withColumn("homebiz_dum", F.when(F.col("IBX_HOME_BUSINESS_AGG_HHD") == 'Y', 1).otherwise(0))
df_scoring_allmodel = df_scoring_allmodel.withColumn("advo_s1_dum", F.when(F.col("advo_segment_cd") == 'S1', 1).otherwise(0))
df_scoring_allmodel = df_scoring_allmodel.withColumn("curterm_36_dum", F.when(F.col("cur_term") == '36', 1).otherwise(0))
df_scoring_allmodel = df_scoring_allmodel.withColumn("curterm_60_dum", F.when(F.col("cur_term") == '60', 1).otherwise(0))
df_scoring_allmodel = df_scoring_allmodel.withColumn("masters22_16to20_dum", F.when(F.col("old_score22_vigintile").isin('16','17','18','19','20'), 1).otherwise(0))
df_scoring_allmodel = df_scoring_allmodel.withColumn("masters35_18to20_dum", F.when(F.col("old_score35_vigintile").isin('18','19','20'), 1).otherwise(0))
df_scoring_allmodel = df_scoring_allmodel.withColumn("cntct_lifstyle_12mo_agg_hhd_c", F.when(F.col("cntct_lifstyle_12mo_agg_hhd").isNull(), 7.5457431).otherwise(F.col("cntct_lifstyle_12mo_agg_hhd")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("cntct_lifstyle_3mo_agg_hhd_c", F.when(F.col("cntct_lifstyle_3mo_agg_hhd").isNull(), 2.8474899).otherwise(F.col("cntct_lifstyle_3mo_agg_hhd")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("Age_c", F.when(F.col("age_agg_ind").isNull(), 63.8792719).otherwise(F.col("age_agg_ind")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("SecAge_c", F.when(F.col("SecAge").isNull(), 60.0000793).otherwise(F.col("SecAge")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("DRVS_Flag_c", F.when(F.col("DRVS_Flag").isNull(), 0.0303019).otherwise(F.col("DRVS_Flag")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("rpm_score_c", F.when(F.col("rpm_score").isNull(), 8.1991048).otherwise(F.col("rpm_score")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("national_activities_12mo_c", F.when(F.col("national_activities_12mo").isNull(), 0.000942685).otherwise(F.col("national_activities_12mo")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("newsletter_opens_cnt_ytd_c", F.when(F.col("newsletter_opens_cnt_ytd").isNull(), 2.7029642).otherwise(F.col("newsletter_opens_cnt_ytd")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("SY_HEALTHACTIVISTBIN_c", F.lit(74.6853794))
df_scoring_allmodel = df_scoring_allmodel.withColumn("aarporg_i_c", F.when(F.col("aarporg_i").isNull(), 0.6213935).otherwise(F.col("aarporg_i")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("CENS_STATE_CODE_c", F.when(F.col("CENS_STATE_CODE").isNull(), 27.7155399).otherwise(F.col("CENS_STATE_CODE")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("CENS_AGE_POP_MEDIAN_AGE_OF_FEM_c", F.when(F.col("CENS_AGE_POP_MEDIAN_AGE_OF_FEMAL").isNull(), 41.9193005).otherwise(F.col("CENS_AGE_POP_MEDIAN_AGE_OF_FEMAL")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("CENS_CHILD_HH_PERCENT_FAM_WITH_c", F.when(F.col("CENS_CHILD_HH_PERCENT_FAM_WITH_P").isNull(), 31.98366).otherwise(F.col("CENS_CHILD_HH_PERCENT_FAM_WITH_P")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("CENS_CHILD_HH_PERCENT_FEMALE_H_c", F.when(F.col("CENS_CHILD_HH_PERCENT_FEMALE_HOH").isNull(), 7.1573913).otherwise(F.col("CENS_CHILD_HH_PERCENT_FEMALE_HOH")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("CENS_COMMUTE_WRKRS_PERCENT_PUB_c", F.when(F.col("CENS_COMMUTE_WRKRS_PERCENT_PUBLI").isNull(), 4.0107932).otherwise(F.col("CENS_COMMUTE_WRKRS_PERCENT_PUBLI")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("CENS_EDUC_POP25_PLUS_PERCENT_B_c", F.when(F.col("CENS_EDUC_POP25_PLUS_PERCENT_BAC").isNull(), 20.3699).otherwise(F.col("CENS_EDUC_POP25_PLUS_PERCENT_BAC")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("CENS_EARN_HH_PERCENT_WITH_PUBL_c", F.when(F.col("CENS_EARN_HH_PERCENT_WITH_PUBLIC").isNull(), 2.267018).otherwise(F.col("CENS_EARN_HH_PERCENT_WITH_PUBLIC")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("CENS_OCCUP_EMPLD_PERCENT_SALES_c", F.when(F.col("CENS_OCCUP_EMPLD_PERCENT_SALES_A").isNull(), 11.6377736).otherwise(F.col("CENS_OCCUP_EMPLD_PERCENT_SALES_A")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("CENS_OCCUP_EMPLD_PERCENT_TRANS_c", F.when(F.col("CENS_OCCUP_EMPLD_PERCENT_TRANS_A").isNull(), 0.6033211).otherwise(F.col("CENS_OCCUP_EMPLD_PERCENT_TRANS_A")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("CENS_RENT_RNTL_MEDIAN_RENT_c", F.when(F.col("CENS_RENT_RNTL_MEDIAN_RENT").isNull(), 886.6904086).otherwise(F.col("CENS_RENT_RNTL_MEDIAN_RENT")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("CENS_TYP_POP_PERCENT_STEPCHILD_c", F.when(F.col("CENS_TYP_POP_PERCENT_STEPCHILD_I").isNull(), 1.2548025).otherwise(F.col("CENS_TYP_POP_PERCENT_STEPCHILD_I")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("MAX_INDV_INSIGHT_UPDATE_DT_c", F.when(F.col("MAX_INDV_INSIGHT_UPDATE_DT").isNull(), 1732662332).otherwise(F.col("MAX_INDV_INSIGHT_UPDATE_DT")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("ELECTIONDAYAGE_c", F.when(F.col("NOVEMBER_GENERAL_ELECTION_DAY_AG").isNull(), 63.7112733).otherwise(F.col("NOVEMBER_GENERAL_ELECTION_DAY_AG")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("MEDIA_RADIO_c", F.when(F.col("RADIO").isNull(), 44.8587859).otherwise(F.col("RADIO")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("MEDIA_SMARTPHONE_c", F.when(F.col("SMARTPHONE").isNull(), 24.1648341).otherwise(F.col("SMARTPHONE")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("SY_GUNSCORE_c", F.when(F.col("GUN_OWNERSHIP_MODEL").isNull(), 0.3558337).otherwise(F.col("GUN_OWNERSHIP_MODEL")))
df_scoring_allmodel = df_scoring_allmodel.withColumn("click_count", F.coalesce(F.col("click_count"), F.lit(0)))
df_scoring_allmodel = df_scoring_allmodel.withColumn("mailercount_click", F.coalesce(F.col("mailercount_click"), F.lit(0)))
df_scoring_allmodel = df_scoring_allmodel.withColumn("click_count_0_dum", F.when(F.col("click_count") == 0, 1).otherwise(0))

df_scoring_allmodel = df_scoring_allmodel.withColumn("logit_drvsafe_pro_em_old", 
    F.lit(-83.0339) + 
    F.col("mailercount_click") * 0.286 + 
    F.col("click_count_0_dum") * -0.7103 + 
    F.col("emailable_dum") * -0.4905 + 
    F.col("goi_missing_dum") * -0.2132 + 
    F.col("child_6to10_dum") * -0.2118 + 
    F.col("homebiz_dum") * 0.3521 + 
    F.col("advo_s1_dum") * -0.1999 + 
    F.col("curterm_36_dum") * 0.3797 + 
    F.col("curterm_60_dum") * 0.7942 + 
    F.col("masters22_16to20_dum") * 0.1572 + 
    F.col("masters35_18to20_dum") * 0.4746 + 
    F.col("cntct_lifstyle_12mo_agg_hhd_c") * -0.0323 + 
    F.col("cntct_lifstyle_3mo_agg_hhd_c") * 0.1097 + 
    F.col("Age_c") * -0.0359 + 
    F.col("DRVS_Flag_c") * 1.9304 + 
    F.col("rpm_score_c") * 0.1059 + 
    F.col("national_activities_12mo_c") * 1.3003 + 
    F.col("SY_HEALTHACTIVISTBIN_c") * 0.00497 + 
    F.col("aarporg_i_c") * 0.196 + 
    F.col("CENS_STATE_CODE_c") * -0.00798 + 
    F.col("CENS_AGE_POP_MEDIAN_AGE_OF_FEM_c") * -0.028 + 
    F.col("CENS_CHILD_HH_PERCENT_FAM_WITH_c") * 0.0109 + 
    F.col("CENS_CHILD_HH_PERCENT_FEMALE_H_c") * -0.0222 + 
    F.col("CENS_COMMUTE_WRKRS_PERCENT_PUB_c") * 0.0133 + 
    F.col("CENS_EARN_HH_PERCENT_WITH_PUBL_c") * -0.0218 + 
    F.col("CENS_EDUC_POP25_PLUS_PERCENT_B_c") * -0.013 + 
    F.col("CENS_OCCUP_EMPLD_PERCENT_SALES_c") * 0.0139 + 
    F.col("CENS_OCCUP_EMPLD_PERCENT_TRANS_c") * 0.0369 + 
    F.col("CENS_RENT_RNTL_MEDIAN_RENT_c") * 0.000365 + 
    F.col("CENS_TYP_POP_PERCENT_STEPCHILD_c") * -0.214 + 
    F.col("MAX_INDV_INSIGHT_UPDATE_DT_c") * 0.00000004236 + 
    F.col("ELECTIONDAYAGE_c") * 0.0415 + 
    F.col("MEDIA_RADIO_c") * 0.0234 + 
    F.col("MEDIA_SMARTPHONE_c") * -0.0201 + 
    F.col("SY_GUNSCORE_c") * -0.4455
)
df_scoring_allmodel = df_scoring_allmodel.withColumn("drvsafe_pro_em_score_old", F.exp(F.col("logit_drvsafe_pro_em_old")) / (1 + F.exp(F.col("logit_drvsafe_pro_em_old"))))

df_scoring_allmodel = df_scoring_allmodel.select("drvsafe_pro_em_score_old", "mid_key")

df_scoring_allmodel_deduped = df_scoring_allmodel.dropDuplicates(["mid_key"])

df_scoring_allmodel_with_dummy = df_scoring_allmodel_deduped.withColumn("dummy", F.lit(1))
window_spec = Window.partitionBy("dummy").orderBy(F.col("drvsafe_pro_em_score_old").desc())

df_bl_rank = df_scoring_allmodel_with_dummy.withColumn("drvsafe_pro_em_old", F.ntile(99).over(window_spec))

df_bl_rank = df_bl_rank.select("mid_key", "drvsafe_pro_em_old")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_geo_appends_rpm_dedup = df_geo_appends_rpm.dropDuplicates(["merkleid"])

if "drvsafe_pro_em_old" in df_geo_appends_rpm_dedup.columns:
    df_geo_appends_rpm_dedup = df_geo_appends_rpm_dedup.drop("drvsafe_pro_em_old")

df_joined = df_geo_appends_rpm_dedup.join(
    df_bl_rank,
    F.col("merkleid").cast("long") == df_bl_rank["mid_key"],
    "left"
)

select_cols = df_geo_appends_rpm_dedup.columns + [(F.col("drvsafe_pro_em_old") + 1).alias("drvsafe_pro_em_old")]

df_final_geo_appends_rpm = df_joined.select(*select_cols)

df_final_geo_appends_rpm.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

df_geo_appends_rpm_for_freq = spark.table("intermed.geo_appends_rpm")

print(f"Frequency Distribution for Old Driver Safety Email Model Scores: {MULDATE} Data")
df_freq = df_geo_appends_rpm_for_freq.groupBy("drvsafe_pro_em_old").count().orderBy(F.col("drvsafe_pro_em_old").asc_nulls_last())
df_freq.show(df_freq.count(), truncate=False)

#End-DBShift