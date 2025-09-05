import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from dateutil.relativedelta import relativedelta

spark = SparkSession.builder.appName("new_rpm_scoring").getOrCreate()

# Placeholder for macro variables that would be passed into the script
# In a real Databricks environment, these might be set by widgets
muldate = '20230101' # Example date in YYYYMMDD format
conn = "your_jdbc_url"
dsn = "your_dsn" # This is often part of the JDBC URL
usern = "your_user"
passw = "your_password"
ref3 = "your_ref3_db"

# Date calculations based on the SAS DATA _NULL_ step
muldate_dt_obj = datetime.strptime(muldate, '%Y%m%d')
lastYear = muldate_dt_obj - relativedelta(years=1)
lastyear_1 = lastYear.replace(day=1, month=1)
currentyear = lastYear + relativedelta(years=1)

st_date = (lastyear_1 + relativedelta(days=1)).strftime('%Y-%m-%d')
ed_date = currentyear.replace(day=1, month=1).strftime('%Y-%m-%d')

st_date_for_sql = f"'{st_date}'"
ed_date_for_sql = f"'{ed_date}'"

# proc sql with CONNECT TO
sql_query = f"""
select chid_key,
		/*case when transtype_key=12020 and (substring(d_mm_source_cd_drv,1,6) ='CHECKB' or 
			substring(d_mm_source_cd_drv,1,3) ='CCB') and cast(date_key as date) between {st_date_for_sql} and {ed_date_for_sql} then 1
				end as foundation_donations_checkb_12mo, */
		case when transtype_key=12020 and (substring(d_mm_source_cd_drv,1,6) <>'CHECKB' and
			substring(d_mm_source_cd_drv,1,3) <>'CCB') and cast(date_key as date) between {st_date_for_sql} and {ed_date_for_sql} then 1
			end as foundation_donations_12mo,
		case when transtype_key=12010 and petition_fl = '1' 
			and cast(date_key as date) between {st_date_for_sql} and {ed_date_for_sql} then 1
			end as advocacy_petitions_12mo,
		case when transtype_key=12010 and contrib_amt <> 0
			and cast(date_key as date) between {st_date_for_sql} and {ed_date_for_sql} then 1
			end as advocacy_donations_12mo
	from {ref3}.f_mm_contribution
	where cast(date_key as date) between {st_date_for_sql} and {ed_date_for_sql} and transtype_key in (12020,12010) 
		and chid_key not in (1,0)
"""

df_advo_fndn_chid_all = spark.read \
    .format("jdbc") \
    .option("url", conn) \
    .option("dbtable", f"({sql_query}) as subquery") \
    .option("user", usern) \
    .option("password", passw) \
    .load() \
    .filter(F.col("chid_key").isNotNull())

# proc sql to aggregate
df_advo_fndn_chid_all.createOrReplaceTempView("advo_fndn_chid_all")
df_advo_fndn_chid_all_clean = spark.sql("""
    select chid_key, 
           max(foundation_donations_12mo) as foundation_donations_12mo_max,
           max(advocacy_petitions_12mo) as advocacy_petitions_12mo_max, 
           max(advocacy_donations_12mo) as advocacy_donations_12mo_max
    from advo_fndn_chid_all
    group by chid_key
""")

# Reading special keycodes
df_one = spark.table("aarpdata.special_keycodes").select(
    "orphan_key_codes",
    "corp_memshp_key_codes",
    "gift_memshp_key_codes",
    "cmmnty_memshp_key_codes",
    "hardship_key_codes"
)

# Creating separate dataframes for each keycode type
df_onea = df_one.select(F.col("orphan_key_codes").alias("memoriginkey")) \
                .filter(F.col("memoriginkey").isNotNull() & (F.col("memoriginkey") != ' '))

df_oneb = df_one.select(F.col("corp_memshp_key_codes").alias("memoriginkey")) \
                .filter(F.col("memoriginkey").isNotNull() & (F.col("memoriginkey") != ' '))

df_onec = df_one.select(F.col("gift_memshp_key_codes").alias("memoriginkey")) \
                .filter(F.col("memoriginkey").isNotNull() & (F.col("memoriginkey") != ' '))

df_oned = df_one.select(F.col("cmmnty_memshp_key_codes").alias("memoriginkey")) \
                .filter(F.col("memoriginkey").isNotNull() & (F.col("memoriginkey") != ' '))

df_onee = df_one.select(F.col("hardship_key_codes").alias("memoriginkey")) \
                .filter(F.col("memoriginkey").isNotNull() & (F.col("memoriginkey") != ' '))

# Unioning the keycode dataframes
df_final = df_onea.unionByName(df_oneb).unionByName(df_onec).unionByName(df_oned).unionByName(df_onee)

# Preparing the format control dataframe
df_rpm_0 = df_final.withColumnRenamed("memoriginkey", "start") \
                   .withColumn("fmtname", F.lit("override")) \
                   .withColumn("type", F.lit("c")) \
                   .withColumn("label", F.lit("*"))

# proc sort nodupkey
df_rpm_0 = df_rpm_0.dropDuplicates(["start"])

# proc sql to join geo data with advocacy/foundation data
df_geo_appends = spark.table("intermed.geo_appends").select(
    "age_agg_ind", "memacctnum", "na3", "memxrenew", "Overall_Active_SP_Reltshps", "diversity_flag_agg_ind",
    "na2", "workstatus", "maritalstatus", "globally_opted_in", "vtm_vol_flag_act", "vtm_num_assignments_act",
    "memoriginkey", "mempaiddate"
)

df_geo_appends.createOrReplaceTempView("geo_appends")
df_advo_fndn_chid_all_clean.createOrReplaceTempView("advo_fndn_chid_all_clean")

df_combined2_all = spark.sql("""
    select a.*, 
           b.foundation_donations_12mo_max,
           b.advocacy_petitions_12mo_max, 
           b.advocacy_donations_12mo_max
    from geo_appends as a
    left join advo_fndn_chid_all_clean as b
    on a.memacctnum=b.chid_key
""")

# data toscore_all step
df_toscore_all = df_combined2_all.withColumn("renewals", F.col("memxrenew")) \
    .withColumn("sp_rel", F.when(F.col("Overall_Active_SP_Reltshps").isNull(), 0).otherwise(F.col("Overall_Active_SP_Reltshps"))) \
    .withColumn("donfnd12", F.when(F.col("foundation_donations_12mo_max").isNull(), 0).otherwise(F.col("foundation_donations_12mo_max"))) \
    .withColumn("donadv12", F.when(F.col("advocacy_donations_12mo_max").isNull(), 0).otherwise(F.col("advocacy_donations_12mo_max"))) \
    .withColumn("petadv12", F.when(F.col("advocacy_petitions_12mo_max").isNull(), 0).otherwise(F.col("advocacy_petitions_12mo_max"))) \
    .withColumn("cur_term", F.col("na2")) \
    .withColumn("work", F.col("workstatus")) \
    .withColumn("marital", F.col("maritalstatus")) \
    .withColumn("category", F.when(F.col("renewals") == 0, 'N').otherwise('R')) \
    .withColumn("GOI", F.col("globally_opted_in")) \
    .withColumn("ch_acq", F.substring(F.col("memoriginkey"), 1, 1))

df_toscore_all = df_toscore_all.select(
    "memacctnum", "renewals", "sp_rel", "donfnd12", "donadv12", "petadv12", "ch_acq", "cur_term",
    "mempaiddate", "work", "marital", "category", "GOI", "vtm_vol_flag_act", "vtm_num_assignments_act",
    "memoriginkey", "NA3", "age_agg_ind", "diversity_flag_agg_ind"
)

# Scoring logic
# This is a direct translation of the nested IF/THEN/ELSE logic into a single withColumn call with nested when/otherwise clauses.
df_scored = df_toscore_all.withColumn("rpm_new_score",
    F.when(F.col("category") == 'N',
        F.when(F.col("sp_rel") == 0,
            F.when(F.col("petadv12") > 0, 7)
            .otherwise(
                F.when(F.col("petadv12") == 0,
                    F.when(F.col("ch_acq").isin('N','B','M','C','J','Q','W','X'),
                        F.when(F.col("cur_term") > 12, 2)
                        .when(F.col("cur_term") <= 12,
                            F.when(F.col("age_agg_ind") <= 63, 4)
                            .when(F.col("age_agg_ind") > 63, 7)
                        )
                    )
                    .when(F.col("ch_acq").isin('D','Y'),
                        F.when(F.col("cur_term") <= 12,
                            F.when(F.col("diversity_flag_agg_ind") == 1, 4)
                            .when(F.col("diversity_flag_agg_ind") != 1,
                                F.when(F.col("age_agg_ind") <= 50, 4)
                                .when((F.col("age_agg_ind") > 57) & (F.col("age_agg_ind") <= 60), 4)
                                .when((F.col("age_agg_ind") > 60) & (F.col("age_agg_ind") <= 67), 7)
                                .when((F.col("age_agg_ind") > 67) & (F.col("age_agg_ind") <= 77), 4)
                                .when(F.col("age_agg_ind") > 77, 4)
                                .when((F.col("age_agg_ind") > 50) & (F.col("age_agg_ind") <= 52),
                                    F.when(F.col("marital") == 'M', 4).otherwise(3)
                                )
                                .when((F.col("age_agg_ind") > 52) & (F.col("age_agg_ind") <= 57),
                                    F.when(F.col("marital") == 'M', 4)
                                    .when(F.col("marital").isin('X','U','B','D','W'), 3)
                                    .when(F.col("marital").isin('S','I'), 4)
                                )
                            )
                        )
                        .when(F.col("cur_term") > 12,
                            F.when(F.col("marital") == 'M',
                                F.when(F.col("age_agg_ind") <= 60, 3)
                                .when(F.col("age_agg_ind") > 60, 4)
                            )
                            .otherwise(
                                F.when(F.col("age_agg_ind") <= 60, 3)
                                .when((F.col("age_agg_ind") > 60) & (F.col("age_agg_ind") <= 67), 4)
                                .when(F.col("age_agg_ind") > 67, 2)
                            )
                        )
                    )
                    .when(F.col("ch_acq").isin('H','F','U','G','T','R','#','A','E'),
                        F.when(F.col("marital").isin('X','U','B','P','W','D'), 2)
                        .when(F.col("marital").isin('S','I'), 3)
                        .when(F.col("marital") == 'M',
                            F.when(F.col("age_agg_ind") <= 57, 3)
                            .when((F.col("age_agg_ind") > 57) & (F.col("age_agg_ind") <= 63), 4)
                            .when(F.col("age_agg_ind") > 63, 7)
                        )
                    )
                    .when(F.col("ch_acq").isin('I','S','V',';','P',"'",'2','7','\\','Z','6'), 1)
                    .when(F.col("ch_acq").isin('K','L','1','3','0'),
                        F.when(F.col("marital").isin('M','S'),
                            F.when(F.col("age_agg_ind") <= 57, 4)
                            .when((F.col("age_agg_ind") > 57) & (F.col("age_agg_ind") <= 67), 7)
                            .when(F.col("age_agg_ind") > 67, 7)
                        )
                        .when(F.col("marital").isin('X','U','I','B','W','D'), 4)
                    )
                )
            )
        )
        .when(F.col("sp_rel") > 0,
            F.when(F.col("ch_acq").isin('I','F','G'), 7)
            .otherwise(
                F.when(F.col("age_agg_ind") <= 54, 7)
                .when(F.col("age_agg_ind") > 54, 7)
            )
        )
        .otherwise(4) # Default for category='N' if rpm_new_score is missing
    )
    .when(F.col("category") == 'R',
        F.when(F.col("sp_rel") == 0,
            F.when(F.col("petadv12") <= 0,
                F.when(F.col("renewals") == 1,
                    F.when(F.col("marital") == 'M',
                        F.when(F.col("cur_term") <= 12, 8)
                        .when(F.col("cur_term") > 12,
                            F.when(F.col("GOI") == 0, 6)
                            .when(F.col("GOI") > 0, 8)
                        )
                    )
                    .when(F.col("marital").isin('S','I','B'),
                        F.when(F.col("cur_term") <= 12, 8)
                        .when(F.col("cur_term") > 12, 6)
                    )
                    .when(F.col("marital").isin('X','U','W'), 5)
                )
                .when(F.col("renewals") == 2,
                    F.when(F.col("cur_term") <= 12,
                        F.when(F.col("marital").isin('M','B'), 9)
                        .when(F.col("marital").isin('S','X','I','U','W'), 9)
                    )
                    .when(F.col("cur_term") > 12,
                        F.when(F.col("age_agg_ind") <= 65, 9)
                        .when(F.col("age_agg_ind") > 65, 6)
                    )
                )
                .when(F.col("renewals") == 3,
                    F.when(F.col("cur_term") <= 12,
                        F.when(F.col("marital") == 'M',
                            F.when(F.col("age_agg_ind") <= 65, 10)
                            .when(F.col("age_agg_ind") > 65, 9)
                        )
                        .otherwise(9)
                    )
                    .when(F.col("cur_term") > 12,
                        F.when(F.col("age_agg_ind") <= 65, 9)
                        .when(F.col("age_agg_ind") > 65, 8)
                    )
                )
                .when((F.col("renewals") > 3) & (F.col("renewals") <= 5),
                    F.when(F.col("age_agg_ind") <= 65, 10)
                    .when((F.col("age_agg_ind") > 65) & (F.col("age_agg_ind") <= 68), 9)
                    .when((F.col("age_agg_ind") > 68) & (F.col("age_agg_ind") <= 80),
                        F.when(F.col("cur_term") <= 12, 10)
                        .when(F.col("cur_term") > 12, 9)
                    )
                    .when(F.col("age_agg_ind") > 80, 8)
                )
                .when((F.col("renewals") > 5) & (F.col("renewals") <= 8),
                    F.when(F.col("cur_term") <= 12,
                        F.when(F.col("age_agg_ind") <= 65, 11)
                        .when((F.col("age_agg_ind") > 65) & (F.col("age_agg_ind") <= 75), 10)
                        .when(F.col("age_agg_ind") > 75, 9)
                    )
                    .when(F.col("cur_term") > 12,
                        F.when(F.col("age_agg_ind") <= 80, 10)
                        .when(F.col("age_agg_ind") > 80, 8)
                    )
                )
                .when(F.col("renewals") > 8,
                    F.when(F.col("cur_term") <= 12,
                        F.when(F.col("age_agg_ind") <= 71, 11)
                        .when((F.col("age_agg_ind") > 71) & (F.col("age_agg_ind") <= 80), 11)
                        .when(F.col("age_agg_ind") > 80, 10)
                    )
                    .when(F.col("cur_term") > 12,
                        F.when(F.col("age_agg_ind") <= 75, 10)
                        .when((F.col("age_agg_ind") > 75) & (F.col("age_agg_ind") <= 85), 9)
                        .when(F.col("age_agg_ind") > 85, 6)
                    )
                )
            )
            .when(F.col("petadv12") > 0,
                F.when(F.col("donadv12") <= 0,
                    F.when(F.col("renewals") <= 8, 11)
                    .when(F.col("renewals") > 8, 11)
                )
                .when(F.col("donadv12") > 0, 11)
            )
        )
        .when(F.col("sp_rel") > 0,
            F.when(F.col("petadv12") <= 0,
                F.when(F.col("renewals") == 1, 11)
                .when(F.col("renewals") == 2,
                    F.when(F.col("marital") == 'M', 11)
                    .otherwise(11)
                )
                .when(F.col("renewals") > 2, 11)
            )
            .when(F.col("petadv12") > 0, 11)
        )
        .otherwise(10) # Default for category='R' if rpm_new_score is missing
    )
)

# Apply the override format logic
df_scored_with_override_check = df_scored.join(df_rpm_0, df_scored.memoriginkey == df_rpm_0.start, "left")

df_rpm_final = df_scored_with_override_check.withColumn(
    "rpm_new_score",
    F.when(F.col("start").isNotNull(), 0).otherwise(F.col("rpm_new_score"))
).drop("start", "fmtname", "type", "label")

# Write to scoring table
df_rpm_final.write.format("delta").mode("overwrite").saveAsTable(f"scoring.rpm_{muldate}")

# PROC SORT NODUPKEY on the output table before next join
df_rpm_final_dedup = spark.table(f"scoring.rpm_{muldate}").dropDuplicates(["MEMACCTNUM"])

# Merge back with geo_appends
df_geo_appends_for_merge = spark.table("intermed.geo_appends")

df_geo_appends_rpm = df_geo_appends_for_merge.join(
    df_rpm_final_dedup,
    df_geo_appends_for_merge.memacctnum == df_rpm_final_dedup.memacctnum,
    "left"
).drop(df_rpm_final_dedup.memacctnum)

df_geo_appends_rpm = df_geo_appends_rpm.withColumnRenamed("rpm_new_score", "rpm_score")
df_geo_appends_rpm.write.format("delta").mode("overwrite").saveAsTable("intermed.geo_appends_rpm")


# Final join step from PROC SQL
df_geo_appends_rpm_current = spark.table("intermed.geo_appends_rpm")
df_geo_appends_rpm_apr = spark.table("intermed.geo_appends_rpm_apr")

# Resolve column name conflicts for the `select *` join
cols_a = set(df_geo_appends_rpm_current.columns)
cols_b = set(df_geo_appends_rpm_apr.columns)
join_keys = {"mid_key"}
conflicting_cols = (cols_a & cols_b) - join_keys

df_b_renamed = df_geo_appends_rpm_apr
for col_name in conflicting_cols:
    df_b_renamed = df_b_renamed.withColumnRenamed(col_name, f"{col_name}_b")

# Perform the left join
df_final_join = df_geo_appends_rpm_current.join(
    df_b_renamed,
    df_geo_appends_rpm_current.mid_key == df_b_renamed.mid_key,
    "left"
).drop(df_b_renamed.mid_key)

# Overwrite the table with the joined result
df_final_join.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

#End-DBShift