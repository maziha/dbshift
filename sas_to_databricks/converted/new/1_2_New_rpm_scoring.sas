import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from datetime import datetime
from dateutil.relativedelta import relativedelta

# This script assumes the SparkSession is available as 'spark'.
# It also assumes that SAS macro variables like '&muldate', '&conn', etc.,
# are provided as Python variables.
# Example placeholder values:
muldate = "20230115"
conn = "your_db_connection"
dsn = "your_dsn"
usern = "your_user"
passw = "your_password"
ref3 = "your_ref3_db"

# Equivalent of the DATA _NULL_ step for date calculation
muldate_dt = datetime.strptime(muldate, "%Y%m%d")
lastYear_dt = muldate_dt - relativedelta(years=1)
lastyear_1_dt = lastYear_dt.replace(month=1, day=1, day=1)
currentyear_dt = lastyear_1_dt + relativedelta(years=1)

st_date = f"'{lastyear_1_dt.strftime('%Y-%m-%d')}'"
ed_date = f"'{currentyear_dt.strftime('%Y-%m-%d')}'"

# %put _user_;
# The user can be retrieved from the spark context if needed, but we will print the resolved dates
print(f"st_date: {st_date}")
print(f"ed_date: {ed_date}")

# proc sql; CONNECT TO...
# This block translates the passthrough query.
# The following code is the direct translation but requires a configured JDBC connection.
# It is commented out but represents the required logic.
# jdbc_url = f"jdbc:your_db_url;databaseName={dsn}"
# connection_properties = {
#   "user": usern,
#   "password": passw,
#   "driver": "your.jdbc.Driver"
# }
sql_passthrough_query = f"""
(select chid_key,
		case when transtype_key=12020 and (substring(d_mm_source_cd_drv,1,6) <>'CHECKB' and
			substring(d_mm_source_cd_drv,1,3) <>'CCB') and cast(date_key as date) between {st_date} and {ed_date} then 1
			end as foundation_donations_12mo,
		case when transtype_key=12010 and petition_fl = '1' 
			and cast(date_key as date) between {st_date} and {ed_date} then 1
			end as advocacy_petitions_12mo,
		case when transtype_key=12010 and contrib_amt <> 0
			and cast(date_key as date) between {st_date} and {ed_date} then 1
			end as advocacy_donations_12mo
	from {ref3}.f_mm_contribution
	where cast(date_key as date) between {st_date} and {ed_date} and transtype_key in (12020,12010) 
		and chid_key not in (1,0)) as t
"""
# df_advo_fndn_chid_all_raw = spark.read.jdbc(url=jdbc_url, table=sql_passthrough_query, properties=connection_properties)
# df_advo_fndn_chid_all = df_advo_fndn_chid_all_raw.filter(F.col("chid_key").isNotNull())

# To make the rest of the script runnable without a live DB connection,
# we will create a placeholder DataFrame. In a production environment,
# the JDBC code above would be used.
schema_advo_fndn_chid_all = StructType([
    StructField("chid_key", IntegerType(), True),
    StructField("foundation_donations_12mo", IntegerType(), True),
    StructField("advocacy_petitions_12mo", IntegerType(), True),
    StructField("advocacy_donations_12mo", IntegerType(), True),
])
df_advo_fndn_chid_all = spark.createDataFrame([], schema=schema_advo_fndn_chid_all)


# proc sql; create table advo_fndn_chid_all_clean
df_advo_fndn_chid_all.createOrReplaceTempView("advo_fndn_chid_all")
df_advo_fndn_chid_all_clean = spark.sql("""
    SELECT 
        chid_key, 
        max(foundation_donations_12mo) as foundation_donations_12mo_max,
        max(advocacy_petitions_12mo) as advocacy_petitions_12mo_max, 
        max(advocacy_donations_12mo) as advocacy_donations_12mo_max
    FROM advo_fndn_chid_all
    GROUP BY chid_key
""")

# data one;
df_one = spark.table("aarpdata.special_keycodes").select(
    "orphan_key_codes",
    "corp_memshp_key_codes",
    "gift_memshp_key_codes",
    "cmmnty_memshp_key_codes",
    "hardship_key_codes"
)

# data onea, oneb, ...
df_onea = df_one.select(F.col("orphan_key_codes").alias("memoriginkey")).filter((F.col("memoriginkey") != ' ') & F.col("memoriginkey").isNotNull())
df_oneb = df_one.select(F.col("corp_memshp_key_codes").alias("memoriginkey")).filter((F.col("memoriginkey") != ' ') & F.col("memoriginkey").isNotNull())
df_onec = df_one.select(F.col("gift_memshp_key_codes").alias("memoriginkey")).filter((F.col("memoriginkey") != ' ') & F.col("memoriginkey").isNotNull())
df_oned = df_one.select(F.col("cmmnty_memshp_key_codes").alias("memoriginkey")).filter((F.col("memoriginkey") != ' ') & F.col("memoriginkey").isNotNull())
df_onee = df_one.select(F.col("hardship_key_codes").alias("memoriginkey")).filter((F.col("memoriginkey") != ' ') & F.col("memoriginkey").isNotNull())

# data final;
df_final = df_onea.unionByName(df_oneb).unionByName(df_onec).unionByName(df_oned).unionByName(df_onee)

# data rpm_0 and proc sort nodupkey;
df_rpm_0_fmt_source = df_final.withColumnRenamed("memoriginkey", "start")
df_rpm_0 = df_rpm_0_fmt_source.dropDuplicates(["start"])
# The PROC FORMAT is translated into this lookup DataFrame 'df_rpm_0'

# proc sql; create table combined2_all
df_geo_appends = spark.table("intermed.geo_appends").select(
    "age_agg_ind", "memacctnum", "na3", "memxrenew", "Overall_Active_SP_Reltshps", "diversity_flag_agg_ind",
    "na2", "workstatus", "maritalstatus", "globally_opted_in", "vtm_vol_flag_act", "vtm_num_assignments_act",
    "memoriginkey", "mempaiddate"
)

df_combined2_all = df_geo_appends.join(
    df_advo_fndn_chid_all_clean,
    df_geo_appends.memacctnum == df_advo_fndn_chid_all_clean.chid_key,
    "left"
)

# data toscore_all;
df_toscore_all_prep = df_combined2_all.withColumn("renewals", F.col("memxrenew")) \
    .withColumn("sp_rel", F.coalesce(F.col("Overall_Active_SP_Reltshps"), F.lit(0))) \
    .withColumn("donfnd12", F.coalesce(F.col("foundation_donations_12mo_max"), F.lit(0))) \
    .withColumn("donadv12", F.coalesce(F.col("advocacy_donations_12mo_max"), F.lit(0))) \
    .withColumn("petadv12", F.coalesce(F.col("advocacy_petitions_12mo_max"), F.lit(0))) \
    .withColumn("cur_term", F.col("na2")) \
    .withColumn("work", F.col("workstatus")) \
    .withColumn("marital", F.col("maritalstatus")) \
    .withColumn("category", F.when(F.col("renewals") == 0, 'N').otherwise('R')) \
    .withColumn("GOI", F.col("globally_opted_in")) \
    .withColumn("ch_acq", F.substring(F.col("memoriginkey"), 1, 1))

df_toscore_all = df_toscore_all_prep.select(
    "memacctnum", "renewals", "sp_rel", "donfnd12", "donadv12", "petadv12", "ch_acq", "cur_term", 
    "mempaiddate", "memacctnum", "work", "marital", "category", "GOI", "vtm_vol_flag_act", 
    "vtm_num_assignments_act", "memoriginkey", "NA3", "age_agg_ind", "diversity_flag_agg_ind"
)

# data scoring.rpm_&muldate
# Join with the format lookup table for the override condition
df_toscore_for_scoring = df_toscore_all.join(
    df_rpm_0.withColumnRenamed("start", "fmt_key"),
    df_toscore_all.memoriginkey == F.col("fmt_key"),
    "left"
)

# Define conditions for readability
ch_acq_grp1 = ['N','B','M','C','J','Q','W','X']
ch_acq_grp2 = ['D','Y']
ch_acq_grp3 = ['H','F','U','G','T','R','#','A','E']
ch_acq_grp4 = ['I','S','V',';','P',"'",'2','7','\\','Z','6']
ch_acq_grp5 = ['K','L','1','3','0']
marital_grp1 = ['X','U','B','D','W']
marital_grp2 = ['S','I']
marital_grp3 = ['M']
marital_grp4 = ['X','U','B','P','W','D']
marital_grp5 = ['M','S']
marital_grp6 = ['X','U','I','B','W','D']
marital_grp7 = ['S','I','B']
marital_grp8 = ['X','U','W']
marital_grp9 = ['M','B']
marital_grp10 = ['S','X','I','U','W']

# Build the complex scoring logic using nested F.when()
rpm_new_score_calc = F.when(F.col("category") == 'N',
    F.when(F.col("sp_rel") == 0,
        F.when(F.col("petadv12") > 0, 7)
        .when(F.col("petadv12") == 0,
            F.when(F.col("ch_acq").isin(ch_acq_grp1),
                F.when(F.col("cur_term") > 12, 2)
                .when(F.col("cur_term") <= 12,
                    F.when(F.col("age_agg_ind") <= 63, 4)
                    .when(F.col("age_agg_ind") > 63, 7)
                )
            )
            .when(F.col("ch_acq").isin(ch_acq_grp2),
                F.when(F.col("cur_term") <= 12,
                    F.when(F.col("diversity_flag_agg_ind") == 1, 4)
                    .when(F.col("diversity_flag_agg_ind") != 1,
                        F.when(F.col("age_agg_ind") <= 50, 4)
                        .when((F.col("age_agg_ind") > 57) & (F.col("age_agg_ind") <= 60), 4)
                        .when((F.col("age_agg_ind") > 60) & (F.col("age_agg_ind") <= 67), 7)
                        .when((F.col("age_agg_ind") > 67) & (F.col("age_agg_ind") <= 77), 4)
                        .when(F.col("age_agg_ind") > 77, 4)
                        .when((F.col("age_agg_ind") > 50) & (F.col("age_agg_ind") <= 52), F.when(F.col("marital") == 'M', 4).otherwise(3))
                        .when((F.col("age_agg_ind") > 52) & (F.col("age_agg_ind") <= 57),
                            F.when(F.col("marital") == 'M', 4)
                            .when(F.col("marital").isin(marital_grp1), 3)
                            .when(F.col("marital").isin(marital_grp2), 4)
                        )
                    )
                )
                .when(F.col("cur_term") > 12,
                    F.when(F.col("marital") == 'M',
                        F.when(F.col("age_agg_ind") <= 60, 3)
                        .when(F.col("age_agg_ind") > 60, 4)
                    ).otherwise(
                        F.when(F.col("age_agg_ind") <= 60, 3)
                        .when((F.col("age_agg_ind") > 60) & (F.col("age_agg_ind") <= 67), 4)
                        .when(F.col("age_agg_ind") > 67, 2)
                    )
                )
            )
            .when(F.col("ch_acq").isin(ch_acq_grp3),
                F.when(F.col("marital").isin(marital_grp4), 2)
                .when(F.col("marital").isin(marital_grp2), 3)
                .when(F.col("marital") == 'M',
                    F.when(F.col("age_agg_ind") <= 57, 3)
                    .when((F.col("age_agg_ind") > 57) & (F.col("age_agg_ind") <= 63), 4)
                    .when(F.col("age_agg_ind") > 63, 7)
                )
            )
            .when(F.col("ch_acq").isin(ch_acq_grp4), 1)
            .when(F.col("ch_acq").isin(ch_acq_grp5),
                F.when(F.col("marital").isin(marital_grp5),
                    F.when(F.col("age_agg_ind") <= 57, 4)
                    .when((F.col("age_agg_ind") > 57) & (F.col("age_agg_ind") <= 67), 7)
                    .when(F.col("age_agg_ind") > 67, 7)
                ).when(F.col("marital").isin(marital_grp6), 4)
            )
        )
    ).when(F.col("sp_rel") > 0,
        F.when(F.col("ch_acq").isin('I','F','G'), 7)
        .otherwise(
            F.when(F.col("age_agg_ind") <= 54, 7)
            .when(F.col("age_agg_ind") > 54, 7)
        )
    ).otherwise(4)
).when(F.col("category") == 'R',
    F.when(F.col("sp_rel") == 0,
        F.when(F.col("petadv12") <= 0,
            F.when(F.col("renewals") == 1,
                F.when(F.col("marital") == 'M',
                    F.when(F.col("cur_term") <= 12, 8)
                    .when(F.col("cur_term") > 12,
                        F.when(F.col("goi") == 0, 6)
                        .when(F.col("goi") > 0, 8)
                    )
                )
                .when(F.col("marital").isin(marital_grp7),
                    F.when(F.col("cur_term") <= 12, 8)
                    .when(F.col("cur_term") > 12, 6)
                ).when(F.col("marital").isin(marital_grp8), 5)
            )
            .when(F.col("renewals") == 2,
                F.when(F.col("cur_term") <= 12,
                    F.when(F.col("marital").isin(marital_grp9), 9)
                    .when(F.col("marital").isin(marital_grp10), 9)
                ).when(F.col("cur_term") > 12,
                    F.when(F.col("age_agg_ind") <= 65, 9)
                    .when(F.col("age_agg_ind") > 65, 6)
                )
            )
            .when(F.col("renewals") == 3,
                F.when(F.col("cur_term") <= 12,
                    F.when(F.col("marital") == 'M',
                        F.when(F.col("age_agg_ind") <= 65, 10)
                        .when(F.col("age_agg_ind") > 65, 9)
                    ).otherwise(9)
                ).when(F.col("cur_term") > 12,
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
                ).when(F.col("age_agg_ind") > 80, 8)
            )
            .when((F.col("renewals") > 5) & (F.col("renewals") <= 8),
                F.when(F.col("cur_term") <= 12,
                    F.when(F.col("age_agg_ind") <= 65, 11)
                    .when((F.col("age_agg_ind") > 65) & (F.col("age_agg_ind") <= 75), 10)
                    .when(F.col("age_agg_ind") > 75, 9)
                ).when(F.col("cur_term") > 12,
                    F.when(F.col("age_agg_ind") <= 80, 10)
                    .when(F.col("age_agg_ind") > 80, 8)
                )
            )
            .when(F.col("renewals") > 8,
                F.when(F.col("cur_term") <= 12,
                    F.when(F.col("age_agg_ind") <= 71, 11)
                    .when((F.col("age_agg_ind") > 71) & (F.col("age_agg_ind") <= 80), 11)
                    .when(F.col("age_agg_ind") > 80, 10)
                ).when(F.col("cur_term") > 12,
                    F.when(F.col("age_agg_ind") <= 75, 10)
                    .when((F.col("age_agg_ind") > 75) & (F.col("age_agg_ind") <= 85), 9)
                    .when(F.col("age_agg_ind") > 85, 6)
                )
            )
        ).when(F.col("petadv12") > 0,
            F.when(F.col("donadv12") <= 0,
                F.when(F.col("renewals") <= 8, 11)
                .when(F.col("renewals") > 8, 11)
            ).when(F.col("donadv12") > 0, 11)
        )
    ).when(F.col("sp_rel") > 0,
        F.when(F.col("petadv12") <= 0,
            F.when(F.col("renewals") == 1, 11)
            .when(F.col("renewals") == 2, F.when(F.col("marital") == 'M', 11).otherwise(11))
            .when(F.col("renewals") > 2, 11)
        ).when(F.col("petadv12") > 0, 11)
    ).otherwise(10)
)

df_rpm_scored = df_toscore_for_scoring.withColumn("rpm_new_score_unformatted", rpm_new_score_calc) \
    .withColumn("rpm_new_score", F.when(F.col("fmt_key").isNotNull(), 0).otherwise(F.col("rpm_new_score_unformatted"))) \
    .drop("rpm_new_score_unformatted", "fmt_key")

# proc sort data=scoring.rpm_&muldate nodupkey
df_rpm_final_deduped = df_rpm_scored.dropDuplicates(['memacctnum'])

# Write final scored data
df_rpm_final_deduped.write.format("delta").mode("overwrite").saveAsTable(f"scoring.rpm_{muldate.replace('-', '')}")

# data intermed.geo_appends_rpm; merge...
df_geo_appends_input = spark.table("intermed.geo_appends")
df_rpm_scored_input = spark.table(f"scoring.rpm_{muldate.replace('-', '')}")

df_geo_appends_rpm = df_geo_appends_input.join(
    df_rpm_scored_input,
    on="memacctnum",
    how="left"
).withColumnRenamed("rpm_new_score", "rpm_score")

# proc sql; create table intermed.geo_appends_rpm as select * from...
df_geo_appends_rpm_apr = spark.table("intermed.geo_appends_rpm_apr")
df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_geo_appends_rpm_apr.createOrReplaceTempView("geo_appends_rpm_apr")

# To correctly replicate SAS `SELECT *` behavior where right-table columns overwrite
# left-table columns with the same name, we dynamically build the SELECT statement.
cols_a = df_geo_appends_rpm.columns
cols_b = df_geo_appends_rpm_apr.columns
select_list = []

for c in cols_a:
    if c in cols_b and c != 'mid_key':
        select_list.append(f"b.{c} AS {c}")
    else:
        select_list.append(f"a.{c} AS {c}")
for c in cols_b:
    if c not in cols_a:
        select_list.append(f"b.{c} AS {c}")

sql_query_final_join = f"SELECT {', '.join(select_list)} FROM geo_appends_rpm a LEFT JOIN geo_appends_rpm_apr b ON a.mid_key = b.mid_key"

df_geo_appends_rpm_final = spark.sql(sql_query_final_join)

# Write the final updated table
df_geo_appends_rpm_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")
#End-DBShift