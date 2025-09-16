import pyspark.sql.functions as F
from pyspark.sql.window import Window
from functools import reduce
from operator import add

# proc import datafile='/vg04/twalters/email_contact_model_curmonth_score.csv' out=email_contact_model_curmonth_sc1 replace dbms=csv;
df_email_contact_model_curmonth_sc1 = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/vg04/twalters/email_contact_model_curmonth_score.csv")

# proc contents data=intermed.bymailerid_transpose;
df_bymailerid_transpose = spark.table("intermed.bymailerid_transpose")
print("--- Schema for intermed.bymailerid_transpose ---")
df_bymailerid_transpose.printSchema()
print("--- Data Sample for intermed.bymailerid_transpose ---")
df_bymailerid_transpose.show(5, truncate=False)

# proc contents data=intermed.contact_history_sum varnum;
df_contact_history_sum = spark.table("intermed.contact_history_sum")
print("--- Schema for intermed.contact_history_sum ---")
df_contact_history_sum.printSchema()
print("--- Data Sample for intermed.contact_history_sum ---")
df_contact_history_sum.show(5, truncate=False)

# proc sql;
# create table email_contact_curmonth_scor as ...
df_email_contact_model_curmonth_sc1.createOrReplaceTempView("email_contact_model_curmonth_sc1")
df_bymailerid_transpose.createOrReplaceTempView("bymailerid_transpose")
df_contact_history_sum.createOrReplaceTempView("contact_history_sum")

df_email_contact_curmonth_scor = spark.sql("""
    SELECT *
    FROM email_contact_model_curmonth_sc1 AS a
    CROSS JOIN bymailerid_transpose AS b
    LEFT JOIN contact_history_sum AS c
    ON a.mid_key = c.mid_key
""")

# proc sql; select NAME into :vlist_sent_blank ...;
all_columns = df_email_contact_curmonth_scor.columns
vlist_sent_blank = sorted([
    col for col in all_columns
    if 'sent' in col and 'mailer' not in col and 'num' not in col and
       not col.startswith('.') and not col.startswith('1') and not col.startswith('9')
])

# %Let vlist_clicked_blank = %SysFunc( TranWrd( &vlist_sent_blank , sent , %str(clickrate) ) ) ;
vlist_clicked_blank = [col.replace('sent', 'clickrate') for col in vlist_sent_blank]

# data email_contact_curmonth_scor;
df_email_contact_curmonth_scor = df_email_contact_curmonth_scor.withColumn("AARP_clickrate", F.lit(0))

# array processing and pred_clicks calculation
product_expressions = []
for sent_col, clicked_col in zip(vlist_sent_blank, vlist_clicked_blank):
    if sent_col in df_email_contact_curmonth_scor.columns and clicked_col in df_email_contact_curmonth_scor.columns:
        product_expressions.append(F.col(sent_col) * F.col(clicked_col))

# The SAS sum function over an accumulator treats nulls as zero.
# We coalesce the result of each multiplication to 0 if it's null.
safe_products = [F.coalesce(p, F.lit(0)) for p in product_expressions]
if safe_products:
    pred_clicks_expr = reduce(add, safe_products)
else:
    pred_clicks_expr = F.lit(0)

df_email_contact_curmonth_scor = df_email_contact_curmonth_scor.withColumn("pred_clicks", pred_clicks_expr)

df_email_contact_curmonth_scor = df_email_contact_curmonth_scor.withColumn(
    "num_clicked_past12",
    F.coalesce(F.col("num_clicked_past12"), F.lit(0))
)

df_email_contact_curmonth_scor = df_email_contact_curmonth_scor.withColumn(
    "pctdiff_clicks",
    (F.col("num_clicked_past12") - F.col("pred_clicks")) / F.col("num_sent_past12")
)

# data email_contact_curmonth_sort
df_temp_renamed = df_email_contact_curmonth_scor.withColumnRenamed("forestresults", "forestresults_i")

df_temp_calcs = df_temp_renamed.withColumn("regscore", 
    F.lit(0.01025) + 
    (F.lit(0.00185) * F.col("ch_acq_u")) + 
    (F.lit(0.00102) * F.col("MemXRenew")) + 
    (F.lit(-0.00004588) * F.col("Age")) + 
    (F.lit(0.00316) * F.col("clickrate_past12")) + 
    (F.lit(0.27111) * F.col("clickrate_pastmonth")) + 
    (F.lit(-0.00007572) * F.col("AAES_sent")) + 
    (F.lit(0.00033604) * F.col("AAMD_sent")) + 
    (F.lit(-0.00030162) * F.col("AAOS_sent")) + 
    (F.lit(0.00052416) * F.col("AWSO_sent")) + 
    (F.lit(-0.00010579) * F.col("AAVT_sent")) + 
    (F.lit(0.00167) * F.col("AAES_click")) + 
    (F.lit(0.00244) * F.col("AWSO_click")) + 
    (F.lit(0.0075) * F.col("AAVT_click")) + 
    (F.lit(0.00644) * F.col("AAMD_click"))
)

df_temp_calcs = df_temp_calcs.withColumn(
    "forestresults",
    F.when(F.col("forestresults_i") == "", None)
     .otherwise(F.col("forestresults_i"))
     .cast("double")
)

df_email_contact_curmonth_sort = df_temp_calcs.select(
    "mid_key", "regscore", "pctdiff_clicks", "forestresults", "clickrate_past12", "clickrate_pastmonth"
)

# proc rank data=email_contact_curmonth_sort out=score_ranks ties=low descending groups=99;
df_with_dummy = df_email_contact_curmonth_sort.withColumn("dummy_partition", F.lit(1))
window_pctdiff = Window.partitionBy("dummy_partition").orderBy(F.col("pctdiff_clicks").desc())
window_regscore = Window.partitionBy("dummy_partition").orderBy(F.col("regscore").desc())
window_forest = Window.partitionBy("dummy_partition").orderBy(F.col("forestresults").desc())

df_score_ranks = df_with_dummy \
    .withColumn("centile_pctdiff", F.ntile(99).over(window_pctdiff)) \
    .withColumn("centile_reg", F.ntile(99).over(window_regscore)) \
    .withColumn("centile_forest", F.ntile(99).over(window_forest)) \
    .drop("dummy_partition")

# data score_ranks_avg;
df_score_ranks_avg = df_score_ranks.withColumn(
    "average_centile_threemethod",
    (F.col("centile_forest") + F.col("centile_pctdiff") + F.col("centile_reg")) / 3
)

# proc rank data=score_ranks_avg out=score_ranks_final ties=low groups=99;
df_avg_with_dummy = df_score_ranks_avg.withColumn("dummy_partition", F.lit(1))
window_avg = Window.partitionBy("dummy_partition").orderBy(F.col("average_centile_threemethod").asc())

df_score_ranks_final = df_avg_with_dummy \
    .withColumn("centile_average_threemethod", F.ntile(99).over(window_avg)) \
    .drop("dummy_partition")

# proc sql; create table intermed.geo_appends_rpm as ...
df_geo_appends_rpm_in = spark.table("intermed.geo_appends_rpm")

# To handle potential column collision on `em_clickrate` from `a.*`
if 'em_clickrate' in df_geo_appends_rpm_in.columns:
    df_geo_appends_rpm_in = df_geo_appends_rpm_in.drop('em_clickrate')

df_geo_appends_rpm_in.createOrReplaceTempView("geo_appends_rpm_view")
df_score_ranks_final.createOrReplaceTempView("score_ranks_final")

df_geo_appends_rpm_updated = spark.sql("""
    SELECT
        a.*,
        CASE
            WHEN b.centile_average_threemethod IS NULL THEN 99
            ELSE b.centile_average_threemethod + 1
        END AS EM_Clickrate
    FROM
        geo_appends_rpm_view AS a
    LEFT JOIN
        score_ranks_final AS b ON a.mid_key = b.mid_key
""")

df_geo_appends_rpm_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

# proc freq data=intermed.geo_appends_rpm;
df_geo_appends_rpm_final = spark.table("intermed.geo_appends_rpm")

print("Frequency Distribution for EM_Clickrate Models Score")
df_freq_em_clickrate = df_geo_appends_rpm_final.groupBy("EM_Clickrate").count().orderBy("EM_Clickrate")
df_freq_em_clickrate.show(df_freq_em_clickrate.count())

#End-DBShift