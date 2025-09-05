import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import *
import re

spark = SparkSession.builder.appName("SAS to PySpark Conversion").getOrCreate()

df_email_contact_model_curmonth_sc1 = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/vg04/twalters/email_contact_model_curmonth_score.csv")

df_bymailerid_transpose = spark.table("intermed.bymailerid_transpose")
df_bymailerid_transpose.printSchema()

df_contact_history_sum = spark.table("intermed.contact_history_sum")
df_contact_history_sum.printSchema()

df_email_contact_model_curmonth_sc1.createOrReplaceTempView("email_contact_model_curmonth_sc1")
df_bymailerid_transpose.createOrReplaceTempView("bymailerid_transpose")
df_contact_history_sum.createOrReplaceTempView("contact_history_sum")

df_email_contact_curmonth_scor = spark.sql("""
    select *
    from email_contact_model_curmonth_sc1 as a
    cross join bymailerid_transpose as b
    left join contact_history_sum as c
    on a.mid_key=c.mid_key
""")

all_cols = df_email_contact_curmonth_scor.columns
vlist_sent_blank = sorted([
    c for c in all_cols
    if 'sent' in c.lower()
    and 'mailer' not in c.lower()
    and 'num' not in c.lower()
    and not c.startswith('.')
    and not c.startswith('1')
    and not c.startswith('9')
])

vlist_clicked_blank = [re.sub('sent', 'clickrate', s, flags=re.IGNORECASE) for s in vlist_sent_blank]

df_email_contact_curmonth_scor_updated = df_email_contact_curmonth_scor.withColumn("AARP_clickrate", F.lit(0))

pred_clicks_expr = F.lit(0)
for sent_col, clicked_col in zip(vlist_sent_blank, vlist_clicked_blank):
    if clicked_col in df_email_contact_curmonth_scor_updated.columns:
        pred_clicks_expr = pred_clicks_expr + (F.coalesce(F.col(sent_col), F.lit(0)) * F.coalesce(F.col(clicked_col), F.lit(0)))

df_email_contact_curmonth_scor_updated = df_email_contact_curmonth_scor_updated.withColumn("pred_clicks", pred_clicks_expr)

df_email_contact_curmonth_scor_updated = df_email_contact_curmonth_scor_updated.withColumn(
    "num_clicked_past12", F.coalesce(F.col("num_clicked_past12"), F.lit(0))
)

df_email_contact_curmonth_scor_final = df_email_contact_curmonth_scor_updated.withColumn(
    "pctdiff_clicks", (F.col("num_clicked_past12") - F.col("pred_clicks")) / F.col("num_sent_past12")
)

df_renamed = df_email_contact_curmonth_scor_final.withColumnRenamed("forestresults", "forestresults_i")

df_with_regscore = df_renamed.withColumn("regscore", 
    F.lit(0.01025) + 
    (F.lit(0.00185) * F.coalesce(F.col("ch_acq_u"), F.lit(0))) + 
    (F.lit(0.00102) * F.coalesce(F.col("MemXRenew"), F.lit(0))) + 
    (F.lit(-0.00004588) * F.coalesce(F.col("Age"), F.lit(0))) + 
    (F.lit(0.00316) * F.coalesce(F.col("clickrate_past12"), F.lit(0))) + 
    (F.lit(0.27111) * F.coalesce(F.col("clickrate_pastmonth"), F.lit(0))) + 
    (F.lit(-0.00007572) * F.coalesce(F.col("AAES_sent"), F.lit(0))) + 
    (F.lit(0.00033604) * F.coalesce(F.col("AAMD_sent"), F.lit(0))) + 
    (F.lit(-0.00030162) * F.coalesce(F.col("AAOS_sent"), F.lit(0))) + 
    (F.lit(0.00052416) * F.coalesce(F.col("AWSO_sent"), F.lit(0))) + 
    (F.lit(-0.00010579) * F.coalesce(F.col("AAVT_sent"), F.lit(0))) + 
    (F.lit(0.00167) * F.coalesce(F.col("AAES_click"), F.lit(0))) + 
    (F.lit(0.00244) * F.coalesce(F.col("AWSO_click"), F.lit(0))) + 
    (F.lit(0.0075) * F.coalesce(F.col("AAVT_click"), F.lit(0))) + 
    (F.lit(0.00644) * F.coalesce(F.col("AAMD_click"), F.lit(0)))
)

df_with_forestresults = df_with_regscore.withColumn(
    "forestresults",
    F.when(F.col("forestresults_i") == "", None)
     .otherwise(F.col("forestresults_i"))
     .cast("double")
)

df_email_contact_curmonth_sort = df_with_forestresults.select(
    "mid_key", "regscore", "pctdiff_clicks", "forestresults", "clickrate_past12", "clickrate_pastmonth"
)

df_with_dummy = df_email_contact_curmonth_sort.withColumn("dummy", F.lit(1))

window_pctdiff = Window.partitionBy("dummy").orderBy(F.col("pctdiff_clicks").desc())
window_regscore = Window.partitionBy("dummy").orderBy(F.col("regscore").desc())
window_forest = Window.partitionBy("dummy").orderBy(F.col("forestresults").desc())

df_score_ranks = df_with_dummy.withColumn("centile_pctdiff", F.ntile(99).over(window_pctdiff) - 1) \
                              .withColumn("centile_reg", F.ntile(99).over(window_regscore) - 1) \
                              .withColumn("centile_forest", F.ntile(99).over(window_forest) - 1) \
                              .drop("dummy")

df_score_ranks_avg = df_score_ranks.withColumn(
    "average_centile_threemethod", 
    (F.col("centile_forest") + F.col("centile_pctdiff") + F.col("centile_reg")) / 3
)

df_avg_with_dummy = df_score_ranks_avg.withColumn("dummy", F.lit(1))
window_avg = Window.partitionBy("dummy").orderBy(F.col("average_centile_threemethod").asc())

df_score_ranks_final = df_avg_with_dummy.withColumn(
    "centile_average_threemethod", 
    F.ntile(99).over(window_avg) - 1
).drop("dummy")

df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_score_ranks_final.createOrReplaceTempView("score_ranks_final")

df_geo_appends_rpm_updated = spark.sql("""
    select 
        a.*,
		case
		    when b.centile_average_threemethod is null then 99
		    else b.centile_average_threemethod+1
		end as EM_Clickrate
	from geo_appends_rpm as a
	left join score_ranks_final as b
	on a.mid_key=b.mid_key
""")

df_geo_appends_rpm_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")

df_geo_appends_rpm_reloaded = spark.table("intermed.geo_appends_rpm")

df_freq = df_geo_appends_rpm_reloaded.groupBy("EM_Clickrate").count().orderBy("EM_Clickrate")
df_freq.show(df_freq.count(), truncate=False)

#End-DBShift