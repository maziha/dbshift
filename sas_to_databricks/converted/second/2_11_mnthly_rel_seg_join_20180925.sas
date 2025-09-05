import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sas_conversion").getOrCreate()

ref = "your_ref_db"

df_geo_appends_rpm_source = spark.table("intermed.geo_appends_rpm")
df_d_model_scores = spark.table(f"{ref}.d_model_scores")

df_geo_appends_rpm_source.createOrReplaceTempView("geo_appends_rpm")
df_d_model_scores.createOrReplaceTempView("d_model_scores")

df_geo_appends_rpm = spark.sql("""
    SELECT
        a.*,
        b.relationship_seg
    FROM
        geo_appends_rpm AS a
    LEFT JOIN
        d_model_scores AS b
        ON a.mid_key = b.mid_key
""")
#End-DBShift