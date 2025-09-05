import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# This script assumes a SparkSession is available as 'spark'.

# The SAS macro variable '&ref' is translated to a Python variable.
ref = "ref"

# Read the source tables into DataFrames.
df_geo_appends_rpm = spark.table("intermed.geo_appends_rpm")
df_d_model_scores = spark.table(f"{ref}.d_model_scores").select("mid_key", "relationship_seg")

# Register the DataFrames as temporary SQL views for the spark.sql() call.
df_geo_appends_rpm.createOrReplaceTempView("geo_appends_rpm")
df_d_model_scores.createOrReplaceTempView("d_model_scores")

# Execute the equivalent SQL query using spark.sql().
df_intermed_geo_appends_rpm = spark.sql("""
    SELECT
        a.*,
        b.relationship_seg
    FROM
        geo_appends_rpm AS a
    LEFT JOIN
        d_model_scores AS b ON a.mid_key = b.mid_key
""")

# Overwrite the original table with the transformed data.
df_intermed_geo_appends_rpm.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends_rpm")
#End-DBShift