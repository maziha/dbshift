import pyspark.sql.functions as F
from pyspark.sql import SparkSession

run_prefix = ""
if runtype == "Weekly":
	run_prefix = "wkly"
if runtype == "Monthly":
	run_prefix = "mnthly"

dbutils.fs.mv(
    f"/vg01/aarp_sas/ftp/temp/{run_prefix}_SCORES_{muldate}.txt",
    f"/vg01/aarp_sas/ftp/outgoing/{run_prefix}_SCORES_{muldate}.txt"
)

dbutils.fs.mv(
    f"/vg01/aarp_sas/ftp/temp/{run_prefix}_SCORES_{muldate}.CNT",
    f"/vg01/aarp_sas/ftp/outgoing/{run_prefix}_SCORES_{muldate}.CNT"
)

dbutils.fs.mv(
    f"/vg01/aarp_sas/ftp/temp/{run_prefix}_SCORES_{muldate}.MNCNT",
    f"/vg01/aarp_sas/ftp/outgoing/{run_prefix}_SCORES_{muldate}.MNCNT"
)
#End-DBShift