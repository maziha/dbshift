from datetime import datetime
import pyspark.sql.functions as F

runtype = 'Weekly'
muldate = datetime.now().strftime('%d%b%Y').upper()

run_prefix = ""
if runtype == "Weekly":
	run_prefix = "wkly"
if runtype == "Monthly":
	run_prefix = "mnthly"

source_path_txt = f"/vg01/aarp_sas/ftp/temp/{run_prefix}_SCORES_{muldate}.txt"
destination_path_txt = f"/vg01/aarp_sas/ftp/outgoing/{run_prefix}_SCORES_{muldate}.txt"
dbutils.fs.mv(source_path_txt, destination_path_txt)

source_path_cnt = f"/vg01/aarp_sas/ftp/temp/{run_prefix}_SCORES_{muldate}.CNT"
destination_path_cnt = f"/vg01/aarp_sas/ftp/outgoing/{run_prefix}_SCORES_{muldate}.CNT"
dbutils.fs.mv(source_path_cnt, destination_path_cnt)

source_path_mncnt = f"/vg01/aarp_sas/ftp/temp/{run_prefix}_SCORES_{muldate}.MNCNT"
destination_path_mncnt = f"/vg01/aarp_sas/ftp/outgoing/{run_prefix}_SCORES_{muldate}.MNCNT"
dbutils.fs.mv(source_path_mncnt, destination_path_mncnt)

#End-DBShift